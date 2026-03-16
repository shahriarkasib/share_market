"""Chat endpoint — pipes user messages to Claude CLI (Max subscription)."""

import asyncio
import logging
import os
import subprocess
import tempfile
import time
import uuid

from fastapi import APIRouter
from pydantic import BaseModel

logger = logging.getLogger(__name__)
router = APIRouter()

# ── Session store (in-memory) ────────────────────────────────────────
# { session_id: { "messages": [{role, content}, ...], "last_active": timestamp } }
_sessions: dict[str, dict] = {}
SESSION_TTL = 24 * 3600  # 24 hours
MAX_HISTORY = 20  # keep last N messages per session

# ── Request queue (serializes Claude CLI calls) ──────────────────────
_queue: asyncio.Queue | None = None
_worker_started = False

SYSTEM_PROMPT = """You are a DSE (Dhaka Stock Exchange) trading assistant. You help users analyze Bangladesh stock market data, make trading decisions, and understand market trends.

Key facts:
- DSE uses BDT (Bangladeshi Taka). Minimum tick size: 0.10 BDT.
- Bangladesh weekends: Friday + Saturday. Sunday is a trading day.
- T+2 settlement cycle.
- Stock categories: A (best), B, Z (worst).
- DSEX is the main index.

You have access to market data through the DSE Trading Assistant platform. Be concise, actionable, and honest about uncertainty. When discussing specific stocks, mention key indicators (RSI, MACD, CMF, volume) and give clear buy/sell/hold recommendations with stop-loss levels.

Always format numbers with 1 decimal place for prices. Use tables when comparing multiple stocks."""


class ChatRequest(BaseModel):
    message: str
    session_id: str | None = None


class ChatResponse(BaseModel):
    session_id: str
    response: str
    history: list[dict]


def _get_claude_env() -> dict:
    """Build environment with CLAUDE_CODE_OAUTH_TOKEN."""
    env = os.environ.copy()
    if not env.get("CLAUDE_CODE_OAUTH_TOKEN"):
        bashrc = os.path.expanduser("~/.bashrc")
        if os.path.exists(bashrc):
            try:
                with open(bashrc) as f:
                    for line in f:
                        if "CLAUDE_CODE_OAUTH_TOKEN=" in line and line.strip().startswith("export"):
                            token = line.split('"')[1] if '"' in line else line.split("=", 1)[1].strip()
                            env["CLAUDE_CODE_OAUTH_TOKEN"] = token
                            break
            except Exception as e:
                logger.warning(f"Failed to extract token from .bashrc: {e}")
    return env


def _build_prompt(messages: list[dict], new_message: str) -> str:
    """Build full prompt with system context + conversation history."""
    parts = [SYSTEM_PROMPT, "\n\nConversation so far:"]
    for msg in messages[-MAX_HISTORY:]:
        role = "User" if msg["role"] == "user" else "Assistant"
        parts.append(f"\n{role}: {msg['content']}")
    parts.append(f"\nUser: {new_message}")
    parts.append("\n\nRespond helpfully and concisely.")
    return "\n".join(parts)


def _call_claude_cli(prompt: str, timeout: int = 120) -> str:
    """Call Claude via CLI subprocess (same as llm_daily_analyzer.py)."""
    prompt_file = None
    try:
        prompt_file = tempfile.NamedTemporaryFile(
            mode="w", suffix=".txt", delete=False, prefix="claude_chat_"
        )
        prompt_file.write(prompt)
        prompt_file.close()

        env = _get_claude_env()
        model = os.getenv("CLAUDE_MODEL", "sonnet")
        bash_cmd = f'cat "{prompt_file.name}" | claude -p --model {model} --max-turns 1'

        result = subprocess.run(
            ["bash", "-c", bash_cmd],
            capture_output=True,
            text=True,
            timeout=timeout,
            env=env,
        )

        stderr_msg = (result.stderr or "").strip()
        if stderr_msg:
            logger.warning(f"Claude CLI stderr: {stderr_msg[:500]}")

        if result.returncode != 0:
            err_msg = (stderr_msg or result.stdout or "")[:300]
            logger.error(f"Claude CLI error (exit {result.returncode}): {err_msg}")
            return "Sorry, I couldn't process your request right now. Please try again."

        resp = result.stdout.strip()
        if "Not logged in" in resp or "Please run /login" in resp:
            logger.error("Claude CLI not authenticated.")
            return "Claude is not authenticated on the server. Please check the CLAUDE_CODE_OAUTH_TOKEN."

        if not resp:
            return "I received an empty response. Please try again."

        logger.info(f"Chat response: {len(resp)} chars")
        return resp

    except subprocess.TimeoutExpired:
        logger.error(f"Claude CLI timed out ({timeout}s)")
        return "Request timed out. Please try a shorter question."
    except FileNotFoundError:
        logger.error("Claude CLI not found")
        return "Claude CLI is not installed on the server."
    finally:
        if prompt_file and os.path.exists(prompt_file.name):
            os.unlink(prompt_file.name)


async def _process_chat(session_id: str, message: str) -> str:
    """Process a chat message (runs CLI in thread pool)."""
    session = _sessions.get(session_id, {"messages": [], "last_active": time.time()})
    prompt = _build_prompt(session["messages"], message)

    loop = asyncio.get_event_loop()
    response = await loop.run_in_executor(None, _call_claude_cli, prompt)

    # Update session
    session["messages"].append({"role": "user", "content": message})
    session["messages"].append({"role": "assistant", "content": response})
    session["last_active"] = time.time()

    # Trim history
    if len(session["messages"]) > MAX_HISTORY * 2:
        session["messages"] = session["messages"][-(MAX_HISTORY * 2):]

    _sessions[session_id] = session
    return response


async def _queue_worker():
    """Background worker that processes chat requests sequentially."""
    global _queue
    while True:
        session_id, message, future = await _queue.get()
        try:
            result = await _process_chat(session_id, message)
            future.set_result(result)
        except Exception as e:
            logger.error(f"Chat worker error: {e}")
            future.set_result("An error occurred. Please try again.")
        finally:
            _queue.task_done()


def _cleanup_sessions():
    """Remove expired sessions."""
    now = time.time()
    expired = [sid for sid, s in _sessions.items() if now - s["last_active"] > SESSION_TTL]
    for sid in expired:
        del _sessions[sid]


@router.post("", response_model=ChatResponse)
async def chat(req: ChatRequest):
    global _queue, _worker_started

    # Initialize queue and worker on first request
    if _queue is None:
        _queue = asyncio.Queue()
    if not _worker_started:
        asyncio.create_task(_queue_worker())
        _worker_started = True

    # Cleanup old sessions
    _cleanup_sessions()

    # Get or create session
    session_id = req.session_id or str(uuid.uuid4())
    if session_id not in _sessions:
        _sessions[session_id] = {"messages": [], "last_active": time.time()}

    # Queue the request and wait for result
    future = asyncio.get_event_loop().create_future()
    await _queue.put((session_id, req.message, future))
    response = await future

    return ChatResponse(
        session_id=session_id,
        response=response,
        history=_sessions[session_id]["messages"],
    )


@router.get("/sessions/{session_id}")
async def get_session(session_id: str):
    """Retrieve conversation history for a session."""
    session = _sessions.get(session_id)
    if not session:
        return {"session_id": session_id, "messages": []}
    return {"session_id": session_id, "messages": session["messages"]}


@router.delete("/sessions/{session_id}")
async def clear_session(session_id: str):
    """Clear a chat session."""
    _sessions.pop(session_id, None)
    return {"status": "cleared"}
