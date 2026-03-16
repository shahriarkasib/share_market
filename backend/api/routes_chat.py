"""Chat endpoint — proxies to GCP VM where Claude CLI runs, or runs locally."""

import asyncio
import logging
import os
import subprocess
import tempfile
import time
import uuid

import httpx
from fastapi import APIRouter
from pydantic import BaseModel

logger = logging.getLogger(__name__)
router = APIRouter()

# GCP VM chat service URL — set this env var on Render
GCP_CHAT_URL = os.getenv("GCP_CHAT_URL", "")  # e.g. "http://34.63.227.229:8787"

# ── Session store (in-memory) ────────────────────────────────────────
_sessions: dict[str, dict] = {}
SESSION_TTL = 24 * 3600
MAX_HISTORY = 20

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


# ── Claude CLI (runs on GCP VM directly) ─────────────────────────────

def _get_claude_env() -> dict:
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
    parts = [SYSTEM_PROMPT, "\n\nConversation so far:"]
    for msg in messages[-MAX_HISTORY:]:
        role = "User" if msg["role"] == "user" else "Assistant"
        parts.append(f"\n{role}: {msg['content']}")
    parts.append(f"\nUser: {new_message}")
    parts.append("\n\nRespond helpfully and concisely.")
    return "\n".join(parts)


def _call_claude_cli(prompt: str, timeout: int = 120) -> str:
    prompt_file = None
    try:
        prompt_file = tempfile.NamedTemporaryFile(
            mode="w", suffix=".txt", delete=False, prefix="claude_chat_"
        )
        prompt_file.write(prompt)
        prompt_file.close()

        env = _get_claude_env()
        model = os.getenv("CLAUDE_MODEL", "sonnet")
        bash_cmd = f'cat "{prompt_file.name}" | claude -p --model {model} --max-turns 1 --no-tool-use'

        result = subprocess.run(
            ["bash", "-c", bash_cmd],
            capture_output=True, text=True, timeout=timeout, env=env,
        )

        stderr_msg = (result.stderr or "").strip()
        if stderr_msg:
            logger.warning(f"Claude CLI stderr: {stderr_msg[:500]}")
        if result.returncode != 0:
            logger.error(f"Claude CLI error (exit {result.returncode}): {(stderr_msg or result.stdout or '')[:300]}")
            return "Sorry, I couldn't process your request right now. Please try again."

        resp = result.stdout.strip()
        if "Not logged in" in resp or "Please run /login" in resp:
            logger.error("Claude CLI not authenticated.")
            return "Claude is not authenticated on the server."
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


# ── Chat processing ──────────────────────────────────────────────────

async def _process_local(session_id: str, message: str) -> str:
    """Process chat locally using Claude CLI."""
    session = _sessions.get(session_id, {"messages": [], "last_active": time.time()})
    prompt = _build_prompt(session["messages"], message)
    loop = asyncio.get_event_loop()
    response = await loop.run_in_executor(None, _call_claude_cli, prompt)
    session["messages"].append({"role": "user", "content": message})
    session["messages"].append({"role": "assistant", "content": response})
    session["last_active"] = time.time()
    if len(session["messages"]) > MAX_HISTORY * 2:
        session["messages"] = session["messages"][-(MAX_HISTORY * 2):]
    _sessions[session_id] = session
    return response


async def _process_via_gcp(session_id: str, message: str) -> str:
    """Proxy chat to GCP VM chat service."""
    async with httpx.AsyncClient(timeout=130) as client:
        resp = await client.post(
            f"{GCP_CHAT_URL}/chat",
            json={"message": message, "session_id": session_id},
        )
        resp.raise_for_status()
        data = resp.json()
        # Sync session history from GCP
        _sessions[session_id] = {
            "messages": data.get("history", []),
            "last_active": time.time(),
        }
        return data["response"]


# ── Request queue ────────────────────────────────────────────────────
_queue: asyncio.Queue | None = None
_worker_started = False


async def _queue_worker():
    global _queue
    while True:
        session_id, message, future = await _queue.get()
        try:
            if GCP_CHAT_URL:
                result = await _process_via_gcp(session_id, message)
            else:
                result = await _process_local(session_id, message)
            future.set_result(result)
        except Exception as e:
            logger.error(f"Chat worker error: {e}")
            future.set_result(f"Error: {e}")
        finally:
            _queue.task_done()


def _cleanup_sessions():
    now = time.time()
    expired = [sid for sid, s in _sessions.items() if now - s["last_active"] > SESSION_TTL]
    for sid in expired:
        del _sessions[sid]


@router.post("", response_model=ChatResponse)
async def chat(req: ChatRequest):
    global _queue, _worker_started

    if _queue is None:
        _queue = asyncio.Queue()
    if not _worker_started:
        asyncio.create_task(_queue_worker())
        _worker_started = True

    _cleanup_sessions()

    session_id = req.session_id or str(uuid.uuid4())
    if session_id not in _sessions:
        _sessions[session_id] = {"messages": [], "last_active": time.time()}

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
    session = _sessions.get(session_id)
    if not session:
        return {"session_id": session_id, "messages": []}
    return {"session_id": session_id, "messages": session["messages"]}


@router.delete("/sessions/{session_id}")
async def clear_session(session_id: str):
    _sessions.pop(session_id, None)
    return {"status": "cleared"}
