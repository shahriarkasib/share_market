#!/usr/bin/env python3
"""Lightweight chat service for GCP VM — receives requests, calls Claude CLI.

Run: python3 gcp/chat_service.py
Listens on port 8787. Render proxies /api/v1/chat → here.
"""

import asyncio
import json
import logging
import os
import subprocess
import sys
import tempfile
import time
import uuid
from http.server import HTTPServer, BaseHTTPRequestHandler
from threading import Thread, Lock
from queue import Queue

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%H:%M:%S",
)
logger = logging.getLogger(__name__)

PORT = int(os.getenv("CHAT_PORT", "8787"))
MAX_HISTORY = 20
SESSION_TTL = 24 * 3600

SYSTEM_PROMPT = """You are a DSE (Dhaka Stock Exchange) trading assistant. You help users analyze Bangladesh stock market data, make trading decisions, and understand market trends.

Key facts:
- DSE uses BDT (Bangladeshi Taka). Minimum tick size: 0.10 BDT.
- Bangladesh weekends: Friday + Saturday. Sunday is a trading day.
- T+2 settlement cycle.
- Stock categories: A (best), B, Z (worst).
- DSEX is the main index.

Be concise, actionable, and honest about uncertainty. When discussing specific stocks, mention key indicators (RSI, MACD, CMF, volume) and give clear buy/sell/hold recommendations with stop-loss levels.
Format prices with 1 decimal place. Use tables when comparing multiple stocks."""

# ── Session store ────────────────────────────────────────────────────
sessions: dict[str, dict] = {}
sessions_lock = Lock()

# ── Claude CLI queue (one at a time) ────────────────────────────────
claude_queue: Queue = Queue()


def get_claude_env() -> dict:
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
            except Exception:
                pass
    return env


def build_prompt(messages: list[dict], new_message: str) -> str:
    parts = [SYSTEM_PROMPT, "\n\nConversation so far:"]
    for msg in messages[-MAX_HISTORY:]:
        role = "User" if msg["role"] == "user" else "Assistant"
        parts.append(f"\n{role}: {msg['content']}")
    parts.append(f"\nUser: {new_message}")
    parts.append("\n\nRespond helpfully and concisely.")
    return "\n".join(parts)


def call_claude(prompt: str, timeout: int = 120) -> str:
    prompt_file = None
    try:
        prompt_file = tempfile.NamedTemporaryFile(
            mode="w", suffix=".txt", delete=False, prefix="claude_chat_"
        )
        prompt_file.write(prompt)
        prompt_file.close()

        env = get_claude_env()
        model = os.getenv("CLAUDE_MODEL", "sonnet")
        bash_cmd = f'cat "{prompt_file.name}" | claude -p --model {model} --max-turns 1 --append-system-prompt "You are a DSE (Dhaka Stock Exchange) trading assistant. Answer questions directly about Bangladesh stock market. Do NOT attempt to use any tools, read files, or run commands. Just respond with helpful text based on your knowledge."'

        result = subprocess.run(
            ["bash", "-c", bash_cmd],
            capture_output=True, text=True, timeout=timeout, env=env,
        )

        if result.returncode != 0:
            logger.error(f"Claude error (exit {result.returncode}): {(result.stderr or '')[:300]}")
            return "Sorry, couldn't process your request. Please try again."

        resp = result.stdout.strip()
        if "Not logged in" in resp or "Please run /login" in resp:
            return "Claude is not authenticated. Check CLAUDE_CODE_OAUTH_TOKEN."
        if not resp:
            return "Empty response from Claude. Please try again."

        logger.info(f"Response: {len(resp)} chars")
        return resp

    except subprocess.TimeoutExpired:
        return "Request timed out. Try a shorter question."
    except FileNotFoundError:
        return "Claude CLI not found on this server."
    finally:
        if prompt_file and os.path.exists(prompt_file.name):
            os.unlink(prompt_file.name)


def cleanup_sessions():
    now = time.time()
    with sessions_lock:
        expired = [sid for sid, s in sessions.items() if now - s["last_active"] > SESSION_TTL]
        for sid in expired:
            del sessions[sid]


class ChatHandler(BaseHTTPRequestHandler):
    def do_POST(self):
        if self.path == "/chat":
            self._handle_chat()
        else:
            self._respond(404, {"error": "not found"})

    def do_GET(self):
        if self.path == "/health":
            self._respond(200, {"status": "ok", "sessions": len(sessions)})
        elif self.path.startswith("/chat/sessions/"):
            sid = self.path.split("/")[-1]
            with sessions_lock:
                session = sessions.get(sid)
            if session:
                self._respond(200, {"session_id": sid, "messages": session["messages"]})
            else:
                self._respond(200, {"session_id": sid, "messages": []})
        else:
            self._respond(404, {"error": "not found"})

    def do_DELETE(self):
        if self.path.startswith("/chat/sessions/"):
            sid = self.path.split("/")[-1]
            with sessions_lock:
                sessions.pop(sid, None)
            self._respond(200, {"status": "cleared"})
        else:
            self._respond(404, {"error": "not found"})

    def _handle_chat(self):
        try:
            length = int(self.headers.get("Content-Length", 0))
            body = json.loads(self.rfile.read(length))
        except Exception:
            self._respond(400, {"error": "invalid JSON"})
            return

        message = body.get("message", "").strip()
        if not message:
            self._respond(400, {"error": "message required"})
            return

        session_id = body.get("session_id") or str(uuid.uuid4())
        cleanup_sessions()

        with sessions_lock:
            if session_id not in sessions:
                sessions[session_id] = {"messages": [], "last_active": time.time()}
            session = sessions[session_id]

        prompt = build_prompt(session["messages"], message)
        logger.info(f"[{session_id[:8]}] Processing: {message[:80]}...")

        response = call_claude(prompt)

        with sessions_lock:
            session["messages"].append({"role": "user", "content": message})
            session["messages"].append({"role": "assistant", "content": response})
            session["last_active"] = time.time()
            if len(session["messages"]) > MAX_HISTORY * 2:
                session["messages"] = session["messages"][-(MAX_HISTORY * 2):]
            history = list(session["messages"])

        self._respond(200, {
            "session_id": session_id,
            "response": response,
            "history": history,
        })

    def _respond(self, code: int, data: dict):
        self.send_response(code)
        self.send_header("Content-Type", "application/json")
        self.send_header("Access-Control-Allow-Origin", "*")
        self.send_header("Access-Control-Allow-Methods", "GET, POST, DELETE, OPTIONS")
        self.send_header("Access-Control-Allow-Headers", "Content-Type")
        self.end_headers()
        self.wfile.write(json.dumps(data).encode())

    def do_OPTIONS(self):
        self.send_response(200)
        self.send_header("Access-Control-Allow-Origin", "*")
        self.send_header("Access-Control-Allow-Methods", "GET, POST, DELETE, OPTIONS")
        self.send_header("Access-Control-Allow-Headers", "Content-Type")
        self.end_headers()

    def log_message(self, format, *args):
        # Suppress default HTTP logging, we use our own
        pass


def main():
    logger.info(f"Starting chat service on port {PORT}...")

    # Verify Claude CLI
    env = get_claude_env()
    token_set = bool(env.get("CLAUDE_CODE_OAUTH_TOKEN"))
    logger.info(f"Claude CLI: {subprocess.run(['which', 'claude'], capture_output=True, text=True).stdout.strip()}")
    logger.info(f"OAuth token: {'set' if token_set else 'NOT SET'}")

    server = HTTPServer(("0.0.0.0", PORT), ChatHandler)
    logger.info(f"Chat service running on http://0.0.0.0:{PORT}")
    logger.info(f"Health check: http://0.0.0.0:{PORT}/health")

    try:
        server.serve_forever()
    except KeyboardInterrupt:
        logger.info("Shutting down...")
        server.shutdown()


if __name__ == "__main__":
    main()
