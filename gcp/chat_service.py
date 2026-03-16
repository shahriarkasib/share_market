#!/usr/bin/env python3
"""Chat service for GCP VM — full Claude Code experience via CLI.

Claude gets full tool access (Bash, Read, etc.) so it can query the database,
read files, and analyze data — just like an interactive Claude Code session.

Run: python3 gcp/chat_service.py
Listens on port 8787.
"""

import json
import logging
import os
import subprocess
import tempfile
import time
import uuid
from http.server import HTTPServer, BaseHTTPRequestHandler
from threading import Lock

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%H:%M:%S",
)
logger = logging.getLogger(__name__)

PORT = int(os.getenv("CHAT_PORT", "8787"))
MAX_HISTORY = 20
SESSION_TTL = 24 * 3600

SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
PROJECT_DIR = os.path.dirname(SCRIPT_DIR)
BACKEND_DIR = os.path.join(PROJECT_DIR, "backend")

# System prompt injected via --append-system-prompt
# This tells Claude WHO it is and WHAT it can do
SYSTEM_PROMPT = """You are a DSE (Dhaka Stock Exchange) trading assistant with FULL access to a PostgreSQL database containing real market data.

IMPORTANT CONTEXT:
- You are running on a GCP VM with access to the project codebase at {project_dir}
- The backend is at {backend_dir}
- You can run Python scripts using {venv_python}
- Database: Supabase PostgreSQL (connection string in backend/config.py)

TO QUERY MARKET DATA, run Python like this:
```python
import psycopg2
conn = psycopg2.connect('postgresql://postgres.iihlezpkpllacztoaguc:160021062Ss%23%23@aws-1-ap-southeast-1.pooler.supabase.com:6543/postgres')
cur = conn.cursor()
cur.execute("SELECT ...")
```

KEY TABLES:
- daily_prices (symbol, date, open, high, low, close, volume) — historical OHLCV
- live_prices (symbol, ltp, high, low, open, close_prev, change, change_pct, volume, updated_at) — latest prices
- daily_analysis (symbol, date, ltp, action, score, entry_low, entry_high, sl, t1, t2, rsi, cmf, stoch_rsi, adx, mfi, macd_status, support, resistance, ...) — algo analysis
- judge_daily_analysis (symbol, date, final_action, final_confidence, entry_low, entry_high, sl, t1, t2) — AI judge verdicts
- fundamentals (symbol, sector, category, company_name) — stock info
- dsex_history (date, dsex_index, total_volume, total_value) — DSEX index
- seasonality_monthly (symbol, month, avg_return, win_rate, median_return, cohens_d, ...) — seasonal patterns

DSE MARKET RULES:
- Currency: BDT (Bangladeshi Taka). Tick size: 0.10 BDT.
- Weekends: Friday + Saturday. Sunday IS a trading day.
- T+2 settlement. Categories: A (best), B, Z.
- Today's date: use Python datetime to get current date.

USER PROFILE:
- Name: Sourav. Friend: Husmoy.
- Halal-only investor. Prefers 20-100 BDT stocks.
- 2-stock strategy, 5% target.
- Current portfolio: ORIONINFU 395@362, HWAWELLTEX 1000@44.98, SPCERAMICS 5612@20.3
- Cash available: ~104K BDT (sold GP at 254)

BEHAVIOR:
- Be concise and actionable. Give clear buy/sell/hold with stop-loss levels.
- Format prices with 1 decimal. Use tables for comparisons.
- Query the database for REAL data — never guess prices or indicators.
- When asked about market/stocks, ALWAYS query live_prices and daily_analysis.
""".format(
    project_dir=PROJECT_DIR,
    backend_dir=BACKEND_DIR,
    venv_python=os.path.join(PROJECT_DIR, "venv", "bin", "python3"),
)

# ── Session store ────────────────────────────────────────────────────
sessions: dict[str, dict] = {}
sessions_lock = Lock()


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
    parts = []
    if messages:
        parts.append("Previous conversation:")
        for msg in messages[-MAX_HISTORY:]:
            role = "User" if msg["role"] == "user" else "Assistant"
            parts.append(f"\n{role}: {msg['content']}")
        parts.append("\n---\n")
    parts.append(f"User: {new_message}")
    return "\n".join(parts)


def call_claude(prompt: str, timeout: int = 180) -> str:
    """Call Claude CLI with full tool access — like a real Claude Code session."""
    prompt_file = None
    sys_prompt_file = None
    try:
        # Write prompt to file
        prompt_file = tempfile.NamedTemporaryFile(
            mode="w", suffix=".txt", delete=False, prefix="claude_chat_"
        )
        prompt_file.write(prompt)
        prompt_file.close()

        # Write system prompt to file (avoid shell escaping issues)
        sys_prompt_file = tempfile.NamedTemporaryFile(
            mode="w", suffix=".txt", delete=False, prefix="claude_sys_"
        )
        sys_prompt_file.write(SYSTEM_PROMPT)
        sys_prompt_file.close()

        env = get_claude_env()
        model = os.getenv("CLAUDE_MODEL", "sonnet")

        # Key flags:
        # --max-turns 10: allow multiple tool calls (query DB, read files, etc.)
        # --dangerously-skip-permissions: don't prompt for permission
        # --add-dir: give access to the backend directory
        cmd = [
            "bash", "-c",
            f'cat "{prompt_file.name}" | claude -p '
            f'--model {model} '
            f'--max-turns 10 '
            f'--dangerously-skip-permissions '
            f'--add-dir "{BACKEND_DIR}" '
            f'--append-system-prompt "$(cat {sys_prompt_file.name})"'
        ]

        result = subprocess.run(
            cmd,
            capture_output=True,
            text=True,
            timeout=timeout,
            env=env,
            cwd=BACKEND_DIR,  # Run from backend dir so it can access files
        )

        stderr_msg = (result.stderr or "").strip()
        if stderr_msg and "error" in stderr_msg.lower():
            logger.warning(f"Claude stderr: {stderr_msg[:500]}")

        if result.returncode != 0:
            logger.error(f"Claude error (exit {result.returncode}): {(stderr_msg or result.stdout or '')[:500]}")
            return "Sorry, couldn't process your request. Please try again."

        resp = result.stdout.strip()
        if "Not logged in" in resp or "Please run /login" in resp:
            return "Claude is not authenticated. Check CLAUDE_CODE_OAUTH_TOKEN."
        if not resp:
            return "Empty response. Please try again."

        logger.info(f"Response: {len(resp)} chars ({model})")
        return resp

    except subprocess.TimeoutExpired:
        logger.error(f"Claude timed out ({timeout}s)")
        return "Request timed out. Try a simpler question."
    except FileNotFoundError:
        logger.error("Claude CLI not found")
        return "Claude CLI not installed."
    finally:
        for f in [prompt_file, sys_prompt_file]:
            if f and os.path.exists(f.name):
                os.unlink(f.name)


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
        pass


def main():
    logger.info(f"Starting DSE Chat Service (full Claude Code mode)...")
    logger.info(f"Project: {PROJECT_DIR}")
    logger.info(f"Backend: {BACKEND_DIR}")

    env = get_claude_env()
    token_set = bool(env.get("CLAUDE_CODE_OAUTH_TOKEN"))
    claude_path = subprocess.run(['which', 'claude'], capture_output=True, text=True).stdout.strip()
    logger.info(f"Claude CLI: {claude_path}")
    logger.info(f"OAuth token: {'set' if token_set else 'NOT SET'}")
    logger.info(f"Model: {os.getenv('CLAUDE_MODEL', 'sonnet')}")
    logger.info(f"Max turns: 10 (full tool access)")

    server = HTTPServer(("0.0.0.0", PORT), ChatHandler)
    logger.info(f"Listening on http://0.0.0.0:{PORT}")

    try:
        server.serve_forever()
    except KeyboardInterrupt:
        logger.info("Shutting down...")
        server.shutdown()


if __name__ == "__main__":
    main()
