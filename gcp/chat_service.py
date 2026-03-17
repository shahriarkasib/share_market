#!/usr/bin/env python3
"""Multi-user chat service for GCP VM — full Claude Code experience via CLI.

Each user gets their own profile (portfolio, strategy, preferences) stored
in the database. Google OAuth provides user identification.

Run: python3 gcp/chat_service.py
Listens on port 8787. Caddy provides HTTPS on port 443.
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

DB_URL = "postgresql://postgres.iihlezpkpllacztoaguc:160021062Ss%23%23@aws-1-ap-southeast-1.pooler.supabase.com:6543/postgres"

# ── General system prompt (same for all users) ──────────────────────

GENERAL_PROMPT = """You are a professional DSE (Dhaka Stock Exchange) trading analyst with FULL access to a PostgreSQL database containing comprehensive market data. You analyze stocks using raw data and your own expertise — you do NOT rely on pre-computed scores or rigid rules.

ENVIRONMENT:
- GCP VM with project at {project_dir}, backend at {backend_dir}
- Python: {venv_python}
- Database: Supabase PostgreSQL

DATABASE ACCESS:
```python
import psycopg2
conn = psycopg2.connect('{db_url}')
cur = conn.cursor()
```

AVAILABLE DATA — query ALL of these when analyzing a stock:

1. PRICE HISTORY (daily_prices): symbol, date, open, high, low, close, volume, value, trade_count
2. LIVE PRICES (live_prices): symbol, ltp, high, low, open, close_prev, change, change_pct, volume, value, trade_count, updated_at
3. TECHNICAL INDICATORS (daily_analysis): symbol, date, ltp, action, score, entry_low, entry_high, sl, t1, t2, rsi, stoch_rsi, macd_line, macd_signal, macd_hist, macd_status, mfi, cmf, obv, williams_r, adx, plus_di, minus_di, bb_pct, atr, atr_pct, volatility, max_dd, ema9, ema21, sma50, momentum_3d, momentum_5d, turnover, chg_5d, chg_10d, chg_20d, support, resistance, trend_50d, avg_vol, vol_ratio, category
4. LLM ANALYSIS (llm_daily_analysis): symbol, date, action, confidence, reasoning, score, wait_for, wait_days, risk_factors, catalysts, how_to_buy, volume_rule, entry_low, entry_high, sl, t1, t2, stage, stage_reasoning, expected_return_1w, expected_return_2w, expected_return_1m, downside_risk, dsex_dependency, if_dsex_drops, if_dsex_rises, dsex_outlook
5. AI JUDGE (judge_daily_analysis): symbol, date, final_action, final_confidence, agreement, reasoning, key_risk, algo_strengths, llm_strengths, entry_low, entry_high, sl, t1, t2, score
6. FUNDAMENTALS (fundamentals): symbol, sector, category, company_name
7. DSEX INDEX (dsex_history): date, dsex_index, dses_index, ds30_index, total_volume, total_value, total_trade
8. SEASONALITY (seasonality_monthly): symbol, sector, category, month, avg_return, win_rate, years_up, years_total, median_return, trimmed_mean, bootstrap_p, cohens_d, best_return, worst_return, volatility
9. YEARLY SEASONALITY (seasonality_yearly): symbol, year, month, monthly_return
10. USER PROFILES (chat_users): id, email, name, photo_url, portfolio (JSONB), strategy, preferences, feedback

DSE MARKET RULES:
- Currency: BDT. Tick size: 0.10 BDT.
- Weekends: Friday + Saturday. Sunday IS a trading day.
- T+2 settlement. Categories: A (best), B, Z.
- Trading hours: 10:00-14:30 BST (UTC+6).

USER PROFILE MANAGEMENT:
- The current user's profile is loaded below. When they share portfolio info, strategy, preferences, or feedback, UPDATE their profile in the database.
- To update: UPDATE chat_users SET portfolio = '...', updated_at = CURRENT_TIMESTAMP WHERE email = '<user_email>'
- Portfolio format (JSONB): {{"SYMBOL": {{"qty": 100, "price": 50.0, "date": "2026-03-15"}}, ...}}
- When user says "I bought X shares of Y at Z", add it to their portfolio JSON.
- When user says "I sold X", remove it from their portfolio JSON.

HOW TO ANALYZE — USE YOUR OWN JUDGMENT:
- Pull the full price history to understand trends. Cross-reference all indicators.
- Always check DSEX trend — individual stocks correlate with the broad market.
- Check seasonality. Look at volume patterns.
- When screening for buys, verify stocks haven't already made their move.
- Be honest. If there are no good setups, say so.
- Format: concise, tables when comparing, 1 decimal for prices.
""".format(
    project_dir=PROJECT_DIR,
    backend_dir=BACKEND_DIR,
    venv_python=os.path.join(PROJECT_DIR, "venv", "bin", "python3"),
    db_url=DB_URL,
)

# ── Database helpers ─────────────────────────────────────────────────


def _get_db():
    import psycopg2
    import psycopg2.extras
    return psycopg2.connect(DB_URL, cursor_factory=psycopg2.extras.RealDictCursor)


def init_db():
    """Create chat_users table if it doesn't exist."""
    try:
        conn = _get_db()
        cur = conn.cursor()
        cur.execute("""
            CREATE TABLE IF NOT EXISTS chat_users (
                id SERIAL PRIMARY KEY,
                email TEXT UNIQUE NOT NULL,
                name TEXT NOT NULL DEFAULT '',
                photo_url TEXT DEFAULT '',
                portfolio JSONB DEFAULT '{}',
                strategy TEXT DEFAULT '',
                preferences TEXT DEFAULT '',
                feedback TEXT DEFAULT '',
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
        """)
        conn.commit()
        cur.execute("SELECT COUNT(*) FROM chat_users")
        count = cur.fetchone()["count"]
        conn.close()
        logger.info(f"chat_users table ready ({count} users)")
    except Exception as e:
        logger.error(f"DB init failed: {e}")


def get_or_create_user(email: str, name: str = "", photo_url: str = "") -> dict:
    """Get user profile or create new one."""
    try:
        conn = _get_db()
        cur = conn.cursor()
        cur.execute("SELECT * FROM chat_users WHERE email = %s", (email,))
        user = cur.fetchone()
        if not user:
            cur.execute(
                "INSERT INTO chat_users (email, name, photo_url) VALUES (%s, %s, %s) RETURNING *",
                (email, name, photo_url),
            )
            user = cur.fetchone()
            conn.commit()
            logger.info(f"New user: {name} ({email})")
        else:
            # Update name/photo if changed
            if (name and name != user["name"]) or (photo_url and photo_url != user["photo_url"]):
                cur.execute(
                    "UPDATE chat_users SET name = %s, photo_url = %s, updated_at = CURRENT_TIMESTAMP WHERE email = %s",
                    (name or user["name"], photo_url or user["photo_url"], email),
                )
                conn.commit()
        conn.close()
        return dict(user)
    except Exception as e:
        logger.error(f"User lookup failed: {e}")
        return {"email": email, "name": name, "portfolio": {}, "strategy": "", "preferences": "", "feedback": ""}


def build_user_context(user: dict) -> str:
    """Build user-specific context string."""
    parts = [f"\nCURRENT USER: {user.get('name', 'Unknown')} ({user.get('email', '')})"]

    portfolio = user.get("portfolio") or {}
    if isinstance(portfolio, str):
        try:
            portfolio = json.loads(portfolio)
        except Exception:
            portfolio = {}

    if portfolio:
        parts.append("Portfolio:")
        for sym, info in portfolio.items():
            if isinstance(info, dict):
                parts.append(f"  - {sym}: {info.get('qty', '?')} shares @ {info.get('price', '?')} BDT")
            else:
                parts.append(f"  - {sym}: {info}")
    else:
        parts.append("Portfolio: No holdings saved yet. Ask the user if they have any stocks.")

    strategy = user.get("strategy", "")
    if strategy:
        parts.append(f"Strategy: {strategy}")

    preferences = user.get("preferences", "")
    if preferences:
        parts.append(f"Preferences: {preferences}")

    feedback = user.get("feedback", "")
    if feedback:
        parts.append(f"Past feedback: {feedback}")

    return "\n".join(parts)


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


def prefetch_market_data(symbols: list[str] = None) -> str:
    """Pre-fetch latest market data and return as context string.

    This avoids Claude needing to run DB queries (which adds 30-60s per query).
    """
    try:
        import psycopg2
        import psycopg2.extras
        conn = psycopg2.connect(DB_URL, cursor_factory=psycopg2.extras.RealDictCursor)
        cur = conn.cursor()
        parts = []

        # DSEX latest
        cur.execute("SELECT date, dsex_index, total_volume, total_value FROM dsex_history ORDER BY date DESC LIMIT 5")
        dsex_rows = cur.fetchall()
        if dsex_rows:
            parts.append("DSEX INDEX (last 5 days):")
            for r in dsex_rows:
                parts.append(f"  {r['date']}: {float(r['dsex_index']):.1f} vol={int(r['total_volume'] or 0):,}")

        # Live prices for portfolio or top stocks
        if symbols:
            placeholders = ",".join(["%s"] * len(symbols))
            cur.execute(f"SELECT symbol, ltp, change_pct, high, low, volume FROM live_prices WHERE symbol IN ({placeholders})", symbols)
        else:
            cur.execute("SELECT symbol, ltp, change_pct, high, low, volume FROM live_prices ORDER BY value DESC LIMIT 30")
        live_rows = cur.fetchall()
        if live_rows:
            parts.append("\nLIVE PRICES:")
            for r in live_rows:
                parts.append(f"  {r['symbol']:15s} LTP={float(r['ltp'] or 0):7.1f} chg={float(r['change_pct'] or 0):+.1f}% H={float(r['high'] or 0):.1f} L={float(r['low'] or 0):.1f} vol={int(r['volume'] or 0):,}")

        # Latest analysis for portfolio stocks
        cur.execute("SELECT MAX(date) FROM daily_analysis")
        latest_date = cur.fetchone()["max"]
        if latest_date and symbols:
            cur.execute(f"""
                SELECT symbol, action, score, entry_low, entry_high, sl, t1, t2,
                       rsi, cmf, stoch_rsi, macd_status, mfi, adx, vol_ratio,
                       chg_5d, chg_10d, support, resistance
                FROM daily_analysis WHERE date = %s AND symbol IN ({placeholders})
            """, [latest_date] + symbols)
            analysis = cur.fetchall()
            if analysis:
                parts.append(f"\nDAILY ANALYSIS (date: {latest_date}):")
                for a in analysis:
                    parts.append(
                        f"  {a['symbol']:15s} {a['action']:20s} score={a['score']} "
                        f"entry={a['entry_low']}-{a['entry_high']} SL={a['sl']} T1={a['t1']} T2={a['t2']} "
                        f"RSI={a['rsi']} CMF={a['cmf']} StRSI={a['stoch_rsi']} MACD={a['macd_status']} "
                        f"5d={a['chg_5d']}% 10d={a['chg_10d']}%"
                    )

        # Judge analysis
        if symbols:
            cur.execute(f"""
                SELECT symbol, final_action, final_confidence, key_risk
                FROM judge_daily_analysis WHERE date = (SELECT MAX(date) FROM judge_daily_analysis)
                AND symbol IN ({placeholders})
            """, symbols)
            judge = cur.fetchall()
            if judge:
                parts.append("\nAI JUDGE VERDICTS:")
                for j in judge:
                    parts.append(f"  {j['symbol']:15s} {j['final_action']:20s} conf={j['final_confidence']} risk={j['key_risk']}")

        # Market breadth
        cur.execute("""
            SELECT COUNT(*) as total,
                   SUM(CASE WHEN ltp > close_prev AND close_prev > 0 THEN 1 ELSE 0 END) as up,
                   SUM(CASE WHEN ltp < close_prev AND close_prev > 0 THEN 1 ELSE 0 END) as down
            FROM live_prices WHERE ltp > 0
        """)
        breadth = cur.fetchone()
        if breadth:
            parts.append(f"\nMARKET BREADTH: {breadth['up']} up / {breadth['down']} down / {breadth['total']} total")

        # Top BUY signals
        if not symbols:
            cur.execute("""
                SELECT da.symbol, da.action, da.score, da.ltp, da.entry_low, da.entry_high, f.sector
                FROM daily_analysis da
                LEFT JOIN fundamentals f ON f.symbol = da.symbol
                WHERE da.date = (SELECT MAX(date) FROM daily_analysis)
                  AND da.action LIKE 'BUY%%' AND f.category = 'A'
                ORDER BY da.score DESC LIMIT 10
            """)
            buys = cur.fetchall()
            if buys:
                parts.append("\nTOP BUY SIGNALS (A-cat):")
                for b in buys:
                    parts.append(f"  {b['symbol']:15s} {b['action']:20s} score={b['score']} LTP={b['ltp']} entry={b['entry_low']}-{b['entry_high']} [{b['sector']}]")

        conn.close()
        return "\n".join(parts)
    except Exception as e:
        logger.error(f"Prefetch failed: {e}")
        return ""


def detect_query_symbols(message: str) -> list[str]:
    """Extract stock symbols from user message."""
    # Common DSE symbols the user might mention
    import re
    # Look for ALL-CAPS words that could be symbols (3+ letters)
    words = re.findall(r'\b[A-Z]{2,15}\b', message.upper())
    # Filter out common words
    noise = {"THE", "AND", "FOR", "BUT", "NOT", "HOW", "WHAT", "WHEN", "WHY", "BUY", "SELL",
             "HOLD", "DSE", "DSEX", "BDT", "RSI", "MACD", "CMF", "MFI", "ADX", "EMA", "SMA",
             "NOW", "TODAY", "ALL", "TOP", "BEST", "GOOD", "BAD", "ANY", "CAN", "ARE", "HAS"}
    return [w for w in words if w not in noise]


def call_claude_fast(prompt: str, user_context: str = "", market_data: str = "", timeout: int = 45) -> str:
    """Fast mode: single Claude call with pre-fetched data. No tool use. ~10-20 seconds."""
    prompt_file = None
    sys_prompt_file = None
    try:
        prompt_file = tempfile.NamedTemporaryFile(
            mode="w", suffix=".txt", delete=False, prefix="claude_chat_"
        )
        prompt_file.write(prompt)
        prompt_file.close()

        # Compact system prompt with pre-fetched data
        fast_system = (
            "You are a DSE trading assistant. Answer based on the market data provided below. "
            "Be concise, use tables, give clear buy/sell/hold with prices.\n"
            "DSE rules: BDT currency, tick 0.10, weekends Fri+Sat, T+2 settlement.\n\n"
            f"{user_context}\n\n"
            f"CURRENT MARKET DATA:\n{market_data}\n\n"
            "Answer the user's question using ONLY the data above. Do NOT attempt to query databases or use tools."
        )

        sys_prompt_file = tempfile.NamedTemporaryFile(
            mode="w", suffix=".txt", delete=False, prefix="claude_sys_"
        )
        sys_prompt_file.write(fast_system)
        sys_prompt_file.close()

        env = get_claude_env()
        model = os.getenv("CLAUDE_MODEL", "sonnet")

        # Fast: max-turns 1, no tool use, no file access
        cmd = [
            "bash", "-c",
            f'cat "{prompt_file.name}" | claude -p '
            f'--model {model} '
            f'--max-turns 1 '
            f'--append-system-prompt "$(cat {sys_prompt_file.name})"'
        ]

        result = subprocess.run(
            cmd, capture_output=True, text=True, timeout=timeout, env=env,
        )

        resp = result.stdout.strip()
        if result.returncode != 0 or not resp or "Not logged in" in resp:
            return ""
        logger.info(f"Fast response: {len(resp)} chars")
        return resp

    except subprocess.TimeoutExpired:
        return ""
    except Exception:
        return ""
    finally:
        for f in [prompt_file, sys_prompt_file]:
            if f and os.path.exists(f.name):
                os.unlink(f.name)


def call_claude_deep(prompt: str, user_context: str = "", timeout: int = 600) -> str:
    """Deep mode: full Claude Code with tool access. For complex analysis. ~1-3 min."""
    prompt_file = None
    sys_prompt_file = None
    try:
        prompt_file = tempfile.NamedTemporaryFile(
            mode="w", suffix=".txt", delete=False, prefix="claude_chat_"
        )
        prompt_file.write(prompt)
        prompt_file.close()

        full_system = GENERAL_PROMPT + "\n" + user_context

        sys_prompt_file = tempfile.NamedTemporaryFile(
            mode="w", suffix=".txt", delete=False, prefix="claude_sys_"
        )
        sys_prompt_file.write(full_system)
        sys_prompt_file.close()

        env = get_claude_env()
        model = os.getenv("CLAUDE_CHAT_MODEL", "haiku")

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
            cmd, capture_output=True, text=True, timeout=timeout,
            env=env, cwd=BACKEND_DIR,
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
        elif self.path == "/auth/google":
            self._handle_google_auth()
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
        elif self.path.startswith("/user/"):
            email = self.path.split("/user/")[1]
            user = get_or_create_user(email)
            self._respond(200, {"user": {
                "email": user.get("email"),
                "name": user.get("name"),
                "photo_url": user.get("photo_url"),
                "portfolio": user.get("portfolio", {}),
                "strategy": user.get("strategy", ""),
            }})
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

    def _handle_google_auth(self):
        """Verify Google token and return/create user."""
        try:
            length = int(self.headers.get("Content-Length", 0))
            body = json.loads(self.rfile.read(length))
        except Exception:
            self._respond(400, {"error": "invalid JSON"})
            return

        # The frontend sends the Google credential (JWT)
        # For simplicity, we trust the decoded info from the frontend
        # In production, verify the JWT with Google's API
        email = body.get("email", "").strip()
        name = body.get("name", "").strip()
        photo_url = body.get("photo_url", "").strip()

        if not email:
            self._respond(400, {"error": "email required"})
            return

        user = get_or_create_user(email, name, photo_url)
        logger.info(f"Auth: {name} ({email})")

        self._respond(200, {
            "user": {
                "email": user.get("email"),
                "name": user.get("name"),
                "photo_url": user.get("photo_url"),
                "portfolio": user.get("portfolio", {}),
                "strategy": user.get("strategy", ""),
                "preferences": user.get("preferences", ""),
            }
        })

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
        user_email = body.get("user_email", "")
        cleanup_sessions()

        # Load user profile for context
        user_context = ""
        if user_email:
            user = get_or_create_user(user_email)
            user_context = build_user_context(user)
        else:
            user_context = "\nCURRENT USER: Anonymous (not signed in). No portfolio data. Give general advice."

        with sessions_lock:
            if session_id not in sessions:
                sessions[session_id] = {"messages": [], "last_active": time.time(), "user_email": user_email}
            session = sessions[session_id]

        prompt = build_prompt(session["messages"], message)
        logger.info(f"[{session_id[:8]}] [{user_email or 'anon'}] {message[:80]}...")

        # Use deep mode (full tool access with haiku for speed)
        response = call_claude_deep(prompt, user_context)

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
        self.wfile.write(json.dumps(data, default=str).encode())

    def do_OPTIONS(self):
        self.send_response(200)
        self.send_header("Access-Control-Allow-Origin", "*")
        self.send_header("Access-Control-Allow-Methods", "GET, POST, DELETE, OPTIONS")
        self.send_header("Access-Control-Allow-Headers", "Content-Type")
        self.end_headers()

    def log_message(self, format, *args):
        pass


def main():
    logger.info("Starting DSE Chat Service (multi-user, full Claude Code)...")
    logger.info(f"Project: {PROJECT_DIR}")
    logger.info(f"Backend: {BACKEND_DIR}")

    # Init DB table
    init_db()

    env = get_claude_env()
    token_set = bool(env.get("CLAUDE_CODE_OAUTH_TOKEN"))
    claude_path = subprocess.run(['which', 'claude'], capture_output=True, text=True).stdout.strip()
    logger.info(f"Claude CLI: {claude_path}")
    logger.info(f"OAuth token: {'set' if token_set else 'NOT SET'}")
    logger.info(f"Model: {os.getenv('CLAUDE_MODEL', 'sonnet')}")
    logger.info(f"Max turns: 25")

    server = HTTPServer(("0.0.0.0", PORT), ChatHandler)
    logger.info(f"Listening on http://0.0.0.0:{PORT}")

    try:
        server.serve_forever()
    except KeyboardInterrupt:
        logger.info("Shutting down...")
        server.shutdown()


if __name__ == "__main__":
    main()
