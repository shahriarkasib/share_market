#!/usr/bin/env python3
"""LLM Daily Analyzer — Five-stage analysis pipeline for GCP VM.

Stage 1: LLM analyzes ALL A-category stocks (batched, 8/call)
Stage 2: Judge LLM compares algo vs LLM and picks best (batched, 30/call)
Stage 3: Snapshot all predictions into prediction_tracker
Stage 4: Override algo entry/exit with AI-computed values
Stage 5: Email notification summary (requires EMAIL_SENDER, EMAIL_PASSWORD, EMAIL_RECIPIENT)

Usage:
    python3 scripts/llm_daily_analyzer.py

Cron (after algo analysis at 15:00 BST):
    5 9 * * 0-4 cd /path/to/backend && python3 scripts/llm_daily_analyzer.py
"""

import json
import logging
import os
import re
import smtplib
import subprocess
import sys
import time
from datetime import datetime
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText

import psycopg2
import psycopg2.extras
import pytz

try:
    import anthropic
    HAS_ANTHROPIC_SDK = True
except ImportError:
    HAS_ANTHROPIC_SDK = False

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%H:%M:%S",
)
logger = logging.getLogger(__name__)

DSE_TZ = pytz.timezone("Asia/Dhaka")

DATABASE_URL = os.getenv(
    "DATABASE_URL",
    "postgresql://postgres.iihlezpkpllacztoaguc:160021062Ss%23%23"
    "@aws-1-ap-southeast-1.pooler.supabase.com:6543/postgres",
)
DATABASE_URL_DIRECT = os.getenv(
    "DATABASE_URL_DIRECT",
    "postgresql://postgres.iihlezpkpllacztoaguc:160021062Ss%23%23"
    "@aws-1-ap-southeast-1.pooler.supabase.com:5432/postgres",
)

# Valid actions (algo uses these; LLM output normalized to match)
VALID_ACTIONS = {
    "BUY (strong)", "BUY", "BUY on pullback", "BUY on dip",
    "BUY (wait for MACD cross)", "HOLD/WAIT", "SELL/AVOID", "AVOID",
}
ACTION_NORMALIZE = {
    "STRONG_BUY": "BUY (strong)",
    "STRONG BUY": "BUY (strong)",
    "BUY ON PULLBACK": "BUY on pullback",
    "BUY ON DIP": "BUY on dip",
    "WAIT FOR MACD CROSS": "BUY (wait for MACD cross)",
    "BUY WAIT FOR MACD CROSS": "BUY (wait for MACD cross)",
    "HOLD": "HOLD/WAIT",
    "WAIT": "HOLD/WAIT",
    "SELL": "SELL/AVOID",
    "SELL/AVOID": "SELL/AVOID",
}

LLM_BATCH_SIZE = 4
JUDGE_BATCH_SIZE = 30
CLAUDE_TIMEOUT = 900
CLAUDE_MODEL = os.getenv("CLAUDE_MODEL", "claude-opus-4-5-20250514")
ANTHROPIC_API_KEY = os.getenv("ANTHROPIC_API_KEY", "")


# ─── Database helpers ───


def get_conn():
    conn = psycopg2.connect(DATABASE_URL, cursor_factory=psycopg2.extras.RealDictCursor)
    conn.autocommit = False
    return conn


def ensure_tables():
    """Create tables if they don't exist (DDL needs direct connection)."""
    conn = psycopg2.connect(DATABASE_URL_DIRECT)
    conn.autocommit = True
    cur = conn.cursor()
    for stmt in [
        """CREATE TABLE IF NOT EXISTS llm_daily_analysis (
            id SERIAL PRIMARY KEY, date DATE NOT NULL, symbol TEXT NOT NULL,
            action TEXT NOT NULL, confidence TEXT, reasoning TEXT, wait_for TEXT,
            wait_days TEXT, entry_low DOUBLE PRECISION, entry_high DOUBLE PRECISION,
            sl DOUBLE PRECISION, t1 DOUBLE PRECISION, t2 DOUBLE PRECISION,
            risk_factors TEXT, catalysts TEXT, score DOUBLE PRECISION,
            how_to_buy TEXT, volume_rule TEXT, next_day_plan TEXT, sell_plan TEXT,
            batch_id INTEGER, raw_response TEXT,
            created_at TIMESTAMP DEFAULT NOW(), UNIQUE(date, symbol)
        )""",
        # Add new columns if table already exists
        "ALTER TABLE llm_daily_analysis ADD COLUMN IF NOT EXISTS how_to_buy TEXT",
        "ALTER TABLE llm_daily_analysis ADD COLUMN IF NOT EXISTS volume_rule TEXT",
        "ALTER TABLE llm_daily_analysis ADD COLUMN IF NOT EXISTS next_day_plan TEXT",
        "ALTER TABLE llm_daily_analysis ADD COLUMN IF NOT EXISTS sell_plan TEXT",
        "ALTER TABLE llm_daily_analysis ADD COLUMN IF NOT EXISTS stage TEXT",
        "ALTER TABLE llm_daily_analysis ADD COLUMN IF NOT EXISTS stage_reasoning TEXT",
        "ALTER TABLE llm_daily_analysis ADD COLUMN IF NOT EXISTS expected_return_1w DOUBLE PRECISION",
        "ALTER TABLE llm_daily_analysis ADD COLUMN IF NOT EXISTS expected_return_2w DOUBLE PRECISION",
        "ALTER TABLE llm_daily_analysis ADD COLUMN IF NOT EXISTS expected_return_1m DOUBLE PRECISION",
        "ALTER TABLE llm_daily_analysis ADD COLUMN IF NOT EXISTS downside_risk DOUBLE PRECISION",
        "ALTER TABLE llm_daily_analysis ADD COLUMN IF NOT EXISTS dsex_dependency TEXT",
        "ALTER TABLE llm_daily_analysis ADD COLUMN IF NOT EXISTS if_dsex_drops TEXT",
        "ALTER TABLE llm_daily_analysis ADD COLUMN IF NOT EXISTS if_dsex_rises TEXT",
        "ALTER TABLE llm_daily_analysis ADD COLUMN IF NOT EXISTS dsex_outlook TEXT",
        "CREATE INDEX IF NOT EXISTS idx_llm_daily_date ON llm_daily_analysis(date)",
        """CREATE TABLE IF NOT EXISTS judge_daily_analysis (
            id SERIAL PRIMARY KEY, date DATE NOT NULL, symbol TEXT NOT NULL,
            algo_action TEXT NOT NULL, llm_action TEXT NOT NULL,
            final_action TEXT NOT NULL, final_confidence TEXT,
            agreement BOOLEAN, reasoning TEXT, algo_strengths TEXT,
            llm_strengths TEXT, key_risk TEXT, wait_days TEXT,
            entry_low DOUBLE PRECISION, entry_high DOUBLE PRECISION,
            sl DOUBLE PRECISION, t1 DOUBLE PRECISION, t2 DOUBLE PRECISION,
            score DOUBLE PRECISION, batch_id INTEGER, raw_response TEXT,
            created_at TIMESTAMP DEFAULT NOW(), UNIQUE(date, symbol)
        )""",
        "CREATE INDEX IF NOT EXISTS idx_judge_daily_date ON judge_daily_analysis(date)",
        # DSEX forecast table
        """CREATE TABLE IF NOT EXISTS dsex_forecast (
            id SERIAL PRIMARY KEY, date DATE NOT NULL UNIQUE,
            forecast TEXT, sentiment TEXT, support DOUBLE PRECISION,
            resistance DOUBLE PRECISION, expected_direction TEXT,
            confidence TEXT, key_factors TEXT,
            scenario_bull TEXT, scenario_bear TEXT, scenario_base TEXT,
            raw_response TEXT,
            created_at TIMESTAMP DEFAULT NOW()
        )""",
        # Pre-computed radar cache
        """CREATE TABLE IF NOT EXISTS radar_precomputed (
            id SERIAL PRIMARY KEY, date DATE NOT NULL,
            category TEXT NOT NULL DEFAULT 'A',
            data_json TEXT NOT NULL,
            created_at TIMESTAMP DEFAULT NOW(),
            UNIQUE(date, category)
        )""",
        """CREATE TABLE IF NOT EXISTS prediction_tracker (
            id SERIAL PRIMARY KEY, date DATE NOT NULL, symbol TEXT NOT NULL,
            source TEXT NOT NULL, action TEXT NOT NULL, score DOUBLE PRECISION,
            wait_days TEXT, wait_days_min INTEGER, wait_days_max INTEGER,
            ltp_at_prediction DOUBLE PRECISION, entry_low DOUBLE PRECISION,
            entry_high DOUBLE PRECISION, sl DOUBLE PRECISION,
            t1 DOUBLE PRECISION, t2 DOUBLE PRECISION,
            transitioned_to TEXT, transition_date DATE, transition_days INTEGER,
            transition_within_window BOOLEAN,
            t1_hit_date DATE, t1_hit_days INTEGER,
            t2_hit_date DATE, t2_hit_days INTEGER,
            sl_hit_date DATE, sl_hit_days INTEGER,
            max_gain_pct DOUBLE PRECISION, max_loss_pct DOUBLE PRECISION,
            final_return_pct DOUBLE PRECISION,
            outcome TEXT DEFAULT 'PENDING', outcome_reason TEXT,
            verified_at TIMESTAMP, created_at TIMESTAMP DEFAULT NOW(),
            UNIQUE(date, symbol, source)
        )""",
        "CREATE INDEX IF NOT EXISTS idx_pred_tracker_date ON prediction_tracker(date)",
        """CREATE TABLE IF NOT EXISTS accuracy_summary (
            id SERIAL PRIMARY KEY, date DATE NOT NULL, source TEXT NOT NULL,
            period TEXT NOT NULL, total_predictions INTEGER, correct INTEGER,
            wrong INTEGER, pending INTEGER, accuracy_pct DOUBLE PRECISION,
            avg_return_pct DOUBLE PRECISION, buy_accuracy_pct DOUBLE PRECISION,
            hold_transition_accuracy_pct DOUBLE PRECISION,
            t1_hit_rate DOUBLE PRECISION, sl_hit_rate DOUBLE PRECISION,
            created_at TIMESTAMP DEFAULT NOW(), UNIQUE(date, source, period)
        )""",
        "CREATE INDEX IF NOT EXISTS idx_accuracy_summary_date ON accuracy_summary(date)",
    ]:
        try:
            cur.execute(stmt)
        except Exception as e:
            logger.warning(f"DDL: {e}")
    cur.close()
    conn.close()
    logger.info("Tables verified")


# ─── Data loading ───


def load_algo_analysis(date_str: str) -> list[dict]:
    """Load all A-category stocks from daily_analysis for a given date."""
    conn = get_conn()
    cur = conn.cursor()
    cur.execute("""
        SELECT da.symbol, da.action, da.score, da.reasoning,
               da.entry_low, da.entry_high, da.sl, da.t1, da.t2,
               da.risk_pct, da.reward_pct, da.rsi, da.stoch_rsi,
               da.macd_line, da.macd_signal, da.macd_hist, da.macd_status,
               da.bb_pct, da.atr, da.atr_pct, da.volatility, da.vol_ratio,
               da.avg_vol, da.trend_50d, da.support, da.resistance,
               da.ltp, da.wait_days, da.category,
               f.sector, f.company_name
        FROM daily_analysis da
        JOIN fundamentals f ON da.symbol = f.symbol
        WHERE da.date = %s AND f.category = 'A'
        ORDER BY da.score DESC
    """, (date_str,))
    rows = cur.fetchall()
    conn.close()
    return [dict(r) for r in rows]


def load_market_context() -> dict:
    """Load current market summary for prompt context."""
    conn = get_conn()
    cur = conn.cursor()
    cur.execute("SELECT * FROM market_summary WHERE id = 1")
    market = cur.fetchone()
    conn.close()
    return dict(market) if market else {}


def load_accuracy_feedback() -> str:
    """Load past accuracy + specific mistakes for self-improvement."""
    conn = get_conn()
    cur = conn.cursor()

    # 1. Summary stats
    cur.execute("""
        SELECT source, accuracy_pct, avg_return_pct, buy_accuracy_pct,
               hold_transition_accuracy_pct, t1_hit_rate, sl_hit_rate
        FROM accuracy_summary
        WHERE period = '30d'
        ORDER BY date DESC, source
        LIMIT 3
    """)
    rows = cur.fetchall()

    # 2. Recent wrong predictions (learn from mistakes)
    cur.execute("""
        SELECT symbol, source, action, outcome_reason, final_return_pct, max_loss_pct
        FROM prediction_tracker
        WHERE outcome = 'WRONG' AND source = 'llm'
        ORDER BY date DESC
        LIMIT 10
    """)
    mistakes = cur.fetchall()

    # 3. Best correct calls (reinforce what works)
    cur.execute("""
        SELECT symbol, source, action, outcome_reason, final_return_pct
        FROM prediction_tracker
        WHERE outcome = 'CORRECT' AND source = 'llm' AND final_return_pct IS NOT NULL
        ORDER BY final_return_pct DESC
        LIMIT 5
    """)
    wins = cur.fetchall()

    conn.close()

    lines = []
    if rows:
        lines.append("## LEARN FROM PAST PERFORMANCE (critical for improvement)")
        lines.append("### 30-day Accuracy Summary")
        for r in rows:
            lines.append(
                f"- {r['source']}: accuracy {r['accuracy_pct'] or 0:.1f}%, "
                f"avg return {r['avg_return_pct'] or 0:+.2f}%, "
                f"BUY accuracy {r['buy_accuracy_pct'] or 0:.1f}%, "
                f"T1 hit rate {r['t1_hit_rate'] or 0:.1f}%, "
                f"SL hit rate {r['sl_hit_rate'] or 0:.1f}%"
            )

    if mistakes:
        lines.append("\n### YOUR RECENT MISTAKES (avoid repeating these)")
        for m in mistakes:
            ret = f"{m['final_return_pct']:+.1f}%" if m['final_return_pct'] else "N/A"
            loss = f"{m['max_loss_pct']:.1f}%" if m['max_loss_pct'] else "N/A"
            lines.append(
                f"- {m['symbol']}: you said {m['action']} but was WRONG. "
                f"Return: {ret}, max loss: {loss}. "
                f"Reason: {m['outcome_reason'] or 'unknown'}"
            )
        lines.append("Reflect on these mistakes. What pattern do you see? Adjust your analysis accordingly.")

    if wins:
        lines.append("\n### YOUR BEST CALLS (reinforce this pattern)")
        for w in wins:
            ret = f"{w['final_return_pct']:+.1f}%" if w['final_return_pct'] else "N/A"
            lines.append(f"- {w['symbol']}: {w['action']} was CORRECT, return {ret}. {w['outcome_reason'] or ''}")

    if not lines:
        return ""
    return "\n".join(lines) + "\n"


def load_ohlcv_history(symbols: list[str], daily_days: int = 130, weekly_weeks: int = 52) -> dict[str, str]:
    """Load OHLCV history: 6-month daily + 1-year weekly candles per symbol."""
    conn = get_conn()
    cur = conn.cursor()
    placeholders = ",".join(["%s"] * len(symbols))
    cur.execute(f"""
        SELECT symbol, date, open, high, low, close, volume
        FROM daily_prices
        WHERE symbol IN ({placeholders})
        ORDER BY symbol, date DESC
    """, symbols)
    rows = cur.fetchall()
    conn.close()

    from collections import defaultdict
    by_sym: dict[str, list] = defaultdict(list)
    for r in rows:
        by_sym[r["symbol"]].append(r)

    result = {}
    for sym in symbols:
        sym_rows = by_sym.get(sym, [])
        if not sym_rows:
            result[sym] = ""
            continue

        parts = []

        # ── 1-year weekly candles (aggregate daily into weeks) ──
        all_rows = list(reversed(sym_rows))  # oldest first
        if len(all_rows) > daily_days:
            older_rows = all_rows[:len(all_rows) - daily_days]  # rows older than 6 months
            weeks = []
            week_rows = []
            for r in older_rows:
                d = r["date"]
                iso_week = d.isocalendar()[1] if hasattr(d, "isocalendar") else 0
                iso_year = d.isocalendar()[0] if hasattr(d, "isocalendar") else 0
                key = (iso_year, iso_week)
                if week_rows and (week_rows[0]["_wk"] != key):
                    weeks.append(week_rows)
                    week_rows = []
                r["_wk"] = key
                week_rows.append(r)
            if week_rows:
                weeks.append(week_rows)

            # Take last N weeks
            weeks = weeks[-weekly_weeks:]
            if weeks:
                lines = ["week,O,H,L,C,Vol"]
                for wk in weeks:
                    w_open = float(wk[0]["open"])
                    w_high = max(float(r["high"]) for r in wk)
                    w_low = min(float(r["low"]) for r in wk)
                    w_close = float(wk[-1]["close"])
                    w_vol = sum(int(r["volume"]) for r in wk)
                    lines.append(
                        f"{wk[0]['date']},{w_open:.1f},{w_high:.1f},"
                        f"{w_low:.1f},{w_close:.1f},{w_vol}"
                    )
                parts.append("Weekly (1yr):\n" + "\n".join(lines))

        # ── 6-month daily candles ──
        daily_rows = list(reversed(sym_rows[:daily_days]))  # most recent N days, oldest first
        if daily_rows:
            lines = ["date,O,H,L,C,Vol"]
            for r in daily_rows:
                lines.append(
                    f"{r['date']},{float(r['open']):.1f},{float(r['high']):.1f},"
                    f"{float(r['low']):.1f},{float(r['close']):.1f},{int(r['volume'])}"
                )
            parts.append("Daily (6mo):\n" + "\n".join(lines))

        result[sym] = "\n\n".join(parts)
    return result


def load_dsex_history(days: int = 130) -> str:
    """Load recent DSEX index history (6 months), return compact CSV."""
    conn = get_conn()
    cur = conn.cursor()
    cur.execute("""
        SELECT date, dsex_index, total_volume, total_value
        FROM dsex_history ORDER BY date DESC LIMIT %s
    """, (days,))
    rows = cur.fetchall()
    conn.close()

    rows.reverse()  # Oldest first
    if not rows:
        return ""
    lines = ["date,DSEX,Volume,Turnover"]
    for r in rows:
        lines.append(
            f"{r['date']},{float(r['dsex_index']):.1f},"
            f"{int(r['total_volume'] or 0)},{int(r['total_value'] or 0)}"
        )
    return "\n".join(lines)


# ─── DSEX-Stock Correlation ───


def compute_dsex_correlations(symbols: list[str], days: int = 130) -> dict[str, dict]:
    """Compute beta, correlation, and scenario returns for each stock vs DSEX.

    Returns dict[symbol -> {beta, correlation, avg_return_dsex_down, avg_return_dsex_up,
                             scenario_m3, scenario_m1, scenario_p1, scenario_p3}]
    """
    conn = get_conn()
    cur = conn.cursor()

    # Load DSEX daily returns
    cur.execute("""
        SELECT date, dsex_index FROM dsex_history
        ORDER BY date DESC LIMIT %s
    """, (days + 1,))
    dsex_rows = cur.fetchall()

    # Load stock daily closes
    placeholders = ",".join(["%s"] * len(symbols))
    cur.execute(f"""
        SELECT symbol, date, close FROM daily_prices
        WHERE symbol IN ({placeholders})
        ORDER BY date DESC
    """, symbols)
    price_rows = cur.fetchall()
    conn.close()

    if len(dsex_rows) < 10:
        return {}

    # Build DSEX returns by date
    dsex_rows = list(reversed(dsex_rows))  # oldest first
    dsex_by_date = {}
    dsex_returns = {}
    for i, r in enumerate(dsex_rows):
        d = str(r["date"])
        dsex_by_date[d] = float(r["dsex_index"])
        if i > 0:
            prev_d = str(dsex_rows[i - 1]["date"])
            prev_val = dsex_by_date[prev_d]
            if prev_val > 0:
                dsex_returns[d] = (float(r["dsex_index"]) - prev_val) / prev_val * 100

    # Build stock returns by symbol & date
    from collections import defaultdict
    stock_closes: dict[str, dict[str, float]] = defaultdict(dict)
    for r in price_rows:
        stock_closes[r["symbol"]][str(r["date"])] = float(r["close"])

    results = {}
    sorted_dates = sorted(dsex_returns.keys())

    for sym in symbols:
        closes = stock_closes.get(sym, {})
        if len(closes) < 10:
            continue

        # Align stock returns with DSEX returns
        paired_dsex = []
        paired_stock = []
        stock_sorted = sorted(closes.keys())

        # Build stock returns
        stock_returns = {}
        for i in range(1, len(stock_sorted)):
            d = stock_sorted[i]
            prev_d = stock_sorted[i - 1]
            if closes[prev_d] > 0:
                stock_returns[d] = (closes[d] - closes[prev_d]) / closes[prev_d] * 100

        # Pair up dates where both have returns
        for d in sorted_dates:
            if d in stock_returns:
                paired_dsex.append(dsex_returns[d])
                paired_stock.append(stock_returns[d])

        n = len(paired_dsex)
        if n < 10:
            continue

        # Compute beta and correlation
        mean_d = sum(paired_dsex) / n
        mean_s = sum(paired_stock) / n
        cov = sum((paired_dsex[i] - mean_d) * (paired_stock[i] - mean_s) for i in range(n)) / n
        var_d = sum((x - mean_d) ** 2 for x in paired_dsex) / n
        std_d = var_d ** 0.5
        std_s = (sum((x - mean_s) ** 2 for x in paired_stock) / n) ** 0.5

        beta = cov / var_d if var_d > 0 else 1.0
        corr = cov / (std_d * std_s) if (std_d > 0 and std_s > 0) else 0.0

        # Avg return on DSEX-down days (< -0.3%) and DSEX-up days (> +0.3%)
        down_returns = [paired_stock[i] for i in range(n) if paired_dsex[i] < -0.3]
        up_returns = [paired_stock[i] for i in range(n) if paired_dsex[i] > 0.3]
        avg_down = sum(down_returns) / len(down_returns) if down_returns else 0.0
        avg_up = sum(up_returns) / len(up_returns) if up_returns else 0.0

        # Scenario projections: if DSEX moves X%, stock moves approximately beta * X%
        results[sym] = {
            "beta": round(beta, 2),
            "correlation": round(corr, 2),
            "avg_return_dsex_down": round(avg_down, 2),
            "avg_return_dsex_up": round(avg_up, 2),
            "scenario_m3": round(beta * -3, 1),
            "scenario_m1": round(beta * -1, 1),
            "scenario_p1": round(beta * 1, 1),
            "scenario_p3": round(beta * 3, 1),
        }

    logger.info(f"Computed DSEX correlations for {len(results)}/{len(symbols)} stocks")
    return results


# ─── Claude CLI ───


def call_claude(prompt: str, timeout: int = CLAUDE_TIMEOUT) -> str:
    """Call Claude via Anthropic SDK (preferred) or bash CLI (Max subscription).

    SDK path: uses ANTHROPIC_API_KEY for direct API access.
    CLI path: writes prompt to temp file, calls `claude -p` from bash
              exactly like the data-audit pipeline does — inherits
              CLAUDE_CODE_OAUTH_TOKEN from the shell environment.
    """
    logger.info(f"Calling Claude ({len(prompt)} chars, timeout {timeout}s)...")

    # Prefer Anthropic SDK with API key (works headlessly)
    if HAS_ANTHROPIC_SDK and ANTHROPIC_API_KEY:
        try:
            client = anthropic.Anthropic(api_key=ANTHROPIC_API_KEY)
            message = client.messages.create(
                model=CLAUDE_MODEL,
                max_tokens=8192,
                messages=[{"role": "user", "content": prompt}],
            )
            resp = message.content[0].text.strip()
            logger.info(f"Claude SDK response: {len(resp)} chars (model={CLAUDE_MODEL})")
            return resp
        except anthropic.APIError as e:
            logger.error(f"Anthropic API error: {e}")
            return ""
        except Exception as e:
            logger.error(f"Anthropic SDK error: {e}")
            return ""

    # CLI path: call claude -p from bash (same pattern as data-audit pipeline)
    # Write prompt to temp file to avoid shell escaping issues
    import tempfile
    prompt_file = None
    try:
        prompt_file = tempfile.NamedTemporaryFile(
            mode="w", suffix=".txt", delete=False, prefix="claude_prompt_"
        )
        prompt_file.write(prompt)
        prompt_file.close()

        # Ensure CLAUDE_CODE_OAUTH_TOKEN is available
        # Non-interactive SSH doesn't source .bashrc, so extract token if missing
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
                                logger.info("Extracted CLAUDE_CODE_OAUTH_TOKEN from .bashrc")
                                break
                except Exception as e:
                    logger.warning(f"Failed to extract token from .bashrc: {e}")

        # Bash one-liner: cat prompt file | claude -p --model opus
        bash_cmd = f'cat "{prompt_file.name}" | claude -p --model opus'
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
            return ""
        resp = result.stdout.strip()
        if "Not logged in" in resp or "Please run /login" in resp:
            logger.error("Claude CLI not authenticated. Ensure CLAUDE_CODE_OAUTH_TOKEN is set.")
            return ""
        if not resp:
            logger.warning("Claude CLI returned empty stdout (possible rate limit)")
        else:
            logger.info(f"Claude CLI response: {len(resp)} chars")
        return resp
    except FileNotFoundError:
        logger.error("Claude CLI not found. Install: npm install -g @anthropic-ai/claude-code")
        return ""
    except subprocess.TimeoutExpired:
        logger.error(f"Claude CLI timed out ({timeout}s)")
        return ""
    finally:
        if prompt_file and os.path.exists(prompt_file.name):
            os.unlink(prompt_file.name)


def parse_json_response(raw: str) -> list[dict] | dict | None:
    """Parse Claude's JSON response, stripping markdown fences."""
    if not raw:
        return None
    text = raw.strip()
    # Strip ```json ... ```
    if text.startswith("```"):
        lines = text.split("\n")
        lines = [l for l in lines if not l.strip().startswith("```")]
        text = "\n".join(lines)
    # Try direct parse
    try:
        return json.loads(text)
    except json.JSONDecodeError:
        pass
    # Find JSON array or object
    for start_char, end_char in [("[", "]"), ("{", "}")]:
        start = text.find(start_char)
        end = text.rfind(end_char) + 1
        if start >= 0 and end > start:
            try:
                return json.loads(text[start:end])
            except json.JSONDecodeError:
                continue
    logger.error(f"Failed to parse JSON: {text[:200]}...")
    return None


def normalize_action(action: str) -> str:
    """Normalize LLM action to match algo's valid set."""
    if not action:
        return "HOLD/WAIT"
    action = action.strip()
    if action in VALID_ACTIONS:
        return action
    upper = action.upper()
    if upper in ACTION_NORMALIZE:
        return ACTION_NORMALIZE[upper]
    # Fuzzy match
    if "STRONG" in upper and "BUY" in upper:
        return "BUY (strong)"
    if "PULLBACK" in upper:
        return "BUY on pullback"
    if "DIP" in upper:
        return "BUY on dip"
    if "MACD" in upper:
        return "BUY (wait for MACD cross)"
    if "BUY" in upper:
        return "BUY"
    if "AVOID" in upper:
        return "AVOID"
    if "SELL" in upper:
        return "SELL/AVOID"
    return "HOLD/WAIT"


def parse_wait_days(wait_str: str) -> tuple[int, int]:
    """Parse wait_days string like '5-10 days' → (5, 10)."""
    if not wait_str:
        return (0, 0)
    wait_str = wait_str.upper().strip()
    if "NOW" in wait_str:
        return (0, 1)
    nums = re.findall(r"\d+", wait_str)
    if len(nums) >= 2:
        return (int(nums[0]), int(nums[1]))
    elif len(nums) == 1:
        n = int(nums[0])
        return (n, n)
    return (0, 0)


# ─── Stage 1: LLM Analysis ───


def build_llm_prompt(
    stocks: list[dict], market: dict, feedback: str, batch_num: int, total_batches: int,
    ohlcv_map: dict[str, str] = None, dsex_csv: str = "",
    dsex_corr: dict[str, dict] = None,
) -> str:
    """Build prompt for a batch of stocks — includes 60-day OHLCV history."""
    dsex = market.get("dsex_index", 0)
    dsex_chg = market.get("dsex_change_pct", 0)

    stock_lines = []
    for s in stocks:
        ltp = float(s.get("ltp") or 0)
        rsi = float(s.get("rsi") or 0)
        stoch = float(s.get("stoch_rsi") or 0)
        macd_h = float(s.get("macd_hist") or 0)
        bb = float(s.get("bb_pct") or 0)
        vol_r = float(s.get("vol_ratio") or 0)
        trend = float(s.get("trend_50d") or 0)
        atr_p = float(s.get("atr_pct") or 0)
        mfi = float(s.get("mfi") or 0)
        cmf = float(s.get("cmf") or 0)
        williams = float(s.get("williams_r") or 0)
        adx = float(s.get("adx") or 0)
        plus_di = float(s.get("plus_di") or 0)
        minus_di = float(s.get("minus_di") or 0)
        ema9 = float(s.get("ema9") or 0)
        ema21 = float(s.get("ema21") or 0)
        sma50 = float(s.get("sma50") or 0)
        mom_3d = float(s.get("momentum_3d") or 0)
        mom_5d = float(s.get("momentum_5d") or 0)
        chg_5d = float(s.get("chg_5d") or 0)
        chg_10d = float(s.get("chg_10d") or 0)
        chg_20d = float(s.get("chg_20d") or 0)

        ohlcv_csv = (ohlcv_map or {}).get(s["symbol"], "")
        corr = (dsex_corr or {}).get(s["symbol"], {})
        corr_line = ""
        if corr:
            corr_line = (
                f"DSEX Beta: {corr['beta']:.2f} | Correlation: {corr['correlation']:.2f} | "
                f"When DSEX falls: avg {corr['avg_return_dsex_down']:+.2f}% | "
                f"When DSEX rises: avg {corr['avg_return_dsex_up']:+.2f}%\n"
                f"  DSEX Scenarios → -3%: stock ~{corr['scenario_m3']:+.1f}% | "
                f"-1%: ~{corr['scenario_m1']:+.1f}% | "
                f"+1%: ~{corr['scenario_p1']:+.1f}% | "
                f"+3%: ~{corr['scenario_p3']:+.1f}%\n"
            )

        stock_lines.append(
            f"### {s['symbol']} ({s.get('sector', '?')})\n"
            f"LTP: {ltp:.1f} | Algo: {s['action']} (score {s.get('score', 0):.0f})\n"
            f"RSI: {rsi:.1f} | StochRSI: {stoch:.1f} | MACD: {s.get('macd_status', '')} (hist {macd_h:+.2f})\n"
            f"MFI: {mfi:.1f} | CMF: {cmf:+.3f} | Williams%R: {williams:.1f}\n"
            f"ADX: {adx:.1f} | +DI: {plus_di:.1f} | -DI: {minus_di:.1f}\n"
            f"BB%: {bb:.1f}% | VolRatio: {vol_r:.1f}x | Trend50d: {trend:+.1f}% | ATR%: {atr_p:.1f}%\n"
            f"EMA9: {ema9:.1f} | EMA21: {ema21:.1f} | SMA50: {sma50:.1f}\n"
            f"Momentum: 3d {mom_3d:+.1f}% | 5d {mom_5d:+.1f}% | Chg: 5d {chg_5d:+.1f}% 10d {chg_10d:+.1f}% 20d {chg_20d:+.1f}%\n"
            f"Support: {s.get('support', 0):.1f} | Resistance: {s.get('resistance', 0):.1f}\n"
            + corr_line
            + (f"\nPrice History:\n```\n{ohlcv_csv}\n```\n" if ohlcv_csv else "")
        )

    dsex_block = ""
    if dsex_csv:
        dsex_block = f"""
## DSEX Index — 6-month History
```
{dsex_csv}
```

## DSEX FORECAST TASK
Study the DSEX history above carefully:
1. What is the DSEX trend (last 5 days, 20 days, 60 days)?
2. Where is DSEX relative to its support/resistance levels?
3. Is volume increasing or decreasing?
4. What will DSEX likely do tomorrow and in the next 3-5 days? Why?
5. How will this DSEX movement affect EACH stock? (Use the beta/correlation data per stock)
6. If DSEX drops 1-2%, which stocks are most/least affected?

Include your DSEX forecast in each stock's reasoning.
"""

    return f"""You are a BUY RADAR analyst for DSE (Dhaka Stock Exchange). Your ONLY goal: find the BEST TIME and BEST PRICE to buy stocks for MAXIMUM PROFIT over the next 1-4 weeks.

For each stock you have:
- **1-year weekly candles**: 52-week range, major support/resistance, long-term trend
- **6-month daily candles**: Recent price action, exact bounces, breakouts, volume patterns
- **16 technical indicators**: Momentum, money flow, trend, positioning
- **DSEX index history**: Market context

Your audience is BEGINNERS who want to make money.

## INDICATOR REFERENCE
- **RSI/StochRSI**: Oversold (<30/<20) = sellers exhausted, bounce coming. Overbought (>70/>80) = stretched.
- **MFI**: Volume-weighted RSI. <20 = real selling exhaustion (stronger than RSI alone). >80 = overbought.
- **CMF**: Money flow direction. Positive = money flowing IN (accumulation). Negative = money flowing OUT (distribution). This tells you if smart money is buying.
- **MACD**: Momentum direction. Bullish cross = momentum turning UP. Histogram growing = accelerating.
- **ADX/DI**: Trend strength. >25 = strong trend. +DI > -DI = uptrend. ADX < 15 = no trend (avoid).
- **Williams %R**: -80 to -100 = oversold. 0 to -20 = overbought.
- **BB%**: Where price sits in its 20-day range. <15% = at bottom (cheap). >85% = at top (expensive).
- **EMA9/21, SMA50**: Short/medium/long moving averages. Price above all = strong uptrend.
- **Volume Ratio**: >2x average = strong interest. <0.5x = dead stock, avoid.
- **DSEX Beta**: How much this stock moves per 1% DSEX move. Beta 1.5 = stock moves 1.5x DSEX. Beta 0.5 = stock is defensive. Beta > 1.5 = aggressive, riskier on market drops.
- **DSEX Correlation**: How closely the stock tracks the index. >0.7 = closely tied to index. <0.3 = independent mover.
- **T+2**: After buying, you CANNOT sell for 2 trading days. Factor this into every recommendation.
- **DSE tick size**: 0.10 BDT. All prices in multiples of 0.10.

## BUY RADAR STAGES — Assign one per stock
Think of these as: "How close is this stock to giving me a profitable buy entry?"

- **ENTRY_ZONE**: BUY TODAY. The best price is here NOW. Indicators aligned, money flowing in, price at support. If you wait, you'll pay more.
- **READY**: BUY in 1-2 days. The upward move is coming. You just need ONE trigger — a small dip to support, a volume surge, or a MACD cross. Have your order ready.
- **APPROACHING**: Setting up. 5-10 days out. The pieces are falling into place but it's not time yet. Watch it daily.
- **BUILDING**: Early accumulation. 2-3 weeks. Smart money may be quietly entering. Too early to act.
- **WATCHING**: Not a buy right now. Unclear or problematic.
- **TOO_LATE**: Already moved. Buying now = chasing. Wait for the next pullback.

## THE KEY QUESTION FOR EVERY STOCK
"If I buy at [entry price] today/this week, what's my realistic profit in 1 week? 2 weeks? 1 month? And what's my downside risk?"

## Market Context
- DSEX: {dsex:.1f} ({dsex_chg:+.2f}%)
- Advances: {market.get('advances', 0)} | Declines: {market.get('declines', 0)}
- Volume: {market.get('total_volume', 0):,} | Turnover: {market.get('total_value', 0):,.0f}
{dsex_block}
{feedback}
## Stocks to Analyze (batch {batch_num}/{total_batches})

{chr(10).join(stock_lines)}

## Your Task
For EACH stock, study the full price history and answer:

1. **Where is the money?** Look at CMF, OBV, volume patterns. Is smart money accumulating or distributing?
2. **Where is the price in its range?** Use weekly candles for 52-week context, daily for recent range. Near the bottom = opportunity. Near the top = risk.
3. **What's the setup?** Indicators turning from oversold? MACD about to cross? Or already overbought and extended?
4. **What's the profit potential?** If I buy at [entry], what's realistic T1 (1-2 week target) and T2 (3-4 week target)?
5. **What's the risk?** Where does the thesis break? What price = I was wrong?

Set entry_low/entry_high from ACTUAL support levels in the chart — repeated lows, bounce zones, consolidation areas.

Return a JSON array (NO markdown fences, ONLY valid JSON):
[
  {{
    "symbol": "SYMBOL",
    "action": "BUY|BUY on dip|BUY on pullback|BUY (wait for MACD cross)|HOLD/WAIT|SELL/AVOID|AVOID",
    "confidence": "HIGH|MEDIUM|LOW",
    "stage": "ENTRY_ZONE|READY|APPROACHING|BUILDING|WATCHING|TOO_LATE",
    "stage_reasoning": "WHY this stage. Reference specific prices and dates from the chart. E.g., 'Bounced from 22.0 three times (Jan 15, Feb 8, Mar 3), now at 22.3 with MFI 18 and CMF turning positive — buy zone is here.' Or: 'Rallied 31→33.5 in 5 days, CMF still positive but RSI 58 and approaching resistance at 34 — one more push possible but entry is late.'",
    "reasoning": "3-5 sentence analysis for beginners. Reference specific prices, dates, and explain what each indicator means.",
    "expected_return_1w": 0.0,
    "expected_return_2w": 0.0,
    "expected_return_1m": 0.0,
    "downside_risk": 0.0,
    "wait_for": "Specific trigger. E.g., 'Price dips to 21.5 with volume > 50K' or 'MACD crosses bullish'",
    "wait_days": "e.g., 'NOW', '1-2 days', '3-5 days', '1-2 weeks', '2-4 weeks'",
    "entry_low": 0.0,
    "entry_high": 0.0,
    "sl": 0.0,
    "t1": 0.0,
    "t2": 0.0,
    "how_to_buy": "Step-by-step: when to place order, what price, what volume to look for, what to AVOID.",
    "volume_rule": "Min volume needed. E.g., 'Only buy if volume > 100K.'",
    "next_day_plan": "3 scenarios: opens green / flat / red — what to do in each.",
    "sell_plan": "When to take profit: sell half at T1, trail stop, sell rest at T2.",
    "risk_factors": ["What could go wrong — in plain language"],
    "catalysts": ["What could push it up — in plain language"],
    "dsex_dependency": "HIGH|MEDIUM|LOW",
    "if_dsex_drops": "What happens to this stock if DSEX drops 1-2% tomorrow. E.g., 'Entry improves to 21.0-21.5, set limit order at 21.2'",
    "if_dsex_rises": "What happens if DSEX rallies 1-2%. E.g., 'Stock will gap up, entry zone missed, wait for pullback'",
    "dsex_outlook": "Your 3-5 day DSEX forecast and how it affects this specific stock's buy timing",
    "score": 50
  }}
]

Rules:
1. Analyze ALL {len(stocks)} stocks
2. entry_low/entry_high from actual support in the chart data
3. Score 0-100: buying conviction (0=avoid, 100=strongest buy)
4. expected_return fields: realistic % gain from entry_high price. Be conservative, not optimistic
5. downside_risk: % loss if stop loss is hit (negative number)
6. T+2: buyer cannot sell for 2 days — if the stock peaks tomorrow, the buyer is TRAPPED
7. DSE tick size 0.10 BDT
8. This is a retail-dominated, low-liquidity market. Volume confirmation is everything
9. Be honest. If a stock is not a good buy, say so. Don't force BUY recommendations

CRITICAL OUTPUT FORMAT: Your response must start with [ and end with ]. Return ONLY a valid JSON array. No markdown, no commentary, no explanation — ONLY the JSON array. If you include ANY text outside the JSON array, the system will crash."""


def store_llm_results(date_str: str, results: list[dict], batch_id: int, raw: str):
    """Store LLM analysis results in llm_daily_analysis."""
    conn = get_conn()
    cur = conn.cursor()
    saved = 0
    for r in results:
        symbol = r.get("symbol", "")
        if not symbol:
            continue
        action = normalize_action(r.get("action", ""))
        try:
            cur.execute("""
                INSERT INTO llm_daily_analysis
                    (date, symbol, action, confidence, reasoning, wait_for, wait_days,
                     entry_low, entry_high, sl, t1, t2,
                     risk_factors, catalysts, score,
                     how_to_buy, volume_rule, next_day_plan, sell_plan,
                     stage, stage_reasoning,
                     expected_return_1w, expected_return_2w, expected_return_1m, downside_risk,
                     dsex_dependency, if_dsex_drops, if_dsex_rises, dsex_outlook,
                     batch_id, raw_response)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                ON CONFLICT (date, symbol) DO UPDATE SET
                    action = EXCLUDED.action, confidence = EXCLUDED.confidence,
                    reasoning = EXCLUDED.reasoning, wait_for = EXCLUDED.wait_for,
                    wait_days = EXCLUDED.wait_days, entry_low = EXCLUDED.entry_low,
                    entry_high = EXCLUDED.entry_high, sl = EXCLUDED.sl,
                    t1 = EXCLUDED.t1, t2 = EXCLUDED.t2,
                    risk_factors = EXCLUDED.risk_factors, catalysts = EXCLUDED.catalysts,
                    score = EXCLUDED.score,
                    how_to_buy = EXCLUDED.how_to_buy, volume_rule = EXCLUDED.volume_rule,
                    next_day_plan = EXCLUDED.next_day_plan, sell_plan = EXCLUDED.sell_plan,
                    stage = EXCLUDED.stage, stage_reasoning = EXCLUDED.stage_reasoning,
                    expected_return_1w = EXCLUDED.expected_return_1w,
                    expected_return_2w = EXCLUDED.expected_return_2w,
                    expected_return_1m = EXCLUDED.expected_return_1m,
                    downside_risk = EXCLUDED.downside_risk,
                    dsex_dependency = EXCLUDED.dsex_dependency,
                    if_dsex_drops = EXCLUDED.if_dsex_drops,
                    if_dsex_rises = EXCLUDED.if_dsex_rises,
                    dsex_outlook = EXCLUDED.dsex_outlook,
                    batch_id = EXCLUDED.batch_id, raw_response = EXCLUDED.raw_response
            """, (
                date_str, symbol, action,
                r.get("confidence", "MEDIUM"),
                r.get("reasoning", ""),
                r.get("wait_for", ""),
                r.get("wait_days", ""),
                r.get("entry_low"),
                r.get("entry_high"),
                r.get("sl"),
                r.get("t1"),
                r.get("t2"),
                json.dumps(r.get("risk_factors", [])),
                json.dumps(r.get("catalysts", [])),
                r.get("score"),
                r.get("how_to_buy", ""),
                r.get("volume_rule", ""),
                r.get("next_day_plan", ""),
                r.get("sell_plan", ""),
                r.get("stage", ""),
                r.get("stage_reasoning", ""),
                r.get("expected_return_1w"),
                r.get("expected_return_2w"),
                r.get("expected_return_1m"),
                r.get("downside_risk"),
                r.get("dsex_dependency", ""),
                r.get("if_dsex_drops", ""),
                r.get("if_dsex_rises", ""),
                r.get("dsex_outlook", ""),
                batch_id,
                raw if saved == 0 else None,
            ))
            saved += 1
        except Exception as e:
            logger.error(f"Store LLM result {symbol}: {e}")
    conn.commit()
    conn.close()
    logger.info(f"Stored {saved} LLM results (batch {batch_id})")
    return saved


def run_llm_analysis(date_str: str) -> list[dict]:
    """Stage 1: Batch all A-category stocks through Claude LLM with 60-day OHLCV."""
    stocks = load_algo_analysis(date_str)
    if not stocks:
        logger.warning("No A-category stocks found for LLM analysis")
        return []

    # Skip stocks already analyzed (allows resuming after partial failure)
    conn = get_conn()
    cur = conn.cursor()
    cur.execute("SELECT symbol FROM llm_daily_analysis WHERE date = %s", (date_str,))
    already_done = {row["symbol"] for row in cur.fetchall()}
    conn.close()
    if already_done:
        original_count = len(stocks)
        stocks = [s for s in stocks if s["symbol"] not in already_done]
        logger.info(f"Skipping {original_count - len(stocks)} already-analyzed stocks, {len(stocks)} remaining")
        if not stocks:
            logger.info("All stocks already analyzed — loading existing results")
            conn = get_conn()
            cur = conn.cursor()
            cur.execute("SELECT symbol, action, confidence, reasoning, entry_low, entry_high, sl, t1, t2, score FROM llm_daily_analysis WHERE date = %s", (date_str,))
            existing = [dict(r) for r in cur.fetchall()]
            conn.close()
            return existing

    market = load_market_context()
    feedback = load_accuracy_feedback()

    # Load 6-month DSEX history (shared across all batches)
    dsex_csv = load_dsex_history(130)
    logger.info(f"Loaded DSEX history: {len(dsex_csv)} chars")

    # Load 1-year weekly + 6-month daily OHLCV for all stocks
    all_symbols = [s["symbol"] for s in stocks]
    ohlcv_map = load_ohlcv_history(all_symbols, daily_days=130, weekly_weeks=52)
    logger.info(f"Loaded OHLCV history for {len(ohlcv_map)} stocks")

    # Compute DSEX-stock correlations (beta, correlation, scenario returns)
    dsex_corr = compute_dsex_correlations(all_symbols, days=130)
    logger.info(f"Computed DSEX correlations for {len(dsex_corr)} stocks")

    batches = [stocks[i:i + LLM_BATCH_SIZE] for i in range(0, len(stocks), LLM_BATCH_SIZE)]
    total_batches = len(batches)
    logger.info(f"LLM analysis: {len(stocks)} stocks in {total_batches} batches (batch size {LLM_BATCH_SIZE})")

    MAX_RETRIES = 2
    BATCH_DELAY = 30  # seconds between batches to avoid rate limits
    COOLDOWN_AFTER_FAIL = 120  # 2 min cooldown after consecutive failures
    CONSECUTIVE_FAIL_THRESHOLD = 2  # trigger cooldown after this many consecutive failures

    all_results = []
    failed_batches = []  # (batch_index, batch_data) for retry
    consecutive_fails = 0

    for i, batch in enumerate(batches, 1):
        prompt = build_llm_prompt(
            batch, market, feedback, i, total_batches,
            ohlcv_map=ohlcv_map, dsex_csv=dsex_csv, dsex_corr=dsex_corr,
        )
        logger.info(f"Batch {i} prompt: {len(prompt)} chars")
        raw = call_claude(prompt)
        if not raw:
            consecutive_fails += 1
            logger.error(f"Batch {i}: no response — queued for retry (consecutive fails: {consecutive_fails})")
            failed_batches.append((i, batch))
            if consecutive_fails >= CONSECUTIVE_FAIL_THRESHOLD:
                logger.warning(f"Rate limit likely — cooling down {COOLDOWN_AFTER_FAIL}s...")
                time.sleep(COOLDOWN_AFTER_FAIL)
                consecutive_fails = 0
            continue

        parsed = parse_json_response(raw)
        if not parsed:
            consecutive_fails += 1
            logger.error(f"Batch {i}: failed to parse — queued for retry")
            failed_batches.append((i, batch))
            continue

        # Success — reset consecutive fail counter
        consecutive_fails = 0

        if isinstance(parsed, dict):
            parsed = [parsed]

        stored = store_llm_results(date_str, parsed, i, raw)
        all_results.extend(parsed)
        logger.info(f"Batch {i}/{total_batches}: {stored} stocks stored")

        if i < total_batches:
            time.sleep(BATCH_DELAY)

    # Retry failed batches (up to MAX_RETRIES times)
    for retry_round in range(1, MAX_RETRIES + 1):
        if not failed_batches:
            break
        logger.info(f"Retry round {retry_round}: {len(failed_batches)} failed batches — waiting 3 min before retries...")
        time.sleep(180)  # 3 min cooldown before retry round

        still_failed = []
        consecutive_fails = 0
        for batch_idx, batch in failed_batches:
            symbols = [s["symbol"] for s in batch]
            logger.info(f"Retrying batch {batch_idx} ({symbols})...")
            prompt = build_llm_prompt(
                batch, market, feedback, batch_idx, total_batches,
                ohlcv_map=ohlcv_map, dsex_csv=dsex_csv, dsex_corr=dsex_corr,
            )
            raw = call_claude(prompt)
            if not raw:
                consecutive_fails += 1
                logger.error(f"Retry {retry_round} batch {batch_idx}: still no response")
                still_failed.append((batch_idx, batch))
                if consecutive_fails >= CONSECUTIVE_FAIL_THRESHOLD:
                    logger.warning(f"Rate limit on retries — cooling down {COOLDOWN_AFTER_FAIL}s...")
                    time.sleep(COOLDOWN_AFTER_FAIL)
                    consecutive_fails = 0
                else:
                    time.sleep(BATCH_DELAY)
                continue

            parsed = parse_json_response(raw)
            if not parsed:
                logger.error(f"Retry {retry_round} batch {batch_idx}: still failed to parse")
                still_failed.append((batch_idx, batch))
                time.sleep(BATCH_DELAY)
                continue

            consecutive_fails = 0
            if isinstance(parsed, dict):
                parsed = [parsed]

            stored = store_llm_results(date_str, parsed, batch_idx, raw)
            all_results.extend(parsed)
            logger.info(f"Retry {retry_round} batch {batch_idx}: SUCCESS — {stored} stocks stored")
            time.sleep(BATCH_DELAY)

        failed_batches = still_failed

    if failed_batches:
        missed = [s["symbol"] for _, batch in failed_batches for s in batch]
        logger.warning(f"PERMANENTLY FAILED {len(failed_batches)} batches after {MAX_RETRIES} retries. "
                       f"Missed stocks: {missed}")

    # Merge newly analyzed results with previously existing ones for downstream stages
    if already_done:
        conn = get_conn()
        cur = conn.cursor()
        cur.execute("SELECT symbol, action, confidence, reasoning, entry_low, entry_high, sl, t1, t2, score FROM llm_daily_analysis WHERE date = %s", (date_str,))
        all_db_results = [dict(r) for r in cur.fetchall()]
        conn.close()
        logger.info(f"LLM analysis complete: {len(all_results)} new + {len(already_done)} existing = {len(all_db_results)} total")
        return all_db_results

    logger.info(f"LLM analysis complete: {len(all_results)} total results")
    return all_results


# ─── Stage 2: Judge ───


def build_judge_prompt(
    pairs: list[dict], market: dict, batch_num: int, total_batches: int
) -> str:
    """Build prompt for judge comparing algo vs LLM."""
    dsex = market.get("dsex_index", 0)

    stock_lines = []
    for p in pairs:
        algo = p["algo"]
        llm = p["llm"]
        stock_lines.append(
            f"### {p['symbol']} ({algo.get('sector', '?')})\n"
            f"LTP: {algo.get('ltp', 0):.1f} | RSI: {algo.get('rsi', 0):.1f} | "
            f"MACD: {algo.get('macd_status', '')} ({algo.get('macd_hist', 0):+.2f})\n"
            f"**ALGO**: {algo['action']} (score {algo.get('score', 0):.0f}) — "
            f"{(algo.get('reasoning') or '')[:100]}\n"
            f"  Entry: {algo.get('entry_low', 0):.1f}-{algo.get('entry_high', 0):.1f} | "
            f"SL: {algo.get('sl', 0):.1f} | T1: {algo.get('t1', 0):.1f} | Wait: {algo.get('wait_days', '')}\n"
            f"**LLM**: {llm.get('action', '')} (conf {llm.get('confidence', '?')}, score {llm.get('score', 0)}) — "
            f"{(llm.get('reasoning') or '')[:100]}\n"
            f"  Wait for: {(llm.get('wait_for') or '')[:80]}\n"
            f"  Entry: {llm.get('entry_low', 0)}-{llm.get('entry_high', 0)} | "
            f"SL: {llm.get('sl', 0)} | T1: {llm.get('t1', 0)}"
        )

    return f"""You are a senior trading judge for DSE (Dhaka Stock Exchange). Your audience is BEGINNERS. Compare the ALGORITHMIC and LLM analyses for each stock and produce a FINAL verdict that a new trader can understand and act on.

## Market: DSEX {dsex:.1f}

## Stocks (batch {batch_num}/{total_batches})

{chr(10).join(stock_lines)}

## Your Task
For EACH stock, decide which analysis is better and produce a FINAL recommendation.
Write your reasoning in PLAIN LANGUAGE that someone new to trading can understand.

Return a JSON array (NO markdown fences, ONLY valid JSON):
[
  {{
    "symbol": "SYMBOL",
    "final_action": "BUY|BUY on dip|BUY on pullback|BUY (wait for MACD cross)|HOLD/WAIT|SELL/AVOID|AVOID",
    "final_confidence": "HIGH|MEDIUM|LOW",
    "agreement": true,
    "reasoning": "2-3 sentences in plain language: why you chose this action, what the beginner should DO, and what to WATCH for. Example: 'Both algo and LLM agree this stock is oversold and due for a bounce. Place a limit order at 32.5 and wait. If it drops below 31.0 (stop loss), sell immediately to limit losses.'",
    "algo_strengths": "What the algorithm's math got right (in plain language)",
    "llm_strengths": "What the LLM's narrative/context analysis added (in plain language)",
    "key_risk": "Primary risk in plain language: e.g., 'If the overall market drops, this stock will fall too regardless of how good its indicators look'",
    "wait_days": "e.g., '5-10 days'",
    "entry_low": 0.0,
    "entry_high": 0.0,
    "sl": 0.0,
    "t1": 0.0,
    "t2": 0.0,
    "score": 50
  }}
]

Rules:
1. If both agree, say agreement=true and blend their reasoning into a clear action plan for beginners
2. If they disagree, pick the better analysis and explain WHY in plain language
3. Weight algo higher for pure technical signals (RSI, MACD levels) — numbers don't lie
4. Weight LLM higher for context/narrative (catalysts, sector rotation, volume interpretation) — understanding WHY matters
5. T+2 settlement applies — if a stock might peak today, warn the beginner: "You can't sell for 2 days, so buying now is risky"
6. Your key_risk should be something a beginner can actually monitor (not jargon)
7. Return ONLY valid JSON array"""


def store_judge_results(date_str: str, results: list[dict], batch_id: int, raw: str):
    """Store judge analysis results."""
    conn = get_conn()
    cur = conn.cursor()
    saved = 0
    for r in results:
        symbol = r.get("symbol", "")
        if not symbol:
            continue
        try:
            cur.execute("""
                INSERT INTO judge_daily_analysis
                    (date, symbol, algo_action, llm_action, final_action, final_confidence,
                     agreement, reasoning, algo_strengths, llm_strengths, key_risk,
                     wait_days, entry_low, entry_high, sl, t1, t2, score, batch_id, raw_response)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                ON CONFLICT (date, symbol) DO UPDATE SET
                    algo_action = EXCLUDED.algo_action, llm_action = EXCLUDED.llm_action,
                    final_action = EXCLUDED.final_action, final_confidence = EXCLUDED.final_confidence,
                    agreement = EXCLUDED.agreement, reasoning = EXCLUDED.reasoning,
                    algo_strengths = EXCLUDED.algo_strengths, llm_strengths = EXCLUDED.llm_strengths,
                    key_risk = EXCLUDED.key_risk, wait_days = EXCLUDED.wait_days,
                    entry_low = EXCLUDED.entry_low, entry_high = EXCLUDED.entry_high,
                    sl = EXCLUDED.sl, t1 = EXCLUDED.t1, t2 = EXCLUDED.t2,
                    score = EXCLUDED.score, batch_id = EXCLUDED.batch_id,
                    raw_response = EXCLUDED.raw_response
            """, (
                date_str, symbol,
                r.get("algo_action", ""),
                r.get("llm_action", ""),
                normalize_action(r.get("final_action", "")),
                r.get("final_confidence", "MEDIUM"),
                r.get("agreement", True),
                r.get("reasoning", ""),
                r.get("algo_strengths", ""),
                r.get("llm_strengths", ""),
                r.get("key_risk", ""),
                r.get("wait_days", ""),
                r.get("entry_low"),
                r.get("entry_high"),
                r.get("sl"),
                r.get("t1"),
                r.get("t2"),
                r.get("score"),
                batch_id,
                raw if saved == 0 else None,
            ))
            saved += 1
        except Exception as e:
            logger.error(f"Store judge {symbol}: {e}")
    conn.commit()
    conn.close()
    logger.info(f"Stored {saved} judge results (batch {batch_id})")


def run_judge_analysis(date_str: str, llm_results: list[dict]) -> int:
    """Stage 2: Judge compares algo vs LLM for each stock. Returns pair count."""
    algo_data = load_algo_analysis(date_str)
    algo_by_sym = {a["symbol"]: a for a in algo_data}
    llm_by_sym = {r["symbol"]: r for r in llm_results if r.get("symbol")}

    pairs = []
    for symbol, algo in algo_by_sym.items():
        llm = llm_by_sym.get(symbol)
        if llm:
            pairs.append({"symbol": symbol, "algo": algo, "llm": llm})

    if not pairs:
        logger.warning("No algo+LLM pairs to judge")
        return 0

    market = load_market_context()
    batches = [pairs[i:i + JUDGE_BATCH_SIZE] for i in range(0, len(pairs), JUDGE_BATCH_SIZE)]
    total = len(batches)
    logger.info(f"Judge analysis: {len(pairs)} pairs in {total} batches")

    consecutive_fails = 0
    for i, batch in enumerate(batches, 1):
        prompt = build_judge_prompt(batch, market, i, total)
        raw = call_claude(prompt)
        if not raw:
            consecutive_fails += 1
            logger.error(f"Judge batch {i}: no response (consecutive fails: {consecutive_fails})")
            if consecutive_fails >= 2:
                logger.warning("Rate limit likely on judge — cooling down 120s...")
                time.sleep(120)
                consecutive_fails = 0
            continue

        parsed = parse_json_response(raw)
        if not parsed:
            logger.error(f"Judge batch {i}: failed to parse")
            continue

        consecutive_fails = 0

        if isinstance(parsed, dict):
            parsed = [parsed]

        # Fill algo_action and llm_action from our data (in case judge omits)
        pair_map = {p["symbol"]: p for p in batch}
        for item in parsed:
            sym = item.get("symbol", "")
            if sym in pair_map:
                item.setdefault("algo_action", pair_map[sym]["algo"]["action"])
                item.setdefault("llm_action", pair_map[sym]["llm"].get("action", ""))

        store_judge_results(date_str, parsed, i, raw)
        logger.info(f"Judge batch {i}/{total} done")

        if i < total:
            time.sleep(30)

    return len(pairs)


# ─── Stage 3: Snapshot predictions ───


def snapshot_predictions(date_str: str):
    """Snapshot algo, LLM, and judge predictions into prediction_tracker."""
    conn = get_conn()
    cur = conn.cursor()
    saved = 0

    # Source 1: Algo (from daily_analysis)
    cur.execute("""
        SELECT da.symbol, da.action, da.score, da.wait_days, da.ltp,
               da.entry_low, da.entry_high, da.sl, da.t1, da.t2
        FROM daily_analysis da
        JOIN fundamentals f ON da.symbol = f.symbol
        WHERE da.date = %s AND f.category = 'A'
    """, (date_str,))
    for r in cur.fetchall():
        wd_min, wd_max = parse_wait_days(r["wait_days"])
        try:
            cur.execute("""
                INSERT INTO prediction_tracker
                    (date, symbol, source, action, score, wait_days,
                     wait_days_min, wait_days_max, ltp_at_prediction,
                     entry_low, entry_high, sl, t1, t2)
                VALUES (%s, %s, 'algo', %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                ON CONFLICT (date, symbol, source) DO NOTHING
            """, (
                date_str, r["symbol"], r["action"], r["score"], r["wait_days"],
                wd_min, wd_max, r["ltp"],
                r["entry_low"], r["entry_high"], r["sl"], r["t1"], r["t2"],
            ))
            saved += 1
        except Exception as e:
            logger.error(f"Snapshot algo {r['symbol']}: {e}")

    # Source 2: LLM
    cur.execute("""
        SELECT symbol, action, score, wait_days,
               entry_low, entry_high, sl, t1, t2
        FROM llm_daily_analysis WHERE date = %s
    """, (date_str,))
    for r in cur.fetchall():
        wd_min, wd_max = parse_wait_days(r["wait_days"])
        try:
            # Get LTP from algo data
            cur.execute(
                "SELECT ltp FROM daily_analysis WHERE date = %s AND symbol = %s",
                (date_str, r["symbol"]),
            )
            ltp_row = cur.fetchone()
            ltp = ltp_row["ltp"] if ltp_row else None
            cur.execute("""
                INSERT INTO prediction_tracker
                    (date, symbol, source, action, score, wait_days,
                     wait_days_min, wait_days_max, ltp_at_prediction,
                     entry_low, entry_high, sl, t1, t2)
                VALUES (%s, %s, 'llm', %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                ON CONFLICT (date, symbol, source) DO NOTHING
            """, (
                date_str, r["symbol"], r["action"], r["score"], r["wait_days"],
                wd_min, wd_max, ltp,
                r["entry_low"], r["entry_high"], r["sl"], r["t1"], r["t2"],
            ))
            saved += 1
        except Exception as e:
            logger.error(f"Snapshot llm {r['symbol']}: {e}")

    # Source 3: Judge
    cur.execute("""
        SELECT symbol, final_action, score, wait_days,
               entry_low, entry_high, sl, t1, t2
        FROM judge_daily_analysis WHERE date = %s
    """, (date_str,))
    for r in cur.fetchall():
        wd_min, wd_max = parse_wait_days(r["wait_days"])
        try:
            cur.execute(
                "SELECT ltp FROM daily_analysis WHERE date = %s AND symbol = %s",
                (date_str, r["symbol"]),
            )
            ltp_row = cur.fetchone()
            ltp = ltp_row["ltp"] if ltp_row else None
            cur.execute("""
                INSERT INTO prediction_tracker
                    (date, symbol, source, action, score, wait_days,
                     wait_days_min, wait_days_max, ltp_at_prediction,
                     entry_low, entry_high, sl, t1, t2)
                VALUES (%s, %s, 'judge', %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                ON CONFLICT (date, symbol, source) DO NOTHING
            """, (
                date_str, r["symbol"], r["final_action"], r["score"], r["wait_days"],
                wd_min, wd_max, ltp,
                r["entry_low"], r["entry_high"], r["sl"], r["t1"], r["t2"],
            ))
            saved += 1
        except Exception as e:
            logger.error(f"Snapshot judge {r['symbol']}: {e}")

    conn.commit()
    conn.close()
    logger.info(f"Snapshotted {saved} predictions into tracker")


# ─── Stage 4: Override algo entry/exit with AI values ───


def override_algo_entry_exit(date_str: str):
    """Stage 4: Replace algo's hardcoded entry/exit with Judge (or LLM) values.

    The algo's _compute_entry_exit() uses ATR-based formulas that break when
    indicators are NaN (dividend gaps, insufficient history). The LLM computes
    sensible values by understanding the stock's actual price context.
    """
    conn = get_conn()
    cur = conn.cursor()

    # Get all judge + LLM results for today
    cur.execute("""
        SELECT j.symbol,
               j.entry_low AS j_el, j.entry_high AS j_eh,
               j.sl AS j_sl, j.t1 AS j_t1, j.t2 AS j_t2,
               j.final_action AS j_action,
               l.entry_low AS l_el, l.entry_high AS l_eh,
               l.sl AS l_sl, l.t1 AS l_t1, l.t2 AS l_t2,
               l.action AS l_action,
               da.ltp
        FROM daily_analysis da
        LEFT JOIN judge_daily_analysis j ON j.date = da.date AND j.symbol = da.symbol
        LEFT JOIN llm_daily_analysis l ON l.date = da.date AND l.symbol = da.symbol
        WHERE da.date = %s
          AND (j.entry_low IS NOT NULL OR l.entry_low IS NOT NULL)
    """, (date_str,))
    rows = cur.fetchall()

    updated = 0
    for r in rows:
        ltp = float(r["ltp"] or 0)
        if ltp <= 0:
            continue

        # Prefer judge values, fall back to LLM
        el = r["j_el"] or r["l_el"]
        eh = r["j_eh"] or r["l_eh"]
        sl = r["j_sl"] or r["l_sl"]
        t1 = r["j_t1"] or r["l_t1"]
        t2 = r["j_t2"] or r["l_t2"]
        resolved_action = r["j_action"] or r["l_action"]

        # Sanity check: AI values must be within reasonable range of LTP
        if el and (el < ltp * 0.5 or el > ltp * 1.5):
            continue
        if eh and (eh < ltp * 0.5 or eh > ltp * 1.5):
            continue

        # Build SET clause only for non-null values
        sets = []
        vals = []
        for col, val in [("entry_low", el), ("entry_high", eh),
                         ("sl", sl), ("t1", t1), ("t2", t2)]:
            if val is not None:
                sets.append(f"{col} = %s")
                vals.append(round(float(val), 1))

        # Override action with AI-resolved action (judge > LLM)
        if resolved_action:
            sets.append("action = %s")
            vals.append(resolved_action)

        if not sets:
            continue

        vals.extend([date_str, r["symbol"]])
        try:
            cur.execute(
                f"UPDATE daily_analysis SET {', '.join(sets)} "
                f"WHERE date = %s AND symbol = %s",
                vals,
            )
            updated += 1
        except Exception as e:
            logger.error(f"Override {r['symbol']}: {e}")

    conn.commit()
    conn.close()
    logger.info(f"Stage 4: Overrode entry/exit for {updated} stocks with AI values")
    return updated


# ─── Email notification ───


def send_completion_email(date_str: str, llm_count: int, judge_count: int, override_count: int):
    """Send email summary after LLM pipeline completes.

    Requires EMAIL_SENDER, EMAIL_PASSWORD, EMAIL_RECIPIENT env vars.
    Skips gracefully if any is missing or on any error.
    """
    sender = os.getenv("EMAIL_SENDER", "")
    password = os.getenv("EMAIL_PASSWORD", "")
    recipient = os.getenv("EMAIL_RECIPIENT", "")

    if not all([sender, password, recipient]):
        logger.info("Email not configured (set EMAIL_SENDER, EMAIL_PASSWORD, EMAIL_RECIPIENT)")
        return

    try:
        conn = get_conn()
        cur = conn.cursor()

        # Top 10 buy signals
        cur.execute("""
            SELECT da.symbol, da.action, da.score, da.ltp, da.entry_low, da.entry_high,
                   da.sl, da.t1, da.t2, da.macd_status, da.rsi
            FROM daily_analysis da
            WHERE da.date = %s AND da.action LIKE '%%BUY%%' AND da.action NOT LIKE '%%AVOID%%'
            ORDER BY da.score DESC LIMIT 10
        """, (date_str,))
        top_buys = cur.fetchall()

        # Portfolio stocks — query all stocks user holds (A-cat with LLM data)
        cur.execute("""
            SELECT symbol, ltp, action, entry_low, entry_high, sl, t1, t2
            FROM daily_analysis WHERE date = %s
            AND symbol IN ('ORIONINFU', 'ROBI', 'GP', 'HWAWELLTEX')
        """, (date_str,))
        portfolio = cur.fetchall()

        # DSEX
        cur.execute("""
            SELECT d1.dsex_index,
                   ROUND(((d1.dsex_index - d2.dsex_index) / NULLIF(d2.dsex_index, 0) * 100)::numeric, 2) AS chg_pct
            FROM dsex_history d1
            LEFT JOIN LATERAL (
                SELECT dsex_index FROM dsex_history WHERE date < d1.date ORDER BY date DESC LIMIT 1
            ) d2 ON TRUE
            ORDER BY d1.date DESC LIMIT 1
        """)
        dsex = cur.fetchone()

        conn.close()

        # Build email body
        if dsex:
            dsex_str = f"DSEX: {float(dsex['dsex_index']):.1f} ({float(dsex['chg_pct'] or 0):+.2f}%)"
        else:
            dsex_str = "DSEX: N/A"

        body = f"""DSE AI Analysis Complete -- {date_str}
{'='*50}

{dsex_str}
LLM: {llm_count} stocks | Judge: {judge_count} stocks | Overrides: {override_count}

PORTFOLIO
{'-'*40}
"""
        for p in portfolio:
            body += (
                f"{p['symbol']:12s} LTP:{p['ltp']:>8.1f} | "
                f"{p['action']} | Entry:{p['entry_low']:.1f}-{p['entry_high']:.1f} "
                f"SL:{p['sl']:.1f} T1:{p['t1']:.1f} T2:{p['t2']:.1f}\n"
            )

        body += f"\nTOP BUY SIGNALS\n{'-'*40}\n"
        for i, b in enumerate(top_buys, 1):
            body += (
                f"{i:2d}. {b['symbol']:12s} Score:{b['score']:>3.0f} LTP:{b['ltp']:>8.1f} | "
                f"Entry:{b['entry_low']:.1f}-{b['entry_high']:.1f} "
                f"T1:{b['t1']:.1f} T2:{b['t2']:.1f} | "
                f"MACD:{b['macd_status']} RSI:{b['rsi']:.0f}\n"
            )

        body += f"\n--\nGenerated by DSE AI Analyzer at {datetime.now(DSE_TZ).strftime('%H:%M:%S BST')}"

        # Send via Gmail SMTP
        msg = MIMEMultipart()
        msg['From'] = sender
        msg['To'] = recipient
        msg['Subject'] = f"DSE AI Analysis -- {date_str} | {dsex_str}"
        msg.attach(MIMEText(body, 'plain'))

        with smtplib.SMTP('smtp.gmail.com', 587) as server:
            server.starttls()
            server.login(sender, password)
            server.sendmail(sender, recipient, msg.as_string())
        logger.info(f"Email sent to {recipient}")

    except Exception as e:
        logger.error(f"Email notification failed: {e}")


# ─── Stage 5: DSEX Forecast ───


def run_dsex_forecast(date_str: str):
    """Dedicated DSEX index analysis — forecast tomorrow + next 3-5 days."""
    dsex_csv = load_dsex_history(130)
    if not dsex_csv:
        logger.warning("No DSEX history for forecast")
        return

    market = load_market_context()

    prompt = f"""You are a DSEX (Dhaka Stock Exchange Index) analyst. Analyze the 6-month DSEX history below and provide a detailed forecast.

## DSEX History (6 months)
```
{dsex_csv}
```

## Current Market
- DSEX: {market.get('dsex_index', 0):.1f} ({market.get('dsex_change_pct', 0):+.2f}%)
- Advances: {market.get('advances', 0)} | Declines: {market.get('declines', 0)}
- Volume: {market.get('total_volume', 0):,} | Turnover: {market.get('total_value', 0):,.0f}

## Your Task
Analyze the DSEX chart like an expert technical analyst. Study:
1. **Trend**: 5-day, 20-day, 60-day trends. Is DSEX trending up, down, or sideways?
2. **Support/Resistance**: Where are the key levels? Where has DSEX bounced or been rejected?
3. **Volume**: Is volume increasing or decreasing? What does it mean?
4. **Momentum**: Is the rally/decline accelerating or fading?
5. **Pattern**: Any recognizable patterns (double bottom, head & shoulders, channel)?

Then forecast:
- **Tomorrow**: What will DSEX likely do? Up/Down/Flat? By how much?
- **Next 3-5 trading days**: Direction and range
- **Next 1-2 weeks**: Where is DSEX heading?

Return JSON (NO markdown fences):
{{
    "forecast": "2-3 paragraph detailed analysis of DSEX direction with specific price levels and dates. Written for beginners — explain WHY the index will move that way.",
    "sentiment": "BULLISH|BEARISH|NEUTRAL|CAUTIOUS",
    "support": 0.0,
    "resistance": 0.0,
    "expected_direction": "UP|DOWN|SIDEWAYS",
    "confidence": "HIGH|MEDIUM|LOW",
    "key_factors": "Top 3 factors driving DSEX direction right now",
    "scenario_bull": "Bull case: what happens if DSEX breaks above resistance. Expected move, target, timeline.",
    "scenario_bear": "Bear case: what happens if DSEX breaks below support. Expected drop, floor, timeline.",
    "scenario_base": "Most likely scenario (60%+ probability). What DSEX does in next 5 days and why."
}}"""

    raw = call_claude(prompt, timeout=120)
    if not raw:
        logger.error("DSEX forecast: no response")
        return

    parsed = parse_json_response(raw)
    if not parsed or not isinstance(parsed, dict):
        logger.error("DSEX forecast: failed to parse")
        return

    conn = get_conn()
    cur = conn.cursor()
    try:
        cur.execute("""
            INSERT INTO dsex_forecast
                (date, forecast, sentiment, support, resistance, expected_direction,
                 confidence, key_factors, scenario_bull, scenario_bear, scenario_base,
                 raw_response)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
            ON CONFLICT (date) DO UPDATE SET
                forecast = EXCLUDED.forecast, sentiment = EXCLUDED.sentiment,
                support = EXCLUDED.support, resistance = EXCLUDED.resistance,
                expected_direction = EXCLUDED.expected_direction,
                confidence = EXCLUDED.confidence, key_factors = EXCLUDED.key_factors,
                scenario_bull = EXCLUDED.scenario_bull, scenario_bear = EXCLUDED.scenario_bear,
                scenario_base = EXCLUDED.scenario_base, raw_response = EXCLUDED.raw_response
        """, (
            date_str,
            parsed.get("forecast", ""),
            parsed.get("sentiment", ""),
            parsed.get("support"),
            parsed.get("resistance"),
            parsed.get("expected_direction", ""),
            parsed.get("confidence", ""),
            parsed.get("key_factors", ""),
            parsed.get("scenario_bull", ""),
            parsed.get("scenario_bear", ""),
            parsed.get("scenario_base", ""),
            raw,
        ))
        conn.commit()
        logger.info(f"Stage 5: DSEX forecast stored ({parsed.get('sentiment', '?')}, {parsed.get('expected_direction', '?')})")
    except Exception as e:
        logger.error(f"Store DSEX forecast: {e}")
    finally:
        conn.close()


# ─── Stage 6: Pre-compute Radar ───


def precompute_radar(date_str: str):
    """Pre-compute the Buy Radar and store as JSON for instant API loading."""
    import sys
    sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

    try:
        # Import the radar endpoint function and call its logic
        from api.routes_analysis import get_buy_radar
        import asyncio

        # Run the async endpoint synchronously
        loop = asyncio.new_event_loop()
        result = loop.run_until_complete(get_buy_radar(categories="A"))
        loop.close()

        if not result or not isinstance(result, dict):
            logger.warning("Radar precompute: empty result")
            return

        # Add DSEX forecast to the result
        conn = get_conn()
        cur = conn.cursor()
        cur.execute("SELECT forecast, sentiment, support, resistance, expected_direction, "
                     "confidence, key_factors, scenario_bull, scenario_bear, scenario_base "
                     "FROM dsex_forecast WHERE date = %s", (date_str,))
        dsex_row = cur.fetchone()
        if dsex_row:
            result["dsex_forecast"] = dict(dsex_row)

        # Store as JSON
        data_json = json.dumps(result, default=str)
        cur.execute("""
            INSERT INTO radar_precomputed (date, category, data_json)
            VALUES (%s, 'A', %s)
            ON CONFLICT (date, category) DO UPDATE SET
                data_json = EXCLUDED.data_json, created_at = NOW()
        """, (date_str, data_json))
        conn.commit()
        conn.close()

        stock_count = result.get("count", 0)
        logger.info(f"Stage 6: Radar precomputed and stored ({stock_count} stocks)")

    except Exception as e:
        logger.error(f"Radar precompute failed: {e}")
        import traceback
        traceback.print_exc()


# ─── Main ───


def run():
    """Main entry point: Stage 1 → Stage 2 → Stage 3 → Stage 4 → Stage 5 (email)."""
    now = datetime.now(DSE_TZ)
    logger.info(f"=== LLM Daily Analyzer starting at {now.strftime('%Y-%m-%d %H:%M:%S')} BST ===")

    if HAS_ANTHROPIC_SDK and ANTHROPIC_API_KEY:
        logger.info(f"Using Anthropic SDK (model={CLAUDE_MODEL})")
    else:
        if not ANTHROPIC_API_KEY:
            logger.warning("ANTHROPIC_API_KEY not set — falling back to Claude CLI")
        if not HAS_ANTHROPIC_SDK:
            logger.warning("anthropic package not installed — falling back to Claude CLI")

    ensure_tables()

    # Get latest analysis date
    conn = get_conn()
    cur = conn.cursor()
    cur.execute("SELECT MAX(date) as d FROM daily_analysis")
    row = cur.fetchone()
    conn.close()

    if not row or not row["d"]:
        logger.error("No daily_analysis data found")
        return

    date_str = str(row["d"])
    logger.info(f"Analyzing date: {date_str}")

    # Stage 1: LLM Analysis
    llm_results = run_llm_analysis(date_str)
    if not llm_results:
        logger.warning("LLM analysis produced no results — skipping judge")
        # Still snapshot algo predictions
        snapshot_predictions(date_str)
        return

    # Stage 2: Judge
    judge_count = run_judge_analysis(date_str, llm_results)

    # Stage 3: Snapshot predictions
    snapshot_predictions(date_str)

    # Stage 4: Override algo's hardcoded entry/exit with AI-computed values
    override_count = override_algo_entry_exit(date_str)

    # Stage 5: DSEX forecast (dedicated analysis)
    run_dsex_forecast(date_str)

    # Stage 6: Pre-compute Buy Radar (store for instant API loading)
    precompute_radar(date_str)

    # Stage 7: Email notification
    send_completion_email(date_str, len(llm_results), judge_count, override_count)

    logger.info("=== LLM Daily Analyzer complete ===")


if __name__ == "__main__":
    run()
