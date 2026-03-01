#!/usr/bin/env python3
"""LLM Daily Analyzer — Three-stage analysis pipeline for GCP VM.

Stage 1: LLM analyzes ALL A-category stocks (batched, 30/call)
Stage 2: Judge LLM compares algo vs LLM and picks best (batched, 50/call)
Stage 3: Snapshot all predictions into prediction_tracker

Usage:
    python3 scripts/llm_daily_analyzer.py

Cron (after algo analysis at 15:00 BST):
    5 9 * * 0-4 cd /path/to/backend && python3 scripts/llm_daily_analyzer.py
"""

import json
import logging
import os
import re
import subprocess
import sys
import time
from datetime import datetime

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

LLM_BATCH_SIZE = 15
JUDGE_BATCH_SIZE = 30
CLAUDE_TIMEOUT = 600
CLAUDE_MODEL = os.getenv("CLAUDE_MODEL", "claude-sonnet-4-5-20250514")
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
            batch_id INTEGER, raw_response TEXT,
            created_at TIMESTAMP DEFAULT NOW(), UNIQUE(date, symbol)
        )""",
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


# ─── Claude CLI ───


def call_claude(prompt: str, timeout: int = CLAUDE_TIMEOUT) -> str:
    """Call Claude via Anthropic SDK (preferred) or CLI fallback."""
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

    # Fallback: Claude CLI (requires `claude login` or `claude setup-token`)
    try:
        result = subprocess.run(
            ["claude", "-p", "--model", "sonnet", prompt],
            capture_output=True,
            text=True,
            timeout=timeout,
        )
        if result.returncode != 0:
            logger.error(f"Claude CLI error: {result.stderr[:300]}")
            return ""
        resp = result.stdout.strip()
        if "Not logged in" in resp or "Please run /login" in resp:
            logger.error("Claude CLI not authenticated. Set ANTHROPIC_API_KEY or run: claude setup-token")
            return ""
        logger.info(f"Claude CLI response: {len(resp)} chars")
        return resp
    except FileNotFoundError:
        logger.error("Claude CLI not found. Set ANTHROPIC_API_KEY or install: npm install -g @anthropic-ai/claude-code")
        return ""
    except subprocess.TimeoutExpired:
        logger.error(f"Claude CLI timed out ({timeout}s)")
        return ""


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
    stocks: list[dict], market: dict, feedback: str, batch_num: int, total_batches: int
) -> str:
    """Build prompt for a batch of stocks."""
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

        stock_lines.append(
            f"### {s['symbol']} ({s.get('sector', '?')})\n"
            f"LTP: {ltp:.1f} | Algo: {s['action']} (score {s.get('score', 0):.0f})\n"
            f"Entry: {s.get('entry_low', 0):.1f}-{s.get('entry_high', 0):.1f} | "
            f"SL: {s.get('sl', 0):.1f} | T1: {s.get('t1', 0):.1f} | T2: {s.get('t2', 0):.1f}\n"
            f"RSI: {rsi:.1f} | StochRSI: {stoch:.1f} | MACD: {s.get('macd_status', '')} (hist {macd_h:+.2f})\n"
            f"BB%: {bb:.1f}% | VolRatio: {vol_r:.1f}x | Trend50d: {trend:+.1f}% | ATR%: {atr_p:.1f}%\n"
            f"Support: {s.get('support', 0):.1f} | Resistance: {s.get('resistance', 0):.1f}\n"
            f"Risk: {s.get('risk_pct', 0):.1f}% | Reward: {s.get('reward_pct', 0):.1f}%\n"
            f"Algo reason: {(s.get('reasoning') or '')[:120]}\n"
            f"Algo wait: {s.get('wait_days', '')}"
        )

    return f"""You are a senior DSE (Dhaka Stock Exchange) trading analyst writing for BEGINNERS who are new to the stock market. Your readers do NOT know what RSI, MACD, Bollinger Bands, or ATR mean. You MUST explain every indicator you reference in plain language.

## INDICATOR GLOSSARY (use these explanations in your reasoning)
- **RSI** (Relative Strength Index, 0-100): Measures if a stock is oversold (<30 = sellers exhausted, price likely to bounce UP) or overbought (>70 = buyers exhausted, price likely to drop). 40-60 is neutral.
- **StochRSI** (0-100): A faster version of RSI. <20 = deeply oversold (strong bounce signal). >80 = deeply overbought (pullback coming).
- **MACD**: Shows momentum direction. "Bullish cross" = momentum shifting UP (buy signal). "Bearish cross" = momentum shifting DOWN (sell signal). "Converging" = about to cross (get ready). Histogram > 0 = upward momentum.
- **BB%** (Bollinger Band %): Where price sits within its 20-day range. 0-15% = near bottom (cheap vs average, bounce zone). 85-100% = near top (expensive vs average, may drop). 40-60% = fair value.
- **ATR%** (Average True Range %): How much the stock typically moves per day as % of price. High ATR% = volatile stock (bigger moves, bigger risk). Used to set stop loss distance.
- **Vol Ratio**: Today's volume vs 20-day average. >2x = unusual interest (big move likely). <0.5x = nobody cares (avoid).
- **Trend50d**: Price change over 50 days. Positive = uptrend. Negative = downtrend.
- **SMA50**: 50-day average price. Price above = uptrend. Below = downtrend.
- **Support**: Price level where stock historically stops falling (floor). **Resistance**: Price level where stock historically stops rising (ceiling).
- **T+2**: After buying, you CANNOT sell for 2 trading days. If stock peaks today, buying is BAD.
- **Pullback**: Stock in uptrend that dipped temporarily — buy at a discount before it resumes rising.
- **Dip**: Price dropped to lower part of normal range — buy before it bounces back to average.

## Market Context
- DSEX: {dsex:.1f} ({dsex_chg:+.2f}%)
- Advances: {market.get('advances', 0)} | Declines: {market.get('declines', 0)}
- Volume: {market.get('total_volume', 0):,} | Turnover: {market.get('total_value', 0):,.0f}

{feedback}
## Stocks to Analyze (batch {batch_num}/{total_batches})

{chr(10).join(stock_lines)}

## Your Task
For EACH stock, provide your INDEPENDENT analysis. You may agree or disagree with the algo.

**CRITICAL: Write your reasoning as if teaching a beginner.** Don't just say "RSI is 28" — say "RSI is at 28, which is below 30 (oversold territory) — this means sellers are running out of steam and the price is likely to bounce upward soon."

Return a JSON array (NO markdown fences, ONLY valid JSON):
[
  {{
    "symbol": "SYMBOL",
    "action": "BUY|BUY on dip|BUY on pullback|BUY (wait for MACD cross)|HOLD/WAIT|SELL/AVOID|AVOID",
    "confidence": "HIGH|MEDIUM|LOW",
    "reasoning": "3-5 sentence EDUCATIONAL analysis. For EACH indicator you mention, explain what it means and WHY it matters for this stock. Example: 'RSI is at 28 (below 30 = oversold, sellers exhausted). MACD just crossed bullish (momentum shifting up — this is a buy signal). Price is near the lower Bollinger Band (cheap compared to its 20-day average). Together these signals say: the stock has been beaten down enough and is ready to bounce.'",
    "wait_for": "Specific trigger in PLAIN LANGUAGE: e.g., 'Wait for MACD to cross its signal line from below (this means buying momentum is starting). Also watch for RSI to bounce above 30 (confirms sellers are done).'",
    "wait_days": "e.g., 'NOW', '1-3 days', '5-10 days', '15-30 days'",
    "entry_low": 0.0,
    "entry_high": 0.0,
    "sl": 0.0,
    "t1": 0.0,
    "t2": 0.0,
    "risk_factors": ["Plain language risk: e.g., 'If overall market drops below 5400, this stock will likely fall too regardless of technicals'"],
    "catalysts": ["Plain language catalyst: e.g., 'Dividend announcement expected next week which could push price up'"],
    "score": 50
  }}
]

Rules:
1. Analyze ALL {len(stocks)} stocks, not just BUY candidates
2. T+2 settlement: buyer CANNOT sell for 2 trading days. If stock peaks today, BAD BUY. Flag this risk.
3. DSE tick size: 0.10 BDT. All prices must be multiples of 0.10.
4. EVERY indicator mentioned in reasoning MUST include what it means (use the glossary above)
5. Score 0-100: 0=strong avoid, 50=neutral, 100=strong buy
6. HOLD/WAIT must explain EXACTLY what to watch for and how a beginner would recognize the trigger
7. For AVOID stocks, explain what would need to change (in plain language) for it to become buyable
8. Risk factors and catalysts must be understandable by someone who has never traded before
9. If you made mistakes in past predictions (shown in feedback above), EXPLICITLY adjust your approach. Be more conservative where you were wrong.
10. Return ONLY valid JSON array, no extra text"""


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
                     risk_factors, catalysts, score, batch_id, raw_response)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                ON CONFLICT (date, symbol) DO UPDATE SET
                    action = EXCLUDED.action, confidence = EXCLUDED.confidence,
                    reasoning = EXCLUDED.reasoning, wait_for = EXCLUDED.wait_for,
                    wait_days = EXCLUDED.wait_days, entry_low = EXCLUDED.entry_low,
                    entry_high = EXCLUDED.entry_high, sl = EXCLUDED.sl,
                    t1 = EXCLUDED.t1, t2 = EXCLUDED.t2,
                    risk_factors = EXCLUDED.risk_factors, catalysts = EXCLUDED.catalysts,
                    score = EXCLUDED.score, batch_id = EXCLUDED.batch_id,
                    raw_response = EXCLUDED.raw_response
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
                batch_id,
                raw if saved == 0 else None,  # Store raw only for first stock in batch
            ))
            saved += 1
        except Exception as e:
            logger.error(f"Store LLM result {symbol}: {e}")
    conn.commit()
    conn.close()
    logger.info(f"Stored {saved} LLM results (batch {batch_id})")
    return saved


def run_llm_analysis(date_str: str) -> list[dict]:
    """Stage 1: Batch all A-category stocks through Claude LLM."""
    stocks = load_algo_analysis(date_str)
    if not stocks:
        logger.warning("No A-category stocks found for LLM analysis")
        return []

    market = load_market_context()
    feedback = load_accuracy_feedback()

    batches = [stocks[i:i + LLM_BATCH_SIZE] for i in range(0, len(stocks), LLM_BATCH_SIZE)]
    total_batches = len(batches)
    logger.info(f"LLM analysis: {len(stocks)} stocks in {total_batches} batches")

    all_results = []
    for i, batch in enumerate(batches, 1):
        prompt = build_llm_prompt(batch, market, feedback, i, total_batches)
        raw = call_claude(prompt)
        if not raw:
            logger.error(f"Batch {i}: no response")
            continue

        parsed = parse_json_response(raw)
        if not parsed:
            logger.error(f"Batch {i}: failed to parse")
            continue

        if isinstance(parsed, dict):
            parsed = [parsed]

        stored = store_llm_results(date_str, parsed, i, raw)
        all_results.extend(parsed)
        logger.info(f"Batch {i}/{total_batches}: {stored} stocks stored")

        if i < total_batches:
            time.sleep(5)

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


def run_judge_analysis(date_str: str, llm_results: list[dict]):
    """Stage 2: Judge compares algo vs LLM for each stock."""
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
        return

    market = load_market_context()
    batches = [pairs[i:i + JUDGE_BATCH_SIZE] for i in range(0, len(pairs), JUDGE_BATCH_SIZE)]
    total = len(batches)
    logger.info(f"Judge analysis: {len(pairs)} pairs in {total} batches")

    for i, batch in enumerate(batches, 1):
        prompt = build_judge_prompt(batch, market, i, total)
        raw = call_claude(prompt)
        if not raw:
            logger.error(f"Judge batch {i}: no response")
            continue

        parsed = parse_json_response(raw)
        if not parsed:
            logger.error(f"Judge batch {i}: failed to parse")
            continue

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
            time.sleep(5)


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


# ─── Main ───


def run():
    """Main entry point: Stage 1 → Stage 2 → Stage 3."""
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
    run_judge_analysis(date_str, llm_results)

    # Stage 3: Snapshot predictions
    snapshot_predictions(date_str)

    logger.info("=== LLM Daily Analyzer complete ===")


if __name__ == "__main__":
    run()
