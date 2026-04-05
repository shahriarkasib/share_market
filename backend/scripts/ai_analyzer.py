#!/usr/bin/env python3
"""
AI Stock Analysis Pipeline — V2

Feeds full indicator data to Claude (via Max CLI) and stores structured
JSON results in the ai_analysis table.

Usage:
    ./venv/bin/python3 scripts/ai_analyzer.py                    # All A+B stocks
    ./venv/bin/python3 scripts/ai_analyzer.py --symbols GP,ACMELAB
    ./venv/bin/python3 scripts/ai_analyzer.py --batch-size 3     # 3 stocks per Claude call
    ./venv/bin/python3 scripts/ai_analyzer.py --dry-run           # Print prompt, don't call Claude
"""

import sys
import os
import json
import argparse
import logging
import re
import subprocess
import tempfile
import time
from datetime import datetime

import psycopg2
import psycopg2.extras

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from config import DATABASE_URL

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%H:%M:%S",
)
log = logging.getLogger(__name__)

CLAUDE_TIMEOUT = 600  # 10 min per call
PROMPT_TEMPLATE_PATH = os.path.join(
    os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))),
    "new_instructions", "DSE_Final_Claude_Prompt.md",
)

# Portfolio hardcoded (from memory) — will be overridden by holdings table if populated
PORTFOLIO = {
    "FINEFOODS": {"qty": 200, "avg_cost": 485.84},
    "SPCERAMICS": {"qty": 2500, "avg_cost": 21.08},
    "ACMELAB": {"qty": 650, "avg_cost": 76.91},
    "BXPHARMA": {"qty": 250, "avg_cost": 112.45},
    "HWAWELLTEX": {"qty": 500, "avg_cost": 42.17},
    "RUNNERAUTO": {"qty": 2600, "avg_cost": 40.92},
}


# ---------------------------------------------------------------------------
# Data loaders
# ---------------------------------------------------------------------------

def get_conn():
    return psycopg2.connect(DATABASE_URL, cursor_factory=psycopg2.extras.RealDictCursor)


def load_indicators_csv(conn, symbol: str, timeframe: str, limit: int) -> str:
    """Load indicators as CSV string for prompt."""
    cur = conn.cursor()
    cur.execute("""
        SELECT * FROM stock_indicators
        WHERE symbol = %s AND timeframe = %s
        ORDER BY date DESC LIMIT %s
    """, (symbol, timeframe, limit))
    rows = cur.fetchall()
    cur.close()
    if not rows:
        return "No data"

    rows = list(reversed(rows))  # oldest first
    # Select key columns (skip symbol, timeframe, computed_at to save tokens)
    skip = {"symbol", "timeframe", "computed_at"}
    cols = [k for k in rows[0].keys() if k not in skip]
    lines = [",".join(cols)]
    for r in rows:
        vals = []
        for c in cols:
            v = r[c]
            if v is None:
                vals.append("")
            elif isinstance(v, bool):
                vals.append("1" if v else "0")
            elif isinstance(v, float):
                vals.append(f"{v:.4f}" if abs(v) < 1 else f"{v:.2f}")
            else:
                vals.append(str(v))
        lines.append(",".join(vals))
    return "\n".join(lines)


def load_fundamentals(conn, symbol: str) -> str:
    """Load fundamentals as JSON string."""
    cur = conn.cursor()
    cur.execute("SELECT * FROM fundamentals WHERE symbol = %s", (symbol,))
    row = cur.fetchone()
    cur.close()
    if not row:
        return "{}"
    d = {k: v for k, v in dict(row).items() if v is not None}
    return json.dumps(d, default=str, indent=2)


def load_dsex_data(conn, days: int = 60) -> str:
    """Load recent DSEX index data."""
    cur = conn.cursor()
    cur.execute("""
        SELECT date, dsex_index, total_volume, total_value, total_trade
        FROM dsex_history
        WHERE dsex_index > 0
        ORDER BY date DESC LIMIT %s
    """, (days,))
    rows = cur.fetchall()
    cur.close()
    if not rows:
        return "No DSEX data"
    rows = list(reversed(rows))
    lines = ["date,dsex,volume,value_cr,trades"]
    for r in rows:
        lines.append(f"{r['date']},{r['dsex_index']},{r['total_volume'] or 0},{r['total_value'] or 0},{r['total_trade'] or 0}")
    return "\n".join(lines)


def load_market_breadth(conn) -> dict:
    """Load current market summary."""
    cur = conn.cursor()
    cur.execute("SELECT * FROM market_summary WHERE id = 1")
    row = cur.fetchone()
    cur.close()
    if not row:
        return {"advances": 0, "declines": 0, "unchanged": 0, "turnover": 0}
    return {
        "advances": row.get("advances", 0),
        "declines": row.get("declines", 0),
        "unchanged": row.get("unchanged", 0),
        "turnover": round((row.get("total_value", 0) or 0) / 100, 1),  # crore
    }


def load_news(conn, symbol: str, days: int = 30) -> str:
    """Load recent news for a symbol."""
    cur = conn.cursor()
    cur.execute("""
        SELECT date, title, source
        FROM market_news
        WHERE (affected_symbols LIKE %s OR title LIKE %s)
          AND date >= CURRENT_DATE - INTERVAL '%s days'
        ORDER BY date DESC LIMIT 10
    """, (f"%{symbol}%", f"%{symbol}%", days))
    rows = cur.fetchall()
    cur.close()
    if not rows:
        return "No recent news"
    items = []
    for r in rows:
        items.append(f"[{r['date']}] {r['title'][:200]}")
    return "\n".join(items)


def load_sector_context(conn, symbol: str) -> str:
    """Load sector performance context."""
    cur = conn.cursor()
    cur.execute("SELECT sector FROM fundamentals WHERE symbol = %s", (symbol,))
    row = cur.fetchone()
    cur.close()
    if not row or not row.get("sector"):
        return "Sector unknown"

    sector = row["sector"]
    cur = conn.cursor()
    # Get sector peers' latest performance
    cur.execute("""
        SELECT f.symbol, si.close, si.rsi_14, si.cmf_20, si.adx_14, si.chg_20d
        FROM stock_indicators si
        JOIN fundamentals f ON si.symbol = f.symbol
        WHERE f.sector = %s AND si.timeframe = 'daily'
          AND si.date = (SELECT MAX(date) FROM stock_indicators WHERE timeframe = 'daily')
        ORDER BY si.close DESC
        LIMIT 20
    """, (sector,))
    peers = cur.fetchall()
    cur.close()

    if not peers:
        return f"Sector: {sector} (no peer data)"

    lines = [f"Sector: {sector} ({len(peers)} peers)"]
    lines.append("symbol,close,rsi,cmf,adx,chg_20d")
    for p in peers:
        lines.append(f"{p['symbol']},{p['close']},{p['rsi_14']:.1f},{p['cmf_20']:.3f},{p['adx_14']:.1f},{p['chg_20d']:.1f}")
    return "\n".join(lines)


def get_position_data(symbol: str) -> str:
    """Get portfolio position info if held."""
    if symbol in PORTFOLIO:
        p = PORTFOLIO[symbol]
        return f"HELD: {p['qty']} shares @ avg cost {p['avg_cost']}"
    return "Not currently held"


# ---------------------------------------------------------------------------
# Prompt builder
# ---------------------------------------------------------------------------

def build_prompt(conn, symbols: list[str]) -> str:
    """Build the full analysis prompt for one or more symbols."""
    # Load the prompt template
    if os.path.exists(PROMPT_TEMPLATE_PATH):
        with open(PROMPT_TEMPLATE_PATH) as f:
            template = f.read()
        # Extract just the prompt part (before ### DATA PROVIDED)
        idx = template.find("### DATA PROVIDED:")
        if idx > 0:
            system_prompt = template[:idx].strip()
        else:
            system_prompt = template.strip()
    else:
        system_prompt = "You are a DSE stock analyst. Analyze the following stock data and return JSON."

    breadth = load_market_breadth(conn)
    dsex_csv = load_dsex_data(conn, 60)

    parts = [system_prompt, ""]

    for symbol in symbols:
        daily_csv = load_indicators_csv(conn, symbol, "daily", 500)
        weekly_csv = load_indicators_csv(conn, symbol, "weekly", 104)
        monthly_csv = load_indicators_csv(conn, symbol, "monthly", 36)
        fundamentals = load_fundamentals(conn, symbol)
        news = load_news(conn, symbol)
        sector = load_sector_context(conn, symbol)
        position = get_position_data(symbol)

        n_daily = daily_csv.count("\n")
        n_weekly = weekly_csv.count("\n")
        n_monthly = monthly_csv.count("\n")

        parts.append(f"\n{'='*60}")
        parts.append(f"STOCK: {symbol}")
        parts.append(f"{'='*60}")
        parts.append(f"\n=== DAILY DATA ({n_daily} trading days) ===")
        parts.append(daily_csv)
        parts.append(f"\n=== WEEKLY DATA ({n_weekly} weeks) ===")
        parts.append(weekly_csv)
        parts.append(f"\n=== MONTHLY DATA ({n_monthly} months) ===")
        parts.append(monthly_csv)
        parts.append(f"\n=== FUNDAMENTALS ===")
        parts.append(fundamentals)
        parts.append(f"\n=== RECENT NEWS ===")
        parts.append(news)
        parts.append(f"\n=== SECTOR CONTEXT ===")
        parts.append(sector)
        parts.append(f"\n=== MY POSITION ===")
        parts.append(position)

    parts.append(f"\n=== DSEX MARKET DATA (last 60 days) ===")
    parts.append(dsex_csv)
    parts.append(f"\n=== MARKET BREADTH TODAY ===")
    parts.append(f"Advances: {breadth['advances']} | Declines: {breadth['declines']} | Unchanged: {breadth['unchanged']}")
    parts.append(f"Turnover: {breadth['turnover']} crore")

    # Strict output format instruction at the end (where Claude pays most attention)
    output_schema = """
Now analyze completely. Return ONLY a JSON object with EXACTLY these top-level keys (use these EXACT key names):

{
  "ticker": "SYMBOL",
  "overall_signal": "BUY | HOLD | SELL | AVOID | WATCH",
  "signal_strength": "STRONG | MEDIUM | WEAK",
  "confidence": "HIGH | MEDIUM | LOW",
  "classification": "ENTRY_ZONE | READY | APPROACHING | BUILDING | WATCHING",
  "position_type": "STRONG_TREND | TREND | EMERGING | RANGE | CHOPPY",
  "one_liner": "One sentence summary",
  "score": {
    "overall": 0, "money_flow": 0, "momentum": 0,
    "price_action": 0, "volatility": 0, "fundamentals": 0, "news_sentiment": 0
  },
  "action": {
    "for_new_buyer": "...", "for_holder": "...",
    "entry_range": "low-high", "stop_loss": 0, "stop_loss_method": "...",
    "target_1": 0, "target_2": 0
  },
  "support_resistance": { "immediate_support": 0, "major_support": 0, "immediate_resistance": 0, "major_resistance": 0 },
  "risk_factors": ["..."],
  "favorable_factors": ["..."],
  "ai_reasoning": "Full detailed analysis here"
}

CRITICAL: The key MUST be "ticker" (not "symbol" or "stock"). The signal MUST be "overall_signal" (not "signal"). Scores are 0-100 integers. All prices in BDT with 0.1 precision. No markdown wrapping. Raw JSON only."""

    if len(symbols) == 1:
        parts.append(output_schema)
    else:
        parts.append(f"\nAnalyze these {len(symbols)} stocks: {', '.join(symbols)}.")
        parts.append(output_schema.replace("a JSON object", "a JSON ARRAY of objects, one per stock"))

    return "\n".join(parts)


# ---------------------------------------------------------------------------
# Claude CLI caller
# ---------------------------------------------------------------------------

def call_claude(prompt: str, timeout: int = CLAUDE_TIMEOUT) -> str:
    """Call Claude via Max CLI (claude -p)."""
    log.info(f"Calling Claude ({len(prompt):,} chars, ~{len(prompt)//4:,} tokens)...")

    prompt_file = None
    try:
        prompt_file = tempfile.NamedTemporaryFile(
            mode="w", suffix=".txt", delete=False, prefix="ai_prompt_"
        )
        prompt_file.write(prompt)
        prompt_file.close()

        # Source bash env for CLAUDE_CODE_OAUTH_TOKEN
        env = os.environ.copy()
        if not env.get("CLAUDE_CODE_OAUTH_TOKEN"):
            bashrc = os.path.expanduser("~/.bashrc")
            if os.path.exists(bashrc):
                with open(bashrc) as f:
                    for line in f:
                        if "CLAUDE_CODE_OAUTH_TOKEN=" in line and line.strip().startswith("export"):
                            token = line.split('"')[1] if '"' in line else line.split("=", 1)[1].strip()
                            env["CLAUDE_CODE_OAUTH_TOKEN"] = token
                            break

        cmd = f'cat "{prompt_file.name}" | claude -p --model opus --max-turns 1'
        result = subprocess.run(
            ["bash", "-c", cmd],
            capture_output=True, text=True, timeout=timeout, env=env,
        )

        if result.returncode != 0:
            log.error(f"Claude CLI error (exit {result.returncode}): {(result.stderr or result.stdout)[:300]}")
            return ""

        resp = result.stdout.strip()
        if "Not logged in" in resp or "Please run /login" in resp:
            log.error("Claude CLI not authenticated")
            return ""
        if not resp:
            log.warning("Claude CLI returned empty response")
            return ""

        log.info(f"Claude response: {len(resp):,} chars")
        return resp

    except subprocess.TimeoutExpired:
        log.error(f"Claude CLI timed out ({timeout}s)")
        return ""
    except Exception as e:
        log.error(f"Claude CLI error: {e}")
        return ""
    finally:
        if prompt_file and os.path.exists(prompt_file.name):
            os.unlink(prompt_file.name)


# ---------------------------------------------------------------------------
# JSON parser
# ---------------------------------------------------------------------------

def extract_json(text: str) -> list[dict]:
    """Extract JSON object(s) from Claude's response."""
    text = text.strip()

    # Remove markdown code fences
    text = re.sub(r'^```json\s*', '', text)
    text = re.sub(r'^```\s*', '', text)
    text = re.sub(r'\s*```$', '', text)
    text = text.strip()

    try:
        parsed = json.loads(text)
        if isinstance(parsed, list):
            return parsed
        if isinstance(parsed, dict):
            return [parsed]
    except json.JSONDecodeError:
        pass

    # Try to find JSON in the response
    # Look for array
    match = re.search(r'\[[\s\S]*\]', text)
    if match:
        try:
            return json.loads(match.group())
        except json.JSONDecodeError:
            pass

    # Look for object
    match = re.search(r'\{[\s\S]*\}', text)
    if match:
        try:
            return [json.loads(match.group())]
        except json.JSONDecodeError:
            pass

    log.error(f"Failed to parse JSON from response: {text[:200]}")
    return []


# ---------------------------------------------------------------------------
# Store results
# ---------------------------------------------------------------------------

def _num(v):
    """Convert value to float or None."""
    if v is None:
        return None
    try:
        return float(v)
    except (ValueError, TypeError):
        return None


def store_analysis(conn, result: dict) -> bool:
    """Store one stock's analysis in ai_analysis table."""
    # Extract ticker — prompt enforces "ticker" but handle fallbacks defensively
    symbol = str(result.get("ticker") or result.get("symbol") or result.get("stock") or "").upper().strip()
    if not symbol:
        log.warning(f"No ticker in result. Keys: {list(result.keys())[:8]}")
        return False

    today = datetime.now().strftime("%Y-%m-%d")
    score = result.get("score") or {}
    action = result.get("action") or {}

    # Parse entry range from action
    entry_range = action.get("entry_range", "")
    entry_low = entry_high = None
    if isinstance(entry_range, str) and "-" in entry_range:
        nums = re.findall(r'[\d.]+', entry_range)
        if len(nums) >= 2:
            entry_low, entry_high = float(nums[0]), float(nums[1])
    elif isinstance(entry_range, (int, float)):
        entry_low = entry_high = float(entry_range)

    # Normalize fields — prompt enforces standard names but handle non-compliance
    overall_signal = str(result.get("overall_signal") or result.get("signal") or "WATCH").upper().strip()
    signal_strength = result.get("signal_strength")
    if isinstance(signal_strength, str):
        signal_strength = signal_strength.upper()

    confidence_val = result.get("confidence")
    if isinstance(confidence_val, (int, float)):
        confidence_str = "HIGH" if confidence_val >= 70 else "MEDIUM" if confidence_val >= 40 else "LOW"
    elif isinstance(confidence_val, str):
        confidence_str = confidence_val.upper()
    else:
        confidence_str = None

    classification = result.get("classification")
    position_type = result.get("position_type")
    one_liner = result.get("one_liner") or ""

    score_overall = score.get("overall") if isinstance(score, dict) else None
    if isinstance(score_overall, float):
        score_overall = int(score_overall)

    sl = _num(action.get("stop_loss") or result.get("stop_loss"))
    sl_method = action.get("stop_loss_method")
    t1 = _num(action.get("target_1"))
    t2 = _num(action.get("target_2"))
    for_new = action.get("for_new_buyer") or ""
    for_hold = action.get("for_holder") or ""

    cur = conn.cursor()

    cur.execute("""
        INSERT INTO ai_analysis (
            symbol, date, overall_signal, signal_strength, confidence,
            classification, position_type,
            score_overall, score_money_flow, score_momentum,
            score_price_action, score_volatility, score_fundamentals, score_news,
            one_liner,
            entry_low, entry_high, stop_loss, stop_loss_method,
            target_1, target_2, for_new_buyer, for_holder,
            analysis_json, model
        ) VALUES (
            %s, %s, %s, %s, %s, %s, %s,
            %s, %s, %s, %s, %s, %s, %s,
            %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s
        )
        ON CONFLICT (symbol, date) DO UPDATE SET
            overall_signal = EXCLUDED.overall_signal,
            signal_strength = EXCLUDED.signal_strength,
            confidence = EXCLUDED.confidence,
            classification = EXCLUDED.classification,
            position_type = EXCLUDED.position_type,
            score_overall = EXCLUDED.score_overall,
            score_money_flow = EXCLUDED.score_money_flow,
            score_momentum = EXCLUDED.score_momentum,
            score_price_action = EXCLUDED.score_price_action,
            score_volatility = EXCLUDED.score_volatility,
            score_fundamentals = EXCLUDED.score_fundamentals,
            score_news = EXCLUDED.score_news,
            one_liner = EXCLUDED.one_liner,
            entry_low = EXCLUDED.entry_low,
            entry_high = EXCLUDED.entry_high,
            stop_loss = EXCLUDED.stop_loss,
            stop_loss_method = EXCLUDED.stop_loss_method,
            target_1 = EXCLUDED.target_1,
            target_2 = EXCLUDED.target_2,
            for_new_buyer = EXCLUDED.for_new_buyer,
            for_holder = EXCLUDED.for_holder,
            analysis_json = EXCLUDED.analysis_json,
            model = EXCLUDED.model,
            created_at = NOW()
    """, (
        symbol, today,
        overall_signal,
        signal_strength,
        confidence_str,
        classification,
        position_type,
        score_overall,
        score.get("money_flow"),
        score.get("momentum"),
        score.get("price_action"),
        score.get("volatility"),
        score.get("fundamentals"),
        score.get("news_sentiment"),
        one_liner,
        entry_low, entry_high,
        sl, sl_method,
        t1, t2,
        for_new, for_hold,
        json.dumps(result),
        "claude-opus",
    ))
    conn.commit()
    log.info(f"  Stored: {symbol} → {overall_signal} (confidence: {confidence_str}, score: {score_overall})")
    return True


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def get_active_symbols(conn, categories: list[str]) -> list[str]:
    """Get A+B symbols with recent data."""
    cur = conn.cursor()
    cur.execute("""
        SELECT DISTINCT si.symbol
        FROM stock_indicators si
        JOIN fundamentals f ON si.symbol = f.symbol
        WHERE si.timeframe = 'daily'
          AND si.date = (SELECT MAX(date) FROM stock_indicators WHERE timeframe = 'daily')
          AND f.category IN %s
        ORDER BY si.symbol
    """, (tuple(categories),))
    symbols = [r["symbol"] for r in cur.fetchall()]
    cur.close()
    return symbols


def main():
    parser = argparse.ArgumentParser(description="AI Stock Analyzer (Claude Max CLI)")
    parser.add_argument("--symbols", type=str, help="Comma-separated symbols")
    parser.add_argument("--batch-size", type=int, default=1,
                        help="Stocks per Claude call (default: 1)")
    parser.add_argument("--categories", type=str, default="A,B")
    parser.add_argument("--dry-run", action="store_true",
                        help="Print prompt stats without calling Claude")
    parser.add_argument("--max-stocks", type=int, default=0,
                        help="Max stocks to analyze (0=all)")
    args = parser.parse_args()

    conn = get_conn()

    if args.symbols:
        symbols = [s.strip() for s in args.symbols.split(",")]
    else:
        categories = [c.strip() for c in args.categories.split(",")]
        symbols = get_active_symbols(conn, categories)

    if args.max_stocks > 0:
        symbols = symbols[:args.max_stocks]

    log.info(f"Analyzing {len(symbols)} stocks, batch_size={args.batch_size}")

    success = 0
    failed = 0

    # Process in batches
    for i in range(0, len(symbols), args.batch_size):
        batch = symbols[i:i + args.batch_size]
        batch_num = i // args.batch_size + 1
        total_batches = (len(symbols) + args.batch_size - 1) // args.batch_size

        log.info(f"[Batch {batch_num}/{total_batches}] {', '.join(batch)}")

        prompt = build_prompt(conn, batch)

        if args.dry_run:
            log.info(f"  Prompt: {len(prompt):,} chars (~{len(prompt)//4:,} tokens)")
            log.info(f"  First 500 chars: {prompt[:500]}")
            continue

        response = call_claude(prompt)
        if not response:
            failed += len(batch)
            log.error(f"  No response for batch")
            continue

        results = extract_json(response)
        if not results:
            failed += len(batch)
            log.error(f"  Failed to parse JSON")
            # Save raw response for debugging
            debug_path = f"/tmp/ai_debug_{batch[0]}_{datetime.now().strftime('%H%M%S')}.txt"
            with open(debug_path, "w") as f:
                f.write(response)
            log.error(f"  Raw response saved to {debug_path}")
            continue

        for result in results:
            try:
                if store_analysis(conn, result):
                    success += 1
                else:
                    failed += 1
            except Exception as e:
                failed += 1
                log.error(f"  Store failed: {e}")
                conn.rollback()

        # Rate limiting between batches
        if i + args.batch_size < len(symbols):
            time.sleep(2)

    conn.close()
    log.info(f"Done. {success} analyzed, {failed} failed out of {len(symbols)} stocks")


if __name__ == "__main__":
    main()
