#!/usr/bin/env python3
"""LLM-powered market analysis scanner for GCP VM.

Connects to Supabase directly, reads daily analysis + live scan data,
sends to Claude via `claude -p` for deep analysis, stores results back.

Usage:
    # Single run
    python3 llm_scanner.py

    # Cron job (every 10 min during market hours, Sun-Thu)
    # 55 9-14 * * 0-4 cd /path/to/backend && python3 scripts/llm_scanner.py >> /tmp/llm_scanner.log 2>&1

Environment:
    DATABASE_URL  — Supabase PostgreSQL connection string (pooler)
    DATABASE_URL_DIRECT — Supabase direct connection (for DDL)
"""

import json
import logging
import os
import subprocess
import sys
from datetime import datetime

import psycopg2
import psycopg2.extras
import pytz

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%H:%M:%S",
)
logger = logging.getLogger(__name__)

DSE_TZ = pytz.timezone("Asia/Dhaka")

# Supabase connection
DATABASE_URL = os.getenv(
    "DATABASE_URL",
    "postgresql://postgres.iihlezpkpllacztoaguc:160021062Ss%23%23@aws-1-ap-southeast-1.pooler.supabase.com:6543/postgres",
)
DATABASE_URL_DIRECT = os.getenv(
    "DATABASE_URL_DIRECT",
    "postgresql://postgres.iihlezpkpllacztoaguc:160021062Ss%23%23@aws-1-ap-southeast-1.pooler.supabase.com:5432/postgres",
)


def get_conn():
    """Get a psycopg2 connection with RealDictCursor."""
    conn = psycopg2.connect(DATABASE_URL, cursor_factory=psycopg2.extras.RealDictCursor)
    conn.autocommit = False
    return conn


def ensure_table():
    """Create the llm_scan_results table if it doesn't exist."""
    conn = psycopg2.connect(DATABASE_URL_DIRECT)
    conn.autocommit = True
    cur = conn.cursor()
    cur.execute("""
        CREATE TABLE IF NOT EXISTS llm_scan_results (
            id SERIAL PRIMARY KEY,
            date DATE NOT NULL,
            scan_time TIMESTAMP NOT NULL,
            analysis_type TEXT NOT NULL,
            symbol TEXT,
            recommendation TEXT,
            confidence TEXT,
            reasoning TEXT,
            key_insights TEXT,
            risk_factors TEXT,
            raw_response TEXT,
            created_at TIMESTAMP DEFAULT NOW(),
            UNIQUE(date, scan_time, analysis_type, symbol)
        )
    """)
    cur.execute("""
        CREATE INDEX IF NOT EXISTS idx_llm_scan_date ON llm_scan_results(date)
    """)
    cur.close()
    conn.close()
    logger.info("llm_scan_results table ready")


def load_analysis_data() -> dict:
    """Load daily analysis + live prices for BUY stocks from Supabase."""
    conn = get_conn()
    cur = conn.cursor()

    # Get latest analysis date
    cur.execute("SELECT MAX(date) as max_date FROM daily_analysis")
    row = cur.fetchone()
    analysis_date = str(row["max_date"]) if row and row["max_date"] else datetime.now(DSE_TZ).strftime("%Y-%m-%d")

    # Load BUY-type stocks with live prices
    cur.execute("""
        SELECT da.symbol, da.action, da.entry_low, da.entry_high,
               da.sl, da.t1, da.t2, da.score, da.category,
               da.rsi, da.stoch_rsi, da.macd_status, da.macd_hist,
               da.bb_pct, da.atr, da.atr_pct, da.volatility,
               da.risk_pct, da.reward_pct, da.reasoning,
               da.support, da.resistance, da.trend_50d,
               da.vol_ratio, da.ltp as analysis_ltp,
               lp.ltp AS live_ltp, lp.change_pct AS live_change_pct,
               lp.volume AS live_volume, lp.high AS live_high, lp.low AS live_low,
               f.sector, f.pe_ratio, f.eps, f.market_cap
        FROM daily_analysis da
        JOIN live_prices lp ON da.symbol = lp.symbol
        LEFT JOIN fundamentals f ON da.symbol = f.symbol
        WHERE da.date = %s
          AND da.action LIKE 'BUY%%'
        ORDER BY da.score DESC
    """, (analysis_date,))
    buy_stocks = cur.fetchall()

    # Load market summary
    cur.execute("SELECT * FROM market_summary WHERE id = 1")
    market = cur.fetchone()

    # Load overall analysis summary
    cur.execute("""
        SELECT action, COUNT(*) as count
        FROM daily_analysis WHERE date = %s
        GROUP BY action ORDER BY count DESC
    """, (analysis_date,))
    summary_rows = cur.fetchall()

    # Load past decision accuracy (feedback loop)
    cur.execute("""
        SELECT COUNT(*) as total,
               COUNT(CASE WHEN outcome = 'CORRECT' THEN 1 END) as correct,
               COUNT(CASE WHEN outcome = 'WRONG' THEN 1 END) as wrong,
               AVG(return_t2_pct) as avg_return
        FROM scan_decisions
        WHERE outcome IS NOT NULL
          AND recommendation IN ('BUY NOW', 'READY', 'ACCUMULATE')
          AND date >= CURRENT_DATE - INTERVAL '30 days'
    """)
    accuracy_row = cur.fetchone()

    # Load worst recent calls to learn from
    cur.execute("""
        SELECT symbol, date, recommendation, live_ltp, actual_t2,
               return_t2_pct, t2_risk, reasoning
        FROM scan_decisions
        WHERE outcome = 'WRONG' AND return_t2_pct IS NOT NULL
          AND date >= CURRENT_DATE - INTERVAL '14 days'
        ORDER BY return_t2_pct ASC LIMIT 5
    """)
    worst_calls = cur.fetchall()

    # Load best recent calls
    cur.execute("""
        SELECT symbol, date, recommendation, live_ltp, actual_t2, return_t2_pct
        FROM scan_decisions
        WHERE outcome = 'CORRECT' AND return_t2_pct IS NOT NULL
          AND date >= CURRENT_DATE - INTERVAL '14 days'
        ORDER BY return_t2_pct DESC LIMIT 5
    """)
    best_calls = cur.fetchall()

    conn.close()

    return {
        "date": analysis_date,
        "buy_stocks": [dict(r) for r in buy_stocks],
        "market": dict(market) if market else {},
        "summary": {r["action"]: r["count"] for r in summary_rows},
        "accuracy": dict(accuracy_row) if accuracy_row else {},
        "worst_calls": [dict(r) for r in worst_calls],
        "best_calls": [dict(r) for r in best_calls],
    }


def format_prompt(data: dict) -> str:
    """Format analysis data into a prompt for Claude."""
    now = datetime.now(DSE_TZ)
    market = data["market"]
    buy_stocks = data["buy_stocks"]
    summary = data["summary"]

    # Build stock summaries
    stock_lines = []
    for s in buy_stocks[:30]:  # Top 30 by score to keep prompt manageable
        ltp = float(s.get("live_ltp") or 0)
        entry_low = float(s.get("entry_low") or 0)
        entry_high = float(s.get("entry_high") or 0)
        sl = float(s.get("sl") or 0)
        t1 = float(s.get("t1") or 0)
        t2 = float(s.get("t2") or 0)
        change = float(s.get("live_change_pct") or 0)

        # Compute simple status
        if sl > 0 and ltp <= sl:
            status = "SL_HIT"
        elif t2 > 0 and ltp >= t2:
            status = "T2_HIT"
        elif t1 > 0 and ltp >= t1:
            status = "T1_HIT"
        elif entry_low > 0 and entry_high > 0 and entry_low <= ltp <= entry_high:
            status = "ENTRY_ZONE"
        elif entry_low > 0 and ltp < entry_low:
            status = "BELOW_ENTRY"
        elif entry_high > 0 and ltp <= entry_high * 1.02:
            status = "APPROACHING"
        else:
            status = "WATCHING"

        stock_lines.append(
            f"- {s['symbol']} ({s.get('sector','?')}, Cat:{s.get('category','?')}) | "
            f"Action: {s['action']} | Score: {s.get('score',0):.0f} | "
            f"LTP: {ltp:.1f} ({change:+.1f}%) | "
            f"Entry: {entry_low:.1f}-{entry_high:.1f} | SL: {sl:.1f} | T1: {t1:.1f} | T2: {t2:.1f} | "
            f"Status: {status} | "
            f"RSI: {s.get('rsi',0):.1f} | MACD: {s.get('macd_status','')} | "
            f"Risk: {s.get('risk_pct',0):.1f}% | Reward: {s.get('reward_pct',0):.1f}% | "
            f"VolRatio: {s.get('vol_ratio',0):.1f}x | "
            f"Rule reason: {(s.get('reasoning',''))[:100]}"
        )

    dsex = market.get("dsex_index", 0)
    dsex_chg = market.get("dsex_change_pct", 0)

    # Build feedback section from past decisions
    accuracy = data.get("accuracy", {})
    worst = data.get("worst_calls", [])
    best = data.get("best_calls", [])

    feedback_lines = []
    total_verified = int(accuracy.get("total") or 0)
    if total_verified > 0:
        correct = int(accuracy.get("correct") or 0)
        wrong = int(accuracy.get("wrong") or 0)
        avg_ret = float(accuracy.get("avg_return") or 0)
        acc_pct = round(correct / total_verified * 100, 1) if total_verified > 0 else 0
        feedback_lines.append(
            f"Past 30 days: {total_verified} BUY decisions verified — "
            f"{acc_pct}% correct ({correct} correct, {wrong} wrong), "
            f"avg T+2 return: {avg_ret:+.2f}%"
        )
        if worst:
            feedback_lines.append("WORST recent calls (learn from these):")
            for w in worst:
                feedback_lines.append(
                    f"  - {w['symbol']} on {w['date']}: recommended {w['recommendation']} at {w['live_ltp']}, "
                    f"T+2 actual: {w.get('actual_t2', '?')} ({w.get('return_t2_pct', 0):+.1f}%), "
                    f"T+2 risk was: {w.get('t2_risk', '?')}, "
                    f"reason: {(w.get('reasoning', ''))[:80]}"
                )
        if best:
            feedback_lines.append("BEST recent calls:")
            for b in best:
                feedback_lines.append(
                    f"  - {b['symbol']} on {b['date']}: {b['recommendation']} at {b['live_ltp']}, "
                    f"T+2: {b.get('actual_t2', '?')} ({b.get('return_t2_pct', 0):+.1f}%)"
                )

    feedback_section = ""
    if feedback_lines:
        feedback_section = "\n## Past Decision Feedback (Learn From This)\n" + "\n".join(feedback_lines) + "\n"

    prompt = f"""You are a DSE (Dhaka Stock Exchange) trading analyst. Analyze the following market data and BUY-signal stocks.

CRITICAL: All recommendations must consider T+2 settlement. The investor CANNOT sell for 2 trading days after buying. If a stock is likely to peak today and drop tomorrow, that is a BAD buy.

## Market Context
- Date: {data['date']}, Time: {now.strftime('%H:%M')} BST
- DSEX Index: {dsex:.1f} ({dsex_chg:+.2f}%)
- Market Status: {market.get('market_status', 'UNKNOWN')}
- Advances: {market.get('advances', 0)} | Declines: {market.get('declines', 0)} | Unchanged: {market.get('unchanged', 0)}
- Total Volume: {market.get('total_volume', 0):,} | Turnover: {market.get('total_value', 0):,.0f}
{feedback_section}
## Analysis Summary
Total stocks analyzed: {sum(summary.values())}
{chr(10).join(f"- {action}: {count}" for action, count in summary.items())}

## Top {len(stock_lines)} BUY Signal Stocks (sorted by composite score)
{chr(10).join(stock_lines)}

## Your Task
Provide a JSON response with this EXACT structure:
{{
  "market_outlook": {{
    "sentiment": "BULLISH|BEARISH|NEUTRAL|CAUTIOUS",
    "summary": "2-3 sentence overall market assessment",
    "key_risks": ["risk1", "risk2"]
  }},
  "top_picks": [
    {{
      "symbol": "SYMBOL",
      "recommendation": "STRONG_BUY|BUY|WAIT|AVOID",
      "confidence": "HIGH|MEDIUM|LOW",
      "entry_strategy": "Specific entry advice with prices",
      "reasoning": "2-3 sentence deep analysis explaining WHY",
      "risk_note": "Key risk to watch"
    }}
  ],
  "stocks_to_avoid_today": [
    {{
      "symbol": "SYMBOL",
      "reason": "Why to avoid despite BUY signal"
    }}
  ],
  "sector_insights": "1-2 sentence observation about sectors",
  "timing_advice": "When to enter today, any patterns to watch"
}}

Rules:
1. Select max 5-7 top_picks — the BEST opportunities right now
2. Be specific with entry prices, not vague
3. Consider RSI overbought (>70) or oversold (<30) context
4. Flag stocks where MACD is bearish despite BUY signal
5. Consider volume ratio — low volume BUY signals are weaker
6. If a stock is at SL_HIT or T1_HIT, note that
7. Prefer ENTRY_ZONE and APPROACHING stocks over WATCHING
8. Bangladesh market context: T+2 settlement, tick size 0.10 BDT
9. Return ONLY valid JSON, no markdown fences, no extra text"""

    return prompt


def call_claude(prompt: str) -> str:
    """Call Claude via `claude -p` CLI and return the response."""
    logger.info(f"Calling Claude (prompt: {len(prompt)} chars)...")
    try:
        result = subprocess.run(
            ["claude", "-p", prompt],
            capture_output=True,
            text=True,
            timeout=120,
        )
        if result.returncode != 0:
            logger.error(f"Claude CLI error: {result.stderr}")
            return ""
        response = result.stdout.strip()
        logger.info(f"Claude response: {len(response)} chars")
        return response
    except FileNotFoundError:
        logger.error("Claude CLI not found. Install with: npm install -g @anthropic-ai/claude-code")
        return ""
    except subprocess.TimeoutExpired:
        logger.error("Claude CLI timed out (120s)")
        return ""


def parse_response(raw: str) -> dict | None:
    """Parse Claude's JSON response, handling edge cases."""
    if not raw:
        return None

    # Strip markdown fences if present
    text = raw.strip()
    if text.startswith("```"):
        lines = text.split("\n")
        # Remove first and last lines (```json and ```)
        lines = [l for l in lines if not l.strip().startswith("```")]
        text = "\n".join(lines)

    try:
        return json.loads(text)
    except json.JSONDecodeError:
        # Try to find JSON in the response
        start = text.find("{")
        end = text.rfind("}") + 1
        if start >= 0 and end > start:
            try:
                return json.loads(text[start:end])
            except json.JSONDecodeError:
                pass
        logger.error(f"Failed to parse Claude response as JSON: {text[:200]}...")
        return None


def store_results(date_str: str, scan_time: str, parsed: dict, raw: str):
    """Store LLM analysis results in Supabase."""
    conn = get_conn()
    cur = conn.cursor()

    # Store market overview
    market = parsed.get("market_outlook", {})
    cur.execute("""
        INSERT INTO llm_scan_results
            (date, scan_time, analysis_type, symbol, recommendation, confidence,
             reasoning, key_insights, risk_factors, raw_response)
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
        ON CONFLICT (date, scan_time, analysis_type, symbol)
        DO UPDATE SET recommendation = EXCLUDED.recommendation,
                      confidence = EXCLUDED.confidence,
                      reasoning = EXCLUDED.reasoning,
                      key_insights = EXCLUDED.key_insights,
                      risk_factors = EXCLUDED.risk_factors,
                      raw_response = EXCLUDED.raw_response
    """, (
        date_str, scan_time, "market_overview", None,
        market.get("sentiment", ""),
        None,
        market.get("summary", ""),
        json.dumps({
            "sector_insights": parsed.get("sector_insights", ""),
            "timing_advice": parsed.get("timing_advice", ""),
            "stocks_to_avoid": parsed.get("stocks_to_avoid_today", []),
        }),
        json.dumps(market.get("key_risks", [])),
        raw,
    ))

    # Store per-stock picks
    for pick in parsed.get("top_picks", []):
        cur.execute("""
            INSERT INTO llm_scan_results
                (date, scan_time, analysis_type, symbol, recommendation, confidence,
                 reasoning, key_insights, risk_factors, raw_response)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
            ON CONFLICT (date, scan_time, analysis_type, symbol)
            DO UPDATE SET recommendation = EXCLUDED.recommendation,
                          confidence = EXCLUDED.confidence,
                          reasoning = EXCLUDED.reasoning,
                          key_insights = EXCLUDED.key_insights,
                          risk_factors = EXCLUDED.risk_factors
        """, (
            date_str, scan_time, "stock_pick", pick.get("symbol", ""),
            pick.get("recommendation", ""),
            pick.get("confidence", ""),
            pick.get("reasoning", ""),
            json.dumps({"entry_strategy": pick.get("entry_strategy", "")}),
            json.dumps([pick.get("risk_note", "")]),
            None,
        ))

    conn.commit()
    conn.close()
    logger.info(f"Stored LLM results: 1 market overview + {len(parsed.get('top_picks', []))} stock picks")


def run():
    """Main entry point."""
    now = datetime.now(DSE_TZ)
    logger.info(f"=== LLM Scanner starting at {now.strftime('%Y-%m-%d %H:%M:%S')} BST ===")

    # Ensure table exists
    ensure_table()

    # Load data
    data = load_analysis_data()
    if not data["buy_stocks"]:
        logger.warning("No BUY stocks found in analysis. Nothing to analyze.")
        return

    logger.info(f"Loaded {len(data['buy_stocks'])} BUY stocks for {data['date']}")

    # Format prompt
    prompt = format_prompt(data)

    # Call Claude
    raw_response = call_claude(prompt)
    if not raw_response:
        logger.error("No response from Claude")
        return

    # Parse response
    parsed = parse_response(raw_response)
    if not parsed:
        logger.error("Failed to parse response")
        # Store raw response anyway for debugging
        conn = get_conn()
        cur = conn.cursor()
        cur.execute("""
            INSERT INTO llm_scan_results
                (date, scan_time, analysis_type, reasoning, raw_response)
            VALUES (%s, %s, %s, %s, %s)
            ON CONFLICT DO NOTHING
        """, (data["date"], now.isoformat(), "parse_error", "Failed to parse JSON", raw_response))
        conn.commit()
        conn.close()
        return

    # Store results
    store_results(data["date"], now.isoformat(), parsed, raw_response)

    # Log summary
    outlook = parsed.get("market_outlook", {})
    picks = parsed.get("top_picks", [])
    logger.info(f"Market: {outlook.get('sentiment', '?')} — {outlook.get('summary', '')[:100]}")
    for p in picks:
        logger.info(f"  {p.get('symbol', '?')}: {p.get('recommendation', '?')} ({p.get('confidence', '?')}) — {p.get('reasoning', '')[:80]}")

    logger.info("=== LLM Scanner complete ===")


if __name__ == "__main__":
    run()
