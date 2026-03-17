#!/usr/bin/env python3
"""Intraday Portfolio Monitor — checks portfolio stocks every 30 min.

Compares live prices against daily analysis (entry zones, SL, targets)
and calls Claude for quick assessment on any material changes.

Usage:
    python3 scripts/intraday_monitor.py

Cron (every 30 min during market hours, Sun-Thu):
    */30 4-9 * * 0-4 cd /path/to/backend && python3 scripts/intraday_monitor.py
    # 4:00-9:30 UTC = 10:00-15:30 BST
"""

import json
import logging
import os
import subprocess
import sys
import tempfile
import time

import psycopg2
import psycopg2.extras

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%H:%M:%S",
)
logger = logging.getLogger(__name__)

DATABASE_URL = os.getenv(
    "DATABASE_URL",
    "postgresql://postgres.iihlezpkpllacztoaguc:160021062Ss%23%23"
    "@aws-1-ap-southeast-1.pooler.supabase.com:6543/postgres",
)


def get_conn():
    return psycopg2.connect(DATABASE_URL, cursor_factory=psycopg2.extras.RealDictCursor)


def get_portfolio_stocks() -> list[dict]:
    """Get all portfolio stocks from chat_users table."""
    conn = get_conn()
    cur = conn.cursor()
    try:
        cur.execute("SELECT email, name, portfolio FROM chat_users WHERE portfolio != '{}' AND portfolio IS NOT NULL")
        users = cur.fetchall()
    except Exception:
        users = []
    conn.close()

    # Collect all unique symbols across all users
    stocks = {}  # symbol -> {qty, price, users: [names]}
    for u in users:
        portfolio = u.get("portfolio") or {}
        if isinstance(portfolio, str):
            try:
                portfolio = json.loads(portfolio)
            except Exception:
                continue
        for sym, info in portfolio.items():
            if sym not in stocks:
                stocks[sym] = {"symbol": sym, "users": []}
            stocks[sym]["users"].append(u.get("name", "?"))
            if isinstance(info, dict):
                stocks[sym]["qty"] = info.get("qty", 0)
                stocks[sym]["price"] = info.get("price", 0)

    return list(stocks.values())


def get_live_and_analysis(symbols: list[str]) -> dict:
    """Get live prices + latest daily analysis for portfolio stocks."""
    if not symbols:
        return {}

    conn = get_conn()
    cur = conn.cursor()
    placeholders = ",".join(["%s"] * len(symbols))

    # Live prices
    cur.execute(f"""
        SELECT symbol, ltp, high, low, open, close_prev, change, change_pct, volume, updated_at
        FROM live_prices WHERE symbol IN ({placeholders})
    """, symbols)
    live = {r["symbol"]: dict(r) for r in cur.fetchall()}

    # Latest daily analysis
    cur.execute(f"""
        SELECT da.symbol, da.date, da.ltp as analysis_ltp, da.action, da.score,
               da.entry_low, da.entry_high, da.sl, da.t1, da.t2,
               da.rsi, da.cmf, da.stoch_rsi, da.macd_status, da.vol_ratio, da.mfi,
               da.support, da.resistance
        FROM daily_analysis da
        WHERE da.date = (SELECT MAX(date) FROM daily_analysis)
          AND da.symbol IN ({placeholders})
    """, symbols)
    analysis = {r["symbol"]: dict(r) for r in cur.fetchall()}

    # Judge analysis
    cur.execute(f"""
        SELECT symbol, final_action, final_confidence, key_risk, entry_low, entry_high, sl, t1, t2
        FROM judge_daily_analysis
        WHERE date = (SELECT MAX(date) FROM judge_daily_analysis)
          AND symbol IN ({placeholders})
    """, symbols)
    judge = {r["symbol"]: dict(r) for r in cur.fetchall()}

    conn.close()
    return {"live": live, "analysis": analysis, "judge": judge}


def build_monitor_prompt(portfolio_stocks: list[dict], data: dict) -> str:
    """Build a focused prompt for portfolio monitoring."""
    blocks = []
    for stock in portfolio_stocks:
        sym = stock["symbol"]
        live = data["live"].get(sym, {})
        anal = data["analysis"].get(sym, {})
        judge = data["judge"].get(sym, {})

        if not live:
            continue

        ltp = float(live.get("ltp") or 0)
        change_pct = float(live.get("change_pct") or 0)
        volume = int(live.get("volume") or 0)
        buy_price = float(stock.get("price") or 0)
        qty = int(stock.get("qty") or 0)
        pnl = (ltp - buy_price) * qty if buy_price > 0 and qty > 0 else 0
        pnl_pct = ((ltp / buy_price) - 1) * 100 if buy_price > 0 else 0

        sl = float(anal.get("sl") or judge.get("sl") or 0)
        t1 = float(anal.get("t1") or judge.get("t1") or 0)
        t2 = float(anal.get("t2") or judge.get("t2") or 0)
        entry_low = float(anal.get("entry_low") or 0)
        entry_high = float(anal.get("entry_high") or 0)

        block = f"""### {sym} (held by: {', '.join(stock.get('users', []))})
Position: {qty} shares @ {buy_price:.1f} BDT | Current: {ltp:.1f} ({change_pct:+.1f}%) | P&L: {pnl:+,.0f} BDT ({pnl_pct:+.1f}%)
Volume: {volume:,} | Analysis date: {anal.get('date', '?')}
Entry zone: {entry_low:.1f}-{entry_high:.1f} | SL: {sl:.1f} | T1: {t1:.1f} | T2: {t2:.1f}
Indicators: RSI={anal.get('rsi', '?')} | CMF={anal.get('cmf', '?')} | MACD={anal.get('macd_status', '?')} | StRSI={anal.get('stoch_rsi', '?')} | MFI={anal.get('mfi', '?')} | VolR={anal.get('vol_ratio', '?')}
Support: {anal.get('support', '?')} | Resistance: {anal.get('resistance', '?')}
Judge: {judge.get('final_action', '?')} ({judge.get('final_confidence', '?')}) | Risk: {judge.get('key_risk', '?')}
"""
        blocks.append(block)

    if not blocks:
        return ""

    return f"""You are monitoring a DSE trading portfolio in real-time. Check each stock for urgent signals.

For each stock, determine:
1. Is the stop-loss about to be hit? (price within 2% of SL)
2. Has target T1 or T2 been reached? (take profit?)
3. Is there a volume spike or unusual activity?
4. Should the user HOLD, ADD MORE, or EXIT?

Be brief. Only flag stocks with ACTIONABLE changes. If everything is normal, say so.

## Portfolio Stocks (live data)

{chr(10).join(blocks)}

Respond with a JSON array of alerts:
[
  {{
    "symbol": "SYMBOL",
    "alert_type": "SL_WARNING|T1_HIT|T2_HIT|VOLUME_SPIKE|REVERSAL|EXIT_NOW|ADD_MORE|ALL_CLEAR",
    "severity": "CRITICAL|HIGH|MEDIUM|LOW",
    "message": "Brief actionable message"
  }}
]

If all stocks are normal, return: [{{"symbol":"ALL","alert_type":"ALL_CLEAR","severity":"LOW","message":"All positions stable. No action needed."}}]

Start with [ end with ]. ONLY JSON."""


def call_claude(prompt: str, timeout: int = 120) -> str:
    """Call Claude CLI for quick assessment."""
    prompt_file = None
    try:
        prompt_file = tempfile.NamedTemporaryFile(
            mode="w", suffix=".txt", delete=False, prefix="monitor_"
        )
        prompt_file.write(prompt)
        prompt_file.close()

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

        model = os.getenv("CLAUDE_MODEL", "haiku")  # Use haiku for speed
        bash_cmd = f'cat "{prompt_file.name}" | claude -p --model {model} --max-turns 1 --append-system-prompt "Respond with ONLY JSON. Start with ["'

        result = subprocess.run(
            ["bash", "-c", bash_cmd],
            capture_output=True, text=True, timeout=timeout, env=env,
        )

        if result.returncode != 0:
            logger.error(f"Claude error: {(result.stderr or '')[:200]}")
            return ""

        return result.stdout.strip()

    except Exception as e:
        logger.error(f"Monitor Claude call failed: {e}")
        return ""
    finally:
        if prompt_file and os.path.exists(prompt_file.name):
            os.unlink(prompt_file.name)


def store_alerts(alerts: list[dict]):
    """Store alerts in intraday_alerts table."""
    if not alerts:
        return
    conn = get_conn()
    cur = conn.cursor()
    from datetime import date
    today = date.today().isoformat()
    for a in alerts:
        if a.get("alert_type") == "ALL_CLEAR":
            continue
        try:
            cur.execute("""
                INSERT INTO intraday_alerts (date, symbol, alert_type, severity, message, ltp_at_alert)
                VALUES (%s, %s, %s, %s, %s, %s)
            """, (today, a.get("symbol", ""), a.get("alert_type", ""), a.get("severity", "LOW"),
                  a.get("message", ""), a.get("ltp_at_alert")))
        except Exception as e:
            logger.warning(f"Failed to store alert: {e}")
            conn.rollback()
    conn.commit()
    conn.close()


def run():
    """Run intraday monitoring."""
    logger.info("=== Intraday Monitor ===")

    # Get portfolio stocks from all users
    portfolio = get_portfolio_stocks()
    if not portfolio:
        logger.info("No portfolio stocks found in chat_users. Nothing to monitor.")
        return

    symbols = [s["symbol"] for s in portfolio]
    logger.info(f"Monitoring {len(symbols)} stocks: {symbols}")

    # Get live + analysis data
    data = get_live_and_analysis(symbols)
    live_count = len(data.get("live", {}))
    logger.info(f"Live data for {live_count}/{len(symbols)} stocks")

    if live_count == 0:
        logger.warning("No live prices available. Market may be closed.")
        return

    # Build prompt and call Claude
    prompt = build_monitor_prompt(portfolio, data)
    if not prompt:
        logger.info("No stocks with live data to monitor.")
        return

    logger.info(f"Monitor prompt: {len(prompt)} chars")
    raw = call_claude(prompt)
    if not raw:
        logger.warning("Empty response from Claude.")
        return

    # Parse alerts
    import re
    json_match = re.search(r'\[.*\]', raw, re.DOTALL)
    if not json_match:
        logger.warning(f"Could not parse JSON from response: {raw[:200]}")
        return

    try:
        alerts = json.loads(json_match.group())
    except json.JSONDecodeError as e:
        logger.warning(f"JSON parse error: {e}")
        return

    # Log and store alerts
    for a in alerts:
        severity = a.get("severity", "LOW")
        if a.get("alert_type") == "ALL_CLEAR":
            logger.info(f"  {a.get('symbol', 'ALL')}: {a.get('message', 'OK')}")
        else:
            logger.warning(f"  [{severity}] {a.get('symbol', '?')}: {a.get('alert_type', '?')} — {a.get('message', '')}")

    store_alerts(alerts)
    logger.info(f"Stored {len([a for a in alerts if a.get('alert_type') != 'ALL_CLEAR'])} alerts")


if __name__ == "__main__":
    run()
