"""Signal performance / accuracy tracker.

For every signal in live_signals, track how it performed in the days AFTER
it triggered. Provides the empirical feedback loop:

  triggered at ৳X on day 0
  → day 1 close: ৳X1 (Δ%)
  → day 2 close: ৳X2 (Δ%)
  → day 3 close: ৳X3 (Δ%)
  → day 5 close: ৳X5 (Δ%)
  → max_favorable_pct = best high reached / entry - 1
  → max_adverse_pct   = worst low reached / entry - 1
  → days_to_t1        = N or null
  → days_to_stop      = N or null
  → outcome           = WIN_T1 | WIN_T2 | LOSS_STOP | OPEN | TIMEOUT

Aggregations:
  - Overall win rate
  - Win rate by regime (TRENDING_UP, SIDEWAYS, …)
  - Win rate by score bucket (50-64, 65-79, 80+)
  - Win rate by which strategy voted BUY (which strategy's vote correlates with wins?)
  - Win rate by stock

Runs daily at 15:30 BST after market close. Reads daily_prices to get
the close price of each day since the signal first_triggered.
"""
from __future__ import annotations

import json
import logging
import os
import sys
from datetime import datetime, timedelta, timezone
from typing import Optional

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from database import get_connection


log = logging.getLogger("signal_perf")

PERF_DAYS = [1, 2, 3, 5, 10, 15]


def ensure_perf_schema():
    conn = get_connection()
    # Idempotent column adds
    cols = [
        ("day_1_pct", "NUMERIC(8,2)"),
        ("day_2_pct", "NUMERIC(8,2)"),
        ("day_3_pct", "NUMERIC(8,2)"),
        ("day_5_pct", "NUMERIC(8,2)"),
        ("day_10_pct", "NUMERIC(8,2)"),
        ("day_15_pct", "NUMERIC(8,2)"),
        ("max_favorable_pct", "NUMERIC(8,2)"),
        ("max_adverse_pct", "NUMERIC(8,2)"),
        ("days_to_t1", "INTEGER"),
        ("days_to_stop", "INTEGER"),
        ("outcome", "VARCHAR(15)"),  # WIN_T1, WIN_T2, LOSS_STOP, OPEN, TIMEOUT
        ("perf_updated_at", "TIMESTAMPTZ"),
    ]
    for name, sql_type in cols:
        try:
            conn.execute(f"ALTER TABLE live_signals ADD COLUMN IF NOT EXISTS {name} {sql_type}")
        except Exception:
            pass
    conn.commit()
    conn.close()
    log.info("signal performance columns ready")


def get_daily_prices_after(symbol: str, after_date: str, limit: int = 20):
    """Return list of (date, open, high, low, close) AFTER `after_date`."""
    conn = get_connection()
    rows = conn.execute(
        """SELECT date, open, high, low, close FROM daily_prices
           WHERE symbol = %s AND date > %s::date
           ORDER BY date ASC LIMIT %s""",
        (symbol.upper(), after_date, limit),
    ).fetchall()
    conn.close()
    return [dict(r) for r in rows]


def compute_performance(signal: dict) -> dict:
    """Walk forward from signal's first_triggered date and compute metrics."""
    sym = signal["symbol"]
    entry = float(signal["entry"]) if signal.get("entry") else None
    stop = float(signal["stop_loss"]) if signal.get("stop_loss") else None
    t1 = float(signal["target1"]) if signal.get("target1") else None
    t2 = float(signal["target2"]) if signal.get("target2") else None

    if not entry or entry <= 0:
        return {"outcome": "OPEN"}

    triggered_at = signal["first_triggered"]
    if isinstance(triggered_at, str):
        triggered_at = datetime.fromisoformat(triggered_at.replace("Z", "+00:00"))
    trigger_date = triggered_at.date().isoformat()

    bars = get_daily_prices_after(sym, trigger_date, limit=20)
    if not bars:
        return {"outcome": "OPEN", "perf_updated_at": datetime.now(timezone.utc)}

    out: dict = {"perf_updated_at": datetime.now(timezone.utc)}
    max_fav = 0.0
    max_adv = 0.0
    days_to_t1 = None
    days_to_stop = None
    outcome = "OPEN"

    for i, b in enumerate(bars, start=1):
        bar_high = float(b["high"])
        bar_low = float(b["low"])
        bar_close = float(b["close"])

        # Day-N close % returns at chosen checkpoints
        if i in PERF_DAYS:
            pct = (bar_close - entry) / entry * 100
            out[f"day_{i}_pct"] = round(pct, 2)

        # Max favorable / adverse excursion
        fav = (bar_high - entry) / entry * 100
        adv = (bar_low - entry) / entry * 100
        max_fav = max(max_fav, fav)
        max_adv = min(max_adv, adv)

        # T1 / Stop fill detection
        if stop and days_to_stop is None and bar_low <= stop:
            days_to_stop = i
            outcome = "LOSS_STOP"
            break  # Stop hit ends the trade
        if t1 and days_to_t1 is None and bar_high >= t1:
            days_to_t1 = i
            outcome = "WIN_T1"
            # Don't break — keep tracking for T2
        if t2 and bar_high >= t2 and outcome == "WIN_T1":
            outcome = "WIN_T2"
            break

    out["max_favorable_pct"] = round(max_fav, 2)
    out["max_adverse_pct"] = round(max_adv, 2)
    out["days_to_t1"] = days_to_t1
    out["days_to_stop"] = days_to_stop
    if outcome == "OPEN" and len(bars) >= 15:
        outcome = "TIMEOUT"  # 15-day stop without target/stop
    out["outcome"] = outcome
    return out


def update_all_performances():
    """Update perf metrics for every signal in live_signals."""
    ensure_perf_schema()
    conn = get_connection()
    rows = conn.execute(
        "SELECT * FROM live_signals "
        "WHERE first_triggered IS NOT NULL "
        "ORDER BY first_triggered DESC LIMIT 500"
    ).fetchall()
    conn.close()

    updated = 0
    for r in rows:
        sig = dict(r)
        try:
            perf = compute_performance(sig)
            if not perf:
                continue
            cols = [k for k in perf if k != "perf_updated_at"]
            set_clause = ", ".join(f"{k} = %s" for k in cols) + ", perf_updated_at = NOW()"
            params = tuple(perf[k] for k in cols) + (sig["id"],)
            c2 = get_connection()
            c2.execute(f"UPDATE live_signals SET {set_clause} WHERE id = %s", params)
            c2.commit()
            c2.close()
            updated += 1
        except Exception as e:
            log.warning(f"{sig.get('symbol')}: {e}")
            continue
    log.info(f"updated performance for {updated}/{len(rows)} signals")
    return updated


def aggregate_accuracy() -> dict:
    """Return overall + by-regime + by-score-bucket + by-strategy-vote stats."""
    conn = get_connection()

    def _wr(rows):
        if not rows:
            return {"trades": 0, "wins": 0, "win_rate": 0, "avg_t1_days": 0,
                    "avg_max_fav": 0, "avg_max_adv": 0}
        total = len(rows)
        wins = sum(1 for r in rows if r.get("outcome") in ("WIN_T1", "WIN_T2"))
        avg_t1 = [r["days_to_t1"] for r in rows if r.get("days_to_t1")]
        avg_fav = [float(r["max_favorable_pct"]) for r in rows if r.get("max_favorable_pct") is not None]
        avg_adv = [float(r["max_adverse_pct"]) for r in rows if r.get("max_adverse_pct") is not None]
        return {
            "trades": total,
            "wins": wins,
            "win_rate": round(wins / total * 100, 1),
            "avg_t1_days": round(sum(avg_t1) / len(avg_t1), 1) if avg_t1 else 0,
            "avg_max_fav_pct": round(sum(avg_fav) / len(avg_fav), 2) if avg_fav else 0,
            "avg_max_adv_pct": round(sum(avg_adv) / len(avg_adv), 2) if avg_adv else 0,
        }

    closed = conn.execute(
        "SELECT * FROM live_signals WHERE outcome IS NOT NULL AND outcome != 'OPEN' "
        "ORDER BY first_triggered DESC LIMIT 1000"
    ).fetchall()
    closed = [dict(r) for r in closed]

    overall = _wr(closed)

    by_regime: dict = {}
    for regime in ("TRENDING_UP", "TRENDING_DOWN", "SIDEWAYS", "VOLATILE_EXPANSION"):
        sub = [r for r in closed if r.get("regime") == regime]
        by_regime[regime] = _wr(sub)

    by_score: dict = {}
    for label, lo, hi in [("50-64", 50, 64), ("65-79", 65, 79), ("80+", 80, 100)]:
        sub = [r for r in closed if r.get("composite_score") and lo <= r["composite_score"] <= hi]
        by_score[label] = _wr(sub)

    by_action: dict = {}
    for action in ("BUY_NOW", "BUY_LIMIT", "SETUP", "BREAKOUT_PENDING"):
        sub = [r for r in closed if r.get("action_type") == action]
        by_action[action] = _wr(sub)

    # Strategy contribution: for each strategy name, win rate when it voted BUY
    strategy_perf: dict = {}
    strategies = ["smc", "mtf", "order_flow", "wyckoff", "liquidity_sweep",
                  "volume_profile", "bb_squeeze", "sector_rs", "fib_confluence"]
    for strat in strategies:
        wins_when_buy = total_when_buy = 0
        for r in closed:
            votes = r.get("votes")
            if isinstance(votes, str):
                try: votes = json.loads(votes)
                except: votes = {}
            if not isinstance(votes, dict): continue
            v = votes.get(strat)
            if v and v.get("vote") == "BUY":
                total_when_buy += 1
                if r.get("outcome") in ("WIN_T1", "WIN_T2"):
                    wins_when_buy += 1
        strategy_perf[strat] = {
            "buy_signals": total_when_buy,
            "wins": wins_when_buy,
            "win_rate": round(wins_when_buy / total_when_buy * 100, 1) if total_when_buy > 0 else 0,
        }

    by_stock: dict = {}
    stock_groups: dict = {}
    for r in closed:
        stock_groups.setdefault(r["symbol"], []).append(r)
    by_stock_list = []
    for sym, rs in stock_groups.items():
        if len(rs) >= 3:  # need at least 3 trades to be meaningful
            agg = _wr(rs)
            agg["symbol"] = sym
            by_stock_list.append(agg)
    by_stock_list.sort(key=lambda x: -x["win_rate"])
    by_stock = {"top": by_stock_list[:20]}

    conn.close()
    return {
        "overall": overall,
        "by_regime": by_regime,
        "by_score_bucket": by_score,
        "by_action_type": by_action,
        "by_strategy_vote": strategy_perf,
        "by_stock": by_stock,
        "total_closed": len(closed),
    }


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
    update_all_performances()
    print(json.dumps(aggregate_accuracy(), indent=2, default=str))
