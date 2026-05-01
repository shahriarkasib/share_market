"""Live signal lifecycle tracker.

Runs every 5 min during market hours. For each tracked stock:
  1. Calls signal_engine.compute_composite_signal()
  2. If composite_score >= 65 (BUY threshold) and no active signal exists:
       insert new row in live_signals (status='active')
  3. If signal already active, update last_seen + check validity:
       - stop hit  → status='stopped_out'
       - T1 hit    → status='hit_t1'   (still active for T2)
       - T2 hit    → status='completed'
       - structure broken (bias flips) → status='invalidated'
       - >7 days old without progress → status='expired'
  4. Records every signal into history for audit.

Schema:
  live_signals(
    id, symbol, first_triggered TIMESTAMP, last_seen TIMESTAMP,
    status VARCHAR, composite_score INT, signal_level VARCHAR, risk_score INT,
    entry NUMERIC, stop_loss NUMERIC, target1 NUMERIC, target2 NUMERIC,
    bias VARCHAR, active_signals JSONB, reasons JSONB,
    triggered_high NUMERIC, triggered_low NUMERIC,    -- to detect T1/stop fills
    closed_at TIMESTAMP, close_price NUMERIC, pl_pct NUMERIC
  )
"""
from __future__ import annotations

import json
import logging
import sys
import os
from datetime import datetime, timezone, timedelta

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from database import get_connection


log = logging.getLogger("live_signals")


def ensure_schema():
    conn = get_connection()
    conn.execute("""
        CREATE TABLE IF NOT EXISTS live_signals (
            id BIGSERIAL PRIMARY KEY,
            symbol VARCHAR(20) NOT NULL,
            first_triggered TIMESTAMPTZ NOT NULL DEFAULT NOW(),
            last_seen TIMESTAMPTZ NOT NULL DEFAULT NOW(),
            status VARCHAR(20) NOT NULL DEFAULT 'active',
            composite_score INT NOT NULL,
            signal_level VARCHAR(15) NOT NULL,
            risk_score INT NOT NULL,
            entry NUMERIC(12,2),
            stop_loss NUMERIC(12,2),
            target1 NUMERIC(12,2),
            target2 NUMERIC(12,2),
            bias VARCHAR(15),
            active_signals JSONB,
            reasons JSONB,
            triggered_high NUMERIC(12,2),
            triggered_low NUMERIC(12,2),
            closed_at TIMESTAMPTZ,
            close_price NUMERIC(12,2),
            pl_pct NUMERIC(8,2),
            current_price NUMERIC(12,2)
        )
    """)
    conn.execute("CREATE INDEX IF NOT EXISTS idx_live_signals_symbol_status ON live_signals (symbol, status)")
    conn.execute("CREATE INDEX IF NOT EXISTS idx_live_signals_status_seen ON live_signals (status, last_seen DESC)")
    conn.commit()
    conn.close()


def get_active_signal(symbol: str):
    conn = get_connection()
    row = conn.execute(
        "SELECT * FROM live_signals WHERE symbol = %s AND status IN ('active','hit_t1') "
        "ORDER BY first_triggered DESC LIMIT 1",
        (symbol.upper(),),
    ).fetchone()
    conn.close()
    return dict(row) if row else None


def insert_signal(sig: dict):
    conn = get_connection()
    conn.execute(
        """INSERT INTO live_signals
           (symbol, status, composite_score, signal_level, risk_score,
            entry, stop_loss, target1, target2, bias, active_signals, reasons,
            triggered_high, triggered_low, current_price)
           VALUES (%s, 'active', %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)""",
        (
            sig["symbol"],
            sig["composite_score"], sig["signal_level"], sig["risk_score"],
            sig.get("entry"), sig.get("stop_loss"),
            sig.get("target1"), sig.get("target2"),
            sig.get("bias"),
            json.dumps(sig.get("active_signals", [])),
            json.dumps(sig.get("reasons", [])),
            sig["current_price"], sig["current_price"], sig["current_price"],
        ),
    )
    conn.commit()
    conn.close()
    log.info(f"NEW signal: {sig['symbol']} score={sig['composite_score']} level={sig['signal_level']}")


def update_signal_state(active_row: dict, sig: dict, last_high: float, last_low: float):
    """Refresh existing signal's last_seen + check stop/target hits."""
    new_status = active_row["status"]
    closed_at = None
    close_price = None
    pl_pct = None
    cur_price = sig["current_price"]

    entry = float(active_row["entry"]) if active_row.get("entry") else None
    stop = float(active_row["stop_loss"]) if active_row.get("stop_loss") else None
    t1 = float(active_row["target1"]) if active_row.get("target1") else None
    t2 = float(active_row["target2"]) if active_row.get("target2") else None

    # Stop hit (intra-bar low went below stop)
    if stop and last_low <= stop and active_row["status"] == "active":
        new_status = "stopped_out"
        closed_at = datetime.now(timezone.utc)
        close_price = stop
        if entry:
            pl_pct = (stop - entry) / entry * 100

    # T2 hit
    elif t2 and last_high >= t2:
        new_status = "completed"
        closed_at = datetime.now(timezone.utc)
        close_price = t2
        if entry:
            pl_pct = (t2 - entry) / entry * 100

    # T1 hit (still active for T2 chase)
    elif t1 and last_high >= t1 and active_row["status"] == "active":
        new_status = "hit_t1"

    # Structure flip — bias changed bearish + composite score plummeted
    elif sig["bias"] in ("BEARISH", "WHIPSAW") and sig["composite_score"] < 40:
        new_status = "invalidated"
        closed_at = datetime.now(timezone.utc)
        close_price = cur_price
        if entry:
            pl_pct = (cur_price - entry) / entry * 100

    # Time expiry — 7 trading days without target
    elif active_row.get("first_triggered"):
        ft = active_row["first_triggered"]
        if isinstance(ft, str):
            ft = datetime.fromisoformat(ft)
        days_old = (datetime.now(timezone.utc) - ft).days
        if days_old > 7 and active_row["status"] == "active":
            new_status = "expired"
            closed_at = datetime.now(timezone.utc)
            close_price = cur_price
            if entry:
                pl_pct = (cur_price - entry) / entry * 100

    conn = get_connection()
    conn.execute(
        """UPDATE live_signals
           SET last_seen = NOW(), status = %s, composite_score = %s,
               risk_score = %s, current_price = %s,
               triggered_high = GREATEST(COALESCE(triggered_high, 0), %s),
               triggered_low = LEAST(COALESCE(triggered_low, 9999999), %s),
               active_signals = %s, reasons = %s,
               closed_at = COALESCE(closed_at, %s),
               close_price = COALESCE(close_price, %s),
               pl_pct = COALESCE(pl_pct, %s)
           WHERE id = %s""",
        (
            new_status, sig["composite_score"], sig["risk_score"],
            cur_price, last_high, last_low,
            json.dumps(sig.get("active_signals", [])),
            json.dumps(sig.get("reasons", [])),
            closed_at, close_price, pl_pct, active_row["id"],
        ),
    )
    conn.commit()
    conn.close()


def run_cycle(min_score: int = 65):
    """One pass — scan all A-cat DSE stocks and update signals."""
    from api.smc_chart import get_smc_chart
    from api.signal_engine import compute_composite_signal

    ensure_schema()
    conn = get_connection()
    rows = conn.execute(
        "SELECT symbol FROM fundamentals WHERE category = 'A' ORDER BY symbol"
    ).fetchall()
    symbols = [r[0] for r in rows]
    conn.close()

    new_count = updated_count = closed_count = 0
    for sym in symbols:
        try:
            chart = get_smc_chart(sym, days=730, interval="daily")
            if chart is None:
                continue

            db_conn = get_connection()
            sig = compute_composite_signal(chart, conn=db_conn)
            db_conn.close()

            # Skip if score below threshold AND no active signal
            active_row = get_active_signal(sym)
            if sig["composite_score"] < min_score and not active_row:
                continue

            # Get today's bar high/low for stop/target check
            candles = chart.get("candles", [])
            if candles:
                last = candles[-1]
                last_high = float(last["high"])
                last_low = float(last["low"])
            else:
                last_high = last_low = sig["current_price"]

            if active_row:
                update_signal_state(active_row, sig, last_high, last_low)
                if active_row["status"] in ("active", "hit_t1"):
                    updated_count += 1
                else:
                    closed_count += 1
            elif sig["composite_score"] >= min_score:
                insert_signal(sig)
                new_count += 1

        except Exception as e:
            log.warning(f"{sym}: {e}")
            continue

    log.info(f"cycle done: new={new_count} updated={updated_count} closed={closed_count}")
    return {"new": new_count, "updated": updated_count, "closed": closed_count}


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
    run_cycle()
