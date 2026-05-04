"""Per-methodology live signals tracker.

Stores one row per (symbol, method) so the user can view BUY/WATCH/MISSED
signals filtered by their preferred methodology (SMC, RSI, Wyckoff, etc.).

Schema is similar to live_signals but keyed on (symbol, method).
"""
from __future__ import annotations

import json
import logging
from datetime import datetime, timezone

from database import get_connection

log = logging.getLogger("method_signals")


def ensure_schema():
    conn = get_connection()
    conn.execute("""
        CREATE TABLE IF NOT EXISTS method_signals (
            id BIGSERIAL PRIMARY KEY,
            symbol VARCHAR(20) NOT NULL,
            method VARCHAR(25) NOT NULL,
            market VARCHAR(10) NOT NULL DEFAULT 'dse',
            last_seen TIMESTAMPTZ NOT NULL DEFAULT NOW(),
            signal VARCHAR(15),
            bucket VARCHAR(20),
            confidence VARCHAR(15),
            entry NUMERIC(12,2),
            entry_zone_low NUMERIC(12,2),
            entry_zone_high NUMERIC(12,2),
            stop_loss NUMERIC(12,2),
            target1 NUMERIC(12,2),
            current_price NUMERIC(12,2),
            reason TEXT,
            trigger_date DATE,
            bars_since_trigger INTEGER,
            max_profit_since_pct NUMERIC(8,2),
            max_drawdown_since_pct NUMERIC(8,2),
            UNIQUE(symbol, method, market)
        )
    """)
    conn.execute("CREATE INDEX IF NOT EXISTS idx_method_signals_method_bucket ON method_signals (method, bucket)")
    conn.execute("CREATE INDEX IF NOT EXISTS idx_method_signals_symbol ON method_signals (symbol)")
    conn.commit()
    conn.close()


def upsert_signal(sig: dict, market: str = "dse"):
    """Insert or update a single (symbol, method) signal row."""
    conn = get_connection()
    conn.execute(
        """INSERT INTO method_signals
           (symbol, method, market, last_seen, signal, bucket, confidence,
            entry, entry_zone_low, entry_zone_high, stop_loss, target1,
            current_price, reason, trigger_date, bars_since_trigger,
            max_profit_since_pct, max_drawdown_since_pct)
           VALUES (%s, %s, %s, NOW(), %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
           ON CONFLICT (symbol, method, market) DO UPDATE SET
               last_seen = NOW(),
               signal = EXCLUDED.signal,
               bucket = EXCLUDED.bucket,
               confidence = EXCLUDED.confidence,
               entry = EXCLUDED.entry,
               entry_zone_low = EXCLUDED.entry_zone_low,
               entry_zone_high = EXCLUDED.entry_zone_high,
               stop_loss = EXCLUDED.stop_loss,
               target1 = EXCLUDED.target1,
               current_price = EXCLUDED.current_price,
               reason = EXCLUDED.reason,
               trigger_date = EXCLUDED.trigger_date,
               bars_since_trigger = EXCLUDED.bars_since_trigger,
               max_profit_since_pct = EXCLUDED.max_profit_since_pct,
               max_drawdown_since_pct = EXCLUDED.max_drawdown_since_pct
        """,
        (
            sig["symbol"], sig["method"], market,
            sig.get("signal"), sig.get("bucket"), sig.get("confidence"),
            sig.get("entry"), sig.get("entry_zone_low"), sig.get("entry_zone_high"),
            sig.get("stop_loss"), sig.get("target1"),
            sig.get("current_price"), sig.get("reason"),
            sig.get("trigger_date"), sig.get("bars_since_trigger"),
            sig.get("max_profit_since_pct"), sig.get("max_drawdown_since_pct"),
        ),
    )
    conn.commit()
    conn.close()


def run_cycle_for_market(market: str = "dse", limit: int = 200):
    """Compute method signals for every A-cat stock, persist."""
    from api.smc_chart import get_smc_chart
    from api.methodology_signals import compute_all_method_signals
    from data.repository import read_historical_for_symbol

    ensure_schema()
    conn = get_connection()
    rows = conn.execute(
        "SELECT symbol FROM fundamentals WHERE category = 'A' ORDER BY symbol LIMIT %s",
        (limit,),
    ).fetchall()
    symbols = [r[0] for r in rows]
    conn.close()

    written = 0
    for sym in symbols:
        try:
            chart = get_smc_chart(sym, days=730, interval="daily")
            if chart is None:
                continue
            df = read_historical_for_symbol(sym, min_rows=200)
            sigs = compute_all_method_signals(chart, df)
            for sig in sigs:
                if sig.get("signal") in (None, "NONE"):
                    # Only persist methods that have a real signal (not noise)
                    # but still upsert so old rows update to NONE if signal disappears
                    pass
                upsert_signal(sig, market=market)
                written += 1
        except Exception as e:
            log.warning(f"{sym}: {e}")
            continue
    log.info(f"method-signals cycle done: {written} rows written across {len(symbols)} stocks ({market})")
    return {"symbols": len(symbols), "rows": written}


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
    print(run_cycle_for_market())
