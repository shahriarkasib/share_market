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
            current_price NUMERIC(12,2),
            regime VARCHAR(25),
            action_type VARCHAR(20),
            entry_distance_pct NUMERIC(8,2),
            votes JSONB
        )
    """)
    # Add new columns if upgrading from old schema (idempotent)
    for col, sql_type in [
        ("regime", "VARCHAR(25)"),
        ("action_type", "VARCHAR(20)"),
        ("entry_distance_pct", "NUMERIC(8,2)"),
        ("votes", "JSONB"),
        ("state_label", "TEXT"),
        ("days_since_trigger", "INTEGER"),
        ("fvg_distance_pct", "NUMERIC(8,2)"),
        ("t_plus_2_friendly", "BOOLEAN"),
        ("t_plus_2_reasons", "JSONB"),
        ("t_plus_2_bonuses", "JSONB"),
        ("buy_votes", "INTEGER"),
        ("weighted_buy_pct", "NUMERIC(6,1)"),
        # New SMC-aligned fields (Annaly Trader rules + Tier-1 aggressive)
        ("entry_label", "TEXT"),
        ("entry_status", "VARCHAR(25)"),
        ("chase_warning", "TEXT"),
        ("aggressive_entry", "NUMERIC(12,2)"),
        ("aggressive_entry_label", "TEXT"),
        ("aggressive_entry_distance_pct", "NUMERIC(8,2)"),
        ("confidence", "VARCHAR(15)"),
        ("hedge_fund_verdict", "TEXT"),
        ("structure_verdict", "VARCHAR(25)"),
        ("order_flow_verdict", "VARCHAR(15)"),
        ("volume_verdict", "VARCHAR(15)"),
        ("htf_bias", "JSONB"),
        ("liquidity_sweep", "VARCHAR(30)"),
        # Entry zone (range) + technical trigger fields
        ("entry_zone_low", "NUMERIC(12,2)"),
        ("entry_zone_high", "NUMERIC(12,2)"),
        ("aggressive_entry_zone_low", "NUMERIC(12,2)"),
        ("aggressive_entry_zone_high", "NUMERIC(12,2)"),
        ("primary_trigger_date", "DATE"),  # actual technical trigger
        ("primary_trigger_bars_ago", "INTEGER"),
        ("primary_trigger_max_profit_pct", "NUMERIC(8,2)"),
        ("primary_trigger_max_drawdown_pct", "NUMERIC(8,2)"),
        ("tier1_trigger_date", "DATE"),
        ("tier1_trigger_bars_ago", "INTEGER"),
        ("tier1_max_profit_pct", "NUMERIC(8,2)"),
        ("tier2_trigger_date", "DATE"),
        ("tier2_trigger_bars_ago", "INTEGER"),
        ("tier2_max_profit_pct", "NUMERIC(8,2)"),
        ("bucket", "VARCHAR(15)"),  # IN_ZONE | WATCHING | MISSED | STALE
    ]:
        try:
            conn.execute(f"ALTER TABLE live_signals ADD COLUMN IF NOT EXISTS {col} {sql_type}")
        except Exception:
            pass
    conn.execute("CREATE INDEX IF NOT EXISTS idx_live_signals_symbol_status ON live_signals (symbol, status)")
    conn.execute("CREATE INDEX IF NOT EXISTS idx_live_signals_status_seen ON live_signals (status, last_seen DESC)")
    conn.commit()
    conn.close()


def get_active_signal(symbol: str):
    conn = get_connection()
    row = conn.execute(
        "SELECT * FROM live_signals WHERE symbol = %s "
        "AND (market = 'dse' OR market IS NULL) "
        "AND status IN ('active','hit_t1') "
        "ORDER BY first_triggered DESC LIMIT 1",
        (symbol.upper(),),
    ).fetchone()
    conn.close()
    return dict(row) if row else None


def derive_bucket(sig: dict) -> str:
    """Categorise a signal:
      IN_ZONE       — price is inside (or barely above) an entry zone — buy now.
      WATCHING      — price 1.5-8% above zone — set a buy limit.
      MISSED        — we triggered ≥2 days ago AND max profit since ≥3% but we
                      didn't buy. The opportunity was real and is now past.
      WRONG_TRIGGER — we triggered ≥2 days ago BUT price went below the zone /
                      max profit < 0%. Our zone was wrong; learn from it.
      STALE         — no entry / no recent trigger.
    """
    cp = sig.get("current_price")
    t1l = sig.get("aggressive_entry_zone_low")
    t1h = sig.get("aggressive_entry_zone_high")
    t2l = sig.get("entry_zone_low")
    t2h = sig.get("entry_zone_high")

    if cp is None:
        return "STALE"

    # Past trigger info — used to classify MISSED vs WRONG_TRIGGER
    bars_ago = sig.get("primary_trigger_bars_ago") or 0
    max_profit = sig.get("primary_trigger_max_profit_pct") or 0
    max_drawdown = sig.get("primary_trigger_max_drawdown_pct") or 0
    triggered_in_past = bars_ago >= 2  # at least 2 trading days old
    delivered_profit = max_profit >= 3.0
    zone_broke = max_drawdown < -3.0  # price dropped >3% below zone after trigger

    # Strictly inside a zone (current actionable buy)
    in_t1 = t1l is not None and t1h is not None and t1l <= cp <= t1h
    in_t2 = t2l is not None and t2h is not None and t2l <= cp <= t2h
    if in_t1 or in_t2:
        return "IN_ZONE"

    # Below all zones — could be DISCOUNT (still actionable) OR WRONG_TRIGGER
    # (zone broke). Use the bars_ago + drawdown heuristic.
    below_t1 = t1l is not None and cp < t1l
    below_t2 = t2l is not None and cp < t2l
    if below_t1 or below_t2:
        if triggered_in_past and zone_broke:
            return "WRONG_TRIGGER"  # triggered, then price fell BELOW zone
        return "IN_ZONE"  # below zone but stable = discount

    # Above zones: distance from closest zone_high
    candidates_high = [h for h in (t1h, t2h) if h is not None]
    if not candidates_high:
        # No zone at all but might still have past trigger
        if triggered_in_past and delivered_profit:
            return "MISSED"
        return "STALE"
    closest_high = max(candidates_high)
    pct_above = (cp - closest_high) / closest_high * 100 if closest_high > 0 else 999

    # Past meaningful trigger always wins — that's a real missed opportunity
    # even if currently still close to zone.
    if triggered_in_past and delivered_profit:
        return "MISSED"

    # Slight overshoot (≤1.5%) still counts as actionable
    if pct_above <= 1.5:
        return "IN_ZONE"
    if pct_above <= 8.0:
        return "WATCHING"
    # >8% above with no past meaningful trigger = setup is stale (price ran away)
    return "MISSED"


def _trigger_fields(sig: dict) -> dict:
    """Extract trigger-related sub-objects from sig as flat columns."""
    pt = sig.get("primary_trigger") or {}
    t1 = sig.get("tier1_trigger") or {}
    t2 = sig.get("tier2_trigger") or {}
    return {
        "entry_zone_low": sig.get("entry_zone_low"),
        "entry_zone_high": sig.get("entry_zone_high"),
        "aggressive_entry_zone_low": sig.get("aggressive_entry_zone_low"),
        "aggressive_entry_zone_high": sig.get("aggressive_entry_zone_high"),
        "primary_trigger_date": pt.get("last_hit_date"),
        "primary_trigger_bars_ago": pt.get("bars_since_hit"),
        "primary_trigger_max_profit_pct": pt.get("max_profit_pct_mid_entry"),
        "primary_trigger_max_drawdown_pct": pt.get("max_drawdown_pct"),
        "tier1_trigger_date": t1.get("last_hit_date"),
        "tier1_trigger_bars_ago": t1.get("bars_since_hit"),
        "tier1_max_profit_pct": t1.get("max_profit_pct_mid_entry"),
        "tier2_trigger_date": t2.get("last_hit_date"),
        "tier2_trigger_bars_ago": t2.get("bars_since_hit"),
        "tier2_max_profit_pct": t2.get("max_profit_pct_mid_entry"),
        "bucket": derive_bucket(sig),
    }


def insert_signal(sig: dict):
    conn = get_connection()
    conn.execute(
        """INSERT INTO live_signals
           (symbol, market, status, composite_score, signal_level, risk_score,
            entry, stop_loss, target1, target2, bias, active_signals, reasons,
            triggered_high, triggered_low, current_price,
            regime, action_type, entry_distance_pct, votes,
            state_label, days_since_trigger, fvg_distance_pct,
            t_plus_2_friendly, t_plus_2_reasons, t_plus_2_bonuses)
           VALUES (%s, 'dse', 'active', %s, %s, %s, %s, %s, %s, %s, %s, %s, %s,
                   %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)""",
        (
            sig["symbol"],
            sig["composite_score"], sig["signal_level"], sig["risk_score"],
            sig.get("entry"), sig.get("stop_loss"),
            sig.get("target1"), sig.get("target2"),
            sig.get("bias"),
            json.dumps(sig.get("active_signals", [])),
            json.dumps(sig.get("reasons", [])),
            sig["current_price"], sig["current_price"], sig["current_price"],
            sig.get("regime"), sig.get("action_type"),
            sig.get("entry_distance_pct"),
            json.dumps(sig.get("votes", {})),
            sig.get("state_label"), sig.get("days_since_trigger"),
            sig.get("fvg_distance_pct"),
            sig.get("t_plus_2_friendly"),
            json.dumps(sig.get("t_plus_2_reasons", [])),
            json.dumps(sig.get("t_plus_2_bonuses", [])),
        ),
    )
    # patch buy_votes / weighted_buy_pct + new SMC fields via separate UPDATE
    # (the INSERT was finalised earlier — we add new fields post-row).
    tf = _trigger_fields(sig)
    conn.execute(
        """UPDATE live_signals SET
            buy_votes = %s,
            weighted_buy_pct = %s,
            entry_label = %s,
            entry_status = %s,
            chase_warning = %s,
            aggressive_entry = %s,
            aggressive_entry_label = %s,
            aggressive_entry_distance_pct = %s,
            confidence = %s,
            hedge_fund_verdict = %s,
            structure_verdict = %s,
            order_flow_verdict = %s,
            volume_verdict = %s,
            htf_bias = %s,
            liquidity_sweep = %s,
            entry_zone_low = %s, entry_zone_high = %s,
            aggressive_entry_zone_low = %s, aggressive_entry_zone_high = %s,
            primary_trigger_date = %s, primary_trigger_bars_ago = %s,
            primary_trigger_max_profit_pct = %s, primary_trigger_max_drawdown_pct = %s,
            tier1_trigger_date = %s, tier1_trigger_bars_ago = %s, tier1_max_profit_pct = %s,
            tier2_trigger_date = %s, tier2_trigger_bars_ago = %s, tier2_max_profit_pct = %s,
            bucket = %s
           WHERE id = (SELECT MAX(id) FROM live_signals WHERE symbol = %s)""",
        (
            sig.get("buy_votes"),
            sig.get("weighted_buy_pct"),
            sig.get("entry_label"),
            sig.get("entry_status"),
            sig.get("chase_warning"),
            sig.get("aggressive_entry"),
            sig.get("aggressive_entry_label"),
            sig.get("aggressive_entry_distance_pct"),
            sig.get("confidence"),
            sig.get("hedge_fund_verdict"),
            sig.get("structure_verdict"),
            sig.get("order_flow_verdict"),
            sig.get("volume_verdict"),
            json.dumps(sig.get("htf_bias")) if sig.get("htf_bias") else None,
            sig.get("liquidity_sweep"),
            tf["entry_zone_low"], tf["entry_zone_high"],
            tf["aggressive_entry_zone_low"], tf["aggressive_entry_zone_high"],
            tf["primary_trigger_date"], tf["primary_trigger_bars_ago"],
            tf["primary_trigger_max_profit_pct"], tf["primary_trigger_max_drawdown_pct"],
            tf["tier1_trigger_date"], tf["tier1_trigger_bars_ago"], tf["tier1_max_profit_pct"],
            tf["tier2_trigger_date"], tf["tier2_trigger_bars_ago"], tf["tier2_max_profit_pct"],
            tf["bucket"],
            sig["symbol"],
        ),
    )
    conn.commit()
    conn.close()
    log.info(f"NEW signal: {sig['symbol']} regime={sig.get('regime')} score={sig['composite_score']} "
             f"level={sig['signal_level']} action={sig.get('action_type')} "
             f"agreement={sig.get('buy_votes')}/9")


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
    tf = _trigger_fields(sig)
    conn.execute(
        """UPDATE live_signals
           SET last_seen = NOW(), status = %s, composite_score = %s,
               risk_score = %s, current_price = %s,
               triggered_high = GREATEST(COALESCE(triggered_high, 0), %s),
               triggered_low = LEAST(COALESCE(triggered_low, 9999999), %s),
               active_signals = %s, reasons = %s,
               regime = %s, action_type = %s, entry_distance_pct = %s, votes = %s,
               state_label = %s, days_since_trigger = %s, fvg_distance_pct = %s,
               t_plus_2_friendly = %s, t_plus_2_reasons = %s, t_plus_2_bonuses = %s,
               buy_votes = %s, weighted_buy_pct = %s,
               signal_level = %s,
               entry = %s, stop_loss = %s, target1 = %s, target2 = %s,
               bias = %s,
               entry_label = %s, entry_status = %s, chase_warning = %s,
               aggressive_entry = %s, aggressive_entry_label = %s,
               aggressive_entry_distance_pct = %s,
               confidence = %s, hedge_fund_verdict = %s,
               structure_verdict = %s, order_flow_verdict = %s, volume_verdict = %s,
               htf_bias = %s, liquidity_sweep = %s,
               entry_zone_low = %s, entry_zone_high = %s,
               aggressive_entry_zone_low = %s, aggressive_entry_zone_high = %s,
               primary_trigger_date = %s, primary_trigger_bars_ago = %s,
               primary_trigger_max_profit_pct = %s, primary_trigger_max_drawdown_pct = %s,
               tier1_trigger_date = %s, tier1_trigger_bars_ago = %s, tier1_max_profit_pct = %s,
               tier2_trigger_date = %s, tier2_trigger_bars_ago = %s, tier2_max_profit_pct = %s,
               bucket = %s,
               closed_at = COALESCE(closed_at, %s),
               close_price = COALESCE(close_price, %s),
               pl_pct = COALESCE(pl_pct, %s)
           WHERE id = %s""",
        (
            new_status, sig["composite_score"], sig["risk_score"],
            cur_price, last_high, last_low,
            json.dumps(sig.get("active_signals", [])),
            json.dumps(sig.get("reasons", [])),
            sig.get("regime"), sig.get("action_type"),
            sig.get("entry_distance_pct"),
            json.dumps(sig.get("votes", {})),
            sig.get("state_label"), sig.get("days_since_trigger"),
            sig.get("fvg_distance_pct"),
            sig.get("t_plus_2_friendly"),
            json.dumps(sig.get("t_plus_2_reasons", [])),
            json.dumps(sig.get("t_plus_2_bonuses", [])),
            sig.get("buy_votes"), sig.get("weighted_buy_pct"),
            sig.get("signal_level"),
            sig.get("entry"), sig.get("stop_loss"), sig.get("target1"), sig.get("target2"),
            sig.get("bias"),
            sig.get("entry_label"), sig.get("entry_status"), sig.get("chase_warning"),
            sig.get("aggressive_entry"), sig.get("aggressive_entry_label"),
            sig.get("aggressive_entry_distance_pct"),
            sig.get("confidence"), sig.get("hedge_fund_verdict"),
            sig.get("structure_verdict"), sig.get("order_flow_verdict"),
            sig.get("volume_verdict"),
            json.dumps(sig.get("htf_bias")) if sig.get("htf_bias") else None,
            sig.get("liquidity_sweep"),
            tf["entry_zone_low"], tf["entry_zone_high"],
            tf["aggressive_entry_zone_low"], tf["aggressive_entry_zone_high"],
            tf["primary_trigger_date"], tf["primary_trigger_bars_ago"],
            tf["primary_trigger_max_profit_pct"], tf["primary_trigger_max_drawdown_pct"],
            tf["tier1_trigger_date"], tf["tier1_trigger_bars_ago"], tf["tier1_max_profit_pct"],
            tf["tier2_trigger_date"], tf["tier2_trigger_bars_ago"], tf["tier2_max_profit_pct"],
            tf["bucket"],
            closed_at, close_price, pl_pct, active_row["id"],
        ),
    )
    conn.commit()
    conn.close()


def run_cycle(min_score: int = 50):
    """One pass — scan all A-cat DSE stocks and update signals.
    min_score=50 captures lifecycle states (MISSED_ENTRY, RUNNING) for
    monitoring, not just fresh BUY. UI filters restrict display."""
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
            # Skip STALE entries — they're aspirational zones price hasn't
            # tested in a long time. Don't pollute the live tracker with them.
            if sig.get("action_type") == "STALE" and not active_row:
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
