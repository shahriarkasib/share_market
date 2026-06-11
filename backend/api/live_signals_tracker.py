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
        ("short_term_trend", "JSONB"),
        ("analyst_verdict", "JSONB"),
        ("today_candle_quality", "JSONB"),
        ("flow_divergence", "JSONB"),
        ("pattern_failure", "JSONB"),
        ("volume_signature", "JSONB"),
        ("rvol", "NUMERIC(6,2)"),  # quick filter on relative volume
        ("absorption_pattern", "JSONB"),
        ("absorption_score", "NUMERIC(5,1)"),  # quick filter, NULL if no pattern
        ("analyst_score", "NUMERIC(6,1)"),  # quick filter without parsing JSONB
        # ── Locked trigger snapshot (set ONCE at first save, never updated) ──
        ("actual_trigger_price", "NUMERIC(12,2)"),  # LTP at first trigger ts
        ("trigger_locked", "BOOLEAN DEFAULT FALSE"),  # guard against overwrite
        # ── Bid ladder: dynamic position-size suggestion per signal ──
        ("bid_ladder", "JSONB"),  # [{price, size_pct, label, edge, risk_pct, reward_pct}, ...]
        # ── T+N OHLC tracking (populated by perf update) ──
        ("t1_high", "NUMERIC(12,2)"), ("t1_low", "NUMERIC(12,2)"), ("t1_close", "NUMERIC(12,2)"), ("t1_date", "DATE"),
        ("t2_high", "NUMERIC(12,2)"), ("t2_low", "NUMERIC(12,2)"), ("t2_close", "NUMERIC(12,2)"), ("t2_date", "DATE"),
        ("t3_high", "NUMERIC(12,2)"), ("t3_low", "NUMERIC(12,2)"), ("t3_close", "NUMERIC(12,2)"),
        ("t5_high", "NUMERIC(12,2)"), ("t5_low", "NUMERIC(12,2)"), ("t5_close", "NUMERIC(12,2)"),
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
        ("bucket", "VARCHAR(25)"),  # IN_ZONE | JUST_BOUNCED | PULLBACK_IN_PROGRESS | WATCHING | MISSED | WRONG_TRIGGER | STALE
    ]:
        try:
            conn.execute(f"ALTER TABLE live_signals ADD COLUMN IF NOT EXISTS {col} {sql_type}")
        except Exception:
            pass
    conn.execute("CREATE INDEX IF NOT EXISTS idx_live_signals_symbol_status ON live_signals (symbol, status)")
    conn.execute("CREATE INDEX IF NOT EXISTS idx_live_signals_status_seen ON live_signals (status, last_seen DESC)")
    conn.commit()
    conn.close()


def cleanup_old_signals(days: int = 30) -> int:
    """Delete signals older than `days` to keep the table lean.

    Keeps: active rows + signals from last `days` days (for backtest).
    """
    conn = get_connection()
    cur = conn.execute(
        """DELETE FROM live_signals
           WHERE status NOT IN ('active', 'hit_t1')
             AND first_triggered < NOW() - INTERVAL '%s days'
        """ % days
    )
    deleted = cur.rowcount if hasattr(cur, "rowcount") else 0
    conn.commit()
    conn.close()
    return deleted


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
      IN_ZONE              — price inside (or ≤1.5% above) the entry zone.
      JUST_BOUNCED         — touched zone in last 5 bars + bounced ≥1% +
                              short-term trend is UP/SIDEWAYS + last bar
                              confirms (close > prior close OR green bar).
                              Support CONFIRMED.
      PULLBACK_IN_PROGRESS — touched zone recently BUT trend is DOWN
                              and/or last bars still red. Falling knife —
                              wait for deeper Tier-2 zone or trend flip.
      WATCHING             — above zone 1.5-8%, no recent touch. Also used
                              as the observational "watch today" bucket when
                              the analyst verdict flags a fresh BUY/STRONG_BUY
                              even if structurally far above zone.
      MISSED               — triggered ≥2d, delivered ≥3% profit, didn't buy.
      WRONG_TRIGGER        — triggered ≥2d, zone broke (>3% drawdown).
      STALE                — no entry / no recent trigger.

    Analyst-verdict overlay: structural bucket is computed first, then the
    observational analyst_verdict (today's candle + flow divergence + pattern
    failures) can promote a STALE bucket to WATCHING so a fresh BUY signal
    doesn't get hidden as historical noise.
    """
    cp = sig.get("current_price")
    t1l = sig.get("aggressive_entry_zone_low")
    t1h = sig.get("aggressive_entry_zone_high")
    t2l = sig.get("entry_zone_low")
    t2h = sig.get("entry_zone_high")

    # Pre-extract analyst verdict — used as an overlay at the end
    av = sig.get("analyst_verdict") or {}
    if isinstance(av, str):
        try:
            import json as _json
            av = _json.loads(av) or {}
        except Exception:
            av = {}
    av_verdict = (av.get("verdict") or "").upper() if isinstance(av, dict) else ""

    if cp is None:
        return "STALE"

    # Trigger info — try flat fields first (DB row), fall back to nested
    # `primary_trigger` dict (computed signal from compute_composite_signal).
    pt = sig.get("primary_trigger") or {}
    bars_ago = sig.get("primary_trigger_bars_ago")
    if bars_ago is None: bars_ago = pt.get("bars_since_hit") or 0
    max_profit = sig.get("primary_trigger_max_profit_pct")
    if max_profit is None: max_profit = pt.get("max_profit_pct_mid_entry") or 0
    max_drawdown = sig.get("primary_trigger_max_drawdown_pct")
    if max_drawdown is None: max_drawdown = pt.get("max_drawdown_pct") or 0
    # Coerce to float (DB may return Decimal)
    try: bars_ago = int(bars_ago)
    except: bars_ago = 0
    try: max_profit = float(max_profit)
    except: max_profit = 0
    try: max_drawdown = float(max_drawdown)
    except: max_drawdown = 0
    # MISSED is "recently missed" — cap at 21 trading days. Older triggers
    # are STALE history, not actionable lessons.
    MISSED_MAX_AGE_BARS = 21
    triggered_in_past = bars_ago >= 2
    delivered_profit = max_profit >= 3.0
    zone_broke = max_drawdown < -3.0
    # Recent bounce: zone hit in last 5 bars AND price moved up since
    recent_touch = 0 <= bars_ago <= 5
    bounced_up = max_profit >= 1.0
    # Short-term trend gate — don't call it a bounce if price is still falling
    st_trend = sig.get("short_term_trend") or {}
    if isinstance(st_trend, str):
        try:
            import json as _json
            st_trend = _json.loads(st_trend) or {}
        except Exception:
            st_trend = {}
    trend_dir = st_trend.get("direction") or "SIDEWAYS"
    consec_red = int(st_trend.get("consecutive_red") or 0)
    bounce_confirmed = bool(st_trend.get("bounce_confirmed"))
    # Real bounce: trend not DOWN AND not multiple consecutive red OR bounce confirmed today
    is_real_bounce = (trend_dir != "DOWN" and consec_red <= 1) or bounce_confirmed
    # Falling knife: trend DOWN and last bar didn't confirm a bounce
    is_falling_knife = trend_dir == "DOWN" and not bounce_confirmed

    # POLARITY FLIP only overrides falling-knife when price has at least
    # STOPPED FALLING. A multi-touch support means buyers SHOULD step in,
    # but until they actually do (close >= prior close, OR not 2+ red bars
    # in a row), price is still in a pullback — not yet a bounce.
    last_close_diff = float(st_trend.get("last_close_vs_prior") or 0)
    is_key_level = bool(sig.get("aggressive_entry_is_key_level"))
    if is_key_level:
        # Require at least minimal bounce evidence:
        #   bounce_confirmed (today's bar green AND > prior close)
        #   OR last close didn't drop further (≥ prior close)
        #   AND consec_red <= 2 (not in a strong red streak)
        has_min_evidence = (bounce_confirmed or last_close_diff >= 0) and consec_red <= 2
        if has_min_evidence:
            is_real_bounce = True
            is_falling_knife = False
        # If price is STILL actively falling (last close down + key level not yet
        # defended), keep is_falling_knife True. The level may fail.

    def _overlay(structural: str) -> str:
        """Apply analyst-verdict overlay: a fresh BUY/STRONG_BUY verdict
        promotes STALE to WATCHING (observational opportunity even though
        price is structurally far from zone). Does not downgrade actionable
        buckets — IN_ZONE / JUST_BOUNCED / PULLBACK_IN_PROGRESS / WRONG_TRIGGER
        / MISSED keep their structural meaning so the user still sees both
        signals.
        """
        if structural == "STALE" and av_verdict in ("STRONG_BUY", "BUY"):
            return "WATCHING"
        return structural

    in_t1 = t1l is not None and t1h is not None and t1l <= cp <= t1h
    in_t2 = t2l is not None and t2h is not None and t2l <= cp <= t2h
    if in_t1 or in_t2:
        return _overlay("IN_ZONE")

    below_t1 = t1l is not None and cp < t1l
    below_t2 = t2l is not None and cp < t2l
    if below_t1 or below_t2:
        if triggered_in_past and zone_broke:
            return _overlay("WRONG_TRIGGER")
        return _overlay("IN_ZONE")

    candidates_high = [h for h in (t1h, t2h) if h is not None]
    if not candidates_high:
        if triggered_in_past and delivered_profit and bars_ago <= MISSED_MAX_AGE_BARS:
            return _overlay("MISSED")
        return _overlay("STALE")
    closest_high = max(candidates_high)
    pct_above = (cp - closest_high) / closest_high * 100 if closest_high > 0 else 999

    # JUST_BOUNCED only if the bounce is REAL (trend not falling, close confirms)
    if recent_touch and bounced_up and pct_above <= 6.0 and not zone_broke:
        if is_real_bounce:
            return _overlay("JUST_BOUNCED")
        if is_falling_knife:
            return _overlay("PULLBACK_IN_PROGRESS")
        # Ambiguous (sideways, mixed signals) — treat as JUST_BOUNCED
        # but with the user-warning baked into the bucket-tab description
        return _overlay("JUST_BOUNCED")

    if triggered_in_past and delivered_profit and bars_ago <= MISSED_MAX_AGE_BARS:
        return _overlay("MISSED")

    if pct_above <= 1.5:
        return _overlay("IN_ZONE")
    if pct_above <= 8.0:
        return _overlay("WATCHING")
    # Far above zone — MISSED only if recent (≤21 bars). Otherwise STALE.
    if bars_ago <= MISSED_MAX_AGE_BARS and delivered_profit:
        return _overlay("MISSED")
    return _overlay("STALE")


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
            actual_trigger_price, trigger_locked,
            regime, action_type, entry_distance_pct, votes,
            state_label, days_since_trigger, fvg_distance_pct,
            t_plus_2_friendly, t_plus_2_reasons, t_plus_2_bonuses)
           VALUES (%s, 'dse', 'active', %s, %s, %s, %s, %s, %s, %s, %s, %s, %s,
                   %s, %s, %s, %s, TRUE, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)""",
        (
            sig["symbol"],
            sig["composite_score"], sig["signal_level"], sig["risk_score"],
            sig.get("entry"), sig.get("stop_loss"),
            sig.get("target1"), sig.get("target2"),
            sig.get("bias"),
            json.dumps(sig.get("active_signals", [])),
            json.dumps(sig.get("reasons", [])),
            sig["current_price"], sig["current_price"], sig["current_price"],
            sig["current_price"],  # actual_trigger_price = LTP at first trigger
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
            short_term_trend = %s,
            analyst_verdict = %s, today_candle_quality = %s, flow_divergence = %s,
            pattern_failure = %s, volume_signature = %s, rvol = %s,
            absorption_pattern = %s, absorption_score = %s, bid_ladder = %s, analyst_score = %s,
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
            json.dumps(sig.get("short_term_trend")) if sig.get("short_term_trend") else None,
            json.dumps(sig.get("analyst_verdict")) if sig.get("analyst_verdict") else None,
            json.dumps(sig.get("today_candle_quality")) if sig.get("today_candle_quality") else None,
            json.dumps(sig.get("flow_divergence")) if sig.get("flow_divergence") else None,
            json.dumps(sig.get("pattern_failure")) if sig.get("pattern_failure") else None,
            json.dumps(sig.get("volume_signature")) if sig.get("volume_signature") else None,
            (sig.get("volume_signature") or {}).get("rvol"),
            json.dumps(sig.get("absorption_pattern")) if sig.get("absorption_pattern") else None,
            (sig.get("absorption_pattern") or {}).get("score"),
            json.dumps(sig.get("bid_ladder")) if sig.get("bid_ladder") else None,
            (sig.get("analyst_verdict") or {}).get("score"),
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
               -- triggered_high/low are LOCKED at insert. Do NOT update —
               -- previously used GREATEST/LEAST which made them running
               -- max/min, corrupting the "entry price" reference.
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
               htf_bias = %s, liquidity_sweep = %s, short_term_trend = %s,
               analyst_verdict = %s, today_candle_quality = %s, flow_divergence = %s,
               pattern_failure = %s, volume_signature = %s, rvol = %s,
               absorption_pattern = %s, absorption_score = %s, bid_ladder = %s, analyst_score = %s,
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
            cur_price,
            # NOTE: last_high, last_low removed — triggered_high/low locked at insert
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
            json.dumps(sig.get("short_term_trend")) if sig.get("short_term_trend") else None,
            json.dumps(sig.get("analyst_verdict")) if sig.get("analyst_verdict") else None,
            json.dumps(sig.get("today_candle_quality")) if sig.get("today_candle_quality") else None,
            json.dumps(sig.get("flow_divergence")) if sig.get("flow_divergence") else None,
            json.dumps(sig.get("pattern_failure")) if sig.get("pattern_failure") else None,
            json.dumps(sig.get("volume_signature")) if sig.get("volume_signature") else None,
            (sig.get("volume_signature") or {}).get("rvol"),
            json.dumps(sig.get("absorption_pattern")) if sig.get("absorption_pattern") else None,
            (sig.get("absorption_pattern") or {}).get("score"),
            json.dumps(sig.get("bid_ladder")) if sig.get("bid_ladder") else None,
            (sig.get("analyst_verdict") or {}).get("score"),
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


def run_cycle(min_score: int = 40):
    """One pass — scan all A-cat DSE stocks and update signals.
    min_score=50 captures lifecycle states (MISSED_ENTRY, RUNNING) for
    monitoring, not just fresh BUY. UI filters restrict display."""
    from api.smc_chart import get_smc_chart
    from api.signal_engine import compute_composite_signal

    ensure_schema()
    conn = get_connection()
    rows = conn.execute(
        "SELECT symbol FROM fundamentals WHERE category IN ('A', 'B') ORDER BY symbol"
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
