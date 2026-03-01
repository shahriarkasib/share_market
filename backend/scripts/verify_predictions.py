#!/usr/bin/env python3
"""Prediction Verification — Checks historical predictions against actual outcomes.

Runs daily after market close. Checks:
1. HOLD/WAIT stocks: did action transition to BUY within wait window?
2. BUY stocks: did price hit T1, T2, or SL?
3. Computes accuracy summaries per source (algo/llm/judge).

Usage:
    python3 scripts/verify_predictions.py
"""

import logging
import os
from datetime import datetime, timedelta

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

DATABASE_URL = os.getenv(
    "DATABASE_URL",
    "postgresql://postgres.iihlezpkpllacztoaguc:160021062Ss%23%23"
    "@aws-1-ap-southeast-1.pooler.supabase.com:6543/postgres",
)


def get_conn():
    conn = psycopg2.connect(DATABASE_URL, cursor_factory=psycopg2.extras.RealDictCursor)
    conn.autocommit = False
    return conn


def verify_hold_transitions(lookback_days: int = 30):
    """Check if HOLD/WAIT predictions transitioned to BUY within wait window.

    Logic:
    - For each PENDING prediction where action was HOLD/WAIT:
    - Look at daily_analysis for the same symbol on subsequent dates
    - If action changed to BUY*, record the transition
    - CORRECT if transition happened within wait_days_min..wait_days_max
    - PARTIAL if transition happened but outside window
    - WRONG if wait window expired with no transition
    """
    conn = get_conn()
    cur = conn.cursor()
    today = datetime.now(DSE_TZ).date()

    # Get PENDING HOLD/WAIT predictions older than 2 days
    cur.execute("""
        SELECT id, date, symbol, source, action, wait_days_min, wait_days_max,
               ltp_at_prediction
        FROM prediction_tracker
        WHERE outcome = 'PENDING'
          AND action IN ('HOLD/WAIT', 'SELL/AVOID', 'AVOID')
          AND date < CURRENT_DATE - INTERVAL '2 days'
          AND date > CURRENT_DATE - INTERVAL '%s days'
        ORDER BY date
    """, (lookback_days,))
    pending = cur.fetchall()

    if not pending:
        conn.close()
        logger.info("No HOLD/WAIT predictions to verify")
        return 0

    updated = 0
    for p in pending:
        pred_date = p["date"]
        symbol = p["symbol"]
        source = p["source"]
        wd_max = p["wait_days_max"] or 15
        wd_min = p["wait_days_min"] or 0

        # How many trading days since prediction?
        days_elapsed = (today - pred_date).days

        # Check if this stock got a BUY action on any later date
        # Use the SOURCE's own table for the check
        if source == "algo":
            cur.execute("""
                SELECT date, action FROM daily_analysis
                WHERE symbol = %s AND date > %s AND action LIKE 'BUY%%'
                ORDER BY date LIMIT 1
            """, (symbol, pred_date))
        elif source == "llm":
            cur.execute("""
                SELECT date, action FROM llm_daily_analysis
                WHERE symbol = %s AND date > %s AND action LIKE 'BUY%%'
                ORDER BY date LIMIT 1
            """, (symbol, pred_date))
        else:  # judge
            cur.execute("""
                SELECT date, final_action as action FROM judge_daily_analysis
                WHERE symbol = %s AND date > %s AND final_action LIKE 'BUY%%'
                ORDER BY date LIMIT 1
            """, (symbol, pred_date))

        transition = cur.fetchone()

        # Also check price movement for return calculation
        cur.execute("""
            SELECT MIN(low) as min_low, MAX(high) as max_high
            FROM daily_prices
            WHERE symbol = %s AND date > %s AND date <= %s
        """, (symbol, pred_date, pred_date + timedelta(days=wd_max + 5)))
        price_range = cur.fetchone()

        ltp = p["ltp_at_prediction"] or 0
        max_gain = 0
        max_loss = 0
        if price_range and ltp > 0:
            if price_range["max_high"]:
                max_gain = (price_range["max_high"] - ltp) / ltp * 100
            if price_range["min_low"]:
                max_loss = (price_range["min_low"] - ltp) / ltp * 100

        if transition:
            t_days = (transition["date"] - pred_date).days
            within_window = wd_min <= t_days <= wd_max

            if within_window:
                outcome = "CORRECT"
                reason = f"Transitioned to {transition['action']} in {t_days}d (within {wd_min}-{wd_max}d window)"
            else:
                outcome = "PARTIAL"
                reason = f"Transitioned to {transition['action']} in {t_days}d (window was {wd_min}-{wd_max}d)"

            cur.execute("""
                UPDATE prediction_tracker SET
                    transitioned_to = %s, transition_date = %s,
                    transition_days = %s, transition_within_window = %s,
                    max_gain_pct = %s, max_loss_pct = %s,
                    outcome = %s, outcome_reason = %s,
                    verified_at = NOW()
                WHERE id = %s
            """, (
                transition["action"], transition["date"],
                t_days, within_window,
                round(max_gain, 2), round(max_loss, 2),
                outcome, reason, p["id"],
            ))
            updated += 1

        elif days_elapsed > wd_max + 3:
            # Wait window expired, no transition
            outcome = "WRONG"
            reason = f"No BUY transition within {wd_max}d window ({days_elapsed}d elapsed)"

            cur.execute("""
                UPDATE prediction_tracker SET
                    max_gain_pct = %s, max_loss_pct = %s,
                    outcome = %s, outcome_reason = %s,
                    verified_at = NOW()
                WHERE id = %s
            """, (round(max_gain, 2), round(max_loss, 2), outcome, reason, p["id"]))
            updated += 1

    conn.commit()
    conn.close()
    logger.info(f"Verified {updated} HOLD/WAIT predictions")
    return updated


def verify_buy_outcomes(lookback_days: int = 30):
    """Check if BUY predictions hit T1/T2 or SL.

    Logic:
    - For each PENDING prediction where action contained BUY:
    - Check daily_prices for actual prices in subsequent days
    - CORRECT if T1 hit before SL (or no SL hit)
    - WRONG if SL hit before T1
    """
    conn = get_conn()
    cur = conn.cursor()
    today = datetime.now(DSE_TZ).date()

    cur.execute("""
        SELECT id, date, symbol, source, action, ltp_at_prediction,
               entry_low, entry_high, sl, t1, t2,
               wait_days_max
        FROM prediction_tracker
        WHERE outcome = 'PENDING'
          AND action LIKE 'BUY%%'
          AND date < CURRENT_DATE - INTERVAL '3 days'
          AND date > CURRENT_DATE - INTERVAL '%s days'
        ORDER BY date
    """, (lookback_days,))
    pending = cur.fetchall()

    if not pending:
        conn.close()
        logger.info("No BUY predictions to verify")
        return 0

    updated = 0
    for p in pending:
        pred_date = p["date"]
        symbol = p["symbol"]
        ltp = p["ltp_at_prediction"] or 0
        sl = p["sl"] or 0
        t1 = p["t1"] or 0
        t2 = p["t2"] or 0
        window = min(p["wait_days_max"] or 10, 30)

        # Get daily prices after prediction date
        cur.execute("""
            SELECT date, high, low, close FROM daily_prices
            WHERE symbol = %s AND date > %s
            ORDER BY date LIMIT %s
        """, (symbol, pred_date, window))
        prices = cur.fetchall()

        if len(prices) < 2:
            continue

        t1_hit_date = None
        t1_hit_days = None
        t2_hit_date = None
        t2_hit_days = None
        sl_hit_date = None
        sl_hit_days = None
        max_high = 0
        min_low = float("inf")

        for i, pr in enumerate(prices, 1):
            high = pr["high"] or 0
            low = pr["low"] or 0
            if high > max_high:
                max_high = high
            if low < min_low:
                min_low = low

            if t1 > 0 and high >= t1 and not t1_hit_date:
                t1_hit_date = pr["date"]
                t1_hit_days = i
            if t2 > 0 and high >= t2 and not t2_hit_date:
                t2_hit_date = pr["date"]
                t2_hit_days = i
            if sl > 0 and low <= sl and not sl_hit_date:
                sl_hit_date = pr["date"]
                sl_hit_days = i

        max_gain = ((max_high - ltp) / ltp * 100) if ltp > 0 else 0
        max_loss = ((min_low - ltp) / ltp * 100) if ltp > 0 else 0

        # Determine outcome
        last_close = prices[-1]["close"] or ltp
        final_return = ((last_close - ltp) / ltp * 100) if ltp > 0 else 0

        if t1_hit_date and (not sl_hit_date or t1_hit_days <= sl_hit_days):
            outcome = "CORRECT"
            reason = f"T1 ({t1:.1f}) hit in {t1_hit_days}d"
            if t2_hit_date:
                reason += f", T2 ({t2:.1f}) also hit in {t2_hit_days}d"
        elif sl_hit_date and (not t1_hit_date or sl_hit_days < t1_hit_days):
            outcome = "WRONG"
            reason = f"SL ({sl:.1f}) hit in {sl_hit_days}d before T1"
        elif (today - pred_date).days > window + 3:
            # Window expired
            if final_return > 0:
                outcome = "PARTIAL"
                reason = f"Window expired, gain {final_return:+.1f}% but T1 not hit"
            else:
                outcome = "WRONG"
                reason = f"Window expired, return {final_return:+.1f}%"
        else:
            continue  # Still within window, leave PENDING

        cur.execute("""
            UPDATE prediction_tracker SET
                t1_hit_date = %s, t1_hit_days = %s,
                t2_hit_date = %s, t2_hit_days = %s,
                sl_hit_date = %s, sl_hit_days = %s,
                max_gain_pct = %s, max_loss_pct = %s,
                final_return_pct = %s,
                outcome = %s, outcome_reason = %s,
                verified_at = NOW()
            WHERE id = %s
        """, (
            t1_hit_date, t1_hit_days,
            t2_hit_date, t2_hit_days,
            sl_hit_date, sl_hit_days,
            round(max_gain, 2), round(max_loss, 2),
            round(final_return, 2),
            outcome, reason, p["id"],
        ))
        updated += 1

    conn.commit()
    conn.close()
    logger.info(f"Verified {updated} BUY predictions")
    return updated


def compute_accuracy_summaries(date_str: str | None = None):
    """Aggregate prediction_tracker into accuracy_summary for each source and period."""
    if not date_str:
        date_str = datetime.now(DSE_TZ).strftime("%Y-%m-%d")

    conn = get_conn()
    cur = conn.cursor()

    for source in ("algo", "llm", "judge"):
        for period, days in [("7d", 7), ("30d", 30), ("90d", 90)]:
            cur.execute("""
                SELECT
                    COUNT(*) as total,
                    COUNT(CASE WHEN outcome = 'CORRECT' THEN 1 END) as correct,
                    COUNT(CASE WHEN outcome = 'WRONG' THEN 1 END) as wrong,
                    COUNT(CASE WHEN outcome = 'PENDING' THEN 1 END) as pending,
                    AVG(CASE WHEN final_return_pct IS NOT NULL THEN final_return_pct END) as avg_return,
                    -- BUY accuracy
                    COUNT(CASE WHEN action LIKE 'BUY%%' AND outcome = 'CORRECT' THEN 1 END) as buy_correct,
                    COUNT(CASE WHEN action LIKE 'BUY%%' AND outcome IN ('CORRECT','WRONG','PARTIAL') THEN 1 END) as buy_total,
                    -- HOLD transition accuracy
                    COUNT(CASE WHEN action IN ('HOLD/WAIT') AND outcome = 'CORRECT' THEN 1 END) as hold_correct,
                    COUNT(CASE WHEN action IN ('HOLD/WAIT') AND outcome IN ('CORRECT','WRONG','PARTIAL') THEN 1 END) as hold_total,
                    -- T1/SL rates
                    COUNT(CASE WHEN t1_hit_days IS NOT NULL THEN 1 END) as t1_hits,
                    COUNT(CASE WHEN sl_hit_days IS NOT NULL THEN 1 END) as sl_hits,
                    COUNT(CASE WHEN action LIKE 'BUY%%' THEN 1 END) as buy_all
                FROM prediction_tracker
                WHERE source = %s
                  AND date > CURRENT_DATE - INTERVAL '%s days'
            """, (source, days))
            r = cur.fetchone()

            total = r["total"] or 0
            verified = (r["correct"] or 0) + (r["wrong"] or 0)
            accuracy = (r["correct"] / verified * 100) if verified > 0 else None
            buy_total = r["buy_total"] or 0
            buy_acc = (r["buy_correct"] / buy_total * 100) if buy_total > 0 else None
            hold_total = r["hold_total"] or 0
            hold_acc = (r["hold_correct"] / hold_total * 100) if hold_total > 0 else None
            buy_all = r["buy_all"] or 0
            t1_rate = (r["t1_hits"] / buy_all * 100) if buy_all > 0 else None
            sl_rate = (r["sl_hits"] / buy_all * 100) if buy_all > 0 else None

            try:
                cur.execute("""
                    INSERT INTO accuracy_summary
                        (date, source, period, total_predictions, correct, wrong, pending,
                         accuracy_pct, avg_return_pct, buy_accuracy_pct,
                         hold_transition_accuracy_pct, t1_hit_rate, sl_hit_rate)
                    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                    ON CONFLICT (date, source, period) DO UPDATE SET
                        total_predictions = EXCLUDED.total_predictions,
                        correct = EXCLUDED.correct, wrong = EXCLUDED.wrong,
                        pending = EXCLUDED.pending, accuracy_pct = EXCLUDED.accuracy_pct,
                        avg_return_pct = EXCLUDED.avg_return_pct,
                        buy_accuracy_pct = EXCLUDED.buy_accuracy_pct,
                        hold_transition_accuracy_pct = EXCLUDED.hold_transition_accuracy_pct,
                        t1_hit_rate = EXCLUDED.t1_hit_rate,
                        sl_hit_rate = EXCLUDED.sl_hit_rate
                """, (
                    date_str, source, period, total,
                    r["correct"] or 0, r["wrong"] or 0, r["pending"] or 0,
                    round(accuracy, 2) if accuracy is not None else None,
                    round(r["avg_return"] or 0, 2) if r["avg_return"] else None,
                    round(buy_acc, 2) if buy_acc is not None else None,
                    round(hold_acc, 2) if hold_acc is not None else None,
                    round(t1_rate, 2) if t1_rate is not None else None,
                    round(sl_rate, 2) if sl_rate is not None else None,
                ))
            except Exception as e:
                logger.error(f"Accuracy summary {source}/{period}: {e}")

    conn.commit()
    conn.close()
    logger.info("Accuracy summaries computed for algo/llm/judge × 7d/30d/90d")


def run():
    """Main entry point."""
    logger.info("=== Prediction Verification starting ===")
    verify_hold_transitions(lookback_days=30)
    verify_buy_outcomes(lookback_days=30)
    compute_accuracy_summaries()
    logger.info("=== Prediction Verification complete ===")


if __name__ == "__main__":
    run()
