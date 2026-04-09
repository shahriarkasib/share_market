"""
Live Event Alert Scanner — runs every 5 minutes during trading.

Compares live prices against price_structure levels and triggers alerts:
- Price crossed pivot R1/R2/R3 or S1/S2/S3
- Price broke above resistance (from historical S/R)
- Price dropped to strong support
- Price broke 52-week high/low
- Gap up/down > 2% at open
- Volume spike > 2x average
- Bullish/bearish candle forming at key level
- Fibonacci level breakout

Stores alerts in `live_alerts` table. Frontend reads and displays.
"""

import sys
import os
import json
import logging
from datetime import datetime, date

import psycopg2
import psycopg2.extras

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from config import DATABASE_URL

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s", datefmt="%H:%M:%S")
log = logging.getLogger(__name__)

CREATE_TABLE = """
CREATE TABLE IF NOT EXISTS live_alerts (
    id SERIAL PRIMARY KEY,
    symbol TEXT NOT NULL,
    date DATE NOT NULL DEFAULT CURRENT_DATE,
    time TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    alert_type TEXT NOT NULL,
    severity TEXT NOT NULL DEFAULT 'INFO',
    price DOUBLE PRECISION,
    level_name TEXT,
    level_price DOUBLE PRECISION,
    message TEXT NOT NULL,
    extra JSONB
);
CREATE INDEX IF NOT EXISTS idx_alerts_date ON live_alerts(date DESC);
CREATE INDEX IF NOT EXISTS idx_alerts_symbol ON live_alerts(symbol, date);
"""


def get_conn():
    return psycopg2.connect(DATABASE_URL, cursor_factory=psycopg2.extras.RealDictCursor)


def ensure_table(conn):
    cur = conn.cursor()
    for stmt in CREATE_TABLE.strip().split(";"):
        stmt = stmt.strip()
        if stmt:
            cur.execute(stmt)
    conn.commit()
    cur.close()


def already_alerted(conn, symbol: str, alert_type: str, level_name: str = None) -> bool:
    """Check if this alert was already fired today to avoid duplicates."""
    cur = conn.cursor()
    if level_name:
        cur.execute(
            "SELECT 1 FROM live_alerts WHERE symbol = %s AND alert_type = %s AND level_name = %s AND date = CURRENT_DATE LIMIT 1",
            (symbol, alert_type, level_name),
        )
    else:
        cur.execute(
            "SELECT 1 FROM live_alerts WHERE symbol = %s AND alert_type = %s AND date = CURRENT_DATE LIMIT 1",
            (symbol, alert_type),
        )
    result = cur.fetchone()
    cur.close()
    return result is not None


def fire_alert(conn, symbol: str, alert_type: str, severity: str, price: float,
               level_name: str, level_price: float, message: str, extra: dict = None):
    """Store an alert if not already fired today."""
    if already_alerted(conn, symbol, alert_type, level_name):
        return False

    cur = conn.cursor()
    cur.execute("""
        INSERT INTO live_alerts (symbol, alert_type, severity, price, level_name, level_price, message, extra)
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
    """, (symbol, alert_type, severity, price, level_name, level_price, message,
          json.dumps(extra) if extra else None))
    conn.commit()
    cur.close()
    log.info(f"  ALERT [{severity}] {symbol}: {message}")
    return True


def scan_all(conn):
    """Run all alert checks against current live prices."""
    cur = conn.cursor()

    # Load live prices + price structure + indicators in one query
    cur.execute("""
        SELECT
            lp.symbol, lp.ltp, lp.open, lp.close_prev, lp.change_pct, lp.volume,
            lp.high, lp.low,
            f.category, f.high_52w, f.low_52w,
            si.avg_volume_20, si.rsi_14, si.cmf_20, si.cmf_pos_streak, si.adx_14, si.ma_aligned,
            ps.pivot_daily, ps.pivot_weekly, ps.support_levels, ps.resistance_levels,
            ps.fib_levels, ps.swing_structure, ps.mean_reversion_score,
            ps.squeeze_active, ps.squeeze_json
        FROM live_prices lp
        LEFT JOIN fundamentals f ON lp.symbol = f.symbol
        LEFT JOIN stock_indicators si ON lp.symbol = si.symbol
            AND si.timeframe = 'daily'
            AND si.date = (SELECT MAX(date) FROM stock_indicators WHERE timeframe = 'daily')
        LEFT JOIN price_structure ps ON lp.symbol = ps.symbol
            AND ps.date = (SELECT MAX(date) FROM price_structure)
        WHERE lp.ltp > 0 AND f.category IN ('A', 'B')
    """)
    rows = cur.fetchall()
    cur.close()

    alert_count = 0

    for r in rows:
        symbol = r["symbol"]
        ltp = float(r["ltp"] or 0)
        open_price = float(r["open"] or 0)
        prev_close = float(r["close_prev"] or 0)
        change_pct = float(r["change_pct"] or 0)
        volume = int(r["volume"] or 0)
        high = float(r["high"] or 0)
        low = float(r["low"] or 0)
        avg_vol = float(r["avg_volume_20"] or 0)
        rsi = r["rsi_14"]
        cmf = r["cmf_20"]
        cmf_streak = r["cmf_pos_streak"] or 0
        adx = r["adx_14"]
        ma_aligned = r["ma_aligned"]
        high_52w = float(r["high_52w"] or 0)
        low_52w = float(r["low_52w"] or 0)
        pivot = r["pivot_daily"] or {}
        support_levels = r["support_levels"] or []
        resistance_levels = r["resistance_levels"] or []
        fib = r["fib_levels"] or {}
        swing = r["swing_structure"]
        mr_score = r["mean_reversion_score"] or 0

        if ltp == 0 or prev_close == 0:
            continue

        # --- 1. Pivot Breakouts ---
        pivot_p = float(pivot.get("p", 0))
        pivot_r1 = float(pivot.get("r1", 0))
        pivot_r2 = float(pivot.get("r2", 0))
        pivot_r3 = float(pivot.get("r3", 0))
        pivot_s1 = float(pivot.get("s1", 0))
        pivot_s2 = float(pivot.get("s2", 0))

        if pivot_r2 and ltp > pivot_r2 and prev_close <= pivot_r2:
            alert_count += fire_alert(conn, symbol, "PIVOT_BREAK", "HIGH", ltp,
                "R2", pivot_r2, f"Broke above Pivot R2 ({pivot_r2}) — strong bullish momentum",
                {"pivot": pivot, "change_pct": change_pct})

        elif pivot_r1 and ltp > pivot_r1 and prev_close <= pivot_r1:
            alert_count += fire_alert(conn, symbol, "PIVOT_BREAK", "MEDIUM", ltp,
                "R1", pivot_r1, f"Crossed Pivot R1 ({pivot_r1}) — bullish, next target R2 ({pivot_r2})",
                {"pivot": pivot, "change_pct": change_pct})

        if pivot_s1 and ltp < pivot_s1 and prev_close >= pivot_s1:
            alert_count += fire_alert(conn, symbol, "PIVOT_BREAK", "MEDIUM", ltp,
                "S1", pivot_s1, f"Dropped below Pivot S1 ({pivot_s1}) — bearish, watch S2 ({pivot_s2})",
                {"pivot": pivot, "change_pct": change_pct})

        if pivot_s2 and ltp < pivot_s2 and prev_close >= pivot_s2:
            alert_count += fire_alert(conn, symbol, "PIVOT_BREAK", "HIGH", ltp,
                "S2", pivot_s2, f"Broke below Pivot S2 ({pivot_s2}) — strong selling",
                {"pivot": pivot, "change_pct": change_pct})

        # --- 2. Resistance Breakout ---
        for res in resistance_levels[:3]:
            res_price = float(res["price"])
            touches = res["touches"]
            if ltp > res_price and prev_close <= res_price:
                severity = "HIGH" if touches >= 5 else "MEDIUM"
                alert_count += fire_alert(conn, symbol, "RESISTANCE_BREAK", severity, ltp,
                    f"RES_{res_price}", res_price,
                    f"Broke above resistance {res_price} ({touches} touches) — potential breakout",
                    {"resistance": res, "volume": volume, "avg_volume": avg_vol})

        # --- 3. Support Touch ---
        for sup in support_levels[:3]:
            sup_price = float(sup["price"])
            touches = sup["touches"]
            dist_pct = abs(ltp - sup_price) / ltp * 100 if ltp > 0 else 999
            if dist_pct < 1.5 and ltp <= sup_price * 1.015:
                severity = "HIGH" if touches >= 5 else "MEDIUM"
                alert_count += fire_alert(conn, symbol, "SUPPORT_TOUCH", severity, ltp,
                    f"SUP_{sup_price}", sup_price,
                    f"Touching support {sup_price} ({touches} touches) — potential bounce zone",
                    {"support": sup, "rsi": rsi, "cmf": cmf, "mr_score": mr_score})

        # --- 4. 52-Week High/Low ---
        if high_52w and ltp > high_52w:
            alert_count += fire_alert(conn, symbol, "52W_HIGH", "HIGH", ltp,
                "52W_HIGH", high_52w, f"NEW 52-WEEK HIGH at {ltp} (prev high: {high_52w})")

        if low_52w and ltp < low_52w and ltp > 0:
            alert_count += fire_alert(conn, symbol, "52W_LOW", "HIGH", ltp,
                "52W_LOW", low_52w, f"NEW 52-WEEK LOW at {ltp} (prev low: {low_52w})")

        # --- 5. Gap Alert (at open) ---
        if open_price > 0 and prev_close > 0:
            gap_pct = (open_price - prev_close) / prev_close * 100
            if gap_pct > 2:
                alert_count += fire_alert(conn, symbol, "GAP_UP", "MEDIUM", ltp,
                    "GAP", open_price, f"Gap up +{gap_pct:.1f}% (opened {open_price} vs prev close {prev_close})",
                    {"gap_pct": gap_pct, "volume": volume})
            elif gap_pct < -2:
                alert_count += fire_alert(conn, symbol, "GAP_DOWN", "MEDIUM", ltp,
                    "GAP", open_price, f"Gap down {gap_pct:.1f}% (opened {open_price} vs prev close {prev_close})",
                    {"gap_pct": gap_pct, "volume": volume})

        # --- 6. Volume Spike ---
        if avg_vol > 0 and volume > avg_vol * 2.5:
            vol_ratio = volume / avg_vol
            alert_count += fire_alert(conn, symbol, "VOLUME_SPIKE", "MEDIUM", ltp,
                "VOL", volume, f"Volume spike {vol_ratio:.1f}x average ({volume:,} vs avg {int(avg_vol):,})",
                {"vol_ratio": vol_ratio, "change_pct": change_pct})

        # --- 7. Fibonacci Level Break ---
        fib_ext = fib.get("extension", {})
        fib_trend = fib.get("trend", "")
        if fib_trend == "UP":
            for level_name_str, fib_price in fib_ext.items():
                fib_p = float(fib_price)
                if fib_p > 0 and ltp > fib_p and prev_close <= fib_p:
                    alert_count += fire_alert(conn, symbol, "FIB_BREAK", "MEDIUM", ltp,
                        f"FIB_{level_name_str}", fib_p,
                        f"Broke above Fib extension {level_name_str} ({fib_p}) — momentum continuation",
                        {"fib": fib})

        # --- 8. Mean Reversion at Support ---
        if mr_score >= 60 and rsi and float(rsi) < 40:
            alert_count += fire_alert(conn, symbol, "BOUNCE_SETUP", "HIGH", ltp,
                "MR", mr_score, f"High bounce probability (MR score {mr_score}/100, RSI {float(rsi):.0f}) at support",
                {"mr_score": mr_score, "rsi": float(rsi), "cmf": cmf, "support": support_levels[:2] if support_levels else []})

        # --- 9. Circuit Breaker Approaching ---
        if change_pct > 8:
            alert_count += fire_alert(conn, symbol, "CIRCUIT_UP", "HIGH", ltp,
                "CB", ltp, f"Up +{change_pct:.1f}% — approaching +10% circuit breaker",
                {"change_pct": change_pct, "volume": volume})
        elif change_pct < -8:
            alert_count += fire_alert(conn, symbol, "CIRCUIT_DOWN", "HIGH", ltp,
                "CB", ltp, f"Down {change_pct:.1f}% — approaching -10% circuit breaker",
                {"change_pct": change_pct, "volume": volume})

        # --- 10. Squeeze / Triangle Alert ---
        squeeze_json = r.get("squeeze_json")
        if squeeze_json and isinstance(squeeze_json, dict) and squeeze_json.get("active"):
            sq_type = squeeze_json.get("type", "SQUEEZE")
            compression = squeeze_json.get("compression", 0)
            upper = squeeze_json.get("upper_bound", 0)
            lower = squeeze_json.get("lower_bound", 0)
            days = squeeze_json.get("days_to_apex")
            days_str = f", ~{days} days to apex" if days and days < 20 else ""
            alert_count += fire_alert(conn, symbol, "SQUEEZE", "MEDIUM", ltp,
                "SQUEEZE", ltp,
                f"SQUEEZE forming ({sq_type}, compression {compression:.0%}) — range {lower}-{upper}{days_str}. Big move coming. Watch for breakdown to support = buy signal.",
                {"squeeze": squeeze_json, "rsi": rsi, "support": support_levels[:2] if support_levels else []})

    return alert_count


def main():
    conn = get_conn()
    ensure_table(conn)

    # Clear old alerts (keep last 7 days)
    cur = conn.cursor()
    cur.execute("DELETE FROM live_alerts WHERE date < CURRENT_DATE - INTERVAL '7 days'")
    conn.commit()
    cur.close()

    count = scan_all(conn)
    conn.close()
    log.info(f"Scan complete. {count} new alerts fired.")


if __name__ == "__main__":
    main()
