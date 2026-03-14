"""Floor Detection — indicator-floor approach for DSE stocks.

For each stock, tracks RSI, MACD histogram, and StochRSI from daily_analysis.
Finds historical lows in a configurable window, calculates the pace of decline,
and estimates days until each indicator reaches its floor.

A stock is "approaching floor" when ≥2 of 3 indicators are declining toward
their historical low and will reach it within ~3 trading days at current pace.
"""

import logging
import os
from datetime import date, timedelta

import psycopg2
import psycopg2.extras

logger = logging.getLogger(__name__)

DATABASE_URL = os.getenv(
    "DATABASE_URL",
    "postgresql://postgres.iihlezpkpllacztoaguc:160021062Ss%23%23"
    "@aws-1-ap-southeast-1.pooler.supabase.com:6543/postgres",
)


def _get_conn():
    conn = psycopg2.connect(DATABASE_URL, cursor_factory=psycopg2.extras.RealDictCursor)
    conn.autocommit = False
    return conn


# ════════════════════════════════════════════════════════════════
#  Core: compute floor data for all A-category stocks
# ════════════════════════════════════════════════════════════════


def compute_floor_table(lookback_months: int = 6, as_of_date: str | None = None) -> list[dict]:
    """Build floor detection table for all stocks.

    Args:
        lookback_months: How many months to look back for historical lows.
        as_of_date: ISO date string. If set, pretend "today" is this date
                    (for historical replay). None = latest available date.

    Returns list of dicts, one per stock, sorted by approaching_score DESC.
    """
    conn = _get_conn()
    cur = conn.cursor()

    # Determine the reference date
    if as_of_date:
        ref_date = as_of_date
    else:
        cur.execute("SELECT MAX(date) AS d FROM daily_analysis")
        row = cur.fetchone()
        ref_date = row["d"].isoformat() if row and row["d"] else date.today().isoformat()

    lookback_start = (
        date.fromisoformat(ref_date) - timedelta(days=lookback_months * 30)
    ).isoformat()

    # Get current indicators for each stock on ref_date
    cur.execute("""
        SELECT da.symbol, da.rsi, da.stoch_rsi, da.macd_hist,
               da.ltp, f.sector
        FROM daily_analysis da
        JOIN fundamentals f ON f.symbol = da.symbol
        WHERE da.date = %s
          AND UPPER(COALESCE(f.category, 'A')) = 'A'
    """, (ref_date,))
    current_rows = {r["symbol"]: r for r in cur.fetchall()}

    if not current_rows:
        conn.close()
        return []

    symbols = list(current_rows.keys())

    # Get historical indicator data for lookback window
    placeholders = ",".join(["%s"] * len(symbols))
    cur.execute(f"""
        SELECT symbol, date, rsi, stoch_rsi, macd_hist
        FROM daily_analysis
        WHERE symbol IN ({placeholders})
          AND date >= %s AND date <= %s
          AND rsi IS NOT NULL
        ORDER BY symbol, date
    """, symbols + [lookback_start, ref_date])
    history_rows = cur.fetchall()
    conn.close()

    # Group by symbol
    history: dict[str, list[dict]] = {}
    for r in history_rows:
        history.setdefault(r["symbol"], []).append(r)

    results = []
    for sym, cur_data in current_rows.items():
        hist = history.get(sym, [])
        if len(hist) < 10:
            continue

        cur_rsi = float(cur_data["rsi"] or 50)
        cur_stoch = float(cur_data["stoch_rsi"] or 50)
        cur_macd = float(cur_data["macd_hist"] or 0)
        ltp = float(cur_data["ltp"] or 0)

        # Extract time series
        rsi_series = [float(h["rsi"] or 50) for h in hist]
        stoch_series = [float(h["stoch_rsi"] or 50) for h in hist]
        macd_series = [float(h["macd_hist"] or 0) for h in hist]

        # Historical lows
        rsi_floor = min(rsi_series)
        stoch_floor = min(stoch_series)
        macd_floor = min(macd_series)

        # Historical highs (for bands)
        rsi_high = max(rsi_series)
        stoch_high = max(stoch_series)

        # Pace: rate of change over last 5 trading days
        def pace(series: list[float], n: int = 5) -> float:
            if len(series) < n + 1:
                return 0.0
            return (series[-1] - series[-n - 1]) / n

        rsi_pace = pace(rsi_series)
        stoch_pace = pace(stoch_series)
        macd_pace = pace(macd_series)

        # Days to floor at current pace
        def days_to_floor(current: float, floor: float, daily_pace: float) -> float | None:
            if daily_pace >= 0:
                return None  # Not declining
            gap = current - floor
            if gap <= 0:
                return 0  # Already at or below floor
            return round(gap / abs(daily_pace), 1)

        rsi_dtf = days_to_floor(cur_rsi, rsi_floor, rsi_pace)
        stoch_dtf = days_to_floor(cur_stoch, stoch_floor, stoch_pace)
        macd_dtf = days_to_floor(cur_macd, macd_floor, macd_pace)

        # How close to floor (0 = at floor, 100 = at ceiling)
        def floor_proximity(current: float, floor: float, high: float) -> float:
            rng = high - floor
            if rng <= 0:
                return 50
            return round(((current - floor) / rng) * 100, 1)

        rsi_prox = floor_proximity(cur_rsi, rsi_floor, rsi_high)
        stoch_prox = floor_proximity(cur_stoch, stoch_floor, stoch_high)

        # "Approaching floor" = declining + will reach floor in ≤ 3 days
        THRESHOLD_DAYS = 3.0
        rsi_approaching = rsi_dtf is not None and rsi_dtf <= THRESHOLD_DAYS
        stoch_approaching = stoch_dtf is not None and stoch_dtf <= THRESHOLD_DAYS
        macd_approaching = macd_dtf is not None and macd_dtf <= THRESHOLD_DAYS

        approaching_count = sum([rsi_approaching, stoch_approaching, macd_approaching])

        # Score: higher = closer to floor on more indicators
        # Weight: proximity (inverted) + pace bonus for approaching
        score = 0.0
        if rsi_approaching:
            score += (100 - rsi_prox) / 100 * 40
        if stoch_approaching:
            score += (100 - stoch_prox) / 100 * 30
        if macd_approaching:
            score += 30  # MACD doesn't have a 0-100 range

        results.append({
            "symbol": sym,
            "sector": cur_data.get("sector"),
            "ltp": round(ltp, 1),
            # Current values
            "rsi": round(cur_rsi, 1),
            "stoch_rsi": round(cur_stoch, 1),
            "macd_hist": round(cur_macd, 4),
            # Floor (historical lows)
            "rsi_floor": round(rsi_floor, 1),
            "stoch_floor": round(stoch_floor, 1),
            "macd_floor": round(macd_floor, 4),
            # Ceiling (historical highs)
            "rsi_high": round(rsi_high, 1),
            "stoch_high": round(stoch_high, 1),
            # Proximity to floor (0 = at floor, 100 = at ceiling)
            "rsi_proximity": rsi_prox,
            "stoch_proximity": stoch_prox,
            # Pace (per trading day)
            "rsi_pace": round(rsi_pace, 2),
            "stoch_pace": round(stoch_pace, 2),
            "macd_pace": round(macd_pace, 4),
            # Days to floor
            "rsi_days_to_floor": rsi_dtf,
            "stoch_days_to_floor": stoch_dtf,
            "macd_days_to_floor": macd_dtf,
            # Approaching flags
            "rsi_approaching": rsi_approaching,
            "stoch_approaching": stoch_approaching,
            "macd_approaching": macd_approaching,
            "approaching_count": approaching_count,
            "score": round(score, 1),
        })

    # Sort: stocks approaching floor first, then by score
    results.sort(key=lambda r: (-r["approaching_count"], -r["score"]))
    return results


def get_available_dates(limit: int = 60) -> list[str]:
    """Return the last N trading dates that have daily_analysis data."""
    conn = _get_conn()
    cur = conn.cursor()
    cur.execute("""
        SELECT DISTINCT date FROM daily_analysis
        ORDER BY date DESC LIMIT %s
    """, (limit,))
    dates = [r["date"].isoformat() for r in cur.fetchall()]
    conn.close()
    return dates
