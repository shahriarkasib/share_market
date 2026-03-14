"""Dividend / record-date impact analyzer.

Analyzes how stocks behave around record dates historically and generates
post-dividend buy signals.  All prices are rounded to DSE tick (0.10 BDT).
"""

import logging
import re
from datetime import date, datetime, timedelta
from typing import Optional

from database import get_connection

logger = logging.getLogger(__name__)


# ── Helpers ─────────────────────────────────────────────────


def _parse_dividend_pct(details: str) -> float:
    """Extract cash dividend percentage from event details text.

    Returns the percentage (e.g. 20.0 means 20% of face value = 2 BDT on
    a 10 BDT face-value stock).
    """
    if not details:
        return 0.0
    m = re.search(r"Cash(?:\s+Dividend)?[:\s]*(\d+(?:\.\d+)?)%", details, re.I)
    return float(m.group(1)) if m else 0.0


def _tick_round(price: float) -> float:
    """Round to nearest DSE tick (0.10 BDT)."""
    return round(round(price / 0.1) * 0.1, 1)


def _pct_change(old: float, new: float) -> float:
    """Percentage change from old to new, safe against zero."""
    if old == 0:
        return 0.0
    return round((new - old) / old * 100, 2)


def _compute_rsi(closes: list[float], period: int = 14) -> Optional[float]:
    """Compute RSI from a list of closing prices (most recent last).

    Returns None if not enough data.
    """
    if len(closes) < period + 1:
        return None
    deltas = [closes[i] - closes[i - 1] for i in range(1, len(closes))]
    gains = [d if d > 0 else 0.0 for d in deltas]
    losses = [-d if d < 0 else 0.0 for d in deltas]

    avg_gain = sum(gains[:period]) / period
    avg_loss = sum(losses[:period]) / period

    for i in range(period, len(gains)):
        avg_gain = (avg_gain * (period - 1) + gains[i]) / period
        avg_loss = (avg_loss * (period - 1) + losses[i]) / period

    if avg_loss == 0:
        return 100.0
    rs = avg_gain / avg_loss
    return round(100 - 100 / (1 + rs), 2)


# ── 1. Record-date impact analysis for a single stock ───────


def analyze_record_date_impact(symbol: str) -> dict:
    """Analyze historical price behavior around record dates for *symbol*.

    For each RECORD_DATE event, fetches daily prices 5 days before and
    20 days after, and measures:
      - ex-date drop %  (day-of-record close vs. next trading day close)
      - bottom day       (trading day offset where price was lowest)
      - bottom drop %    (lowest close vs. pre-record close)
      - recovery by day 7, 14, 20

    Returns ``{"symbol", "events": [...], "averages": {...}}``.
    """
    sym = symbol.upper()
    conn = get_connection()

    # Fetch all RECORD_DATE events for this symbol
    events = conn.execute(
        "SELECT date, title, details, amount "
        "FROM corporate_events "
        "WHERE symbol = ? AND event_type = 'RECORD_DATE' "
        "ORDER BY date ASC",
        (sym,),
    ).fetchall()

    if not events:
        conn.close()
        return {"symbol": sym, "events": [], "averages": {}}

    results = []

    for ev in events:
        record_date = ev["date"] if isinstance(ev["date"], date) else datetime.strptime(str(ev["date"]), "%Y-%m-%d").date()
        dividend_pct = _parse_dividend_pct(ev.get("details", "") or "")

        # Get prices from 10 days before to 30 days after (extra buffer for
        # non-trading days / weekends / holidays)
        start = record_date - timedelta(days=10)
        end = record_date + timedelta(days=35)

        prices = conn.execute(
            "SELECT date, close, volume FROM daily_prices "
            "WHERE symbol = ? AND date >= ? AND date <= ? "
            "ORDER BY date ASC",
            (sym, start.isoformat(), end.isoformat()),
        ).fetchall()

        if len(prices) < 5:
            continue

        # Convert to lists
        dates = [p["date"] for p in prices]
        closes = [float(p["close"]) for p in prices]

        # Find record date index (or closest trading day on or before)
        rec_idx = None
        for i, d in enumerate(dates):
            d_val = d if isinstance(d, date) else datetime.strptime(str(d), "%Y-%m-%d").date()
            if d_val <= record_date:
                rec_idx = i
            else:
                break

        if rec_idx is None or rec_idx + 1 >= len(closes):
            continue

        pre_close = closes[rec_idx]  # close on record date (or last trading day before)

        # Post-record prices (trading days after record date)
        post_closes = closes[rec_idx + 1:]
        post_dates = dates[rec_idx + 1:]

        if not post_closes:
            continue

        # Ex-date drop (first trading day after record date)
        ex_drop_pct = _pct_change(pre_close, post_closes[0])

        # Bottom: lowest close in the 20 trading days after
        window = post_closes[:20]
        bottom_price = min(window)
        bottom_day = window.index(bottom_price) + 1  # 1-indexed trading day
        bottom_drop_pct = _pct_change(pre_close, bottom_price)

        # Recovery checks at day 7, 14, 20
        recovery = {}
        for check_day in (7, 14, 20):
            if len(post_closes) >= check_day:
                recovery[f"day_{check_day}_pct"] = _pct_change(pre_close, post_closes[check_day - 1])
            else:
                recovery[f"day_{check_day}_pct"] = None

        # Expected ex-price drop based on dividend (face value 10 BDT)
        expected_drop_pct = 0.0
        if dividend_pct > 0 and pre_close > 0:
            cash_per_share = dividend_pct / 100 * 10  # face value 10 BDT
            expected_drop_pct = round(-cash_per_share / pre_close * 100, 2)

        results.append({
            "record_date": record_date.isoformat(),
            "dividend_pct": dividend_pct,
            "pre_close": _tick_round(pre_close),
            "ex_close": _tick_round(post_closes[0]),
            "ex_drop_pct": ex_drop_pct,
            "expected_drop_pct": expected_drop_pct,
            "excess_drop_pct": round(ex_drop_pct - expected_drop_pct, 2) if expected_drop_pct else None,
            "bottom_day": bottom_day,
            "bottom_price": _tick_round(bottom_price),
            "bottom_drop_pct": bottom_drop_pct,
            **recovery,
        })

    conn.close()

    # Compute averages across all events
    averages = {}
    if results:
        numeric_keys = [
            "ex_drop_pct", "bottom_day", "bottom_drop_pct",
            "day_7_pct", "day_14_pct", "day_20_pct",
        ]
        for key in numeric_keys:
            vals = [r[key] for r in results if r.get(key) is not None]
            averages[f"avg_{key}"] = round(sum(vals) / len(vals), 2) if vals else None

        averages["event_count"] = len(results)

    return {"symbol": sym, "events": results, "averages": averages}


# ── 2. Post-dividend buying opportunities ───────────────────


def find_post_dividend_opportunities(days_after_record: int = 7) -> list[dict]:
    """Find stocks where the post-record-date price drop exceeds the
    expected dividend-adjusted ex-price, with volume and RSI confirmation.

    Scans stocks that had a RECORD_DATE between *days_after_record* and
    20 days ago.  Filters for:
      - price dropped more than the expected dividend ex-drop
      - current volume > 20-day average (accumulation)
      - RSI < 40 (oversold from the dividend drop)

    Returns list of opportunity dicts sorted by excess_drop descending.
    """
    today = date.today()
    window_start = today - timedelta(days=20)
    window_end = today - timedelta(days=max(days_after_record, 1))

    conn = get_connection()

    # Recent RECORD_DATE events in our window
    events = conn.execute(
        "SELECT date, symbol, details FROM corporate_events "
        "WHERE event_type = 'RECORD_DATE' AND date >= ? AND date <= ? "
        "ORDER BY date DESC",
        (window_start.isoformat(), window_end.isoformat()),
    ).fetchall()

    if not events:
        conn.close()
        return []

    opportunities = []

    for ev in events:
        sym = ev["symbol"]
        record_date = ev["date"] if isinstance(ev["date"], date) else datetime.strptime(str(ev["date"]), "%Y-%m-%d").date()
        dividend_pct = _parse_dividend_pct(ev.get("details", "") or "")
        days_since = (today - record_date).days

        # Get prices: 30 days before record date through today
        price_start = record_date - timedelta(days=40)
        prices = conn.execute(
            "SELECT date, close, volume FROM daily_prices "
            "WHERE symbol = ? AND date >= ? AND date <= ? "
            "ORDER BY date ASC",
            (sym, price_start.isoformat(), today.isoformat()),
        ).fetchall()

        if len(prices) < 25:
            continue

        closes = [float(p["close"]) for p in prices]
        volumes = [int(p["volume"] or 0) for p in prices]
        dates_list = [p["date"] for p in prices]

        # Find record date index
        rec_idx = None
        for i, d in enumerate(dates_list):
            d_val = d if isinstance(d, date) else datetime.strptime(str(d), "%Y-%m-%d").date()
            if d_val <= record_date:
                rec_idx = i

        if rec_idx is None or rec_idx >= len(closes) - 1:
            continue

        pre_close = closes[rec_idx]
        current_close = closes[-1]
        current_volume = volumes[-1]
        drop_pct = _pct_change(pre_close, current_close)

        # Expected ex-drop (cash dividend on face value 10 BDT)
        expected_drop = 0.0
        if dividend_pct > 0 and pre_close > 0:
            cash_per_share = dividend_pct / 100 * 10
            expected_drop = round(-cash_per_share / pre_close * 100, 2)

        excess_drop = round(drop_pct - expected_drop, 2) if expected_drop else None

        # Skip if price hasn't dropped more than expected
        if excess_drop is None or excess_drop >= 0:
            continue

        # Volume check: current volume vs. 20-day average
        recent_vols = volumes[-21:-1] if len(volumes) > 21 else volumes[:-1]
        avg_vol = sum(recent_vols) / len(recent_vols) if recent_vols else 1
        volume_ratio = round(current_volume / avg_vol, 2) if avg_vol > 0 else 0

        if volume_ratio < 1.0:
            continue

        # RSI check
        rsi = _compute_rsi(closes[-30:])
        if rsi is None or rsi >= 40:
            continue

        opportunities.append({
            "symbol": sym,
            "record_date": record_date.isoformat(),
            "days_since": days_since,
            "drop_pct": drop_pct,
            "expected_drop": expected_drop,
            "excess_drop": excess_drop,
            "current_price": _tick_round(current_close),
            "pre_record_price": _tick_round(pre_close),
            "volume_ratio": volume_ratio,
            "rsi": rsi,
            "dividend_pct": dividend_pct,
        })

    conn.close()

    # Sort by excess drop (most oversold first)
    opportunities.sort(key=lambda x: x["excess_drop"])
    return opportunities


# ── 3. Upcoming record dates ────────────────────────────────


def _batch_historical_averages(conn, symbols: list[str]) -> dict[str, dict]:
    """Compute historical record-date impact averages for multiple symbols in ONE query.

    Uses a single JOIN between corporate_events and daily_prices instead of
    N separate analyze_record_date_impact() calls.
    """
    if not symbols:
        return {}

    placeholders = ",".join(["?"] * len(symbols))

    # Get all historical RECORD_DATE events + surrounding prices in one shot
    rows = conn.execute(
        f"""SELECT ce.symbol, ce.date AS record_date, ce.details,
                   dp.date AS price_date, dp.close
            FROM corporate_events ce
            JOIN daily_prices dp ON dp.symbol = ce.symbol
                AND dp.date BETWEEN ce.date - INTERVAL '5 days' AND ce.date + INTERVAL '25 days'
            WHERE ce.event_type = 'RECORD_DATE'
              AND ce.symbol IN ({placeholders})
            ORDER BY ce.symbol, ce.date, dp.date""",
        symbols,
    ).fetchall()

    # Group by (symbol, record_date)
    from collections import defaultdict
    groups: dict[tuple, list] = defaultdict(list)
    for r in rows:
        key = (r["symbol"], str(r["record_date"]))
        groups[key].append({"date": r["price_date"], "close": float(r["close"] or 0)})

    # Compute per-event stats, then average per symbol
    symbol_stats: dict[str, list[dict]] = defaultdict(list)
    for (sym, rec_str), prices in groups.items():
        if len(prices) < 3:
            continue
        rec_date = datetime.strptime(rec_str, "%Y-%m-%d").date() if isinstance(rec_str, str) else rec_str
        dates = [p["date"] for p in prices]
        closes = [p["close"] for p in prices]

        # Find record date index
        rec_idx = None
        for i, d in enumerate(dates):
            d_val = d if isinstance(d, date) else datetime.strptime(str(d), "%Y-%m-%d").date()
            if d_val <= rec_date:
                rec_idx = i
            else:
                break
        if rec_idx is None or rec_idx + 1 >= len(closes) or closes[rec_idx] == 0:
            continue

        pre = closes[rec_idx]
        post = closes[rec_idx + 1:]
        if not post:
            continue

        ex_drop = (post[0] - pre) / pre * 100
        window = post[:20]
        bottom = min(window)
        bottom_day = window.index(bottom) + 1

        symbol_stats[sym].append({
            "ex_drop": ex_drop,
            "bottom_day": bottom_day,
        })

    # Average per symbol
    result: dict[str, dict] = {}
    for sym, stats in symbol_stats.items():
        n = len(stats)
        result[sym] = {
            "avg_ex_drop_pct": round(sum(s["ex_drop"] for s in stats) / n, 2),
            "avg_bottom_day": round(sum(s["bottom_day"] for s in stats) / n, 1),
            "event_count": n,
        }
    return result


def get_upcoming_record_dates(days_ahead: int = 30) -> list[dict]:
    """Return upcoming RECORD_DATE events enriched with current price and
    historical average post-record drop.

    Uses batch queries — no N+1 problem.
    """
    today = date.today()
    cutoff = today + timedelta(days=days_ahead)

    conn = get_connection()

    events = conn.execute(
        "SELECT date, symbol, title, details, amount "
        "FROM corporate_events "
        "WHERE event_type = 'RECORD_DATE' AND date >= ? AND date <= ? "
        "ORDER BY date ASC",
        (today.isoformat(), cutoff.isoformat()),
    ).fetchall()

    if not events:
        conn.close()
        return []

    # Get current live prices for all symbols at once
    symbols = list({ev["symbol"] for ev in events})
    placeholders = ",".join(["?" for _ in symbols])
    live_rows = conn.execute(
        f"SELECT symbol, ltp FROM live_prices WHERE symbol IN ({placeholders})",
        symbols,
    ).fetchall()
    live_map = {r["symbol"]: float(r["ltp"] or 0) for r in live_rows}

    # Batch historical impact — single query for all symbols
    impact_map = _batch_historical_averages(conn, symbols)
    conn.close()

    upcoming = []
    for ev in events:
        sym = ev["symbol"]
        record_date = ev["date"] if isinstance(ev["date"], date) else datetime.strptime(str(ev["date"]), "%Y-%m-%d").date()
        dividend_pct = _parse_dividend_pct(ev.get("details", "") or "")
        current_price = live_map.get(sym, 0)

        avgs = impact_map.get(sym, {})

        # Expected ex-price
        expected_ex_price = None
        if dividend_pct > 0 and current_price > 0:
            cash_per_share = dividend_pct / 100 * 10  # face value 10 BDT
            expected_ex_price = _tick_round(current_price - cash_per_share)

        days_until = (record_date - today).days

        upcoming.append({
            "symbol": sym,
            "record_date": record_date.isoformat(),
            "days_until": days_until,
            "dividend_pct": dividend_pct,
            "title": ev.get("title", ""),
            "current_price": _tick_round(current_price) if current_price else None,
            "expected_ex_price": expected_ex_price,
            "avg_historical_ex_drop_pct": avgs.get("avg_ex_drop_pct"),
            "avg_historical_bottom_day": avgs.get("avg_bottom_day"),
            "historical_events": avgs.get("event_count", 0),
        })

    return upcoming
