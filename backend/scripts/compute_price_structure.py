#!/usr/bin/env python3
"""
Compute price structure analysis for all stocks.
Detects swing points, support/resistance, gaps, Fibonacci levels,
pivot points, candlestick patterns, volume profile, and more.

Usage:
    ./venv/bin/python3 scripts/compute_price_structure.py
    ./venv/bin/python3 scripts/compute_price_structure.py --symbols GP,ACMELAB
    ./venv/bin/python3 scripts/compute_price_structure.py --dsex-only
"""

import sys
import os
import argparse
import json
import logging
import warnings
from datetime import datetime, date

import numpy as np
import pandas as pd
import psycopg2
import psycopg2.extras

warnings.filterwarnings("ignore", message=".*pandas only supports SQLAlchemy.*")

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from config import DATABASE_URL

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s", datefmt="%H:%M:%S")
log = logging.getLogger(__name__)


def get_conn():
    return psycopg2.connect(DATABASE_URL)


def get_dict_conn():
    return psycopg2.connect(DATABASE_URL, cursor_factory=psycopg2.extras.RealDictCursor)


# ---------------------------------------------------------------------------
# Data loaders
# ---------------------------------------------------------------------------

def load_prices(conn, symbol: str, days: int = 500) -> pd.DataFrame:
    df = pd.read_sql(
        "SELECT date, open, high, low, close, volume FROM daily_prices "
        "WHERE symbol = %s AND close > 0 ORDER BY date ASC",
        conn, params=(symbol,),
    )
    if df.empty:
        return df
    df["date"] = pd.to_datetime(df["date"], format="mixed")
    df = df.set_index("date").sort_index()
    df = df[~df.index.duplicated(keep="last")]
    if len(df) > days:
        df = df.iloc[-days:]
    return df


def load_dsex(conn, days: int = 500) -> pd.DataFrame:
    df = pd.read_sql(
        "SELECT date, dsex_index as close, "
        "COALESCE(total_volume, 0) as volume, COALESCE(total_value, 0) as total_value "
        "FROM dsex_history WHERE dsex_index > 0 ORDER BY date ASC",
        conn,
    )
    if df.empty:
        return df
    df["date"] = pd.to_datetime(df["date"], format="mixed")
    df = df.set_index("date").sort_index()
    df = df[~df.index.duplicated(keep="last")]
    # DSEX doesn't have OHLC, approximate from close
    df["open"] = df["close"].shift(1).fillna(df["close"])
    df["high"] = df["close"]  # approximate
    df["low"] = df["close"]
    if len(df) > days:
        df = df.iloc[-days:]
    return df


# ---------------------------------------------------------------------------
# 1. Swing Point Detection
# ---------------------------------------------------------------------------

def detect_swing_points(df: pd.DataFrame, window: int = 5) -> dict:
    """Detect swing highs/lows and classify trend structure."""
    if len(df) < window * 2 + 1:
        return {"structure": "UNKNOWN", "swings": [], "last_swing_high": None, "last_swing_low": None}

    highs = df["high"].values
    lows = df["low"].values
    dates = df.index

    swing_highs = []
    swing_lows = []

    for i in range(window, len(df) - window):
        # Swing high: highest high in window
        if highs[i] == max(highs[i - window:i + window + 1]):
            swing_highs.append({"date": str(dates[i].date()), "price": float(highs[i]), "idx": i})
        # Swing low: lowest low in window
        if lows[i] == min(lows[i - window:i + window + 1]):
            swing_lows.append({"date": str(dates[i].date()), "price": float(lows[i]), "idx": i})

    # Classify structure from last 4 swing points
    all_swings = []
    for s in swing_highs:
        all_swings.append({"type": "H", **s})
    for s in swing_lows:
        all_swings.append({"type": "L", **s})
    all_swings.sort(key=lambda x: x["idx"])

    structure = "UNKNOWN"
    if len(all_swings) >= 4:
        recent = all_swings[-4:]
        highs_list = [s for s in recent if s["type"] == "H"]
        lows_list = [s for s in recent if s["type"] == "L"]

        if len(highs_list) >= 2 and len(lows_list) >= 2:
            hh = highs_list[-1]["price"] > highs_list[-2]["price"]
            hl = lows_list[-1]["price"] > lows_list[-2]["price"]
            lh = highs_list[-1]["price"] < highs_list[-2]["price"]
            ll = lows_list[-1]["price"] < lows_list[-2]["price"]

            if hh and hl:
                structure = "UPTREND"
            elif lh and ll:
                structure = "DOWNTREND"
            elif hh and ll:
                structure = "EXPANDING"
            elif lh and hl:
                structure = "CONTRACTING"
            elif hl:
                structure = "HIGHER_LOWS"
            elif lh:
                structure = "LOWER_HIGHS"

    last_sh = swing_highs[-1]["price"] if swing_highs else None
    last_sl = swing_lows[-1]["price"] if swing_lows else None

    # Keep only last 10 swings for storage
    return {
        "structure": structure,
        "swings": all_swings[-10:],
        "last_swing_high": last_sh,
        "last_swing_low": last_sl,
    }


# ---------------------------------------------------------------------------
# 2. Historical Support/Resistance
# ---------------------------------------------------------------------------

def detect_support_resistance(df: pd.DataFrame, tolerance_pct: float = 1.5, min_touches: int = 2) -> dict:
    """Find price levels where stock reversed multiple times."""
    if len(df) < 20:
        return {"support": [], "resistance": []}

    close = df["close"].values
    high = df["high"].values
    low = df["low"].values
    current = close[-1]

    # Collect reversal points (swing highs and lows with smaller window)
    reversal_prices = []
    for i in range(2, len(df) - 2):
        if low[i] == min(low[i-2:i+3]):
            reversal_prices.append(float(low[i]))
        if high[i] == max(high[i-2:i+3]):
            reversal_prices.append(float(high[i]))

    if not reversal_prices:
        return {"support": [], "resistance": []}

    # Cluster nearby prices
    reversal_prices.sort()
    clusters = []
    current_cluster = [reversal_prices[0]]

    for p in reversal_prices[1:]:
        if abs(p - current_cluster[-1]) / current_cluster[-1] * 100 <= tolerance_pct:
            current_cluster.append(p)
        else:
            if len(current_cluster) >= min_touches:
                clusters.append(current_cluster)
            current_cluster = [p]
    if len(current_cluster) >= min_touches:
        clusters.append(current_cluster)

    # Convert clusters to levels
    levels = []
    for cluster in clusters:
        avg_price = round(np.mean(cluster), 1)
        levels.append({
            "price": avg_price,
            "touches": len(cluster),
            "strength": "STRONG" if len(cluster) >= 4 else "MODERATE" if len(cluster) >= 3 else "WEAK",
        })

    # Split into support (below current) and resistance (above current)
    support = sorted([l for l in levels if l["price"] < current * 0.995], key=lambda x: -x["price"])[:5]
    resistance = sorted([l for l in levels if l["price"] > current * 1.005], key=lambda x: x["price"])[:5]

    return {"support": support, "resistance": resistance}


# ---------------------------------------------------------------------------
# 3. Volume Profile
# ---------------------------------------------------------------------------

def compute_volume_profile(df: pd.DataFrame, n_bins: int = 30) -> list:
    """Compute volume at price levels."""
    if len(df) < 20:
        return []

    close = df["close"].values
    volume = df["volume"].values
    price_min, price_max = close.min(), close.max()

    if price_max == price_min:
        return []

    bin_edges = np.linspace(price_min, price_max, n_bins + 1)
    vol_profile = []

    for i in range(n_bins):
        mask = (close >= bin_edges[i]) & (close < bin_edges[i + 1])
        vol = float(volume[mask].sum())
        mid = round((bin_edges[i] + bin_edges[i + 1]) / 2, 1)
        vol_profile.append({"price": mid, "volume": vol})

    # Classify HVN/LVN
    volumes = [v["volume"] for v in vol_profile]
    if not volumes or max(volumes) == 0:
        return []

    avg_vol = np.mean(volumes)
    result = []
    for v in vol_profile:
        if v["volume"] > 0:
            vtype = "HVN" if v["volume"] > avg_vol * 1.5 else "LVN" if v["volume"] < avg_vol * 0.5 else "NORMAL"
            v["type"] = vtype
            result.append(v)

    # Return only significant nodes
    return [v for v in result if v["type"] in ("HVN", "LVN")][:10]


# ---------------------------------------------------------------------------
# 4. Gap Detection
# ---------------------------------------------------------------------------

def detect_gaps(df: pd.DataFrame, min_gap_pct: float = 1.0) -> list:
    """Find unfilled price gaps."""
    if len(df) < 2:
        return []

    gaps = []
    high = df["high"].values
    low = df["low"].values
    close = df["close"].values
    dates = df.index

    for i in range(1, len(df)):
        # Gap up: today's low > yesterday's high
        if low[i] > high[i-1]:
            gap_pct = (low[i] - high[i-1]) / high[i-1] * 100
            if gap_pct >= min_gap_pct:
                gaps.append({
                    "date": str(dates[i].date()),
                    "type": "up",
                    "gap_low": round(float(high[i-1]), 1),
                    "gap_high": round(float(low[i]), 1),
                    "gap_pct": round(gap_pct, 2),
                    "filled": False,
                })
        # Gap down: today's high < yesterday's low
        elif high[i] < low[i-1]:
            gap_pct = (low[i-1] - high[i]) / low[i-1] * 100
            if gap_pct >= min_gap_pct:
                gaps.append({
                    "date": str(dates[i].date()),
                    "type": "down",
                    "gap_low": round(float(high[i]), 1),
                    "gap_high": round(float(low[i-1]), 1),
                    "gap_pct": round(gap_pct, 2),
                    "filled": False,
                })

    # Check if gaps are filled by subsequent price action
    for gap in gaps:
        gap_date_idx = df.index.get_loc(pd.Timestamp(gap["date"]))
        subsequent = df.iloc[gap_date_idx + 1:] if gap_date_idx + 1 < len(df) else pd.DataFrame()

        if gap["type"] == "up":
            # Filled when price drops into the gap
            if len(subsequent) > 0 and subsequent["low"].min() <= gap["gap_low"]:
                gap["filled"] = True
        elif gap["type"] == "down":
            # Filled when price rises into the gap
            if len(subsequent) > 0 and subsequent["high"].max() >= gap["gap_high"]:
                gap["filled"] = True

    # Return only unfilled gaps
    unfilled = [g for g in gaps if not g["filled"]]
    return unfilled[-10:]  # last 10 unfilled


# ---------------------------------------------------------------------------
# 5. Fibonacci Levels
# ---------------------------------------------------------------------------

def compute_fibonacci(df: pd.DataFrame) -> dict:
    """Compute Fibonacci retracement and extension from last major swing."""
    if len(df) < 30:
        return {}

    # Find last major swing high and low (using 20-bar window)
    window = min(20, len(df) // 3)
    highs = df["high"].values
    lows = df["low"].values

    # Last 100 bars for swing detection
    recent = min(100, len(df))
    recent_highs = highs[-recent:]
    recent_lows = lows[-recent:]

    swing_high = float(recent_highs.max())
    swing_low = float(recent_lows.min())
    swing_high_idx = int(recent_highs.argmax())
    swing_low_idx = int(recent_lows.argmin())

    if swing_high == swing_low:
        return {}

    # Determine direction: if swing low came after swing high, we're in a downtrend (use retracement up)
    diff = swing_high - swing_low
    uptrend = swing_low_idx < swing_high_idx  # low came first = uptrend

    retrace_levels = [0.236, 0.382, 0.5, 0.618, 0.786]
    extend_levels = [1.0, 1.272, 1.618, 2.0, 2.618]

    fib = {"swing_high": round(swing_high, 1), "swing_low": round(swing_low, 1), "trend": "UP" if uptrend else "DOWN"}

    if uptrend:
        # Retracement from high back toward low
        fib["retracement"] = {}
        for level in retrace_levels:
            fib["retracement"][str(level)] = round(swing_high - diff * level, 1)
        # Extension above high
        fib["extension"] = {}
        for level in extend_levels:
            fib["extension"][str(level)] = round(swing_low + diff * level, 1)
    else:
        # Retracement from low back toward high
        fib["retracement"] = {}
        for level in retrace_levels:
            fib["retracement"][str(level)] = round(swing_low + diff * level, 1)
        # Extension below low
        fib["extension"] = {}
        for level in extend_levels:
            fib["extension"][str(level)] = round(swing_high - diff * level, 1)

    return fib


# ---------------------------------------------------------------------------
# 6. Pivot Points
# ---------------------------------------------------------------------------

def compute_pivots(df: pd.DataFrame) -> dict:
    """Compute classic pivot points from previous day/week."""
    if len(df) < 2:
        return {}

    # Daily pivots from previous bar
    prev = df.iloc[-2]
    h, l, c = float(prev["high"]), float(prev["low"]), float(prev["close"])

    p = round((h + l + c) / 3, 1)
    r1 = round(2 * p - l, 1)
    s1 = round(2 * p - h, 1)
    r2 = round(p + (h - l), 1)
    s2 = round(p - (h - l), 1)
    r3 = round(h + 2 * (p - l), 1)
    s3 = round(l - 2 * (h - p), 1)

    daily = {"p": p, "r1": r1, "r2": r2, "r3": r3, "s1": s1, "s2": s2, "s3": s3}

    # Weekly pivots from last 5 bars
    weekly = {}
    if len(df) >= 5:
        last5 = df.iloc[-5:]
        wh = float(last5["high"].max())
        wl = float(last5["low"].min())
        wc = float(last5["close"].iloc[-1])
        wp = round((wh + wl + wc) / 3, 1)
        weekly = {
            "p": wp,
            "r1": round(2 * wp - wl, 1),
            "s1": round(2 * wp - wh, 1),
            "r2": round(wp + (wh - wl), 1),
            "s2": round(wp - (wh - wl), 1),
        }

    return {"daily": daily, "weekly": weekly}


# ---------------------------------------------------------------------------
# 7. Candlestick Patterns
# ---------------------------------------------------------------------------

def detect_candle_patterns(df: pd.DataFrame) -> dict:
    """Detect candlestick patterns on the last few bars."""
    if len(df) < 3:
        return {"pattern": None, "confirmed": False}

    last = df.iloc[-1]
    prev = df.iloc[-2]
    o, h, l, c = float(last["open"]), float(last["high"]), float(last["low"]), float(last["close"])
    po, ph, pl, pc = float(prev["open"]), float(prev["high"]), float(prev["low"]), float(prev["close"])
    vol = float(last["volume"])
    avg_vol = float(df["volume"].iloc[-20:].mean()) if len(df) >= 20 else vol

    body = abs(c - o)
    full_range = h - l
    upper_shadow = h - max(o, c)
    lower_shadow = min(o, c) - l
    vol_confirmed = vol > avg_vol * 1.3

    if full_range == 0:
        return {"pattern": None, "confirmed": False}

    body_pct = body / full_range

    pattern = None

    # Doji: very small body
    if body_pct < 0.1:
        if lower_shadow > upper_shadow * 2:
            pattern = "DRAGONFLY_DOJI"
        elif upper_shadow > lower_shadow * 2:
            pattern = "GRAVESTONE_DOJI"
        else:
            pattern = "DOJI"

    # Hammer: small body at top, long lower shadow (bullish at support)
    elif body_pct < 0.35 and lower_shadow > body * 2 and upper_shadow < body * 0.5:
        pattern = "HAMMER" if c > o else "INVERTED_HAMMER"

    # Shooting star: small body at bottom, long upper shadow (bearish at resistance)
    elif body_pct < 0.35 and upper_shadow > body * 2 and lower_shadow < body * 0.5:
        pattern = "SHOOTING_STAR"

    # Bullish engulfing
    elif c > o and pc < po and c > po and o < pc and body > abs(pc - po):
        pattern = "BULLISH_ENGULFING"

    # Bearish engulfing
    elif c < o and pc > po and c < po and o > pc and body > abs(pc - po):
        pattern = "BEARISH_ENGULFING"

    # Bullish harami
    elif c > o and pc < po and o > pc and c < po:
        pattern = "BULLISH_HARAMI"

    # Bearish harami
    elif c < o and pc > po and o < pc and c > po:
        pattern = "BEARISH_HARAMI"

    # Marubozu (strong full body candle)
    elif body_pct > 0.85:
        pattern = "BULLISH_MARUBOZU" if c > o else "BEARISH_MARUBOZU"

    return {"pattern": pattern, "confirmed": vol_confirmed}


# ---------------------------------------------------------------------------
# 8. EMA Dynamic Support/Resistance
# ---------------------------------------------------------------------------

def detect_ema_dynamic(df: pd.DataFrame) -> dict:
    """Check which EMA is acting as support or resistance."""
    if len(df) < 50:
        return {"support": None, "resistance": None}

    close = df["close"]
    low = df["low"]
    high = df["high"]
    current = float(close.iloc[-1])

    emas = {
        "EMA9": close.ewm(span=9).mean(),
        "EMA21": close.ewm(span=21).mean(),
        "EMA50": close.ewm(span=50).mean(),
    }
    if len(df) >= 200:
        emas["SMA200"] = close.rolling(200).mean()

    support_ema = None
    resistance_ema = None

    for name, ema in emas.items():
        if ema.isna().all():
            continue
        ema_val = float(ema.iloc[-1])
        if np.isnan(ema_val):
            continue

        # Check if price bounced off this EMA in last 20 bars
        bounces = 0
        last20 = min(20, len(df) - 1)
        for i in range(-last20, 0):
            ema_at_i = float(ema.iloc[i])
            low_at_i = float(low.iloc[i])
            high_at_i = float(high.iloc[i])

            # Support: low touched EMA but close stayed above
            if abs(low_at_i - ema_at_i) / ema_at_i < 0.015 and float(close.iloc[i]) > ema_at_i:
                bounces += 1
            # Resistance: high touched EMA but close stayed below
            elif abs(high_at_i - ema_at_i) / ema_at_i < 0.015 and float(close.iloc[i]) < ema_at_i:
                bounces -= 1

        if bounces >= 2 and ema_val < current:
            if support_ema is None:
                support_ema = name
        elif bounces <= -2 and ema_val > current:
            if resistance_ema is None:
                resistance_ema = name

    return {"support": support_ema, "resistance": resistance_ema}


# ---------------------------------------------------------------------------
# 9. Mean Reversion Score
# ---------------------------------------------------------------------------

def compute_mean_reversion_score(df: pd.DataFrame, sr_levels: dict) -> int:
    """Score 0-100 for bounce probability."""
    if len(df) < 20:
        return 0

    close = df["close"]
    volume = df["volume"]
    current = float(close.iloc[-1])

    score = 0

    # RSI oversold component (0-30)
    from ta.momentum import RSIIndicator
    rsi = RSIIndicator(close, window=14).rsi()
    rsi_val = float(rsi.iloc[-1]) if not rsi.isna().iloc[-1] else 50

    if rsi_val < 30:
        score += 30
    elif rsi_val < 40:
        score += 20
    elif rsi_val < 50:
        score += 10

    # At support level component (0-30)
    support_levels = sr_levels.get("support", [])
    for s in support_levels:
        dist_pct = abs(current - s["price"]) / current * 100
        if dist_pct < 2:
            score += 15 * min(s["touches"], 2)  # Max 30
            break

    # Volume spike component (0-20)
    avg_vol = float(volume.iloc[-20:].mean())
    curr_vol = float(volume.iloc[-1])
    if avg_vol > 0 and curr_vol > avg_vol * 1.5:
        score += 20
    elif avg_vol > 0 and curr_vol > avg_vol * 1.2:
        score += 10

    # Bullish candle at support (0-20)
    last = df.iloc[-1]
    o, c = float(last["open"]), float(last["close"])
    if c > o:  # Green candle
        score += 10
        lower_shadow = min(o, c) - float(last["low"])
        body = abs(c - o)
        if body > 0 and lower_shadow > body * 1.5:  # Hammer-like
            score += 10

    return min(score, 100)


# ---------------------------------------------------------------------------
# Main: Process one symbol
# ---------------------------------------------------------------------------

def process_symbol(conn, symbol: str, df: pd.DataFrame) -> dict:
    """Compute all price structure features for one symbol."""
    if len(df) < 30:
        return None

    swings = detect_swing_points(df)
    sr = detect_support_resistance(df)
    vol_profile = compute_volume_profile(df)
    gaps = detect_gaps(df)
    fib = compute_fibonacci(df)
    pivots = compute_pivots(df)
    candle = detect_candle_patterns(df)
    ema_dynamic = detect_ema_dynamic(df)
    mr_score = compute_mean_reversion_score(df, sr)

    return {
        "swing_structure": swings["structure"],
        "last_swing_high": swings["last_swing_high"],
        "last_swing_low": swings["last_swing_low"],
        "support_levels": json.dumps(sr["support"]),
        "resistance_levels": json.dumps(sr["resistance"]),
        "volume_nodes": json.dumps(vol_profile),
        "unfilled_gaps": json.dumps(gaps),
        "fib_levels": json.dumps(fib),
        "pivot_daily": json.dumps(pivots.get("daily", {})),
        "pivot_weekly": json.dumps(pivots.get("weekly", {})),
        "candle_pattern": candle["pattern"],
        "candle_confirmed": candle["confirmed"],
        "ema_support": ema_dynamic["support"],
        "ema_resistance": ema_dynamic["resistance"],
        "mean_reversion_score": mr_score,
        "swings_json": json.dumps(swings["swings"]),
    }

    # Add squeeze detection
    squeeze = detect_squeeze(df)
    result["squeeze_active"] = squeeze["active"]
    result["squeeze_json"] = json.dumps(squeeze) if squeeze["active"] else None

    return result


# ---------------------------------------------------------------------------
# 10. Squeeze / Triangle Detection
# ---------------------------------------------------------------------------

def detect_squeeze(df: pd.DataFrame) -> dict:
    """Detect if stock is in a consolidation squeeze (symmetrical triangle)."""
    if len(df) < 30:
        return {"active": False}

    high = df["high"].values
    low = df["low"].values

    lookback = 15
    h = high[-lookback:]
    l = low[-lookback:]

    third = lookback // 3
    range1 = h[:third].max() - l[:third].min()
    range2 = h[third:2*third].max() - l[third:2*third].min()
    range3 = h[2*third:].max() - l[2*third:].min()
    converging = range1 > range2 > range3

    half = lookback // 2
    lower_highs = h[half:].max() < h[:half].max()
    higher_lows = l[half:].min() > l[:half].min()
    symmetrical = lower_highs and higher_lows

    total_range = h.max() - l.min()
    compression = range3 / total_range if total_range > 0 else 1

    bb_squeeze = False
    bb_width_pct = 50.0
    if len(df) >= 50:
        from ta.volatility import BollingerBands
        try:
            c_series = df["close"].iloc[-50:]
            bb = BollingerBands(c_series, window=20, window_dev=2)
            bb_width = (bb.bollinger_hband() - bb.bollinger_lband()) / bb.bollinger_mavg() * 100
            bw = bb_width.dropna()
            if len(bw) > 5:
                current_w = float(bw.iloc[-1])
                bb_width_pct = float((bw < current_w).sum() / len(bw) * 100)
                bb_squeeze = bb_width_pct < 20
        except Exception:
            pass

    vol = df["volume"].astype(float).values
    vol_first = vol[-lookback:-half].mean() if vol[-lookback:-half].mean() > 0 else 1
    vol_second = vol[-half:].mean()
    vol_declining = bool(vol_second < vol_first * 0.8)

    active = converging or symmetrical or bb_squeeze
    if not active:
        return {"active": False}

    # Days to apex
    days_to_apex = None
    if converging and range3 > 0 and range1 > range3:
        shrink_rate = (range1 - range3) / lookback
        days_to_apex = int(range3 / shrink_rate) if shrink_rate > 0 else 99

    upper_bound = round(float(h[-5:].max()), 1)
    lower_bound = round(float(l[-5:].min()), 1)

    squeeze_type = []
    if symmetrical:
        squeeze_type.append("SYMMETRICAL")
    if bb_squeeze:
        squeeze_type.append("BB_SQUEEZE")
    if converging:
        squeeze_type.append("CONVERGING")

    return {
        "active": True,
        "type": "+".join(squeeze_type),
        "compression": round(compression, 3),
        "bb_width_pct": round(bb_width_pct, 1),
        "vol_declining": vol_declining,
        "days_to_apex": days_to_apex,
        "upper_bound": upper_bound,
        "lower_bound": lower_bound,
    }


# ---------------------------------------------------------------------------
# Database
# ---------------------------------------------------------------------------

CREATE_TABLE = """
CREATE TABLE IF NOT EXISTS price_structure (
    symbol TEXT NOT NULL,
    date DATE NOT NULL,
    swing_structure TEXT,
    last_swing_high DOUBLE PRECISION,
    last_swing_low DOUBLE PRECISION,
    support_levels JSONB,
    resistance_levels JSONB,
    volume_nodes JSONB,
    unfilled_gaps JSONB,
    fib_levels JSONB,
    pivot_daily JSONB,
    pivot_weekly JSONB,
    candle_pattern TEXT,
    candle_confirmed BOOLEAN DEFAULT false,
    ema_support TEXT,
    ema_resistance TEXT,
    mean_reversion_score INTEGER DEFAULT 0,
    swings_json JSONB,
    squeeze_active BOOLEAN DEFAULT false,
    squeeze_json JSONB,
    computed_at TIMESTAMPTZ DEFAULT NOW(),
    PRIMARY KEY (symbol, date)
);
CREATE INDEX IF NOT EXISTS idx_ps_date ON price_structure(date);
"""


def ensure_table(conn):
    cur = conn.cursor()
    for stmt in CREATE_TABLE.strip().split(";"):
        stmt = stmt.strip()
        if stmt:
            cur.execute(stmt)
    conn.commit()
    cur.close()


def store_result(conn, symbol: str, today: str, data: dict):
    cur = conn.cursor()
    cur.execute("""
        INSERT INTO price_structure (
            symbol, date, swing_structure, last_swing_high, last_swing_low,
            support_levels, resistance_levels, volume_nodes, unfilled_gaps,
            fib_levels, pivot_daily, pivot_weekly,
            candle_pattern, candle_confirmed, ema_support, ema_resistance,
            mean_reversion_score, swings_json, squeeze_active, squeeze_json
        ) VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
        ON CONFLICT (symbol, date) DO UPDATE SET
            swing_structure = EXCLUDED.swing_structure,
            last_swing_high = EXCLUDED.last_swing_high,
            last_swing_low = EXCLUDED.last_swing_low,
            support_levels = EXCLUDED.support_levels,
            resistance_levels = EXCLUDED.resistance_levels,
            volume_nodes = EXCLUDED.volume_nodes,
            unfilled_gaps = EXCLUDED.unfilled_gaps,
            fib_levels = EXCLUDED.fib_levels,
            pivot_daily = EXCLUDED.pivot_daily,
            pivot_weekly = EXCLUDED.pivot_weekly,
            candle_pattern = EXCLUDED.candle_pattern,
            candle_confirmed = EXCLUDED.candle_confirmed,
            ema_support = EXCLUDED.ema_support,
            ema_resistance = EXCLUDED.ema_resistance,
            mean_reversion_score = EXCLUDED.mean_reversion_score,
            swings_json = EXCLUDED.swings_json,
            squeeze_active = EXCLUDED.squeeze_active,
            squeeze_json = EXCLUDED.squeeze_json,
            computed_at = NOW()
    """, (
        symbol, today,
        data["swing_structure"], data["last_swing_high"], data["last_swing_low"],
        data["support_levels"], data["resistance_levels"],
        data["volume_nodes"], data["unfilled_gaps"],
        data["fib_levels"], data["pivot_daily"], data["pivot_weekly"],
        data["candle_pattern"], data["candle_confirmed"],
        data["ema_support"], data["ema_resistance"],
        data["mean_reversion_score"], data["swings_json"],
        data.get("squeeze_active", False), data.get("squeeze_json"),
    ))
    conn.commit()
    cur.close()


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def get_active_symbols(conn, categories: list[str]) -> list[str]:
    cur = conn.cursor()
    cur.execute("""
        SELECT DISTINCT dp.symbol
        FROM daily_prices dp
        JOIN fundamentals f ON dp.symbol = f.symbol
        WHERE dp.date >= CURRENT_DATE - INTERVAL '10 days'
          AND dp.close > 0 AND f.category IN %s
        ORDER BY dp.symbol
    """, (tuple(categories),))
    symbols = [r["symbol"] for r in cur.fetchall()]
    cur.close()
    return symbols


def main():
    parser = argparse.ArgumentParser(description="Compute price structure analysis")
    parser.add_argument("--symbols", type=str, help="Comma-separated symbols")
    parser.add_argument("--categories", type=str, default="A,B")
    parser.add_argument("--dsex-only", action="store_true", help="Only compute DSEX")
    args = parser.parse_args()

    conn = get_conn()       # plain conn for pd.read_sql
    db = get_dict_conn()    # dict cursor for inserts/queries
    ensure_table(db)

    today = date.today().isoformat()

    # Always compute DSEX
    log.info("Computing DSEX price structure...")
    dsex_df = load_dsex(conn)
    if len(dsex_df) >= 30:
        dsex_result = process_symbol(db, "DSEX", dsex_df)
        if dsex_result:
            store_result(db, "DSEX", today, dsex_result)
            log.info(f"  DSEX: structure={dsex_result['swing_structure']}, "
                     f"MR score={dsex_result['mean_reversion_score']}, "
                     f"candle={dsex_result['candle_pattern']}")

    if args.dsex_only:
        conn.close()
        db.close()
        return

    if args.symbols:
        symbols = [s.strip() for s in args.symbols.split(",")]
    else:
        categories = [c.strip() for c in args.categories.split(",")]
        symbols = get_active_symbols(db, categories)

    log.info(f"Processing {len(symbols)} stocks...")
    success = 0
    failed = 0

    for i, symbol in enumerate(symbols, 1):
        try:
            df = load_prices(conn, symbol, days=500)
            if len(df) < 30:
                continue
            result = process_symbol(db, symbol, df)
            if result:
                store_result(db, symbol, today, result)
                success += 1
                if i % 50 == 0 or i == len(symbols):
                    log.info(f"  [{i}/{len(symbols)}] {symbol}: {result['swing_structure']}, "
                             f"S/R={len(json.loads(result['support_levels']))}/{len(json.loads(result['resistance_levels']))}, "
                             f"gaps={len(json.loads(result['unfilled_gaps']))}, "
                             f"candle={result['candle_pattern']}, MR={result['mean_reversion_score']}")
        except Exception as e:
            failed += 1
            log.error(f"  [{i}/{len(symbols)}] {symbol}: {e}")
            db.rollback()

    conn.close()
    db.close()
    log.info(f"Done. {success} stocks processed, {failed} failed")


if __name__ == "__main__":
    main()
