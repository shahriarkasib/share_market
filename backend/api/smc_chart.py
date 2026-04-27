"""SMC chart data — OHLCV + FVG zones + BOS/ChoCh events for DSE stocks."""

import pandas as pd
from datetime import datetime
from data.repository import read_historical_for_symbol
from database import get_connection


def _append_live_bar_if_missing(df: pd.DataFrame, symbol: str) -> pd.DataFrame:
    """If today's bar isn't in daily_prices yet, append it from live_prices.

    This makes intraday SMC analysis reflect today's actual OHLC instead of
    yesterday's EOD numbers. Without this, charts show stale data until EOD
    daily bar lands in the DB.
    """
    if df.empty:
        return df
    today_str = datetime.now().strftime("%Y-%m-%d")
    df["date"] = pd.to_datetime(df["date"])
    last_date_str = df["date"].max().strftime("%Y-%m-%d")
    if last_date_str == today_str:
        return df  # already have today's bar

    try:
        conn = get_connection()
        row = conn.execute(
            "SELECT symbol, ltp, open, high, low, close_prev, volume "
            "FROM live_prices WHERE symbol = ? AND ltp > 0",
            (symbol.upper(),),
        ).fetchone()
        conn.close()
        if not row:
            return df

        live = dict(row)
        # Construct today's bar from live data — close = LTP (last traded price)
        # Open: prefer real session open; if missing, use LTP (first observed
        # traded price). DO NOT fall back to close_prev — that fakes the candle
        # direction to be "gap direction" instead of "session direction".
        open_px = float(live["open"]) if live.get("open") and float(live["open"]) > 0 else float(live["ltp"])
        live_bar = pd.DataFrame([{
            "date": pd.to_datetime(today_str),
            "symbol": symbol.upper(),
            "open": open_px,
            "high": float(live["high"] or live["ltp"]),
            "low": float(live["low"] or live["ltp"]),
            "close": float(live["ltp"]),
            "volume": float(live.get("volume") or 0),
        }])
        df = pd.concat([df, live_bar], ignore_index=True)
        df = df.sort_values("date").reset_index(drop=True)
    except Exception:
        pass
    return df


def find_swings(h, l, n=3):
    swings = []
    for i in range(n, len(h) - n):
        if float(h.iloc[i]) == float(h.iloc[max(0, i-n):i+n+1].max()):
            swings.append({"idx": i, "type": "high", "price": float(h.iloc[i])})
        if float(l.iloc[i]) == float(l.iloc[max(0, i-n):i+n+1].min()):
            swings.append({"idx": i, "type": "low", "price": float(l.iloc[i])})
    return swings


def detect_structure(swings):
    events = []
    trend = None
    last_sh = None
    last_sl = None
    for sw in swings:
        if sw["type"] == "high":
            if last_sh is not None:
                if sw["price"] > last_sh["price"]:
                    if trend == "up":
                        events.append({"idx": sw["idx"], "type": "bullish_BOS",
                                       "price": sw["price"], "from_idx": last_sh["idx"],
                                       "from_price": last_sh["price"]})
                    elif trend == "down":
                        events.append({"idx": sw["idx"], "type": "bullish_ChoCh",
                                       "price": sw["price"], "from_idx": last_sh["idx"],
                                       "from_price": last_sh["price"]})
                        trend = "up"
                    else:
                        trend = "up"
                elif sw["price"] < last_sh["price"] and trend is None:
                    trend = "down"
            last_sh = sw
        elif sw["type"] == "low":
            if last_sl is not None:
                if sw["price"] < last_sl["price"]:
                    if trend == "down":
                        events.append({"idx": sw["idx"], "type": "bearish_BOS",
                                       "price": sw["price"], "from_idx": last_sl["idx"],
                                       "from_price": last_sl["price"]})
                    elif trend == "up":
                        events.append({"idx": sw["idx"], "type": "bearish_ChoCh",
                                       "price": sw["price"], "from_idx": last_sl["idx"],
                                       "from_price": last_sl["price"]})
                        trend = "down"
                    else:
                        trend = "down"
                elif sw["price"] > last_sl["price"] and trend is None:
                    trend = "up"
            last_sl = sw
    return events


def detect_premium_discount(swings, df, lookback=60):
    """
    SMC Premium / Discount zones — based on the most recent dealing range.

    The dealing range is the latest meaningful swing-high to swing-low (or vice
    versa) within the lookback window. Smart money sells in PREMIUM (top half)
    and buys in DISCOUNT (bottom half). The 50% line is EQUILIBRIUM.

    Returns:
        {
          "range_high": float, "range_low": float, "equilibrium": float,
          "premium_top": float, "premium_bottom": float,         # 50%-100%
          "discount_top": float, "discount_bottom": float,       # 0%-50%
          "extreme_premium": float,  # 79% (OTE upper boundary)
          "extreme_discount": float, # 21% (OTE lower boundary)
          "current_zone": "premium"|"discount"|"equilibrium"|"extreme_premium"|"extreme_discount",
          "current_pct": float,      # where price sits in the range, 0..100
          "bias_action": str         # "look for shorts" / "look for longs" / "neutral"
        }
    """
    if not swings or len(df) < 10:
        return None

    cutoff = max(0, len(df) - lookback)
    recent = [s for s in swings if s["idx"] >= cutoff]
    if len(recent) < 2:
        return None

    range_high = max((s["price"] for s in recent if s["type"] == "high"), default=None)
    range_low = min((s["price"] for s in recent if s["type"] == "low"), default=None)
    if range_high is None or range_low is None or range_high <= range_low:
        return None

    rng = range_high - range_low
    eq = range_low + rng * 0.50
    extreme_premium = range_low + rng * 0.79
    extreme_discount = range_low + rng * 0.21

    cp = float(df["close"].iloc[-1])
    pct = (cp - range_low) / rng * 100

    if pct >= 79:
        zone, action = "extreme_premium", "Strong sell zone — avoid longs, look for shorts"
    elif pct >= 55:
        zone, action = "premium", "Premium — favor shorts, no fresh longs"
    elif pct >= 45:
        zone, action = "equilibrium", "At equilibrium — wait for premium/discount before acting"
    elif pct >= 21:
        zone, action = "discount", "Discount — favor longs"
    else:
        zone, action = "extreme_discount", "Strong buy zone — institutional buy area"

    return {
        "range_high": round(range_high, 2),
        "range_low": round(range_low, 2),
        "equilibrium": round(eq, 2),
        "extreme_premium": round(extreme_premium, 2),
        "extreme_discount": round(extreme_discount, 2),
        "current_zone": zone,
        "current_pct": round(pct, 1),
        "bias_action": action,
    }


def detect_bos_zones(swings, structure_events, df):
    """
    BOS trigger zones — the price levels that, if broken on a daily close,
    will print the next BOS event.

    Bullish trigger: most recent unbroken swing high (above current price).
    Bearish trigger: most recent unbroken swing low (below current price).

    Returns:
        {
          "bullish_trigger": {"price": float, "from_idx": int, "label": "BOS up"},
          "bearish_trigger": {"price": float, "from_idx": int, "label": "BOS down"},
        }
    """
    if not swings or len(df) < 5:
        return None

    cp = float(df["close"].iloc[-1])
    highs = sorted([s for s in swings if s["type"] == "high" and s["price"] > cp],
                   key=lambda x: x["idx"], reverse=True)
    lows = sorted([s for s in swings if s["type"] == "low" and s["price"] < cp],
                  key=lambda x: x["idx"], reverse=True)

    out = {}
    if highs:
        out["bullish_trigger"] = {
            "price": round(highs[0]["price"], 2),
            "from_idx": highs[0]["idx"],
            "label": "BOS↑ trigger",
        }
    if lows:
        out["bearish_trigger"] = {
            "price": round(lows[0]["price"], 2),
            "from_idx": lows[0]["idx"],
            "label": "BOS↓ trigger",
        }
    return out or None


def detect_order_blocks(o, h, l, c, structure_events, df):
    """
    Order Block = last opposing candle before a BOS/ChoCh.

    Bullish OB: last RED candle before a bullish BOS or ChoCh — institutions
    bought aggressively into that bar's lows, then drove price up. Price often
    returns to retest the OB before continuing.

    Bearish OB: last GREEN candle before a bearish BOS or ChoCh — institutions
    sold into that bar's highs.

    Each OB is tagged with mitigation status:
      - "fresh": untouched (highest probability of reaction on retest)
      - "tested": price wicked into the OB but didn't close beyond
      - "mitigated": price closed beyond the OB (zone is exhausted)
    """
    obs = []
    for ev in structure_events:
        ev_idx = ev["idx"]
        is_bullish_break = ev["type"] in ("bullish_BOS", "bullish_ChoCh")
        is_bearish_break = ev["type"] in ("bearish_BOS", "bearish_ChoCh")

        # Scan backward up to 15 bars for the last opposing candle
        for j in range(ev_idx - 1, max(ev_idx - 16, 0), -1):
            candle_red = float(c.iloc[j]) < float(o.iloc[j])
            candle_green = float(c.iloc[j]) > float(o.iloc[j])

            if is_bullish_break and candle_red:
                ob_top = float(h.iloc[j])
                ob_bottom = float(l.iloc[j])
                # Validate: must have a meaningful body
                if ob_top <= ob_bottom or (ob_top - ob_bottom) / ob_bottom < 0.005:
                    break
                obs.append({
                    "type": "bullish",
                    "candle_idx": j,
                    "break_idx": ev_idx,
                    "break_type": ev["type"],
                    "top": ob_top,
                    "bottom": ob_bottom,
                })
                break

            if is_bearish_break and candle_green:
                ob_top = float(h.iloc[j])
                ob_bottom = float(l.iloc[j])
                if ob_top <= ob_bottom or (ob_top - ob_bottom) / ob_bottom < 0.005:
                    break
                obs.append({
                    "type": "bearish",
                    "candle_idx": j,
                    "break_idx": ev_idx,
                    "break_type": ev["type"],
                    "top": ob_top,
                    "bottom": ob_bottom,
                })
                break

    # Tag mitigation status by scanning forward from break_idx
    for ob in obs:
        ob["status"] = "fresh"
        for k in range(ob["break_idx"] + 1, len(df)):
            row_high = float(h.iloc[k])
            row_low = float(l.iloc[k])
            row_close = float(c.iloc[k])
            if ob["type"] == "bullish":
                # Touched if low pierces the OB top (entered the zone)
                if row_low <= ob["top"] and row_close > ob["bottom"]:
                    if ob["status"] == "fresh":
                        ob["status"] = "tested"
                # Mitigated if close is below the bottom (zone broken)
                if row_close < ob["bottom"]:
                    ob["status"] = "mitigated"
                    break
            else:
                if row_high >= ob["bottom"] and row_close < ob["top"]:
                    if ob["status"] == "fresh":
                        ob["status"] = "tested"
                if row_close > ob["top"]:
                    ob["status"] = "mitigated"
                    break

    return obs


def detect_fvgs(o, h, l, c, v=None):
    """
    Detect Fair Value Gaps with SMC-grade validity scoring.

    A 3-candle FVG is "valid" only if it represents real institutional displacement:
      1. Direction-aligned middle candle: bullish FVG needs middle candle close>open
      2. Body dominates range: |close-open| / (high-low) >= 0.40 (no doji/pin)
      3. Meaningful gap size: >= 0.3% of price (filters DSE tick-noise micro-gaps)
      4. Volume confirmation: middle-candle volume >= 0.9x of 20-bar median (when available)

    Each FVG returned with a `quality` score 0..4 (count of passed filters) and
    `valid` boolean (True if quality >= 3, i.e. at least direction + body + size).
    Callers can filter on `valid` or render quality as opacity.
    """
    fvgs = []
    n = len(h)
    if n < 3:
        return fvgs

    has_vol = v is not None and len(v) == n
    for i in range(2, n):
        # Bullish FVG: candle[i-2].high < candle[i].low (gap up between them)
        if float(h.iloc[i-2]) < float(l.iloc[i]):
            mid_o, mid_c = float(o.iloc[i-1]), float(c.iloc[i-1])
            mid_h, mid_l = float(h.iloc[i-1]), float(l.iloc[i-1])
            top, bottom = float(l.iloc[i]), float(h.iloc[i-2])
            size_pct = (top - bottom) / bottom * 100

            # Filter 1: middle candle must be bullish
            f1_direction = mid_c > mid_o
            # Filter 2: body dominates range
            mid_range = max(mid_h - mid_l, 1e-9)
            f2_body = (mid_c - mid_o) / mid_range >= 0.40 if f1_direction else False
            # Filter 3: minimum gap size
            f3_size = size_pct >= 0.30
            # Filter 4: volume confirmation
            f4_vol = True
            if has_vol:
                lo = max(0, i - 21); hi = i - 1
                if hi > lo:
                    vol_med = float(v.iloc[lo:hi].median() or 0)
                    f4_vol = float(v.iloc[i-1]) >= vol_med * 0.9 if vol_med > 0 else True
            quality = sum([f1_direction, f2_body, f3_size, f4_vol])
            fvgs.append({
                "idx": i - 1, "start_idx": i - 2, "type": "bullish",
                "top": top, "bottom": bottom, "size_pct": size_pct,
                "quality": quality, "valid": quality >= 3,
            })

        # Bearish FVG: candle[i-2].low > candle[i].high (gap down)
        if float(l.iloc[i-2]) > float(h.iloc[i]):
            mid_o, mid_c = float(o.iloc[i-1]), float(c.iloc[i-1])
            mid_h, mid_l = float(h.iloc[i-1]), float(l.iloc[i-1])
            top, bottom = float(l.iloc[i-2]), float(h.iloc[i])
            size_pct = (top - bottom) / bottom * 100

            f1_direction = mid_c < mid_o
            mid_range = max(mid_h - mid_l, 1e-9)
            f2_body = (mid_o - mid_c) / mid_range >= 0.40 if f1_direction else False
            f3_size = size_pct >= 0.30
            f4_vol = True
            if has_vol:
                lo = max(0, i - 21); hi = i - 1
                if hi > lo:
                    vol_med = float(v.iloc[lo:hi].median() or 0)
                    f4_vol = float(v.iloc[i-1]) >= vol_med * 0.9 if vol_med > 0 else True
            quality = sum([f1_direction, f2_body, f3_size, f4_vol])
            fvgs.append({
                "idx": i - 1, "start_idx": i - 2, "type": "bearish",
                "top": top, "bottom": bottom, "size_pct": size_pct,
                "quality": quality, "valid": quality >= 3,
            })

    return fvgs


def calc_fibonacci(h, l):
    """Auto Fib retracement from highest high to lowest low in the visible window."""
    if len(h) < 5:
        return None
    hi = float(h.max())
    lo = float(l.min())
    rng = hi - lo
    if rng <= 0:
        return None
    return {
        "high": round(hi, 2),
        "low": round(lo, 2),
        "levels": [
            {"label": "0%", "price": round(hi, 2)},
            {"label": "23.6%", "price": round(hi - rng * 0.236, 2)},
            {"label": "38.2%", "price": round(hi - rng * 0.382, 2)},
            {"label": "50%", "price": round(hi - rng * 0.5, 2)},
            {"label": "61.8%", "price": round(hi - rng * 0.618, 2)},
            {"label": "78.6%", "price": round(hi - rng * 0.786, 2)},
            {"label": "100%", "price": round(lo, 2)},
        ],
    }


def calc_pivot_points(h, l, c):
    """Classic floor-trader pivot points using last bar's H/L/C."""
    if len(h) < 1:
        return None
    hi = float(h.iloc[-1])
    lo = float(l.iloc[-1])
    cl = float(c.iloc[-1])
    p = (hi + lo + cl) / 3
    r1 = 2 * p - lo
    s1 = 2 * p - hi
    r2 = p + (hi - lo)
    s2 = p - (hi - lo)
    r3 = hi + 2 * (p - lo)
    s3 = lo - 2 * (hi - p)
    return {
        "pivot": round(p, 2),
        "r1": round(r1, 2), "r2": round(r2, 2), "r3": round(r3, 2),
        "s1": round(s1, 2), "s2": round(s2, 2), "s3": round(s3, 2),
    }


def calc_moving_averages(c, periods=(20, 50, 200)):
    """Returns MA series aligned to candle indices (None for warm-up)."""
    out = {}
    for p in periods:
        ma = c.rolling(p).mean()
        out[f"ma_{p}"] = [
            None if pd.isna(v) else round(float(v), 2) for v in ma
        ]
    return out


def calc_rsi(c, period=14):
    """Wilder's RSI."""
    delta = c.diff()
    gain = delta.where(delta > 0, 0).ewm(alpha=1/period, adjust=False).mean()
    loss = (-delta.where(delta < 0, 0)).ewm(alpha=1/period, adjust=False).mean()
    rs = gain / loss.replace(0, 1e-10)
    rsi = 100 - (100 / (1 + rs))
    return [None if pd.isna(v) else round(float(v), 2) for v in rsi]


def calc_macd(c, fast=12, slow=26, signal=9):
    """MACD line, signal line, histogram."""
    ema_fast = c.ewm(span=fast, adjust=False).mean()
    ema_slow = c.ewm(span=slow, adjust=False).mean()
    macd_line = ema_fast - ema_slow
    signal_line = macd_line.ewm(span=signal, adjust=False).mean()
    hist = macd_line - signal_line
    return {
        "macd": [None if pd.isna(v) else round(float(v), 3) for v in macd_line],
        "signal": [None if pd.isna(v) else round(float(v), 3) for v in signal_line],
        "histogram": [None if pd.isna(v) else round(float(v), 3) for v in hist],
    }


def calc_stochastic(h, l, c, k_period=14, d_period=3):
    """Stochastic oscillator (%K and %D)."""
    lowest = l.rolling(k_period).min()
    highest = h.rolling(k_period).max()
    k = 100 * (c - lowest) / (highest - lowest).replace(0, 1e-10)
    d = k.rolling(d_period).mean()
    return {
        "k": [None if pd.isna(v) else round(float(v), 2) for v in k],
        "d": [None if pd.isna(v) else round(float(v), 2) for v in d],
    }


def detect_accumulation_distribution(df, lookback=40):
    """
    Wyckoff accumulation / distribution detection.

    Accumulation: price ranges sideways with elevated volume after a downtrend.
        → Smart money is buying without pushing price up.
        → Breakout target = top of range + (top - bottom).

    Distribution: price ranges sideways with elevated volume after an uptrend.
        → Smart money is selling without crashing the price.
        → Breakdown target = bottom of range - (top - bottom).

    Returns dict with phase, range, target, confidence, reasoning.
    """
    if len(df) < lookback + 10:
        return None

    recent = df.tail(lookback).reset_index(drop=True)
    h = recent["high"]; l = recent["low"]; c = recent["close"]; v = recent["volume"]

    range_high = float(h.max())
    range_low = float(l.min())
    range_size = range_high - range_low
    if range_size <= 0 or range_low <= 0:
        return None

    range_pct = range_size / range_low * 100

    # Range filter — clean accumulation/distribution should be tight
    # >15% range typically means trending, not ranging
    if range_pct > 18:
        return None  # too wide, not a clear range

    # Volume filter — recent average should be ≥ 80% of older average
    full = df.tail(lookback * 2)
    older_vol = float(full["volume"].iloc[:lookback].mean()) if len(full) >= lookback * 2 else float(v.mean())
    recent_vol = float(v.mean())
    if older_vol <= 0:
        return None
    vol_ratio = recent_vol / older_vol

    # Trend BEFORE the range to determine if it's accumulation or distribution
    pre_range = df.iloc[-(lookback * 2):-lookback] if len(df) >= lookback * 2 else df.iloc[:-lookback]
    if len(pre_range) < 5:
        return None
    pre_start = float(pre_range["close"].iloc[0])
    pre_end = float(pre_range["close"].iloc[-1])
    pre_change = (pre_end - pre_start) / pre_start * 100

    # Range tightness — count how many bars stay in inner 70% of range
    inner_low = range_low + range_size * 0.15
    inner_high = range_high - range_size * 0.15
    bars_inside = sum(1 for i in range(len(recent))
                      if inner_low <= float(c.iloc[i]) <= inner_high)
    inside_pct = bars_inside / len(recent) * 100

    # Test counts — multiple tests of support/resistance = stronger range
    tol = range_size * 0.02
    support_tests = sum(1 for i in range(len(recent)) if float(l.iloc[i]) <= range_low + tol)
    resistance_tests = sum(1 for i in range(len(recent)) if float(h.iloc[i]) >= range_high - tol)

    # Confidence score
    confidence = "LOW"
    score = 0
    if vol_ratio >= 0.8: score += 1
    if vol_ratio >= 1.2: score += 1
    if inside_pct >= 60: score += 1
    if support_tests >= 3 and resistance_tests >= 3: score += 1
    if abs(pre_change) >= 10: score += 1
    if score >= 4: confidence = "HIGH"
    elif score >= 2: confidence = "MEDIUM"

    # Phase determination
    if pre_change < -10:
        phase = "ACCUMULATION"
        target_up = round(range_high + range_size, 2)
        target_down = None
        bias = "bullish"
        summary = f"Sideways range after a {abs(round(pre_change, 1))}% drop with elevated volume = institutional buying"
    elif pre_change > 10:
        phase = "DISTRIBUTION"
        target_up = None
        target_down = round(range_low - range_size, 2)
        bias = "bearish"
        summary = f"Sideways range after a {round(pre_change, 1)}% rally with elevated volume = institutional selling"
    else:
        phase = "CONSOLIDATION"
        target_up = round(range_high + range_size, 2)
        target_down = round(range_low - range_size, 2)
        bias = "neutral"
        summary = f"Tight consolidation, no clear trend before = could break either way"

    return {
        "phase": phase,
        "bias": bias,
        "confidence": confidence,
        "range_high": round(range_high, 2),
        "range_low": round(range_low, 2),
        "range_pct": round(range_pct, 2),
        "target_up": target_up,
        "target_down": target_down,
        "volume_ratio": round(vol_ratio, 2),
        "support_tests": support_tests,
        "resistance_tests": resistance_tests,
        "bars_inside": bars_inside,
        "lookback": lookback,
        "pre_trend_pct": round(pre_change, 1),
        "summary": summary,
    }


def detect_support_resistance(swings, df, current_price, max_levels=8, cluster_tol_pct=1.5):
    """
    Detect multi-touch Support and Resistance zones by clustering swing
    highs/lows that fall within `cluster_tol_pct`% of each other.

    Each level reports:
      - level price (cluster average)
      - touches (number of swings forming the cluster)
      - last_touch_idx (most recent swing in the cluster)
      - role: 'support' if level < current_price, else 'resistance'
      - strength: 1 (2 touches) → 5 (5+ touches)

    Returns at most max_levels strongest levels.
    """
    if not swings:
        return []

    # Cluster all swings by price proximity
    sorted_swings = sorted(swings, key=lambda s: s["price"])
    clusters: list[list[dict]] = []
    for s in sorted_swings:
        if not clusters:
            clusters.append([s])
            continue
        cluster_avg = sum(x["price"] for x in clusters[-1]) / len(clusters[-1])
        if abs(s["price"] - cluster_avg) / cluster_avg * 100 <= cluster_tol_pct:
            clusters[-1].append(s)
        else:
            clusters.append([s])

    levels = []
    for cluster in clusters:
        if len(cluster) < 2:
            continue  # need at least 2 touches to be a real level
        avg_price = sum(s["price"] for s in cluster) / len(cluster)
        last_idx = max(s["idx"] for s in cluster)
        # Skip if level is far from current action (>30% away)
        if abs(avg_price - current_price) / current_price * 100 > 30:
            continue
        role = "resistance" if avg_price > current_price else "support"
        # Strength: more touches AND more recent = stronger
        recency_score = 1.0 if last_idx >= len(df) - 30 else 0.5 if last_idx >= len(df) - 90 else 0.25
        strength_raw = len(cluster) * recency_score
        strength = min(5, max(1, int(round(strength_raw))))
        levels.append({
            "price": round(avg_price, 2),
            "touches": len(cluster),
            "last_touch_idx": last_idx,
            "role": role,
            "strength": strength,
        })

    # Sort by closeness to current price first, then strength
    levels.sort(key=lambda l: (abs(l["price"] - current_price), -l["strength"]))
    return levels[:max_levels]


def detect_candle_patterns(df, lookback=60):
    """
    Detect classical candlestick patterns on the most recent candles.
    Returns a list of {idx, type, bias, strength, description}.

    Strength: 1 (weak) — 3 (strong) based on body/wick ratios.
    """
    patterns = []
    if len(df) < 5:
        return patterns

    o = df["open"].astype(float)
    h = df["high"].astype(float)
    l = df["low"].astype(float)
    c = df["close"].astype(float)

    # Helpers
    def body(i): return abs(c.iloc[i] - o.iloc[i])
    def rng(i):  return h.iloc[i] - l.iloc[i]
    def upper_wick(i): return h.iloc[i] - max(o.iloc[i], c.iloc[i])
    def lower_wick(i): return min(o.iloc[i], c.iloc[i]) - l.iloc[i]
    def is_green(i): return c.iloc[i] > o.iloc[i]
    def is_red(i): return c.iloc[i] < o.iloc[i]

    start = max(2, len(df) - lookback)

    for i in range(start, len(df)):
        b = body(i)
        r = rng(i)
        if r <= 0:
            continue
        body_pct = b / r
        upper_pct = upper_wick(i) / r
        lower_pct = lower_wick(i) / r

        # 1. DOJI (open ≈ close)
        if body_pct < 0.1 and r > 0:
            patterns.append({
                "idx": i, "type": "Doji", "bias": "neutral", "strength": 1,
                "description": "Open ≈ Close — indecision",
            })

        # 2. HAMMER (small body, long lower wick, after downtrend)
        elif body_pct < 0.35 and lower_pct > 0.6 and upper_pct < 0.15:
            # Confirm downtrend: prior 3 candles trending down
            prior_down = i >= 3 and c.iloc[i-1] < c.iloc[i-3]
            patterns.append({
                "idx": i,
                "type": "Hammer" if prior_down else "Hanging Man",
                "bias": "bullish" if prior_down else "bearish",
                "strength": 2 if prior_down else 1,
                "description": (
                    "Long lower wick after downtrend = potential reversal up"
                    if prior_down
                    else "Hanging man at top — caution"
                ),
            })

        # 3. INVERTED HAMMER / SHOOTING STAR
        elif body_pct < 0.35 and upper_pct > 0.6 and lower_pct < 0.15:
            prior_up = i >= 3 and c.iloc[i-1] > c.iloc[i-3]
            patterns.append({
                "idx": i,
                "type": "Shooting Star" if prior_up else "Inverted Hammer",
                "bias": "bearish" if prior_up else "bullish",
                "strength": 2,
                "description": (
                    "Long upper wick after uptrend = potential reversal down"
                    if prior_up
                    else "Inverted hammer at bottom = potential reversal up"
                ),
            })

        # 4. MARUBOZU (almost no wicks, full body)
        elif body_pct > 0.95:
            patterns.append({
                "idx": i,
                "type": "Bullish Marubozu" if is_green(i) else "Bearish Marubozu",
                "bias": "bullish" if is_green(i) else "bearish",
                "strength": 3,
                "description": "Strong directional candle, no rejection",
            })

        # 5. SPINNING TOP (small body, two wicks)
        elif body_pct < 0.3 and upper_pct > 0.3 and lower_pct > 0.3:
            patterns.append({
                "idx": i, "type": "Spinning Top",
                "bias": "neutral", "strength": 1,
                "description": "Indecision — both sides fought, no winner",
            })

        # === Two-candle patterns (need i ≥ 1) ===
        if i >= 1:
            b_prev = body(i-1)
            if b_prev > 0 and b > 0:
                # 6. BULLISH ENGULFING
                if (is_red(i-1) and is_green(i)
                    and o.iloc[i] <= c.iloc[i-1] and c.iloc[i] >= o.iloc[i-1]
                    and b > b_prev * 1.05):
                    patterns.append({
                        "idx": i, "type": "Bullish Engulfing",
                        "bias": "bullish", "strength": 3,
                        "description": "Today's green body engulfs yesterday's red — strong reversal up",
                    })
                # 7. BEARISH ENGULFING
                elif (is_green(i-1) and is_red(i)
                      and o.iloc[i] >= c.iloc[i-1] and c.iloc[i] <= o.iloc[i-1]
                      and b > b_prev * 1.05):
                    patterns.append({
                        "idx": i, "type": "Bearish Engulfing",
                        "bias": "bearish", "strength": 3,
                        "description": "Today's red body engulfs yesterday's green — strong reversal down",
                    })
                # 8. PIERCING LINE
                elif (is_red(i-1) and is_green(i)
                      and o.iloc[i] < l.iloc[i-1]
                      and c.iloc[i] > (o.iloc[i-1] + c.iloc[i-1]) / 2
                      and c.iloc[i] < o.iloc[i-1]):
                    patterns.append({
                        "idx": i, "type": "Piercing Line",
                        "bias": "bullish", "strength": 2,
                        "description": "Gap down then close past midpoint of red candle = bull reversal",
                    })
                # 9. DARK CLOUD COVER
                elif (is_green(i-1) and is_red(i)
                      and o.iloc[i] > h.iloc[i-1]
                      and c.iloc[i] < (o.iloc[i-1] + c.iloc[i-1]) / 2
                      and c.iloc[i] > o.iloc[i-1]):
                    patterns.append({
                        "idx": i, "type": "Dark Cloud Cover",
                        "bias": "bearish", "strength": 2,
                        "description": "Gap up then close into green body = bear reversal",
                    })
                # 10. BULLISH HARAMI
                elif (is_red(i-1) and is_green(i)
                      and o.iloc[i] > c.iloc[i-1] and c.iloc[i] < o.iloc[i-1]
                      and b < b_prev * 0.7):
                    patterns.append({
                        "idx": i, "type": "Bullish Harami",
                        "bias": "bullish", "strength": 2,
                        "description": "Small green inside large red = momentum slowing",
                    })
                # 11. BEARISH HARAMI
                elif (is_green(i-1) and is_red(i)
                      and o.iloc[i] < c.iloc[i-1] and c.iloc[i] > o.iloc[i-1]
                      and b < b_prev * 0.7):
                    patterns.append({
                        "idx": i, "type": "Bearish Harami",
                        "bias": "bearish", "strength": 2,
                        "description": "Small red inside large green = momentum slowing",
                    })
                # 12. TWEEZER TOP
                elif (abs(h.iloc[i] - h.iloc[i-1]) / max(h.iloc[i], h.iloc[i-1]) < 0.005
                      and is_green(i-1) and is_red(i)):
                    patterns.append({
                        "idx": i, "type": "Tweezer Top",
                        "bias": "bearish", "strength": 2,
                        "description": "Two equal highs back-to-back = top forming",
                    })
                # 13. TWEEZER BOTTOM
                elif (abs(l.iloc[i] - l.iloc[i-1]) / max(l.iloc[i], l.iloc[i-1]) < 0.005
                      and is_red(i-1) and is_green(i)):
                    patterns.append({
                        "idx": i, "type": "Tweezer Bottom",
                        "bias": "bullish", "strength": 2,
                        "description": "Two equal lows back-to-back = bottom forming",
                    })

        # === Three-candle patterns (need i ≥ 2) ===
        if i >= 2:
            b1, b2 = body(i-2), body(i-1)
            r1, r2 = rng(i-2), rng(i-1)
            # 14. MORNING STAR (red, small body, green)
            if (is_red(i-2) and r1 > 0 and r2 > 0 and r > 0
                and b1 / r1 > 0.5         # first is solid red
                and b2 / r2 < 0.4         # middle is small (star)
                and is_green(i)
                and b / r > 0.5
                and c.iloc[i] > (o.iloc[i-2] + c.iloc[i-2]) / 2):
                patterns.append({
                    "idx": i, "type": "Morning Star",
                    "bias": "bullish", "strength": 3,
                    "description": "Three-bar reversal: red → indecision → green close above mid = strong bull",
                })
            # 15. EVENING STAR
            elif (is_green(i-2) and r1 > 0 and r2 > 0 and r > 0
                  and b1 / r1 > 0.5
                  and b2 / r2 < 0.4
                  and is_red(i)
                  and b / r > 0.5
                  and c.iloc[i] < (o.iloc[i-2] + c.iloc[i-2]) / 2):
                patterns.append({
                    "idx": i, "type": "Evening Star",
                    "bias": "bearish", "strength": 3,
                    "description": "Three-bar reversal: green → indecision → red close below mid = strong bear",
                })
            # 16. THREE WHITE SOLDIERS
            elif (all(is_green(i-k) for k in (0, 1, 2))
                  and c.iloc[i] > c.iloc[i-1] > c.iloc[i-2]
                  and o.iloc[i] > o.iloc[i-1] > o.iloc[i-2]
                  and all(body(i-k) / max(rng(i-k), 1e-9) > 0.6 for k in (0, 1, 2))):
                patterns.append({
                    "idx": i, "type": "Three White Soldiers",
                    "bias": "bullish", "strength": 3,
                    "description": "Three consecutive solid green closes = strong bullish trend",
                })
            # 17. THREE BLACK CROWS
            elif (all(is_red(i-k) for k in (0, 1, 2))
                  and c.iloc[i] < c.iloc[i-1] < c.iloc[i-2]
                  and o.iloc[i] < o.iloc[i-1] < o.iloc[i-2]
                  and all(body(i-k) / max(rng(i-k), 1e-9) > 0.6 for k in (0, 1, 2))):
                patterns.append({
                    "idx": i, "type": "Three Black Crows",
                    "bias": "bearish", "strength": 3,
                    "description": "Three consecutive solid red closes = strong bearish trend",
                })

    return patterns


def detect_harmonic_patterns(swings, df, tolerance=0.05):
    """
    Detect XABCD harmonic patterns (Butterfly, Gartley, Bat, Crab, Shark).

    Each pattern requires 5 alternating swing points with specific Fib ratio
    relationships between the legs:
        XA  : initial leg
        AB  : retracement of XA
        BC  : retracement of AB (toward A)
        CD  : extension toward / past X
        AD  : projection from A through D

    `tolerance` is +/- band on the ideal ratios.
    """
    patterns = []
    if len(swings) < 5:
        return patterns

    # Filter to alternating high/low swings only (real zigzag)
    cleaned = []
    for s in swings:
        if not cleaned or cleaned[-1]["type"] != s["type"]:
            cleaned.append(s)
        else:
            # Same direction — keep the more extreme
            if s["type"] == "high" and s["price"] > cleaned[-1]["price"]:
                cleaned[-1] = s
            elif s["type"] == "low" and s["price"] < cleaned[-1]["price"]:
                cleaned[-1] = s

    def near(value, target, tol=tolerance):
        return abs(value - target) <= tol

    # Each harmonic pattern is defined by 4 ratio bands:
    # AB/XA, BC/AB, CD/BC, AD/XA
    DEFINITIONS = {
        "gartley": {
            "AB_XA": (0.618, 0.05),
            "BC_AB": ((0.382, 0.886), 0.05),
            "CD_BC": ((1.13, 1.618), 0.10),
            "AD_XA": (0.786, 0.05),
        },
        "bat": {
            "AB_XA": ((0.382, 0.50), 0.05),
            "BC_AB": ((0.382, 0.886), 0.05),
            "CD_BC": ((1.618, 2.618), 0.10),
            "AD_XA": (0.886, 0.05),
        },
        "butterfly": {
            "AB_XA": (0.786, 0.05),
            "BC_AB": ((0.382, 0.886), 0.05),
            "CD_BC": ((1.618, 2.618), 0.10),
            "AD_XA": (1.27, 0.10),
        },
        "crab": {
            "AB_XA": ((0.382, 0.618), 0.05),
            "BC_AB": ((0.382, 0.886), 0.05),
            "CD_BC": ((2.24, 3.618), 0.20),
            "AD_XA": (1.618, 0.10),
        },
        "shark": {
            "AB_XA": ((0.382, 0.618), 0.05),
            "BC_AB": ((1.13, 1.618), 0.05),
            "CD_BC": ((1.618, 2.24), 0.10),
            "AD_XA": ((0.886, 1.13), 0.05),
        },
    }

    def matches(ratio, spec):
        target, tol = spec
        if isinstance(target, tuple):
            lo, hi = target
            return (lo - tol) <= ratio <= (hi + tol)
        return abs(ratio - target) <= tol

    # Slide a 5-point window across swing zigzag
    for i in range(len(cleaned) - 4):
        X, A, B, C, D = cleaned[i:i + 5]
        # Bullish pattern: X-high, A-low, B-high, C-low, D-low (final low)
        # Bearish pattern: opposite
        seq = [X["type"], A["type"], B["type"], C["type"], D["type"]]
        if seq == ["high", "low", "high", "low", "low"]:
            bias = "bearish"
        elif seq == ["low", "high", "low", "high", "high"]:
            bias = "bullish"
        else:
            continue
        # Skip — actually XABCD harmonic alternates X→A→B→C→D
        # so X_high -> A_low -> B_high -> C_low -> D_low (bull) or
        # X_low -> A_high -> B_low -> C_high -> D_high (bear)
        # The above seq has D=C type which is wrong. Re-check:

    # Cleaner: alternate XABC then D extends past A (in same direction as XA)
    for i in range(len(cleaned) - 4):
        X, A, B, C, D = cleaned[i:i + 5]
        types = [s["type"] for s in (X, A, B, C, D)]
        # Strict zigzag: high-low-high-low-high or low-high-low-high-low
        if not (types == ["high", "low", "high", "low", "high"] or
                types == ["low", "high", "low", "high", "low"]):
            continue
        bias = "bearish" if types[-1] == "high" else "bullish"

        XA = abs(A["price"] - X["price"])
        AB = abs(B["price"] - A["price"])
        BC = abs(C["price"] - B["price"])
        CD = abs(D["price"] - C["price"])
        AD = abs(D["price"] - A["price"])
        if XA == 0 or AB == 0 or BC == 0:
            continue

        ab_xa = AB / XA
        bc_ab = BC / AB
        cd_bc = CD / BC
        ad_xa = AD / XA

        for name, spec in DEFINITIONS.items():
            if (matches(ab_xa, spec["AB_XA"]) and
                matches(bc_ab, spec["BC_AB"]) and
                matches(cd_bc, spec["CD_BC"]) and
                matches(ad_xa, spec["AD_XA"])):
                patterns.append({
                    "type": f"harmonic_{name}",
                    "bias": bias,
                    "x_idx": X["idx"], "x_price": round(X["price"], 2),
                    "a_idx": A["idx"], "a_price": round(A["price"], 2),
                    "b_idx": B["idx"], "b_price": round(B["price"], 2),
                    "c_idx": C["idx"], "c_price": round(C["price"], 2),
                    "d_idx": D["idx"], "d_price": round(D["price"], 2),
                    "ratios": {
                        "AB/XA": round(ab_xa, 3),
                        "BC/AB": round(bc_ab, 3),
                        "CD/BC": round(cd_bc, 3),
                        "AD/XA": round(ad_xa, 3),
                    },
                })
                break  # one pattern per window
    return patterns[-3:]  # cap to most recent 3


def detect_double_top(swings, df, tolerance_pct=2.0, min_separation=5):
    """Two highs within tolerance% of each other, with a valley between."""
    patterns = []
    highs = [s for s in swings if s["type"] == "high"]
    for i in range(len(highs)):
        for j in range(i + 1, len(highs)):
            h1, h2 = highs[i], highs[j]
            sep = h2["idx"] - h1["idx"]
            if sep < min_separation or sep > 50:
                continue
            avg = (h1["price"] + h2["price"]) / 2
            if abs(h1["price"] - h2["price"]) / avg * 100 > tolerance_pct:
                continue
            # Valley between them
            valley_low = float(df["low"].iloc[h1["idx"]:h2["idx"]].min())
            if valley_low > min(h1["price"], h2["price"]) * 0.97:
                continue  # not enough valley depth
            neckline = valley_low
            target = neckline - (avg - neckline)  # measured move
            patterns.append({
                "type": "double_top",
                "p1_idx": h1["idx"], "p1_price": round(h1["price"], 2),
                "p2_idx": h2["idx"], "p2_price": round(h2["price"], 2),
                "neckline": round(neckline, 2),
                "target": round(target, 2),
                "bias": "bearish",
            })
    return patterns


def detect_double_bottom(swings, df, tolerance_pct=2.0, min_separation=5):
    """Two lows within tolerance% of each other, with a peak between."""
    patterns = []
    lows = [s for s in swings if s["type"] == "low"]
    for i in range(len(lows)):
        for j in range(i + 1, len(lows)):
            l1, l2 = lows[i], lows[j]
            sep = l2["idx"] - l1["idx"]
            if sep < min_separation or sep > 50:
                continue
            avg = (l1["price"] + l2["price"]) / 2
            if abs(l1["price"] - l2["price"]) / avg * 100 > tolerance_pct:
                continue
            peak_high = float(df["high"].iloc[l1["idx"]:l2["idx"]].max())
            if peak_high < max(l1["price"], l2["price"]) * 1.03:
                continue
            neckline = peak_high
            target = neckline + (neckline - avg)
            patterns.append({
                "type": "double_bottom",
                "p1_idx": l1["idx"], "p1_price": round(l1["price"], 2),
                "p2_idx": l2["idx"], "p2_price": round(l2["price"], 2),
                "neckline": round(neckline, 2),
                "target": round(target, 2),
                "bias": "bullish",
            })
    return patterns


def detect_triangle(swings, df, lookback=40):
    """Converging triangle: highs descending + lows ascending (or one flat)."""
    patterns = []
    if len(df) < lookback:
        return patterns
    recent = [s for s in swings if s["idx"] >= len(df) - lookback]
    highs = [s for s in recent if s["type"] == "high"]
    lows = [s for s in recent if s["type"] == "low"]
    if len(highs) < 2 or len(lows) < 2:
        return patterns

    # Take last 3 of each
    highs = highs[-3:]; lows = lows[-3:]

    # Check trends
    h_descending = all(highs[i]["price"] < highs[i-1]["price"] * 1.01 for i in range(1, len(highs)))
    l_ascending = all(lows[i]["price"] > lows[i-1]["price"] * 0.99 for i in range(1, len(lows)))

    if h_descending and l_ascending and len(highs) >= 2 and len(lows) >= 2:
        triangle_type = "symmetric"
        bias = "neutral"
        if highs[-1]["price"] - highs[0]["price"] > -0.005 * highs[0]["price"]:
            triangle_type = "ascending"
            bias = "bullish"
        elif lows[-1]["price"] - lows[0]["price"] < 0.005 * lows[0]["price"]:
            triangle_type = "descending"
            bias = "bearish"

        patterns.append({
            "type": f"triangle_{triangle_type}",
            "upper_start_idx": highs[0]["idx"], "upper_start_price": round(highs[0]["price"], 2),
            "upper_end_idx": highs[-1]["idx"], "upper_end_price": round(highs[-1]["price"], 2),
            "lower_start_idx": lows[0]["idx"], "lower_start_price": round(lows[0]["price"], 2),
            "lower_end_idx": lows[-1]["idx"], "lower_end_price": round(lows[-1]["price"], 2),
            "bias": bias,
        })
    return patterns


def detect_flag(df, swings, lookback=20):
    """Flag pattern: sharp move (the pole) followed by tight consolidation."""
    patterns = []
    if len(df) < lookback + 5:
        return patterns

    c = df["close"]; h = df["high"]; l = df["low"]
    # Look at most recent lookback bars for consolidation
    recent_h = float(h.iloc[-lookback:].max())
    recent_l = float(l.iloc[-lookback:].min())
    range_pct = (recent_h - recent_l) / recent_l * 100
    if range_pct > 8:  # too volatile, not a flag
        return patterns

    # Check the pole — last 5-15 bars before the consolidation
    pole_start = max(0, len(df) - lookback - 15)
    pole_end = len(df) - lookback
    if pole_end - pole_start < 5:
        return patterns
    pole_low = float(l.iloc[pole_start:pole_end].min())
    pole_high = float(h.iloc[pole_start:pole_end].max())

    # Bull flag: strong up move + tight consolidation drift down
    if pole_high > recent_h * 0.99 and (pole_high - pole_low) / pole_low * 100 > 8:
        target = recent_h + (pole_high - pole_low)
        patterns.append({
            "type": "bull_flag",
            "pole_start_idx": pole_start, "pole_high": round(pole_high, 2),
            "pole_low": round(pole_low, 2),
            "flag_top_idx": len(df) - 1, "flag_top": round(recent_h, 2),
            "flag_bottom": round(recent_l, 2),
            "target": round(target, 2),
            "bias": "bullish",
        })
    # Bear flag: strong down move + tight consolidation drift up
    elif pole_low < recent_l * 1.01 and (pole_high - pole_low) / pole_low * 100 > 8:
        target = recent_l - (pole_high - pole_low)
        patterns.append({
            "type": "bear_flag",
            "pole_start_idx": pole_start, "pole_high": round(pole_high, 2),
            "pole_low": round(pole_low, 2),
            "flag_top_idx": len(df) - 1, "flag_top": round(recent_h, 2),
            "flag_bottom": round(recent_l, 2),
            "target": round(max(target, 0), 2),
            "bias": "bearish",
        })
    return patterns


def detect_cup_and_handle(df, swings, min_cup_bars=15, max_cup_bars=120):
    """Cup & handle: U-shape recovery to previous high + small handle pullback."""
    patterns = []
    if len(df) < min_cup_bars + 5:
        return patterns

    c = df["close"]; h = df["high"]; l = df["low"]
    highs = [s for s in swings if s["type"] == "high"]
    lows = [s for s in swings if s["type"] == "low"]

    # Look for: high → low (cup bottom) → high near first → small dip (handle)
    for i in range(len(highs) - 1):
        for j in range(i + 1, len(highs)):
            left_rim = highs[i]
            right_rim = highs[j]
            cup_bars = right_rim["idx"] - left_rim["idx"]
            if cup_bars < min_cup_bars or cup_bars > max_cup_bars:
                continue
            # Rims should be similar height
            avg_rim = (left_rim["price"] + right_rim["price"]) / 2
            if abs(left_rim["price"] - right_rim["price"]) / avg_rim > 0.05:
                continue
            # Cup bottom — find lowest low between rims
            cup_low_idx = int(l.iloc[left_rim["idx"]:right_rim["idx"]].idxmin())
            cup_bottom = float(l.iloc[cup_low_idx])
            cup_depth = (avg_rim - cup_bottom) / avg_rim
            if cup_depth < 0.10 or cup_depth > 0.50:  # 10-50% depth
                continue
            # Handle: small pullback after right rim, max 20% of cup depth
            handle_end_idx = min(right_rim["idx"] + 15, len(df) - 1)
            handle_low = float(l.iloc[right_rim["idx"]:handle_end_idx + 1].min())
            handle_pullback = (avg_rim - handle_low) / avg_rim
            if handle_pullback < 0.02 or handle_pullback > cup_depth * 0.5:
                continue
            target = avg_rim + (avg_rim - cup_bottom)
            patterns.append({
                "type": "cup_and_handle",
                "left_rim_idx": left_rim["idx"], "left_rim_price": round(left_rim["price"], 2),
                "cup_bottom_idx": cup_low_idx, "cup_bottom_price": round(cup_bottom, 2),
                "right_rim_idx": right_rim["idx"], "right_rim_price": round(right_rim["price"], 2),
                "handle_end_idx": handle_end_idx, "handle_low": round(handle_low, 2),
                "neckline": round(avg_rim, 2),
                "target": round(target, 2),
                "bias": "bullish",
            })
    # Return only the most recent
    return patterns[-2:] if patterns else []


def calc_bollinger_bands(c, period=20, num_std=2):
    """Bollinger Bands: middle (SMA), upper, lower."""
    middle = c.rolling(period).mean()
    std = c.rolling(period).std()
    upper = middle + num_std * std
    lower = middle - num_std * std
    return {
        "upper": [None if pd.isna(v) else round(float(v), 2) for v in upper],
        "middle": [None if pd.isna(v) else round(float(v), 2) for v in middle],
        "lower": [None if pd.isna(v) else round(float(v), 2) for v in lower],
    }


def calc_gann_fan(df, pivot_idx, pivot_price, direction="up"):
    """
    Gann Fan from a pivot point. Returns 9 angle lines at the classic ratios.
    Each line is defined by start point + slope (price-per-bar).
    Scale = average bar range so the 1x1 line draws at a meaningful 45° slope.
    """
    h = df["high"]; l = df["low"]
    avg_range = float((h - l).rolling(20).mean().iloc[-1] or (h.iloc[-20:] - l.iloc[-20:]).mean())
    if not avg_range or avg_range <= 0:
        avg_range = float(df["close"].std() or 1)

    # Classic Gann ratios — slope = ratio * avg_range per bar
    ratios = [
        ("1x8", 0.125),
        ("1x4", 0.25),
        ("1x3", 0.333),
        ("1x2", 0.5),
        ("1x1", 1.0),
        ("2x1", 2.0),
        ("3x1", 3.0),
        ("4x1", 4.0),
        ("8x1", 8.0),
    ]
    sign = 1 if direction == "up" else -1
    pivot_time = df.iloc[pivot_idx]["date"].strftime("%Y-%m-%d")
    end_idx = len(df) - 1
    end_time = df.iloc[end_idx]["date"].strftime("%Y-%m-%d")
    bars_forward = end_idx - pivot_idx

    lines = []
    for label, ratio in ratios:
        end_price = pivot_price + sign * ratio * avg_range * bars_forward
        lines.append({
            "label": label,
            "start_time": pivot_time,
            "start_price": round(pivot_price, 2),
            "end_time": end_time,
            "end_price": round(end_price, 2),
        })
    return {
        "pivot_time": pivot_time,
        "pivot_price": round(pivot_price, 2),
        "direction": direction,
        "lines": lines,
    }


def calc_fib_circles(df, pivot_idx, pivot_price, ref_idx, ref_price):
    """
    Fibonacci circles from pivot point with radius based on swing range.
    Radius is in price units; circles are drawn proportionally on chart.
    """
    base_radius = abs(pivot_price - ref_price)
    if base_radius <= 0:
        return None
    pivot_time = df.iloc[pivot_idx]["date"].strftime("%Y-%m-%d")
    ratios = [0.382, 0.5, 0.618, 1.0, 1.272, 1.618, 2.618]
    return {
        "center_time": pivot_time,
        "center_price": round(pivot_price, 2),
        "base_radius": round(base_radius, 2),
        "circles": [
            {"ratio": r, "radius": round(r * base_radius, 2)} for r in ratios
        ],
    }


def generate_analysis(structure_events, fvgs, order_blocks, key_levels, current_price, df):
    """
    Translate structural data into a plain-language trade card:
    bias, confidence, recommended action, entry/stop/targets, tomorrow's triggers.
    """
    swing_high = next(
        (k["price"] for k in key_levels if k["label"] == "Swing High"),
        current_price * 1.05,
    )
    swing_low = next(
        (k["price"] for k in key_levels if k["label"] == "Swing Low"),
        current_price * 0.95,
    )
    breakout_trigger = next(
        (k["price"] for k in key_levels if k["label"] == "Breakout Trigger"),
        round(swing_high * 1.02, 2),
    )
    breakdown_trigger = next(
        (k["price"] for k in key_levels if k["label"] == "Breakdown Trigger"),
        round(swing_low * 0.98, 2),
    )

    fresh_bull_fvgs_below = sorted(
        [f for f in fvgs if f.get("type") == "bullish" and not f.get("mitigated")
         and f["top"] < current_price],
        key=lambda x: -x["top"],
    )
    fresh_bear_fvgs_above = sorted(
        [f for f in fvgs if f.get("type") == "bearish" and not f.get("mitigated")
         and f["bottom"] > current_price],
        key=lambda x: x["bottom"],
    )
    fresh_bull_obs = [o for o in order_blocks if o["type"] == "bullish"
                      and o["status"] == "fresh" and o["top"] < current_price]
    fresh_bear_obs = [o for o in order_blocks if o["type"] == "bearish"
                      and o["status"] == "fresh" and o["bottom"] > current_price]

    # === Bias ===
    bias = "NEUTRAL"
    if structure_events:
        latest = structure_events[-1]
        if latest["type"].startswith("bullish"):
            bias = "BULLISH"
        elif latest["type"].startswith("bearish"):
            bias = "BEARISH"

    # Whipsaw detection: 4+ ChoCh in last 60 bars, alternating
    recent_chochs = [e for e in structure_events if "ChoCh" in e["type"]]
    if len(recent_chochs) >= 4:
        types = [e["type"] for e in recent_chochs[-4:]]
        alternations = sum(
            1 for i in range(1, len(types))
            if types[i].startswith("bullish") != types[i - 1].startswith("bullish")
        )
        if alternations >= 3:
            bias = "WHIPSAW"

    # Parabolic check: too extended from SMA50
    parabolic = False
    extension = 0.0
    if len(df) >= 50:
        sma50 = float(df["close"].iloc[-50:].mean())
        if sma50 > 0:
            extension = (current_price - sma50) / sma50 * 100
            if extension > 30:
                parabolic = True

    # === SMC contextual penalties (caught the ECABLES top-of-range trap) ===
    # 1. Premium-zone check — price in top portion of recent 60-bar range
    range_pct = None
    in_extreme_premium = False
    in_premium = False
    if len(df) >= 20:
        recent_window = min(60, len(df))
        rh = float(df["high"].iloc[-recent_window:].max())
        rl = float(df["low"].iloc[-recent_window:].min())
        if rh > rl:
            range_pct = (current_price - rl) / (rh - rl) * 100
            in_extreme_premium = range_pct >= 79
            in_premium = range_pct >= 65

    # 2. Overhead bearish FVG within 3% of price (institutional supply near)
    overhead_bear_fvg = None
    if fresh_bear_fvgs_above:
        nearest_bear = fresh_bear_fvgs_above[0]
        if (nearest_bear["bottom"] - current_price) / current_price <= 0.03:
            overhead_bear_fvg = nearest_bear

    # 3. Demand-absorption — multiple bullish FVGs mitigated in the last
    # 10 bars means buyers are getting eaten through
    recent_mitigated_bull = sum(
        1 for f in fvgs
        if f.get("type") == "bullish" and f.get("mitigated")
    )

    # === Confidence ===
    confidence = "LOW"
    if bias == "BULLISH":
        bull_count_recent = sum(
            1 for e in structure_events[-3:]
            if e["type"].startswith("bullish")
        )
        bos_count_recent = sum(
            1 for e in structure_events[-3:]
            if "BOS" in e["type"] and e["type"].startswith("bullish")
        )
        if bull_count_recent >= 3 and bos_count_recent >= 2 and len(fresh_bull_fvgs_below) >= 2:
            confidence = "HIGH"
        elif bull_count_recent >= 2 and len(fresh_bull_fvgs_below) >= 1:
            confidence = "MEDIUM"
        else:
            confidence = "LOW"
    elif bias == "BEARISH":
        bear_count = sum(
            1 for e in structure_events[-3:]
            if e["type"].startswith("bearish")
        )
        confidence = "MEDIUM" if bear_count >= 2 else "LOW"
    elif bias == "WHIPSAW":
        confidence = "LOW"

    # Apply SMC contextual confidence caps (BULLISH only — these warn against
    # buying into supply, premium, or absorbed demand)
    if bias == "BULLISH":
        # Overhead bearish FVG within 3% = institutional supply right above
        if overhead_bear_fvg is not None and confidence == "HIGH":
            confidence = "LOW"
        elif overhead_bear_fvg is not None and confidence == "MEDIUM":
            confidence = "LOW"
        # In extreme premium = smart-money sell zone
        if in_extreme_premium and confidence in ("HIGH", "MEDIUM"):
            confidence = "LOW"
        # Demand absorption: 2+ bullish FVGs already mitigated in window
        if recent_mitigated_bull >= 2 and confidence == "HIGH":
            confidence = "MEDIUM"

    # === Action ===
    action = "WAIT"
    action_color = "gray"
    summary = ""

    # Premium-zone action override: even with bullish bias, fresh longs in
    # extreme premium are low-edge. Wait for pullback to discount.
    if bias == "BULLISH" and in_extreme_premium:
        action = "TAKE PROFIT / WAIT FOR PULLBACK"
        action_color = "orange"
        summary = (
            f"Bullish trend but price at {round(range_pct or 0)}% of range "
            f"(extreme premium). Smart money sells here — wait for pullback to discount."
        )
    elif parabolic and bias == "BULLISH":
        action = "TAKE PROFIT / DO NOT BUY"
        action_color = "orange"
        summary = "Stock is parabolic — too extended from average. Don't chase, wait for pullback."
    elif bias == "BULLISH" and overhead_bear_fvg is not None:
        action = "WAIT — SUPPLY OVERHEAD"
        action_color = "orange"
        summary = (
            f"Bullish bias but fresh bearish FVG ৳{overhead_bear_fvg['bottom']}-"
            f"{overhead_bear_fvg['top']} sits right above price. "
            "Wait for that zone to mitigate or for deeper pullback."
        )
    elif bias == "WHIPSAW":
        action = "AVOID"
        action_color = "red"
        summary = "Whipsaw pattern — multiple reversals, no clean trend. No edge in trading."
    elif bias == "BEARISH":
        action = "AVOID / EXIT"
        action_color = "red"
        summary = "Latest structure is bearish. Don't catch falling knives — wait for reversal signal."
    elif bias == "BULLISH":
        if current_price >= breakout_trigger:
            action = "BUY (BREAKOUT ACTIVE)"
            action_color = "green"
            summary = "Price has cleared the breakout trigger. Trend confirmed bullish."
        elif current_price > swing_high * 0.98:
            action = "BUY ON CLOSE ABOVE TRIGGER"
            action_color = "yellow"
            summary = f"Price testing breakout level. Wait for daily close above ৳{breakout_trigger}."
        elif fresh_bull_fvgs_below:
            action = "BUY ON DIP TO FVG"
            action_color = "yellow"
            top_fvg = fresh_bull_fvgs_below[0]
            summary = f"Bullish bias intact. Best entry on pullback to FVG ৳{top_fvg['bottom']}-{top_fvg['top']}."
        else:
            action = "WAIT FOR SETUP"
            action_color = "gray"
            summary = "Bullish bias but no clear entry. Wait for pullback or breakout."
    else:
        summary = "No clear directional signal. Wait for structure to develop."

    # === Reasons (max 4 bullet points) ===
    reasons = []
    if structure_events:
        latest = structure_events[-1]
        type_label = latest["type"].replace("_", " ").replace("bullish", "↑").replace("bearish", "↓")
        reasons.append(f"Latest structure: {type_label} @ ৳{latest['price']}")
    if fresh_bull_fvgs_below and bias == "BULLISH":
        reasons.append(
            f"{len(fresh_bull_fvgs_below)} fresh bullish FVG(s) below = layered support"
        )
    if fresh_bear_fvgs_above and bias != "BULLISH":
        reasons.append(
            f"{len(fresh_bear_fvgs_above)} bearish FVG(s) above = institutional resistance"
        )
    if fresh_bull_obs:
        ob = fresh_bull_obs[0]
        reasons.append(
            f"Smart money buying zone: ৳{ob['bottom']}-{ob['top']} (fresh OB)"
        )
    if fresh_bear_obs:
        ob = fresh_bear_obs[0]
        reasons.append(
            f"Smart money selling zone: ৳{ob['bottom']}-{ob['top']} (fresh bearish OB)"
        )
    if bias == "WHIPSAW":
        reasons.append("⚠ Whipsaw pattern detected — multiple alternating reversals")
    if parabolic:
        reasons.append(f"⚠ Price is +{round(extension)}% above SMA50 — parabolic risk")
    if in_extreme_premium and bias == "BULLISH":
        reasons.append(
            f"⚠ Price at {round(range_pct or 0)}% of range = extreme premium "
            "(smart money sells here)"
        )
    elif in_premium and bias == "BULLISH":
        reasons.append(
            f"⚠ Price in premium zone ({round(range_pct or 0)}% of range) — "
            "low-edge for fresh longs"
        )
    if overhead_bear_fvg is not None:
        reasons.append(
            f"⚠ Fresh bearish FVG ৳{overhead_bear_fvg['bottom']}-{overhead_bear_fvg['top']} "
            "overhead = institutional supply"
        )
    if recent_mitigated_bull >= 2 and bias == "BULLISH":
        reasons.append(
            f"⚠ {recent_mitigated_bull} bullish FVG(s) already mitigated = demand absorbed"
        )

    # === Trade levels ===
    entry = None
    entry_label = None
    stop_loss = None
    target1 = None
    target2 = None
    rr = None

    if action.startswith("BUY"):
        # Pick entry
        if "DIP" in action and fresh_bull_fvgs_below:
            entry = round(fresh_bull_fvgs_below[0]["top"], 2)
            entry_label = f"Limit ৳{entry} (FVG retest)"
        elif "BREAKOUT" in action and current_price >= breakout_trigger:
            entry = round(current_price, 2)
            entry_label = f"Market ৳{entry} (chase ok, in breakout)"
        elif "ABOVE TRIGGER" in action:
            entry = round(breakout_trigger, 2)
            entry_label = f"Stop-buy at ৳{entry} (only fires above trigger)"
        else:
            entry = round(current_price, 2)
            entry_label = f"Market ৳{entry}"

        # Stop loss: use deepest fresh support
        if fresh_bull_obs:
            stop_loss = round(fresh_bull_obs[0]["bottom"] * 0.985, 2)
        elif fresh_bull_fvgs_below:
            deepest_fvg = fresh_bull_fvgs_below[-1]
            stop_loss = round(deepest_fvg["bottom"] * 0.985, 2)
        else:
            stop_loss = round(swing_low * 0.98, 2)

        # Targets
        target1 = round(swing_high * 1.05, 2)
        target2 = round(swing_high * 1.12, 2)

        # R/R
        risk = entry - stop_loss
        reward = target1 - entry
        if risk > 0:
            rr = round(reward / risk, 2)

    # === Tomorrow's triggers ===
    triggers = []
    if bias == "BULLISH":
        if current_price < breakout_trigger:
            triggers.append({
                "icon": "🟢",
                "text": f"Daily close above ৳{breakout_trigger} = breakout confirmed → buy/add",
            })
        if fresh_bull_fvgs_below:
            top_fvg = fresh_bull_fvgs_below[0]
            triggers.append({
                "icon": "🎯",
                "text": f"Pullback to ৳{top_fvg['bottom']}-{top_fvg['top']} = high-probability entry",
            })
        triggers.append({
            "icon": "🔴",
            "text": f"Daily close below ৳{swing_low} = trend failed, exit immediately",
        })
    elif bias == "BEARISH":
        triggers.append({
            "icon": "🟢",
            "text": f"Daily close above ৳{swing_high} (clears resistance) = re-evaluate",
        })
        triggers.append({
            "icon": "🔴",
            "text": f"Daily close below ৳{breakdown_trigger} = downtrend confirmed → avoid/short",
        })
    elif bias == "WHIPSAW":
        # Use last 20 bars range for actionable near-term levels (not full-window swings)
        recent_window = min(20, len(df))
        recent_high = round(float(df["high"].iloc[-recent_window:].max()), 2)
        recent_low = round(float(df["low"].iloc[-recent_window:].min()), 2)
        triggers.append({
            "icon": "⏸",
            "text": f"Wait for daily close above ৳{recent_high} OR below ৳{recent_low} (20-day range) to pick a side",
        })
    else:
        triggers.append({
            "icon": "⏸",
            "text": "Watch for first ChoCh / BOS event to develop bias",
        })

    return {
        "bias": bias,
        "confidence": confidence,
        "action": action,
        "action_color": action_color,
        "summary": summary,
        "reasons": reasons,
        "entry": entry,
        "entry_label": entry_label,
        "stop_loss": stop_loss,
        "target1": target1,
        "target2": target2,
        "risk_reward": rr,
        "triggers": triggers,
    }


def get_smc_chart(symbol: str, days: int = 180, interval: str = "daily"):
    """Returns OHLCV candles + volume + FVG zones + BOS/ChoCh + Fib + Pivot + MAs.

    interval: 'daily' (1 candle = 1 day) or 'weekly' (resampled to ISO weeks).
    For weekly, fetch ~6x more rows so we still have ~30+ weekly candles to analyze.
    """
    fetch_days = days * 2 if interval == "weekly" else days
    min_rows_needed = int(fetch_days * 1.5)
    df = read_historical_for_symbol(symbol, min_rows=min_rows_needed)
    if df.empty:
        return None

    df["date"] = pd.to_datetime(df["date"])
    df = df.sort_values("date").reset_index(drop=True)

    # Append today's live bar if missing — this is what makes the chart reflect
    # the actual current session instead of being stuck on yesterday's EOD data.
    df = _append_live_bar_if_missing(df, symbol)

    # Resample to weekly if requested (Mon-Sun ISO weeks)
    if interval == "weekly":
        df = df.set_index("date")
        agg = {"open": "first", "high": "max", "low": "min", "close": "last"}
        if "volume" in df.columns:
            agg["volume"] = "sum"
        df = df.resample("W-FRI").agg(agg).dropna(subset=["open"]).reset_index()

    # Limit to requested period
    cutoff = df["date"].max() - pd.Timedelta(days=days)
    df = df[df["date"] >= cutoff].reset_index(drop=True)
    if len(df) < 20:
        return None

    o, h, l, c = df["open"], df["high"], df["low"], df["close"]
    v = df["volume"] if "volume" in df.columns else pd.Series([0] * len(df))

    candles = []
    volumes = []
    for i, row in df.iterrows():
        time_str = row["date"].strftime("%Y-%m-%d")
        candles.append({
            "time": time_str,
            "open": round(float(row["open"]), 2),
            "high": round(float(row["high"]), 2),
            "low": round(float(row["low"]), 2),
            "close": round(float(row["close"]), 2),
        })
        vol_val = float(row["volume"]) if "volume" in df.columns and pd.notna(row.get("volume")) else 0
        volumes.append({
            "time": time_str,
            "value": vol_val,
            "color": "#26a69a" if row["close"] >= row["open"] else "#ef5350",
        })

    swings = find_swings(h, l, n=3)
    events = detect_structure(swings)
    fvgs = detect_fvgs(o, h, l, c, v)
    raw_obs = detect_order_blocks(o, h, l, c, events, df)

    # Recent + valid only: last 60 bars + passed SMC quality filters
    cutoff_idx = max(0, len(df) - 60)
    recent_fvgs = [f for f in fvgs if f["valid"] and f["idx"] >= cutoff_idx]
    recent_events = [e for e in events if e["idx"] >= cutoff_idx]

    # Tag each FVG with mitigation state but return them all so the user can see
    # the full history. Mitigated zones rendered dimmer in the UI.
    def _is_mitigated(f):
        for j in range(f["idx"] + 1, len(df)):
            row_high = float(h.iloc[j])
            row_low = float(l.iloc[j])
            if f["type"] == "bullish" and row_low < f["bottom"]:
                return True
            if f["type"] == "bearish" and row_high > f["top"]:
                return True
        return False

    for f in recent_fvgs:
        f["mitigated"] = _is_mitigated(f)

    # Cap to last 30 to avoid an absurd number of overlapping zones
    recent_fvgs = recent_fvgs[-30:]

    def idx_to_time(idx):
        if 0 <= idx < len(df):
            return df.iloc[idx]["date"].strftime("%Y-%m-%d")
        return None

    fvg_zones = []
    for f in recent_fvgs:
        start_time = idx_to_time(f["start_idx"])
        # Match TradingView SMC indicator: extend FVG to the candle that
        # mitigated it (filled the gap) — or to current bar if still active.
        # This makes active zones reach forward to the right edge as resistance/support.
        if f["mitigated"]:
            mit_idx = None
            for j in range(f["idx"] + 1, len(df)):
                if f["type"] == "bullish" and float(l.iloc[j]) < f["bottom"]:
                    mit_idx = j; break
                if f["type"] == "bearish" and float(h.iloc[j]) > f["top"]:
                    mit_idx = j; break
            end_idx = mit_idx if mit_idx is not None else len(df) - 1
        else:
            end_idx = len(df) - 1  # active zones extend to current bar
        end_time = idx_to_time(end_idx)
        if start_time and end_time:
            fvg_zones.append({
                "type": f["type"],
                "top": round(f["top"], 2),
                "bottom": round(f["bottom"], 2),
                "start_time": start_time,
                "end_time": end_time,
                "mitigated": f["mitigated"],
                "quality": f.get("quality", 0),
                "valid": f.get("valid", True),
                "size_pct": round(f.get("size_pct", 0), 2),
            })

    # Order Block output — keep only those visible in the recent window
    order_blocks = []
    for ob in raw_obs:
        if ob["candle_idx"] < cutoff_idx:
            continue
        # Mitigated OBs older than the visible window are noise — keep fresh + tested
        if ob["status"] == "mitigated" and ob["candle_idx"] < len(df) - 30:
            continue
        start_time = idx_to_time(ob["candle_idx"])
        # Extend the OB box forward to current bar so retest opportunities are visible
        end_time = idx_to_time(len(df) - 1)
        if start_time and end_time:
            order_blocks.append({
                "type": ob["type"],
                "top": round(ob["top"], 2),
                "bottom": round(ob["bottom"], 2),
                "start_time": start_time,
                "end_time": end_time,
                "status": ob["status"],
                "break_type": ob["break_type"],
            })

    # Cap to the most recent 8 OBs
    order_blocks = order_blocks[-8:]

    # Cap structure events at most recent 6 for readability
    structure_events = []
    for e in recent_events[-6:]:
        time = idx_to_time(e["idx"])
        from_time = idx_to_time(e["from_idx"])
        if time and from_time:
            structure_events.append({
                "type": e["type"],
                "price": round(e["price"], 2),
                "from_price": round(e["from_price"], 2),
                "time": time,
                "from_time": from_time,
            })

    # Optional indicators (cheap to compute, sent in same payload for instant toggle)
    fibonacci = calc_fibonacci(h, l)
    pivots = calc_pivot_points(h, l, c)
    mas = calc_moving_averages(c, periods=(20, 50, 200))

    # Phase 1 indicators
    rsi_vals = calc_rsi(c, period=14)
    macd_data = calc_macd(c)
    stoch_data = calc_stochastic(h, l, c)
    bb_data = calc_bollinger_bands(c, period=20, num_std=2)

    # Phase 2 chart patterns
    period_swings = find_swings(h, l, n=3)
    chart_patterns = []
    try:
        chart_patterns.extend(detect_double_top(period_swings, df))
        chart_patterns.extend(detect_double_bottom(period_swings, df))
        chart_patterns.extend(detect_triangle(period_swings, df))
        chart_patterns.extend(detect_flag(df, period_swings))
        chart_patterns.extend(detect_cup_and_handle(df, period_swings))
    except Exception:
        pass

    # Phase 3 harmonic patterns
    harmonic_patterns = []
    try:
        harmonic_patterns = detect_harmonic_patterns(period_swings, df)
    except Exception:
        pass

    # Wyckoff accumulation / distribution detection — try multiple windows
    # and pick the cleanest one (smallest range_pct that still passes filter).
    accumulation = None
    try:
        candidates = []
        for lb in (20, 30, 40, 60):
            r = detect_accumulation_distribution(df, lookback=lb)
            if r is not None:
                candidates.append(r)
        if candidates:
            # Prefer the window with the lowest range_pct (tightest range)
            accumulation = min(candidates, key=lambda x: x["range_pct"])
    except Exception:
        pass

    # Multi-touch Support/Resistance levels
    sr_levels = []
    try:
        current_price = float(c.iloc[-1])
        sr_raw = detect_support_resistance(period_swings, df, current_price)
        for lvl in sr_raw:
            ti = lvl["last_touch_idx"]
            last_touch_time = (
                df.iloc[ti]["date"].strftime("%Y-%m-%d")
                if 0 <= ti < len(df) else None
            )
            sr_levels.append({
                "price": lvl["price"],
                "touches": lvl["touches"],
                "role": lvl["role"],
                "strength": lvl["strength"],
                "last_touch_time": last_touch_time,
            })
    except Exception:
        pass

    # Phase 4 candlestick patterns
    candle_patterns_raw = []
    try:
        candle_patterns_raw = detect_candle_patterns(df, lookback=60)
    except Exception:
        pass

    candle_patterns = []
    for p in candle_patterns_raw:
        idx = p["idx"]
        if 0 <= idx < len(df):
            candle_patterns.append({
                "time": df.iloc[idx]["date"].strftime("%Y-%m-%d"),
                "type": p["type"],
                "bias": p["bias"],
                "strength": p["strength"],
                "description": p["description"],
                "price_high": round(float(df.iloc[idx]["high"]), 2),
                "price_low": round(float(df.iloc[idx]["low"]), 2),
            })
    # Cap at the most recent 25 — that's plenty for a single chart
    candle_patterns = candle_patterns[-25:]

    # Convert pattern indices to times
    def _idx_t(i):
        if 0 <= i < len(df):
            return df.iloc[i]["date"].strftime("%Y-%m-%d")
        return None
    for p in chart_patterns:
        for k in list(p.keys()):
            if k.endswith("_idx"):
                tk = k.replace("_idx", "_time")
                p[tk] = _idx_t(p[k])
    chart_patterns = chart_patterns[-6:]  # cap visible patterns

    # Convert harmonic indices to times
    for p in harmonic_patterns:
        for k in list(p.keys()):
            if k.endswith("_idx"):
                tk = k.replace("_idx", "_time")
                p[tk] = _idx_t(p[k])

    # Build aligned indicator series with timestamps
    times_iso = [df.iloc[i]["date"].strftime("%Y-%m-%d") for i in range(len(df))]
    rsi_series = [
        {"time": times_iso[i], "value": rsi_vals[i]}
        for i in range(len(df)) if rsi_vals[i] is not None
    ]
    macd_series = {
        "macd": [{"time": times_iso[i], "value": macd_data["macd"][i]}
                 for i in range(len(df)) if macd_data["macd"][i] is not None],
        "signal": [{"time": times_iso[i], "value": macd_data["signal"][i]}
                   for i in range(len(df)) if macd_data["signal"][i] is not None],
        "histogram": [{"time": times_iso[i], "value": macd_data["histogram"][i],
                       "color": "#26a69a" if macd_data["histogram"][i] >= 0 else "#ef5350"}
                      for i in range(len(df)) if macd_data["histogram"][i] is not None],
    }
    stoch_series = {
        "k": [{"time": times_iso[i], "value": stoch_data["k"][i]}
              for i in range(len(df)) if stoch_data["k"][i] is not None],
        "d": [{"time": times_iso[i], "value": stoch_data["d"][i]}
              for i in range(len(df)) if stoch_data["d"][i] is not None],
    }
    bb_series = {
        "upper": [{"time": times_iso[i], "value": bb_data["upper"][i]}
                  for i in range(len(df)) if bb_data["upper"][i] is not None],
        "middle": [{"time": times_iso[i], "value": bb_data["middle"][i]}
                   for i in range(len(df)) if bb_data["middle"][i] is not None],
        "lower": [{"time": times_iso[i], "value": bb_data["lower"][i]}
                  for i in range(len(df)) if bb_data["lower"][i] is not None],
    }

    # Key levels for trade decisions: recent swing high/low, breakout trigger,
    # support break, and current price reference.
    key_levels = []
    try:
        recent_swings = find_swings(h, l, n=3)
        last_n = 12  # look at last ~12 swings for "recent" structure
        recent_swings = recent_swings[-last_n:] if len(recent_swings) > last_n else recent_swings

        recent_highs = [s for s in recent_swings if s["type"] == "high"]
        recent_lows = [s for s in recent_swings if s["type"] == "low"]

        if recent_highs:
            sh = max(recent_highs, key=lambda s: s["price"])
            key_levels.append({
                "label": "Swing High",
                "price": round(sh["price"], 2),
                "color": "#fbbf24",
                "purpose": "resistance",
            })
            key_levels.append({
                "label": "Breakout Trigger",
                "price": round(sh["price"] * 1.02, 2),
                "color": "#22c55e",
                "purpose": "breakout_long",
            })

        if recent_lows:
            sl = min(recent_lows, key=lambda s: s["price"])
            key_levels.append({
                "label": "Swing Low",
                "price": round(sl["price"], 2),
                "color": "#fbbf24",
                "purpose": "support",
            })
            key_levels.append({
                "label": "Breakdown Trigger",
                "price": round(sl["price"] * 0.98, 2),
                "color": "#ef4444",
                "purpose": "breakout_short",
            })
    except Exception:
        pass

    # Gann Fan + Fib Circles use the period's most extreme swing as pivot
    gann = None
    fib_circles = None
    try:
        period_swings = find_swings(h, l, n=5)
        if period_swings:
            highs = [s for s in period_swings if s["type"] == "high"]
            lows = [s for s in period_swings if s["type"] == "low"]
            if lows and highs:
                lowest = min(lows, key=lambda x: x["price"])
                highest = max(highs, key=lambda x: x["price"])
                # Pick the more recent extreme as Gann pivot (project forward)
                if lowest["idx"] >= highest["idx"]:
                    gann = calc_gann_fan(df, lowest["idx"], lowest["price"], direction="up")
                else:
                    gann = calc_gann_fan(df, highest["idx"], highest["price"], direction="down")
                # Fib circles centered on lowest, ref to highest (covers the full swing)
                fib_circles = calc_fib_circles(
                    df,
                    lowest["idx"], lowest["price"],
                    highest["idx"], highest["price"],
                )
    except Exception:
        pass

    # Add MA values aligned to candle times
    ma_lines = {}
    for key, vals in mas.items():
        ma_lines[key] = [
            {"time": df.iloc[i]["date"].strftime("%Y-%m-%d"), "value": vals[i]}
            for i in range(len(vals))
            if vals[i] is not None
        ]

    analysis = generate_analysis(
        structure_events,
        fvg_zones,
        order_blocks,
        key_levels,
        round(float(c.iloc[-1]), 2),
        df,
    )

    premium_discount = detect_premium_discount(swings, df, lookback=60)
    bos_zones = detect_bos_zones(swings, structure_events, df)

    return {
        "symbol": symbol.upper(),
        "candles": candles,
        "volumes": volumes,
        "fvgs": fvg_zones,
        "structure": structure_events,
        "fibonacci": fibonacci,
        "pivots": pivots,
        "moving_averages": ma_lines,
        "gann_fan": gann,
        "fib_circles": fib_circles,
        "key_levels": key_levels,
        "order_blocks": order_blocks,
        "analysis": analysis,
        "rsi": rsi_series,
        "macd": macd_series,
        "stochastic": stoch_series,
        "bollinger_bands": bb_series,
        "chart_patterns": chart_patterns,
        "harmonic_patterns": harmonic_patterns,
        "candle_patterns": candle_patterns,
        "support_resistance": sr_levels,
        "accumulation": accumulation,
        "premium_discount": premium_discount,
        "bos_zones": bos_zones,
        "current_price": round(float(c.iloc[-1]), 2),
    }
