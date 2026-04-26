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
        live_bar = pd.DataFrame([{
            "date": pd.to_datetime(today_str),
            "symbol": symbol.upper(),
            "open": float(live["open"] or live["close_prev"] or live["ltp"]),
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


def detect_fvgs(h, l):
    fvgs = []
    for i in range(2, len(h)):
        if float(h.iloc[i-2]) < float(l.iloc[i]):
            fvgs.append({
                "idx": i - 1, "start_idx": i - 2, "type": "bullish",
                "top": float(l.iloc[i]), "bottom": float(h.iloc[i-2]),
                "size_pct": (float(l.iloc[i]) - float(h.iloc[i-2])) / float(h.iloc[i-2]) * 100,
            })
        if float(l.iloc[i-2]) > float(h.iloc[i]):
            fvgs.append({
                "idx": i - 1, "start_idx": i - 2, "type": "bearish",
                "top": float(l.iloc[i-2]), "bottom": float(h.iloc[i]),
                "size_pct": (float(l.iloc[i-2]) - float(h.iloc[i])) / float(h.iloc[i]) * 100,
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
    if len(df) >= 50:
        sma50 = float(df["close"].iloc[-50:].mean())
        if sma50 > 0:
            extension = (current_price - sma50) / sma50 * 100
            if extension > 30:
                parabolic = True

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

    # === Action ===
    action = "WAIT"
    action_color = "gray"
    summary = ""

    if parabolic and bias == "BULLISH":
        action = "TAKE PROFIT / DO NOT BUY"
        action_color = "orange"
        summary = "Stock is parabolic — too extended from average. Don't chase, wait for pullback."
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
        triggers.append({
            "icon": "⏸",
            "text": f"Wait for daily close above ৳{swing_high} OR below ৳{swing_low} to pick a side",
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
    fvgs = detect_fvgs(h, l)
    raw_obs = detect_order_blocks(o, h, l, c, events, df)

    # Recent + meaningful: last 60 bars, size > 0.5% (filter tiny noise)
    cutoff_idx = max(0, len(df) - 60)
    recent_fvgs = [f for f in fvgs if f["size_pct"] > 0.5 and f["idx"] >= cutoff_idx]
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
        # Forward extent: 12 bars (matches TradingView look). Mitigated zones
        # are still drawn so the user can see where price filled the gap.
        end_idx = min(f["idx"] + 12, len(df) - 1)
        end_time = idx_to_time(end_idx)
        if start_time and end_time:
            fvg_zones.append({
                "type": f["type"],
                "top": round(f["top"], 2),
                "bottom": round(f["bottom"], 2),
                "start_time": start_time,
                "end_time": end_time,
                "mitigated": f["mitigated"],
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
        "current_price": round(float(c.iloc[-1]), 2),
    }
