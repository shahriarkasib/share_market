"""SMC chart data — OHLCV + FVG zones + BOS/ChoCh events for DSE stocks."""

import pandas as pd
from data.repository import read_historical_for_symbol


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
        "current_price": round(float(c.iloc[-1]), 2),
    }
