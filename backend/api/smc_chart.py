"""SMC chart data — OHLCV + FVG zones + BOS/ChoCh events for DSE stocks."""

import pandas as pd
import numpy as np
from datetime import datetime, timedelta
from data.repository import read_historical_for_symbol
from database import get_connection


def _is_dse_market_day_now() -> bool:
    """DSE trades Sun-Thu, 10:00-14:30 BST (UTC+6). Don't fake a today bar
    on weekends or holidays.

    Returns True only when:
      - Current Bangladesh weekday is Sun(6), Mon(0), Tue(1), Wed(2), Thu(3)
      - It's between 10:00 and ~16:00 BST (allow 90 min after close
        for late settlement to land — beyond that, treat as next-day close)
    """
    now_utc = datetime.utcnow()
    # BST = UTC+6
    bst = now_utc + timedelta(hours=6)
    weekday = bst.weekday()  # Mon=0, Sun=6
    # DSE trading days: Sun(6), Mon(0), Tue(1), Wed(2), Thu(3)
    if weekday not in (6, 0, 1, 2, 3):
        return False
    minute_of_day = bst.hour * 60 + bst.minute
    # 10:00 to 16:00 BST window (allow ~90 min post-close)
    return 600 <= minute_of_day <= 960


def _append_live_bar_if_missing(df: pd.DataFrame, symbol: str) -> pd.DataFrame:
    """If today's bar isn't in daily_prices yet, append it from live_prices.

    Three guard rails to avoid phantom bars on closed days:
      1. Only append on actual DSE market days (Sun-Thu, 10:00-16:00 BST)
      2. Only append if live_prices.updated_at is from today (BST)
      3. Don't append if last bar in df is already today
    """
    if df.empty:
        return df
    df["date"] = pd.to_datetime(df["date"])

    # BST today
    bst_today = (datetime.utcnow() + timedelta(hours=6)).date()
    today_str = bst_today.strftime("%Y-%m-%d")

    last_date_str = df["date"].max().strftime("%Y-%m-%d")
    if last_date_str == today_str:
        return df  # already have today's bar

    # Guard 1: must be a market day, in-session window
    if not _is_dse_market_day_now():
        return df

    try:
        conn = get_connection()
        # Guard 2: live_prices entry must be FROM TODAY. If updated_at is
        # missing or stale (yesterday or older), the live_prices row is
        # stale data left over from previous session — do NOT append.
        row = conn.execute(
            "SELECT symbol, ltp, open, high, low, close_prev, volume, updated_at "
            "FROM live_prices WHERE symbol = ? AND ltp > 0",
            (symbol.upper(),),
        ).fetchone()
        conn.close()
        if not row:
            return df

        live = dict(row)
        upd = live.get("updated_at")
        if upd is None:
            return df  # no timestamp = can't verify freshness, skip
        # Convert to BST date for comparison
        if isinstance(upd, str):
            try:
                upd = datetime.fromisoformat(upd.replace("Z", "+00:00"))
            except Exception:
                return df
        # Strip tzinfo to compare on UTC then add 6h for BST
        if upd.tzinfo is not None:
            upd_utc = upd.astimezone(tz=None).replace(tzinfo=None) if hasattr(upd, "astimezone") else upd.replace(tzinfo=None)
        else:
            upd_utc = upd
        upd_bst_date = (upd_utc + timedelta(hours=6)).date()
        if upd_bst_date != bst_today:
            return df  # stale live_prices row — do not fabricate today's bar

        # Construct today's bar from live data — close = LTP (last traded price)
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
    """Vectorized via numpy arrays — ~10x faster than per-row .iloc."""
    h_arr = h.values if hasattr(h, "values") else h
    l_arr = l.values if hasattr(l, "values") else l
    N = len(h_arr)
    swings = []
    for i in range(n, N - n):
        win_h = h_arr[max(0, i - n):i + n + 1]
        win_l = l_arr[max(0, i - n):i + n + 1]
        hi = h_arr[i]
        lo = l_arr[i]
        if hi == win_h.max():
            swings.append({"idx": i, "type": "high", "price": float(hi)})
        if lo == win_l.min():
            swings.append({"idx": i, "type": "low", "price": float(lo)})
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


def detect_demand_supply_zones(df, lookback=60):
    """Demand & Supply zones (Sam Seiden-style supply/demand methodology).

    A DEMAND zone is a "base" (1-3 narrow candles) followed by a strong
    bullish impulse — Rally-Base-Rally (RBR) or Drop-Base-Rally (DBR).
    A SUPPLY zone is a base followed by a strong bearish impulse —
    Drop-Base-Drop (DBD) or Rally-Base-Drop (RBD).

    Each zone is the high+low of the BASE candle(s). Fresh = unmitigated.
    """
    if len(df) < 20:
        return {"demand": [], "supply": []}

    o = df["open"].astype(float).values
    h = df["high"].astype(float).values
    l = df["low"].astype(float).values
    c = df["close"].astype(float).values
    n = len(df)
    rng = h - l
    avg_rng = pd.Series(rng).rolling(20).mean().fillna(0).values
    body = np.abs(c - o)

    demand = []
    supply = []
    cp = float(c[-1])

    start = max(20, n - lookback)
    for i in range(start, n - 2):
        # Base candle: narrow range AND small body (consolidation)
        if avg_rng[i] <= 0:
            continue
        is_base = rng[i] <= avg_rng[i] * 0.7 and body[i] < rng[i] * 0.5
        if not is_base:
            continue

        # Look at next 1-3 bars for strong impulse
        impulse_idx = i + 1
        if impulse_idx >= n:
            continue
        next_rng = rng[impulse_idx]
        next_body = body[impulse_idx]
        is_strong_impulse = next_rng >= avg_rng[impulse_idx] * 1.5 and next_body >= next_rng * 0.6

        if not is_strong_impulse:
            continue

        zone_top = float(h[i])
        zone_bot = float(l[i])
        time_str = df.iloc[i]["date"].strftime("%Y-%m-%d") if "date" in df.columns else None

        # Bullish impulse (close > open by a wide margin)
        if c[impulse_idx] > o[impulse_idx] and (c[impulse_idx] - o[impulse_idx]) / o[impulse_idx] > 0.015:
            # Demand zone — was it tested afterward?
            mitigated = False
            for j in range(impulse_idx + 1, n):
                if l[j] < zone_bot:
                    mitigated = True
                    break
            if zone_top < cp:  # only show below current price
                demand.append({
                    "type": "DEMAND",
                    "subtype": "RBR" if c[i - 1] > o[i - 1] else "DBR",
                    "top": round(zone_top, 2), "bottom": round(zone_bot, 2),
                    "base_time": time_str,
                    "impulse_pct": round((c[impulse_idx] - o[impulse_idx]) / o[impulse_idx] * 100, 1),
                    "mitigated": bool(mitigated),
                })
        # Bearish impulse
        elif c[impulse_idx] < o[impulse_idx] and (o[impulse_idx] - c[impulse_idx]) / o[impulse_idx] > 0.015:
            mitigated = False
            for j in range(impulse_idx + 1, n):
                if h[j] > zone_top:
                    mitigated = True
                    break
            if zone_bot > cp:  # only show above current price
                supply.append({
                    "type": "SUPPLY",
                    "subtype": "DBD" if c[i - 1] < o[i - 1] else "RBD",
                    "top": round(zone_top, 2), "bottom": round(zone_bot, 2),
                    "base_time": time_str,
                    "impulse_pct": round((o[impulse_idx] - c[impulse_idx]) / o[impulse_idx] * 100, 1),
                    "mitigated": bool(mitigated),
                })

    # Keep only fresh (unmitigated), nearest to price
    fresh_demand = sorted([d for d in demand if not d["mitigated"]],
                           key=lambda x: -x["top"])[:3]
    fresh_supply = sorted([s for s in supply if not s["mitigated"]],
                           key=lambda x: x["bottom"])[:3]
    return {"demand": fresh_demand, "supply": fresh_supply,
            "demand_all": demand[-10:], "supply_all": supply[-10:]}


def detect_htf_bias(df, weeks_lookback=12):
    """Higher-Timeframe (weekly) bias — per Annaly Trader Step 1 + #9.4.

    Resamples daily OHLC to weekly, then scans the last `weeks_lookback`
    weekly bars for HH/HL (bullish) or LH/LL (bearish) pattern. Used to
    gate daily-timeframe BUY signals: a daily BUY without weekly alignment
    is a "good entry in wrong context" = high-risk.

    Returns dict with bias, direction, weekly_swing_high, weekly_swing_low,
    and a narrative.
    """
    if df is None or len(df) < 30:
        return None
    try:
        d = df.copy()
        if "date" not in d.columns:
            return None
        d["date"] = pd.to_datetime(d["date"])
        d = d.set_index("date")
        wk = pd.DataFrame({
            "open": d["open"].resample("W").first(),
            "high": d["high"].resample("W").max(),
            "low": d["low"].resample("W").min(),
            "close": d["close"].resample("W").last(),
        }).dropna()
        if len(wk) < 4:
            return None

        recent = wk.tail(min(weeks_lookback, len(wk)))
        highs = recent["high"].values
        lows = recent["low"].values
        closes = recent["close"].values

        # Compare last 4 weeks pivots: HH/HL vs LH/LL
        n = len(recent)
        last_high = highs[-1]
        last_low = lows[-1]
        prev_high = max(highs[max(0, n - 4):n - 1]) if n >= 2 else last_high
        prev_low = min(lows[max(0, n - 4):n - 1]) if n >= 2 else last_low

        # Long-trend slope: close trend over the lookback
        slope_pct = ((closes[-1] - closes[0]) / closes[0] * 100) if closes[0] else 0

        bullish = last_high >= prev_high * 0.99 and last_low > prev_low * 0.97 and slope_pct > 2
        bearish = last_low <= prev_low * 1.01 and last_high < prev_high * 1.03 and slope_pct < -2

        if bullish:
            bias = "BULLISH"
            narrative = (
                f"Weekly: making higher highs (৳{last_high:.1f} ≥ ৳{prev_high:.1f}) and higher lows "
                f"(৳{last_low:.1f} > ৳{prev_low:.1f}). Trend +{slope_pct:.0f}% over {n}w."
            )
        elif bearish:
            bias = "BEARISH"
            narrative = (
                f"Weekly: lower highs (৳{last_high:.1f} < ৳{prev_high:.1f}) and lower lows "
                f"(৳{last_low:.1f} ≤ ৳{prev_low:.1f}). Trend {slope_pct:.0f}% over {n}w."
            )
        else:
            bias = "RANGE"
            narrative = (
                f"Weekly: ranging — HH/LL pattern unclear. Trend {slope_pct:+.0f}% over {n}w. "
                f"Range ৳{lows.min():.1f}-৳{highs.max():.1f}."
            )

        return {
            "bias": bias,
            "narrative": narrative,
            "weekly_swing_high": round(float(highs.max()), 2),
            "weekly_swing_low": round(float(lows.min()), 2),
            "trend_pct": round(slope_pct, 1),
            "weeks_analysed": int(n),
        }
    except Exception:
        return None


def detect_liquidity_sweep(df, swings, lookback=20):
    """Liquidity sweep vs real breakout — per Annaly Trader #9.3.

    A liquidity sweep = price wicks above a recent swing high (or below a
    swing low) but CLOSES BACK INSIDE the prior range within 1-2 bars. This
    is institutions hunting retail stop orders, NOT a breakout.

    A real breakout = price closes above the swing high WITH displacement
    (large body) and continues higher on the next bar.

    Returns dict with recent sweeps + a "latest event" classification.
    """
    if df is None or len(df) < 10 or not swings:
        return None
    try:
        h = df["high"].astype(float).values
        l = df["low"].astype(float).values
        c = df["close"].astype(float).values
        o = df["open"].astype(float).values
        n = len(df)

        # ATR proxy for displacement
        rng = h - l
        avg_rng = pd.Series(rng).rolling(14).mean().fillna(method="ffill").values

        # Recent swing pivots (last lookback bars)
        recent_swings = [s for s in swings if s.get("idx", 0) >= n - lookback]
        if not recent_swings:
            return {"events": [], "latest": None}

        events = []
        for s in recent_swings:
            sidx = s.get("idx", 0)
            spx = float(s.get("price", 0))
            stype = s.get("type")  # "high" or "low"
            if stype not in ("high", "low") or sidx >= n - 1 or spx <= 0:
                continue

            # Look for piercing within 1-3 bars after the swing
            for j in range(sidx + 1, min(sidx + 4, n)):
                if stype == "high":
                    pierced = h[j] > spx
                    closed_inside = c[j] < spx
                    body = abs(c[j] - o[j])
                    is_displacement = body > avg_rng[j] * 0.6 if avg_rng[j] else False
                    is_green = c[j] > o[j]
                    if pierced and closed_inside:
                        events.append({
                            "type": "bull_sweep",  # bullish-side sweep = swept high, reversed
                            "idx": int(j),
                            "date": df.iloc[j]["date"].strftime("%Y-%m-%d") if hasattr(df.iloc[j]["date"], "strftime") else str(df.iloc[j]["date"]),
                            "swing_price": round(spx, 2),
                            "wick_high": round(float(h[j]), 2),
                            "close": round(float(c[j]), 2),
                            "interpretation": (
                                f"Wicked above ৳{round(spx,2)} but closed inside at ৳{round(float(c[j]),2)} "
                                f"= liquidity sweep, NOT breakout. Stops above were hunted."
                            ),
                        })
                        break
                    elif pierced and not closed_inside and is_displacement and is_green:
                        events.append({
                            "type": "real_breakout_up",
                            "idx": int(j),
                            "date": df.iloc[j]["date"].strftime("%Y-%m-%d") if hasattr(df.iloc[j]["date"], "strftime") else str(df.iloc[j]["date"]),
                            "swing_price": round(spx, 2),
                            "close": round(float(c[j]), 2),
                            "interpretation": (
                                f"Closed above ৳{round(spx,2)} at ৳{round(float(c[j]),2)} with "
                                f"displacement (body > 0.6× ATR) = real bullish breakout."
                            ),
                        })
                        break
                else:  # swing low
                    pierced = l[j] < spx
                    closed_inside = c[j] > spx
                    body = abs(c[j] - o[j])
                    is_displacement = body > avg_rng[j] * 0.6 if avg_rng[j] else False
                    is_red = c[j] < o[j]
                    if pierced and closed_inside:
                        events.append({
                            "type": "bear_sweep",  # swept low, reversed
                            "idx": int(j),
                            "date": df.iloc[j]["date"].strftime("%Y-%m-%d") if hasattr(df.iloc[j]["date"], "strftime") else str(df.iloc[j]["date"]),
                            "swing_price": round(spx, 2),
                            "wick_low": round(float(l[j]), 2),
                            "close": round(float(c[j]), 2),
                            "interpretation": (
                                f"Wicked below ৳{round(spx,2)} but closed inside at ৳{round(float(c[j]),2)} "
                                f"= liquidity sweep, NOT breakdown. Stops below were hunted (often a bottom)."
                            ),
                        })
                        break
                    elif pierced and not closed_inside and is_displacement and is_red:
                        events.append({
                            "type": "real_breakdown",
                            "idx": int(j),
                            "date": df.iloc[j]["date"].strftime("%Y-%m-%d") if hasattr(df.iloc[j]["date"], "strftime") else str(df.iloc[j]["date"]),
                            "swing_price": round(spx, 2),
                            "close": round(float(c[j]), 2),
                            "interpretation": (
                                f"Closed below ৳{round(spx,2)} at ৳{round(float(c[j]),2)} with "
                                f"displacement = real bearish breakdown."
                            ),
                        })
                        break

        latest = events[-1] if events else None
        return {"events": events[-10:], "latest": latest}
    except Exception:
        return None


def detect_volatility_imbalance(df, lookback=40):
    """Volatility Imbalance (VI) — single-candle gap caused by news/spike.

    Different from FVG which is a 3-candle pattern. VI is when a single
    candle's BODY is fully above (or below) the previous candle's body,
    leaving an inefficient transition. Common around earnings, news.

    Bullish VI: today's open >= yesterday's high (gap up) AND green close
    Bearish VI: today's open <= yesterday's low (gap down) AND red close
    """
    if len(df) < 5:
        return []
    o = df["open"].astype(float).values
    h = df["high"].astype(float).values
    l = df["low"].astype(float).values
    c = df["close"].astype(float).values
    n = len(df)
    cp = float(c[-1])

    out = []
    start = max(1, n - lookback)
    for i in range(start, n):
        # Bullish VI: gap-up + green
        if o[i] >= h[i - 1] * 1.001 and c[i] > o[i]:
            top = float(o[i])
            bot = float(h[i - 1])
            mitigated = False
            for j in range(i + 1, n):
                if l[j] < bot:
                    mitigated = True
                    break
            time_str = df.iloc[i]["date"].strftime("%Y-%m-%d") if "date" in df.columns else None
            out.append({
                "type": "VI_BULLISH", "top": round(top, 2), "bottom": round(bot, 2),
                "time": time_str, "mitigated": bool(mitigated),
                "is_below_price": bot < cp,
            })
        # Bearish VI: gap-down + red
        elif o[i] <= l[i - 1] * 0.999 and c[i] < o[i]:
            top = float(l[i - 1])
            bot = float(o[i])
            mitigated = False
            for j in range(i + 1, n):
                if h[j] > top:
                    mitigated = True
                    break
            time_str = df.iloc[i]["date"].strftime("%Y-%m-%d") if "date" in df.columns else None
            out.append({
                "type": "VI_BEARISH", "top": round(top, 2), "bottom": round(bot, 2),
                "time": time_str, "mitigated": bool(mitigated),
                "is_above_price": top > cp,
            })
    return out[-10:]


def detect_elliott_triangle(swings, df, lookback=80):
    """5-point Elliott Wave triangle (A-B-C-D-E).

    The pattern: 5 alternating swings forming a converging triangle, where each
    successive swing is SMALLER than the previous (contracting). After E, price
    typically breaks out in the prior trend direction. This is what the Bengali
    analyst is watching.

    Pattern requirements:
      - 5 alternating swings (H-L-H-L-H or L-H-L-H-L)
      - Each leg shorter than the prior (contracting)
      - Trendline from B-D points should converge with trendline from A-C-E
      - Last point E hasn't broken yet (still inside the triangle)
    """
    if not swings or len(df) < 30:
        return None

    cutoff = max(0, len(df) - lookback)
    recent = sorted([s for s in swings if s["idx"] >= cutoff], key=lambda s: s["idx"])
    if len(recent) < 5:
        return None

    # Look at last 5-7 alternating swings
    # Find the latest 5-point alternating sequence
    for start in range(max(0, len(recent) - 7), len(recent) - 4):
        seq = recent[start:start + 5]
        if len(seq) < 5:
            continue
        types = [s["type"] for s in seq]
        # Must alternate
        if not (types == ["high","low","high","low","high"] or
                types == ["low","high","low","high","low"]):
            continue

        prices = [s["price"] for s in seq]
        # Each leg shorter than the previous (contracting)
        legs = [abs(prices[i+1] - prices[i]) for i in range(4)]
        if not all(legs[i] >= legs[i+1] * 0.85 for i in range(3)):
            continue
        contracting = all(legs[i] > legs[i+1] for i in range(3))
        if not contracting:
            continue

        a, b, c, d, e = seq
        # Triangle direction
        is_descending_high = a["type"] == "high" and prices[0] > prices[2] > prices[4]
        is_ascending_low = a["type"] == "low" and prices[0] < prices[2] < prices[4]
        # Project breakout: after E, the prior trend resumes
        # Estimate the height of the triangle as A-B distance
        height = abs(prices[0] - prices[1])
        breakout_target_up = round(prices[4] + height, 2)
        breakout_target_dn = round(prices[4] - height, 2)
        cp = float(df["close"].iloc[-1])
        bias = "bullish" if cp > prices[4] else "bearish" if cp < min(prices[1], prices[3]) else "pending"

        return {
            "type": "ELLIOTT_TRIANGLE",
            "points": [
                {"label": "A", "price": round(prices[0], 2),
                 "time": df.iloc[a["idx"]]["date"].strftime("%Y-%m-%d") if "date" in df.columns else None},
                {"label": "B", "price": round(prices[1], 2),
                 "time": df.iloc[b["idx"]]["date"].strftime("%Y-%m-%d") if "date" in df.columns else None},
                {"label": "C", "price": round(prices[2], 2),
                 "time": df.iloc[c["idx"]]["date"].strftime("%Y-%m-%d") if "date" in df.columns else None},
                {"label": "D", "price": round(prices[3], 2),
                 "time": df.iloc[d["idx"]]["date"].strftime("%Y-%m-%d") if "date" in df.columns else None},
                {"label": "E", "price": round(prices[4], 2),
                 "time": df.iloc[e["idx"]]["date"].strftime("%Y-%m-%d") if "date" in df.columns else None},
            ],
            "contracting": True,
            "kind": "descending" if is_descending_high else ("ascending" if is_ascending_low else "symmetrical"),
            "current_price": round(cp, 2),
            "bias": bias,
            "breakout_up_target": breakout_target_up,
            "breakdown_target": breakout_target_dn,
            "narrative": (
                f"Elliott 5-point contracting triangle (A→E). After E, price typically breaks out "
                f"in the prior trend direction. Targets: ↑ ৳{breakout_target_up} (height projection up) "
                f"or ↓ ৳{breakout_target_dn}. Wait for break + close beyond the triangle line before entering."
            ),
        }
    return None


def detect_fib_dealing_range(swings, df, lookback=60):
    """ICT/SMC dealing-range Fibonacci — the strategy in the Bengali post.

    Find the most recent IMPULSE LEG (largest swing low → swing high in
    last lookback bars), then label every key Fib level with a textual
    interpretation. This is the "buy below 50% / sell above premium" play.

    Returns:
      {
        "swing_low": float, "swing_low_time": str,
        "swing_high": float, "swing_high_time": str,
        "leg_size_pct": float,           # how big the impulse was
        "levels": [
          {"ratio": 0.0,   "price": ..., "label": "100% (Range Low)",
           "zone": "extreme_discount", "action": "Strong BUY zone"},
          {"ratio": 0.236, "price": ..., "label": "23.6% retracement",
           "zone": "extreme_discount", "action": "Buy zone — first re-entry"},
          ...
        ],
        "current_pct": float,             # where price is in 0-100% of leg
        "current_zone": str,              # human label
        "action_text": str,               # "BUY", "SELL", "WAIT"
        "narrative": str,                 # plain-English trade plan
        "valid": bool,                    # is this a usable setup?
      }
    """
    if not swings or len(df) < 20:
        return None

    cutoff = max(0, len(df) - lookback)
    recent = [s for s in swings if s["idx"] >= cutoff]
    if len(recent) < 2:
        return None

    # Find the largest impulse leg: the most recent significant swing low
    # paired with the highest swing high after it.
    swing_lows = [s for s in recent if s["type"] == "low"]
    swing_highs = [s for s in recent if s["type"] == "high"]
    if not swing_lows or not swing_highs:
        return None

    # Most recent meaningful leg: pair the deepest low with the highest high
    # AFTER it. If high comes before all lows, swap (downtrend setup).
    sl = min(swing_lows, key=lambda s: s["price"])
    later_highs = [s for s in swing_highs if s["idx"] > sl["idx"]]
    if later_highs:
        sh = max(later_highs, key=lambda s: s["price"])
        is_uptrend_leg = True
    else:
        sh = max(swing_highs, key=lambda s: s["price"])
        # Pair with deepest low AFTER sh
        later_lows = [s for s in swing_lows if s["idx"] > sh["idx"]]
        if later_lows:
            sl = min(later_lows, key=lambda s: s["price"])
        is_uptrend_leg = False

    leg_size_pct = (sh["price"] - sl["price"]) / sl["price"] * 100
    if leg_size_pct < 5:
        return None  # leg too small to trade

    # Current price position on the leg
    cp = float(df["close"].iloc[-1])
    rng = sh["price"] - sl["price"]
    if is_uptrend_leg:
        # Pullback measured from high downward: 0% = high, 100% = low
        retracement_pct = (sh["price"] - cp) / rng * 100
    else:
        retracement_pct = (cp - sl["price"]) / rng * 100

    # Build the level table with textual explanations
    fib_ratios = [
        (0.000, "0% (Swing High — Premium top)", "premium_extreme",
         "Take-profit / sell zone — exhaustion"),
        (0.236, "23.6% retracement", "premium",
         "Shallow pullback — premium zone, weak buy"),
        (0.382, "38.2% retracement", "premium",
         "Premium zone — selling pressure dominant"),
        (0.500, "50% Equilibrium", "equilibrium",
         "Decision line — wait for direction"),
        (0.618, "61.8% Golden Pocket", "discount",
         "Discount zone — first BUY trigger"),
        (0.786, "78.6% Deep Discount", "discount_extreme",
         "Strong BUY — institutional re-entry zone"),
        (1.000, "100% (Swing Low — Discount floor)", "discount_extreme",
         "Strongest BUY — leg invalidation if breaks"),
    ]
    levels = []
    for ratio, label, zone, action in fib_ratios:
        price = round(sh["price"] - rng * ratio, 2) if is_uptrend_leg \
                else round(sl["price"] + rng * ratio, 2)
        levels.append({
            "ratio": ratio, "price": price, "label": label,
            "zone": zone, "action": action,
        })

    # Determine where price sits → action
    pct = round(retracement_pct, 1)
    if pct < 23.6:
        zone, action_text = "premium_extreme", "SELL — take profit"
    elif pct < 38.2:
        zone, action_text = "premium", "PARTIAL SELL — premium zone"
    elif pct < 50:
        zone, action_text = "premium_lower", "WAIT — between EQ and premium"
    elif pct < 61.8:
        zone, action_text = "equilibrium_lower", "WATCH — just below 50%, early discount"
    elif pct < 78.6:
        zone, action_text = "discount_golden", "BUY — Golden Pocket discount"
    elif pct < 100:
        zone, action_text = "discount_extreme", "STRONG BUY — deep discount"
    else:
        zone, action_text = "below_leg", "INVALIDATED — broke swing low"

    if pct < 0:
        zone, action_text = "above_leg", "EXTENDED — past swing high"

    # Plain-language narrative
    if "BUY" in action_text:
        narrative = (
            f"Price retraced {pct:.0f}% of the last impulse leg "
            f"(৳{sl['price']:.1f} → ৳{sh['price']:.1f}, +{leg_size_pct:.0f}%). "
            f"This is a {zone.replace('_', ' ')} entry — institutions typically "
            f"reload longs in the Golden Pocket (61.8-78.6%) where smart-money "
            f"orders sit. Target: 1st sell at 23.6% retracement (premium), "
            f"2nd sell at 0% (range high)."
        )
    elif "SELL" in action_text:
        narrative = (
            f"Price at {pct:.0f}% retracement = inside premium zone. "
            f"This is where institutions distribute. Take partial profits at "
            f"23.6% (1st sell) and full exit at 0% (range high). "
            f"Don't initiate fresh longs from here."
        )
    elif "WAIT" in action_text or "WATCH" in action_text:
        narrative = (
            f"Price near 50% equilibrium ({pct:.0f}%). No edge either side. "
            f"Wait for either: deeper retrace into Golden Pocket (61.8%) for "
            f"BUY, or rejection from premium zone for SELL."
        )
    else:
        narrative = (
            f"Price {pct:.0f}% of leg — outside actionable range. "
            f"Wait for a new impulse leg to form before applying Fib strategy."
        )

    valid = 23.6 <= pct <= 100  # actionable retracement zone

    return {
        "swing_low": round(sl["price"], 2),
        "swing_low_time": df.iloc[sl["idx"]]["date"].strftime("%Y-%m-%d") if "date" in df.columns else None,
        "swing_high": round(sh["price"], 2),
        "swing_high_time": df.iloc[sh["idx"]]["date"].strftime("%Y-%m-%d") if "date" in df.columns else None,
        "leg_size_pct": round(leg_size_pct, 1),
        "is_uptrend_leg": is_uptrend_leg,
        "current_pct": pct,
        "current_zone": zone,
        "action_text": action_text,
        "narrative": narrative,
        "levels": levels,
        "valid": valid,
    }


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
    # Numpy arrays for fast scalar access (avoids pandas .iloc overhead)
    o_arr = o.values if hasattr(o, "values") else o
    h_arr = h.values if hasattr(h, "values") else h
    l_arr = l.values if hasattr(l, "values") else l
    c_arr = c.values if hasattr(c, "values") else c

    for ev in structure_events:
        ev_idx = ev["idx"]
        is_bullish_break = ev["type"] in ("bullish_BOS", "bullish_ChoCh")
        is_bearish_break = ev["type"] in ("bearish_BOS", "bearish_ChoCh")

        for j in range(ev_idx - 1, max(ev_idx - 16, 0), -1):
            cj = float(c_arr[j]); oj = float(o_arr[j])
            candle_red = cj < oj
            candle_green = cj > oj

            if is_bullish_break and candle_red:
                ob_top = float(h_arr[j])
                ob_bottom = float(l_arr[j])
                if ob_top <= ob_bottom or (ob_top - ob_bottom) / ob_bottom < 0.005:
                    break
                obs.append({
                    "type": "bullish", "candle_idx": j, "break_idx": ev_idx,
                    "break_type": ev["type"], "top": ob_top, "bottom": ob_bottom,
                })
                break

            if is_bearish_break and candle_green:
                ob_top = float(h_arr[j])
                ob_bottom = float(l_arr[j])
                if ob_top <= ob_bottom or (ob_top - ob_bottom) / ob_bottom < 0.005:
                    break
                obs.append({
                    "type": "bearish", "candle_idx": j, "break_idx": ev_idx,
                    "break_type": ev["type"], "top": ob_top, "bottom": ob_bottom,
                })
                break

    # Tag mitigation status by scanning forward from break_idx (numpy)
    n = len(df)
    for ob in obs:
        ob["status"] = "fresh"
        for k in range(ob["break_idx"] + 1, n):
            row_high = float(h_arr[k]); row_low = float(l_arr[k]); row_close = float(c_arr[k])
            if ob["type"] == "bullish":
                if row_low <= ob["top"] and row_close > ob["bottom"]:
                    if ob["status"] == "fresh":
                        ob["status"] = "tested"
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


def detect_double_top(swings, df, tolerance_pct=2.0, min_separation=5, max_swings=40):
    """Two highs within tolerance% of each other, with a valley between.
    Uses numpy arrays + lookback cap for performance on long histories."""
    patterns = []
    highs = [s for s in swings if s["type"] == "high"][-max_swings:]
    if not highs:
        return patterns
    low_arr = df["low"].values  # avoid pandas .iloc per call
    for i in range(len(highs)):
        for j in range(i + 1, len(highs)):
            h1, h2 = highs[i], highs[j]
            sep = h2["idx"] - h1["idx"]
            if sep < min_separation or sep > 50:
                continue
            avg = (h1["price"] + h2["price"]) / 2
            if abs(h1["price"] - h2["price"]) / avg * 100 > tolerance_pct:
                continue
            valley_low = float(low_arr[h1["idx"]:h2["idx"]].min()) if h2["idx"] > h1["idx"] else avg
            if valley_low > min(h1["price"], h2["price"]) * 0.97:
                continue
            neckline = valley_low
            target = neckline - (avg - neckline)
            patterns.append({
                "type": "double_top",
                "p1_idx": h1["idx"], "p1_price": round(h1["price"], 2),
                "p2_idx": h2["idx"], "p2_price": round(h2["price"], 2),
                "neckline": round(neckline, 2),
                "target": round(target, 2),
                "bias": "bearish",
            })
    return patterns


def detect_double_bottom(swings, df, tolerance_pct=2.0, min_separation=5, max_swings=40):
    """Two lows within tolerance% of each other, with a peak between."""
    patterns = []
    lows = [s for s in swings if s["type"] == "low"][-max_swings:]
    if not lows:
        return patterns
    high_arr = df["high"].values
    for i in range(len(lows)):
        for j in range(i + 1, len(lows)):
            l1, l2 = lows[i], lows[j]
            sep = l2["idx"] - l1["idx"]
            if sep < min_separation or sep > 50:
                continue
            avg = (l1["price"] + l2["price"]) / 2
            if abs(l1["price"] - l2["price"]) / avg * 100 > tolerance_pct:
                continue
            peak_high = float(high_arr[l1["idx"]:l2["idx"]].max()) if l2["idx"] > l1["idx"] else avg
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


def detect_cup_and_handle(df, swings, min_cup_bars=15, max_cup_bars=120, max_swings=30):
    """Cup & handle: U-shape recovery to previous high + small handle pullback.
    Capped to last `max_swings` swings; uses numpy arrays for speed."""
    patterns = []
    if len(df) < min_cup_bars + 5:
        return patterns

    low_arr = df["low"].values
    n = len(df)
    highs = [s for s in swings if s["type"] == "high"][-max_swings:]

    for i in range(len(highs) - 1):
        for j in range(i + 1, len(highs)):
            left_rim = highs[i]
            right_rim = highs[j]
            cup_bars = right_rim["idx"] - left_rim["idx"]
            if cup_bars < min_cup_bars or cup_bars > max_cup_bars:
                continue
            avg_rim = (left_rim["price"] + right_rim["price"]) / 2
            if abs(left_rim["price"] - right_rim["price"]) / avg_rim > 0.05:
                continue
            seg = low_arr[left_rim["idx"]:right_rim["idx"]]
            if len(seg) == 0:
                continue
            cup_low_local = int(seg.argmin())
            cup_low_idx = left_rim["idx"] + cup_low_local
            cup_bottom = float(seg.min())
            cup_depth = (avg_rim - cup_bottom) / avg_rim
            if cup_depth < 0.10 or cup_depth > 0.50:
                continue
            handle_end_idx = min(right_rim["idx"] + 15, n - 1)
            handle_seg = low_arr[right_rim["idx"]:handle_end_idx + 1]
            handle_low = float(handle_seg.min()) if len(handle_seg) else avg_rim
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


def generate_analysis(structure_events, fvgs, order_blocks, key_levels, current_price, df,
                      premium_discount=None, bos_zones=None, fib_dealing_range=None,
                      htf_bias=None, liquidity_sweeps=None,
                      support_resistance=None):
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

    # Trade-grade FVGs only: tier in {core, discount, premium} skip "outside"
    # zones. Among matching, prefer 'core' (near equilibrium) over deep zones.
    def _grade_score(f):
        tier = f.get("tier", "secondary")
        return {"core": 0, "discount": 1, "premium": 1, "secondary": 2, "outside": 3}.get(tier, 9)

    fresh_bull_fvgs_below = sorted(
        [f for f in fvgs if f.get("type") == "bullish" and not f.get("mitigated")
         and f["top"] < current_price
         and f.get("tier") in ("core", "discount", "secondary")],
        key=lambda x: (_grade_score(x), -x["top"]),
    )
    fresh_bear_fvgs_above = sorted(
        [f for f in fvgs if f.get("type") == "bearish" and not f.get("mitigated")
         and f["bottom"] > current_price
         and f.get("tier") in ("core", "premium", "secondary")],
        key=lambda x: (_grade_score(x), x["bottom"]),
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

    # 4. Trendiness gate — backtest showed FVG strategies only have edge in
    # clean uptrends. ADX(14)>25 + 90-bar return positive + 20-day range >= 8%.
    is_trendy_market = False
    adx_value = None
    try:
        if len(df) >= 35:
            h_arr = df["high"].astype(float).values
            l_arr = df["low"].astype(float).values
            c_arr = df["close"].astype(float).values
            n = len(h_arr)
            tr = [0.0] * n
            pdm = [0.0] * n
            mdm = [0.0] * n
            for i in range(1, n):
                tr[i] = max(h_arr[i] - l_arr[i], abs(h_arr[i] - c_arr[i - 1]), abs(l_arr[i] - c_arr[i - 1]))
                up = h_arr[i] - h_arr[i - 1]
                dn = l_arr[i - 1] - l_arr[i]
                pdm[i] = up if (up > dn and up > 0) else 0
                mdm[i] = dn if (dn > up and dn > 0) else 0
            p = 14
            atr_w = [0.0] * n
            pdm_w = [0.0] * n
            mdm_w = [0.0] * n
            atr_w[p] = sum(tr[1:p + 1])
            pdm_w[p] = sum(pdm[1:p + 1])
            mdm_w[p] = sum(mdm[1:p + 1])
            for i in range(p + 1, n):
                atr_w[i] = atr_w[i - 1] - atr_w[i - 1] / p + tr[i]
                pdm_w[i] = pdm_w[i - 1] - pdm_w[i - 1] / p + pdm[i]
                mdm_w[i] = mdm_w[i - 1] - mdm_w[i - 1] / p + mdm[i]
            dx_arr = [0.0] * n
            for i in range(p, n):
                if atr_w[i] > 0:
                    pdi = 100 * pdm_w[i] / atr_w[i]
                    mdi = 100 * mdm_w[i] / atr_w[i]
                    s = pdi + mdi
                    if s > 0:
                        dx_arr[i] = 100 * abs(pdi - mdi) / s
            if 2 * p < n:
                adx_arr = [0.0] * n
                adx_arr[2 * p] = sum(dx_arr[p + 1: 2 * p + 1]) / p
                for i in range(2 * p + 1, n):
                    adx_arr[i] = (adx_arr[i - 1] * (p - 1) + dx_arr[i]) / p
                adx_value = round(adx_arr[-1], 1) if adx_arr[-1] > 0 else None

            ret90 = 0
            if len(df) >= 90:
                p0 = float(df["close"].iloc[-90])
                if p0 > 0:
                    ret90 = (current_price - p0) / p0
            range20_pct = 0
            if len(df) >= 20:
                rh20 = float(df["high"].iloc[-20:].max())
                rl20 = float(df["low"].iloc[-20:].min())
                if current_price > 0:
                    range20_pct = (rh20 - rl20) / current_price

            is_trendy_market = (
                adx_value is not None and adx_value >= 25
                and ret90 > 0 and range20_pct >= 0.08
            )
    except Exception:
        is_trendy_market = False

    # 5. CONFLUENCE — fresh bullish FVG zone overlapping a multi-touch support.
    # Backtest showed this is the highest-edge entry (53% win, 1.49 PF).
    confluence_zone = None
    try:
        # Reuse `support_resistance` style detection: cluster recent swing lows
        if len(df) >= 30 and fresh_bull_fvgs_below:
            lookback = min(60, len(df))
            sub_h = df["high"].iloc[-lookback:]
            sub_l = df["low"].iloc[-lookback:]
            sub_swings = find_swings(sub_h.reset_index(drop=True), sub_l.reset_index(drop=True), n=2)
            lows = sorted([s["price"] for s in sub_swings if s["type"] == "low"])
            clusters = []
            if lows:
                cur = [lows[0]]
                for px in lows[1:]:
                    if (px - cur[-1]) / cur[-1] < 0.015:
                        cur.append(px)
                    else:
                        clusters.append(cur); cur = [px]
                clusters.append(cur)
            for f in fresh_bull_fvgs_below[:3]:
                for cl in clusters:
                    if len(cl) < 2:
                        continue
                    sup_px = sum(cl) / len(cl)
                    if f["bottom"] * 0.98 <= sup_px <= f["top"] * 1.02:
                        confluence_zone = {**f, "support_price": round(sup_px, 2),
                                           "support_touches": len(cl)}
                        break
                if confluence_zone:
                    break
    except Exception:
        confluence_zone = None

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
        # Non-trendy market: backtest shows FVG signals fail in flat/choppy
        # tape. Cap confidence to LOW unless ADX>25 + return + range gates pass.
        if not is_trendy_market and confidence in ("HIGH", "MEDIUM"):
            confidence = "LOW"
        # CONFLUENCE bonus: fresh FVG + support overlap = highest-edge setup,
        # promote one notch (LOW→MEDIUM, MEDIUM→HIGH) — but only in trendy market
        if confluence_zone is not None and is_trendy_market:
            if confidence == "LOW":
                confidence = "MEDIUM"
            elif confidence == "MEDIUM":
                confidence = "HIGH"
        # Order-flow absorption: today's bar shows volume + lower-wick rejection
        # + close strength = real institutional buyer absorbing supply. Promote.
        try:
            from api.order_flow import detect_absorption as _abs
            abs_today = _abs(df)
            if abs_today and abs_today.get("absorbed"):
                if confidence == "LOW":
                    confidence = "MEDIUM"
                elif confidence == "MEDIUM":
                    confidence = "HIGH"
        except Exception:
            pass

        # HTF (weekly) bias gate — Annaly Trader Step 1 / #9.4.
        # A daily BUY without weekly alignment is "good entry in wrong context".
        if htf_bias and htf_bias.get("bias"):
            htf_b = htf_bias["bias"]
            if htf_b == "BEARISH" and confidence in ("HIGH", "MEDIUM"):
                confidence = "LOW"  # weekly bearish, daily bullish = counter-trend
            elif htf_b == "RANGE" and confidence == "HIGH":
                confidence = "MEDIUM"  # weekly ranging, daily can chop too

        # Liquidity sweep penalty — if recent BOS was actually a sweep (not a
        # real breakout), the bullish bias is suspect.
        if liquidity_sweeps and liquidity_sweeps.get("latest"):
            lt = liquidity_sweeps["latest"].get("type", "")
            if lt == "bull_sweep" and confidence == "HIGH":
                confidence = "MEDIUM"  # latest event swept highs (bearish-leaning)
            elif lt == "real_breakout_up":
                if confidence == "LOW":
                    confidence = "MEDIUM"  # real breakout = supports bullish

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
    elif bias == "BULLISH" and not is_trendy_market:
        action = "WAIT — NO TREND"
        action_color = "gray"
        summary = (
            f"Bullish bias but market lacks trend strength "
            f"(ADX {adx_value if adx_value else 'N/A'}, need ≥25). "
            "FVG strategies fail in flat/choppy tape. Wait for trend confirmation."
        )
    elif bias == "BULLISH" and confluence_zone is not None:
        action = "BUY ON DIP — CONFLUENCE"
        action_color = "green"
        summary = (
            f"Best entry on pullback to ৳{confluence_zone['bottom']}-"
            f"{confluence_zone['top']} — fresh FVG overlapping a "
            f"{confluence_zone['support_touches']}-touch support. "
            "Highest-edge setup (53% win in backtest)."
        )
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
    if confluence_zone is not None and bias == "BULLISH":
        reasons.append(
            f"💎 CONFLUENCE: fresh FVG ৳{confluence_zone['bottom']}-{confluence_zone['top']} "
            f"meets {confluence_zone['support_touches']}-touch support @ ৳{confluence_zone['support_price']}"
        )
    if adx_value is not None and bias == "BULLISH":
        if adx_value >= 25:
            reasons.append(f"✅ Trendy market (ADX {adx_value}) — FVG signals reliable here")
        else:
            reasons.append(f"⚠ Weak trend (ADX {adx_value} < 25) — FVG signals less reliable")

    # === Trade levels ===
    entry = None
    entry_label = None
    stop_loss = None
    target1 = None
    target2 = None
    rr = None

    # Entry zone = a RANGE, not a single price.
    # Tier-2 is the HIGH-EDGE setup determined by chart structure (FVG +
    # multi-touch support confluence, deepest tested support). Whatever the
    # formulas find — that's the level. The bucket logic (entry_status) tells
    # the user whether it's at-entry, wait-pullback, or too-far.
    # The Tier-1 "aggressive_entry" computed later picks a CLOSER support
    # so the user has a realistic near-term option alongside the deeper one.
    def _nearest_tested_support_below(min_touches=2):
        """Find the closest multi-touch support below current — no distance cap."""
        if not support_resistance:
            return None
        candidates = [
            r for r in support_resistance
            if r.get("role") == "support"
            and r.get("touches", 0) >= min_touches
            and float(r.get("price", 0)) < current_price
        ]
        if not candidates:
            return None
        return sorted(candidates, key=lambda r: -float(r.get("price", 0)))[0]

    def _deepest_strong_support_below(min_touches=3):
        """Find the DEEPEST strong support below current (high-edge level)."""
        if not support_resistance:
            return None
        candidates = [
            r for r in support_resistance
            if r.get("role") == "support"
            and r.get("touches", 0) >= min_touches
            and float(r.get("price", 0)) < current_price
        ]
        if not candidates:
            return None
        return sorted(candidates, key=lambda r: float(r.get("price", 0)))[0]

    entry_zone_low = None
    entry_zone_high = None
    if action.startswith("BUY"):
        # Tier-2 PATIENT (high-edge): pick the strongest available below
        # current — the chart formulas decide the level, no artificial cap.
        # Priority: CONFLUENCE (FVG + support overlap) > strongest support cluster > deepest FVG
        if "CONFLUENCE" in action and confluence_zone is not None:
            entry = round(confluence_zone["top"], 2)
            entry_zone_low = round(confluence_zone["bottom"], 2)
            entry_zone_high = round(confluence_zone["top"], 2)
            entry_label = (
                f"Limit ৳{entry_zone_low}-{entry_zone_high} "
                f"(FVG retest at {confluence_zone['support_touches']}-touch "
                f"support — confluence)"
            )
        elif (sup := _deepest_strong_support_below(min_touches=3)):
            sup_px = float(sup["price"])
            touches = int(sup["touches"])
            entry = round(sup_px, 2)
            entry_zone_low = round(sup_px * 0.99, 2)
            entry_zone_high = round(sup_px * 1.01, 2)
            entry_label = (
                f"Limit ৳{entry_zone_low}-{entry_zone_high} "
                f"(deepest {touches}-touch tested support)"
            )
        elif "DIP" in action and fresh_bull_fvgs_below:
            # Deepest fresh FVG (high-edge)
            f = sorted(fresh_bull_fvgs_below, key=lambda x: float(x["top"]))[0]
            entry = round(f["top"], 2)
            entry_zone_low = round(f["bottom"], 2)
            entry_zone_high = round(f["top"], 2)
            entry_label = f"Limit ৳{entry_zone_low}-{entry_zone_high} (FVG retest)"
        elif (sup := _nearest_tested_support_below(min_touches=2)):
            sup_px = float(sup["price"])
            touches = int(sup["touches"])
            entry = round(sup_px, 2)
            entry_zone_low = round(sup_px * 0.99, 2)
            entry_zone_high = round(sup_px * 1.01, 2)
            entry_label = f"Limit ৳{entry_zone_low}-{entry_zone_high} ({touches}-touch support)"
        elif "BREAKOUT" in action and current_price >= breakout_trigger:
            entry = round(current_price, 2)
            entry_zone_low = round(current_price * 0.995, 2)
            entry_zone_high = round(current_price * 1.005, 2)
            entry_label = f"Market ৳{entry} (chase ok, in breakout)"
        elif "ABOVE TRIGGER" in action:
            entry = round(breakout_trigger, 2)
            entry_zone_low = round(breakout_trigger * 0.998, 2)
            entry_zone_high = round(breakout_trigger * 1.005, 2)
            entry_label = f"Stop-buy at ৳{entry} (only fires above trigger)"
        else:
            entry = round(current_price, 2)
            entry_zone_low = round(current_price * 0.99, 2)
            entry_zone_high = round(current_price * 1.01, 2)
            entry_label = f"Market ৳{entry}"

        # Stop loss: pick CLOSEST structural support — keep stops tight (≤5%).
        # Wider stops are not tradeable for retail T+2 holders.
        candidate_stops = []
        if "CONFLUENCE" in action and confluence_zone is not None:
            # Stop just below the support that confluence sits on
            sup_px = confluence_zone.get("support_price", confluence_zone["bottom"])
            candidate_stops.append(round(sup_px * 0.98, 2))
            candidate_stops.append(round(confluence_zone["bottom"] * 0.99, 2))
        if fresh_bull_obs:
            candidate_stops.append(round(fresh_bull_obs[0]["bottom"] * 0.99, 2))
        if fresh_bull_fvgs_below:
            shallowest_fvg = fresh_bull_fvgs_below[0]
            candidate_stops.append(round(shallowest_fvg["bottom"] * 0.99, 2))
        candidate_stops.append(round(swing_low * 0.99, 2))
        # Pick the highest stop (closest to entry) that's still BELOW entry
        valid = [s for s in candidate_stops if s and 0 < s < entry]
        if valid:
            stop_loss = max(valid)
        else:
            stop_loss = round(entry * 0.97, 2)  # fallback: 3% stop

        # Hard cap stop loss at 5% from entry — wider is not retail-tradeable
        max_acceptable_stop = entry * 0.95
        if stop_loss < max_acceptable_stop:
            stop_loss = round(max_acceptable_stop, 2)
            # Mark the trade with a warning since natural stop is wider than ideal
            entry_label = (entry_label or "") + " ⚠ tight stop (5% cap)"

        # Targets
        target1 = round(swing_high * 1.05, 2)
        target2 = round(swing_high * 1.12, 2)

        # R/R
        risk = entry - stop_loss
        reward = target1 - entry
        if risk > 0:
            rr = round(reward / risk, 2)

    # === Tiered entries — aggressive (Tier 1) + patient (Tier 2 = the deep entry above) ===
    # Many setups have a deep CONFLUENCE zone (high-edge, may never fill) AND a
    # closer support (recent swing low, equilibrium, shallow FVG, Fib retrace) that
    # is realistically tradeable. Provide both so retail can choose.
    aggressive_entry = None
    aggressive_entry_label = None
    aggressive_entry_distance_pct = None
    aggressive_entry_zone_low = None
    aggressive_entry_zone_high = None
    if entry and current_price and action.startswith(("BUY", "WAIT — ENTRY")):
        # Build candidate aggressive entries: (price, label, source, zone_low, zone_high)
        # Strategy: prefer RECENT (5-7 bar) support over deeper historical lows.
        # For trending stocks, the buyable pullback support lives in the last
        # 1-2 weeks, not in the multi-week swing low.
        candidates = []
        n = len(df)
        low_arr = df["low"].astype(float).values
        high_arr = df["high"].astype(float).values

        # 1a. Last 5-bar swing low — TIGHTEST recent support
        try:
            sl_5 = float(low_arr[max(0, n-5):n].min())
            if sl_5 and sl_5 < current_price:
                zlow = round(sl_5 * 0.995, 2)
                zhigh = round(sl_5 * 1.005, 2)
                candidates.append((round(sl_5, 2), "last 5-bar swing low", "swing_low_5", zlow, zhigh))
        except Exception:
            pass

        # 1b. Last 7-bar swing low — recent consolidation
        try:
            sl_7 = float(low_arr[max(0, n-7):n].min())
            if sl_7 and sl_7 < current_price:
                zlow = round(sl_7 * 0.995, 2)
                zhigh = round(sl_7 * 1.005, 2)
                candidates.append((round(sl_7, 2), "last 7-bar swing low", "swing_low_7", zlow, zhigh))
        except Exception:
            pass

        # 1c. Last 12-bar swing low — broader recent (was original)
        try:
            sl_12 = float(low_arr[max(0, n-12):n].min())
            if sl_12 and sl_12 < current_price:
                zlow = round(sl_12 * 0.993, 2)
                zhigh = round(sl_12 * 1.007, 2)
                candidates.append((round(sl_12, 2), "last 12-bar swing low", "swing_low_12", zlow, zhigh))
        except Exception:
            pass

        # 2. Range equilibrium (50% of full range) — SMC mid-zone
        if premium_discount and premium_discount.get("equilibrium"):
            eq = float(premium_discount["equilibrium"])
            if eq and eq < current_price:
                zlow = round(eq * 0.985, 2)
                zhigh = round(eq * 1.015, 2)
                candidates.append((round(eq, 2), "range equilibrium (50%)", "equilibrium", zlow, zhigh))

        # 3. Shallow fresh bull FVG
        if fresh_bull_fvgs_below:
            for f in fresh_bull_fvgs_below:
                ftop = float(f.get("top", 0))
                fbot = float(f.get("bottom", 0))
                if ftop and ftop < current_price and ftop != entry:
                    candidates.append((
                        round(ftop, 2), "shallow FVG retest", "shallow_fvg",
                        round(fbot, 2), round(ftop, 2),
                    ))
                    break

        # 4. Fib retracement of recent leg (15 bars). For TRENDING stocks the
        # high-retrace Fibs (78.6%, 88.6%) are the realistic light-pullback
        # entries — much closer to current than the 38/50% levels.
        try:
            leg_low = float(low_arr[max(0, n-15):n].min())
            leg_high = float(high_arr[max(0, n-15):n].max())
            if leg_high > leg_low > 0:
                rng = leg_high - leg_low
                fib_786 = leg_high - rng * 0.786  # closest light retrace
                fib_618 = leg_high - rng * 0.618
                fib_500 = leg_high - rng * 0.500
                fib_382 = leg_high - rng * 0.382
                # 78.6% Fib = light pullback (closest to current in trending stocks)
                if fib_786 < current_price:
                    zlow = round(fib_786 * 0.993, 2)
                    zhigh = round(fib_786 * 1.007, 2)
                    candidates.append((round(fib_786, 2), "78.6% Fib (light retrace)", "fib_786", zlow, zhigh))
                if fib_618 < current_price:
                    zlow = round(fib_618 * 0.993, 2)
                    zhigh = round(fib_618 * 1.007, 2)
                    candidates.append((round(fib_618, 2), "61.8% Fib (golden pocket)", "fib_618", zlow, zhigh))
                if fib_500 < current_price:
                    candidates.append((
                        round(fib_500, 2), "50% Fib (mid)", "fib_500",
                        round(fib_618, 2), round(fib_500, 2),
                    ))
                if fib_382 < current_price:
                    candidates.append((
                        round(fib_382, 2), "38.2% Fib (deep retrace)", "fib_382",
                        round(fib_500, 2), round(fib_382, 2),
                    ))
        except Exception:
            pass

        # Filter: must be within 6% of current (was 8%) AND above the deep entry
        # AND at least 0.5% below current. Tighter band = more realistic entries.
        valid = [
            c for c in candidates
            if c[0] > entry
            and (current_price - c[0]) / current_price * 100 <= 6.0
            and c[0] < current_price * 0.995
        ]

        if valid:
            # Priority: closest realistic levels first.
            # 5-7 bar swing lows + 78.6% Fib are the "small pullback" entries.
            # Older swing lows + Fib 38-50% are deeper retracements.
            priority = {
                "swing_low_5": 1,        # tightest recent
                "fib_786": 2,            # light retrace
                "swing_low_7": 3,
                "shallow_fvg": 4,
                "fib_618": 5,            # golden pocket
                "swing_low_12": 6,
                "equilibrium": 7,
                "fib_500": 8,
                "fib_382": 9,
            }
            # Pick best by priority, breaking ties by HIGHEST price (closer to current)
            valid.sort(key=lambda x: (priority.get(x[2], 99), -x[0]))
            agg_px, agg_lbl, _src, agg_zlow, agg_zhigh = valid[0]
            aggressive_entry = agg_px
            aggressive_entry_zone_low = agg_zlow
            aggressive_entry_zone_high = agg_zhigh
            aggressive_entry_label = (
                f"Tier-1 aggressive: ৳{agg_zlow}-{agg_zhigh} ({agg_lbl})"
            )
            aggressive_entry_distance_pct = round((current_price - agg_px) / current_price * 100, 1)

    # === Entry status — distinguish "buy now" vs "wait for pullback" vs "too far" ===
    # Per SMC rule (Image 2): in trending up, buy in DISCOUNT zone, not premium.
    # Per SMC rule (Image 1 #9.5): don't justify emotional buys.
    entry_status = None
    entry_distance_pct = None
    chase_warning = None
    # "In zone" = price is currently INSIDE either the Tier-1 or Tier-2 zone.
    # The user defines this as: above zone_high → don't enter (chase territory).
    in_any_zone = False
    if entry_zone_low and entry_zone_high and entry_zone_low <= current_price <= entry_zone_high:
        in_any_zone = True
    if (aggressive_entry_zone_low and aggressive_entry_zone_high and
        aggressive_entry_zone_low <= current_price <= aggressive_entry_zone_high):
        in_any_zone = True

    if entry and current_price:
        entry_distance_pct = round((current_price - entry) / current_price * 100, 1)
        # AT_ENTRY = price is INSIDE one of the zones (broader than ±2% — uses real zone bounds)
        if in_any_zone:
            entry_status = "AT_ENTRY"
        elif entry_distance_pct <= 2.0 and entry_distance_pct >= -2.0:
            entry_status = "AT_ENTRY"
        elif entry_distance_pct > 2.0 and entry_distance_pct <= 8.0:
            entry_status = "WAIT_PULLBACK"
            if aggressive_entry:
                chase_warning = (
                    f"⚠ Don't chase at ৳{current_price}. Use limits: "
                    f"Tier-1 ৳{aggressive_entry} ({aggressive_entry_distance_pct:.1f}% below); "
                    f"Tier-2 ৳{entry} ({entry_distance_pct:.1f}% below, confluence)."
                )
            else:
                chase_warning = (
                    f"⚠ Don't chase at ৳{current_price} — entry is ৳{entry} "
                    f"({entry_distance_pct:.1f}% below). Place a buy limit; wait for pullback."
                )
        elif entry_distance_pct > 8.0:
            entry_status = "TOO_FAR"
            if aggressive_entry:
                chase_warning = (
                    f"🚫 DO NOT BUY at ৳{current_price}. Two valid limit orders: "
                    f"Tier-1 (aggressive) ৳{aggressive_entry} = {aggressive_entry_distance_pct:.1f}% below "
                    f"({aggressive_entry_label.split(': ')[-1] if aggressive_entry_label else 'closer support'}); "
                    f"Tier-2 (patient, high-edge) ৳{entry} = {entry_distance_pct:.1f}% below (confluence). "
                    f"Buying at ৳{current_price} = chasing premium."
                )
            else:
                chase_warning = (
                    f"🚫 DO NOT BUY at ৳{current_price} — high-edge entry is ৳{entry} "
                    f"({entry_distance_pct:.1f}% below). Wait for pullback or skip this setup. "
                    f"Buying here = chasing premium (smart money is selling)."
                )
        elif entry_distance_pct < -2.0:
            # Price already below entry — entry was the planned limit, now it's a discount
            entry_status = "DISCOUNT_TRIGGERED"

        # Update action label based on status — SMC clarity rule
        if entry_status == "TOO_FAR" and action.startswith("BUY"):
            action = action.replace("BUY ON DIP", "WAIT — ENTRY FAR BELOW").replace("BUY", "WAIT — ENTRY FAR BELOW")
            action_color = "orange"
        elif entry_status == "WAIT_PULLBACK" and action.startswith("BUY"):
            action_color = "yellow"  # downgrade green → yellow to signal "patience"
        elif entry_status == "AT_ENTRY" and action.startswith("BUY"):
            action_color = "green"
        elif entry_status == "DISCOUNT_TRIGGERED" and action.startswith("BUY"):
            action_color = "green"

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
        # Use the LATEST structural break levels from structure_events —
        # these are the actual BOS triggers, always within 5-10% of price.
        # Falling back to a tight 10-bar range if no recent structure.
        latest_bull_break = None
        latest_bear_break = None
        for ev in reversed(structure_events):
            if ev["type"].startswith("bullish") and latest_bull_break is None:
                latest_bull_break = ev["price"]
            elif ev["type"].startswith("bearish") and latest_bear_break is None:
                latest_bear_break = ev["price"]
            if latest_bull_break and latest_bear_break:
                break

        # Cap any trigger to within 8% of current price — wider isn't actionable
        def _cap(level, is_above):
            if level is None:
                return None
            pct = (level - current_price) / current_price * 100
            if is_above and pct > 8:
                return round(current_price * 1.04, 2)  # cap 4% above
            if not is_above and pct < -8:
                return round(current_price * 0.96, 2)  # cap 4% below
            return level

        up_level = _cap(latest_bull_break, True)
        dn_level = _cap(latest_bear_break, False)

        if up_level and dn_level:
            up_dist = (up_level - current_price) / current_price * 100
            dn_dist = (current_price - dn_level) / current_price * 100
            triggers.append({
                "icon": "⏸",
                "text": f"Wait for daily close above ৳{up_level} (+{up_dist:.1f}%) "
                        f"OR below ৳{dn_level} (−{dn_dist:.1f}%) — latest break levels",
            })
        else:
            recent_window = min(10, len(df))
            recent_high = round(float(df["high"].iloc[-recent_window:].max()), 2)
            recent_low = round(float(df["low"].iloc[-recent_window:].min()), 2)
            triggers.append({
                "icon": "⏸",
                "text": f"Wait for daily close above ৳{recent_high} OR below ৳{recent_low}",
            })
    else:
        triggers.append({
            "icon": "⏸",
            "text": "Watch for first ChoCh / BOS event to develop bias",
        })

    # ─── Hedge-fund triangulation narratives ───
    # Plain-English paragraphs synthesising what THIS stock is showing in each
    # of the three pillars: STRUCTURE (where) + ORDER FLOW (who) + VOLUME (strength)

    def _safe(v, default=0):
        try: return float(v) if v is not None else default
        except: return default

    # ── STRUCTURE narrative ──
    structure_lines = []
    if confluence_zone:
        structure_lines.append(
            f"Fresh CONFLUENCE setup: FVG ৳{confluence_zone['bottom']}-{confluence_zone['top']} "
            f"sits at a {confluence_zone['support_touches']}-touch support "
            f"@ ৳{confluence_zone.get('support_price', confluence_zone['bottom'])}."
        )
    elif fresh_bull_fvgs_below and bias == "BULLISH":
        f = fresh_bull_fvgs_below[0]
        structure_lines.append(
            f"Fresh bullish FVG ৳{f['bottom']}-{f['top']} below price ({((current_price - f['top']) / current_price * 100):.1f}% away)."
        )
    if structure_events:
        latest = structure_events[-1]
        structure_lines.append(
            f"Latest structure: {latest['type'].replace('_', ' ').replace('bullish', '↑').replace('bearish', '↓')} "
            f"@ ৳{latest['price']}."
        )
    if premium_discount:
        zone = premium_discount.get("current_zone", "")
        pct = premium_discount.get("current_pct", 0)
        structure_lines.append(
            f"Price at {pct}% of recent range = {zone.replace('_', ' ')} zone."
        )
    if bos_zones and bos_zones.get("bullish_trigger"):
        bt = bos_zones["bullish_trigger"]
        up_dist = (bt["price"] - current_price) / current_price * 100 if current_price else 0
        structure_lines.append(
            f"BOS↑ trigger at ৳{bt['price']} (+{up_dist:.1f}%) — break confirms continuation."
        )
    if bos_zones and bos_zones.get("bearish_trigger"):
        br = bos_zones["bearish_trigger"]
        dn_dist = (current_price - br["price"]) / current_price * 100 if current_price else 0
        structure_lines.append(
            f"BOS↓ trigger at ৳{br['price']} (-{dn_dist:.1f}%) — break invalidates bull case."
        )
    if bias == "WHIPSAW":
        structure_lines.append("⚠ Whipsaw — multiple alternating reversals, no clean institutional setup.")

    # HTF (weekly) bias context — Annaly Trader Step 1 / #9.4
    if htf_bias and htf_bias.get("narrative"):
        htf_b = htf_bias.get("bias")
        prefix = ""
        if htf_b == "BULLISH" and bias == "BULLISH":
            prefix = "✅ HTF aligned: "
        elif htf_b == "BEARISH" and bias == "BULLISH":
            prefix = "⚠ HTF conflict (weekly bearish, daily bullish — high-risk counter-trend): "
        elif htf_b == "BULLISH" and bias == "BEARISH":
            prefix = "⚠ HTF conflict (weekly bullish, daily bearish — likely just a pullback): "
        elif htf_b == "RANGE":
            prefix = "⚠ HTF ranging: "
        else:
            prefix = "HTF: "
        structure_lines.append(prefix + htf_bias["narrative"])

    # Liquidity sweep vs real breakout — Annaly Trader #9.3
    if liquidity_sweeps and liquidity_sweeps.get("latest"):
        latest_evt = liquidity_sweeps["latest"]
        et = latest_evt.get("type", "")
        if "sweep" in et:
            structure_lines.append(
                f"⚠ Recent {latest_evt.get('date','')}: {latest_evt.get('interpretation','')}"
            )
        elif "real_breakout" in et or "real_breakdown" in et:
            structure_lines.append(
                f"✅ Recent {latest_evt.get('date','')}: {latest_evt.get('interpretation','')}"
            )

    structure_narrative = " ".join(structure_lines) if structure_lines else \
        "No clear institutional structure yet. Wait for first BOS or ChoCh."

    # Verdict
    if confluence_zone and bias == "BULLISH":
        structure_verdict = "BUY"
    elif bias == "BULLISH" and fresh_bull_fvgs_below and not in_extreme_premium:
        structure_verdict = "BUY"
    elif bias in ("BEARISH", "WHIPSAW"):
        structure_verdict = "AVOID"
    elif in_extreme_premium:
        structure_verdict = "WAIT (extended)"
    else:
        structure_verdict = "WAIT"

    # ORDER FLOW + VOLUME narratives are computed AFTER analysis returns,
    # in the smc_chart wrapper where order_flow and advanced indicators
    # are in scope. Placeholders here.
    order_flow_narrative = ""
    of_verdict = "PENDING"
    volume_narrative = ""
    volume_verdict = "PENDING"
    hf_verdict = ""

    # ─── Cross-Signal Alignment narrative — how each metric agrees/disagrees ───
    alignment_lines = []

    def _add_align(label: str, vote: str, detail: str):
        emoji = "🟢" if vote == "BUY" else "🔴" if vote == "SELL" else "⚪"
        alignment_lines.append(f"{emoji} **{label}** — {vote}: {detail}")

    # SMC structure
    if bias == "BULLISH":
        if confluence_zone:
            _add_align("SMC Structure", "BUY",
                       f"Bullish trend + CONFLUENCE zone ৳{confluence_zone['bottom']}-{confluence_zone['top']} "
                       f"(FVG meets {confluence_zone['support_touches']}-touch support)")
        elif fresh_bull_fvgs_below:
            top_f = fresh_bull_fvgs_below[0]
            _add_align("SMC Structure", "BUY",
                       f"Bullish bias intact, fresh FVG ৳{top_f['bottom']}-{top_f['top']} below")
        else:
            _add_align("SMC Structure", "WAIT",
                       "Bullish bias but no clear demand zone below")
    elif bias == "BEARISH":
        _add_align("SMC Structure", "AVOID", "Trend flipped bearish — falling-knife risk")
    elif bias == "WHIPSAW":
        _add_align("SMC Structure", "WAIT", "Multiple alternating reversals = no edge")
    else:
        _add_align("SMC Structure", "WAIT", "No clear bias yet")

    # HTF (weekly) bias — multi-timeframe alignment
    if htf_bias:
        htf_b = htf_bias.get("bias")
        if htf_b == "BULLISH" and bias == "BULLISH":
            _add_align("HTF (Weekly)", "BUY",
                       f"Weekly aligned bullish ({htf_bias.get('trend_pct',0):+.0f}% over {htf_bias.get('weeks_analysed',0)}w) — daily entries supported")
        elif htf_b == "BEARISH" and bias == "BULLISH":
            _add_align("HTF (Weekly)", "AVOID",
                       f"Weekly bearish ({htf_bias.get('trend_pct',0):+.0f}% over {htf_bias.get('weeks_analysed',0)}w) — daily BUY is counter-trend, high risk")
        elif htf_b == "BEARISH":
            _add_align("HTF (Weekly)", "AVOID",
                       f"Weekly bearish ({htf_bias.get('trend_pct',0):+.0f}% over {htf_bias.get('weeks_analysed',0)}w)")
        elif htf_b == "RANGE":
            _add_align("HTF (Weekly)", "WAIT",
                       f"Weekly ranging ({htf_bias.get('trend_pct',0):+.0f}%) — wait for HTF clarity")

    # Liquidity sweep vs real breakout
    if liquidity_sweeps and liquidity_sweeps.get("latest"):
        latest_evt = liquidity_sweeps["latest"]
        et = latest_evt.get("type", "")
        if et == "bull_sweep":
            _add_align("Liquidity Sweep", "AVOID",
                       f"Last event: high swept then closed inside = stops hunted, NOT breakout")
        elif et == "bear_sweep":
            _add_align("Liquidity Sweep", "BUY",
                       f"Last event: low swept then closed back inside = often a bottom (institutions absorbed)")
        elif et == "real_breakout_up":
            _add_align("Liquidity Sweep", "BUY",
                       f"Last event: real breakout with displacement candle (not a sweep)")
        elif et == "real_breakdown":
            _add_align("Liquidity Sweep", "AVOID",
                       f"Last event: real breakdown with displacement (genuine bearish)")

    # Premium / Discount
    if premium_discount:
        zone = premium_discount.get("current_zone", "")
        pct = premium_discount.get("current_pct", 0)
        if "discount" in zone:
            _add_align("Premium/Discount", "BUY",
                       f"Price at {pct}% of range — DISCOUNT zone, smart money buys here")
        elif "premium" in zone:
            _add_align("Premium/Discount", "AVOID/SELL",
                       f"Price at {pct}% — PREMIUM zone, smart money sells here")
        else:
            _add_align("Premium/Discount", "WAIT",
                       f"At equilibrium ({pct}%) — wait for premium or discount")

    # Fibonacci dealing range
    if fib_dealing_range and fib_dealing_range.get("valid"):
        ft = fib_dealing_range["action_text"]
        zone = fib_dealing_range["current_zone"]
        pct = fib_dealing_range["current_pct"]
        vote = "BUY" if "BUY" in ft else "SELL" if "SELL" in ft else "WAIT"
        _add_align("Fibonacci",vote,
                   f"{pct:.0f}% retracement of last impulse leg "
                   f"(৳{fib_dealing_range['swing_low']} → ৳{fib_dealing_range['swing_high']}) "
                   f"= {zone.replace('_', ' ')}")

    # Trendiness
    if adx_value is not None:
        if adx_value >= 25:
            _add_align("Trend Strength", "BUY",
                       f"ADX {adx_value:.0f} = trend confirmed (FVG signals reliable)")
        else:
            _add_align("Trend Strength", "WAIT",
                       f"ADX {adx_value:.0f} < 25 = no trend (FVG signals unreliable)")

    # Order flow absorption (if available in this scope — checked via volume_delta)
    if recent_mitigated_bull >= 2:
        _add_align("Demand", "AVOID",
                   f"{recent_mitigated_bull} bullish FVGs already mitigated = demand absorbed")

    if overhead_bear_fvg:
        _add_align("Supply", "AVOID",
                   f"Fresh bearish FVG ৳{overhead_bear_fvg['bottom']}-{overhead_bear_fvg['top']} overhead")

    # ─── Plain-language Trade Thesis (per-stock specific) ───
    thesis_lines = []
    # Section 1: What's happening NOW
    if bias == "BULLISH":
        if confluence_zone:
            thesis_lines.append(
                f"Trend is bullish and we have a high-edge **CONFLUENCE setup**: "
                f"a fresh FVG at ৳{confluence_zone['bottom']}-{confluence_zone['top']} "
                f"sitting on a {confluence_zone['support_touches']}-touch support. "
                f"This is the highest-probability entry pattern (53% historical win rate)."
            )
        elif in_extreme_premium:
            thesis_lines.append(
                f"Trend is bullish but price sits at {round(range_pct or 0)}% of its "
                f"recent range — that's **extreme premium**, where smart money sells. "
                f"Don't chase from here."
            )
        elif overhead_bear_fvg:
            thesis_lines.append(
                f"Bullish bias but a fresh bearish FVG ৳{overhead_bear_fvg['bottom']}-"
                f"{overhead_bear_fvg['top']} sits right above price — institutional supply. "
                f"Wait for that to mitigate or for a deeper pullback."
            )
        elif fresh_bull_fvgs_below:
            top_f = fresh_bull_fvgs_below[0]
            thesis_lines.append(
                f"Bullish bias intact. Best entry on pullback to FVG "
                f"৳{top_f['bottom']}-{top_f['top']} where institutional buyers stepped in."
            )
        else:
            thesis_lines.append(
                "Bullish bias but no clear demand zone below — wait for a structural pullback."
            )
    elif bias == "BEARISH":
        thesis_lines.append(
            "Trend has flipped bearish — recent ChoCh + lower lows. "
            "Don't catch falling knives. Wait for either bullish reversal structure "
            "or move to confirmed support before considering a buy."
        )
    elif bias == "WHIPSAW":
        thesis_lines.append(
            "Multiple alternating reversals in recent structure = no clean trend, "
            "no statistical edge in either direction. The trigger levels in the "
            "card below tell you which side to take ONLY after a confirmed daily close."
        )
    else:
        thesis_lines.append(
            "No clear directional bias yet. Wait for the first BOS or ChoCh event."
        )

    # Section 2: T+2 fitness
    if bias == "BULLISH" and confluence_zone and is_trendy_market and not in_extreme_premium:
        thesis_lines.append(
            "**T+2 friendly**: high-edge setup, trending market, near demand zone. "
            "Move typically resolves in 1-3 trading days."
        )
    elif bias == "BULLISH" and is_trendy_market and not in_extreme_premium:
        thesis_lines.append(
            "**T+2 acceptable**: trend strong (ADX≥25), reasonable position. "
            "Consider entry only on green confirmation candle, not on first wick into FVG."
        )
    elif bias == "WHIPSAW":
        thesis_lines.append(
            "**Not T+2 friendly**: whipsaw means high probability of reversal "
            "within 1-3 days = settlement risk. Wait for break of trigger first."
        )
    elif in_extreme_premium:
        thesis_lines.append(
            "**Not T+2 friendly**: extreme premium = high probability of pullback "
            "within your settlement window. Wait for retest of equilibrium "
            f"(~৳{premium_discount.get('equilibrium') if premium_discount else 'EQ'})."
        )

    # Section 3: Trade plan if we have one
    if entry and stop_loss and target1:
        risk_pct = (entry - stop_loss) / entry * 100
        reward_pct = (target1 - entry) / entry * 100
        thesis_lines.append(
            f"**Trade plan**: Entry ৳{entry} · Stop ৳{stop_loss} (-{risk_pct:.1f}%) · "
            f"T1 ৳{target1} (+{reward_pct:.1f}%) · R/R 1:{rr if rr else '?'}. "
            + ("Risk is tight." if risk_pct <= 4 else
               "⚠ Risk is wider than ideal — size down." if risk_pct <= 6 else
               "⚠ Risk too wide for this entry — wait for closer stop level.")
        )

        # ── Explicit "Don't buy if..." rules ──
        # The chase-prevention zone: how far above current entry is too far?
        max_chase = entry * 1.025  # 2.5% above the ideal entry
        invalidation = entry * 0.97  # 3% below entry = structure broken
        no_buy_lines = []
        if current_price > max_chase:
            no_buy_lines.append(
                f"❌ **DON'T buy at current price ৳{current_price}** — too far above the "
                f"ideal entry zone ৳{entry}. Chasing here = bad R/R."
            )
        no_buy_lines.append(
            f"❌ **DON'T buy above ৳{round(max_chase, 2)}** — past this level you're "
            f"chasing. Wait for pullback to ৳{entry} or skip this trade."
        )
        no_buy_lines.append(
            f"❌ **DON'T buy if daily close below ৳{round(invalidation, 2)}** — that "
            f"breaks the structural support, signal is invalidated."
        )
        if in_extreme_premium:
            no_buy_lines.append(
                f"❌ **DON'T buy at extreme premium ({round(range_pct or 0)}% of range)** — "
                f"smart money sells here. Wait for price to come back to discount zone."
            )
        if in_premium and not in_extreme_premium:
            no_buy_lines.append(
                f"⚠ **CAUTION at premium** ({round(range_pct or 0)}% of range) — high-edge "
                f"entries are in DISCOUNT zone (below {round((swing_low + (swing_high - swing_low)*0.382), 2) if swing_high else 'EQ'}). "
                f"Buying here means buying retail-side liquidity."
            )
        if entry_status == "TOO_FAR":
            if aggressive_entry:
                no_buy_lines.append(
                    f"❌ **DON'T buy at ৳{current_price}.** Two limit orders are valid: "
                    f"**Tier-1** ৳{aggressive_entry} ({aggressive_entry_distance_pct:.1f}% below — "
                    f"closer support, lower edge) OR **Tier-2** ৳{entry} ({entry_distance_pct:.1f}% "
                    f"below — confluence, high edge). Buying at ৳{current_price} = chasing premium."
                )
            else:
                no_buy_lines.append(
                    f"❌ **DON'T buy at ৳{current_price} — entry is {entry_distance_pct:.1f}% below.** "
                    f"This is the #1 SMC mistake (Annaly Trader 9.5): using SMC labels to justify "
                    f"emotional buys. Wait for pullback to ৳{entry} or skip."
                )
        elif entry_status == "WAIT_PULLBACK":
            if aggressive_entry:
                no_buy_lines.append(
                    f"⚠ **Use limits, not market.** Tier-1 ৳{aggressive_entry} "
                    f"({aggressive_entry_distance_pct:.1f}% below); Tier-2 ৳{entry} "
                    f"({entry_distance_pct:.1f}% below, confluence). Don't market-buy at ৳{current_price}."
                )
            else:
                no_buy_lines.append(
                    f"⚠ **Place buy LIMIT at ৳{entry}, don't market-buy at ৳{current_price}** "
                    f"(entry is {entry_distance_pct:.1f}% below). Patience = better R/R."
                )
        if overhead_bear_fvg is not None:
            no_buy_lines.append(
                f"❌ **DON'T buy with overhead supply** — fresh bearish FVG ৳{overhead_bear_fvg['bottom']}-"
                f"{overhead_bear_fvg['top']} above caps any rally. Wait for it to mitigate first."
            )
        thesis_lines.append("**No-buy zones:** " + " ".join(no_buy_lines))

    return {
        "bias": bias,
        "confidence": confidence,
        "action": action,
        "action_color": action_color,
        "summary": summary,
        "reasons": reasons,
        "entry": entry,
        "entry_label": entry_label,
        "entry_zone_low": entry_zone_low,
        "entry_zone_high": entry_zone_high,
        "entry_status": entry_status,
        "entry_distance_pct": entry_distance_pct,
        "chase_warning": chase_warning,
        "aggressive_entry": aggressive_entry,
        "aggressive_entry_label": aggressive_entry_label,
        "aggressive_entry_distance_pct": aggressive_entry_distance_pct,
        "aggressive_entry_zone_low": aggressive_entry_zone_low,
        "aggressive_entry_zone_high": aggressive_entry_zone_high,
        "stop_loss": stop_loss,
        "target1": target1,
        "target2": target2,
        "risk_reward": rr,
        "triggers": triggers,
        "thesis": thesis_lines,
        "alignment": alignment_lines,
        "structure_narrative": structure_narrative,
        "structure_verdict": structure_verdict,
        "order_flow_narrative": order_flow_narrative,
        "order_flow_verdict": of_verdict,
        "volume_narrative": volume_narrative,
        "volume_verdict": volume_verdict,
        "hedge_fund_verdict": hf_verdict,
        "adx": float(adx_value) if adx_value is not None else None,
        "is_trendy": bool(is_trendy_market),
        "confluence": confluence_zone,
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
            # SMC zone-tier: classify FVG by where its midpoint sits in the
            # recent 60-bar dealing range. The "first" FVG nearest to price is
            # often manipulation/induced liquidity — the *real* institutional
            # FVG tends to sit near the equilibrium of the swing range.
            mid = (f["top"] + f["bottom"]) / 2
            recent_window = min(60, len(df))
            rh = float(df["high"].iloc[-recent_window:].max())
            rl = float(df["low"].iloc[-recent_window:].min())
            range_pos = None
            tier = "secondary"
            if rh > rl:
                range_pos = (mid - rl) / (rh - rl) * 100
                # Core = near equilibrium (35–65% of range) = highest-edge zone
                if 35 <= range_pos <= 65:
                    tier = "core"
                # Deep discount = bullish FVG far below 50% (great BUY zone)
                elif range_pos < 30 and f["type"] == "bullish":
                    tier = "discount"
                # Deep premium = bearish FVG far above 50% (great SHORT zone)
                elif range_pos > 70 and f["type"] == "bearish":
                    tier = "premium"
                # Anything else (e.g. bullish FVG in extreme premium) = often fake
                else:
                    tier = "outside"

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
                "tier": tier,
                "range_pos": round(range_pos, 1) if range_pos is not None else None,
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

    # Add MA values aligned to candle times — vectorized via numpy
    ma_lines = {}
    date_strs = [
        d.strftime("%Y-%m-%d") if hasattr(d, "strftime") else str(d)
        for d in df["date"].values
    ]
    for key, vals in mas.items():
        ma_lines[key] = [
            {"time": date_strs[i], "value": vals[i]}
            for i in range(len(vals))
            if vals[i] is not None
        ]

    # Compute extended detectors BEFORE generate_analysis so they can be passed in
    premium_discount = detect_premium_discount(swings, df, lookback=60)
    bos_zones = detect_bos_zones(swings, structure_events, df)
    fib_dealing_range = detect_fib_dealing_range(swings, df, lookback=60)
    elliott_triangle = detect_elliott_triangle(swings, df, lookback=80)
    demand_supply = detect_demand_supply_zones(df, lookback=60)
    volatility_imbalances = detect_volatility_imbalance(df, lookback=40)
    htf_bias = detect_htf_bias(df, weeks_lookback=12)
    liquidity_sweeps = detect_liquidity_sweep(df, swings, lookback=20)
    # Multi-touch support/resistance — used to pick a TESTED, NEARBY Tier-2
    # entry zone instead of the deepest confluence FVG (which may never fill).
    sr_for_entry = detect_support_resistance(swings, df, float(c.iloc[-1]))

    analysis = generate_analysis(
        structure_events,
        fvg_zones,
        order_blocks,
        key_levels,
        round(float(c.iloc[-1]), 2),
        df,
        premium_discount=premium_discount,
        bos_zones=bos_zones,
        fib_dealing_range=fib_dealing_range,
        htf_bias=htf_bias,
        liquidity_sweeps=liquidity_sweeps,
        support_resistance=sr_for_entry,
    )

    # ─── ACTUAL TECHNICAL TRIGGER DATE ───────────────────────────────────
    # Walk back through candle history to find when price LAST entered each
    # entry zone (low ≤ zone_high). This is the REAL trigger date — not when
    # our scheduler first ran. Plus compute would-be max profit since trigger.
    def _scan_zone_trigger(zone_low, zone_high, df_local, lookback=120):
        """Return dict with last_hit_date, days_since_hit, max_high_since_hit,
        max_profit_pct_if_bought_at_zone_high, max_drawdown_pct."""
        if zone_low is None or zone_high is None or df_local is None or len(df_local) == 0:
            return None
        try:
            n = len(df_local)
            start = max(0, n - lookback)
            hit_idx = None
            # Walk forward from `start` to find FIRST hit, then keep updating to LATEST hit.
            # Actually we want the MOST RECENT hit (latest), so walk backward from end.
            for i in range(n - 1, start - 1, -1):
                lo = float(df_local["low"].iloc[i])
                if lo <= zone_high:
                    hit_idx = i
                    break
            if hit_idx is None:
                return None
            hit_date = df_local["date"].iloc[hit_idx]
            hit_str = hit_date.strftime("%Y-%m-%d") if hasattr(hit_date, "strftime") else str(hit_date)
            today_idx = n - 1
            bars_since = today_idx - hit_idx
            # Max high since hit (would-be profit)
            slice_after = df_local.iloc[hit_idx:n]
            max_high = float(slice_after["high"].max()) if len(slice_after) else float(df_local["close"].iloc[-1])
            min_low = float(slice_after["low"].min()) if len(slice_after) else float(df_local["close"].iloc[-1])
            # If bought at zone_high (worst entry case), what's max profit?
            mid = (zone_low + zone_high) / 2
            max_profit_pct = round((max_high - zone_high) / zone_high * 100, 1) if zone_high > 0 else 0
            best_profit_pct = round((max_high - mid) / mid * 100, 1) if mid > 0 else 0
            max_drawdown_pct = round((min_low - zone_high) / zone_high * 100, 1) if zone_high > 0 else 0
            return {
                "last_hit_date": hit_str,
                "bars_since_hit": int(bars_since),
                "max_high_since_hit": round(max_high, 2),
                "min_low_since_hit": round(min_low, 2),
                "max_profit_pct_worst_entry": max_profit_pct,  # bought at zone_high
                "max_profit_pct_mid_entry": best_profit_pct,   # bought at zone midpoint
                "max_drawdown_pct": max_drawdown_pct,
            }
        except Exception:
            return None

    if isinstance(analysis, dict):
        t1_zlow = analysis.get("aggressive_entry_zone_low")
        t1_zhigh = analysis.get("aggressive_entry_zone_high")
        t2_zlow = analysis.get("entry_zone_low")
        t2_zhigh = analysis.get("entry_zone_high")
        analysis["tier1_trigger"] = _scan_zone_trigger(t1_zlow, t1_zhigh, df)
        analysis["tier2_trigger"] = _scan_zone_trigger(t2_zlow, t2_zhigh, df)
        # Pick the most recent hit between Tier-1 and Tier-2 as the "primary trigger"
        t1 = analysis.get("tier1_trigger")
        t2 = analysis.get("tier2_trigger")
        primary = None
        if t1 and t2:
            primary = t1 if t1["bars_since_hit"] <= t2["bars_since_hit"] else t2
        else:
            primary = t1 or t2
        analysis["primary_trigger"] = primary

    # Advanced indicators — VSA, OBV, MFI, Ichimoku, Wyckoff Spring/SOS
    try:
        from api.advanced_indicators import compute_advanced_indicators
        advanced = compute_advanced_indicators(df, accumulation=accumulation)
    except Exception:
        advanced = {}

    # Order Flow stack — Volume Profile, VWAP, Volume Delta, Absorption, OB imbalance
    try:
        from api.order_flow import compute_full_order_flow
        from database import get_connection
        try:
            ob_conn = get_connection()
        except Exception:
            ob_conn = None
        order_flow = compute_full_order_flow(df, symbol=symbol.upper(), conn=ob_conn)
        if ob_conn is not None:
            try: ob_conn.close()
            except Exception: pass
        # Augment with REAL tick data if available (LankaBD tape scraper)
        try:
            from data.dse_tick_analytics import get_tick_order_flow
            tick_flow = get_tick_order_flow(symbol.upper())
            if tick_flow and order_flow:
                order_flow["tick_data"] = tick_flow
        except Exception:
            pass
    except Exception:
        order_flow = None

    # ── Per-metric stock-specific IMPACT narratives ──
    # Each metric gets an `impact` field interpreting what its value means
    # for THIS stock at its current price + whether to buy/wait/skip.
    cp_now = round(float(c.iloc[-1]), 2)
    in_premium = False
    if isinstance(analysis, dict):
        adx_val = analysis.get("adx") or 0
        in_premium = "premium" in (analysis.get("summary", "") or "").lower() or False

    # OBV impact
    obv_data = advanced.get("obv") if advanced else None
    if obv_data:
        div = obv_data.get("divergence")
        trend = obv_data.get("trend")
        if div == "bullish":
            obv_data["impact"] = (
                f"OBV is making higher lows while price made a lower low — institutions are "
                f"quietly accumulating while retail panics. Leading bullish reversal signal. "
                f"FOR THIS STOCK: combined with current state, treat as supporting evidence — "
                f"buy ONLY if price retraces to the FVG/demand zone, don't chase at ৳{cp_now}."
            )
        elif div == "bearish":
            obv_data["impact"] = (
                f"OBV is making lower highs while price made a higher high — buyers running out "
                f"of fuel. Leading bearish exhaustion signal. FOR THIS STOCK: do not buy fresh longs. "
                f"If you're already in a position, take partial profits."
            )
        elif trend == "rising":
            obv_data["impact"] = (
                f"OBV trending up — net buying pressure is real (not just price moving). "
                f"Confirms uptrend. FOR THIS STOCK: aligns with bullish bias; entries in demand zone are valid."
            )
        elif trend == "falling":
            obv_data["impact"] = (
                f"OBV falling — net selling pressure under the surface. Even if price holds, "
                f"the volume isn't supporting it. FOR THIS STOCK: be cautious; wait for OBV to "
                f"flip rising or for bullish divergence before fresh buys."
            )

    # MFI impact
    mfi_data = advanced.get("mfi") if advanced else None
    if mfi_data:
        v = mfi_data.get("current", 50)
        sig = mfi_data.get("signal")
        if sig == "oversold":
            mfi_data["impact"] = (
                f"MFI {v} is below 20 = oversold extreme. Volume-weighted RSI says stock is "
                f"deeply unloved here. FOR THIS STOCK at ৳{cp_now}: high-edge bounce zone — "
                f"COMBINED with structural support (FVG/demand zone) this is a classic buy setup."
            )
        elif sig == "overbought":
            mfi_data["impact"] = (
                f"MFI {v} is above 80 = overbought extreme. FOR THIS STOCK at ৳{cp_now}: "
                f"high probability of pullback within 2-5 days. DON'T buy fresh; if long, take "
                f"partial profits or trail stop tighter."
            )
        else:
            mfi_data["impact"] = (
                f"MFI {v} = neutral zone. No oversold-bounce or overbought-pullback signal. "
                f"FOR THIS STOCK: not contributing edge either way; rely on structure + order flow."
            )

    # Ichimoku impact
    ich_data = advanced.get("ichimoku") if advanced else None
    if ich_data:
        sig = ich_data.get("signal", "")
        tk = ich_data.get("tk_cross")
        if "above_cloud_bullish" in sig:
            tail = ""
            if tk == "bullish":
                tail = f" Plus a fresh TK bullish cross — that's a NEW momentum signal."
            elif tk == "bearish":
                tail = f" BUT a fresh TK bearish cross warns momentum is rolling — be cautious."
            ich_data["impact"] = (
                f"Price is above the cloud — full Ichimoku trend system is bullish (5 of 5 components agree).{tail} "
                f"FOR THIS STOCK at ৳{cp_now}: trend-following entries valid, cloud below acts as dynamic support."
            )
        elif "below_cloud_bearish" in sig:
            ich_data["impact"] = (
                f"Price is below the cloud — full Ichimoku trend system is bearish. "
                f"FOR THIS STOCK at ৳{cp_now}: don't buy fresh; cloud above is dynamic resistance. "
                f"Wait for price to reclaim cloud before considering longs."
            )
        else:
            ich_data["impact"] = (
                f"Price is inside the cloud = consolidation/no-trend phase. "
                f"FOR THIS STOCK: Ichimoku gives no edge here. Use Volume Profile + VSA instead."
            )

    # Absorption impact (in order_flow, not advanced)
    if order_flow and isinstance(order_flow, dict):
        absorption = order_flow.get("absorption")
        if absorption:
            if absorption.get("absorbed"):
                absorption["impact"] = (
                    f"🟢 Today's bar shows institutional buyer absorption — high volume "
                    f"({absorption['vol_ratio']}× avg), long lower wick "
                    f"(rejection {int(absorption['lower_wick_ratio']*100)}% of range), "
                    f"closed near high (strength {int(absorption['close_strength']*100)}%). "
                    f"FOR THIS STOCK: 1-3 day continuation rally is statistically likely. "
                    f"Buy at close (৳{cp_now}) with stop below today's low."
                )
            else:
                strength_pct = int(absorption.get('strength', 0) * 100)
                absorption["impact"] = (
                    f"No buyer absorption today (composite strength {strength_pct}%). "
                    f"FOR THIS STOCK: no institutional commitment YET. Wait for absorption "
                    f"day before entering — that's the typical pre-rally signal."
                )

    # ── Volume narrative (now that VSA / OBV / MFI / Ichimoku are computed) ──
    vol_lines = []
    vsa_events_list = advanced.get("vsa_events", []) if advanced else []
    if vsa_events_list:
        recent_vsa = vsa_events_list[-3:]
        for ev in reversed(recent_vsa):
            vol_lines.append(
                f"VSA {ev['type'].replace('_', ' ').lower()} on {ev['time']} ({ev['bias']})."
            )
    obv_data = advanced.get("obv") if advanced else None
    if obv_data:
        if obv_data.get("divergence") == "bullish":
            vol_lines.append("⚡ OBV bullish divergence — exhaustion ending, reversal coming.")
        elif obv_data.get("divergence") == "bearish":
            vol_lines.append("⚠ OBV bearish divergence — momentum dying, top forming.")
        elif obv_data.get("trend") == "rising":
            vol_lines.append("OBV trend rising — net buying confirmed.")
        elif obv_data.get("trend") == "falling":
            vol_lines.append("OBV trend falling — net selling.")
    mfi_data = advanced.get("mfi") if advanced else None
    if mfi_data:
        if mfi_data.get("signal") == "oversold":
            vol_lines.append(f"MFI {mfi_data['current']} oversold — high-edge bounce zone.")
        elif mfi_data.get("signal") == "overbought":
            vol_lines.append(f"MFI {mfi_data['current']} overbought — pullback risk.")
        else:
            vol_lines.append(f"MFI {mfi_data['current']} neutral.")
    ich_data = advanced.get("ichimoku") if advanced else None
    if ich_data:
        sig = ich_data.get("signal", "").replace("_", " ")
        if "above_cloud_bullish" in (ich_data.get("signal") or ""):
            vol_lines.append(f"Ichimoku above cloud — full trend system bullish.")
        elif "below_cloud_bearish" in (ich_data.get("signal") or ""):
            vol_lines.append(f"Ichimoku below cloud — full trend system bearish.")
        else:
            vol_lines.append(f"Ichimoku {sig}.")
        if ich_data.get("tk_cross") == "bullish":
            vol_lines.append("TK cross bullish (Tenkan above Kijun) — fresh momentum signal.")
        elif ich_data.get("tk_cross") == "bearish":
            vol_lines.append("TK cross bearish — momentum flip down.")
    volume_narrative = " ".join(vol_lines) if vol_lines else "Volume data still building (need 60+ bars)."

    bull_count = sum(1 for ev in vsa_events_list[-5:] if ev.get("bias") == "bullish")
    bear_count = sum(1 for ev in vsa_events_list[-5:] if ev.get("bias") == "bearish")
    obv_bull = obv_data and (obv_data.get("trend") == "rising" or obv_data.get("divergence") == "bullish")
    ich_bull = ich_data and "above_cloud_bullish" in (ich_data.get("signal") or "")
    bull_signals = sum([bull_count > bear_count, bool(obv_bull), bool(ich_bull),
                        bool(mfi_data and mfi_data.get("signal") == "oversold")])
    bear_signals = sum([bear_count > bull_count,
                        bool(obv_data and obv_data.get("trend") == "falling"),
                        bool(ich_data and "below_cloud_bearish" in (ich_data.get("signal") or "")),
                        bool(mfi_data and mfi_data.get("signal") == "overbought")])
    if bull_signals >= 3:
        volume_verdict = "BUY"
    elif bear_signals >= 3:
        volume_verdict = "AVOID"
    elif bull_signals > bear_signals:
        volume_verdict = "LEANS BUY"
    elif bear_signals > bull_signals:
        volume_verdict = "LEANS AVOID"
    else:
        volume_verdict = "MIXED"

    # ── ORDER FLOW narrative + verdict (data only available HERE) ──
    of_lines = []
    of = order_flow or {}
    abs_data = of.get("absorption")
    if abs_data and abs_data.get("absorbed"):
        of_lines.append(
            f"🟢 Absorption fired today (vol {abs_data['vol_ratio']}× avg, lower wick "
            f"{abs_data['lower_wick_ratio']}, close strength {abs_data['close_strength']}) "
            f"— institutions stepped in."
        )
    elif abs_data:
        of_lines.append(f"No absorption today (strength {int(abs_data.get('strength', 0) * 100)}%).")
    obi = of.get("orderbook_imbalance")
    if obi and obi.get("imbalance_pct") is not None:
        sign = "+" if obi["imbalance_pct"] > 0 else ""
        of_lines.append(
            f"Bid/Ask Imbalance {sign}{obi['imbalance_pct']}% — {obi.get('verdict', '').lower()}."
        )
    vd = of.get("volume_delta")
    if vd and vd.get("delta_5d") is not None:
        d5 = vd["delta_5d"]
        of_lines.append(
            f"5-day Δ {'+' if d5 > 0 else ''}{d5:,} = net {'buying' if d5 > 0 else 'selling'}."
        )
    vw = of.get("vwap")
    cp_now = round(float(c.iloc[-1]), 2)
    if vw and vw.get("value"):
        if cp_now > vw.get("upper_2sd", 0):
            of_lines.append(f"Price +2σ above VWAP ৳{vw['value']} = overextended.")
        elif cp_now > vw.get("upper_1sd", 0):
            of_lines.append(f"Price above +1σ VWAP ৳{vw['value']} = strong intraday bias.")
        elif cp_now < vw.get("lower_2sd", 0):
            of_lines.append(f"Price -2σ below VWAP ৳{vw['value']} = oversold fade-long.")
        elif cp_now < vw.get("lower_1sd", 0):
            of_lines.append(f"Price below -1σ VWAP ৳{vw['value']} = weak intraday.")
        else:
            of_lines.append(f"Price near VWAP ৳{vw['value']} = at fair value.")
    vp = of.get("volume_profile")
    if vp and vp.get("poc"):
        if cp_now > vp["poc"]:
            of_lines.append(f"Price above POC ৳{vp['poc']} (fair value) — bulls in control.")
        else:
            of_lines.append(f"Price below POC ৳{vp['poc']} (fair value) — bears in control.")
    order_flow_narrative_real = " ".join(of_lines) if of_lines else "Order flow data unavailable."

    of_buy = sum([
        bool(abs_data and abs_data.get("absorbed")),
        bool(obi and obi.get("imbalance_pct", 0) > 5),
        bool(vd and vd.get("delta_5d", 0) > 0),
        bool(vp and vp.get("poc") and cp_now > vp["poc"]),
    ])
    of_sell = sum([
        bool(obi and obi.get("imbalance_pct", 0) < -5),
        bool(vd and vd.get("delta_5d", 0) < 0),
        bool(vp and vp.get("poc") and cp_now < vp["poc"]),
        bool(vw and cp_now > vw.get("upper_2sd", 1e9)),
    ])
    of_verdict_real = "BUY" if of_buy >= 3 else "AVOID" if of_sell >= 3 else "MIXED"

    # Refresh analysis dict with real narratives + verdicts
    if isinstance(analysis, dict):
        analysis["order_flow_narrative"] = order_flow_narrative_real
        analysis["order_flow_verdict"] = of_verdict_real
        analysis["volume_narrative"] = volume_narrative
        analysis["volume_verdict"] = volume_verdict
        s_v = analysis.get("structure_verdict")
        f_v = of_verdict_real
        v_v = volume_verdict
        buy_pillars = sum(1 for v in (s_v, f_v, v_v) if v == "BUY")
        if buy_pillars >= 2 and s_v != "AVOID":
            analysis["hedge_fund_verdict"] = "🟢 INSTITUTIONAL BUY — multiple pillars aligned"
        elif s_v == "AVOID" or f_v == "AVOID" or v_v == "AVOID":
            analysis["hedge_fund_verdict"] = "🔴 SKIP — at least one pillar rejecting"
        elif s_v == "BUY" and f_v == "MIXED":
            analysis["hedge_fund_verdict"] = "🟡 STRUCTURE OK — wait for FLOW confirmation"
        else:
            analysis["hedge_fund_verdict"] = "⚪ NO EDGE — wait for cleaner setup"

    # ── BISI / SIBI labels on FVGs (ICT terminology) ──
    # BISI = Buy-Side Imbalance Sell-Side Inefficiency = bullish FVG
    # SIBI = Sell-Side Imbalance Buy-Side Inefficiency = bearish FVG
    for f in fvg_zones:
        if f.get("type") == "bullish":
            f["ict_label"] = "BISI"
        elif f.get("type") == "bearish":
            f["ict_label"] = "SIBI"

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
        "fib_dealing_range": fib_dealing_range,
        "elliott_triangle": elliott_triangle,
        "demand_zones": demand_supply.get("demand", []),
        "supply_zones": demand_supply.get("supply", []),
        "volatility_imbalances": volatility_imbalances,
        "htf_bias": htf_bias,
        "liquidity_sweeps": liquidity_sweeps,
        "order_flow": order_flow,
        "vsa_events": advanced.get("vsa_events", []),
        "obv": advanced.get("obv"),
        "mfi": advanced.get("mfi"),
        "ichimoku": advanced.get("ichimoku"),
        "wyckoff_events": advanced.get("wyckoff_events", []),
        "current_price": round(float(c.iloc[-1]), 2),
    }
