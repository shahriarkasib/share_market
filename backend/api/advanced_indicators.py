"""High-edge advanced indicators — VSA, OBV, MFI, Ichimoku Cloud, Wyckoff
Spring/SOS. All compute from daily OHLCV; no tick data required.

Each function returns either a per-bar series for sub-panes or the latest
event(s) for the analysis card.
"""
from __future__ import annotations

from typing import Optional

import pandas as pd
import numpy as np


# ─────────────────────────────────────────────────────────────────────────
#  Volume Spread Analysis (VSA — Tom Williams)
# ─────────────────────────────────────────────────────────────────────────

def classify_vsa_bars(df: pd.DataFrame, lookback: int = 60) -> list[dict]:
    """Detect institutional footprints via VSA bar patterns.

    Patterns scanned (priority order):
      - SELLING_CLIMAX:  wide-spread down bar + ultra volume + close upper third
                         → exhaustion of sellers → BULLISH reversal
      - BUYING_CLIMAX:   wide-spread up bar + ultra volume + close lower third
                         → exhaustion of buyers → BEARISH reversal
      - STOPPING_VOLUME: ultra volume + narrow spread → institutions absorbing
      - SPRING:          wick below recent low + close back above → BULLISH
      - UPTHRUST:        wick above recent high + close back below → BEARISH
      - NO_DEMAND:       up bar + narrow spread + low volume → BEARISH
      - NO_SUPPLY:       down bar + narrow spread + low volume → BULLISH

    Returns list of {idx, time, type, bias, strength, description}.
    """
    if len(df) < 30 or "volume" not in df.columns:
        return []

    n = len(df)
    o = df["open"].astype(float).values
    h = df["high"].astype(float).values
    l = df["low"].astype(float).values
    c = df["close"].astype(float).values
    v = df["volume"].astype(float).values
    spread = h - l
    body = np.abs(c - o)
    rng_mean_20 = pd.Series(spread).rolling(20).mean().fillna(0).values
    vol_mean_20 = pd.Series(v).rolling(20).mean().fillna(1).values

    events = []
    start = max(20, n - lookback)
    for i in range(start, n):
        if rng_mean_20[i] <= 0 or vol_mean_20[i] <= 0:
            continue
        sp_ratio = spread[i] / rng_mean_20[i]      # 1 = avg, >1.5 wide
        vol_ratio = v[i] / vol_mean_20[i]           # 1 = avg, >2 ultra
        is_up = c[i] > o[i]
        is_down = c[i] < o[i]
        rng_safe = max(spread[i], 1e-9)
        close_pos = (c[i] - l[i]) / rng_safe       # 0 = closed at low, 1 = at high

        time_str = df.iloc[i]["date"].strftime("%Y-%m-%d") if "date" in df.columns else None

        # 1. Selling Climax — wide down bar + ultra vol + close upper third
        if is_down and sp_ratio >= 1.6 and vol_ratio >= 2.0 and close_pos >= 0.55:
            events.append({
                "idx": i, "time": time_str, "type": "SELLING_CLIMAX",
                "bias": "bullish", "strength": min(5, int(vol_ratio)),
                "description": f"Wide-down bar + {vol_ratio:.1f}× volume + closed upper {int(close_pos*100)}% — sellers exhausted, bullish reversal candidate",
                "high": float(h[i]), "low": float(l[i]),
            })
            continue

        # 2. Buying Climax — wide up bar + ultra vol + close lower third
        if is_up and sp_ratio >= 1.6 and vol_ratio >= 2.0 and close_pos <= 0.45:
            events.append({
                "idx": i, "time": time_str, "type": "BUYING_CLIMAX",
                "bias": "bearish", "strength": min(5, int(vol_ratio)),
                "description": f"Wide-up bar + {vol_ratio:.1f}× volume + closed lower {int(close_pos*100)}% — buyers exhausted, bearish reversal candidate",
                "high": float(h[i]), "low": float(l[i]),
            })
            continue

        # 3. Stopping Volume — ultra volume but narrow spread (absorption)
        if vol_ratio >= 1.8 and sp_ratio <= 0.7:
            bias = "bullish" if c[i] > c[i - 1] else "bearish"
            events.append({
                "idx": i, "time": time_str, "type": "STOPPING_VOLUME",
                "bias": bias, "strength": min(5, int(vol_ratio)),
                "description": f"Ultra volume {vol_ratio:.1f}× + narrow spread = institutions absorbing — {bias} bias",
                "high": float(h[i]), "low": float(l[i]),
            })
            continue

        # 4. Spring — wick below recent N-bar low, close back above
        recent_lo = float(l[max(0, i - 10):i].min()) if i >= 1 else l[i]
        if l[i] < recent_lo * 0.997 and c[i] > recent_lo and is_up:
            events.append({
                "idx": i, "time": time_str, "type": "SPRING",
                "bias": "bullish", "strength": 4,
                "description": f"Wicked below ৳{recent_lo:.2f} (10-bar low) and reclaimed — Wyckoff Spring, strong bullish reversal",
                "high": float(h[i]), "low": float(l[i]),
            })
            continue

        # 5. Upthrust — wick above recent high, close back below
        recent_hi = float(h[max(0, i - 10):i].max()) if i >= 1 else h[i]
        if h[i] > recent_hi * 1.003 and c[i] < recent_hi and is_down:
            events.append({
                "idx": i, "time": time_str, "type": "UPTHRUST",
                "bias": "bearish", "strength": 4,
                "description": f"Wicked above ৳{recent_hi:.2f} (10-bar high) and rejected — Upthrust, bearish reversal",
                "high": float(h[i]), "low": float(l[i]),
            })
            continue

        # 6. No Demand — up bar with narrow spread + low volume
        if is_up and sp_ratio <= 0.7 and vol_ratio <= 0.7:
            events.append({
                "idx": i, "time": time_str, "type": "NO_DEMAND",
                "bias": "bearish", "strength": 2,
                "description": f"Up bar but narrow spread + {vol_ratio:.1f}× volume — no buying conviction, bearish",
                "high": float(h[i]), "low": float(l[i]),
            })
            continue

        # 7. No Supply — down bar with narrow spread + low volume
        if is_down and sp_ratio <= 0.7 and vol_ratio <= 0.7:
            events.append({
                "idx": i, "time": time_str, "type": "NO_SUPPLY",
                "bias": "bullish", "strength": 2,
                "description": f"Down bar but narrow spread + {vol_ratio:.1f}× volume — no selling pressure, bullish base",
                "high": float(h[i]), "low": float(l[i]),
            })

    return events[-15:]  # cap to 15 most recent for display


# ─────────────────────────────────────────────────────────────────────────
#  On-Balance Volume (OBV) + divergence
# ─────────────────────────────────────────────────────────────────────────

def compute_obv(df: pd.DataFrame) -> Optional[dict]:
    """Cumulative volume flow. OBV rises when close > prev close, falls otherwise.
    Detect divergence vs price: bullish divergence = price LL but OBV HL.
    """
    if len(df) < 30 or "volume" not in df.columns:
        return None
    c = df["close"].astype(float).values
    v = df["volume"].astype(float).values
    obv = np.zeros(len(df))
    for i in range(1, len(df)):
        if c[i] > c[i - 1]:
            obv[i] = obv[i - 1] + v[i]
        elif c[i] < c[i - 1]:
            obv[i] = obv[i - 1] - v[i]
        else:
            obv[i] = obv[i - 1]

    # Divergence detection — last 30 bars
    divergence = None
    if len(df) >= 30:
        recent = slice(-30, None)
        price_low_idx = int(np.argmin(c[recent]))
        price_high_idx = int(np.argmax(c[recent]))
        obv_low_idx = int(np.argmin(obv[recent]))
        obv_high_idx = int(np.argmax(obv[recent]))
        # Bullish divergence: price made lower low but OBV didn't
        if price_low_idx > 5 and obv_low_idx < price_low_idx - 3:
            divergence = "bullish"
        # Bearish divergence: price made higher high but OBV didn't
        elif price_high_idx > 5 and obv_high_idx < price_high_idx - 3:
            divergence = "bearish"

    series = []
    for i in range(len(df)):
        if "date" in df.columns:
            series.append({
                "time": df.iloc[i]["date"].strftime("%Y-%m-%d"),
                "value": float(obv[i]),
            })

    return {
        "current": float(obv[-1]),
        "trend": "rising" if obv[-1] > obv[-5] else "falling",
        "divergence": divergence,  # "bullish" / "bearish" / None
        "series": series,
    }


# ─────────────────────────────────────────────────────────────────────────
#  Money Flow Index (MFI = volume-weighted RSI)
# ─────────────────────────────────────────────────────────────────────────

def compute_mfi(df: pd.DataFrame, period: int = 14) -> Optional[dict]:
    """MFI overbought >80, oversold <20. Better than RSI in volume-driven markets."""
    if len(df) < period + 5 or "volume" not in df.columns:
        return None
    h = df["high"].astype(float)
    l = df["low"].astype(float)
    c = df["close"].astype(float)
    v = df["volume"].astype(float)
    typical = (h + l + c) / 3
    raw_money = typical * v

    pos_flow = []
    neg_flow = []
    for i in range(1, len(df)):
        if typical.iloc[i] > typical.iloc[i - 1]:
            pos_flow.append(raw_money.iloc[i])
            neg_flow.append(0)
        elif typical.iloc[i] < typical.iloc[i - 1]:
            pos_flow.append(0)
            neg_flow.append(raw_money.iloc[i])
        else:
            pos_flow.append(0)
            neg_flow.append(0)

    pos = pd.Series([0] + pos_flow).rolling(period).sum().fillna(0)
    neg = pd.Series([0] + neg_flow).rolling(period).sum().fillna(0).replace(0, 1e-9)
    mfi = 100 - (100 / (1 + pos / neg))

    last = float(mfi.iloc[-1])
    if last >= 80:
        signal = "overbought"
    elif last <= 20:
        signal = "oversold"
    else:
        signal = "neutral"

    series = []
    if "date" in df.columns:
        for i in range(len(df)):
            if pd.notna(mfi.iloc[i]):
                series.append({
                    "time": df.iloc[i]["date"].strftime("%Y-%m-%d"),
                    "value": round(float(mfi.iloc[i]), 1),
                })

    return {
        "current": round(last, 1),
        "signal": signal,
        "overbought_threshold": 80,
        "oversold_threshold": 20,
        "series": series,
    }


# ─────────────────────────────────────────────────────────────────────────
#  Ichimoku Cloud
# ─────────────────────────────────────────────────────────────────────────

def compute_ichimoku(df: pd.DataFrame) -> Optional[dict]:
    """Ichimoku 5-component system:
        Tenkan (9):   conversion line (short-term avg of high/low)
        Kijun (26):   base line (medium-term avg)
        Senkou A:     (Tenkan + Kijun) / 2, projected 26 forward
        Senkou B:     52-period high/low avg, projected 26 forward
        Chikou:       close shifted 26 back

    Cloud = area between Senkou A & B. Above cloud = bullish, below = bearish.
    """
    if len(df) < 60:
        return None
    h = df["high"].astype(float)
    l = df["low"].astype(float)
    c = df["close"].astype(float)

    tenkan = (h.rolling(9).max() + l.rolling(9).min()) / 2
    kijun = (h.rolling(26).max() + l.rolling(26).min()) / 2
    senkou_a = ((tenkan + kijun) / 2).shift(26)
    senkou_b = ((h.rolling(52).max() + l.rolling(52).min()) / 2).shift(26)

    last_close = float(c.iloc[-1])
    last_a = float(senkou_a.iloc[-1]) if pd.notna(senkou_a.iloc[-1]) else None
    last_b = float(senkou_b.iloc[-1]) if pd.notna(senkou_b.iloc[-1]) else None
    last_tenkan = float(tenkan.iloc[-1]) if pd.notna(tenkan.iloc[-1]) else None
    last_kijun = float(kijun.iloc[-1]) if pd.notna(kijun.iloc[-1]) else None

    # Cloud signal
    if last_a is None or last_b is None:
        signal = "unknown"
    else:
        cloud_top = max(last_a, last_b)
        cloud_bot = min(last_a, last_b)
        if last_close > cloud_top:
            signal = "above_cloud_bullish"
        elif last_close < cloud_bot:
            signal = "below_cloud_bearish"
        else:
            signal = "inside_cloud_neutral"

    # TK Cross signal
    tk_cross = None
    if last_tenkan is not None and last_kijun is not None and len(df) >= 2:
        prev_tenkan = float(tenkan.iloc[-2]) if pd.notna(tenkan.iloc[-2]) else None
        prev_kijun = float(kijun.iloc[-2]) if pd.notna(kijun.iloc[-2]) else None
        if prev_tenkan and prev_kijun:
            if prev_tenkan <= prev_kijun and last_tenkan > last_kijun:
                tk_cross = "bullish"
            elif prev_tenkan >= prev_kijun and last_tenkan < last_kijun:
                tk_cross = "bearish"

    series = []
    if "date" in df.columns:
        for i in range(len(df)):
            row = {"time": df.iloc[i]["date"].strftime("%Y-%m-%d")}
            for k, s in [("tenkan", tenkan), ("kijun", kijun),
                         ("senkou_a", senkou_a), ("senkou_b", senkou_b)]:
                row[k] = round(float(s.iloc[i]), 2) if pd.notna(s.iloc[i]) else None
            series.append(row)

    return {
        "tenkan": round(last_tenkan, 2) if last_tenkan else None,
        "kijun": round(last_kijun, 2) if last_kijun else None,
        "senkou_a": round(last_a, 2) if last_a else None,
        "senkou_b": round(last_b, 2) if last_b else None,
        "signal": signal,
        "tk_cross": tk_cross,
        "series": series[-100:],  # last 100 bars for chart
    }


# ─────────────────────────────────────────────────────────────────────────
#  Wyckoff Spring + Sign of Strength (SOS) trigger events
# ─────────────────────────────────────────────────────────────────────────

def detect_wyckoff_events(df: pd.DataFrame, accumulation: Optional[dict],
                           lookback: int = 30) -> list[dict]:
    """Detects entry-trigger events within an accumulation/distribution phase:
      SPRING:  wick below accumulation range support + reclaim → strongest bullish
      SOS (Sign of Strength): wide-range break above range high on >2× volume
      UPTHRUST_AFTER_DIST: wick above distribution range high + reject → bearish
    """
    if len(df) < 20 or accumulation is None:
        return []

    n = len(df)
    o = df["open"].astype(float).values
    h = df["high"].astype(float).values
    l = df["low"].astype(float).values
    c = df["close"].astype(float).values
    v = df["volume"].astype(float).values
    vol_mean_20 = pd.Series(v).rolling(20).mean().fillna(1).values

    range_high = accumulation.get("range_high")
    range_low = accumulation.get("range_low")
    phase = accumulation.get("phase")
    if not range_high or not range_low:
        return []

    events = []
    start = max(0, n - lookback)
    for i in range(start, n):
        if vol_mean_20[i] <= 0:
            continue
        vr = v[i] / vol_mean_20[i]
        time_str = df.iloc[i]["date"].strftime("%Y-%m-%d") if "date" in df.columns else None

        # SPRING — wick below range_low, close back inside the range
        if phase in ("ACCUMULATION", "CONSOLIDATION") and \
           l[i] < range_low * 0.998 and c[i] > range_low and c[i] > o[i]:
            events.append({
                "idx": i, "time": time_str, "type": "WYCKOFF_SPRING",
                "bias": "bullish", "strength": 5,
                "description": f"SPRING — wicked below ৳{range_low:.2f} accumulation low and reclaimed. Strongest Wyckoff bullish signal.",
                "low": float(l[i]),
            })

        # SOS — close above range_high on >2× volume + wide spread
        if phase in ("ACCUMULATION", "CONSOLIDATION") and \
           c[i] > range_high and vr > 2.0 and (h[i] - l[i]) > 1.3 * pd.Series(h - l).iloc[max(0, i - 20):i].mean():
            events.append({
                "idx": i, "time": time_str, "type": "WYCKOFF_SOS",
                "bias": "bullish", "strength": 5,
                "description": f"SIGN OF STRENGTH — closed above ৳{range_high:.2f} range high on {vr:.1f}× volume + wide spread. Markup phase confirmed.",
                "high": float(h[i]),
            })

        # UTAD — wick above distribution range_high, close back below
        if phase == "DISTRIBUTION" and \
           h[i] > range_high * 1.002 and c[i] < range_high and c[i] < o[i]:
            events.append({
                "idx": i, "time": time_str, "type": "WYCKOFF_UTAD",
                "bias": "bearish", "strength": 5,
                "description": f"UPTHRUST AFTER DISTRIBUTION — wicked above ৳{range_high:.2f} and rejected. Markdown imminent.",
                "high": float(h[i]),
            })

    return events[-5:]


# ─────────────────────────────────────────────────────────────────────────
#  Bundle for chart response
# ─────────────────────────────────────────────────────────────────────────

def compute_advanced_indicators(df: pd.DataFrame, accumulation: Optional[dict] = None,
                                  recent_bars: int = 250) -> dict:
    """Run everything; safe-fail per indicator. Caps to last `recent_bars`
    so 5y data doesn't make these O(N) scans slow — these indicators only
    need a few months of history to be informative."""
    out: dict = {}
    # Use a tail window for indicators that don't benefit from deep history
    df_recent = df.tail(recent_bars).reset_index(drop=True) if len(df) > recent_bars else df
    try: out["vsa_events"] = classify_vsa_bars(df_recent, lookback=60)
    except Exception: out["vsa_events"] = []
    try: out["obv"] = compute_obv(df_recent)
    except Exception: out["obv"] = None
    try: out["mfi"] = compute_mfi(df_recent)
    except Exception: out["mfi"] = None
    try: out["ichimoku"] = compute_ichimoku(df_recent)
    except Exception: out["ichimoku"] = None
    try: out["wyckoff_events"] = detect_wyckoff_events(df_recent, accumulation)
    except Exception: out["wyckoff_events"] = []

    # JSON-safe conversion
    try:
        from api.order_flow import _to_native
        out = _to_native(out)
    except Exception:
        pass
    return out
