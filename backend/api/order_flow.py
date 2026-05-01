"""Order-flow analytics — Volume Profile, VWAP, Volume Delta, Absorption.

Designed to be data-source agnostic so the same module powers DSE
(`smc_chart.py`) and NASDAQ (`smc_chart_nasdaq.py`).

Inputs: pandas DataFrame with columns: date, open, high, low, close, volume.
"""
from __future__ import annotations

import math
from typing import Optional

import pandas as pd


def compute_volume_profile(df: pd.DataFrame, lookback: int = 60, n_bins: int = 50) -> Optional[dict]:
    """Build a price-bin volume histogram over the last `lookback` bars.

    Returns:
        {
          "poc": float,          # Point Of Control — highest-volume price
          "vah": float,          # Value Area High (top of 70% volume)
          "val": float,          # Value Area Low (bottom of 70% volume)
          "hvn": [float, ...],   # High Volume Nodes (top 5 bins)
          "lvn": [float, ...],   # Low Volume Nodes inside the value area
          "bins": [{"price": float, "volume": float, "pct": float}, ...]
        }
    """
    if len(df) < 5 or "volume" not in df.columns:
        return None
    sub = df.tail(lookback)
    if sub["high"].max() <= sub["low"].min():
        return None
    lo = float(sub["low"].min())
    hi = float(sub["high"].max())
    if not math.isfinite(lo) or not math.isfinite(hi) or hi <= lo:
        return None

    bin_width = (hi - lo) / n_bins
    bins = [0.0] * n_bins

    # Distribute each bar's volume across the bins it spans (TPO-style)
    for _, row in sub.iterrows():
        bar_lo = float(row["low"])
        bar_hi = float(row["high"])
        vol = float(row.get("volume") or 0)
        if vol <= 0 or bar_hi <= bar_lo:
            continue
        first = max(0, int((bar_lo - lo) / bin_width))
        last = min(n_bins - 1, int((bar_hi - lo) / bin_width))
        if last < first:
            continue
        per = vol / (last - first + 1)
        for i in range(first, last + 1):
            bins[i] += per

    total = sum(bins)
    if total <= 0:
        return None

    # POC = highest-volume bin
    poc_idx = max(range(n_bins), key=lambda i: bins[i])
    poc_price = lo + (poc_idx + 0.5) * bin_width

    # Value Area: expand from POC outward until 70% of volume captured
    target = total * 0.70
    captured = bins[poc_idx]
    left = right = poc_idx
    while captured < target and (left > 0 or right < n_bins - 1):
        left_v = bins[left - 1] if left > 0 else -1
        right_v = bins[right + 1] if right < n_bins - 1 else -1
        if left_v >= right_v and left > 0:
            left -= 1
            captured += bins[left]
        elif right < n_bins - 1:
            right += 1
            captured += bins[right]
        else:
            break
    val = lo + left * bin_width
    vah = lo + (right + 1) * bin_width

    # HVN: 5 highest bins
    sorted_bins = sorted(range(n_bins), key=lambda i: -bins[i])
    hvn = sorted([round(lo + (i + 0.5) * bin_width, 2) for i in sorted_bins[:5]])

    # LVN: bins inside the value area with <40% of POC volume
    lvn_threshold = bins[poc_idx] * 0.4
    lvn = [
        round(lo + (i + 0.5) * bin_width, 2)
        for i in range(left, right + 1)
        if bins[i] < lvn_threshold
    ]

    return {
        "poc": round(poc_price, 2),
        "vah": round(vah, 2),
        "val": round(val, 2),
        "hvn": hvn,
        "lvn": lvn[:8],
        "bins": [
            {"price": round(lo + (i + 0.5) * bin_width, 2),
             "volume": round(bins[i]),
             "pct": round(bins[i] / total * 100, 2)}
            for i in range(n_bins)
        ],
    }


def compute_vwap(df: pd.DataFrame, anchor_idx: Optional[int] = None) -> Optional[dict]:
    """Volume-Weighted Average Price + 1σ and 2σ bands.

    If `anchor_idx` is None, anchors at the start of df. Otherwise anchors
    from that bar (e.g. last BOS event for "anchored VWAP").

    Returns last-bar values + the full series for charting.
    """
    if len(df) < 2 or "volume" not in df.columns:
        return None
    sub = df if anchor_idx is None else df.iloc[anchor_idx:].reset_index(drop=True)
    typical = (sub["high"] + sub["low"] + sub["close"]) / 3.0
    pv = typical * sub["volume"].astype(float)
    cum_pv = pv.cumsum()
    cum_v = sub["volume"].astype(float).cumsum().replace(0, 1e-9)
    vwap = cum_pv / cum_v

    # Variance for bands (volume-weighted)
    diff_sq = ((typical - vwap) ** 2) * sub["volume"].astype(float)
    cum_diff = diff_sq.cumsum()
    var = cum_diff / cum_v
    std = var ** 0.5

    last_vwap = float(vwap.iloc[-1])
    last_std = float(std.iloc[-1])

    return {
        "value": round(last_vwap, 2),
        "upper_1sd": round(last_vwap + last_std, 2),
        "lower_1sd": round(last_vwap - last_std, 2),
        "upper_2sd": round(last_vwap + 2 * last_std, 2),
        "lower_2sd": round(last_vwap - 2 * last_std, 2),
        "anchor_time": sub.iloc[0]["date"].strftime("%Y-%m-%d"),
        "series": [
            {
                "time": sub.iloc[i]["date"].strftime("%Y-%m-%d"),
                "vwap": round(float(vwap.iloc[i]), 2),
                "upper_1sd": round(float(vwap.iloc[i] + std.iloc[i]), 2),
                "lower_1sd": round(float(vwap.iloc[i] - std.iloc[i]), 2),
                "upper_2sd": round(float(vwap.iloc[i] + 2 * std.iloc[i]), 2),
                "lower_2sd": round(float(vwap.iloc[i] - 2 * std.iloc[i]), 2),
            }
            for i in range(len(sub))
        ],
    }


def compute_volume_delta(df: pd.DataFrame) -> Optional[dict]:
    """Per-bar buy/sell volume split using close-position weighting.

    For a bar with range R = high - low and close C:
      buy_ratio  = (close - low) / R     (close near high → buyers won)
      sell_ratio = (high - close) / R    (close near low → sellers won)
      buy_vol    = volume × buy_ratio
      sell_vol   = volume × sell_ratio
      delta      = buy_vol - sell_vol

    Returns last-bar delta + cumulative delta series for charting.
    """
    if len(df) < 2 or "volume" not in df.columns:
        return None
    h = df["high"].astype(float)
    l = df["low"].astype(float)
    c = df["close"].astype(float)
    v = df["volume"].astype(float)
    rng = (h - l).replace(0, 1e-9)
    buy_ratio = ((c - l) / rng).clip(0, 1)
    sell_ratio = ((h - c) / rng).clip(0, 1)
    buy_vol = v * buy_ratio
    sell_vol = v * sell_ratio
    delta = buy_vol - sell_vol
    cum = delta.cumsum()

    n = len(df)
    return {
        "last_delta": round(float(delta.iloc[-1])),
        "last_cum": round(float(cum.iloc[-1])),
        "delta_5d": round(float(delta.iloc[-5:].sum())) if n >= 5 else round(float(delta.sum())),
        "delta_20d": round(float(delta.iloc[-20:].sum())) if n >= 20 else round(float(delta.sum())),
        "series": [
            {
                "time": df.iloc[i]["date"].strftime("%Y-%m-%d"),
                "delta": round(float(delta.iloc[i])),
                "cumulative": round(float(cum.iloc[i])),
                "color": "#26a69a" if delta.iloc[i] >= 0 else "#ef5350",
            }
            for i in range(n)
        ],
    }


def detect_absorption(df: pd.DataFrame) -> Optional[dict]:
    """Did today's bar show institutional absorption?

    A "buyer absorbed sellers" bar has all three:
      1. Above-average volume (>= 1.3× of 20-bar avg) — institutions stepped in
      2. Long lower wick — sellers tried, buyers absorbed (lower-wick / range >= 0.4)
      3. Close in upper third of range — buyers won the close

    Returns the boolean flag + diagnostic numbers.
    """
    if len(df) < 21 or "volume" not in df.columns:
        return None
    last = df.iloc[-1]
    h, l, o, c = float(last["high"]), float(last["low"]), float(last["open"]), float(last["close"])
    rng = max(h - l, 1e-9)
    body = c - o
    lower_wick = min(o, c) - l
    upper_wick = h - max(o, c)
    close_strength = (c - l) / rng

    avg20 = float(df["volume"].iloc[-21:-1].mean() or 1)
    last_vol = float(last["volume"] or 0)
    vol_ratio = last_vol / avg20 if avg20 > 0 else 0

    # Absorption candidate: volume + wick + close strength
    absorbed = (
        vol_ratio >= 1.3
        and lower_wick / rng >= 0.4
        and close_strength >= 0.66
        and c > o  # green close
    )

    # Composite "buyer strength" 0..1
    strength = (
        0.4 * min(vol_ratio / 2.0, 1.0)
        + 0.3 * min(lower_wick / rng / 0.6, 1.0)
        + 0.3 * close_strength
    )

    return {
        "absorbed": bool(absorbed),
        "strength": float(round(strength, 2)),
        "vol_ratio": float(round(vol_ratio, 2)),
        "lower_wick_ratio": float(round(lower_wick / rng, 2)),
        "upper_wick_ratio": float(round(upper_wick / rng, 2)),
        "close_strength": float(round(close_strength, 2)),
        "body_pct": float(round(abs(body) / rng, 2)),
    }


def get_orderbook_imbalance(symbol: str, conn) -> Optional[dict]:
    """DSE-only: rolling 5-snapshot bid/ask imbalance from the
    `order_book_snapshots` table populated by `orderbook_scraper.py`.

    Returns None if no data or table missing.
    """
    try:
        rows = conn.execute(
            "SELECT bid_size, ask_size, snapshot_time FROM order_book_snapshots "
            "WHERE symbol = ? ORDER BY snapshot_time DESC LIMIT 5",
            (symbol.upper(),),
        ).fetchall()
    except Exception:
        return None
    if not rows:
        return None

    total_bid = sum(float(r["bid_size"] or 0) for r in rows)
    total_ask = sum(float(r["ask_size"] or 0) for r in rows)
    total = total_bid + total_ask
    if total <= 0:
        return None
    imb = (total_bid - total_ask) / total

    if imb > 0.15:
        verdict = "Strong buyer leaning"
    elif imb > 0.05:
        verdict = "Mild buyer leaning"
    elif imb < -0.15:
        verdict = "Strong seller leaning"
    elif imb < -0.05:
        verdict = "Mild seller leaning"
    else:
        verdict = "Balanced"

    return {
        "imbalance": round(imb, 3),  # -1..+1
        "imbalance_pct": round(imb * 100, 1),
        "bid_size": round(total_bid),
        "ask_size": round(total_ask),
        "verdict": verdict,
        "snapshots": len(rows),
    }


def _to_native(obj):
    """Force every value to native Python type via JSON round-trip.
    Catches numpy.bool, numpy.float64, numpy.int64, pandas Timestamp, etc."""
    import json
    import numpy as _np

    def _default(o):
        if isinstance(o, _np.bool_):
            return bool(o)
        if isinstance(o, _np.integer):
            return int(o)
        if isinstance(o, _np.floating):
            return float(o)
        if hasattr(o, "tolist"):
            return o.tolist()
        if hasattr(o, "isoformat"):
            return o.isoformat()
        return str(o)

    return json.loads(json.dumps(obj, default=_default))


def compute_full_order_flow(df: pd.DataFrame, symbol: str = None, conn=None) -> dict:
    """Convenience: run everything and return a single dict ready to paste
    into the chart API response."""
    out = {
        "volume_profile": compute_volume_profile(df, lookback=60, n_bins=50),
        "vwap": compute_vwap(df),
        "volume_delta": compute_volume_delta(df),
        "absorption": detect_absorption(df),
        "orderbook_imbalance": None,
    }
    if symbol and conn is not None:
        out["orderbook_imbalance"] = get_orderbook_imbalance(symbol, conn)
    return _to_native(out)
