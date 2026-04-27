#!/usr/bin/env python3
"""
SMC Screener — scores every DSE stock on bullish setup quality.
Output: ranked list of best buy candidates for tomorrow.

Score components (max 100):
  +25  Recent bullish ChoCh in last 30 days
  +20  Subsequent bullish BOS confirmed
  +15  Net unmitigated bullish FVGs (3+ more bullish than bearish below price)
  +10  Latest structure event is bullish
  +10  Price within 8% of swing high (room to break out, not extended)
  +10  Rising volume on recent green candles
  +5   Price above 50-day SMA
  +5   Recent FVG just below price (clean support)
"""

import sys
import os
sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

import pandas as pd
from datetime import datetime, timedelta
from data.repository import read_historical_for_symbol
from database import get_connection
from api.smc_chart import find_swings, detect_structure, detect_fvgs


def score_stock(symbol):
    """Returns (score, details_dict) or None if insufficient data."""
    df = read_historical_for_symbol(symbol, min_rows=250)
    if df.empty or len(df) < 100:
        return None

    df["date"] = pd.to_datetime(df["date"])
    df = df.sort_values("date").reset_index(drop=True)
    if len(df) < 100:
        return None

    # Last ~6 months of data
    cutoff = df["date"].max() - pd.Timedelta(days=180)
    df = df[df["date"] >= cutoff].reset_index(drop=True)
    if len(df) < 30:
        return None

    h, l, c, o = df["high"], df["low"], df["close"], df["open"]
    v = df["volume"] if "volume" in df.columns else pd.Series([0] * len(df))
    current_price = float(c.iloc[-1])
    if current_price <= 0:
        return None

    # === STRUCTURE ===
    swings = find_swings(h, l, n=3)
    events = detect_structure(swings)

    score = 0
    breakdown = {}

    # +25: Recent bullish ChoCh in last 30 bars
    last_30_idx_threshold = len(df) - 30
    recent_bull_choch = [
        e for e in events
        if e["type"] == "bullish_ChoCh" and e["idx"] >= last_30_idx_threshold
    ]
    if recent_bull_choch:
        score += 25
        breakdown["recent_bullish_choch"] = True
        last_choch_idx = recent_bull_choch[-1]["idx"]
    else:
        breakdown["recent_bullish_choch"] = False
        last_choch_idx = None

    # +20: Bullish BOS after the ChoCh
    if last_choch_idx is not None:
        bull_bos_after = [
            e for e in events
            if e["type"] == "bullish_BOS" and e["idx"] > last_choch_idx
        ]
        if bull_bos_after:
            score += 20
            breakdown["bos_after_choch"] = True
        else:
            breakdown["bos_after_choch"] = False
    else:
        breakdown["bos_after_choch"] = False

    # +10: Latest structure event is bullish
    if events:
        latest = events[-1]
        if latest["type"].startswith("bullish") and latest["idx"] >= last_30_idx_threshold:
            score += 10
            breakdown["latest_event_bullish"] = True
        else:
            breakdown["latest_event_bullish"] = False
    else:
        breakdown["latest_event_bullish"] = False

    # === FVG ANALYSIS ===
    fvgs = detect_fvgs(h, l)
    cutoff_idx = max(0, len(df) - 60)
    recent_fvgs = [f for f in fvgs if f["size_pct"] > 0.5 and f["idx"] >= cutoff_idx]

    # Mitigation tag
    def is_mitigated(f):
        for j in range(f["idx"] + 1, len(df)):
            if f["type"] == "bullish" and float(l.iloc[j]) < f["bottom"]:
                return True
            if f["type"] == "bearish" and float(h.iloc[j]) > f["top"]:
                return True
        return False

    unmit = [f for f in recent_fvgs if not is_mitigated(f)]
    bull_unmit = [f for f in unmit if f["type"] == "bullish" and f["top"] < current_price]
    bear_unmit = [f for f in unmit if f["type"] == "bearish" and f["bottom"] > current_price]

    net_bull_fvg = len(bull_unmit) - len(bear_unmit)
    if net_bull_fvg >= 3:
        score += 15
        breakdown["net_bullish_fvgs_above_3"] = True
    elif net_bull_fvg >= 1:
        score += 7
        breakdown["net_bullish_fvgs_above_3"] = False
    else:
        breakdown["net_bullish_fvgs_above_3"] = False

    breakdown["bullish_fvgs_below"] = len(bull_unmit)
    breakdown["bearish_fvgs_above"] = len(bear_unmit)

    # +10: Price within 8% of swing high (not extended)
    recent_swings_in_window = [s for s in swings if s["idx"] >= cutoff_idx]
    recent_highs = [s for s in recent_swings_in_window if s["type"] == "high"]
    if recent_highs:
        recent_swing_high = max(recent_highs, key=lambda s: s["price"])["price"]
        pct_from_high = (current_price - recent_swing_high) / recent_swing_high * 100
        breakdown["pct_from_swing_high"] = round(pct_from_high, 1)
        # Sweet spot: -8% to +2% from swing high
        if -8 <= pct_from_high <= 2:
            score += 10
            breakdown["near_swing_high"] = True
        else:
            breakdown["near_swing_high"] = False
        # Penalty for parabolic: more than 30% above SMA50
        if len(c) >= 50:
            sma50 = float(c.iloc[-50:].mean())
            extension = (current_price - sma50) / sma50 * 100
            breakdown["pct_above_sma50"] = round(extension, 1)
            if extension > 30:
                score -= 15  # parabolic — heavy penalty
                breakdown["parabolic_penalty"] = True
    else:
        breakdown["near_swing_high"] = False

    # +10: Rising volume on green candles (last 5 bars)
    last5_close = c.iloc[-5:].values
    last5_open = o.iloc[-5:].values
    last5_vol = v.iloc[-5:].values
    avg_vol_20 = float(v.iloc[-20:].mean()) if len(v) >= 20 else float(v.mean() or 0)
    green_vols = [last5_vol[i] for i in range(5) if last5_close[i] > last5_open[i]]
    if avg_vol_20 > 0 and green_vols and sum(green_vols) / len(green_vols) > avg_vol_20:
        score += 10
        breakdown["volume_rising_on_green"] = True
    else:
        breakdown["volume_rising_on_green"] = False

    # +5: Price above SMA50
    if len(c) >= 50:
        sma50 = float(c.iloc[-50:].mean())
        if current_price > sma50:
            score += 5
            breakdown["above_sma50"] = True
        else:
            breakdown["above_sma50"] = False

    # +5: Fresh FVG just below price (within 5%)
    immediate_support_fvg = [
        f for f in bull_unmit
        if 0 < (current_price - f["top"]) / current_price * 100 < 5
    ]
    if immediate_support_fvg:
        score += 5
        breakdown["fvg_support_nearby"] = True
    else:
        breakdown["fvg_support_nearby"] = False

    # === Build output ===
    recent_swings_lows = [s for s in recent_swings_in_window if s["type"] == "low"]
    swing_low = (
        min(recent_swings_lows, key=lambda s: s["price"])["price"]
        if recent_swings_lows
        else current_price * 0.9
    )
    swing_high_val = (
        max(recent_highs, key=lambda s: s["price"])["price"]
        if recent_highs
        else current_price * 1.1
    )

    chg5 = (current_price - float(c.iloc[-6])) / float(c.iloc[-6]) * 100 if len(c) > 5 else 0
    chg20 = (current_price - float(c.iloc[-21])) / float(c.iloc[-21]) * 100 if len(c) > 20 else 0

    return {
        "symbol": symbol,
        "price": round(current_price, 2),
        "score": score,
        "chg_5d": round(chg5, 1),
        "chg_20d": round(chg20, 1),
        "swing_low": round(swing_low, 2),
        "swing_high": round(swing_high_val, 2),
        "breakout_trigger": round(swing_high_val * 1.02, 2),
        "stop_loss": round(swing_low * 0.98, 2),
        "bullish_fvgs_below": len(bull_unmit),
        "bearish_fvgs_above": len(bear_unmit),
        "details": breakdown,
    }


def main():
    # Get list of all DSE symbols from the live_prices table
    conn = get_connection()
    rows = conn.execute(
        "SELECT DISTINCT symbol FROM live_prices WHERE ltp > 0 ORDER BY symbol"
    ).fetchall()
    symbols = [r[0] for r in rows]
    conn.close()

    print(f"Scoring {len(symbols)} DSE stocks...\n")

    results = []
    skipped = 0
    for i, sym in enumerate(symbols):
        if i % 50 == 0 and i > 0:
            print(f"  Processed {i}/{len(symbols)}...")
        try:
            r = score_stock(sym)
            if r is not None:
                results.append(r)
            else:
                skipped += 1
        except Exception:
            skipped += 1

    results.sort(key=lambda x: -x["score"])

    print(f"\nProcessed {len(results)} stocks (skipped {skipped} for insufficient data)\n")

    print("=" * 110)
    print(f"{'TOP 30 BUY CANDIDATES — Highest SMC setup quality':^110}")
    print("=" * 110)
    print(f"{'Rank':<5}{'Symbol':<12}{'Price':>8}{'Score':>7}{'5dChg':>8}{'20dChg':>8}{'BullFVG':>9}{'BearFVG':>9}{'Trigger':>10}{'Stop':>9}")
    print("-" * 110)
    for i, r in enumerate(results[:30], 1):
        print(
            f"{i:<5}{r['symbol']:<12}{r['price']:>8.1f}"
            f"{r['score']:>7}{r['chg_5d']:>+7.1f}%{r['chg_20d']:>+7.1f}%"
            f"{r['bullish_fvgs_below']:>9}{r['bearish_fvgs_above']:>9}"
            f"{r['breakout_trigger']:>10.1f}{r['stop_loss']:>9.1f}"
        )

    # Show middle tier as well
    print("\n" + "=" * 110)
    print(f"{'NEXT TIER (rank 31-50)':^110}")
    print("=" * 110)
    print(f"{'Rank':<5}{'Symbol':<12}{'Price':>8}{'Score':>7}{'5dChg':>8}{'20dChg':>8}{'BullFVG':>9}{'BearFVG':>9}")
    print("-" * 110)
    for i, r in enumerate(results[30:50], 31):
        print(
            f"{i:<5}{r['symbol']:<12}{r['price']:>8.1f}"
            f"{r['score']:>7}{r['chg_5d']:>+7.1f}%{r['chg_20d']:>+7.1f}%"
            f"{r['bullish_fvgs_below']:>9}{r['bearish_fvgs_above']:>9}"
        )

    # Detail breakdown for the top 5
    print("\n" + "=" * 110)
    print(f"{'TOP 5 — DETAILED BREAKDOWN':^110}")
    print("=" * 110)
    for i, r in enumerate(results[:5], 1):
        d = r["details"]
        print(f"\n#{i}. {r['symbol']} @ {r['price']} ৳   (Score: {r['score']})")
        print(f"  20-day change: {r['chg_20d']:+.1f}%   |   5-day change: {r['chg_5d']:+.1f}%")
        print(f"  Swing low: {r['swing_low']}   Swing high: {r['swing_high']}")
        print(f"  Breakout trigger: {r['breakout_trigger']}   Stop loss: {r['stop_loss']}")
        print(f"  Bullish FVGs as support: {r['bullish_fvgs_below']}")
        print(f"  Bearish FVGs above: {r['bearish_fvgs_above']}")
        print(f"  Recent bullish ChoCh: {d.get('recent_bullish_choch')}")
        print(f"  BOS after ChoCh: {d.get('bos_after_choch')}")
        print(f"  Latest event bullish: {d.get('latest_event_bullish')}")
        print(f"  Volume rising on green: {d.get('volume_rising_on_green')}")
        print(f"  Above SMA50: {d.get('above_sma50')}")
        print(f"  Near swing high (-8% to +2%): {d.get('near_swing_high')}")
        if d.get("parabolic_penalty"):
            print(f"  ⚠ Parabolic penalty applied (price {d.get('pct_above_sma50')}% above SMA50)")


if __name__ == "__main__":
    main()
