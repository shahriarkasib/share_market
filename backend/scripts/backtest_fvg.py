#!/usr/bin/env python3
"""
Backtest: Fair Value Gap (FVG) on DSE.

Bullish FVG: candle[i-2] high < candle[i] low (gap between them)
Test: When price pulls back to fill the FVG, does it bounce?

Also test: FVG as support zone (buy when price returns to FVG)
"""

import warnings; warnings.filterwarnings("ignore")
import sys, os
import pandas as pd
import numpy as np
import psycopg2

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from config import DATABASE_URL
from ta.momentum import RSIIndicator

conn = psycopg2.connect(DATABASE_URL)

print("Loading 5 years...")
df = pd.read_sql(
    "SELECT dp.symbol, dp.date, dp.open, dp.high, dp.low, dp.close, dp.volume, "
    "f.category "
    "FROM daily_prices dp JOIN fundamentals f ON dp.symbol = f.symbol "
    "WHERE dp.date >= '2021-01-01' AND dp.close > 0 AND f.category IN ('A','B') "
    "AND dp.volume > 5000 ORDER BY dp.symbol, dp.date",
    conn,
)
df["date"] = pd.to_datetime(df["date"])
conn.close()
print(f"Loaded {len(df)} rows, {df.symbol.nunique()} stocks\n")

events = {
    "fvg_created": [],          # FVG just formed (price shot up)
    "fvg_pullback_touch": [],   # price came back to FVG zone
    "fvg_pullback_green": [],   # touched FVG + closed green
    "fvg_pullback_rsi40": [],   # touched FVG + RSI < 40
    "fvg_pullback_strong": [],  # touched FVG + green + volume
    "bearish_fvg_touch": [],    # bearish FVG: price comes up to fill gap
    "fvg_size_small": [],       # small FVG (< 2%)
    "fvg_size_large": [],       # large FVG (> 3%)
}

for symbol, sdf in df.groupby("symbol"):
    if len(sdf) < 60:
        continue
    sdf = sdf.sort_values("date").reset_index(drop=True)
    close = sdf["close"]
    high = sdf["high"]
    low = sdf["low"]
    open_ = sdf["open"]
    volume = sdf["volume"].astype(float)

    rsi = RSIIndicator(close, window=14).rsi()
    vol_avg = volume.rolling(20).mean()

    # Track active FVGs
    active_fvgs = []  # list of (fvg_low, fvg_high, created_idx)

    for i in range(2, len(sdf) - 10):
        c = close.iloc[i]
        o = open_.iloc[i]
        v = volume.iloc[i]
        rsi_v = rsi.iloc[i] if pd.notna(rsi.iloc[i]) else 50
        va = vol_avg.iloc[i] if pd.notna(vol_avg.iloc[i]) else v

        # Detect NEW bullish FVG: candle[i-2] high < candle[i] low
        if high.iloc[i - 2] < low.iloc[i]:
            fvg_low = float(high.iloc[i - 2])
            fvg_high = float(low.iloc[i])
            fvg_size_pct = (fvg_high - fvg_low) / fvg_low * 100

            if fvg_size_pct > 0.5:  # minimum gap size
                active_fvgs.append({
                    "low": fvg_low,
                    "high": fvg_high,
                    "created": i,
                    "size_pct": fvg_size_pct,
                    "filled": False,
                })

        # Detect bearish FVG: candle[i-2] low > candle[i] high
        if low.iloc[i - 2] > high.iloc[i]:
            bear_fvg_low = float(high.iloc[i])
            bear_fvg_high = float(low.iloc[i - 2])
            # Track for later

        # Check if price pulls back to any active bullish FVG
        for fvg in active_fvgs:
            if fvg["filled"]:
                continue
            if i - fvg["created"] < 2:  # need at least 2 bars after creation
                continue
            if i - fvg["created"] > 30:  # FVG expires after 30 bars
                fvg["filled"] = True
                continue

            # Price entered the FVG zone
            if low.iloc[i] <= fvg["high"] and close.iloc[i] >= fvg["low"]:
                fvg["filled"] = True

                # Calculate returns
                ret_3d = (close.iloc[i + 3] - c) / c * 100 if i + 3 < len(sdf) else None
                ret_5d = (close.iloc[i + 5] - c) / c * 100 if i + 5 < len(sdf) else None
                ret_10d = (close.iloc[i + 10] - c) / c * 100 if i + 10 < len(sdf) else None
                max_5d = (high.iloc[i + 1:i + 6].max() - c) / c * 100 if i + 5 < len(sdf) else None

                if ret_5d is None:
                    continue

                base = {
                    "symbol": symbol,
                    "date": str(sdf["date"].iloc[i].date()),
                    "price": c,
                    "fvg_low": fvg["low"],
                    "fvg_high": fvg["high"],
                    "fvg_size": round(fvg["size_pct"], 2),
                    "days_to_fill": i - fvg["created"],
                    "ret_3d": ret_3d,
                    "ret_5d": ret_5d,
                    "ret_10d": ret_10d,
                    "max_5d": max_5d,
                }

                events["fvg_pullback_touch"].append(base)

                green = c > o
                high_vol = v > va * 1.3 if va > 0 else False

                if green:
                    events["fvg_pullback_green"].append(base)
                if rsi_v < 40:
                    events["fvg_pullback_rsi40"].append(base)
                if green and high_vol:
                    events["fvg_pullback_strong"].append(base)
                if fvg["size_pct"] < 2:
                    events["fvg_size_small"].append(base)
                if fvg["size_pct"] > 3:
                    events["fvg_size_large"].append(base)

# Deduplicate
for key in events:
    if events[key]:
        edf = pd.DataFrame(events[key]).drop_duplicates(subset=["symbol", "date"])
        events[key] = edf.to_dict("records")

print("Results:\n")

def report(name, evts):
    if len(evts) < 20:
        print(f"{name}: {len(evts)} events (too few)\n")
        return
    edf = pd.DataFrame(evts)
    print(f"{'=' * 70}")
    print(f"{name} ({len(edf)} events)")
    for d, col, mcol in [(3, "ret_3d", None), (5, "ret_5d", "max_5d"), (10, "ret_10d", None)]:
        v = edf[col].dropna()
        if len(v) == 0:
            continue
        wr = (v > 0).sum() / len(v) * 100
        mstr = ""
        if mcol and mcol in edf.columns:
            m = edf[mcol].dropna()
            if len(m) > 0:
                pct3 = (m >= 3).sum() / len(m) * 100
                mstr = f" MaxAvg:{m.mean():+.1f}% Hit3%:{pct3:.0f}%"
        marker = " ***" if wr >= 55 else ""
        print(f"  Day {d:2d}: Win {wr:.0f}%, Avg {v.mean():+.2f}%, Med {v.median():+.2f}%{mstr}{marker}")
    print()

report("ALL FVG Pullback Touch (price returned to gap zone)", events["fvg_pullback_touch"])
report("FVG Touch + Green Candle", events["fvg_pullback_green"])
report("FVG Touch + RSI < 40", events["fvg_pullback_rsi40"])
report("FVG Touch + Green + High Volume", events["fvg_pullback_strong"])
report("Small FVG (< 2% gap)", events["fvg_size_small"])
report("Large FVG (> 3% gap)", events["fvg_size_large"])

# Ranking
print("=" * 70)
print("FVG RANKING BY 5-DAY WIN RATE")
print("=" * 70)

names = {
    "fvg_pullback_touch": "FVG Touch (any)",
    "fvg_pullback_green": "FVG + Green",
    "fvg_pullback_rsi40": "FVG + RSI<40",
    "fvg_pullback_strong": "FVG + Green + Volume",
    "fvg_size_small": "Small FVG (<2%)",
    "fvg_size_large": "Large FVG (>3%)",
}

rows = []
for key, evts in events.items():
    if len(evts) < 20 or key not in names:
        continue
    edf = pd.DataFrame(evts)
    v5 = edf["ret_5d"].dropna()
    m5 = edf["max_5d"].dropna() if "max_5d" in edf.columns else pd.Series()
    if len(v5) == 0:
        continue
    wr = (v5 > 0).sum() / len(v5) * 100
    rows.append({"name": names[key], "n": len(edf), "wr": wr, "avg": v5.mean(),
                 "hit3": (m5 >= 3).sum() / len(m5) * 100 if len(m5) > 0 else 0})

rows.sort(key=lambda x: -x["wr"])
print(f"\n{'Strategy':<30s} {'Events':>7s} {'5D WR':>7s} {'5D Avg':>8s} {'Hit 3%':>7s}")
print("-" * 65)
for r in rows:
    marker = " ***" if r["wr"] >= 55 else ""
    print(f"{r['name']:<30s} {r['n']:7d} {r['wr']:6.0f}% {r['avg']:+7.2f}% {r['hit3']:6.0f}%{marker}")

# Show recent examples
print(f"\n{'=' * 70}")
print("RECENT FVG PULLBACK EXAMPLES")
print("=" * 70)
if events["fvg_pullback_touch"]:
    recent = pd.DataFrame(events["fvg_pullback_touch"]).sort_values("date", ascending=False)
    print(f"\n{'Date':12s} {'Stock':12s} {'Price':>7s} {'FVG Zone':>15s} {'Gap%':>6s} {'DaysFill':>9s} {'5D Ret':>7s} {'Max5D':>7s}")
    print("-" * 80)
    for _, r in recent.head(20).iterrows():
        ret = f"{r['ret_5d']:+.1f}%" if r['ret_5d'] is not None else "?"
        mx = f"{r['max_5d']:+.1f}%" if r['max_5d'] is not None else "?"
        wl = "WIN" if r["ret_5d"] and r["ret_5d"] > 0 else "LOSS"
        print(f"{r['date']:12s} {r['symbol']:12s} {r['price']:7.1f} {r['fvg_low']:.1f}-{r['fvg_high']:.1f} {r['fvg_size']:5.1f}% {r['days_to_fill']:9d} {ret:>7s} {mx:>7s} {wl}")
