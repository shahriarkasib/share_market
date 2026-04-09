#!/usr/bin/env python3
"""Backtest: Does Fibonacci 1.0 breakout with confirmations predict price increase?"""

import warnings; warnings.filterwarnings("ignore")
import sys, os
import pandas as pd
import numpy as np
import psycopg2

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from config import DATABASE_URL

from ta.volume import ChaikinMoneyFlowIndicator
from ta.trend import EMAIndicator
from ta.momentum import RSIIndicator

conn = psycopg2.connect(DATABASE_URL)

print("Loading 6 months of data...")
df = pd.read_sql(
    "SELECT dp.symbol, dp.date, dp.open, dp.high, dp.low, dp.close, dp.volume, f.category "
    "FROM daily_prices dp JOIN fundamentals f ON dp.symbol = f.symbol "
    "WHERE dp.date >= '2025-10-01' AND dp.close > 0 AND f.category IN ('A','B') "
    "ORDER BY dp.symbol, dp.date",
    conn,
)
df["date"] = pd.to_datetime(df["date"])
print(f"Loaded {len(df)} rows, {df.symbol.nunique()} stocks")

results = []

for symbol, sdf in df.groupby("symbol"):
    if len(sdf) < 60:
        continue
    sdf = sdf.sort_values("date").reset_index(drop=True)

    close = sdf["close"]
    high = sdf["high"]
    low = sdf["low"]
    volume = sdf["volume"].astype(float)

    cmf = ChaikinMoneyFlowIndicator(high, low, close, volume, window=20).chaikin_money_flow()
    ema9 = EMAIndicator(close, window=9).ema_indicator()
    ema21 = EMAIndicator(close, window=21).ema_indicator()
    ema50 = EMAIndicator(close, window=50).ema_indicator()
    rsi = RSIIndicator(close, window=14).rsi()
    vol_avg = volume.rolling(20).mean()

    for i in range(60, len(sdf) - 10):
        lookback = 60
        recent_high = high.iloc[i - lookback : i].max()
        recent_low = low.iloc[i - lookback : i].min()

        if recent_high == recent_low:
            continue

        high_idx = high.iloc[i - lookback : i].idxmax()
        low_idx = low.iloc[i - lookback : i].idxmin()
        diff = recent_high - recent_low

        if low_idx < high_idx:  # uptrend swing
            fib_1_0 = recent_low + diff
        else:
            continue

        prev_close = close.iloc[i - 1]
        curr_close = close.iloc[i]

        # Breakout: crossed above Fib 1.0 today
        if curr_close > fib_1_0 and prev_close <= fib_1_0:
            cmf_val = cmf.iloc[i] if pd.notna(cmf.iloc[i]) else 0
            ema9_v = ema9.iloc[i] if pd.notna(ema9.iloc[i]) else 0
            ema21_v = ema21.iloc[i] if pd.notna(ema21.iloc[i]) else 0
            ema50_v = ema50.iloc[i] if pd.notna(ema50.iloc[i]) else 0
            rsi_v = rsi.iloc[i] if pd.notna(rsi.iloc[i]) else 50
            vol_v = volume.iloc[i]
            vol_avg_v = vol_avg.iloc[i] if pd.notna(vol_avg.iloc[i]) else vol_v

            ma_aligned = bool(ema9_v > ema21_v > ema50_v)
            cmf_positive = bool(cmf_val > 0)
            vol_high = bool(vol_v > vol_avg_v * 1.3) if vol_avg_v > 0 else False
            rsi_ok = bool(40 < rsi_v < 75)

            confirms = sum([ma_aligned, cmf_positive, vol_high, rsi_ok])

            fwd = {}
            for d in [1, 2, 3, 5, 10]:
                if i + d < len(sdf):
                    fwd[f"ret_{d}d"] = (close.iloc[i + d] - curr_close) / curr_close * 100
                else:
                    fwd[f"ret_{d}d"] = None

            results.append({
                "symbol": symbol,
                "date": str(sdf["date"].iloc[i].date()),
                "price": curr_close,
                "cmf": round(float(cmf_val), 3),
                "cmf_positive": cmf_positive,
                "ma_aligned": ma_aligned,
                "vol_high": vol_high,
                "rsi": round(float(rsi_v), 1),
                "rsi_ok": rsi_ok,
                "confirms": confirms,
                **fwd,
            })

rdf = pd.DataFrame(results)
print(f"\nFound {len(rdf)} Fibonacci 1.0 breakout events in 6 months\n")

# Overall
print("=== ALL FIB 1.0 BREAKOUTS ===")
for d in [1, 2, 3, 5, 10]:
    col = f"ret_{d}d"
    valid = rdf[col].dropna()
    win = (valid > 0).sum()
    total = len(valid)
    avg = valid.mean()
    med = valid.median()
    if total > 0:
        print(f"  Day {d:2d}: Win {win}/{total} ({win/total*100:.0f}%), Avg {avg:+.2f}%, Median {med:+.2f}%")

print()

# Strong confirmations (3+)
strong = rdf[rdf["confirms"] >= 3]
print(f"=== 3+ CONFIRMATIONS ({len(strong)} events) ===")
print(f"    (CMF>0 + MA aligned + Vol>1.3x + RSI 40-75)")
for d in [1, 2, 3, 5, 10]:
    col = f"ret_{d}d"
    valid = strong[col].dropna()
    if len(valid) == 0:
        continue
    win = (valid > 0).sum()
    total = len(valid)
    print(f"  Day {d:2d}: Win {win}/{total} ({win/total*100:.0f}%), Avg {valid.mean():+.2f}%, Median {valid.median():+.2f}%")

print()

# Weak (0-1 confirmations)
weak = rdf[rdf["confirms"] <= 1]
print(f"=== 0-1 CONFIRMATIONS ({len(weak)} events) ===")
for d in [1, 2, 3, 5, 10]:
    col = f"ret_{d}d"
    valid = weak[col].dropna()
    if len(valid) == 0:
        continue
    win = (valid > 0).sum()
    total = len(valid)
    print(f"  Day {d:2d}: Win {win}/{total} ({win/total*100:.0f}%), Avg {valid.mean():+.2f}%, Median {valid.median():+.2f}%")

print()

# Factor breakdown
print("=== FACTOR IMPACT (5-day return) ===")
for factor in ["cmf_positive", "ma_aligned", "vol_high", "rsi_ok"]:
    yes = rdf[rdf[factor] == True]["ret_5d"].dropna()
    no = rdf[rdf[factor] == False]["ret_5d"].dropna()
    if len(yes) > 3 and len(no) > 3:
        y_wr = (yes > 0).sum() / len(yes) * 100
        n_wr = (no > 0).sum() / len(no) * 100
        print(f"  {factor:15s}: WITH={len(yes):3d} win {y_wr:.0f}% avg {yes.mean():+.2f}% | WITHOUT={len(no):3d} win {n_wr:.0f}% avg {no.mean():+.2f}%")

# Top/bottom
print()
print("=== TOP 10 BEST (5-day) ===")
for _, r in rdf.nlargest(10, "ret_5d").iterrows():
    print(f"  {r['symbol']:12s} {r['date']} ৳{r['price']:.1f} → {r['ret_5d']:+.1f}% conf={r['confirms']} CMF={r['cmf']:+.3f} MA={'Y' if r['ma_aligned'] else 'N'}")

print()
print("=== TOP 10 WORST (5-day) ===")
for _, r in rdf.nsmallest(10, "ret_5d").iterrows():
    print(f"  {r['symbol']:12s} {r['date']} ৳{r['price']:.1f} → {r['ret_5d']:+.1f}% conf={r['confirms']} CMF={r['cmf']:+.3f} MA={'Y' if r['ma_aligned'] else 'N'}")

conn.close()
