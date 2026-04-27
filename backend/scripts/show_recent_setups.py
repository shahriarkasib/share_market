#!/usr/bin/env python3
"""Show recent examples of the 79-83% win rate setup with actual results."""

import warnings; warnings.filterwarnings("ignore")
import sys, os
import pandas as pd
import numpy as np
import psycopg2

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from config import DATABASE_URL
from ta.momentum import RSIIndicator

conn = psycopg2.connect(DATABASE_URL)

df = pd.read_sql(
    "SELECT dp.symbol, dp.date, dp.open, dp.high, dp.low, dp.close, dp.volume "
    "FROM daily_prices dp JOIN fundamentals f ON dp.symbol = f.symbol "
    "WHERE dp.date >= '2025-12-01' AND dp.close > 0 AND f.category IN ('A','B') "
    "AND dp.volume > 10000 ORDER BY dp.symbol, dp.date",
    conn,
)
df["date"] = pd.to_datetime(df["date"])
conn.close()

results = []

for symbol, sdf in df.groupby("symbol"):
    if len(sdf) < 60:
        continue
    sdf = sdf.sort_values("date").reset_index(drop=True)
    close = sdf["close"]
    high = sdf["high"]
    low = sdf["low"]
    open_ = sdf["open"]

    rsi = RSIIndicator(close, window=14).rsi()

    for i in range(60, len(sdf) - 10):
        if sdf["date"].iloc[i] < pd.Timestamp("2026-02-01"):
            continue

        c = close.iloc[i]
        l_val = low.iloc[i]
        o = open_.iloc[i]
        rsi_v = rsi.iloc[i] if pd.notna(rsi.iloc[i]) else 50

        if rsi_v >= 40:
            continue

        # Find support clusters
        lookback_lows = low.iloc[i-60:i]
        swing_lows = []
        for j in range(2, len(lookback_lows) - 2):
            idx = i - 60 + j
            if low.iloc[idx] == min(low.iloc[idx-2:idx+3]):
                swing_lows.append(float(low.iloc[idx]))

        if len(swing_lows) < 3:
            continue

        swing_lows.sort()
        clusters = []
        current = [swing_lows[0]]
        for sl in swing_lows[1:]:
            if abs(sl - current[-1]) / current[-1] * 100 <= 1.5:
                current.append(sl)
            else:
                if len(current) >= 3:
                    clusters.append(current)
                current = [sl]
        if len(current) >= 3:
            clusters.append(current)

        at_support = False
        sup_price = 0
        sup_touches = 0
        for cl in clusters:
            avg = np.mean(cl)
            if abs(l_val - avg) / avg * 100 < 2:
                at_support = True
                sup_price = round(avg, 1)
                sup_touches = len(cl)
                break

        if not at_support:
            continue

        green = c > o
        body = abs(c - o)
        lower_shadow = min(c, o) - l_val
        hammer = lower_shadow > body * 2 if body > 0 else False
        bullish = green or hammer

        if not bullish:
            continue

        ret_5d = (close.iloc[i+5] - c) / c * 100 if i+5 < len(sdf) else None
        ret_10d = (close.iloc[i+10] - c) / c * 100 if i+10 < len(sdf) else None
        max_5d = (high.iloc[i+1:i+6].max() - c) / c * 100 if i+5 < len(sdf) else None

        results.append({
            "symbol": symbol,
            "date": str(sdf["date"].iloc[i].date()),
            "price": c,
            "rsi": round(rsi_v, 1),
            "support": sup_price,
            "touches": sup_touches,
            "ret_5d": round(ret_5d, 1) if ret_5d is not None else None,
            "ret_10d": round(ret_10d, 1) if ret_10d is not None else None,
            "max_5d": round(max_5d, 1) if max_5d is not None else None,
        })

rdf = pd.DataFrame(results).drop_duplicates(subset=["symbol", "date"])

print("RECENT EXAMPLES: Support 3T + RSI<40 + Bullish Candle")
print("(The 83% win rate setup — backtested on 6 months, 277 stocks)")
print("=" * 95)
header = f"{'Date':12s} {'Symbol':12s} {'Entry':>7s} {'RSI':>6s} {'Sup':>7s} {'T':>3s} {'5D':>7s} {'10D':>7s} {'5D Max':>7s} {'Result':>7s}"
print(header)
print("-" * 95)

rdf_valid = rdf[rdf["ret_5d"].notna()].sort_values("date", ascending=False)

for _, r in rdf_valid.head(40).iterrows():
    result = "WIN" if r["ret_5d"] > 0 else "LOSS"
    ret5 = f"{r['ret_5d']:+.1f}%"
    ret10 = f"{r['ret_10d']:+.1f}%" if r["ret_10d"] is not None else "?"
    max5 = f"{r['max_5d']:+.1f}%" if r["max_5d"] is not None else "?"
    print(f"{r['date']:12s} {r['symbol']:12s} {r['price']:7.1f} {r['rsi']:6.1f} {r['support']:7.1f} {r['touches']:3d} {ret5:>7s} {ret10:>7s} {max5:>7s} {'  ' + result:>7s}")

print()
total = len(rdf_valid)
wins = len(rdf_valid[rdf_valid["ret_5d"] > 0])
losses = total - wins
print(f"Total: {total} signals")
print(f"Winners: {wins} ({wins/total*100:.0f}%)")
print(f"Losers: {losses} ({losses/total*100:.0f}%)")
print(f"Avg 5D return: {rdf_valid['ret_5d'].mean():+.2f}%")
print(f"Avg 5D max gain: {rdf_valid['max_5d'].dropna().mean():+.2f}%")
print(f"Median 5D return: {rdf_valid['ret_5d'].median():+.2f}%")
