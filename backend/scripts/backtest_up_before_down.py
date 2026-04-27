#!/usr/bin/env python3
"""
The REAL question: After buying, does the stock go UP first or DOWN first?

For each stock, buy at Sunday close:
- Track HIGH and LOW of each subsequent day (Mon, Tue, Wed, Thu)
- Did it hit +1% BEFORE hitting -1%?
- Did it hit +2% BEFORE hitting -2%?
- This tells us: is the first move UP or DOWN?
"""

import warnings; warnings.filterwarnings("ignore")
import sys, os
import pandas as pd
import numpy as np
import psycopg2

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from config import DATABASE_URL

conn = psycopg2.connect(DATABASE_URL)

print("Loading data...")
df = pd.read_sql(
    "SELECT dp.symbol, dp.date, dp.open, dp.high, dp.low, dp.close, dp.volume, "
    "f.category "
    "FROM daily_prices dp JOIN fundamentals f ON dp.symbol = f.symbol "
    "WHERE dp.date >= '2021-01-01' AND dp.close > 0 AND f.category IN ('A','B') "
    "AND dp.volume > 5000 ORDER BY dp.symbol, dp.date",
    conn,
)
df["date"] = pd.to_datetime(df["date"])
df["dow"] = df["date"].dt.dayofweek
conn.close()

print(f"Loaded {len(df)} rows, {df.symbol.nunique()} stocks\n")

stock_results = []

for symbol, sdf in df.groupby("symbol"):
    if len(sdf) < 100:
        continue
    sdf = sdf.sort_values("date").reset_index(drop=True)
    close = sdf["close"]
    high = sdf["high"]
    low = sdf["low"]

    cat = sdf["category"].iloc[0]

    trades_1pct = []  # did +1% come before -1%?
    trades_2pct = []
    trades_3pct = []

    for i in range(len(sdf)):
        if sdf["dow"].iloc[i] != 6:  # not Sunday
            continue

        buy_price = close.iloc[i]
        if buy_price == 0:
            continue

        # Track next 4 trading days (Mon-Thu) bar by bar
        hit_up_1 = False
        hit_down_1 = False
        hit_up_2 = False
        hit_down_2 = False
        hit_up_3 = False
        hit_down_3 = False
        first_1pct = None  # "UP" or "DOWN"
        first_2pct = None
        first_3pct = None

        for j in range(i + 1, min(i + 5, len(sdf))):
            day_high = high.iloc[j]
            day_low = low.iloc[j]

            up_pct = (day_high - buy_price) / buy_price * 100
            down_pct = (day_low - buy_price) / buy_price * 100

            # 1% threshold
            if not hit_up_1 and up_pct >= 1:
                hit_up_1 = True
                if first_1pct is None:
                    first_1pct = "UP"
            if not hit_down_1 and down_pct <= -1:
                hit_down_1 = True
                if first_1pct is None:
                    first_1pct = "DOWN"

            # 2% threshold
            if not hit_up_2 and up_pct >= 2:
                hit_up_2 = True
                if first_2pct is None:
                    first_2pct = "UP"
            if not hit_down_2 and down_pct <= -2:
                hit_down_2 = True
                if first_2pct is None:
                    first_2pct = "DOWN"

            # 3% threshold
            if not hit_up_3 and up_pct >= 3:
                hit_up_3 = True
                if first_3pct is None:
                    first_3pct = "UP"
            if not hit_down_3 and down_pct <= -3:
                hit_down_3 = True
                if first_3pct is None:
                    first_3pct = "DOWN"

        if first_1pct:
            trades_1pct.append(first_1pct)
        if first_2pct:
            trades_2pct.append(first_2pct)
        if first_3pct:
            trades_3pct.append(first_3pct)

    if len(trades_1pct) < 20:
        continue

    up_first_1 = sum(1 for t in trades_1pct if t == "UP") / len(trades_1pct) * 100
    up_first_2 = sum(1 for t in trades_2pct if t == "UP") / len(trades_2pct) * 100 if trades_2pct else 0
    up_first_3 = sum(1 for t in trades_3pct if t == "UP") / len(trades_3pct) * 100 if trades_3pct else 0

    stock_results.append({
        "symbol": symbol,
        "category": cat,
        "weeks": len(trades_1pct),
        "up_first_1pct": round(up_first_1, 0),
        "up_first_2pct": round(up_first_2, 0),
        "up_first_3pct": round(up_first_3, 0),
        "n_2pct": len(trades_2pct),
        "n_3pct": len(trades_3pct),
    })

rdf = pd.DataFrame(stock_results)

print("=" * 75)
print("DOES +1% COME BEFORE -1%? (Buy Sunday, track Mon-Thu)")
print("=" * 75)
print()
print("If 'Up First' > 60% = stock tends to go UP first after Sunday buy")
print("If 'Up First' < 40% = stock tends to go DOWN first (bad for buying)")
print()

# 1% threshold
print("=== +1% vs -1%: Which comes first? ===")
top_up = rdf[rdf["up_first_1pct"] >= 60].sort_values("up_first_1pct", ascending=False)
print(f"\nStocks where +1% comes FIRST 60%+ of weeks ({len(top_up)} stocks):")
print(f"{'Stock':<12s} {'Cat':>3s} {'Weeks':>6s} {'+1% First':>10s} {'+2% First':>10s} {'+3% First':>10s}")
print("-" * 55)
for _, r in top_up.head(25).iterrows():
    print(f"{r['symbol']:<12s} {r['category']:>3s} {r['weeks']:6.0f} {r['up_first_1pct']:9.0f}% {r['up_first_2pct']:9.0f}% {r['up_first_3pct']:9.0f}%")

print(f"\nStocks where +1% comes first 70%+ ({len(rdf[rdf['up_first_1pct'] >= 70])} stocks):")
for _, r in rdf[rdf["up_first_1pct"] >= 70].sort_values("up_first_1pct", ascending=False).iterrows():
    print(f"  {r['symbol']:<12s} {r['category']:>3s} weeks={r['weeks']:.0f} +1%first={r['up_first_1pct']:.0f}% +2%first={r['up_first_2pct']:.0f}%")

# Overall distribution
print(f"\n=== DISTRIBUTION ===")
for thresh in [70, 65, 60, 55, 50, 45, 40]:
    count = (rdf["up_first_1pct"] >= thresh).sum()
    print(f"  +1% first >= {thresh}%: {count:3d} stocks")

print(f"\nOverall average: +1% comes first {rdf['up_first_1pct'].mean():.0f}% of weeks")
print(f"Overall average: +2% comes first {rdf['up_first_2pct'].mean():.0f}% of weeks")

# Worst (go down first)
print(f"\n=== WORST: Stocks that go DOWN first ===")
worst = rdf[rdf["up_first_1pct"] <= 40].sort_values("up_first_1pct")
print(f"{len(worst)} stocks go down first 60%+ of weeks:")
for _, r in worst.head(15).iterrows():
    print(f"  {r['symbol']:<12s} +1% first only {r['up_first_1pct']:.0f}% (goes DOWN first {100-r['up_first_1pct']:.0f}%)")
