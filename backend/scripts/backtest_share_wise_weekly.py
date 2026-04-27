#!/usr/bin/env python3
"""Per-share weekly analysis: Which stocks give 90%+ win rate buying Sunday selling Thursday?"""

import warnings; warnings.filterwarnings("ignore")
import sys, os
import pandas as pd
import numpy as np
import psycopg2

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from config import DATABASE_URL

conn = psycopg2.connect(DATABASE_URL)

print("Loading 5 years...")
df = pd.read_sql(
    "SELECT dp.symbol, dp.date, dp.open, dp.high, dp.low, dp.close, dp.volume, "
    "f.category, f.sector "
    "FROM daily_prices dp JOIN fundamentals f ON dp.symbol = f.symbol "
    "WHERE dp.date >= '2021-01-01' AND dp.close > 0 AND f.category IN ('A','B') "
    "AND dp.volume > 5000 ORDER BY dp.symbol, dp.date",
    conn,
)
df["date"] = pd.to_datetime(df["date"])
df["dow"] = df["date"].dt.dayofweek  # 6=Sun, 0=Mon, 3=Thu
conn.close()

print(f"Loaded {len(df)} rows, {df.symbol.nunique()} stocks\n")

# For each stock, compute: buy Sunday close, sell Thursday close
results = []

for symbol, sdf in df.groupby("symbol"):
    if len(sdf) < 100:
        continue
    sdf = sdf.sort_values("date").reset_index(drop=True)

    cat = sdf["category"].iloc[0]
    sector = sdf["sector"].iloc[0] or "Unknown"

    trades = []

    for i in range(len(sdf)):
        if sdf["dow"].iloc[i] != 6:  # not Sunday
            continue

        buy_price = sdf["close"].iloc[i]
        buy_date = sdf["date"].iloc[i]

        # Find Thursday (3-4 days later)
        for j in range(i + 1, min(i + 6, len(sdf))):
            if sdf["dow"].iloc[j] == 3:  # Thursday
                sell_close = sdf["close"].iloc[j]
                sell_high = sdf["high"].iloc[j]

                # Also get max high Sun-Thu (best possible exit including shadows)
                max_high = sdf["high"].iloc[i + 1:j + 1].max()

                ret_close = (sell_close - buy_price) / buy_price * 100
                ret_high = (max_high - buy_price) / buy_price * 100

                trades.append({
                    "ret_close": ret_close,
                    "ret_high": ret_high,
                    "win_close": ret_close > 0,
                    "win_1pct": ret_close >= 1,
                    "win_high_1pct": ret_high >= 1,
                    "win_high_3pct": ret_high >= 3,
                })
                break

    if len(trades) < 20:
        continue

    tdf = pd.DataFrame(trades)

    results.append({
        "symbol": symbol,
        "category": cat,
        "sector": sector,
        "weeks": len(tdf),
        "close_wr": round(tdf["win_close"].mean() * 100, 0),
        "close_avg": round(tdf["ret_close"].mean(), 2),
        "close_med": round(tdf["ret_close"].median(), 2),
        "pct_1pct": round(tdf["win_1pct"].mean() * 100, 0),
        "high_wr": round((tdf["ret_high"] > 0).mean() * 100, 0),
        "high_1pct": round(tdf["win_high_1pct"].mean() * 100, 0),
        "high_3pct": round(tdf["win_high_3pct"].mean() * 100, 0),
        "high_avg": round(tdf["ret_high"].mean(), 2),
        "worst": round(tdf["ret_close"].min(), 1),
    })

rdf = pd.DataFrame(results)

# === CLOSE-BASED: Buy Sunday close, sell Thursday close ===
print("=" * 80)
print("BUY SUNDAY CLOSE → SELL THURSDAY CLOSE: Per-Stock Win Rate (5 years)")
print("=" * 80)

print(f"\n90%+ Win Rate Stocks (close-based):")
top_close = rdf[rdf["close_wr"] >= 90].sort_values("close_wr", ascending=False)
if len(top_close) > 0:
    print(f"{'Stock':<12s} {'Cat':>3s} {'Weeks':>6s} {'Win%':>6s} {'Avg':>7s} {'Med':>7s} {'>=1%':>5s} {'Worst':>7s} {'Sector':<20s}")
    print("-" * 85)
    for _, r in top_close.iterrows():
        print(f"{r['symbol']:<12s} {r['category']:>3s} {r['weeks']:6.0f} {r['close_wr']:5.0f}% {r['close_avg']:+6.2f}% {r['close_med']:+6.2f}% {r['pct_1pct']:4.0f}% {r['worst']:+6.1f}% {r['sector']:<20s}")
else:
    print("  None found.")

print(f"\n80%+ Win Rate Stocks:")
top80 = rdf[(rdf["close_wr"] >= 80) & (rdf["close_wr"] < 90)].sort_values("close_wr", ascending=False)
if len(top80) > 0:
    print(f"{'Stock':<12s} {'Cat':>3s} {'Weeks':>6s} {'Win%':>6s} {'Avg':>7s} {'Med':>7s} {'>=1%':>5s} {'Worst':>7s}")
    print("-" * 65)
    for _, r in top80.iterrows():
        print(f"{r['symbol']:<12s} {r['category']:>3s} {r['weeks']:6.0f} {r['close_wr']:5.0f}% {r['close_avg']:+6.2f}% {r['close_med']:+6.2f}% {r['pct_1pct']:4.0f}% {r['worst']:+6.1f}%")

print(f"\n70%+ Win Rate (reliable weekly trades):")
top70 = rdf[(rdf["close_wr"] >= 70) & (rdf["close_wr"] < 80)].sort_values("close_wr", ascending=False)
print(f"  {len(top70)} stocks found")
if len(top70) > 0:
    print(f"{'Stock':<12s} {'Cat':>3s} {'Weeks':>6s} {'Win%':>6s} {'Avg':>7s} {'>=1%':>5s}")
    print("-" * 45)
    for _, r in top70.head(20).iterrows():
        print(f"{r['symbol']:<12s} {r['category']:>3s} {r['weeks']:6.0f} {r['close_wr']:5.0f}% {r['close_avg']:+6.2f}% {r['pct_1pct']:4.0f}%")

# === HIGH-BASED: At any point Sun-Thu the price hits +1% or +3% ===
print(f"\n{'=' * 80}")
print("HIGH-BASED: Stocks where price hits +1% at SOME POINT during the week")
print("(You just need to catch the intraday spike)")
print("=" * 80)

print(f"\n90%+ of weeks hit +1% intraday:")
top_high = rdf[rdf["high_1pct"] >= 90].sort_values("high_1pct", ascending=False)
if len(top_high) > 0:
    print(f"{'Stock':<12s} {'Cat':>3s} {'Weeks':>6s} {'Hit 1%':>7s} {'Hit 3%':>7s} {'Avg Max':>8s} {'Close WR':>9s}")
    print("-" * 65)
    for _, r in top_high.iterrows():
        print(f"{r['symbol']:<12s} {r['category']:>3s} {r['weeks']:6.0f} {r['high_1pct']:6.0f}% {r['high_3pct']:6.0f}% {r['high_avg']:+7.2f}% {r['close_wr']:8.0f}%")
else:
    print("  None found with 90%+")

print(f"\n80%+ of weeks hit +1% intraday:")
top_h80 = rdf[(rdf["high_1pct"] >= 80) & (rdf["high_1pct"] < 90)].sort_values("high_1pct", ascending=False)
if len(top_h80) > 0:
    print(f"{'Stock':<12s} {'Cat':>3s} {'Weeks':>6s} {'Hit 1%':>7s} {'Hit 3%':>7s} {'Avg Max':>8s}")
    print("-" * 55)
    for _, r in top_h80.head(20).iterrows():
        print(f"{r['symbol']:<12s} {r['category']:>3s} {r['weeks']:6.0f} {r['high_1pct']:6.0f}% {r['high_3pct']:6.0f}% {r['high_avg']:+7.2f}%")

# === WORST STOCKS (avoid these) ===
print(f"\n{'=' * 80}")
print("WORST STOCKS: Lowest win rate (avoid for weekly trading)")
print("=" * 80)
worst = rdf.sort_values("close_wr").head(15)
print(f"{'Stock':<12s} {'Weeks':>6s} {'Win%':>6s} {'Avg':>7s}")
print("-" * 35)
for _, r in worst.iterrows():
    print(f"{r['symbol']:<12s} {r['weeks']:6.0f} {r['close_wr']:5.0f}% {r['close_avg']:+6.2f}%")

# === SUMMARY STATS ===
print(f"\n{'=' * 80}")
print("DISTRIBUTION OF WIN RATES ACROSS ALL STOCKS")
print("=" * 80)
for thresh in [90, 80, 70, 60, 50, 40]:
    count = (rdf["close_wr"] >= thresh).sum()
    count_h = (rdf["high_1pct"] >= thresh).sum()
    print(f"  Close WR >= {thresh}%: {count:3d} stocks | High hits +1% >= {thresh}%: {count_h:3d} stocks")

print(f"\nOverall avg close WR: {rdf['close_wr'].mean():.0f}%")
print(f"Overall avg high +1% hit rate: {rdf['high_1pct'].mean():.0f}%")
