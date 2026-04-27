#!/usr/bin/env python3
"""Prove RSI<30 + DSEX weak with actual trade examples day-by-day."""

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
    "SELECT dp.symbol, dp.date, dp.close, dp.high, dp.volume "
    "FROM daily_prices dp JOIN fundamentals f ON dp.symbol = f.symbol "
    "WHERE dp.date >= '2025-10-01' AND dp.close > 0 AND f.category IN ('A','B') "
    "AND dp.volume > 10000 ORDER BY dp.symbol, dp.date",
    conn,
)
df["date"] = pd.to_datetime(df["date"])

dsex = pd.read_sql(
    "SELECT date, dsex_index as close FROM dsex_history WHERE dsex_index > 0 ORDER BY date", conn)
dsex["date"] = pd.to_datetime(dsex["date"])
dsex = dsex.set_index("date")
dsex["sma20"] = dsex["close"].rolling(20).mean()
conn.close()

results = []
for symbol, sdf in df.groupby("symbol"):
    if len(sdf) < 30:
        continue
    sdf = sdf.sort_values("date").reset_index(drop=True)
    close = sdf["close"]
    high = sdf["high"]
    rsi = RSIIndicator(close, window=14).rsi()

    for i in range(20, len(sdf) - 10):
        rsi_v = rsi.iloc[i]
        if pd.isna(rsi_v) or rsi_v >= 30:
            continue
        dt = sdf["date"].iloc[i]
        c = close.iloc[i]

        if len(dsex.loc[:dt]) == 0:
            continue
        dr = dsex.loc[:dt].iloc[-1]
        if pd.isna(dr["sma20"]) or dr["close"] >= dr["sma20"]:
            continue

        ret_5d = (close.iloc[i + 5] - c) / c * 100 if i + 5 < len(sdf) else None

        day_prices = []
        for d in range(1, 6):
            if i + d < len(sdf):
                day_prices.append(float(close.iloc[i + d]))

        results.append({
            "symbol": symbol,
            "date": str(dt.date()),
            "entry": float(c),
            "rsi": round(float(rsi_v), 1),
            "dsex": round(float(dr["close"]), 1),
            "dsex_sma20": round(float(dr["sma20"]), 1),
            "ret_5d": round(ret_5d, 1) if ret_5d else None,
            "day_prices": day_prices,
        })

rdf = pd.DataFrame(results).drop_duplicates(subset=["symbol", "date"])
rdf = rdf[rdf["ret_5d"].notna()]

print("=" * 85)
print("RSI<30 + DSEX WEAK: PROOF WITH EVERY TRADE")
print("=" * 85)
print()
print("HOW IT WORKS:")
print("  Step 1: Check DSEX index. Is it BELOW its 20-day moving average?")
print("          If YES = market is weak, falling, scared sellers everywhere")
print("          If NO = don't trade, wait")
print()
print("  Step 2: Find stocks with RSI below 30")
print("          RSI 30 means the stock dropped so much it's deeply oversold")
print("          Like a rubber band stretched too far — it snaps back")
print()
print("  Step 3: BUY that stock. Hold 5 days. Sell.")
print("          In a weak market, oversold stocks bounce FAST")
print("          Because panic sellers are done, bargain hunters step in")
print()

total = len(rdf)
wins = (rdf["ret_5d"] > 0).sum()
losses = total - wins
print(f"RESULTS: {wins} wins out of {total} trades = {wins/total*100:.0f}% win rate")
print(f"Average return: {rdf['ret_5d'].mean():+.1f}% in 5 days")
print(f"Losers: {losses} ({losses/total*100:.0f}%)")
print()

# Show trades grouped by stock for clarity
print("EVERY SINGLE TRADE (day-by-day prices after buying):")
print("-" * 85)

header = "Date         Stock        Entry   RSI  DSEX     D1      D2      D3      D4      D5    5D Ret  W/L"
print(header)
print("-" * 85)

for _, r in rdf.sort_values("date").iterrows():
    dp = r["day_prices"]
    entry = r["entry"]

    cols = [f"{r['date']:12s}", f"{r['symbol']:12s}", f"{entry:7.1f}", f"{r['rsi']:5.1f}",
            f"{r['dsex']:7.1f}"]

    for d in range(5):
        if d < len(dp):
            p = dp[d]
            chg = (p - entry) / entry * 100
            cols.append(f"{p:6.1f}")
        else:
            cols.append("     ?")

    ret = r["ret_5d"]
    wl = "WIN" if ret > 0 else "LOSS"
    cols.append(f"{ret:+5.1f}%")
    cols.append(f"  {wl}")

    print("  ".join(cols))

# Summary by stock
print()
print("=" * 85)
print("PER-STOCK SUMMARY")
print("=" * 85)
stock_summary = rdf.groupby("symbol").agg(
    trades=("ret_5d", "count"),
    wins=("ret_5d", lambda x: (x > 0).sum()),
    avg_ret=("ret_5d", "mean"),
).reset_index()
stock_summary["win_pct"] = (stock_summary["wins"] / stock_summary["trades"] * 100).round(0)
stock_summary = stock_summary.sort_values("trades", ascending=False)

print(f"\n{'Stock':<12s} {'Trades':>7s} {'Wins':>6s} {'Win%':>6s} {'Avg Ret':>8s}")
print("-" * 45)
for _, r in stock_summary.head(30).iterrows():
    print(f"{r['symbol']:<12s} {r['trades']:7.0f} {r['wins']:6.0f} {r['win_pct']:5.0f}% {r['avg_ret']:+7.1f}%")

print(f"\nTotal unique stocks: {len(stock_summary)}")
print(f"Stocks with 100% win rate: {(stock_summary['win_pct'] == 100).sum()}")
print(f"Stocks with 0% win rate: {(stock_summary['win_pct'] == 0).sum()}")
