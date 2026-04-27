#!/usr/bin/env python3
"""
Test: DSEX crash day → buy RSI<30 stocks → what happens?
Also test using HIGH/LOW (shadows) not just close for returns.

Gap fill = price fills back to a level including intraday wicks.
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

df = pd.read_sql(
    "SELECT dp.symbol, dp.date, dp.open, dp.high, dp.low, dp.close, dp.volume "
    "FROM daily_prices dp JOIN fundamentals f ON dp.symbol = f.symbol "
    "WHERE dp.date >= '2021-01-01' AND dp.close > 0 AND f.category IN ('A','B') "
    "AND dp.volume > 5000 ORDER BY dp.symbol, dp.date",
    conn,
)
df["date"] = pd.to_datetime(df["date"])

dsex = pd.read_sql(
    "SELECT date, dsex_index as close FROM dsex_history WHERE dsex_index > 0 ORDER BY date", conn)
dsex["date"] = pd.to_datetime(dsex["date"])
dsex = dsex.set_index("date")
dsex["chg"] = dsex["close"].pct_change() * 100
dsex["sma20"] = dsex["close"].rolling(20).mean()
dsex["below_sma20"] = dsex["close"] < dsex["sma20"]
conn.close()

print(f"Loaded {len(df)} rows, {df.symbol.nunique()} stocks")
print(f"DSEX: {len(dsex)} days\n")

# Find DSEX crash days
crash_days = dsex[(dsex["chg"] < -2) & (dsex["below_sma20"] == True)].copy()
print(f"DSEX crash days (>2% drop + below SMA20): {len(crash_days)}")
print()

# For each crash day, find RSI<30 stocks and track using CLOSE and HIGH (shadows)
results = []

for crash_date, crash_row in crash_days.iterrows():
    dsex_drop = crash_row["chg"]

    # Get next trading day for buying
    future_dsex = dsex.loc[crash_date:].iloc[1:2]
    if len(future_dsex) == 0:
        continue
    buy_date = future_dsex.index[0]

    for symbol, sdf in df.groupby("symbol"):
        if len(sdf) < 30:
            continue
        sdf = sdf.sort_values("date").reset_index(drop=True)
        close = sdf["close"]
        high = sdf["high"]
        low = sdf["low"]
        open_ = sdf["open"]

        # Find the crash day index in this stock's data
        crash_idx = sdf[sdf["date"] == crash_date].index
        if len(crash_idx) == 0:
            continue
        ci = crash_idx[0]

        if ci < 20 or ci + 10 >= len(sdf):
            continue

        rsi = RSIIndicator(close.iloc[:ci + 1], window=14).rsi()
        if len(rsi) == 0 or pd.isna(rsi.iloc[-1]):
            continue
        rsi_v = float(rsi.iloc[-1])

        if rsi_v >= 35:  # test RSI<35 for more events
            continue

        entry_price = float(close.iloc[ci])  # buy at close of crash day

        # CLOSE-based returns (what we tested before)
        close_rets = {}
        for d in [1, 2, 3, 5, 10]:
            if ci + d < len(sdf):
                close_rets[f"close_ret_{d}d"] = (close.iloc[ci + d] - entry_price) / entry_price * 100

        # HIGH-based returns (includes shadows/wicks — the actual max you could sell at)
        high_rets = {}
        for d in [1, 2, 3, 5, 10]:
            if ci + d < len(sdf):
                # Max HIGH in day 1 to day d (the peak you could have sold at intraday)
                max_high = high.iloc[ci + 1:ci + d + 1].max()
                high_rets[f"high_ret_{d}d"] = (max_high - entry_price) / entry_price * 100

        # LOW-based (worst case — the worst dip including shadows)
        low_rets = {}
        for d in [1, 2, 3]:
            if ci + d < len(sdf):
                min_low = low.iloc[ci + 1:ci + d + 1].min()
                low_rets[f"low_ret_{d}d"] = (min_low - entry_price) / entry_price * 100

        results.append({
            "symbol": symbol,
            "crash_date": str(crash_date.date()),
            "dsex_drop": round(dsex_drop, 1),
            "entry": entry_price,
            "rsi": round(rsi_v, 1),
            **close_rets,
            **high_rets,
            **low_rets,
        })

rdf = pd.DataFrame(results)
if len(rdf) == 0:
    print("No events found")
    exit()

rdf = rdf.drop_duplicates(subset=["symbol", "crash_date"])
print(f"Total trades: {len(rdf)}\n")

sep = "=" * 70

# === CLOSE vs HIGH comparison ===
print(sep)
print("CLOSE-BASED RETURNS (selling at day's close price)")
print(sep)
for d in [1, 2, 3, 5, 10]:
    col = f"close_ret_{d}d"
    if col not in rdf.columns:
        continue
    v = rdf[col].dropna()
    wr = (v > 0).sum() / len(v) * 100
    pct3 = (v >= 3).sum() / len(v) * 100
    print(f"  Day {d:2d}: {len(v)} trades, Win {wr:.0f}%, Avg {v.mean():+.1f}%, >=3%: {pct3:.0f}%")

print(f"\n{sep}")
print("HIGH-BASED RETURNS (selling at day's HIGH — includes shadows/wicks)")
print("This is the MAX you could have gotten if you sold at the peak")
print(sep)
for d in [1, 2, 3, 5, 10]:
    col = f"high_ret_{d}d"
    if col not in rdf.columns:
        continue
    v = rdf[col].dropna()
    wr = (v > 0).sum() / len(v) * 100
    pct3 = (v >= 3).sum() / len(v) * 100
    pct5 = (v >= 5).sum() / len(v) * 100
    print(f"  Day {d:2d}: {len(v)} trades, >0%: {wr:.0f}%, >=3%: {pct3:.0f}%, >=5%: {pct5:.0f}%, Avg max: {v.mean():+.1f}%")

print(f"\n{sep}")
print("WORST CASE: Max drawdown including shadows (how much it dips BEFORE bouncing)")
print(sep)
for d in [1, 2, 3]:
    col = f"low_ret_{d}d"
    if col not in rdf.columns:
        continue
    v = rdf[col].dropna()
    print(f"  Day {d:2d}: Avg worst dip: {v.mean():+.1f}%, Worst ever: {v.min():+.1f}%, 90th pctile: {v.quantile(0.1):+.1f}%")

# === RSI SPLIT ===
print(f"\n{sep}")
print("RSI SPLIT: Does lower RSI = better result?")
print(sep)
for rsi_lo, rsi_hi in [(0, 20), (20, 25), (25, 30), (30, 35)]:
    sub = rdf[(rdf["rsi"] >= rsi_lo) & (rdf["rsi"] < rsi_hi)]
    if len(sub) < 5:
        continue
    c5 = sub["close_ret_5d"].dropna()
    h5 = sub["high_ret_5d"].dropna()
    wr = (c5 > 0).sum() / len(c5) * 100 if len(c5) > 0 else 0
    print(f"  RSI {rsi_lo}-{rsi_hi}: {len(sub)} trades, Close 5d win: {wr:.0f}% avg: {c5.mean():+.1f}%, High 5d avg: {h5.mean():+.1f}%")

# === DSEX DROP SIZE ===
print(f"\n{sep}")
print("DSEX DROP SIZE: Bigger crash = better bounce?")
print(sep)
for drop_lo, drop_hi, label in [(-3, -2, "2-3% drop"), (-5, -3, "3-5% drop"), (-99, -5, "5%+ crash")]:
    sub = rdf[(rdf["dsex_drop"] >= drop_lo) & (rdf["dsex_drop"] < drop_hi)]
    if len(sub) < 5:
        continue
    c5 = sub["close_ret_5d"].dropna()
    h5 = sub["high_ret_5d"].dropna()
    wr = (c5 > 0).sum() / len(c5) * 100 if len(c5) > 0 else 0
    print(f"  {label}: {len(sub)} trades, Close 5d win: {wr:.0f}% avg: {c5.mean():+.1f}%, High 5d max: {h5.mean():+.1f}%")

# === SHOW ACTUAL TRADES ===
print(f"\n{sep}")
print("RECENT TRADES WITH DAY-BY-DAY (last 20)")
print(sep)
recent = rdf.sort_values("crash_date", ascending=False).head(20)
for _, r in recent.iterrows():
    c5 = r.get("close_ret_5d", "?")
    h5 = r.get("high_ret_5d", "?")
    low2 = r.get("low_ret_2d", "?")
    c5_str = f"{c5:+.1f}%" if isinstance(c5, float) else "?"
    h5_str = f"{h5:+.1f}%" if isinstance(h5, float) else "?"
    low2_str = f"{low2:+.1f}%" if isinstance(low2, float) else "?"
    wl = "WIN" if isinstance(c5, float) and c5 > 0 else "LOSS" if isinstance(c5, float) else "?"
    print(f"  {r['crash_date']} {r['symbol']:12s} Entry={r['entry']:7.1f} RSI={r['rsi']:4.1f} DSEX drop={r['dsex_drop']:+.1f}% → Close 5d:{c5_str} High 5d:{h5_str} Worst 2d:{low2_str} {wl}")
