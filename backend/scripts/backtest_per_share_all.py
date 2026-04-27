#!/usr/bin/env python3
"""
Per-share analysis across ALL hypotheses.
Find which stock + strategy combo gives 95%+ win rate regardless of DSEX.
Minimum 10 events required for reliability.
"""

import warnings; warnings.filterwarnings("ignore")
import sys, os
import pandas as pd
import numpy as np
import psycopg2

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from config import DATABASE_URL
from ta.momentum import RSIIndicator
from ta.volume import ChaikinMoneyFlowIndicator, OnBalanceVolumeIndicator
from ta.trend import ADXIndicator, EMAIndicator

conn = psycopg2.connect(DATABASE_URL)

print("Loading 5 years of data...")
df = pd.read_sql(
    "SELECT dp.symbol, dp.date, dp.open, dp.high, dp.low, dp.close, dp.volume, "
    "f.category "
    "FROM daily_prices dp JOIN fundamentals f ON dp.symbol = f.symbol "
    "WHERE dp.date >= '2021-01-01' AND dp.close > 0 AND f.category IN ('A','B') "
    "AND dp.volume > 3000 ORDER BY dp.symbol, dp.date",
    conn,
)
df["date"] = pd.to_datetime(df["date"])
conn.close()
print(f"Loaded {len(df)} rows, {df.symbol.nunique()} stocks\n")

# Results: stock x strategy x win rate
all_combos = []

stock_count = 0
for symbol, sdf in df.groupby("symbol"):
    if len(sdf) < 200:
        continue
    stock_count += 1
    sdf = sdf.sort_values("date").reset_index(drop=True)
    close = sdf["close"]
    high = sdf["high"]
    low = sdf["low"]
    open_ = sdf["open"]
    volume = sdf["volume"].astype(float)

    rsi = RSIIndicator(close, window=14).rsi()
    obv = OnBalanceVolumeIndicator(close, volume).on_balance_volume()
    vol_avg = volume.rolling(20).mean()
    ema9 = EMAIndicator(close, window=9).ema_indicator()
    ema21 = EMAIndicator(close, window=21).ema_indicator()

    obv_slope = obv.rolling(10).apply(lambda x: np.polyfit(range(len(x)), x, 1)[0] if len(x) > 1 else 0, raw=True)
    price_slope = close.rolling(10).apply(lambda x: np.polyfit(range(len(x)), x, 1)[0] if len(x) > 1 else 0, raw=True)

    cat = sdf["category"].iloc[0]

    # Track events per strategy
    strats = {
        "RSI<25": [], "RSI<30": [], "RSI<35": [],
        "3+RedDays": [], "5dDrop>7%": [], "5dDrop>10%": [],
        "OBVDiv": [], "OBVDiv+RSI<40": [],
        "AtSupport+RSI<40": [], "AtSupport+RSI<35": [],
        "Hammer20dLow": [], "BullishEngulf": [],
        "VolSpike+Green": [],
        "LiqGrab": [],
        "RSI<30+3Red": [],
        "20dLow+Green": [],
    }

    for i in range(60, len(sdf) - 10):
        c = close.iloc[i]
        o = open_.iloc[i]
        h = high.iloc[i]
        l = low.iloc[i]
        v = volume.iloc[i]
        c_prev = close.iloc[i - 1]

        rsi_v = rsi.iloc[i] if pd.notna(rsi.iloc[i]) else 50
        va = vol_avg.iloc[i] if pd.notna(vol_avg.iloc[i]) else v
        e9 = ema9.iloc[i] if pd.notna(ema9.iloc[i]) else 0
        e21 = ema21.iloc[i] if pd.notna(ema21.iloc[i]) else 0
        obv_s = obv_slope.iloc[i] if pd.notna(obv_slope.iloc[i]) else 0
        price_s = price_slope.iloc[i] if pd.notna(price_slope.iloc[i]) else 0

        green = c > o
        body = abs(c - o)
        total_range = h - l
        lower_shadow = min(c, o) - l
        vol_ratio = v / va if va > 0 else 1

        # 5d return and max high day 1-5 (including shadows)
        ret_5d = (close.iloc[i + 5] - c) / c * 100 if i + 5 < len(sdf) else None
        max_high_5d = (high.iloc[i + 1:i + 6].max() - c) / c * 100 if i + 5 < len(sdf) else None
        if ret_5d is None:
            continue

        # Consecutive red days
        red_days = 0
        for j in range(1, min(6, i)):
            if close.iloc[i - j] < close.iloc[i - j - 1]:
                red_days += 1
            else:
                break

        chg_5d = (c - close.iloc[i - 5]) / close.iloc[i - 5] * 100 if i >= 5 else 0

        # Support
        swing_lows = []
        for j in range(max(2, i - 40), i - 2):
            if low.iloc[j] == min(low.iloc[max(0, j - 2):j + 3]):
                swing_lows.append(float(low.iloc[j]))
        at_support = False
        if swing_lows:
            for sl in swing_lows:
                if abs(l - sl) / sl * 100 < 2:
                    at_support = True
                    break

        hammer = lower_shadow > body * 2 if body > 0 and total_range > 0 else False
        bullish_engulf = (c > o and c_prev < open_.iloc[i - 1] and body > abs(c_prev - open_.iloc[i - 1])
                         and o < c_prev and c > open_.iloc[i - 1]) if i > 0 else False

        # 20-day low
        at_20d_low = l <= low.iloc[max(0, i - 20):i].min() * 1.01

        # Liquidity grab
        recent_low = low.iloc[max(0, i - 5):i].min()
        liq_grab = l < recent_low * 0.99 and c > recent_low

        entry = (ret_5d, max_high_5d)

        # === Strategies ===
        if rsi_v < 25:
            strats["RSI<25"].append(entry)
        if rsi_v < 30:
            strats["RSI<30"].append(entry)
        if rsi_v < 35:
            strats["RSI<35"].append(entry)
        if red_days >= 3:
            strats["3+RedDays"].append(entry)
        if chg_5d < -7:
            strats["5dDrop>7%"].append(entry)
        if chg_5d < -10:
            strats["5dDrop>10%"].append(entry)
        if price_s < 0 and obv_s > 0:
            strats["OBVDiv"].append(entry)
            if rsi_v < 40:
                strats["OBVDiv+RSI<40"].append(entry)
        if at_support and rsi_v < 40:
            strats["AtSupport+RSI<40"].append(entry)
        if at_support and rsi_v < 35:
            strats["AtSupport+RSI<35"].append(entry)
        if hammer and at_20d_low:
            strats["Hammer20dLow"].append(entry)
        if bullish_engulf:
            strats["BullishEngulf"].append(entry)
        if vol_ratio > 2.5 and green:
            strats["VolSpike+Green"].append(entry)
        if liq_grab:
            strats["LiqGrab"].append(entry)
        if rsi_v < 30 and red_days >= 3:
            strats["RSI<30+3Red"].append(entry)
        if at_20d_low and green:
            strats["20dLow+Green"].append(entry)

    for strat_name, trades in strats.items():
        if len(trades) < 8:  # minimum events
            continue
        rets = [t[0] for t in trades]
        maxs = [t[1] for t in trades if t[1] is not None]
        wr_close = sum(1 for r in rets if r > 0) / len(rets) * 100
        wr_high_1pct = sum(1 for m in maxs if m >= 1) / len(maxs) * 100 if maxs else 0
        wr_high_3pct = sum(1 for m in maxs if m >= 3) / len(maxs) * 100 if maxs else 0
        avg_ret = np.mean(rets)
        avg_max = np.mean(maxs) if maxs else 0

        all_combos.append({
            "symbol": symbol,
            "category": cat,
            "strategy": strat_name,
            "events": len(trades),
            "wr_close_5d": round(wr_close, 0),
            "wr_high_1pct": round(wr_high_1pct, 0),
            "wr_high_3pct": round(wr_high_3pct, 0),
            "avg_ret": round(avg_ret, 2),
            "avg_max_high": round(avg_max, 2),
        })

    if stock_count % 50 == 0:
        print(f"  Processed {stock_count} stocks...")

cdf = pd.DataFrame(all_combos)
print(f"\nProcessed {stock_count} stocks, {len(cdf)} stock×strategy combos\n")

# === 95%+ WIN RATE (close-based) ===
print("=" * 85)
print("95%+ WIN RATE (5-day close return > 0) — ANY DSEX, minimum 10 events")
print("=" * 85)
top95 = cdf[(cdf["wr_close_5d"] >= 95) & (cdf["events"] >= 10)].sort_values(
    ["wr_close_5d", "events"], ascending=[False, False])
if len(top95) > 0:
    print(f"\n{'Stock':<12s} {'Strategy':<20s} {'Events':>7s} {'CloseWR':>8s} {'AvgRet':>7s} {'Hit1%':>6s} {'Hit3%':>6s} {'AvgMax':>7s}")
    print("-" * 80)
    for _, r in top95.iterrows():
        print(f"{r['symbol']:<12s} {r['strategy']:<20s} {r['events']:7d} {r['wr_close_5d']:7.0f}% {r['avg_ret']:+6.2f}% {r['wr_high_1pct']:5.0f}% {r['wr_high_3pct']:5.0f}% {r['avg_max_high']:+6.2f}%")
else:
    print("  None found with close-based 95%+ on 10+ events")

# === 90%+ WIN RATE ===
print(f"\n{'=' * 85}")
print("90%+ WIN RATE — minimum 10 events")
print("=" * 85)
top90 = cdf[(cdf["wr_close_5d"] >= 90) & (cdf["events"] >= 10)].sort_values(
    ["wr_close_5d", "events"], ascending=[False, False])
if len(top90) > 0:
    print(f"\n{'Stock':<12s} {'Strategy':<20s} {'Events':>7s} {'CloseWR':>8s} {'AvgRet':>7s} {'Hit3%':>6s} {'Cat':>4s}")
    print("-" * 65)
    for _, r in top90.head(40).iterrows():
        print(f"{r['symbol']:<12s} {r['strategy']:<20s} {r['events']:7d} {r['wr_close_5d']:7.0f}% {r['avg_ret']:+6.2f}% {r['wr_high_3pct']:5.0f}% {r['category']:>4s}")

# === 95%+ HIGH HITS 3% ===
print(f"\n{'=' * 85}")
print("95%+ chance of hitting +3% intraday within 5 days — minimum 10 events")
print("=" * 85)
top_high = cdf[(cdf["wr_high_3pct"] >= 95) & (cdf["events"] >= 10)].sort_values(
    ["wr_high_3pct", "events"], ascending=[False, False])
if len(top_high) > 0:
    print(f"\n{'Stock':<12s} {'Strategy':<20s} {'Events':>7s} {'Hit3%':>7s} {'Hit1%':>7s} {'AvgMax':>7s} {'CloseWR':>8s}")
    print("-" * 70)
    for _, r in top_high.head(40).iterrows():
        print(f"{r['symbol']:<12s} {r['strategy']:<20s} {r['events']:7d} {r['wr_high_3pct']:6.0f}% {r['wr_high_1pct']:6.0f}% {r['avg_max_high']:+6.2f}% {r['wr_close_5d']:7.0f}%")

# === STRATEGY SUMMARY across all stocks ===
print(f"\n{'=' * 85}")
print("STRATEGY SUMMARY: Average win rate across all stocks")
print("=" * 85)
strat_summary = cdf.groupby("strategy").agg(
    stocks=("symbol", "nunique"),
    total_events=("events", "sum"),
    avg_wr=("wr_close_5d", "mean"),
    avg_high_3pct=("wr_high_3pct", "mean"),
    avg_ret=("avg_ret", "mean"),
    stocks_90plus=("wr_close_5d", lambda x: (x >= 90).sum()),
).sort_values("avg_wr", ascending=False)

print(f"\n{'Strategy':<20s} {'Stocks':>7s} {'Events':>8s} {'AvgWR':>7s} {'Avg3%Hit':>9s} {'AvgRet':>7s} {'90%+Stocks':>11s}")
print("-" * 75)
for strat, r in strat_summary.iterrows():
    print(f"{strat:<20s} {r['stocks']:7.0f} {r['total_events']:8.0f} {r['avg_wr']:6.0f}% {r['avg_high_3pct']:8.0f}% {r['avg_ret']:+6.2f}% {r['stocks_90plus']:10.0f}")

# === BEST STOCKS (consistent across multiple strategies) ===
print(f"\n{'=' * 85}")
print("MOST CONSISTENT STOCKS: 80%+ win rate in 3+ strategies")
print("=" * 85)
good = cdf[cdf["wr_close_5d"] >= 80]
stock_counts = good.groupby("symbol").agg(
    strategies=("strategy", "count"),
    avg_wr=("wr_close_5d", "mean"),
    total_events=("events", "sum"),
    best_strat=("wr_close_5d", "max"),
).sort_values("strategies", ascending=False)

multi = stock_counts[stock_counts["strategies"] >= 3]
if len(multi) > 0:
    print(f"\n{'Stock':<12s} {'#Strats':>8s} {'AvgWR':>7s} {'BestWR':>7s} {'Events':>8s}")
    print("-" * 45)
    for sym, r in multi.head(25).iterrows():
        print(f"{sym:<12s} {r['strategies']:8.0f} {r['avg_wr']:6.0f}% {r['best_strat']:6.0f}% {r['total_events']:8.0f}")

        # Show which strategies work for this stock
        stock_strats = cdf[(cdf["symbol"] == sym) & (cdf["wr_close_5d"] >= 80)].sort_values("wr_close_5d", ascending=False)
        for _, sr in stock_strats.iterrows():
            print(f"    {sr['strategy']:<20s} {sr['events']:3.0f} events, {sr['wr_close_5d']:.0f}% win")
