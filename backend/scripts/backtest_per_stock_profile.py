#!/usr/bin/env python3
"""
Per-stock strategy profiling.
For EACH A+B stock, test ALL hypotheses and find which works best.
WIN = Close +2%+ on Day 3, 4, or 5.
Results for: Full period (5yr) AND Last 6 months AND Last 2 months.
"""

import warnings; warnings.filterwarnings("ignore")
import sys, os
import pandas as pd
import numpy as np
import psycopg2
import json

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from config import DATABASE_URL
from ta.momentum import RSIIndicator
from ta.volume import ChaikinMoneyFlowIndicator, OnBalanceVolumeIndicator
from ta.trend import EMAIndicator

conn = psycopg2.connect(DATABASE_URL)

print("Loading data...")
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

STRATEGIES = [
    "RSI<30", "RSI<35", "RSI<40",
    "3+RedDays", "5dDrop>5%", "5dDrop>10%",
    "LiqGrab", "RSI<30+LiqGrab",
    "OBVDiv", "OBVDiv+RSI<40",
    "ChoCh+BOS", "ChoCh+BOS+CMF>0",
    "Hammer", "Hammer+RSI<40",
    "Engulfing",
    "FVG", "FVG+Green",
    "52wHigh", "52wHigh+CMF",
    "BB<0", "BB<0+RSI<35",
    "VolSpike+Green",
    "Support3T+RSI<40",
]


def real_win(close, entry_price, idx):
    if idx + 5 >= len(close):
        return None
    for d in [3, 4, 5]:
        if (close.iloc[idx + d] - entry_price) / entry_price * 100 >= 2:
            return True
    return False


all_stock_profiles = []
stock_count = 0

for symbol, sdf in df.groupby("symbol"):
    if len(sdf) < 100:
        continue
    stock_count += 1
    sdf = sdf.sort_values("date").reset_index(drop=True)
    close = sdf["close"]
    high = sdf["high"]
    low = sdf["low"]
    open_ = sdf["open"]
    volume = sdf["volume"].astype(float)

    rsi = RSIIndicator(close, window=14).rsi()
    cmf = ChaikinMoneyFlowIndicator(high, low, close, volume, window=20).chaikin_money_flow()
    obv = OnBalanceVolumeIndicator(close, volume).on_balance_volume()
    ema9 = EMAIndicator(close, window=9).ema_indicator()
    ema21 = EMAIndicator(close, window=21).ema_indicator()
    vol_avg = volume.rolling(20).mean()

    obv_slope = obv.rolling(10).apply(lambda x: np.polyfit(range(len(x)), x, 1)[0] if len(x) > 1 else 0, raw=True)
    price_slope = close.rolling(10).apply(lambda x: np.polyfit(range(len(x)), x, 1)[0] if len(x) > 1 else 0, raw=True)

    bb_pct_vals = close.rolling(20).mean()
    bb_std = close.rolling(20).std()
    bb_upper = bb_pct_vals + 2 * bb_std
    bb_lower = bb_pct_vals - 2 * bb_std
    bb_pct = (close - bb_lower) / (bb_upper - bb_lower)

    cat = sdf["category"].iloc[0]

    # Swing points for ChoCh/BOS
    swing_highs = []
    swing_lows = []
    for i in range(3, len(sdf) - 3):
        if high.iloc[i] == max(high.iloc[i-3:i+4]):
            swing_highs.append((i, float(high.iloc[i])))
        if low.iloc[i] == min(low.iloc[i-3:i+4]):
            swing_lows.append((i, float(low.iloc[i])))

    # Collect events per strategy per time period
    strat_events = {s: {"all": [], "6m": [], "2m": []} for s in STRATEGIES}

    cutoff_6m = pd.Timestamp("2025-10-01")
    cutoff_2m = pd.Timestamp("2026-02-01")

    for i in range(60, len(sdf) - 5):
        c = close.iloc[i]
        o = open_.iloc[i]
        h = high.iloc[i]
        l = low.iloc[i]
        v = volume.iloc[i]
        dt = sdf["date"].iloc[i]
        c_prev = close.iloc[i-1]

        rsi_v = rsi.iloc[i] if pd.notna(rsi.iloc[i]) else 50
        cmf_v = cmf.iloc[i] if pd.notna(cmf.iloc[i]) else 0
        va = vol_avg.iloc[i] if pd.notna(vol_avg.iloc[i]) else v
        obv_s = obv_slope.iloc[i] if pd.notna(obv_slope.iloc[i]) else 0
        price_s = price_slope.iloc[i] if pd.notna(price_slope.iloc[i]) else 0
        bb_v = bb_pct.iloc[i] if pd.notna(bb_pct.iloc[i]) else 0.5

        green = c > o
        body = abs(c - o)
        total_range = h - l
        lower_shadow = min(c, o) - l
        vol_ratio = v / va if va > 0 else 1

        red_days = 0
        for j in range(1, min(6, i)):
            if close.iloc[i-j] < close.iloc[i-j-1]:
                red_days += 1
            else:
                break

        chg_5d = (c - close.iloc[i-5]) / close.iloc[i-5] * 100 if i >= 5 else 0

        # Support
        at_support = False
        for si_idx, sl_p in swing_lows:
            if si_idx >= i or si_idx < i - 60:
                continue
            count = sum(1 for s2, p2 in swing_lows if abs(p2 - sl_p) / sl_p * 100 < 1.5 and s2 < i)
            if abs(l - sl_p) / sl_p * 100 < 2 and count >= 3:
                at_support = True
                break

        hammer = lower_shadow > body * 2 if body > 0 and total_range > 0 else False
        engulf = (c > o and c_prev < open_.iloc[i-1] and body > abs(c_prev - open_.iloc[i-1])) if i > 0 else False

        recent_low = low.iloc[max(0,i-5):i].min()
        liq_grab = l < recent_low * 0.99 and c > recent_low

        obv_div = price_s < 0 and obv_s > 0

        # FVG
        has_fvg = False
        if i >= 3:
            for j in range(i-15, i-2):
                if j >= 2 and high.iloc[j-2] < low.iloc[j]:
                    if l <= low.iloc[j] and c >= high.iloc[j-2]:
                        has_fvg = True
                        break

        # ChoCh + BOS
        choch_bos = False
        choch_bos_cmf = False
        recent_sh = [(idx, p) for idx, p in swing_highs if i-30 < idx < i]
        recent_sl = [(idx, p) for idx, p in swing_lows if i-30 < idx < i]
        if len(recent_sh) >= 2 and len(recent_sl) >= 2:
            if recent_sl[-1][1] < recent_sl[-2][1]:
                lh_p = recent_sh[-1][1]
                if c > lh_p and h > recent_sh[-1][1]:
                    choch_bos = True
                    if cmf_v > 0:
                        choch_bos_cmf = True

        # 52w high
        h52w = high.iloc[max(0,i-252):i].max()
        at_52w = c > h52w and c_prev <= h52w

        w = real_win(close, c, i)
        if w is None:
            continue

        periods = ["all"]
        if dt >= cutoff_6m:
            periods.append("6m")
        if dt >= cutoff_2m:
            periods.append("2m")

        def record(strat):
            for p in periods:
                strat_events[strat][p].append(w)

        if rsi_v < 30: record("RSI<30")
        if rsi_v < 35: record("RSI<35")
        if rsi_v < 40: record("RSI<40")
        if red_days >= 3: record("3+RedDays")
        if chg_5d < -5: record("5dDrop>5%")
        if chg_5d < -10: record("5dDrop>10%")
        if liq_grab: record("LiqGrab")
        if liq_grab and rsi_v < 30: record("RSI<30+LiqGrab")
        if obv_div: record("OBVDiv")
        if obv_div and rsi_v < 40: record("OBVDiv+RSI<40")
        if choch_bos: record("ChoCh+BOS")
        if choch_bos_cmf: record("ChoCh+BOS+CMF>0")
        if hammer: record("Hammer")
        if hammer and rsi_v < 40: record("Hammer+RSI<40")
        if engulf: record("Engulfing")
        if has_fvg: record("FVG")
        if has_fvg and green: record("FVG+Green")
        if at_52w: record("52wHigh")
        if at_52w and cmf_v > 0: record("52wHigh+CMF")
        if bb_v < 0: record("BB<0")
        if bb_v < 0 and rsi_v < 35: record("BB<0+RSI<35")
        if vol_ratio > 2 and green: record("VolSpike+Green")
        if at_support and rsi_v < 40: record("Support3T+RSI<40")

    # Build profile for this stock
    profile = {"symbol": symbol, "category": cat}
    best_strat_all = None
    best_wr_all = 0

    for strat in STRATEGIES:
        for period in ["all", "6m", "2m"]:
            evts = strat_events[strat][period]
            if len(evts) >= 3:
                wr = sum(evts) / len(evts) * 100
                profile[f"{strat}_{period}_n"] = len(evts)
                profile[f"{strat}_{period}_wr"] = round(wr, 0)
                if period == "all" and wr > best_wr_all and len(evts) >= 5:
                    best_wr_all = wr
                    best_strat_all = strat

    profile["best_strategy"] = best_strat_all
    profile["best_wr"] = best_wr_all
    all_stock_profiles.append(profile)

    if stock_count % 50 == 0:
        print(f"  Processed {stock_count} stocks...")

print(f"\nProcessed {stock_count} stocks\n")

# === OUTPUT ===
pdf = pd.DataFrame(all_stock_profiles)

print("=" * 80)
print("PER-STOCK STRATEGY PROFILE — Best strategy for each stock")
print("WIN = Close +2%+ on Day 3/4/5 | NO DSEX")
print("=" * 80)

# Stocks with 60%+ win rate on any strategy (full period)
print(f"\n### STOCKS WITH 60%+ WIN RATE (full period, min 5 events) ###\n")
good = pdf[pdf["best_wr"] >= 60].sort_values("best_wr", ascending=False)
print(f"{'Stock':<12s} {'Cat':>3s} {'Best Strategy':<25s} {'WR':>5s} {'N':>4s}")
print("-" * 55)
for _, r in good.head(40).iterrows():
    strat = r["best_strategy"]
    n_key = f"{strat}_all_n"
    n = r.get(n_key, 0)
    print(f"{r['symbol']:<12s} {r['category']:>3s} {strat:<25s} {r['best_wr']:4.0f}% {n:4.0f}")

# Per strategy: which stocks have best win rate?
print(f"\n\n{'=' * 80}")
print("TOP STOCKS PER STRATEGY (full period, min 5 events)")
print("=" * 80)

for strat in STRATEGIES:
    wr_col = f"{strat}_all_wr"
    n_col = f"{strat}_all_n"
    if wr_col not in pdf.columns:
        continue
    valid = pdf[(pdf[wr_col].notna()) & (pdf[n_col] >= 5)].sort_values(wr_col, ascending=False)
    if len(valid) == 0:
        continue
    top3 = valid.head(3)
    avg_wr = valid[wr_col].mean()
    print(f"\n{strat} (avg WR: {avg_wr:.0f}%, {len(valid)} stocks):")
    for _, r in top3.iterrows():
        n6m = r.get(f"{strat}_6m_n", 0) or 0
        wr6m = r.get(f"{strat}_6m_wr", 0) or 0
        n2m = r.get(f"{strat}_2m_n", 0) or 0
        wr2m = r.get(f"{strat}_2m_wr", 0) or 0
        print(f"  {r['symbol']:<12s} All:{r[wr_col]:.0f}%({r[n_col]:.0f}) 6m:{wr6m:.0f}%({n6m:.0f}) 2m:{wr2m:.0f}%({n2m:.0f})")

# Recent 6 months: which stocks + strategies work NOW?
print(f"\n\n{'=' * 80}")
print("LAST 6 MONTHS: Best stock+strategy combos (min 3 events)")
print("=" * 80)

recent_combos = []
for _, r in pdf.iterrows():
    for strat in STRATEGIES:
        n_col = f"{strat}_6m_n"
        wr_col = f"{strat}_6m_wr"
        if pd.notna(r.get(n_col)) and r.get(n_col, 0) >= 3:
            recent_combos.append({
                "symbol": r["symbol"], "category": r["category"],
                "strategy": strat, "n": r[n_col], "wr": r[wr_col],
            })

rdf = pd.DataFrame(recent_combos)
if len(rdf) > 0:
    top_recent = rdf[rdf["wr"] >= 70].sort_values(["wr", "n"], ascending=[False, False])
    print(f"\n70%+ win rate in last 6 months ({len(top_recent)} combos):\n")
    print(f"{'Stock':<12s} {'Cat':>3s} {'Strategy':<25s} {'Events':>7s} {'WR':>6s}")
    print("-" * 60)
    for _, r in top_recent.head(50).iterrows():
        print(f"{r['symbol']:<12s} {r['category']:>3s} {r['strategy']:<25s} {r['n']:7.0f} {r['wr']:5.0f}%")

# Last 2 months
print(f"\n\n{'=' * 80}")
print("LAST 2 MONTHS: What's working RIGHT NOW? (min 2 events)")
print("=" * 80)

recent2m = []
for _, r in pdf.iterrows():
    for strat in STRATEGIES:
        n_col = f"{strat}_2m_n"
        wr_col = f"{strat}_2m_wr"
        if pd.notna(r.get(n_col)) and r.get(n_col, 0) >= 2:
            recent2m.append({
                "symbol": r["symbol"], "category": r["category"],
                "strategy": strat, "n": r[n_col], "wr": r[wr_col],
            })

r2df = pd.DataFrame(recent2m)
if len(r2df) > 0:
    top_2m = r2df[r2df["wr"] >= 80].sort_values(["wr", "n"], ascending=[False, False])
    print(f"\n80%+ win rate in last 2 months ({len(top_2m)} combos):\n")
    print(f"{'Stock':<12s} {'Cat':>3s} {'Strategy':<25s} {'Events':>7s} {'WR':>6s}")
    print("-" * 60)
    for _, r in top_2m.head(50).iterrows():
        print(f"{r['symbol']:<12s} {r['category']:>3s} {r['strategy']:<25s} {r['n']:7.0f} {r['wr']:5.0f}%")
