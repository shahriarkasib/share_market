#!/usr/bin/env python3
"""
Resistance break analysis + Per-share EDA signals.
When does breaking resistance actually work on DSE?
"""

import warnings; warnings.filterwarnings("ignore")
import sys, os
import pandas as pd
import numpy as np
import psycopg2

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from config import DATABASE_URL
from ta.momentum import RSIIndicator
from ta.volume import ChaikinMoneyFlowIndicator
from ta.trend import ADXIndicator, EMAIndicator

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
conn.close()
print(f"Loaded {len(df)} rows, {df.symbol.nunique()} stocks\n")

events = {
    "52w_high_break": [],
    "52w_high_break_vol": [],
    "52w_high_break_adx25": [],
    "52w_high_break_ma_aligned": [],
    "52w_high_break_cmf_pos": [],
    "52w_high_break_full_combo": [],
    "resistance_break_3t": [],
    "resistance_break_5t": [],
    "resistance_break_vol": [],
    "resistance_break_ma_cmf": [],
    "20d_high_break": [],
    "20d_high_break_vol_adx": [],
    "fib_1_0_break": [],
    "fib_1_0_break_strong": [],
}


def fwd_ret(close, high, i):
    fwd = {}
    for d in [1, 3, 5, 10, 20]:
        if i + d < len(close):
            fwd[f"ret_{d}d"] = (close.iloc[i + d] - close.iloc[i]) / close.iloc[i] * 100
            fwd[f"max_{d}d"] = (high.iloc[i + 1:i + d + 1].max() - close.iloc[i]) / close.iloc[i] * 100
    return fwd


for symbol, sdf in df.groupby("symbol"):
    if len(sdf) < 260:
        continue
    sdf = sdf.sort_values("date").reset_index(drop=True)
    close = sdf["close"]
    high = sdf["high"]
    low = sdf["low"]
    volume = sdf["volume"].astype(float)

    rsi = RSIIndicator(close, window=14).rsi()
    cmf = ChaikinMoneyFlowIndicator(high, low, close, volume, window=20).chaikin_money_flow()
    adx_obj = ADXIndicator(high, low, close, window=14)
    adx = adx_obj.adx()
    ema9 = EMAIndicator(close, window=9).ema_indicator()
    ema21 = EMAIndicator(close, window=21).ema_indicator()
    ema50 = EMAIndicator(close, window=50).ema_indicator()
    vol_avg = volume.rolling(20).mean()

    for i in range(260, len(sdf) - 20):
        c = close.iloc[i]
        c_prev = close.iloc[i - 1]
        v = volume.iloc[i]
        rsi_v = rsi.iloc[i] if pd.notna(rsi.iloc[i]) else 50
        cmf_v = cmf.iloc[i] if pd.notna(cmf.iloc[i]) else 0
        adx_v = adx.iloc[i] if pd.notna(adx.iloc[i]) else 0
        va = vol_avg.iloc[i] if pd.notna(vol_avg.iloc[i]) else v
        e9 = ema9.iloc[i] if pd.notna(ema9.iloc[i]) else 0
        e21 = ema21.iloc[i] if pd.notna(ema21.iloc[i]) else 0
        e50 = ema50.iloc[i] if pd.notna(ema50.iloc[i]) else 0

        vol_ratio = v / va if va > 0 else 1
        ma_ok = e9 > e21 > e50 and c > e9
        cmf_pos = cmf_v > 0
        adx_25 = adx_v > 25
        high_vol = vol_ratio > 1.5

        fwd = fwd_ret(close, high, i)
        base = {"symbol": symbol, "date": str(sdf["date"].iloc[i].date()), "price": c, **fwd}

        # 52-week high break
        h52w = high.iloc[i - 252:i].max()
        if c > h52w and c_prev <= h52w:
            events["52w_high_break"].append(base)
            if high_vol:
                events["52w_high_break_vol"].append(base)
            if adx_25:
                events["52w_high_break_adx25"].append(base)
            if ma_ok:
                events["52w_high_break_ma_aligned"].append(base)
            if cmf_pos:
                events["52w_high_break_cmf_pos"].append(base)
            if ma_ok and cmf_pos and high_vol:
                events["52w_high_break_full_combo"].append(base)

        # 20-day high break
        h20 = high.iloc[i - 20:i].max()
        if c > h20 and c_prev <= h20:
            events["20d_high_break"].append(base)
            if high_vol and adx_25:
                events["20d_high_break_vol_adx"].append(base)

        # Resistance break (from swing highs)
        swing_highs = []
        for j in range(max(2, i - 60), i - 2):
            if high.iloc[j] == max(high.iloc[max(0, j - 2):j + 3]):
                swing_highs.append(float(high.iloc[j]))

        if swing_highs:
            swing_highs.sort()
            # Cluster
            clusters = []
            curr = [swing_highs[0]]
            for sh in swing_highs[1:]:
                if abs(sh - curr[-1]) / curr[-1] * 100 <= 1.5:
                    curr.append(sh)
                else:
                    if len(curr) >= 2:
                        clusters.append(curr)
                    curr = [sh]
            if len(curr) >= 2:
                clusters.append(curr)

            for cl in clusters:
                avg = np.mean(cl)
                touches = len(cl)
                if c > avg and c_prev <= avg:
                    if touches >= 3:
                        events["resistance_break_3t"].append(base)
                        if high_vol:
                            events["resistance_break_vol"].append(base)
                        if ma_ok and cmf_pos:
                            events["resistance_break_ma_cmf"].append(base)
                    if touches >= 5:
                        events["resistance_break_5t"].append(base)
                    break

        # Fibonacci 1.0 extension break
        lookback = 100
        if i >= lookback:
            recent_high = high.iloc[i - lookback:i].max()
            recent_low = low.iloc[i - lookback:i].min()
            high_idx = high.iloc[i - lookback:i].idxmax()
            low_idx = low.iloc[i - lookback:i].idxmin()
            diff = recent_high - recent_low

            if low_idx < high_idx and diff > 0:  # uptrend
                fib_1_0 = recent_low + diff
                if c > fib_1_0 and c_prev <= fib_1_0:
                    events["fib_1_0_break"].append(base)
                    if ma_ok and high_vol and adx_25:
                        events["fib_1_0_break_strong"].append(base)

# Deduplicate
for key in events:
    if events[key]:
        edf = pd.DataFrame(events[key]).drop_duplicates(subset=["symbol", "date"])
        events[key] = edf.to_dict("records")

# Report
def report(name, evts):
    if len(evts) < 10:
        print(f"\n{name}: {len(evts)} events (too few)")
        return
    edf = pd.DataFrame(evts)
    print(f"\n{'=' * 70}")
    print(f"{name} ({len(edf)} events)")
    for d in [3, 5, 10, 20]:
        col = f"ret_{d}d"
        mcol = f"max_{d}d"
        v = edf[col].dropna()
        m = edf[mcol].dropna() if mcol in edf.columns else pd.Series()
        if len(v) == 0:
            continue
        wr = (v > 0).sum() / len(v) * 100
        marker = " ***" if wr >= 55 else ""
        mstr = f" max:{m.mean():+.1f}%" if len(m) > 0 else ""
        print(f"  Day {d:2d}: Win {wr:.0f}%, Avg {v.mean():+.2f}%{mstr}{marker}")

print("=" * 70)
print("RESISTANCE BREAK ANALYSIS — 5 YEARS DSE DATA")
print("=" * 70)

print("\n### 52-WEEK HIGH BREAKOUT ###")
report("52w High Break (any)", events["52w_high_break"])
report("52w High + Volume >1.5x", events["52w_high_break_vol"])
report("52w High + ADX>25", events["52w_high_break_adx25"])
report("52w High + MA Aligned", events["52w_high_break_ma_aligned"])
report("52w High + CMF Positive", events["52w_high_break_cmf_pos"])
report("52w High + MA + CMF + Volume (FULL COMBO)", events["52w_high_break_full_combo"])

print("\n### RESISTANCE LEVEL BREAKOUT ###")
report("Resistance Break 3+ touches", events["resistance_break_3t"])
report("Resistance Break 5+ touches", events["resistance_break_5t"])
report("Resistance Break + Volume", events["resistance_break_vol"])
report("Resistance Break + MA aligned + CMF positive", events["resistance_break_ma_cmf"])

print("\n### OTHER BREAKOUTS ###")
report("20-day High Break", events["20d_high_break"])
report("20d High + Volume + ADX>25", events["20d_high_break_vol_adx"])
report("Fibonacci 1.0 Extension Break", events["fib_1_0_break"])
report("Fib 1.0 + MA + Volume + ADX (STRONG)", events["fib_1_0_break_strong"])

# Ranking
print(f"\n{'=' * 70}")
print("RESISTANCE BREAK RANKING BY DAY-5 WIN RATE")
print(f"{'=' * 70}\n")

names = {
    "52w_high_break": "52w High Break",
    "52w_high_break_vol": "52w High + Volume",
    "52w_high_break_adx25": "52w High + ADX>25",
    "52w_high_break_ma_aligned": "52w High + MA Aligned",
    "52w_high_break_cmf_pos": "52w High + CMF+",
    "52w_high_break_full_combo": "52w High FULL COMBO",
    "resistance_break_3t": "Resistance 3T Break",
    "resistance_break_5t": "Resistance 5T Break",
    "resistance_break_vol": "Resistance + Volume",
    "resistance_break_ma_cmf": "Resistance + MA + CMF",
    "20d_high_break": "20d High Break",
    "20d_high_break_vol_adx": "20d High + Vol + ADX",
    "fib_1_0_break": "Fib 1.0 Break",
    "fib_1_0_break_strong": "Fib 1.0 STRONG",
}

all_r = []
for key, evts in events.items():
    if len(evts) < 10:
        continue
    edf = pd.DataFrame(evts)
    v5 = edf["ret_5d"].dropna()
    v10 = edf["ret_10d"].dropna()
    m5 = edf["max_5d"].dropna() if "max_5d" in edf.columns else pd.Series()
    if len(v5) == 0:
        continue
    wr5 = (v5 > 0).sum() / len(v5) * 100
    all_r.append({"name": names.get(key, key), "n": len(edf), "wr5": wr5, "avg5": v5.mean(),
                  "wr10": (v10 > 0).sum() / len(v10) * 100 if len(v10) > 0 else 0,
                  "max5": m5.mean() if len(m5) > 0 else 0})

all_r.sort(key=lambda x: -x["wr5"])
print(f"{'Factor':<35} {'N':>6} {'5D WR':>6} {'5D Avg':>7} {'5D Max':>7} {'10D WR':>7}")
print("-" * 75)
for r in all_r:
    marker = " ***" if r["wr5"] >= 55 else ""
    print(f"{r['name']:<35} {r['n']:6d} {r['wr5']:5.0f}% {r['avg5']:+6.2f}% {r['max5']:+6.2f}% {r['wr10']:6.0f}%{marker}")
