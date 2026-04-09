#!/usr/bin/env python3
"""
Backtest: Momentum / Trend breakout setups.
Testing what ACMEPL, BDAUTOCA, LOVELLO had in common:
- MA aligned (EMA9 > EMA21 > EMA50) = uptrend
- ADX rising / strong
- CMF positive streak
- Price above all EMAs
- RSI NOT oversold (50-75 range = momentum zone)
- Breaking new highs within trend
"""

import warnings; warnings.filterwarnings("ignore")
import sys, os
import pandas as pd
import numpy as np
import psycopg2

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from config import DATABASE_URL

from ta.volume import ChaikinMoneyFlowIndicator
from ta.trend import EMAIndicator, SMAIndicator, ADXIndicator
from ta.momentum import RSIIndicator

conn = psycopg2.connect(DATABASE_URL)

print("Loading data...")
df = pd.read_sql(
    "SELECT dp.symbol, dp.date, dp.open, dp.high, dp.low, dp.close, dp.volume "
    "FROM daily_prices dp JOIN fundamentals f ON dp.symbol = f.symbol "
    "WHERE dp.date >= '2025-10-01' AND dp.close > 0 AND f.category IN ('A','B') "
    "AND dp.volume > 0 ORDER BY dp.symbol, dp.date",
    conn,
)
df["date"] = pd.to_datetime(df["date"])
conn.close()
print(f"Loaded {len(df)} rows, {df.symbol.nunique()} stocks\n")


def fwd_ret(close, i):
    fwd = {}
    for d in [1, 2, 3, 5, 10]:
        if i + d < len(close):
            fwd[f"ret_{d}d"] = (close.iloc[i+d] - close.iloc[i]) / close.iloc[i] * 100
        else:
            fwd[f"ret_{d}d"] = None
    return fwd


def report(name, events, note=""):
    if not events:
        print(f"\n{'='*70}\n{name}\n  No events.\n")
        return
    rdf = pd.DataFrame(events)
    n = len(rdf)
    print(f"\n{'='*70}")
    print(f"{name}")
    if note:
        print(f"  {note}")
    print(f"  Events: {n}")
    for d in [1, 3, 5, 10]:
        col = f"ret_{d}d"
        valid = rdf[col].dropna()
        if len(valid) == 0:
            continue
        win = (valid > 0).sum()
        total = len(valid)
        wr = win / total * 100
        marker = " <<<" if wr >= 60 and d == 5 else ""
        print(f"  Day {d:2d}: Win {win}/{total} ({wr:.0f}%), Avg {valid.mean():+.2f}%, Median {valid.median():+.2f}%{marker}")


events = {
    "ma_aligned": [],
    "ma_aligned_adx25": [],
    "ma_aligned_adx30": [],
    "ma_aligned_cmf5": [],
    "ma_aligned_cmf10": [],
    "ma_aligned_rsi_momentum": [],
    "ma_aligned_adx25_cmf5": [],
    "ma_aligned_adx30_cmf5_rsi": [],
    "pullback_to_ema9": [],
    "pullback_to_ema21": [],
    "pullback_ema9_green": [],
    "pullback_ema21_green": [],
    "new_20d_high": [],
    "new_20d_high_vol": [],
    "new_20d_high_ma_aligned": [],
    "new_20d_high_ma_vol": [],
    "breakout_consolidation": [],
    "acmepl_pattern": [],  # MA aligned + ADX>35 + CMF pos 5d+ + RSI 60-80
    "trend_continuation": [],  # MA aligned + pullback to EMA9/21 + green bounce
    "early_trend": [],  # EMA9 just crossed above EMA21 + price above both
    "vol_expansion_trend": [],  # MA aligned + volume > 2x avg + green
}

stock_count = 0
for symbol, sdf in df.groupby("symbol"):
    if len(sdf) < 80:
        continue
    stock_count += 1
    sdf = sdf.sort_values("date").reset_index(drop=True)

    close = sdf["close"]
    high = sdf["high"]
    low = sdf["low"]
    open_ = sdf["open"]
    volume = sdf["volume"].astype(float)

    ema9 = EMAIndicator(close, window=9).ema_indicator()
    ema21 = EMAIndicator(close, window=21).ema_indicator()
    ema50 = EMAIndicator(close, window=50).ema_indicator()
    rsi = RSIIndicator(close, window=14).rsi()
    cmf = ChaikinMoneyFlowIndicator(high, low, close, volume, window=20).chaikin_money_flow()
    adx_obj = ADXIndicator(high, low, close, window=14)
    adx = adx_obj.adx()
    vol_avg = volume.rolling(20).mean()

    # CMF streak
    cmf_streak = pd.Series(0, index=close.index)
    for j in range(1, len(cmf)):
        if pd.notna(cmf.iloc[j]) and cmf.iloc[j] > 0:
            cmf_streak.iloc[j] = cmf_streak.iloc[j-1] + 1

    for i in range(60, len(sdf) - 10):
        c = close.iloc[i]
        o = open_.iloc[i]
        h = high.iloc[i]
        l = low.iloc[i]
        v = volume.iloc[i]
        c_prev = close.iloc[i-1]

        e9 = ema9.iloc[i] if pd.notna(ema9.iloc[i]) else 0
        e21 = ema21.iloc[i] if pd.notna(ema21.iloc[i]) else 0
        e50 = ema50.iloc[i] if pd.notna(ema50.iloc[i]) else 0
        r = rsi.iloc[i] if pd.notna(rsi.iloc[i]) else 50
        cm = cmf.iloc[i] if pd.notna(cmf.iloc[i]) else 0
        a = adx.iloc[i] if pd.notna(adx.iloc[i]) else 0
        va = vol_avg.iloc[i] if pd.notna(vol_avg.iloc[i]) else v
        cs = int(cmf_streak.iloc[i])

        green = c > o
        ma_ok = e9 > e21 > e50 and c > e9
        vr = v / va if va > 0 else 1

        fwd = fwd_ret(close, i)
        base = {"symbol": symbol, "date": str(sdf["date"].iloc[i].date()), "price": c, **fwd}

        if not ma_ok:
            continue

        # === MA ALIGNED (base) ===
        events["ma_aligned"].append(base)

        # === MA + ADX ===
        if a > 25:
            events["ma_aligned_adx25"].append(base)
        if a > 30:
            events["ma_aligned_adx30"].append(base)

        # === MA + CMF streak ===
        if cs >= 5:
            events["ma_aligned_cmf5"].append(base)
        if cs >= 10:
            events["ma_aligned_cmf10"].append(base)

        # === MA + RSI momentum zone (50-75) ===
        if 50 < r < 75:
            events["ma_aligned_rsi_momentum"].append(base)

        # === COMBOS ===
        if a > 25 and cs >= 5:
            events["ma_aligned_adx25_cmf5"].append(base)

        if a > 30 and cs >= 5 and 55 < r < 80:
            events["ma_aligned_adx30_cmf5_rsi"].append(base)

        # === PULLBACK TO EMA ===
        # Price dipped to EMA9 but closed above
        if l <= e9 * 1.005 and c > e9:
            events["pullback_to_ema9"].append(base)
            if green:
                events["pullback_ema9_green"].append(base)

        if l <= e21 * 1.005 and c > e21:
            events["pullback_to_ema21"].append(base)
            if green:
                events["pullback_ema21_green"].append(base)

        # === NEW 20-DAY HIGH ===
        h20 = high.iloc[i-20:i].max()
        if h > h20:
            events["new_20d_high"].append(base)
            if vr > 1.5:
                events["new_20d_high_vol"].append(base)
            events["new_20d_high_ma_aligned"].append(base)
            if vr > 1.5:
                events["new_20d_high_ma_vol"].append(base)

        # === CONSOLIDATION BREAKOUT ===
        # Price range < 5% for 10 days then breaks out
        if i >= 10:
            last10_range = (high.iloc[i-10:i].max() - low.iloc[i-10:i].min()) / close.iloc[i-10] * 100
            if last10_range < 5 and c > high.iloc[i-10:i].max():
                events["breakout_consolidation"].append(base)

        # === ACMEPL PATTERN ===
        # MA aligned + ADX > 35 + CMF 5+ days + RSI 60-80 + volume above avg
        if a > 35 and cs >= 5 and 60 < r < 80 and vr > 1:
            events["acmepl_pattern"].append(base)

        # === TREND CONTINUATION ===
        # MA aligned + pulled back to EMA9 or EMA21 + bounced green
        if green and (l <= e9 * 1.01 or l <= e21 * 1.01) and c > e9:
            events["trend_continuation"].append(base)

        # === EARLY TREND ===
        # EMA9 just crossed above EMA21 (within last 3 bars)
        if i >= 3:
            just_crossed = False
            for j in range(1, 4):
                e9p = ema9.iloc[i-j] if pd.notna(ema9.iloc[i-j]) else 0
                e21p = ema21.iloc[i-j] if pd.notna(ema21.iloc[i-j]) else 0
                if e9p <= e21p:
                    just_crossed = True
                    break
            if just_crossed and e9 > e21 and c > e9:
                events["early_trend"].append(base)

        # === VOLUME EXPANSION IN TREND ===
        if vr > 2 and green:
            events["vol_expansion_trend"].append(base)

# Deduplicate
for key in events:
    if events[key]:
        edf = pd.DataFrame(events[key]).drop_duplicates(subset=["symbol", "date"])
        events[key] = edf.to_dict("records")

print(f"Processed {stock_count} stocks\n")

# === REPORTS ===
print("=" * 70)
print("MOMENTUM / TREND FACTOR STUDY — DSE 6 MONTHS")
print("What makes stocks like ACMEPL, BDAUTOCA, LOVELLO run?")
print("=" * 70)

print("\n### BASE: MA ALIGNED (price > EMA9 > EMA21 > EMA50) ###")
report("MA Aligned (base)", events["ma_aligned"])
report("MA Aligned + ADX > 25 (trending)", events["ma_aligned_adx25"])
report("MA Aligned + ADX > 30 (strong trend)", events["ma_aligned_adx30"])

print("\n### MA + MONEY FLOW ###")
report("MA Aligned + CMF positive 5+ days", events["ma_aligned_cmf5"])
report("MA Aligned + CMF positive 10+ days", events["ma_aligned_cmf10"])
report("MA + ADX>25 + CMF 5+ days", events["ma_aligned_adx25_cmf5"])

print("\n### MA + MOMENTUM ###")
report("MA Aligned + RSI 50-75 (momentum zone)", events["ma_aligned_rsi_momentum"])
report("ACMEPL Pattern: MA + ADX>35 + CMF 5d + RSI 60-80 + Vol>avg", events["acmepl_pattern"],
       "This is what ACMEPL looked like before its 28% run")
report("MA + ADX>30 + CMF 5d + RSI 55-80", events["ma_aligned_adx30_cmf5_rsi"])

print("\n### PULLBACK IN UPTREND ###")
report("Pullback to EMA9 (in uptrend)", events["pullback_to_ema9"])
report("Pullback to EMA9 + Green Candle", events["pullback_ema9_green"])
report("Pullback to EMA21 (in uptrend)", events["pullback_to_ema21"])
report("Pullback to EMA21 + Green Candle", events["pullback_ema21_green"])
report("Trend Continuation: pullback + green bounce off EMA", events["trend_continuation"])

print("\n### BREAKOUT ###")
report("New 20-day High (MA aligned)", events["new_20d_high_ma_aligned"])
report("New 20-day High + Volume > 1.5x", events["new_20d_high_ma_vol"])
report("Consolidation Breakout (<5% range 10d then break)", events["breakout_consolidation"])
report("Volume Expansion in Trend (>2x vol + green + MA aligned)", events["vol_expansion_trend"])

print("\n### EARLY TREND ###")
report("Early Trend: EMA9 just crossed EMA21 + price above", events["early_trend"])

# === FINAL RANKING ===
print(f"\n{'='*70}")
print("MOMENTUM RANKING BY 5-DAY WIN RATE")
print(f"{'='*70}\n")

names = {
    "ma_aligned": "MA Aligned (base)",
    "ma_aligned_adx25": "MA + ADX>25",
    "ma_aligned_adx30": "MA + ADX>30",
    "ma_aligned_cmf5": "MA + CMF 5d+",
    "ma_aligned_cmf10": "MA + CMF 10d+",
    "ma_aligned_rsi_momentum": "MA + RSI 50-75",
    "ma_aligned_adx25_cmf5": "MA + ADX>25 + CMF 5d",
    "ma_aligned_adx30_cmf5_rsi": "MA + ADX>30 + CMF 5d + RSI 55-80",
    "pullback_to_ema9": "Pullback to EMA9",
    "pullback_to_ema21": "Pullback to EMA21",
    "pullback_ema9_green": "Pullback EMA9 + Green",
    "pullback_ema21_green": "Pullback EMA21 + Green",
    "trend_continuation": "Trend Continuation (pullback bounce)",
    "new_20d_high_ma_aligned": "New 20d High (MA aligned)",
    "new_20d_high_ma_vol": "New 20d High + Volume",
    "breakout_consolidation": "Consolidation Breakout",
    "vol_expansion_trend": "Volume Expansion in Trend",
    "acmepl_pattern": "ACMEPL Pattern (full combo)",
    "early_trend": "Early Trend (EMA9 cross EMA21)",
}

all_r = {}
for key, evts in events.items():
    if len(evts) < 10:
        all_r[key] = {"name": names.get(key, key), "n": len(evts), "wr": 0, "avg": 0, "note": "too few"}
        continue
    edf = pd.DataFrame(evts)
    v5 = edf["ret_5d"].dropna()
    if len(v5) == 0:
        continue
    wr = (v5 > 0).sum() / len(v5) * 100
    all_r[key] = {"name": names.get(key, key), "n": len(edf), "wr": wr, "avg": v5.mean()}

ranked = sorted(all_r.items(), key=lambda x: -x[1].get("wr", 0))
print(f"{'Rank':>4} {'Factor':<45} {'Events':>7} {'5D WR':>7} {'5D Avg':>8}")
print("-" * 75)
for rank, (key, info) in enumerate(ranked, 1):
    note = info.get("note", "")
    if note:
        print(f"{rank:4d} {info['name']:<45} {info['n']:7d}    {note}")
    else:
        marker = " ***" if info["wr"] >= 55 else ""
        print(f"{rank:4d} {info['name']:<45} {info['n']:7d} {info['wr']:6.0f}% {info['avg']:+7.2f}%{marker}")
