#!/usr/bin/env python3
"""
Find the EXACT indicator values that give profit after T+2.
Since DSE has T+2 settlement, we can't sell for 2 days.
So Day 3 return is the FIRST possible exit.
Tests: At what RSI, CMF, ADX, volume, support touches, price level
does buying give the best Day 3 and Day 5 profit?
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

print("Loading data...")
df = pd.read_sql(
    "SELECT dp.symbol, dp.date, dp.open, dp.high, dp.low, dp.close, dp.volume, "
    "f.category, f.pe_ratio "
    "FROM daily_prices dp JOIN fundamentals f ON dp.symbol = f.symbol "
    "WHERE dp.date >= '2025-07-01' AND dp.close > 0 AND f.category IN ('A','B') "
    "AND dp.volume > 10000 ORDER BY dp.symbol, dp.date",
    conn,
)
df["date"] = pd.to_datetime(df["date"])

dsex = pd.read_sql(
    "SELECT date, dsex_index as close FROM dsex_history WHERE dsex_index > 0 ORDER BY date", conn)
dsex["date"] = pd.to_datetime(dsex["date"])
dsex = dsex.set_index("date")
dsex["sma20"] = dsex["close"].rolling(20).mean()
dsex["below_sma20"] = dsex["close"] < dsex["sma20"]
conn.close()

print(f"Loaded {len(df)} rows, {df.symbol.nunique()} stocks\n")

all_entries = []

for symbol, sdf in df.groupby("symbol"):
    if len(sdf) < 80:
        continue
    sdf = sdf.sort_values("date").reset_index(drop=True)
    close = sdf["close"]
    high = sdf["high"]
    low = sdf["low"]
    open_ = sdf["open"]
    volume = sdf["volume"].astype(float)

    rsi = RSIIndicator(close, window=14).rsi()
    cmf = ChaikinMoneyFlowIndicator(high, low, close, volume, window=20).chaikin_money_flow()
    adx_obj = ADXIndicator(high, low, close, window=14)
    adx = adx_obj.adx()
    ema9 = EMAIndicator(close, window=9).ema_indicator()
    ema21 = EMAIndicator(close, window=21).ema_indicator()
    vol_avg = volume.rolling(20).mean()
    obv = OnBalanceVolumeIndicator(close, volume).on_balance_volume()

    # OBV slope
    obv_slope = obv.rolling(10).apply(lambda x: np.polyfit(range(len(x)), x, 1)[0], raw=True)
    price_slope = close.rolling(10).apply(lambda x: np.polyfit(range(len(x)), x, 1)[0], raw=True)

    cat = sdf["category"].iloc[0]
    pe = sdf["pe_ratio"].iloc[0]

    # Find support levels
    for i in range(60, len(sdf) - 5):
        if sdf["date"].iloc[i] < pd.Timestamp("2025-10-01"):
            continue

        c = close.iloc[i]
        o = open_.iloc[i]
        l_val = low.iloc[i]
        h_val = high.iloc[i]
        v = volume.iloc[i]
        dt = sdf["date"].iloc[i]

        rsi_v = rsi.iloc[i] if pd.notna(rsi.iloc[i]) else None
        cmf_v = cmf.iloc[i] if pd.notna(cmf.iloc[i]) else None
        adx_v = adx.iloc[i] if pd.notna(adx.iloc[i]) else None
        va = vol_avg.iloc[i] if pd.notna(vol_avg.iloc[i]) else v
        e9 = ema9.iloc[i] if pd.notna(ema9.iloc[i]) else 0
        e21 = ema21.iloc[i] if pd.notna(ema21.iloc[i]) else 0
        obv_s = obv_slope.iloc[i] if pd.notna(obv_slope.iloc[i]) else 0
        price_s = price_slope.iloc[i] if pd.notna(price_slope.iloc[i]) else 0

        if rsi_v is None:
            continue

        # Support detection
        swing_lows = []
        for j in range(max(2, i-60), i-2):
            if low.iloc[j] == min(low.iloc[max(0,j-2):j+3]):
                swing_lows.append(float(low.iloc[j]))

        sup_touches = 0
        at_support = False
        if len(swing_lows) >= 2:
            swing_lows.sort()
            clusters = []
            curr = [swing_lows[0]]
            for sl in swing_lows[1:]:
                if abs(sl - curr[-1]) / curr[-1] * 100 <= 1.5:
                    curr.append(sl)
                else:
                    if len(curr) >= 2:
                        clusters.append(curr)
                    curr = [sl]
            if len(curr) >= 2:
                clusters.append(curr)
            for cl in clusters:
                avg = np.mean(cl)
                if abs(l_val - avg) / avg * 100 < 2:
                    at_support = True
                    sup_touches = len(cl)
                    break

        # Candle analysis
        green = c > o
        body = abs(c - o)
        lower_shadow = min(c, o) - l_val
        hammer = lower_shadow > body * 2 if body > 0 else False
        bullish_candle = green or hammer

        # Volume ratio
        vol_ratio = v / va if va > 0 else 1

        # Consecutive red days
        red_days = 0
        for j in range(1, min(6, i)):
            if close.iloc[i-j] < close.iloc[i-j-1]:
                red_days += 1
            else:
                break

        # OBV divergence
        obv_div = price_s < 0 and obv_s > 0

        # DSEX
        dsex_below = None
        if dt in dsex.index:
            dsex_below = bool(dsex.loc[dt, "below_sma20"]) if pd.notna(dsex.loc[dt, "below_sma20"]) else None
        elif len(dsex.loc[:dt]) > 0:
            dsex_below = bool(dsex.loc[:dt].iloc[-1]["below_sma20"])

        # Returns — focus on Day 3 (first T+2 exit) and Day 5
        ret_3d = (close.iloc[i+3] - c) / c * 100 if i+3 < len(sdf) else None
        ret_5d = (close.iloc[i+5] - c) / c * 100 if i+5 < len(sdf) else None
        # Max gain in day 3-5 (earliest profitable exit after T+2)
        if i+5 < len(sdf):
            max_3to5 = (high.iloc[i+3:i+6].max() - c) / c * 100
        else:
            max_3to5 = None

        all_entries.append({
            "symbol": symbol, "date": str(dt.date()), "price": c,
            "rsi": round(rsi_v, 1),
            "cmf": round(float(cmf_v), 3) if cmf_v is not None else None,
            "adx": round(float(adx_v), 1) if adx_v is not None else None,
            "vol_ratio": round(vol_ratio, 2),
            "at_support": at_support, "sup_touches": sup_touches,
            "bullish_candle": bullish_candle, "green": green,
            "red_days": red_days, "obv_div": obv_div,
            "dsex_below_sma20": dsex_below,
            "category": cat, "pe": pe,
            "ma_above": c > e9 > e21 if e9 > 0 and e21 > 0 else False,
            "ret_3d": ret_3d, "ret_5d": ret_5d, "max_3to5": max_3to5,
        })

adf = pd.DataFrame(all_entries)
adf = adf[adf["ret_3d"].notna()]
print(f"Total entries analyzed: {len(adf)}\n")


def scan(name, subset):
    if len(subset) < 20:
        return None
    v3 = subset["ret_3d"].dropna()
    v5 = subset["ret_5d"].dropna()
    mx = subset["max_3to5"].dropna()
    wr3 = (v3 > 0).sum() / len(v3) * 100 if len(v3) > 0 else 0
    wr5 = (v5 > 0).sum() / len(v5) * 100 if len(v5) > 0 else 0
    return {"name": name, "n": len(subset), "wr3": wr3, "avg3": v3.mean(),
            "wr5": wr5, "avg5": v5.mean(), "max_3to5": mx.mean()}


print("=" * 80)
print("OPTIMAL ENTRY POINTS — PROFIT AFTER T+2 (Day 3+ returns)")
print("=" * 80)

# === RSI SWEEP ===
print("\n### RSI — At what RSI is Day 3 profitable? ###")
print(f"{'RSI Range':<20} {'Events':>7} {'D3 WR':>7} {'D3 Avg':>8} {'D5 WR':>7} {'D5 Avg':>8} {'Max D3-5':>8}")
print("-" * 70)
for lo, hi in [(15,20),(20,25),(25,30),(30,35),(35,40),(40,45),(45,50),(50,55),(55,60),(60,65),(65,70),(70,75),(75,80)]:
    sub = adf[(adf["rsi"] >= lo) & (adf["rsi"] < hi)]
    r = scan(f"RSI {lo}-{hi}", sub)
    if r:
        marker = " ***" if r["wr3"] >= 55 else ""
        print(f"RSI {lo:>2}-{hi:<3}            {r['n']:7d} {r['wr3']:6.0f}% {r['avg3']:+7.2f}% {r['wr5']:6.0f}% {r['avg5']:+7.2f}% {r['max_3to5']:+7.2f}%{marker}")

# === RSI + SUPPORT ===
print("\n### RSI + At Support (3T+) — Day 3 profit ###")
print(f"{'Setup':<30} {'Events':>7} {'D3 WR':>7} {'D3 Avg':>8} {'D5 WR':>7} {'D5 Avg':>8}")
print("-" * 70)
for rsi_max in [25, 30, 35, 40, 45]:
    sub = adf[(adf["rsi"] < rsi_max) & (adf["at_support"]) & (adf["sup_touches"] >= 3)]
    r = scan(f"RSI<{rsi_max} + Sup 3T", sub)
    if r:
        marker = " ***" if r["wr3"] >= 55 else ""
        print(f"RSI<{rsi_max} + Support 3T       {r['n']:7d} {r['wr3']:6.0f}% {r['avg3']:+7.2f}% {r['wr5']:6.0f}% {r['avg5']:+7.2f}%{marker}")

# === RSI + SUPPORT + BULLISH CANDLE ===
print("\n### RSI + Support + Bullish Candle — Day 3 profit ###")
for rsi_max in [30, 35, 40]:
    sub = adf[(adf["rsi"] < rsi_max) & (adf["at_support"]) & (adf["sup_touches"] >= 3) & (adf["bullish_candle"])]
    r = scan(f"RSI<{rsi_max}+Sup+Bull", sub)
    if r:
        marker = " ***" if r["wr3"] >= 55 else ""
        print(f"RSI<{rsi_max} + Sup 3T + Bullish  {r['n']:7d} {r['wr3']:6.0f}% {r['avg3']:+7.2f}% {r['wr5']:6.0f}% {r['avg5']:+7.2f}%{marker}")

# === CONSECUTIVE RED DAYS ===
print("\n### Consecutive Red Days — Day 3 profit ###")
print(f"{'Red Days':<20} {'Events':>7} {'D3 WR':>7} {'D3 Avg':>8} {'D5 WR':>7} {'D5 Avg':>8}")
print("-" * 70)
for rd in [1, 2, 3, 4, 5]:
    sub = adf[adf["red_days"] >= rd]
    r = scan(f"{rd}+ red days", sub)
    if r:
        marker = " ***" if r["wr3"] >= 55 else ""
        print(f"{rd}+ consecutive red     {r['n']:7d} {r['wr3']:6.0f}% {r['avg3']:+7.2f}% {r['wr5']:6.0f}% {r['avg5']:+7.2f}%{marker}")

# === RED DAYS + RSI ===
print("\n### Red Days + RSI combo ###")
for rd in [2, 3]:
    for rsi_max in [35, 40, 45]:
        sub = adf[(adf["red_days"] >= rd) & (adf["rsi"] < rsi_max)]
        r = scan(f"{rd}red+RSI<{rsi_max}", sub)
        if r:
            marker = " ***" if r["wr3"] >= 55 else ""
            print(f"{rd}+ red + RSI<{rsi_max}        {r['n']:7d} {r['wr3']:6.0f}% {r['avg3']:+7.2f}% {r['wr5']:6.0f}% {r['avg5']:+7.2f}%{marker}")

# === DSEX REGIME ===
print("\n### DSEX Regime — Day 3 profit ###")
for dsex_val, label in [(True, "DSEX below SMA20"), (False, "DSEX above SMA20")]:
    sub = adf[adf["dsex_below_sma20"] == dsex_val]
    r = scan(label, sub)
    if r:
        marker = " ***" if r["wr3"] >= 55 else ""
        print(f"{label:<30} {r['n']:7d} {r['wr3']:6.0f}% {r['avg3']:+7.2f}% {r['wr5']:6.0f}% {r['avg5']:+7.2f}%{marker}")

# === OBV DIVERGENCE ===
print("\n### OBV Divergence — Day 3 profit ###")
sub = adf[adf["obv_div"]]
r = scan("OBV Divergence", sub)
if r:
    print(f"OBV Divergence           {r['n']:7d} {r['wr3']:6.0f}% {r['avg3']:+7.2f}% {r['wr5']:6.0f}% {r['avg5']:+7.2f}%")
sub2 = adf[(adf["obv_div"]) & (adf["rsi"] < 40)]
r2 = scan("OBV Div + RSI<40", sub2)
if r2:
    print(f"OBV Div + RSI<40         {r2['n']:7d} {r2['wr3']:6.0f}% {r2['avg3']:+7.2f}% {r2['wr5']:6.0f}% {r2['avg5']:+7.2f}%")

# === VOLUME RATIO ===
print("\n### Volume Ratio — Day 3 profit ###")
for lo, hi, label in [(0, 0.5, "Very Low (<0.5x)"), (0.5, 0.8, "Low (0.5-0.8x)"), (0.8, 1.2, "Normal"), (1.2, 2, "High (1.2-2x)"), (2, 100, "Spike (2x+)")]:
    sub = adf[(adf["vol_ratio"] >= lo) & (adf["vol_ratio"] < hi)]
    r = scan(label, sub)
    if r:
        print(f"{label:<25}  {r['n']:7d} {r['wr3']:6.0f}% {r['avg3']:+7.2f}% {r['wr5']:6.0f}% {r['avg5']:+7.2f}%")

# === THE ULTIMATE COMBO ===
print("\n" + "=" * 80)
print("ULTIMATE COMBOS — T+2 PROFIT OPTIMIZATION")
print("=" * 80)

combos = [
    ("RSI<35 + Sup3T + DSEX weak", adf[(adf["rsi"]<35) & (adf["at_support"]) & (adf["sup_touches"]>=3) & (adf["dsex_below_sma20"]==True)]),
    ("RSI<35 + Sup3T + Bullish + DSEX weak", adf[(adf["rsi"]<35) & (adf["at_support"]) & (adf["sup_touches"]>=3) & (adf["bullish_candle"]) & (adf["dsex_below_sma20"]==True)]),
    ("RSI<30 + DSEX weak", adf[(adf["rsi"]<30) & (adf["dsex_below_sma20"]==True)]),
    ("3red + RSI<40 + DSEX weak", adf[(adf["red_days"]>=3) & (adf["rsi"]<40) & (adf["dsex_below_sma20"]==True)]),
    ("RSI<35 + Sup3T + 2red", adf[(adf["rsi"]<35) & (adf["at_support"]) & (adf["sup_touches"]>=3) & (adf["red_days"]>=2)]),
    ("OBV Div + RSI<35 + Support", adf[(adf["obv_div"]) & (adf["rsi"]<35) & (adf["at_support"])]),
    ("RSI<35 + Sup5T+ + Bullish", adf[(adf["rsi"]<35) & (adf["at_support"]) & (adf["sup_touches"]>=5) & (adf["bullish_candle"])]),
]

print(f"\n{'Combo':<45} {'N':>5} {'D3 WR':>6} {'D3 Avg':>7} {'D5 WR':>6} {'D5 Avg':>7} {'Max35':>7}")
print("-" * 85)
for name, sub in combos:
    if len(sub) < 5:
        print(f"{name:<45} {len(sub):5d}   too few")
        continue
    v3 = sub["ret_3d"].dropna()
    v5 = sub["ret_5d"].dropna()
    mx = sub["max_3to5"].dropna()
    wr3 = (v3>0).sum()/len(v3)*100
    wr5 = (v5>0).sum()/len(v5)*100
    marker = " ***" if wr3 >= 60 else ""
    print(f"{name:<45} {len(sub):5d} {wr3:5.0f}% {v3.mean():+6.2f}% {wr5:5.0f}% {v5.mean():+6.2f}% {mx.mean():+6.2f}%{marker}")
