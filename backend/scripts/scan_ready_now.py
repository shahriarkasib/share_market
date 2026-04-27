#!/usr/bin/env python3
"""Scan: Which stocks are ready RIGHT NOW or just started bouncing?"""

import warnings; warnings.filterwarnings("ignore")
import sys, os
import pandas as pd, numpy as np, psycopg2

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from config import DATABASE_URL
from ta.momentum import RSIIndicator
from ta.volume import ChaikinMoneyFlowIndicator, OnBalanceVolumeIndicator

conn = psycopg2.connect(DATABASE_URL)
df = pd.read_sql(
    "SELECT dp.symbol, dp.date, dp.open, dp.high, dp.low, dp.close, dp.volume "
    "FROM daily_prices dp JOIN fundamentals f ON dp.symbol = f.symbol "
    "WHERE dp.close > 0 AND f.category IN ('A','B') AND dp.volume > 5000 "
    "ORDER BY dp.symbol, dp.date", conn)
df["date"] = pd.to_datetime(df["date"])
live = pd.read_sql("SELECT symbol, ltp, change_pct FROM live_prices WHERE ltp > 0", conn)
live_map = dict(zip(live["symbol"], live[["ltp", "change_pct"]].to_dict("records")))
conn.close()

results = []
for symbol, sdf in df.groupby("symbol"):
    if len(sdf) < 60:
        continue
    sdf = sdf.sort_values("date").reset_index(drop=True)
    close = sdf["close"]; high = sdf["high"]; low = sdf["low"]
    open_ = sdf["open"]; volume = sdf["volume"].astype(float)

    rsi = RSIIndicator(close, window=14).rsi()
    cmf = ChaikinMoneyFlowIndicator(high, low, close, volume, window=20).chaikin_money_flow()
    obv = OnBalanceVolumeIndicator(close, volume).on_balance_volume()
    vol_avg = volume.rolling(20).mean()
    obv_slope = obv.rolling(10).apply(lambda x: np.polyfit(range(len(x)), x, 1)[0] if len(x) > 1 else 0, raw=True)
    price_slope = close.rolling(10).apply(lambda x: np.polyfit(range(len(x)), x, 1)[0] if len(x) > 1 else 0, raw=True)

    i = len(sdf) - 1
    c, o, h, l, v = close.iloc[i], open_.iloc[i], high.iloc[i], low.iloc[i], volume.iloc[i]
    rsi_v = float(rsi.iloc[i]) if pd.notna(rsi.iloc[i]) else 50
    cmf_v = float(cmf.iloc[i]) if pd.notna(cmf.iloc[i]) else 0
    va = float(vol_avg.iloc[i]) if pd.notna(vol_avg.iloc[i]) else v
    obv_s = float(obv_slope.iloc[i]) if pd.notna(obv_slope.iloc[i]) else 0
    price_s = float(price_slope.iloc[i]) if pd.notna(price_slope.iloc[i]) else 0

    lp = live_map.get(symbol, {}); ltp = lp.get("ltp", c); chg = lp.get("change_pct", 0) or 0

    red_days = 0
    for j in range(1, min(6, i)):
        if close.iloc[i - j] < close.iloc[i - j - 1]: red_days += 1
        else: break

    chg_5d = (c - close.iloc[i - 5]) / close.iloc[i - 5] * 100 if i >= 5 else 0
    green = c > o
    vol_ratio = v / va if va > 0 else 1
    obv_div = price_s < 0 and obv_s > 0
    recent_low = low.iloc[max(0, i - 5):i].min()
    liq_grab = l < recent_low * 0.99 and c > recent_low
    h52w = high.iloc[max(0, i - 252):i].max() if i >= 252 else high.iloc[:i].max()
    at_52w = c > h52w
    cmf_5ago = float(cmf.iloc[i - 5]) if i >= 5 and pd.notna(cmf.iloc[i - 5]) else cmf_v
    cmf_rising = cmf_v > cmf_5ago + 0.03

    matched = []
    if rsi_v < 30: matched.append("RSI<30")
    if 30 <= rsi_v < 35: matched.append("RSI30-35")
    if red_days >= 3: matched.append("3+Red")
    if chg_5d < -5: matched.append("Drop5%")
    if chg_5d < -10: matched.append("Drop10%")
    if liq_grab: matched.append("LiqGrab")
    if liq_grab and rsi_v < 30: matched.append("RSI30+Liq")
    if obv_div: matched.append("OBVDiv")
    if obv_div and rsi_v < 40: matched.append("OBVDiv+RSI40")
    if at_52w: matched.append("52wHigh")
    if at_52w and cmf_v > 0: matched.append("52wH+CMF")
    if green and red_days >= 2 and rsi_v < 45: matched.append("BounceStart")
    if green and chg_5d < -5 and vol_ratio > 1.3: matched.append("Recovery+Vol")
    if cmf_rising and rsi_v < 45: matched.append("CMFrising")

    if not matched:
        continue

    results.append({
        "sym": symbol, "ltp": round(ltp, 1), "chg": round(chg, 1),
        "rsi": round(rsi_v, 1), "cmf": round(cmf_v, 3), "chg5d": round(chg_5d, 1),
        "red": red_days, "volx": round(vol_ratio, 1), "sigs": matched, "n": len(matched),
    })

results.sort(key=lambda x: -x["n"])

print("=" * 85)
print("STOCKS READY NOW — Multiple signals firing")
print("=" * 85)

for r in results:
    if r["n"] >= 3:
        sigs = ", ".join(r["sigs"])
        print(f"  {r['sym']:<12s} LTP={r['ltp']:7.1f} Chg={r['chg']:+.1f}% RSI={r['rsi']:.0f} CMF={r['cmf']:+.3f} 5D={r['chg5d']:+.1f}% Red={r['red']} Vol={r['volx']}x  [{sigs}]")

print(f"\n{'=' * 85}")
print("JUST STARTED BOUNCING")
print("=" * 85)

for r in results:
    if "BounceStart" in r["sigs"] or "Recovery+Vol" in r["sigs"]:
        sigs = ", ".join(r["sigs"])
        print(f"  {r['sym']:<12s} LTP={r['ltp']:7.1f} Chg={r['chg']:+.1f}% RSI={r['rsi']:.0f} CMF={r['cmf']:+.3f} 5D={r['chg5d']:+.1f}%  [{sigs}]")

print(f"\n{'=' * 85}")
print("52-WEEK HIGH BREAKS")
print("=" * 85)

for r in results:
    if any("52w" in s for s in r["sigs"]):
        sigs = ", ".join(r["sigs"])
        print(f"  {r['sym']:<12s} LTP={r['ltp']:7.1f} RSI={r['rsi']:.0f} CMF={r['cmf']:+.3f}  [{sigs}]")

print(f"\n{'=' * 85}")
print("OBV DIVERGENCE (smart money buying while price drops)")
print("=" * 85)

for r in results:
    if "OBVDiv" in r["sigs"] and r["rsi"] < 45:
        sigs = ", ".join(r["sigs"])
        print(f"  {r['sym']:<12s} LTP={r['ltp']:7.1f} RSI={r['rsi']:.0f} CMF={r['cmf']:+.3f} 5D={r['chg5d']:+.1f}%  [{sigs}]")
