#!/usr/bin/env python3
"""5-year comprehensive backtest: Short-term trading vs Long-term investing on DSE."""

import warnings; warnings.filterwarnings("ignore")
import sys, os
import pandas as pd
import numpy as np
import psycopg2

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from config import DATABASE_URL
from ta.momentum import RSIIndicator

conn = psycopg2.connect(DATABASE_URL)

print("Loading 5+ years of data...")
df = pd.read_sql(
    "SELECT dp.symbol, dp.date, dp.open, dp.high, dp.low, dp.close, dp.volume, "
    "f.category, f.pe_ratio, f.eps_ttm, f.dividend_yield_pct "
    "FROM daily_prices dp JOIN fundamentals f ON dp.symbol = f.symbol "
    "WHERE dp.date >= '2021-01-01' AND dp.close > 0 AND f.category IN ('A','B') "
    "AND dp.volume > 1000 ORDER BY dp.symbol, dp.date",
    conn,
)
df["date"] = pd.to_datetime(df["date"])

dsex = pd.read_sql(
    "SELECT date, dsex_index as close FROM dsex_history WHERE dsex_index > 0 ORDER BY date", conn)
dsex["date"] = pd.to_datetime(dsex["date"])
dsex = dsex.set_index("date")
dsex["sma20"] = dsex["close"].rolling(20).mean()
dsex["sma50"] = dsex["close"].rolling(50).mean()
dsex["below_sma20"] = dsex["close"] < dsex["sma20"]
conn.close()

print(f"Loaded {len(df)} rows, {df.symbol.nunique()} stocks, {df.date.min().date()} to {df.date.max().date()}\n")

# Build dataset
print("Building dataset (this takes a few minutes)...")
rows = []
count = 0

for symbol, sdf in df.groupby("symbol"):
    if len(sdf) < 250:  # need at least 1 year
        continue
    count += 1
    sdf = sdf.sort_values("date").reset_index(drop=True)
    close = sdf["close"]
    high = sdf["high"]
    low = sdf["low"]
    volume = sdf["volume"].astype(float)

    rsi = RSIIndicator(close, window=14).rsi()
    vol_avg = volume.rolling(20).mean()

    pe = sdf["pe_ratio"].iloc[0]
    eps = sdf["eps_ttm"].iloc[0]
    div_y = sdf["dividend_yield_pct"].iloc[0] or 0

    # Sample every 5th day to keep dataset manageable
    for i in range(60, len(sdf) - 250, 5):
        c = close.iloc[i]
        dt = sdf["date"].iloc[i]
        rsi_v = rsi.iloc[i] if pd.notna(rsi.iloc[i]) else 50

        dsex_below = None
        if len(dsex.loc[:dt]) > 0:
            r = dsex.loc[:dt].iloc[-1]
            dsex_below = bool(r["below_sma20"]) if pd.notna(r["below_sma20"]) else None

        # Support check (simplified for speed)
        swing_lows = []
        for j in range(max(2, i - 40), i - 2):
            if low.iloc[j] == min(low.iloc[max(0, j - 2):j + 3]):
                swing_lows.append(float(low.iloc[j]))
        at_support = False
        if swing_lows:
            for sl in swing_lows:
                if abs(low.iloc[i] - sl) / sl * 100 < 2:
                    at_support = True
                    break

        # Consecutive red days
        red_days = 0
        for j in range(1, min(6, i)):
            if close.iloc[i - j] < close.iloc[i - j - 1]:
                red_days += 1
            else:
                break

        chg_5d = (c - close.iloc[i - 5]) / close.iloc[i - 5] * 100 if i >= 5 else 0

        # Returns at ALL horizons
        rets = {}
        for d in [3, 5, 10, 20, 60, 120, 250]:
            if i + d < len(sdf):
                rets[f"ret_{d}d"] = round((close.iloc[i + d] - c) / c * 100, 2)
                rets[f"max_{d}d"] = round((high.iloc[i + 1:i + d + 1].max() - c) / c * 100, 2)

        rows.append({
            "symbol": symbol, "date": str(dt.date()), "price": c,
            "rsi": round(rsi_v, 1), "div_yield": div_y,
            "pe": pe, "has_eps": eps is not None and eps > 0,
            "dsex_weak": dsex_below, "at_support": at_support,
            "red_days": red_days, "chg_5d": round(chg_5d, 1),
            **rets,
        })

d = pd.DataFrame(rows)
print(f"Dataset: {len(d)} samples from {count} stocks\n")

sep = "=" * 80


def pr(label, sub, horizons):
    if len(sub) < 20:
        return
    parts = [f"  {label:<30s} N={len(sub):6d}"]
    for h in horizons:
        col = f"ret_{h}d"
        if col in sub.columns:
            v = sub[col].dropna()
            if len(v) > 0:
                wr = (v > 0).sum() / len(v) * 100
                parts.append(f"{h}d: {wr:.0f}%/{v.mean():+.1f}%")
    print("  ".join(parts))


# ============================================================
# SHORT TERM TRADING (5-10 days)
# ============================================================
print(sep)
print("SHORT TERM TRADING (5 YEARS OF DATA)")
print(sep)

print("\n--- ALL entries (baseline) ---")
pr("Random entry", d, [5, 10, 20])

print("\n--- RSI levels ---")
for rsi_max in [20, 25, 30, 35, 40]:
    pr(f"RSI < {rsi_max}", d[d["rsi"] < rsi_max], [5, 10, 20])

print("\n--- DSEX regime ---")
pr("DSEX weak (below SMA20)", d[d["dsex_weak"] == True], [5, 10, 20])
pr("DSEX strong (above SMA20)", d[d["dsex_weak"] == False], [5, 10, 20])

print("\n--- THE PROVEN SETUP: RSI + DSEX ---")
pr("RSI<30 + DSEX weak", d[(d["rsi"] < 30) & (d["dsex_weak"] == True)], [3, 5, 10, 20])
pr("RSI<30 + DSEX strong", d[(d["rsi"] < 30) & (d["dsex_weak"] == False)], [3, 5, 10, 20])
pr("RSI<35 + DSEX weak", d[(d["rsi"] < 35) & (d["dsex_weak"] == True)], [3, 5, 10, 20])
pr("RSI<40 + DSEX weak", d[(d["rsi"] < 40) & (d["dsex_weak"] == True)], [3, 5, 10, 20])

print("\n--- OTHER SHORT SETUPS ---")
pr("3+ red days", d[d["red_days"] >= 3], [5, 10, 20])
pr("3red + DSEX weak", d[(d["red_days"] >= 3) & (d["dsex_weak"] == True)], [5, 10, 20])
pr("5d drop >10%", d[d["chg_5d"] < -10], [5, 10, 20])
pr("5d drop>10% + DSEX weak", d[(d["chg_5d"] < -10) & (d["dsex_weak"] == True)], [5, 10, 20])
pr("At support + RSI<40", d[(d["at_support"]) & (d["rsi"] < 40)], [5, 10, 20])
pr("Sup + RSI<40 + DSEX weak", d[(d["at_support"]) & (d["rsi"] < 40) & (d["dsex_weak"] == True)], [5, 10, 20])

# ============================================================
# LONG TERM INVESTING (60-250 days)
# ============================================================
print(f"\n{sep}")
print("LONG TERM INVESTING (5 YEARS OF DATA)")
print(sep)

valid = d[d.get("ret_250d", pd.Series(dtype=float)).notna()] if "ret_250d" in d.columns else d

print("\n--- 60-day hold ---")
pr("All stocks", valid, [60])
pr("Div yield > 3%", valid[valid["div_yield"] > 3], [60])
pr("Div yield > 5%", valid[valid["div_yield"] > 5], [60])
pr("No dividend", valid[valid["div_yield"] == 0], [60])
pr("PE < 15", valid[(valid["pe"].notna()) & (valid["pe"] < 15)], [60])
pr("PE < 10", valid[(valid["pe"].notna()) & (valid["pe"] < 10)], [60])
pr("PE 10-20", valid[(valid["pe"].notna()) & (valid["pe"] >= 10) & (valid["pe"] < 20)], [60])
pr("Positive EPS", valid[valid["has_eps"]], [60])
pr("DSEX weak entry", valid[valid["dsex_weak"] == True], [60])
pr("DSEX strong entry", valid[valid["dsex_weak"] == False], [60])
pr("RSI<30 entry", valid[valid["rsi"] < 30], [60])

print("\n--- 120-day hold (4 months) ---")
pr("All stocks", valid, [120])
pr("Div yield > 3%", valid[valid["div_yield"] > 3], [120])
pr("Div yield > 5%", valid[valid["div_yield"] > 5], [120])
pr("No dividend", valid[valid["div_yield"] == 0], [120])
pr("PE < 15", valid[(valid["pe"].notna()) & (valid["pe"] < 15)], [120])
pr("Positive EPS", valid[valid["has_eps"]], [120])
pr("DSEX weak entry", valid[valid["dsex_weak"] == True], [120])
pr("DSEX strong entry", valid[valid["dsex_weak"] == False], [120])
pr("RSI<30 entry", valid[valid["rsi"] < 30], [120])
pr("RSI<30 + DSEX weak", valid[(valid["rsi"] < 30) & (valid["dsex_weak"] == True)], [120])
pr("Div>3% + DSEX weak", valid[(valid["div_yield"] > 3) & (valid["dsex_weak"] == True)], [120])

print("\n--- 250-day hold (1 year) ---")
v250 = d[d["ret_250d"].notna()] if "ret_250d" in d.columns else pd.DataFrame()
if len(v250) > 0:
    pr("All stocks", v250, [250])
    pr("Div yield > 3%", v250[v250["div_yield"] > 3], [250])
    pr("Div yield > 5%", v250[v250["div_yield"] > 5], [250])
    pr("No dividend", v250[v250["div_yield"] == 0], [250])
    pr("PE < 15", v250[(v250["pe"].notna()) & (v250["pe"] < 15)], [250])
    pr("Positive EPS", v250[v250["has_eps"]], [250])
    pr("DSEX weak entry", v250[v250["dsex_weak"] == True], [250])
    pr("DSEX strong entry", v250[v250["dsex_weak"] == False], [250])
    pr("RSI<30 entry", v250[v250["rsi"] < 30], [250])
    pr("RSI<30 + DSEX weak", v250[(v250["rsi"] < 30) & (v250["dsex_weak"] == True)], [250])
    pr("Div>3% + PE<15", v250[(v250["div_yield"] > 3) & (v250["pe"].notna()) & (v250["pe"] < 15)], [250])
    pr("Div>3% + DSEX weak", v250[(v250["div_yield"] > 3) & (v250["dsex_weak"] == True)], [250])

# FINAL COMPARISON
print(f"\n{sep}")
print("FINAL COMPARISON: TRADING vs INVESTING (5 YEAR DATA)")
print(sep)
print("""
Compare the best short-term strategy vs best long-term strategy:
- Short: RSI<30 + DSEX weak (check 5d and 10d numbers above)
- Long: Div>3% hold 1 year (check 250d numbers above)

If short-term gives +6% in 5 days and happens 2x/month:
  Annual return = ~24 trades × 6% = ~144% (compounding would be more)

If long-term gives +X% in 250 days:
  Annual return = X%

Which is better?
""")
