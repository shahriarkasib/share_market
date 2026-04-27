#!/usr/bin/env python3
"""
Dimensional Analysis: How does the 79% support+oversold setup vary by:
1. DSEX regime (green vs red day, trending vs choppy)
2. Category (A vs B)
3. P/E ratio (cheap vs expensive vs no data)
4. EPS (positive vs negative)
5. Dividend yield (high vs low vs none)
6. Volume/liquidity (high vs low)
7. Sector
8. Market breadth (strong vs weak)
9. Stock price level (penny vs mid vs high)
10. ADX (trending vs choppy)
"""

import warnings; warnings.filterwarnings("ignore")
import sys, os
import pandas as pd
import numpy as np
import psycopg2

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from config import DATABASE_URL

from ta.momentum import RSIIndicator
from ta.trend import ADXIndicator

conn = psycopg2.connect(DATABASE_URL)

print("Loading data...")
df = pd.read_sql(
    "SELECT dp.symbol, dp.date, dp.open, dp.high, dp.low, dp.close, dp.volume, "
    "f.category, f.pe_ratio, f.eps_ttm, f.dividend_yield_pct, f.sector "
    "FROM daily_prices dp JOIN fundamentals f ON dp.symbol = f.symbol "
    "WHERE dp.date >= '2025-10-01' AND dp.close > 0 AND f.category IN ('A','B') "
    "AND dp.volume > 0 ORDER BY dp.symbol, dp.date",
    conn,
)
df["date"] = pd.to_datetime(df["date"])

# DSEX data
dsex = pd.read_sql(
    "SELECT date, dsex_index as close FROM dsex_history WHERE dsex_index > 0 ORDER BY date",
    conn,
)
dsex["date"] = pd.to_datetime(dsex["date"])
dsex = dsex.set_index("date")
dsex["dsex_chg"] = dsex["close"].pct_change() * 100
dsex["dsex_sma20"] = dsex["close"].rolling(20).mean()
dsex["dsex_up"] = dsex["dsex_chg"] > 0
dsex["dsex_trending"] = dsex["close"] > dsex["dsex_sma20"]

# Market breadth proxy: count advances/declines per date
breadth = df.groupby("date").apply(lambda g: pd.Series({
    "advances": (g["close"] > g["open"]).sum(),
    "declines": (g["close"] < g["open"]).sum(),
}))
breadth["strong_breadth"] = breadth["advances"] > breadth["declines"] * 1.5
breadth["weak_breadth"] = breadth["declines"] > breadth["advances"] * 1.5

conn.close()
print(f"Loaded {len(df)} rows, {df.symbol.nunique()} stocks\n")

# === FIND ALL SUPPORT+OVERSOLD SETUPS ===
results = []

for symbol, sdf in df.groupby("symbol"):
    if len(sdf) < 60:
        continue
    sdf = sdf.sort_values("date").reset_index(drop=True)
    close = sdf["close"]
    high = sdf["high"]
    low = sdf["low"]
    open_ = sdf["open"]
    volume = sdf["volume"].astype(float)

    rsi = RSIIndicator(close, window=14).rsi()
    try:
        adx_obj = ADXIndicator(high, low, close, window=14)
        adx = adx_obj.adx()
    except:
        adx = pd.Series(np.nan, index=close.index)
    vol_avg = volume.rolling(20).mean()

    cat = sdf["category"].iloc[0]
    pe = sdf["pe_ratio"].iloc[0]
    eps = sdf["eps_ttm"].iloc[0]
    div_yield = sdf["dividend_yield_pct"].iloc[0]
    sector = sdf["sector"].iloc[0]

    for i in range(60, len(sdf) - 10):
        c = close.iloc[i]
        o = open_.iloc[i]
        l_val = low.iloc[i]
        rsi_v = rsi.iloc[i] if pd.notna(rsi.iloc[i]) else 50
        adx_v = adx.iloc[i] if pd.notna(adx.iloc[i]) else 20
        v = volume.iloc[i]
        va = vol_avg.iloc[i] if pd.notna(vol_avg.iloc[i]) else v
        dt = sdf["date"].iloc[i]

        if rsi_v >= 40:
            continue

        # Support check
        swing_lows = []
        for j in range(max(2, i-60), i-2):
            if low.iloc[j] == min(low.iloc[max(0,j-2):j+3]):
                swing_lows.append(float(low.iloc[j]))
        if len(swing_lows) < 3:
            continue

        swing_lows.sort()
        clusters = []
        curr = [swing_lows[0]]
        for sl in swing_lows[1:]:
            if abs(sl - curr[-1]) / curr[-1] * 100 <= 1.5:
                curr.append(sl)
            else:
                if len(curr) >= 3:
                    clusters.append(curr)
                curr = [sl]
        if len(curr) >= 3:
            clusters.append(curr)

        at_support = False
        for cl in clusters:
            avg = np.mean(cl)
            if abs(l_val - avg) / avg * 100 < 2:
                at_support = True
                break
        if not at_support:
            continue

        # Forward returns
        ret_5d = (close.iloc[i+5] - c) / c * 100 if i+5 < len(sdf) else None
        ret_10d = (close.iloc[i+10] - c) / c * 100 if i+10 < len(sdf) else None

        # DSEX info for this date
        dsex_up = None
        dsex_trend = None
        if dt in dsex.index:
            dsex_up = bool(dsex.loc[dt, "dsex_up"]) if pd.notna(dsex.loc[dt, "dsex_up"]) else None
            dsex_trend = bool(dsex.loc[dt, "dsex_trending"]) if pd.notna(dsex.loc[dt, "dsex_trending"]) else None
        elif len(dsex.loc[:dt]) > 0:
            last = dsex.loc[:dt].iloc[-1]
            dsex_up = bool(last["dsex_up"]) if pd.notna(last["dsex_up"]) else None
            dsex_trend = bool(last["dsex_trending"]) if pd.notna(last["dsex_trending"]) else None

        # Breadth
        strong_b = None
        weak_b = None
        if dt in breadth.index:
            strong_b = bool(breadth.loc[dt, "strong_breadth"])
            weak_b = bool(breadth.loc[dt, "weak_breadth"])

        results.append({
            "symbol": symbol,
            "date": str(dt.date()),
            "price": c,
            "rsi": rsi_v,
            "adx": adx_v,
            "volume": v,
            "vol_avg": va,
            "category": cat,
            "pe_ratio": pe,
            "eps": eps,
            "div_yield": div_yield,
            "sector": sector or "Unknown",
            "dsex_up": dsex_up,
            "dsex_trending": dsex_trend,
            "strong_breadth": strong_b,
            "weak_breadth": weak_b,
            "ret_5d": ret_5d,
            "ret_10d": ret_10d,
        })

rdf = pd.DataFrame(results).drop_duplicates(subset=["symbol", "date"])
rdf_valid = rdf[rdf["ret_5d"].notna()]

print(f"Total support+oversold events: {len(rdf_valid)}")
total_wr = (rdf_valid["ret_5d"] > 0).sum() / len(rdf_valid) * 100
print(f"Overall 5D win rate: {total_wr:.0f}%\n")


def split_report(name, col, bins=None, labels=None):
    """Report win rate split by a dimension."""
    print(f"\n{'='*60}")
    print(f"DIMENSION: {name}")
    print(f"{'='*60}")

    if bins is not None:
        rdf_valid[f"_{col}_bin"] = pd.cut(rdf_valid[col], bins=bins, labels=labels)
        groups = rdf_valid.groupby(f"_{col}_bin")
    elif rdf_valid[col].dtype == bool or rdf_valid[col].dtype == object:
        groups = rdf_valid.groupby(col)
    else:
        groups = rdf_valid.groupby(col)

    print(f"{'Group':<25} {'Events':>7} {'5D WR':>7} {'5D Avg':>8} {'10D WR':>7} {'10D Avg':>8}")
    print("-" * 65)

    for group_name, group in groups:
        if len(group) < 5:
            continue
        v5 = group["ret_5d"].dropna()
        v10 = group["ret_10d"].dropna()
        if len(v5) == 0:
            continue
        wr5 = (v5 > 0).sum() / len(v5) * 100
        wr10 = (v10 > 0).sum() / len(v10) * 100 if len(v10) > 0 else 0
        marker = " ***" if wr5 >= 65 else " !" if wr5 < 45 else ""
        print(f"{str(group_name):<25} {len(group):7d} {wr5:6.0f}% {v5.mean():+7.2f}% {wr10:6.0f}% {v10.mean():+7.2f}%{marker}")


# === DIMENSION ANALYSIS ===

# 1. DSEX regime
split_report("DSEX Day (green vs red)", "dsex_up")

# 2. DSEX Trend (above/below SMA20)
split_report("DSEX Trend (above SMA20?)", "dsex_trending")

# 3. Market Breadth
rdf_valid["breadth"] = "Normal"
rdf_valid.loc[rdf_valid["strong_breadth"] == True, "breadth"] = "Strong (adv>1.5x dec)"
rdf_valid.loc[rdf_valid["weak_breadth"] == True, "breadth"] = "Weak (dec>1.5x adv)"
split_report("Market Breadth", "breadth")

# 4. Category
split_report("Stock Category", "category")

# 5. P/E Ratio
rdf_valid["pe_group"] = "No Data"
rdf_valid.loc[rdf_valid["pe_ratio"].notna() & (rdf_valid["pe_ratio"] < 15), "pe_group"] = "Cheap (<15)"
rdf_valid.loc[rdf_valid["pe_ratio"].notna() & (rdf_valid["pe_ratio"] >= 15) & (rdf_valid["pe_ratio"] < 30), "pe_group"] = "Fair (15-30)"
rdf_valid.loc[rdf_valid["pe_ratio"].notna() & (rdf_valid["pe_ratio"] >= 30), "pe_group"] = "Expensive (30+)"
split_report("P/E Ratio", "pe_group")

# 6. EPS
rdf_valid["eps_group"] = "No Data"
rdf_valid.loc[rdf_valid["eps"].notna() & (rdf_valid["eps"] > 0), "eps_group"] = "Positive EPS"
rdf_valid.loc[rdf_valid["eps"].notna() & (rdf_valid["eps"] <= 0), "eps_group"] = "Negative EPS"
split_report("EPS", "eps_group")

# 7. Dividend Yield
rdf_valid["div_group"] = "No Dividend"
rdf_valid.loc[rdf_valid["div_yield"].notna() & (rdf_valid["div_yield"] > 3), "div_group"] = "High Yield (>3%)"
rdf_valid.loc[rdf_valid["div_yield"].notna() & (rdf_valid["div_yield"] > 0) & (rdf_valid["div_yield"] <= 3), "div_group"] = "Low Yield (0-3%)"
split_report("Dividend Yield", "div_group")

# 8. Stock Price Level
split_report("Stock Price", "price",
             bins=[0, 10, 30, 100, 500, 99999],
             labels=["Penny (<10)", "Low (10-30)", "Mid (30-100)", "High (100-500)", "Premium (500+)"])

# 9. Volume/Liquidity
rdf_valid["liq_group"] = "Normal"
rdf_valid.loc[rdf_valid["volume"] > 500000, "liq_group"] = "High Vol (>500K)"
rdf_valid.loc[rdf_valid["volume"] < 50000, "liq_group"] = "Low Vol (<50K)"
split_report("Liquidity", "liq_group")

# 10. ADX (trending vs choppy)
split_report("ADX (Trend Strength)", "adx",
             bins=[0, 15, 25, 40, 100],
             labels=["Choppy (<15)", "Weak (15-25)", "Moderate (25-40)", "Strong (40+)"])

# 11. RSI level
split_report("RSI Level", "rsi",
             bins=[0, 25, 30, 35, 40],
             labels=["Extreme (<25)", "Deep (25-30)", "Oversold (30-35)", "Mild (35-40)"])

# 12. Sector
split_report("Sector", "sector")

# === SUMMARY ===
print(f"\n{'='*60}")
print("SUMMARY: WHICH DIMENSIONS MATTER MOST?")
print(f"{'='*60}")
print("""
Look for dimensions where win rate varies significantly (>10% difference).
If a dimension shows ~55% in all groups, it doesn't matter.
If it shows 70% in one group and 40% in another, THAT'S what matters.
""")
