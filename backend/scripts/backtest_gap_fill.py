#!/usr/bin/env python3
"""
Backtest: Gap Fill Analysis on DSE.

Tests:
1. How often do gaps fill? (within 1, 3, 5, 10 days)
2. Buy at gap fill level — does it bounce?
3. Gap up vs gap down fill rates
4. Volume impact on gap fill probability
5. Gap size impact (small vs large gaps)
6. Trading strategy: wait for gap fill, then buy
"""

import warnings; warnings.filterwarnings("ignore")
import sys, os
import pandas as pd
import numpy as np
import psycopg2

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from config import DATABASE_URL

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

gap_events = []
fill_trades = []

for symbol, sdf in df.groupby("symbol"):
    if len(sdf) < 30:
        continue
    sdf = sdf.sort_values("date").reset_index(drop=True)

    close = sdf["close"]
    high = sdf["high"]
    low = sdf["low"]
    open_ = sdf["open"]
    volume = sdf["volume"].astype(float)
    dates = sdf["date"]
    vol_avg = volume.rolling(20).mean()

    for i in range(1, len(sdf) - 10):
        prev_close = close.iloc[i-1]
        prev_high = high.iloc[i-1]
        prev_low = low.iloc[i-1]
        curr_open = open_.iloc[i]
        curr_close = close.iloc[i]
        curr_vol = volume.iloc[i]
        avg_vol = vol_avg.iloc[i] if pd.notna(vol_avg.iloc[i]) else curr_vol

        if prev_close == 0:
            continue

        gap_pct = (curr_open - prev_close) / prev_close * 100

        # Detect gaps (>1% to filter noise)
        if abs(gap_pct) < 1:
            continue

        is_gap_up = gap_pct > 0
        gap_level = prev_close  # the level that needs to be filled

        # For gap up: gap fills when price drops back to prev_close
        # For gap down: gap fills when price rises back to prev_close

        # Check if gap fills within N days
        fill_day = None
        for d in range(0, min(20, len(sdf) - i)):
            if is_gap_up:
                if low.iloc[i+d] <= gap_level:
                    fill_day = d
                    break
            else:
                if high.iloc[i+d] >= gap_level:
                    fill_day = d
                    break

        filled_1d = fill_day is not None and fill_day <= 1
        filled_3d = fill_day is not None and fill_day <= 3
        filled_5d = fill_day is not None and fill_day <= 5
        filled_10d = fill_day is not None and fill_day <= 10

        vol_ratio = curr_vol / avg_vol if avg_vol > 0 else 1

        gap_events.append({
            "symbol": symbol,
            "date": str(dates.iloc[i].date()),
            "type": "UP" if is_gap_up else "DOWN",
            "gap_pct": round(gap_pct, 2),
            "gap_level": round(gap_level, 1),
            "open": curr_open,
            "close": curr_close,
            "held": (is_gap_up and curr_close > curr_open) or (not is_gap_up and curr_close < curr_open),
            "faded": (is_gap_up and curr_close < curr_open) or (not is_gap_up and curr_close > curr_open),
            "vol_ratio": round(vol_ratio, 2),
            "high_vol": vol_ratio > 1.5,
            "fill_day": fill_day,
            "filled_1d": filled_1d,
            "filled_3d": filled_3d,
            "filled_5d": filled_5d,
            "filled_10d": filled_10d,
        })

        # TRADE: Buy when gap fills (price returns to gap level)
        # For gap up that fills: price dropped to prev_close — buy the bounce
        if fill_day is not None and fill_day > 0 and fill_day <= 10:
            fill_idx = i + fill_day
            if fill_idx + 10 < len(sdf):
                fill_price = close.iloc[fill_idx]
                fwd = {}
                for fd in [1, 2, 3, 5, 10]:
                    if fill_idx + fd < len(sdf):
                        fwd[f"ret_{fd}d"] = (close.iloc[fill_idx + fd] - fill_price) / fill_price * 100
                    else:
                        fwd[f"ret_{fd}d"] = None

                fill_trades.append({
                    "symbol": symbol,
                    "date": str(dates.iloc[fill_idx].date()),
                    "gap_date": str(dates.iloc[i].date()),
                    "gap_type": "UP" if is_gap_up else "DOWN",
                    "gap_pct": round(gap_pct, 2),
                    "fill_day": fill_day,
                    "fill_price": fill_price,
                    "vol_ratio": round(vol_ratio, 2),
                    **fwd,
                })

gdf = pd.DataFrame(gap_events)
tdf = pd.DataFrame(fill_trades)

print(f"Found {len(gdf)} gap events, {len(tdf)} gap fill trade opportunities\n")

# === GAP FILL PROBABILITY ===
print("=" * 70)
print("HOW OFTEN DO GAPS FILL?")
print("=" * 70)

for gap_type in ["UP", "DOWN"]:
    subset = gdf[gdf["type"] == gap_type]
    n = len(subset)
    if n == 0:
        continue
    print(f"\nGAP {gap_type} ({n} events):")
    for period, col in [(1, "filled_1d"), (3, "filled_3d"), (5, "filled_5d"), (10, "filled_10d")]:
        filled = subset[col].sum()
        print(f"  Fills within {period:2d} days: {filled}/{n} ({filled/n*100:.0f}%)")

# By gap size
print(f"\n{'='*70}")
print("FILL RATE BY GAP SIZE")
print(f"{'='*70}")
for gap_type in ["UP", "DOWN"]:
    subset = gdf[gdf["type"] == gap_type]
    print(f"\nGAP {gap_type}:")
    for min_gap, max_gap, label in [(1, 2, "Small (1-2%)"), (2, 4, "Medium (2-4%)"), (4, 100, "Large (4%+)")]:
        if gap_type == "UP":
            sz = subset[(subset["gap_pct"] >= min_gap) & (subset["gap_pct"] < max_gap)]
        else:
            sz = subset[(subset["gap_pct"] <= -min_gap) & (subset["gap_pct"] > -max_gap)]
        if len(sz) < 5:
            continue
        f5 = sz["filled_5d"].sum() / len(sz) * 100
        f10 = sz["filled_10d"].sum() / len(sz) * 100
        print(f"  {label:20s}: {len(sz):4d} events, fills in 5d: {f5:.0f}%, 10d: {f10:.0f}%")

# Volume impact
print(f"\n{'='*70}")
print("DOES VOLUME AFFECT GAP FILL?")
print(f"{'='*70}")
for gap_type in ["UP", "DOWN"]:
    subset = gdf[gdf["type"] == gap_type]
    hv = subset[subset["high_vol"] == True]
    lv = subset[subset["high_vol"] == False]
    if len(hv) > 5 and len(lv) > 5:
        print(f"\nGAP {gap_type}:")
        print(f"  High volume (>1.5x): {len(hv):4d} events, 5d fill: {hv['filled_5d'].sum()/len(hv)*100:.0f}%")
        print(f"  Normal volume:       {len(lv):4d} events, 5d fill: {lv['filled_5d'].sum()/len(lv)*100:.0f}%")

# === GAP FILL TRADING STRATEGY ===
print(f"\n{'='*70}")
print("TRADING STRATEGY: BUY AT GAP FILL LEVEL")
print("(When price returns to fill the gap, does it bounce?)")
print(f"{'='*70}")

def report_trades(name, subset):
    if len(subset) < 5:
        print(f"\n{name}: Too few events ({len(subset)})")
        return
    print(f"\n{name} ({len(subset)} trades):")
    for d in [1, 3, 5, 10]:
        col = f"ret_{d}d"
        valid = subset[col].dropna()
        if len(valid) == 0:
            continue
        win = (valid > 0).sum()
        total = len(valid)
        wr = win / total * 100
        marker = " <<<" if wr >= 60 and d == 5 else ""
        print(f"  Day {d:2d}: Win {win}/{total} ({wr:.0f}%), Avg {valid.mean():+.2f}%, Median {valid.median():+.2f}%{marker}")

report_trades("ALL gap fill trades", tdf)
report_trades("Buy when GAP UP fills (price dropped back)", tdf[tdf["gap_type"] == "UP"])
report_trades("Buy when GAP DOWN fills (price recovered)", tdf[tdf["gap_type"] == "DOWN"])

# Fill speed
report_trades("Gap filled in 1-2 days (fast fill)", tdf[tdf["fill_day"] <= 2])
report_trades("Gap filled in 3-5 days (slow fill)", tdf[(tdf["fill_day"] >= 3) & (tdf["fill_day"] <= 5)])
report_trades("Gap filled in 6-10 days (very slow)", tdf[tdf["fill_day"] > 5])

# Gap size + fill
report_trades("Small gap (1-2%) filled", tdf[tdf["gap_pct"].abs() < 2])
report_trades("Medium gap (2-4%) filled", tdf[(tdf["gap_pct"].abs() >= 2) & (tdf["gap_pct"].abs() < 4)])
report_trades("Large gap (4%+) filled", tdf[tdf["gap_pct"].abs() >= 4])

# === BEST COMBO ===
# Gap up fills + slow fill (3-5 days) — price had time to build support
best = tdf[(tdf["gap_type"] == "UP") & (tdf["fill_day"] >= 3) & (tdf["fill_day"] <= 5)]
report_trades("BEST: Gap UP filled slowly (3-5 days) — support building", best)

# Gap down fills quickly — V-recovery
vshape = tdf[(tdf["gap_type"] == "DOWN") & (tdf["fill_day"] <= 2)]
report_trades("V-RECOVERY: Gap DOWN filled in 1-2 days", vshape)

# === RANKING ===
print(f"\n{'='*70}")
print("GAP FILL STRATEGY RANKING BY 5-DAY WIN RATE")
print(f"{'='*70}\n")

tests = [
    ("All gap fill trades", tdf),
    ("Gap UP fills (buy the dip)", tdf[tdf["gap_type"] == "UP"]),
    ("Gap DOWN fills (buy recovery)", tdf[tdf["gap_type"] == "DOWN"]),
    ("Fast fill (1-2 days)", tdf[tdf["fill_day"] <= 2]),
    ("Slow fill (3-5 days)", tdf[(tdf["fill_day"] >= 3) & (tdf["fill_day"] <= 5)]),
    ("Very slow fill (6-10 days)", tdf[tdf["fill_day"] > 5]),
    ("Small gap filled", tdf[tdf["gap_pct"].abs() < 2]),
    ("Large gap filled", tdf[tdf["gap_pct"].abs() >= 4]),
    ("Gap UP slow fill (best?)", best),
    ("Gap DOWN V-recovery", vshape),
]

print(f"{'Strategy':<42} {'Events':>7} {'5D WR':>7} {'5D Avg':>8} {'10D WR':>7}")
print("-" * 70)
for name, subset in tests:
    if len(subset) < 5:
        continue
    v5 = subset["ret_5d"].dropna()
    v10 = subset["ret_10d"].dropna()
    if len(v5) == 0:
        continue
    wr5 = (v5 > 0).sum() / len(v5) * 100
    wr10 = (v10 > 0).sum() / len(v10) * 100 if len(v10) > 0 else 0
    marker = " ***" if wr5 >= 55 else ""
    print(f"{name:<42} {len(subset):7d} {wr5:6.0f}% {v5.mean():+7.2f}% {wr10:6.0f}%{marker}")

# Recent examples
print(f"\n{'='*70}")
print("RECENT UNFILLED GAPS (still open)")
print(f"{'='*70}")
unfilled = gdf[gdf["filled_10d"] == False].sort_values("date", ascending=False)
for _, g in unfilled.head(15).iterrows():
    dir_sym = "↑" if g["type"] == "UP" else "↓"
    print(f"  {g['symbol']:12s} {g['date']} {dir_sym} gap {g['gap_pct']:+.1f}% gap_level=৳{g['gap_level']} (unfilled — price may return here)")
