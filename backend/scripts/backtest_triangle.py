#!/usr/bin/env python3
"""
Backtest: Symmetrical Triangle / Squeeze formations on DSE.

Detection: Price range compressing over 10-20 bars (lower highs + higher lows).
Tests: What happens after the squeeze breaks? Does volume confirm direction?
Also tests on DSEX index.
"""

import warnings; warnings.filterwarnings("ignore")
import sys, os
import pandas as pd
import numpy as np
import psycopg2

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from config import DATABASE_URL

from ta.volatility import BollingerBands
from ta.momentum import RSIIndicator

conn = psycopg2.connect(DATABASE_URL)

# === Load stock data ===
print("Loading stock data...")
df = pd.read_sql(
    "SELECT dp.symbol, dp.date, dp.open, dp.high, dp.low, dp.close, dp.volume "
    "FROM daily_prices dp JOIN fundamentals f ON dp.symbol = f.symbol "
    "WHERE dp.date >= '2025-10-01' AND dp.close > 0 AND f.category IN ('A','B') "
    "AND dp.volume > 0 ORDER BY dp.symbol, dp.date",
    conn,
)
df["date"] = pd.to_datetime(df["date"])

# === Load DSEX data ===
print("Loading DSEX data...")
dsex = pd.read_sql(
    "SELECT date, dsex_index as close, total_volume as volume "
    "FROM dsex_history WHERE dsex_index > 0 AND date >= '2025-10-01' ORDER BY date",
    conn,
)
dsex["date"] = pd.to_datetime(dsex["date"])
dsex["symbol"] = "DSEX"
dsex["open"] = dsex["close"].shift(1).fillna(dsex["close"])
dsex["high"] = dsex["close"]
dsex["low"] = dsex["close"]
dsex["volume"] = dsex["volume"].fillna(0)
conn.close()

print(f"Stocks: {len(df)} rows, {df.symbol.nunique()} symbols")
print(f"DSEX: {len(dsex)} rows\n")


def fwd_ret(close, i):
    fwd = {}
    for d in [1, 2, 3, 5, 10]:
        if i + d < len(close):
            fwd[f"ret_{d}d"] = (close.iloc[i+d] - close.iloc[i]) / close.iloc[i] * 100
        else:
            fwd[f"ret_{d}d"] = None
    return fwd


def detect_triangle(high, low, close, volume, i, lookback=15):
    """Detect symmetrical triangle / squeeze at bar i.

    Triangle = lower highs AND higher lows converging.
    Also checks BB squeeze as alternative detection.
    """
    if i < lookback + 5:
        return None

    h = high.iloc[i-lookback:i].values
    l = low.iloc[i-lookback:i].values
    c = close.iloc[i-lookback:i].values
    v = volume.iloc[i-lookback:i].values

    # Method 1: Converging highs and lows
    # Split into 3 sections and check if range is shrinking
    third = lookback // 3
    range1 = h[:third].max() - l[:third].min()
    range2 = h[third:2*third].max() - l[third:2*third].min()
    range3 = h[2*third:].max() - l[2*third:].min()

    converging = range1 > range2 > range3

    # Method 2: Check for lower highs and higher lows
    # Compare first half highs vs second half highs
    half = lookback // 2
    first_high = h[:half].max()
    second_high = h[half:].max()
    first_low = l[:half].min()
    second_low = l[half:].min()

    lower_highs = second_high < first_high
    higher_lows = second_low > first_low
    symmetrical = lower_highs and higher_lows

    # Method 3: BB Width percentile (squeeze)
    try:
        close_series = close.iloc[i-50:i]
        bb = BollingerBands(close_series, window=20, window_dev=2)
        bb_width = (bb.bollinger_hband() - bb.bollinger_lband()) / bb.bollinger_mavg() * 100
        current_width = bb_width.iloc[-1]
        width_percentile = (bb_width < current_width).sum() / len(bb_width) * 100
        bb_squeeze = width_percentile < 20  # width in bottom 20% = squeeze
    except:
        bb_squeeze = False
        current_width = 0
        width_percentile = 50

    # Compression ratio
    total_range = h.max() - l.min()
    recent_range = range3
    compression = recent_range / total_range if total_range > 0 else 1

    # Avg volume in triangle vs before
    vol_in_triangle = v[half:].mean() if v[half:].mean() > 0 else 1
    vol_before = v[:half].mean() if v[:half].mean() > 0 else 1
    vol_declining = vol_in_triangle < vol_before * 0.8  # volume drying up = more compressed

    if not (converging or symmetrical or bb_squeeze):
        return None

    return {
        "converging": converging,
        "symmetrical": symmetrical,
        "bb_squeeze": bb_squeeze,
        "compression": round(compression, 3),
        "bb_width_pct": round(width_percentile, 1),
        "vol_declining": vol_declining,
        "range1": round(range1, 2),
        "range3": round(range3, 2),
    }


def detect_breakout(high, low, close, volume, i, lookback=15):
    """After a triangle, detect breakout direction and volume."""
    if i < lookback:
        return None

    h_range = high.iloc[i-lookback:i]
    l_range = low.iloc[i-lookback:i]

    upper = h_range.max()
    lower = l_range.min()

    curr_close = close.iloc[i]
    curr_vol = volume.iloc[i]
    avg_vol = volume.iloc[i-20:i].mean() if i >= 20 else curr_vol
    vol_ratio = curr_vol / avg_vol if avg_vol > 0 else 1

    if curr_close > upper:
        return {"direction": "UP", "vol_ratio": round(vol_ratio, 2), "vol_confirmed": vol_ratio > 1.3}
    elif curr_close < lower:
        return {"direction": "DOWN", "vol_ratio": round(vol_ratio, 2), "vol_confirmed": vol_ratio > 1.3}

    return None


def process_data(symbol, sdf):
    """Process one stock/index for triangle patterns."""
    if len(sdf) < 80:
        return []

    sdf = sdf.sort_values("date").reset_index(drop=True)
    close = sdf["close"]
    high = sdf["high"]
    low = sdf["low"]
    volume = sdf["volume"].astype(float)

    results = []
    last_triangle_idx = -20  # avoid duplicate detections

    for i in range(30, len(sdf) - 10):
        if i - last_triangle_idx < 10:
            continue

        # Check for triangle
        for lookback in [15, 20, 10]:
            triangle = detect_triangle(high, low, close, volume, i, lookback)
            if triangle:
                # Check if breakout happens on this bar or next few
                for j in range(i, min(i+5, len(sdf) - 10)):
                    breakout = detect_breakout(high, low, close, volume, j, lookback)
                    if breakout:
                        fwd = fwd_ret(close, j)
                        rsi_val = 50
                        try:
                            rsi = RSIIndicator(close.iloc[:j+1], window=14).rsi()
                            rsi_val = float(rsi.iloc[-1]) if pd.notna(rsi.iloc[-1]) else 50
                        except:
                            pass

                        results.append({
                            "symbol": symbol,
                            "date": str(sdf["date"].iloc[j].date()),
                            "price": float(close.iloc[j]),
                            "direction": breakout["direction"],
                            "vol_confirmed": breakout["vol_confirmed"],
                            "vol_ratio": breakout["vol_ratio"],
                            "compression": triangle["compression"],
                            "symmetrical": triangle["symmetrical"],
                            "bb_squeeze": triangle["bb_squeeze"],
                            "vol_declining": triangle["vol_declining"],
                            "rsi": round(rsi_val, 1),
                            "lookback": lookback,
                            **fwd,
                        })
                        last_triangle_idx = j
                        break
                break

    return results


# === Process all stocks ===
print("Scanning for triangles...")
all_results = []

# DSEX first
dsex_results = process_data("DSEX", dsex)
all_results.extend(dsex_results)
print(f"  DSEX: {len(dsex_results)} triangle breakouts found")

stock_count = 0
for symbol, sdf in df.groupby("symbol"):
    if len(sdf) < 80:
        continue
    stock_count += 1
    results = process_data(symbol, sdf)
    all_results.extend(results)

print(f"  Stocks: {stock_count} processed")
print(f"  Total triangle breakouts: {len(all_results)}\n")

rdf = pd.DataFrame(all_results)

if len(rdf) == 0:
    print("No triangles found!")
    exit()


def report(name, subset):
    if len(subset) == 0:
        print(f"\n{name}: No events\n")
        return
    print(f"\n{'='*70}")
    print(f"{name}")
    print(f"  Events: {len(subset)}")
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


# === REPORTS ===
print("=" * 70)
print("SYMMETRICAL TRIANGLE / SQUEEZE BREAKOUT STUDY — DSE 6 MONTHS")
print("=" * 70)

# All breakouts
report("ALL TRIANGLE BREAKOUTS", rdf)

# By direction
up = rdf[rdf["direction"] == "UP"]
down = rdf[rdf["direction"] == "DOWN"]
report("BREAKOUT UP (bullish)", up)
report("BREAKOUT DOWN (bearish)", down)

# Volume confirmed
report("BREAKOUT UP + VOLUME CONFIRMED (>1.3x avg)", up[up["vol_confirmed"] == True])
report("BREAKOUT UP + NO VOLUME (fake breakout?)", up[up["vol_confirmed"] == False])
report("BREAKOUT DOWN + VOLUME CONFIRMED", down[down["vol_confirmed"] == True])

# Symmetrical triangle specifically
sym = rdf[rdf["symmetrical"] == True]
report("SYMMETRICAL TRIANGLE (lower highs + higher lows)", sym)
report("SYMMETRICAL → UP", sym[sym["direction"] == "UP"])
report("SYMMETRICAL → DOWN", sym[sym["direction"] == "DOWN"])

# BB Squeeze
sq = rdf[rdf["bb_squeeze"] == True]
report("BB SQUEEZE (width in bottom 20%)", sq)
report("BB SQUEEZE → UP", sq[sq["direction"] == "UP"])
report("BB SQUEEZE → DOWN", sq[sq["direction"] == "DOWN"])

# Volume declining in triangle (spring compression)
vd = rdf[rdf["vol_declining"] == True]
report("VOLUME DECLINING IN TRIANGLE (spring compression)", vd)
report("SPRING COMPRESSION → UP + VOL CONFIRMED", vd[(vd["direction"] == "UP") & (vd["vol_confirmed"] == True)])

# Compression ratio
tight = rdf[rdf["compression"] < 0.4]
report("TIGHT COMPRESSION (range < 40% of original)", tight)
report("TIGHT COMPRESSION → UP + VOL", tight[(tight["direction"] == "UP") & (tight["vol_confirmed"] == True)])

# DSEX specific
dsex_df = rdf[rdf["symbol"] == "DSEX"]
report("DSEX TRIANGLE BREAKOUTS", dsex_df)
report("DSEX → UP", dsex_df[dsex_df["direction"] == "UP"])
report("DSEX → DOWN", dsex_df[dsex_df["direction"] == "DOWN"])

# Stocks only (exclude DSEX)
stocks_df = rdf[rdf["symbol"] != "DSEX"]

# === BEST COMBO ===
print(f"\n{'='*70}")
print("BEST COMBO: SYMMETRICAL + VOLUME CONFIRMED + UP")
best = stocks_df[(stocks_df["symmetrical"] == True) & (stocks_df["vol_confirmed"] == True) & (stocks_df["direction"] == "UP")]
report("BEST COMBO", best)

# === FINAL RANKING ===
print(f"\n{'='*70}")
print("TRIANGLE RANKING BY 5-DAY WIN RATE")
print(f"{'='*70}\n")

tests = [
    ("All breakouts", rdf),
    ("Breakout UP", up),
    ("Breakout DOWN", down),
    ("UP + Vol confirmed", up[up["vol_confirmed"] == True]),
    ("UP + No volume", up[up["vol_confirmed"] == False]),
    ("Symmetrical → UP", sym[sym["direction"] == "UP"]),
    ("Symmetrical → DOWN", sym[sym["direction"] == "DOWN"]),
    ("BB Squeeze → UP", sq[sq["direction"] == "UP"]),
    ("BB Squeeze → DOWN", sq[sq["direction"] == "DOWN"]),
    ("Spring (vol declining) → UP + Vol", vd[(vd["direction"] == "UP") & (vd["vol_confirmed"] == True)]),
    ("Tight compression → UP + Vol", tight[(tight["direction"] == "UP") & (tight["vol_confirmed"] == True)]),
    ("DSEX breakouts", dsex_df),
]

print(f"{'Factor':<45} {'Events':>7} {'5D WR':>7} {'5D Avg':>8} {'10D WR':>7} {'10D Avg':>8}")
print("-" * 85)
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
    print(f"{name:<45} {len(subset):7d} {wr5:6.0f}% {v5.mean():+7.2f}% {wr10:6.0f}% {v10.mean():+7.2f}%{marker}")

# Recent examples
print(f"\n{'='*70}")
print("RECENT TRIANGLE BREAKOUTS (last 30 days)")
print(f"{'='*70}\n")
recent = rdf[rdf["date"] >= "2026-03-10"].sort_values("date", ascending=False)
for _, r in recent.head(20).iterrows():
    dir_sym = "↑" if r["direction"] == "UP" else "↓"
    vol = "Vol✓" if r["vol_confirmed"] else "NoVol"
    sym = "Sym" if r["symmetrical"] else ""
    sq = "Sq" if r["bb_squeeze"] else ""
    ret5 = f"{r['ret_5d']:+.1f}%" if pd.notna(r["ret_5d"]) else "?"
    print(f"  {r['symbol']:12s} {r['date']} ৳{r['price']:7.1f} {dir_sym} {vol} {sym} {sq} comp={r['compression']:.2f} → 5d={ret5}")
