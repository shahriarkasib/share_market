#!/usr/bin/env python3
"""Backtest: Does buying at historical support with confirmations work on DSE?

Tests: When price touches a level where it bounced 2+ times before,
and various confirmation signals are present, what happens next?
"""

import warnings; warnings.filterwarnings("ignore")
import sys, os
import pandas as pd
import numpy as np
import psycopg2

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from config import DATABASE_URL

from ta.volume import ChaikinMoneyFlowIndicator
from ta.trend import EMAIndicator
from ta.momentum import RSIIndicator

conn = psycopg2.connect(DATABASE_URL)

print("Loading 6 months of data...")
df = pd.read_sql(
    "SELECT dp.symbol, dp.date, dp.open, dp.high, dp.low, dp.close, dp.volume, f.category "
    "FROM daily_prices dp JOIN fundamentals f ON dp.symbol = f.symbol "
    "WHERE dp.date >= '2025-10-01' AND dp.close > 0 AND f.category IN ('A','B') "
    "ORDER BY dp.symbol, dp.date",
    conn,
)
df["date"] = pd.to_datetime(df["date"])
print(f"Loaded {len(df)} rows, {df.symbol.nunique()} stocks\n")

results = []

for symbol, sdf in df.groupby("symbol"):
    if len(sdf) < 80:
        continue
    sdf = sdf.sort_values("date").reset_index(drop=True)

    close = sdf["close"]
    high = sdf["high"]
    low = sdf["low"]
    volume = sdf["volume"].astype(float)
    open_ = sdf["open"]

    # Indicators
    cmf = ChaikinMoneyFlowIndicator(high, low, close, volume, window=20).chaikin_money_flow()
    ema9 = EMAIndicator(close, window=9).ema_indicator()
    ema21 = EMAIndicator(close, window=21).ema_indicator()
    rsi = RSIIndicator(close, window=14).rsi()
    vol_avg = volume.rolling(20).mean()

    # First pass: find all swing lows in the first 60 bars to build support levels
    # Then scan from bar 60 onwards for touches of those levels

    for i in range(60, len(sdf) - 10):
        curr_low = low.iloc[i]
        curr_close = close.iloc[i]
        curr_open = open_.iloc[i]
        prev_close = close.iloc[i - 1]

        # Find support levels: look back 60 bars for swing lows
        lookback_lows = low.iloc[i - 60 : i]

        # Detect swing lows (3-bar pivot)
        swing_lows = []
        for j in range(2, len(lookback_lows) - 2):
            idx = i - 60 + j
            if low.iloc[idx] == min(low.iloc[idx-2:idx+3]):
                swing_lows.append(float(low.iloc[idx]))

        if len(swing_lows) < 2:
            continue

        # Cluster nearby swing lows (within 1.5%)
        swing_lows.sort()
        clusters = []
        current_cluster = [swing_lows[0]]
        for sl in swing_lows[1:]:
            if abs(sl - current_cluster[-1]) / current_cluster[-1] * 100 <= 1.5:
                current_cluster.append(sl)
            else:
                if len(current_cluster) >= 2:
                    clusters.append(current_cluster)
                current_cluster = [sl]
        if len(current_cluster) >= 2:
            clusters.append(current_cluster)

        if not clusters:
            continue

        # Check if today's low is touching any support cluster
        for cluster in clusters:
            support_price = np.mean(cluster)
            touches = len(cluster)
            dist_pct = abs(curr_low - support_price) / support_price * 100

            # Touch = within 1.5% of support level
            if dist_pct > 1.5:
                continue
            # Must be approaching from above (price dropped TO support)
            if prev_close < support_price * 0.985:
                continue  # already below support = breakdown, not bounce

            # SUPPORT TOUCH EVENT
            cmf_val = cmf.iloc[i] if pd.notna(cmf.iloc[i]) else 0
            rsi_val = rsi.iloc[i] if pd.notna(rsi.iloc[i]) else 50
            vol_v = volume.iloc[i]
            vol_avg_v = vol_avg.iloc[i] if pd.notna(vol_avg.iloc[i]) else vol_v
            ema9_v = ema9.iloc[i] if pd.notna(ema9.iloc[i]) else 0
            ema21_v = ema21.iloc[i] if pd.notna(ema21.iloc[i]) else 0

            # Candle analysis
            body = curr_close - curr_open
            total_range = high.iloc[i] - curr_low
            lower_shadow = min(curr_close, curr_open) - curr_low
            green_candle = curr_close > curr_open
            hammer = lower_shadow > abs(body) * 2 if abs(body) > 0 else False
            bullish_candle = green_candle or hammer

            # Confirmations
            oversold = rsi_val < 40
            cmf_positive = cmf_val > 0
            vol_high = vol_v > vol_avg_v * 1.3 if vol_avg_v > 0 else False
            strong_support = touches >= 3
            candle_ok = bullish_candle

            confirms = sum([oversold, cmf_positive, vol_high, strong_support, candle_ok])

            # Forward returns
            fwd = {}
            for d in [1, 2, 3, 5, 10]:
                if i + d < len(sdf):
                    fwd[f"ret_{d}d"] = (close.iloc[i + d] - curr_close) / curr_close * 100
                else:
                    fwd[f"ret_{d}d"] = None

            results.append({
                "symbol": symbol,
                "date": str(sdf["date"].iloc[i].date()),
                "price": curr_close,
                "support": round(support_price, 1),
                "touches": touches,
                "cmf": round(float(cmf_val), 3),
                "rsi": round(float(rsi_val), 1),
                "oversold": oversold,
                "cmf_positive": cmf_positive,
                "vol_high": vol_high,
                "strong_support": strong_support,
                "bullish_candle": candle_ok,
                "confirms": confirms,
                **fwd,
            })
            break  # only count first matching cluster per bar

rdf = pd.DataFrame(results)
# Deduplicate: same symbol same date
rdf = rdf.drop_duplicates(subset=["symbol", "date"])
print(f"Found {len(rdf)} support touch events in 6 months\n")

# === RESULTS ===

print("=== ALL SUPPORT TOUCHES ===")
for d in [1, 2, 3, 5, 10]:
    col = f"ret_{d}d"
    valid = rdf[col].dropna()
    win = (valid > 0).sum()
    total = len(valid)
    if total > 0:
        print(f"  Day {d:2d}: Win {win}/{total} ({win/total*100:.0f}%), Avg {valid.mean():+.2f}%, Median {valid.median():+.2f}%")

print()

# By number of confirmations
for conf_min in [0, 1, 2, 3, 4]:
    subset = rdf[rdf["confirms"] >= conf_min]
    if len(subset) < 5:
        continue
    v5 = subset["ret_5d"].dropna()
    if len(v5) == 0:
        continue
    wr = (v5 > 0).sum() / len(v5) * 100
    print(f"  {conf_min}+ confirms: {len(subset):4d} events, 5d win {wr:.0f}%, avg {v5.mean():+.2f}%, median {v5.median():+.2f}%")

print()

# Strong support (3+ touches) with confirmations
print("=== STRONG SUPPORT (3+ touches) ===")
strong = rdf[rdf["strong_support"] == True]
for d in [1, 2, 3, 5, 10]:
    col = f"ret_{d}d"
    valid = strong[col].dropna()
    if len(valid) == 0:
        continue
    win = (valid > 0).sum()
    total = len(valid)
    print(f"  Day {d:2d}: Win {win}/{total} ({win/total*100:.0f}%), Avg {valid.mean():+.2f}%, Median {valid.median():+.2f}%")

print()

# Strong support + oversold
print("=== STRONG SUPPORT + RSI OVERSOLD (<40) ===")
combo1 = rdf[(rdf["strong_support"] == True) & (rdf["oversold"] == True)]
print(f"  Events: {len(combo1)}")
for d in [1, 2, 3, 5, 10]:
    col = f"ret_{d}d"
    valid = combo1[col].dropna()
    if len(valid) == 0:
        continue
    win = (valid > 0).sum()
    total = len(valid)
    print(f"  Day {d:2d}: Win {win}/{total} ({win/total*100:.0f}%), Avg {valid.mean():+.2f}%, Median {valid.median():+.2f}%")

print()

# Strong support + oversold + bullish candle
print("=== STRONG SUPPORT + OVERSOLD + BULLISH CANDLE ===")
combo2 = rdf[(rdf["strong_support"] == True) & (rdf["oversold"] == True) & (rdf["bullish_candle"] == True)]
print(f"  Events: {len(combo2)}")
for d in [1, 2, 3, 5, 10]:
    col = f"ret_{d}d"
    valid = combo2[col].dropna()
    if len(valid) == 0:
        continue
    win = (valid > 0).sum()
    total = len(valid)
    print(f"  Day {d:2d}: Win {win}/{total} ({win/total*100:.0f}%), Avg {valid.mean():+.2f}%, Median {valid.median():+.2f}%")

print()

# Strong support + oversold + bullish candle + volume
print("=== FULL COMBO: STRONG SUPPORT + OVERSOLD + BULLISH CANDLE + HIGH VOLUME ===")
combo3 = rdf[(rdf["strong_support"] == True) & (rdf["oversold"] == True) & (rdf["bullish_candle"] == True) & (rdf["vol_high"] == True)]
print(f"  Events: {len(combo3)}")
for d in [1, 2, 3, 5, 10]:
    col = f"ret_{d}d"
    valid = combo3[col].dropna()
    if len(valid) == 0:
        continue
    win = (valid > 0).sum()
    total = len(valid)
    print(f"  Day {d:2d}: Win {win}/{total} ({win/total*100:.0f}%), Avg {valid.mean():+.2f}%, Median {valid.median():+.2f}%")

print()

# Factor breakdown
print("=== INDIVIDUAL FACTOR IMPACT (5-day) ===")
for factor in ["strong_support", "oversold", "cmf_positive", "vol_high", "bullish_candle"]:
    yes = rdf[rdf[factor] == True]["ret_5d"].dropna()
    no = rdf[rdf[factor] == False]["ret_5d"].dropna()
    if len(yes) > 5 and len(no) > 5:
        y_wr = (yes > 0).sum() / len(yes) * 100
        n_wr = (no > 0).sum() / len(no) * 100
        print(f"  {factor:16s}: WITH {len(yes):4d} win {y_wr:.0f}% avg {yes.mean():+.2f}% | WITHOUT {len(no):4d} win {n_wr:.0f}% avg {no.mean():+.2f}%")

print()

# Top winners
print("=== TOP 15 BEST SUPPORT BOUNCES (5-day) ===")
for _, r in rdf.nlargest(15, "ret_5d").iterrows():
    touches = r["touches"]
    print(f"  {r['symbol']:12s} {r['date']} ৳{r['price']:.1f} Sup={r['support']} ({touches}T) → 5d {r['ret_5d']:+.1f}% RSI={r['rsi']:.0f} CMF={r['cmf']:+.3f} conf={r['confirms']}")

print()
print("=== TOP 10 WORST (support broke) ===")
for _, r in rdf.nsmallest(10, "ret_5d").iterrows():
    touches = r["touches"]
    print(f"  {r['symbol']:12s} {r['date']} ৳{r['price']:.1f} Sup={r['support']} ({touches}T) → 5d {r['ret_5d']:+.1f}% RSI={r['rsi']:.0f} CMF={r['cmf']:+.3f} conf={r['confirms']}")

conn.close()
