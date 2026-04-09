#!/usr/bin/env python3
"""
Backtest: Support/Resistance effectiveness on DSE.
Tests multiple S/R hypotheses:
1. Bounce from historical support (2+, 3+, 5+ touches)
2. Rejection at resistance
3. Resistance breakout with volume
4. Support breakdown
5. Role reversal (old resistance becoming support)
6. Pivot point accuracy
7. EMA as dynamic S/R
8. Combinations: S/R + RSI + candle + volume
"""

import warnings; warnings.filterwarnings("ignore")
import sys, os
import pandas as pd
import numpy as np
import psycopg2

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from config import DATABASE_URL

from ta.momentum import RSIIndicator
from ta.trend import EMAIndicator, SMAIndicator

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


def find_sr_levels(high, low, close, end_idx, lookback=60, tol_pct=1.5):
    """Find support and resistance levels from swing points in lookback window."""
    start = max(0, end_idx - lookback)
    h = high.iloc[start:end_idx]
    l = low.iloc[start:end_idx]
    c = close.iloc[end_idx]

    # Swing highs and lows (3-bar pivot)
    reversals = []
    for j in range(2, len(h) - 2):
        idx = start + j
        if low.iloc[idx] == min(low.iloc[idx-2:idx+3]):
            reversals.append(("S", float(low.iloc[idx])))
        if high.iloc[idx] == max(high.iloc[idx-2:idx+3]):
            reversals.append(("R", float(high.iloc[idx])))

    if not reversals:
        return [], []

    # Cluster
    prices = sorted(set(p for _, p in reversals))
    clusters = []
    current = [prices[0]]
    for p in prices[1:]:
        if abs(p - current[-1]) / current[-1] * 100 <= tol_pct:
            current.append(p)
        else:
            clusters.append(current)
            current = [p]
    clusters.append(current)

    supports = []
    resistances = []
    for cl in clusters:
        avg = np.mean(cl)
        touches = len(cl)
        if touches < 2:
            continue
        if avg < c * 0.995:
            supports.append({"price": avg, "touches": touches})
        elif avg > c * 1.005:
            resistances.append({"price": avg, "touches": touches})

    supports.sort(key=lambda x: -x["price"])  # nearest first
    resistances.sort(key=lambda x: x["price"])  # nearest first
    return supports[:5], resistances[:5]


def report(name, events, note=""):
    if not events:
        print(f"\n{'='*70}\n{name}\n  No events found.\n")
        return None
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
    return rdf


# Collect events
events = {
    # Support bounce by touch count
    "sup_2t": [], "sup_3t": [], "sup_5t": [], "sup_8t": [],
    # Support bounce + RSI oversold
    "sup_3t_rsi30": [], "sup_3t_rsi40": [],
    # Support bounce + green candle
    "sup_3t_green": [], "sup_3t_hammer": [],
    # Support bounce + volume
    "sup_3t_highvol": [], "sup_3t_lowvol": [],
    # BEST COMBO: strong support + oversold + bullish candle
    "sup_best_combo": [],
    # Resistance rejection
    "res_reject_2t": [], "res_reject_3t": [], "res_reject_5t": [],
    # Resistance breakout
    "res_break_2t": [], "res_break_3t": [], "res_break_vol": [],
    # Support breakdown (bearish)
    "sup_break_2t": [], "sup_break_3t": [],
    # Role reversal
    "role_reversal_sup": [],
    # Pivot points
    "at_pivot_p": [], "bounce_pivot_s1": [], "break_pivot_r1": [],
    # EMA bounce
    "ema21_bounce_rsi40": [], "ema50_bounce_rsi40": [],
    # Distance from S/R
    "near_support_1pct": [], "near_resistance_1pct": [],
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

    rsi = RSIIndicator(close, window=14).rsi()
    ema21 = EMAIndicator(close, window=21).ema_indicator()
    ema50 = EMAIndicator(close, window=50).ema_indicator()
    vol_avg = volume.rolling(20).mean()

    prev_resistances = []  # track for role reversal

    for i in range(70, len(sdf) - 10):
        c = close.iloc[i]
        c_prev = close.iloc[i-1]
        o = open_.iloc[i]
        h = high.iloc[i]
        l = low.iloc[i]
        v = volume.iloc[i]
        rsi_v = rsi.iloc[i] if pd.notna(rsi.iloc[i]) else 50
        va = vol_avg.iloc[i] if pd.notna(vol_avg.iloc[i]) else v
        ema21_v = ema21.iloc[i] if pd.notna(ema21.iloc[i]) else 0
        ema50_v = ema50.iloc[i] if pd.notna(ema50.iloc[i]) else 0

        green = c > o
        body = abs(c - o)
        total_range = h - l
        lower_shadow = min(c, o) - l
        hammer = lower_shadow > body * 2 if body > 0 and total_range > 0 else False
        high_vol = v > va * 1.5 if va > 0 else False
        low_vol = v < va * 0.7 if va > 0 else False

        fwd = fwd_ret(close, i)
        base = {"symbol": symbol, "date": str(sdf["date"].iloc[i].date()), "price": c, **fwd}

        # Get S/R levels
        sups, ress = find_sr_levels(high, low, close, i, lookback=60)

        # === SUPPORT BOUNCE ===
        for sup in sups[:3]:
            sp = sup["price"]
            touches = sup["touches"]
            dist = abs(l - sp) / sp * 100

            if dist < 1.5:  # touching support
                if touches >= 2:
                    events["sup_2t"].append(base)
                if touches >= 3:
                    events["sup_3t"].append(base)
                    if rsi_v < 30:
                        events["sup_3t_rsi30"].append(base)
                    if rsi_v < 40:
                        events["sup_3t_rsi40"].append(base)
                    if green:
                        events["sup_3t_green"].append(base)
                    if hammer:
                        events["sup_3t_hammer"].append(base)
                    if high_vol:
                        events["sup_3t_highvol"].append(base)
                    if low_vol:
                        events["sup_3t_lowvol"].append(base)
                    if rsi_v < 40 and (green or hammer):
                        events["sup_best_combo"].append(base)
                if touches >= 5:
                    events["sup_5t"].append(base)
                if touches >= 8:
                    events["sup_8t"].append(base)
                break

            if dist < 1:
                events["near_support_1pct"].append(base)

        # === RESISTANCE ===
        for res in ress[:3]:
            rp = res["price"]
            touches = res["touches"]
            dist = abs(h - rp) / rp * 100

            # Rejection: high touched resistance but closed below
            if dist < 1.5 and c < rp:
                if touches >= 2:
                    events["res_reject_2t"].append(base)
                if touches >= 3:
                    events["res_reject_3t"].append(base)
                if touches >= 5:
                    events["res_reject_5t"].append(base)
                break

            # Breakout: closed above resistance
            if c > rp and c_prev <= rp:
                if touches >= 2:
                    events["res_break_2t"].append(base)
                if touches >= 3:
                    events["res_break_3t"].append(base)
                if touches >= 2 and high_vol:
                    events["res_break_vol"].append(base)
                # Track for role reversal
                prev_resistances.append({"price": rp, "date_idx": i})
                break

            if dist < 1:
                events["near_resistance_1pct"].append(base)

        # === SUPPORT BREAKDOWN ===
        for sup in sups[:2]:
            sp = sup["price"]
            touches = sup["touches"]
            if c < sp and c_prev >= sp:
                if touches >= 2:
                    events["sup_break_2t"].append(base)
                if touches >= 3:
                    events["sup_break_3t"].append(base)
                break

        # === ROLE REVERSAL ===
        for pr in prev_resistances[-5:]:
            rp = pr["price"]
            days_since = i - pr["date_idx"]
            if 3 <= days_since <= 30:
                dist = abs(l - rp) / rp * 100
                if dist < 1.5 and c > rp:  # old resistance now support, bouncing
                    events["role_reversal_sup"].append(base)
                    break

        # === PIVOT POINTS ===
        if i >= 1:
            ph = high.iloc[i-1]
            pl = low.iloc[i-1]
            pc = close.iloc[i-1]
            pivot = (ph + pl + pc) / 3
            r1 = 2 * pivot - pl
            s1 = 2 * pivot - ph

            if abs(c - pivot) / pivot * 100 < 0.5:
                events["at_pivot_p"].append(base)
            if abs(l - s1) / s1 * 100 < 1 and c > s1:
                events["bounce_pivot_s1"].append(base)
            if c > r1 and c_prev <= r1:
                events["break_pivot_r1"].append(base)

        # === EMA BOUNCE + OVERSOLD ===
        if ema21_v > 0 and abs(l - ema21_v) / ema21_v * 100 < 1 and c > ema21_v and rsi_v < 40:
            events["ema21_bounce_rsi40"].append(base)
        if ema50_v > 0 and abs(l - ema50_v) / ema50_v * 100 < 1 and c > ema50_v and rsi_v < 40:
            events["ema50_bounce_rsi40"].append(base)

# Deduplicate each
for key in events:
    if events[key]:
        edf = pd.DataFrame(events[key]).drop_duplicates(subset=["symbol", "date"])
        events[key] = edf.to_dict("records")

print(f"Processed {stock_count} stocks\n")

# === REPORTS ===
print("=" * 70)
print("SUPPORT / RESISTANCE FACTOR STUDY — DSE 6 MONTHS")
print("=" * 70)

print("\n### SUPPORT BOUNCE ###")
report("Support Touch (2+ touches)", events["sup_2t"])
report("Support Touch (3+ touches)", events["sup_3t"])
report("Support Touch (5+ touches)", events["sup_5t"])
report("Support Touch (8+ touches)", events["sup_8t"])

print("\n### SUPPORT + CONFIRMATIONS ###")
report("Support 3T + RSI < 30", events["sup_3t_rsi30"])
report("Support 3T + RSI < 40", events["sup_3t_rsi40"])
report("Support 3T + Green Candle", events["sup_3t_green"])
report("Support 3T + Hammer", events["sup_3t_hammer"])
report("Support 3T + High Volume (>1.5x)", events["sup_3t_highvol"])
report("Support 3T + Low Volume (<0.7x)", events["sup_3t_lowvol"])
report("BEST: Support 3T + RSI<40 + Bullish Candle", events["sup_best_combo"])

print("\n### RESISTANCE ###")
report("Resistance Rejection (2+ touches)", events["res_reject_2t"])
report("Resistance Rejection (3+ touches)", events["res_reject_3t"])
report("Resistance Rejection (5+ touches)", events["res_reject_5t"])
report("Resistance Breakout (2+ touches)", events["res_break_2t"])
report("Resistance Breakout (3+ touches)", events["res_break_3t"])
report("Resistance Breakout + High Volume", events["res_break_vol"])

print("\n### BREAKDOWN & ROLE REVERSAL ###")
report("Support Breakdown (2+ touches broke)", events["sup_break_2t"])
report("Support Breakdown (3+ touches broke)", events["sup_break_3t"])
report("Role Reversal (old resistance = new support bounce)", events["role_reversal_sup"])

print("\n### PIVOT POINTS ###")
report("Price at Pivot Point (within 0.5%)", events["at_pivot_p"])
report("Bounce from Pivot S1", events["bounce_pivot_s1"])
report("Breakout above Pivot R1", events["break_pivot_r1"])

print("\n### EMA + OVERSOLD ###")
report("EMA21 Bounce + RSI < 40", events["ema21_bounce_rsi40"])
report("EMA50 Bounce + RSI < 40", events["ema50_bounce_rsi40"])

print("\n### PROXIMITY ###")
report("Within 1% of Support", events["near_support_1pct"])
report("Within 1% of Resistance", events["near_resistance_1pct"])

# === FINAL RANKING ===
print(f"\n{'='*70}")
print("SUPPORT/RESISTANCE RANKING BY 5-DAY WIN RATE")
print(f"{'='*70}\n")

all_r = {}
names = {
    "sup_2t": "Support 2T touch", "sup_3t": "Support 3T touch",
    "sup_5t": "Support 5T touch", "sup_8t": "Support 8T touch",
    "sup_3t_rsi30": "Support 3T + RSI<30", "sup_3t_rsi40": "Support 3T + RSI<40",
    "sup_3t_green": "Support 3T + Green", "sup_3t_hammer": "Support 3T + Hammer",
    "sup_3t_highvol": "Support 3T + HighVol", "sup_3t_lowvol": "Support 3T + LowVol",
    "sup_best_combo": "Support 3T + RSI<40 + Bullish",
    "res_reject_2t": "Resistance Reject 2T", "res_reject_3t": "Resistance Reject 3T",
    "res_reject_5t": "Resistance Reject 5T",
    "res_break_2t": "Resistance Break 2T", "res_break_3t": "Resistance Break 3T",
    "res_break_vol": "Resistance Break + Vol",
    "sup_break_2t": "Support Breakdown 2T", "sup_break_3t": "Support Breakdown 3T",
    "role_reversal_sup": "Role Reversal (R→S bounce)",
    "at_pivot_p": "At Pivot P", "bounce_pivot_s1": "Bounce from S1",
    "break_pivot_r1": "Break above R1",
    "ema21_bounce_rsi40": "EMA21 Bounce + RSI<40",
    "ema50_bounce_rsi40": "EMA50 Bounce + RSI<40",
    "near_support_1pct": "Near Support <1%", "near_resistance_1pct": "Near Resistance <1%",
}

for key, evts in events.items():
    if len(evts) < 10:
        continue
    edf = pd.DataFrame(evts)
    v5 = edf["ret_5d"].dropna()
    if len(v5) == 0:
        continue
    wr = (v5 > 0).sum() / len(v5) * 100
    all_r[key] = {"name": names.get(key, key), "n": len(edf), "wr": wr, "avg": v5.mean()}

ranked = sorted(all_r.items(), key=lambda x: -x[1]["wr"])
print(f"{'Rank':>4} {'Factor':<40} {'Events':>7} {'5D WR':>7} {'5D Avg':>8}")
print("-" * 70)
for rank, (key, info) in enumerate(ranked, 1):
    marker = " ***" if info["wr"] >= 60 else ""
    print(f"{rank:4d} {info['name']:<40} {info['n']:7d} {info['wr']:6.0f}% {info['avg']:+7.2f}%{marker}")
