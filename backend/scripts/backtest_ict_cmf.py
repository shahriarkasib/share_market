#!/usr/bin/env python3
"""
Backtest: ICT sequence WITH CMF direction filter.
The APEXFOODS framework:
  1. ChoCh (break above LH) — CMF can be negative but must be RISING
  2. BOS (new HH) — CMF should be near zero or positive
  3. Entry at BOS with CMF > 0 or CMF rising toward 0

WIN = Close +2%+ on Day 3, 4, or 5 (T+2 real metric)
NO DSEX filter.
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

conn = psycopg2.connect(DATABASE_URL)

print("Loading 5 years...")
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


def real_win(close, entry_price, entry_idx):
    if entry_idx + 5 >= len(close):
        return None
    for d in [3, 4, 5]:
        if (close.iloc[entry_idx + d] - entry_price) / entry_price * 100 >= 2:
            return True
    return False


def max_gain_3to5(close, high, entry_price, entry_idx):
    if entry_idx + 5 >= len(close):
        return None
    return (high.iloc[entry_idx + 3:entry_idx + 6].max() - entry_price) / entry_price * 100


events = {
    # ChoCh only
    "choch": [],
    # ChoCh + CMF rising
    "choch_cmf_rising": [],
    # BOS only
    "bos": [],
    # BOS + CMF positive
    "bos_cmf_pos": [],
    # BOS + CMF rising (was more negative, now less)
    "bos_cmf_rising": [],
    # BOS + CMF > -0.05 (near zero or positive)
    "bos_cmf_near0": [],
    # THE FULL APEXFOODS FRAMEWORK:
    # ChoCh happened with CMF rising → then BOS with CMF positive
    "full_framework": [],
    # Full framework + green candle at BOS
    "full_green": [],
    # Full framework + volume above avg
    "full_vol": [],
    # Full framework + RSI 40-65 (momentum but not overbought)
    "full_rsi": [],
    # The complete package
    "complete": [],
    # OB pullback after BOS with CMF positive
    "ob_cmf_pos": [],
    # BOS + CMF crossed zero in last 5 bars
    "bos_cmf_cross0": [],
}

stock_count = 0
for symbol, sdf in df.groupby("symbol"):
    if len(sdf) < 100:
        continue
    stock_count += 1
    sdf = sdf.sort_values("date").reset_index(drop=True)
    close = sdf["close"]
    high = sdf["high"]
    low = sdf["low"]
    open_ = sdf["open"]
    volume = sdf["volume"].astype(float)

    rsi = RSIIndicator(close, window=14).rsi()
    cmf = ChaikinMoneyFlowIndicator(high, low, close, volume, window=20).chaikin_money_flow()
    vol_avg = volume.rolling(20).mean()

    # CMF 5-bar change (is it rising?)
    cmf_5bar_change = cmf - cmf.shift(5)

    # Find swing points
    swing_highs = []
    swing_lows = []
    for i in range(3, len(sdf) - 3):
        if high.iloc[i] == max(high.iloc[i - 3:i + 4]):
            swing_highs.append((i, float(high.iloc[i])))
        if low.iloc[i] == min(low.iloc[i - 3:i + 4]):
            swing_lows.append((i, float(low.iloc[i])))

    # Look for the ICT sequence
    for i in range(40, len(sdf) - 5):
        c = close.iloc[i]
        o = open_.iloc[i]
        v = volume.iloc[i]
        rsi_v = rsi.iloc[i] if pd.notna(rsi.iloc[i]) else 50
        cmf_v = cmf.iloc[i] if pd.notna(cmf.iloc[i]) else 0
        cmf_change = cmf_5bar_change.iloc[i] if pd.notna(cmf_5bar_change.iloc[i]) else 0
        cmf_prev5 = cmf.iloc[i - 5] if pd.notna(cmf.iloc[i - 5]) else 0
        va = vol_avg.iloc[i] if pd.notna(vol_avg.iloc[i]) else v

        green = c > o
        vol_high = v > va * 1.3 if va > 0 else False

        # CMF direction
        cmf_rising = cmf_change > 0.05  # CMF improved by 0.05+ in 5 bars
        cmf_positive = cmf_v > 0
        cmf_near_zero = cmf_v > -0.05
        cmf_crossed_zero = cmf_prev5 < 0 and cmf_v > 0  # was negative, now positive

        # Find recent swing structure
        recent_sh = [(idx, p) for idx, p in swing_highs if i - 30 < idx < i]
        recent_sl = [(idx, p) for idx, p in swing_lows if i - 30 < idx < i]

        if len(recent_sh) < 2 or len(recent_sl) < 2:
            continue

        # Downtrend: lower lows
        has_ll = recent_sl[-1][1] < recent_sl[-2][1]
        if not has_ll:
            continue

        # LH = the last high before the LL
        lh_price = recent_sh[-1][1]
        lh_idx = recent_sh[-1][0]

        # ChoCh: price breaks above LH
        if c <= lh_price:
            continue

        # This IS a ChoCh bar
        w = real_win(close, c, i)
        gain = max_gain_3to5(close, high, c, i)
        base = {"win": w, "gain": gain, "symbol": symbol,
                "date": str(sdf["date"].iloc[i].date()), "price": c,
                "cmf": round(float(cmf_v), 3), "rsi": round(float(rsi_v), 1)}

        events["choch"].append(base)
        if cmf_rising:
            events["choch_cmf_rising"].append(base)

        # BOS: does price make a new HH after ChoCh?
        # Check if current bar or recent bars broke the previous swing high
        prev_hh = recent_sh[-1][1]
        if high.iloc[i] > prev_hh:
            # This is also BOS
            events["bos"].append(base)

            if cmf_positive:
                events["bos_cmf_pos"].append(base)
            if cmf_rising:
                events["bos_cmf_rising"].append(base)
            if cmf_near_zero:
                events["bos_cmf_near0"].append(base)
            if cmf_crossed_zero:
                events["bos_cmf_cross0"].append(base)

            # THE FULL APEXFOODS FRAMEWORK:
            # BOS + CMF was rising at ChoCh + CMF now positive or near zero
            if cmf_rising and cmf_near_zero:
                events["full_framework"].append(base)
                if green:
                    events["full_green"].append(base)
                if vol_high:
                    events["full_vol"].append(base)
                if 40 < rsi_v < 65:
                    events["full_rsi"].append(base)
                if green and 40 < rsi_v < 65:
                    events["complete"].append(base)

        # OB pullback after BOS with CMF positive
        # Look back for BOS that happened in last 10 bars, now pulling back
        for j in range(max(0, i - 10), i):
            sh_at_j = [(idx, p) for idx, p in swing_highs if idx == j]
            if sh_at_j:
                if c < sh_at_j[0][1] and c > recent_sl[-1][1]:
                    # Price is between last HL and last HH = pullback zone
                    # Find order block
                    for k in range(j - 1, max(j - 8, 0), -1):
                        if close.iloc[k] < open_.iloc[k]:  # bearish candle = OB
                            ob_low = low.iloc[k]
                            ob_high = high.iloc[k]
                            if low.iloc[i] <= ob_high * 1.01 and c >= ob_low:
                                if cmf_positive:
                                    events["ob_cmf_pos"].append(base)
                                break
                    break

    if stock_count % 50 == 0:
        print(f"  Processed {stock_count} stocks...")

print(f"\nProcessed {stock_count} stocks\n")

# === RESULTS ===
print("=" * 75)
print("ICT + CMF DIRECTION FRAMEWORK")
print("WIN = Close +2%+ on Day 3, 4, or 5 | NO DSEX filter")
print("=" * 75)

order = ["choch", "choch_cmf_rising", "bos", "bos_cmf_rising", "bos_cmf_near0",
         "bos_cmf_pos", "bos_cmf_cross0", "full_framework", "full_green",
         "full_vol", "full_rsi", "complete", "ob_cmf_pos"]

labels = {
    "choch": "1. ChoCh only (baseline)",
    "choch_cmf_rising": "2. ChoCh + CMF RISING (+0.05 in 5 bars)",
    "bos": "3. BOS (break of structure)",
    "bos_cmf_rising": "4. BOS + CMF rising",
    "bos_cmf_near0": "5. BOS + CMF > -0.05",
    "bos_cmf_pos": "6. BOS + CMF POSITIVE (safe entry)",
    "bos_cmf_cross0": "7. BOS + CMF just crossed zero",
    "full_framework": "8. FULL: BOS + CMF rising + CMF>-0.05",
    "full_green": "9. FULL + Green candle",
    "full_vol": "10. FULL + High volume",
    "full_rsi": "11. FULL + RSI 40-65",
    "complete": "12. COMPLETE: FULL + Green + RSI 40-65",
    "ob_cmf_pos": "13. OB pullback + CMF positive",
}

print(f"\n{'Strategy':<50s} {'Events':>7s} {'Win%':>7s} {'AvgGain':>8s}")
print("-" * 75)

for key in order:
    evts = events[key]
    if len(evts) < 10:
        print(f"{labels[key]:<50s} {len(evts):7d}    too few")
        continue
    wins = [e["win"] for e in evts if e["win"] is not None]
    gains = [e["gain"] for e in evts if e.get("gain") is not None]
    if not wins:
        continue
    wr = sum(wins) / len(wins) * 100
    avg = np.mean(gains) if gains else 0
    marker = " ***" if wr >= 55 else " **" if wr >= 50 else " *" if wr >= 45 else ""
    print(f"{labels[key]:<50s} {len(evts):7d} {wr:6.1f}% {avg:+7.1f}%{marker}")

# Recent examples of the best strategy
best_key = max([(k, sum(e["win"] for e in v if e["win"] is not None) / max(1, len([e for e in v if e["win"] is not None])))
                 for k, v in events.items() if len(v) >= 20], key=lambda x: x[1])[0]

print(f"\n{'=' * 75}")
print(f"RECENT EXAMPLES: {labels.get(best_key, best_key)}")
print(f"{'=' * 75}")
recent = sorted(events[best_key], key=lambda x: x["date"], reverse=True)
for e in recent[:20]:
    wl = "WIN" if e["win"] else "LOSS" if e["win"] is not None else "?"
    g = f"{e['gain']:+.1f}%" if e.get("gain") is not None else "?"
    print(f"  {e['date']} {e['symbol']:12s} ৳{e['price']:7.1f} CMF={e['cmf']:+.3f} RSI={e['rsi']:.0f} MaxGain={g} {wl}")

# Compare: baseline random = 35.4%, what does this framework achieve?
print(f"\n{'=' * 75}")
print("COMPARISON TO BASELINE")
print(f"{'=' * 75}")
print(f"Random entry: 35.4%")
for key in order:
    evts = events[key]
    if len(evts) < 20:
        continue
    wins = [e["win"] for e in evts if e["win"] is not None]
    if not wins:
        continue
    wr = sum(wins) / len(wins) * 100
    edge = wr - 35.4
    print(f"  {labels[key]:<45s} {wr:.1f}% (edge: {edge:+.1f}%)")
