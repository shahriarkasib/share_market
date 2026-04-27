#!/usr/bin/env python3
"""
Backtest: ICT / SMC sequence exactly as the analyst uses it.

The sequence:
1. LL formed (downtrend established)
2. ChoCh (Change of Character) — price breaks above the last LH
3. BOS (Break of Structure) — price makes a new HH confirming trend change
4. Pullback to Order Block (the last bearish candle before the bullish move)
5. Buy at order block zone
6. Target: next supply zone / ITH above

WIN = HIGH reaches +3% between Day 3 and Day 5 (T+2 metric)
"""

import warnings; warnings.filterwarnings("ignore")
import sys, os
import pandas as pd
import numpy as np
import psycopg2

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from config import DATABASE_URL
from ta.momentum import RSIIndicator

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


def find_swing_points(high, low, window=3):
    """Find swing highs and lows with given window."""
    swings = []
    for i in range(window, len(high) - window):
        if high.iloc[i] == max(high.iloc[i - window:i + window + 1]):
            swings.append({"idx": i, "type": "HH", "price": float(high.iloc[i])})
        if low.iloc[i] == min(low.iloc[i - window:i + window + 1]):
            swings.append({"idx": i, "type": "LL", "price": float(low.iloc[i])})
    swings.sort(key=lambda x: x["idx"])
    return swings


def win_metric(high, entry_price, entry_idx):
    """HIGH reaches +3% between Day 3 and Day 5"""
    if entry_idx + 5 >= len(high):
        return None
    max_high = high.iloc[entry_idx + 3:entry_idx + 6].max()
    return (max_high - entry_price) / entry_price * 100 >= 3


def max_gain_3to5(high, entry_price, entry_idx):
    """Max gain between Day 3 and Day 5"""
    if entry_idx + 5 >= len(high):
        return None
    max_high = high.iloc[entry_idx + 3:entry_idx + 6].max()
    return (max_high - entry_price) / entry_price * 100


events = {
    "choch_only": [],         # just ChoCh detected
    "choch_bos": [],          # ChoCh + BOS confirmed
    "choch_bos_ob": [],       # ChoCh + BOS + pullback to order block
    "choch_bos_ob_green": [], # + green candle at OB
    "choch_bos_ob_vol": [],   # + volume at OB
    "choch_bos_ob_full": [],  # + green + volume
    "choch_bos_fvg": [],      # ChoCh + BOS + pullback to FVG zone
    "ob_bounce_any": [],      # Any order block bounce (without ChoCh/BOS)
    "ob_bounce_green": [],    # OB bounce + green
    "ict_full_sequence": [],  # The complete ICT sequence
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
    vol_avg = volume.rolling(20).mean()

    rsi = RSIIndicator(close, window=14).rsi()

    # Find all swing points
    swings = find_swing_points(high, low, window=3)

    # Track state machine per stock
    # States: DOWNTREND → CHOCH → BOS → WAIT_FOR_PULLBACK
    state = "NONE"
    last_lh = None  # last lower high (ChoCh level)
    last_hh = None  # new higher high after BOS
    last_hl = None  # higher low (order block zone)
    ob_low = None   # order block low
    ob_high = None  # order block high
    choch_idx = None
    bos_idx = None

    # Process swings to detect the sequence
    for si_idx in range(4, len(swings)):
        s = swings[si_idx]
        bar_idx = s["idx"]

        if bar_idx + 5 >= len(sdf):
            break

        # Need at least 4 prior swings to establish pattern
        recent = swings[max(0, si_idx - 6):si_idx + 1]
        highs_list = [x for x in recent if x["type"] == "HH"]
        lows_list = [x for x in recent if x["type"] == "LL"]

        if len(highs_list) < 2 or len(lows_list) < 2:
            continue

        # Step 1: Detect downtrend (LL + LH)
        # Last two lows: is the recent one lower?
        last_two_lows = lows_list[-2:]
        last_two_highs = highs_list[-2:]

        is_downtrend = last_two_lows[1]["price"] < last_two_lows[0]["price"]
        is_lower_highs = last_two_highs[1]["price"] < last_two_highs[0]["price"]

        if not (is_downtrend or is_lower_highs):
            continue

        # The last LH is the ChoCh level
        lh_price = last_two_highs[-1]["price"]
        lh_idx = last_two_highs[-1]["idx"]
        ll_price = last_two_lows[-1]["price"]
        ll_idx = last_two_lows[-1]["idx"]

        # Step 2: Detect ChoCh — did price break above LH?
        # Check bars after the LL
        choch_detected = False
        choch_bar = None
        for k in range(ll_idx + 1, min(ll_idx + 20, len(sdf))):
            if close.iloc[k] > lh_price:
                choch_detected = True
                choch_bar = k
                break

        if not choch_detected or choch_bar is None:
            continue

        c_at_choch = close.iloc[choch_bar]
        w = win_metric(high, c_at_choch, choch_bar)
        gain = max_gain_3to5(high, c_at_choch, choch_bar)

        events["choch_only"].append({"win": w, "gain": gain, "symbol": symbol,
                                     "date": str(sdf["date"].iloc[choch_bar].date()),
                                     "price": c_at_choch})

        # Step 3: BOS — after ChoCh, does price make a NEW higher high?
        bos_detected = False
        bos_bar = None
        hh_after_choch = None
        for k in range(choch_bar + 1, min(choch_bar + 15, len(sdf))):
            if high.iloc[k] > high.iloc[choch_bar]:
                bos_detected = True
                bos_bar = k
                hh_after_choch = float(high.iloc[k])
                break

        if bos_detected and bos_bar is not None:
            events["choch_bos"].append({"win": w, "gain": gain, "symbol": symbol,
                                        "date": str(sdf["date"].iloc[choch_bar].date()),
                                        "price": c_at_choch})

            # Step 4: Find Order Block — the last bearish candle before the ChoCh move
            ob_found = False
            ob_zone_low = None
            ob_zone_high = None
            for k in range(choch_bar - 1, max(ll_idx, choch_bar - 10), -1):
                if close.iloc[k] < open_.iloc[k]:  # bearish candle
                    ob_zone_low = float(low.iloc[k])
                    ob_zone_high = float(high.iloc[k])
                    ob_found = True
                    break

            # Step 5: Does price pull back to the order block after BOS?
            if ob_found and bos_bar + 5 < len(sdf):
                for k in range(bos_bar + 1, min(bos_bar + 15, len(sdf) - 5)):
                    if low.iloc[k] <= ob_zone_high and close.iloc[k] >= ob_zone_low:
                        # PULLBACK TO ORDER BLOCK!
                        entry_price = close.iloc[k]
                        w_ob = win_metric(high, entry_price, k)
                        gain_ob = max_gain_3to5(high, entry_price, k)
                        rsi_v = rsi.iloc[k] if pd.notna(rsi.iloc[k]) else 50
                        green_candle = close.iloc[k] > open_.iloc[k]
                        v = volume.iloc[k]
                        va = vol_avg.iloc[k] if pd.notna(vol_avg.iloc[k]) else v
                        high_vol = v > va * 1.3 if va > 0 else False

                        base = {"win": w_ob, "gain": gain_ob, "symbol": symbol,
                                "date": str(sdf["date"].iloc[k].date()),
                                "price": entry_price, "rsi": rsi_v,
                                "ob_zone": f"{ob_zone_low:.1f}-{ob_zone_high:.1f}"}

                        events["choch_bos_ob"].append(base)

                        if green_candle:
                            events["choch_bos_ob_green"].append(base)
                        if high_vol:
                            events["choch_bos_ob_vol"].append(base)
                        if green_candle and high_vol:
                            events["choch_bos_ob_full"].append(base)

                        # Full ICT sequence: ChoCh + BOS + OB pullback + green + RSI not overbought
                        if green_candle and rsi_v < 65:
                            events["ict_full_sequence"].append(base)

                        break

            # Step 5b: Check FVG pullback after BOS
            if bos_bar + 5 < len(sdf):
                # Find FVG in the ChoCh move
                for k in range(ll_idx + 1, choch_bar):
                    if k >= 2 and high.iloc[k - 2] < low.iloc[k]:
                        fvg_low = float(high.iloc[k - 2])
                        fvg_high = float(low.iloc[k])
                        # Does price return to this FVG after BOS?
                        for m in range(bos_bar + 1, min(bos_bar + 15, len(sdf) - 5)):
                            if low.iloc[m] <= fvg_high and close.iloc[m] >= fvg_low:
                                w_fvg = win_metric(high, close.iloc[m], m)
                                gain_fvg = max_gain_3to5(high, close.iloc[m], m)
                                events["choch_bos_fvg"].append({
                                    "win": w_fvg, "gain": gain_fvg, "symbol": symbol,
                                    "date": str(sdf["date"].iloc[m].date()),
                                    "price": float(close.iloc[m])})
                                break
                        break

    # Also detect order block bounces WITHOUT requiring ChoCh/BOS
    for i in range(30, len(sdf) - 5):
        # Find bearish candle that preceded a strong up move
        for j in range(i - 20, i - 3):
            if j < 1:
                continue
            if close.iloc[j] < open_.iloc[j]:  # bearish
                next_move = (high.iloc[j + 1:j + 4].max() - close.iloc[j]) / close.iloc[j] * 100
                if next_move > 4:
                    ob_lo = float(low.iloc[j])
                    ob_hi = float(high.iloc[j])
                    if low.iloc[i] <= ob_hi * 1.01 and close.iloc[i] >= ob_lo * 0.99:
                        w = win_metric(high, close.iloc[i], i)
                        green = close.iloc[i] > open_.iloc[i]
                        events["ob_bounce_any"].append({"win": w})
                        if green:
                            events["ob_bounce_green"].append({"win": w})
                        break

    if stock_count % 50 == 0:
        print(f"  Processed {stock_count} stocks...")

print(f"\nProcessed {stock_count} stocks\n")

# === RESULTS ===
print("=" * 75)
print("ICT / SMC SEQUENCE BACKTEST — T+2 METRIC (HIGH +3% Day 3-5)")
print("=" * 75)

names = {
    "choch_only": "1. ChoCh only (price breaks LH)",
    "choch_bos": "2. ChoCh + BOS (new HH confirmed)",
    "choch_bos_ob": "3. ChoCh + BOS + Pullback to Order Block",
    "choch_bos_ob_green": "4. ChoCh+BOS+OB + Green candle",
    "choch_bos_ob_vol": "5. ChoCh+BOS+OB + High volume",
    "choch_bos_ob_full": "6. ChoCh+BOS+OB + Green + Volume",
    "choch_bos_fvg": "7. ChoCh+BOS + FVG pullback",
    "ob_bounce_any": "8. Order Block bounce (any, no ChoCh)",
    "ob_bounce_green": "9. Order Block bounce + Green",
    "ict_full_sequence": "10. FULL ICT: ChoCh+BOS+OB+Green+RSI<65",
}

print(f"\n{'Strategy':<50s} {'Events':>7s} {'Hit +3%':>8s} {'Avg Gain':>9s}")
print("-" * 78)

for key in ["choch_only", "choch_bos", "choch_bos_ob", "choch_bos_ob_green",
            "choch_bos_ob_vol", "choch_bos_ob_full", "choch_bos_fvg",
            "ob_bounce_any", "ob_bounce_green", "ict_full_sequence"]:
    evts = events[key]
    if len(evts) < 5:
        print(f"{names[key]:<50s} {len(evts):7d}    too few")
        continue
    wins = [e["win"] for e in evts if e["win"] is not None]
    gains = [e["gain"] for e in evts if e.get("gain") is not None]
    if not wins:
        continue
    hit = sum(wins) / len(wins) * 100
    avg_gain = np.mean(gains) if gains else 0
    marker = " ***" if hit >= 70 else " **" if hit >= 60 else " *" if hit >= 55 else ""
    print(f"{names[key]:<50s} {len(evts):7d} {hit:7.0f}% {avg_gain:+8.1f}%{marker}")

# Show recent examples of the full ICT sequence
if events["ict_full_sequence"]:
    print(f"\n{'=' * 75}")
    print("RECENT EXAMPLES: Full ICT Sequence")
    print(f"{'=' * 75}")
    recent = sorted(events["ict_full_sequence"], key=lambda x: x["date"], reverse=True)
    for e in recent[:15]:
        wl = "WIN" if e["win"] else "LOSS" if e["win"] is not None else "?"
        gain_str = f"{e['gain']:+.1f}%" if e.get("gain") is not None else "?"
        print(f"  {e['date']} {e['symbol']:12s} Entry={e['price']:7.1f} RSI={e.get('rsi',0):.0f} OB={e.get('ob_zone','')} MaxGain={gain_str} {wl}")

# Show recent ChoCh+BOS+OB examples
if events["choch_bos_ob"]:
    print(f"\n{'=' * 75}")
    print("RECENT EXAMPLES: ChoCh + BOS + Order Block Pullback")
    print(f"{'=' * 75}")
    recent = sorted(events["choch_bos_ob"], key=lambda x: x["date"], reverse=True)
    for e in recent[:15]:
        wl = "WIN" if e["win"] else "LOSS" if e["win"] is not None else "?"
        gain_str = f"{e['gain']:+.1f}%" if e.get("gain") is not None else "?"
        print(f"  {e['date']} {e['symbol']:12s} Entry={e['price']:7.1f} OB={e.get('ob_zone','')} MaxGain={gain_str} {wl}")
