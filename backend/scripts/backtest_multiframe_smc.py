#!/usr/bin/env python3
"""
Backtest: Multi-Timeframe Alignment + SMC/VSA patterns on DSE.

Tests:
1. Multi-timeframe: daily+weekly RSI both oversold at support
2. VSA: low volume pullback in uptrend vs high volume pullback
3. Rounded bottom recovery
4. Fair Value Gap (FVG) as entry zone
5. Order Block bounce
6. Liquidity grab (dip below support then recover)
7. Structural shift (LL→HL transition)
"""

import warnings; warnings.filterwarnings("ignore")
import sys, os
import pandas as pd
import numpy as np
import psycopg2

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from config import DATABASE_URL

from ta.momentum import RSIIndicator
from ta.trend import EMAIndicator

conn = psycopg2.connect(DATABASE_URL)

print("Loading daily data...")
df = pd.read_sql(
    "SELECT dp.symbol, dp.date, dp.open, dp.high, dp.low, dp.close, dp.volume "
    "FROM daily_prices dp JOIN fundamentals f ON dp.symbol = f.symbol "
    "WHERE dp.date >= '2025-07-01' AND dp.close > 0 AND f.category IN ('A','B') "
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


def report(name, events, note=""):
    if not events:
        print(f"\n{'='*70}\n{name}\n  No events.\n")
        return
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


events = {
    # Multi-timeframe
    "daily_rsi30": [],
    "daily_weekly_rsi40": [],
    "daily_weekly_rsi30": [],
    "daily_rsi40_weekly_rsi40": [],
    "daily_rsi30_weekly_rsi40": [],
    # VSA
    "low_vol_pullback_uptrend": [],
    "high_vol_pullback_uptrend": [],
    "low_vol_pullback_at_support": [],
    # Structural shift
    "ll_to_hl_shift": [],
    # Order block bounce
    "order_block_bounce": [],
    # FVG fill
    "bullish_fvg_fill": [],
    # Liquidity grab
    "liquidity_grab": [],
    # Rounded bottom + recovery
    "rounded_bottom_entry": [],
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

    # Daily indicators
    rsi_d = RSIIndicator(close, window=14).rsi()
    ema21 = EMAIndicator(close, window=21).ema_indicator()
    ema50 = EMAIndicator(close, window=50).ema_indicator()
    vol_avg = volume.rolling(20).mean()

    # Weekly aggregation
    sdf_weekly = sdf.set_index("date").resample("W-WED").agg({
        "open": "first", "high": "max", "low": "min", "close": "last", "volume": "sum"
    }).dropna(subset=["close"]).reset_index()

    rsi_w = None
    if len(sdf_weekly) >= 20:
        rsi_w = RSIIndicator(sdf_weekly["close"], window=14).rsi()

    # Find support levels
    def get_support_at(idx, lookback=60):
        start = max(0, idx - lookback)
        swing_lows = []
        for j in range(start + 2, idx - 2):
            if low.iloc[j] == min(low.iloc[j-2:j+3]):
                swing_lows.append(float(low.iloc[j]))
        if len(swing_lows) < 3:
            return None, 0
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
        # Find cluster nearest to current low
        best = None
        for cl in clusters:
            avg = np.mean(cl)
            if abs(low.iloc[idx] - avg) / avg * 100 < 2:
                if best is None or len(cl) > best[1]:
                    best = (avg, len(cl))
        return (best[0], best[1]) if best else (None, 0)

    for i in range(80, len(sdf) - 10):
        # Only test recent 6 months
        if sdf["date"].iloc[i] < pd.Timestamp("2025-10-01"):
            continue

        c = close.iloc[i]
        o = open_.iloc[i]
        l_val = low.iloc[i]
        h_val = high.iloc[i]
        v = volume.iloc[i]
        va = vol_avg.iloc[i] if pd.notna(vol_avg.iloc[i]) else v
        rsi_d_val = rsi_d.iloc[i] if pd.notna(rsi_d.iloc[i]) else 50
        ema21_val = ema21.iloc[i] if pd.notna(ema21.iloc[i]) else 0
        ema50_val = ema50.iloc[i] if pd.notna(ema50.iloc[i]) else 0

        # Get weekly RSI for this date
        rsi_w_val = 50
        if rsi_w is not None:
            dt = sdf["date"].iloc[i]
            w_idx = sdf_weekly["date"].searchsorted(dt) - 1
            if 0 <= w_idx < len(rsi_w) and pd.notna(rsi_w.iloc[w_idx]):
                rsi_w_val = float(rsi_w.iloc[w_idx])

        green = c > o
        fwd = fwd_ret(close, i)
        base = {"symbol": symbol, "date": str(sdf["date"].iloc[i].date()), "price": c, **fwd}

        # Support check
        sup_price, sup_touches = get_support_at(i)
        at_support = sup_price is not None and sup_touches >= 3 and abs(l_val - sup_price) / sup_price * 100 < 2

        # === 1. MULTI-TIMEFRAME RSI ===
        if rsi_d_val < 30:
            events["daily_rsi30"].append(base)

        if rsi_d_val < 40 and rsi_w_val < 40:
            events["daily_weekly_rsi40"].append(base)

        if rsi_d_val < 30 and rsi_w_val < 40:
            events["daily_rsi30_weekly_rsi40"].append(base)

        if rsi_d_val < 30 and rsi_w_val < 30:
            events["daily_weekly_rsi30"].append(base)

        if rsi_d_val < 40 and rsi_w_val < 40 and at_support:
            events["daily_rsi40_weekly_rsi40"].append({**base, "support": sup_price, "touches": sup_touches})

        # === 2. VSA: VOLUME ON PULLBACK ===
        # Is this a pullback in an uptrend?
        if ema21_val > ema50_val and c < ema21_val and c > ema50_val:
            vol_ratio = v / va if va > 0 else 1
            if vol_ratio < 0.7:  # low volume pullback
                events["low_vol_pullback_uptrend"].append(base)
                if at_support:
                    events["low_vol_pullback_at_support"].append(base)
            elif vol_ratio > 1.5:  # high volume pullback
                events["high_vol_pullback_uptrend"].append(base)

        # === 3. STRUCTURAL SHIFT (LL → HL) ===
        if i >= 10:
            # Find last 4 swing lows
            recent_swing_lows = []
            for j in range(i - 40, i - 2):
                if j >= 2 and low.iloc[j] == min(low.iloc[j-2:j+3]):
                    recent_swing_lows.append((j, float(low.iloc[j])))
            if len(recent_swing_lows) >= 3:
                last3 = recent_swing_lows[-3:]
                # Pattern: LL then HL (shift from bearish to bullish)
                if last3[1][1] < last3[0][1] and last3[2][1] > last3[1][1]:
                    # The shift just happened (last swing low is higher)
                    if i - last3[2][0] <= 5:  # within 5 bars of the shift
                        events["ll_to_hl_shift"].append(base)

        # === 4. ORDER BLOCK BOUNCE ===
        # Find bearish candles that preceded a strong up move in history
        # Then check if current price is returning to that zone
        for j in range(i - 30, i - 5):
            if j < 1:
                continue
            if close.iloc[j] < open_.iloc[j]:  # bearish candle
                next_move = (high.iloc[j+1:j+4].max() - close.iloc[j]) / close.iloc[j] * 100
                if next_move > 5:  # strong up move after
                    ob_low = float(low.iloc[j])
                    ob_high = float(high.iloc[j])
                    # Is current price touching this order block?
                    if l_val <= ob_high * 1.01 and c > ob_low * 0.99:
                        events["order_block_bounce"].append(base)
                        break

        # === 5. BULLISH FVG FILL ===
        # Find unfilled bullish FVGs and check if price just filled them
        for j in range(i - 20, i - 2):
            if j < 2:
                continue
            fvg_low = float(high.iloc[j-2])
            fvg_high = float(low.iloc[j])
            if fvg_high > fvg_low:  # bullish FVG exists
                # Was it unfilled until today?
                was_unfilled = all(low.iloc[k] > fvg_low for k in range(j, i))
                # Did today fill it?
                if was_unfilled and l_val <= fvg_high and c > fvg_low:
                    events["bullish_fvg_fill"].append(base)
                    break

        # === 6. LIQUIDITY GRAB ===
        # Price dips below recent support then closes back above
        if i >= 5:
            recent_low_5 = low.iloc[i-5:i].min()
            if l_val < recent_low_5 * 0.99 and c > recent_low_5:
                events["liquidity_grab"].append(base)

        # === 7. ROUNDED BOTTOM ===
        # Find stocks that bottomed 30-60 days ago and are now recovering
        if i >= 60:
            lookback_60 = 60
            period_low = low.iloc[i-lookback_60:i].min()
            period_low_idx = low.iloc[i-lookback_60:i].idxmin()
            days_since = i - period_low_idx

            if 20 <= days_since <= 50:
                recovery = (c - period_low) / period_low * 100
                if recovery > 15:
                    # Check: was it a downtrend before the bottom?
                    pre_bottom_high = high.iloc[max(0,period_low_idx-20):period_low_idx].max()
                    drop = (pre_bottom_high - period_low) / pre_bottom_high * 100
                    if drop > 15:
                        # Rounded bottom with recovery
                        events["rounded_bottom_entry"].append(base)

# Deduplicate
for key in events:
    if events[key]:
        edf = pd.DataFrame(events[key]).drop_duplicates(subset=["symbol", "date"])
        events[key] = edf.to_dict("records")

print(f"Processed {stock_count} stocks\n")

# === REPORTS ===
print("=" * 70)
print("MULTI-TIMEFRAME + SMC/VSA STUDY — DSE")
print("=" * 70)

print("\n### MULTI-TIMEFRAME RSI ###")
report("Daily RSI < 30 only", events["daily_rsi30"],
       "Baseline — 74% from previous study")
report("Daily RSI<40 + Weekly RSI<40", events["daily_weekly_rsi40"],
       "Both timeframes oversold")
report("Daily RSI<30 + Weekly RSI<40", events["daily_rsi30_weekly_rsi40"],
       "Daily deeply oversold + weekly oversold")
report("Daily RSI<30 + Weekly RSI<30", events["daily_weekly_rsi30"],
       "Both timeframes deeply oversold")
report("Daily+Weekly RSI<40 + At Support 3T", events["daily_rsi40_weekly_rsi40"],
       "Multi-TF oversold at strong support — THE ULTIMATE COMBO?")

print("\n### VSA (VOLUME SPREAD ANALYSIS) ###")
report("Low Vol Pullback in Uptrend (EMA21>50, price between)", events["low_vol_pullback_uptrend"],
       "Healthy correction — volume drying up on pullback")
report("High Vol Pullback in Uptrend", events["high_vol_pullback_uptrend"],
       "Concerning — heavy selling on pullback")
report("Low Vol Pullback + At Support", events["low_vol_pullback_at_support"],
       "Best VSA: low vol pullback landing on support")

print("\n### SMART MONEY CONCEPTS (SMC) ###")
report("Structural Shift (LL → HL transition)", events["ll_to_hl_shift"],
       "Market structure change from bearish to bullish")
report("Order Block Bounce", events["order_block_bounce"],
       "Price returns to previous demand zone")
report("Bullish FVG Fill (Fair Value Gap)", events["bullish_fvg_fill"],
       "Price fills the imbalance zone")
report("Liquidity Grab (dip below support, close above)", events["liquidity_grab"],
       "Stop-loss hunt then reversal")

print("\n### ROUNDED BOTTOM ###")
report("Rounded Bottom + 15% Recovery (20-50 days after bottom)", events["rounded_bottom_entry"],
       "Long downtrend → bottom → U-shape recovery")

# === FINAL RANKING ===
print(f"\n{'='*70}")
print("FINAL RANKING BY 5-DAY WIN RATE")
print(f"{'='*70}\n")

names = {
    "daily_rsi30": "Daily RSI<30 (baseline)",
    "daily_weekly_rsi40": "Daily+Weekly RSI<40",
    "daily_rsi30_weekly_rsi40": "Daily RSI<30 + Weekly RSI<40",
    "daily_weekly_rsi30": "Daily+Weekly RSI<30",
    "daily_rsi40_weekly_rsi40": "Daily+Weekly RSI<40 + Support 3T",
    "low_vol_pullback_uptrend": "Low Vol Pullback (uptrend)",
    "high_vol_pullback_uptrend": "High Vol Pullback (uptrend)",
    "low_vol_pullback_at_support": "Low Vol Pullback + Support",
    "ll_to_hl_shift": "Structural Shift (LL→HL)",
    "order_block_bounce": "Order Block Bounce",
    "bullish_fvg_fill": "FVG Fill",
    "liquidity_grab": "Liquidity Grab",
    "rounded_bottom_entry": "Rounded Bottom Recovery",
}

all_r = {}
for key, evts in events.items():
    if len(evts) < 5:
        all_r[key] = {"name": names.get(key, key), "n": len(evts), "wr": 0, "avg": 0, "note": f"too few ({len(evts)})"}
        continue
    edf = pd.DataFrame(evts)
    v5 = edf["ret_5d"].dropna()
    if len(v5) == 0:
        continue
    wr = (v5 > 0).sum() / len(v5) * 100
    all_r[key] = {"name": names.get(key, key), "n": len(edf), "wr": wr, "avg": v5.mean()}

ranked = sorted(all_r.items(), key=lambda x: -x[1].get("wr", 0))
print(f"{'Rank':>4} {'Factor':<45} {'Events':>7} {'5D WR':>7} {'5D Avg':>8}")
print("-" * 75)
for rank, (key, info) in enumerate(ranked, 1):
    note = info.get("note", "")
    if note:
        print(f"{rank:4d} {info['name']:<45} {info['n']:7d}    {note}")
    else:
        marker = " ***" if info["wr"] >= 60 else ""
        print(f"{rank:4d} {info['name']:<45} {info['n']:7d} {info['wr']:6.0f}% {info['avg']:+7.2f}%{marker}")
