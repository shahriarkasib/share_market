#!/usr/bin/env python3
"""
ULTIMATE BACKTEST: Test EVERY hypothesis with the CORRECT metric:
  WIN = HIGH reaches +3% at ANY point between Day 3 and Day 5 (T+2 to T+4)

This is what matters for a DSE trader:
  - Buy today
  - Can't sell for 2 days (T+2)
  - Day 3, 4, 5: watch for +3% intraday spike, sell with limit order
"""

import warnings; warnings.filterwarnings("ignore")
import sys, os
import pandas as pd
import numpy as np
import psycopg2

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from config import DATABASE_URL
from ta.momentum import RSIIndicator
from ta.volume import ChaikinMoneyFlowIndicator, OnBalanceVolumeIndicator
from ta.trend import ADXIndicator, EMAIndicator
from ta.volatility import BollingerBands

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

dsex = pd.read_sql(
    "SELECT date, dsex_index as close FROM dsex_history WHERE dsex_index > 0 ORDER BY date", conn)
dsex["date"] = pd.to_datetime(dsex["date"])
dsex = dsex.set_index("date")
dsex["sma20"] = dsex["close"].rolling(20).mean()
dsex["below_sma20"] = dsex["close"] < dsex["sma20"]
dsex["chg_1d"] = dsex["close"].pct_change() * 100
conn.close()

print(f"Loaded {len(df)} rows, {df.symbol.nunique()} stocks\n")
print("Computing indicators and testing ALL hypotheses...\n")

# The ONE metric that matters
def win_metric(high_series, close_series, entry_price, entry_idx):
    """Did HIGH reach +3% between Day 3 and Day 5?"""
    if entry_idx + 5 >= len(close_series):
        return None
    max_high_3to5 = high_series.iloc[entry_idx + 3:entry_idx + 6].max()
    return (max_high_3to5 - entry_price) / entry_price * 100 >= 3

results = {}
strategy_names = {}

def add_event(key, name, win):
    if key not in results:
        results[key] = []
        strategy_names[key] = name
    if win is not None:
        results[key].append(win)

stock_count = 0
for symbol, sdf in df.groupby("symbol"):
    if len(sdf) < 200:
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
    adx_obj = ADXIndicator(high, low, close, window=14)
    adx = adx_obj.adx()
    ema9 = EMAIndicator(close, window=9).ema_indicator()
    ema21 = EMAIndicator(close, window=21).ema_indicator()
    ema50 = EMAIndicator(close, window=50).ema_indicator()
    obv = OnBalanceVolumeIndicator(close, volume).on_balance_volume()
    vol_avg = volume.rolling(20).mean()
    bb = BollingerBands(close, window=20, window_dev=2)
    bb_pct = bb.bollinger_pband()

    obv_slope = obv.rolling(10).apply(lambda x: np.polyfit(range(len(x)), x, 1)[0] if len(x) > 1 else 0, raw=True)
    price_slope = close.rolling(10).apply(lambda x: np.polyfit(range(len(x)), x, 1)[0] if len(x) > 1 else 0, raw=True)

    # CMF streak
    cmf_pos_streak = pd.Series(0, index=close.index, dtype=int)
    for j in range(1, len(cmf)):
        if pd.notna(cmf.iloc[j]) and cmf.iloc[j] > 0:
            cmf_pos_streak.iloc[j] = cmf_pos_streak.iloc[j-1] + 1

    for i in range(60, len(sdf) - 5):
        c = close.iloc[i]
        o = open_.iloc[i]
        h = high.iloc[i]
        l = low.iloc[i]
        v = volume.iloc[i]
        c_prev = close.iloc[i-1]
        dt = sdf["date"].iloc[i]

        rsi_v = rsi.iloc[i] if pd.notna(rsi.iloc[i]) else 50
        cmf_v = cmf.iloc[i] if pd.notna(cmf.iloc[i]) else 0
        adx_v = adx.iloc[i] if pd.notna(adx.iloc[i]) else 0
        e9 = ema9.iloc[i] if pd.notna(ema9.iloc[i]) else 0
        e21 = ema21.iloc[i] if pd.notna(ema21.iloc[i]) else 0
        e50 = ema50.iloc[i] if pd.notna(ema50.iloc[i]) else 0
        va = vol_avg.iloc[i] if pd.notna(vol_avg.iloc[i]) else v
        bb_v = bb_pct.iloc[i] if pd.notna(bb_pct.iloc[i]) else 0.5
        obv_s = obv_slope.iloc[i] if pd.notna(obv_slope.iloc[i]) else 0
        price_s = price_slope.iloc[i] if pd.notna(price_slope.iloc[i]) else 0
        cmf_streak = int(cmf_pos_streak.iloc[i])

        green = c > o
        body = abs(c - o)
        total_range = h - l
        lower_shadow = min(c, o) - l
        vol_ratio = v / va if va > 0 else 1
        ma_aligned = e9 > e21 > e50 and c > e9

        # DSEX
        dsex_weak = False
        dsex_crash = False
        if len(dsex.loc[:dt]) > 0:
            dr = dsex.loc[:dt].iloc[-1]
            dsex_weak = bool(dr["below_sma20"]) if pd.notna(dr["below_sma20"]) else False
            dsex_crash = float(dr["chg_1d"]) < -2 if pd.notna(dr["chg_1d"]) else False

        # Red days
        red_days = 0
        for j in range(1, min(6, i)):
            if close.iloc[i-j] < close.iloc[i-j-1]:
                red_days += 1
            else:
                break

        chg_5d = (c - close.iloc[i-5]) / close.iloc[i-5] * 100 if i >= 5 else 0

        # Support
        swing_lows = []
        for j in range(max(2, i-40), i-2):
            if low.iloc[j] == min(low.iloc[max(0,j-2):j+3]):
                swing_lows.append(float(low.iloc[j]))
        at_support = False
        sup_touches = 0
        for sl in swing_lows:
            count = sum(1 for s in swing_lows if abs(s - sl) / sl * 100 < 1.5)
            if abs(l - sl) / sl * 100 < 2 and count >= 2:
                at_support = True
                sup_touches = count
                break

        # FVG
        has_fvg = False
        if i >= 3:
            for j in range(i-20, i-2):
                if j >= 2 and high.iloc[j-2] < low.iloc[j]:
                    fvg_low = high.iloc[j-2]
                    fvg_high = low.iloc[j]
                    if l <= fvg_high and c >= fvg_low:
                        has_fvg = True
                        break

        # Hammer
        hammer = lower_shadow > body * 2 if body > 0 and total_range > 0 else False

        # Engulfing
        bullish_engulf = (c > o and c_prev < open_.iloc[i-1] and body > abs(c_prev - open_.iloc[i-1])
                         and o < c_prev and c > open_.iloc[i-1]) if i > 0 else False

        # Liquidity grab
        recent_low = low.iloc[max(0,i-5):i].min()
        liq_grab = l < recent_low * 0.99 and c > recent_low

        # 52w high
        if i >= 252:
            h52w = high.iloc[i-252:i].max()
            at_52w_high = c > h52w and c_prev <= h52w
        else:
            at_52w_high = False

        # OBV divergence
        obv_div = price_s < 0 and obv_s > 0

        # The WIN metric
        w = win_metric(high, close, c, i)

        # === TEST EVERY HYPOTHESIS ===

        # Always track baseline
        add_event("baseline", "Random entry (baseline)", w)

        # RSI levels
        if rsi_v < 20: add_event("rsi_20", "RSI < 20", w)
        if rsi_v < 25: add_event("rsi_25", "RSI < 25", w)
        if rsi_v < 30: add_event("rsi_30", "RSI < 30", w)
        if rsi_v < 35: add_event("rsi_35", "RSI < 35", w)
        if rsi_v < 40: add_event("rsi_40", "RSI < 40", w)

        # DSEX
        if dsex_weak: add_event("dsex_weak", "DSEX below SMA20", w)
        if dsex_crash: add_event("dsex_crash", "DSEX dropped >2% today", w)

        # RSI + DSEX combos
        if rsi_v < 30 and dsex_weak: add_event("rsi30_dsex", "RSI<30 + DSEX weak", w)
        if rsi_v < 35 and dsex_weak: add_event("rsi35_dsex", "RSI<35 + DSEX weak", w)
        if rsi_v < 40 and dsex_weak: add_event("rsi40_dsex", "RSI<40 + DSEX weak", w)
        if rsi_v < 30 and dsex_crash: add_event("rsi30_crash", "RSI<30 + DSEX crash day", w)

        # Red days
        if red_days >= 3: add_event("3red", "3+ red days", w)
        if red_days >= 3 and dsex_weak: add_event("3red_dsex", "3red + DSEX weak", w)
        if red_days >= 3 and rsi_v < 40: add_event("3red_rsi40", "3red + RSI<40", w)
        if red_days >= 3 and rsi_v < 40 and dsex_weak: add_event("3red_rsi40_dsex", "3red + RSI<40 + DSEX weak", w)

        # 5d drop
        if chg_5d < -5: add_event("drop5", "5d drop >5%", w)
        if chg_5d < -10: add_event("drop10", "5d drop >10%", w)
        if chg_5d < -10 and dsex_weak: add_event("drop10_dsex", "5d drop>10% + DSEX weak", w)

        # Support
        if at_support and sup_touches >= 3: add_event("sup3t", "Support 3T", w)
        if at_support and sup_touches >= 3 and rsi_v < 40: add_event("sup3t_rsi40", "Support 3T + RSI<40", w)
        if at_support and sup_touches >= 3 and rsi_v < 40 and dsex_weak: add_event("sup3t_rsi40_dsex", "Sup3T+RSI<40+DSEX weak", w)
        if at_support and sup_touches >= 3 and green: add_event("sup3t_green", "Support 3T + green", w)
        if at_support and rsi_v < 35 and green: add_event("sup_rsi35_green", "Support+RSI<35+green", w)

        # Candle patterns
        if hammer: add_event("hammer", "Hammer candle", w)
        if hammer and rsi_v < 40: add_event("hammer_rsi40", "Hammer + RSI<40", w)
        if bullish_engulf: add_event("engulf", "Bullish engulfing", w)
        if bullish_engulf and rsi_v < 40: add_event("engulf_rsi40", "Engulfing + RSI<40", w)

        # OBV divergence
        if obv_div: add_event("obv_div", "OBV divergence", w)
        if obv_div and rsi_v < 40: add_event("obv_div_rsi40", "OBV div + RSI<40", w)
        if obv_div and rsi_v < 40 and dsex_weak: add_event("obv_div_rsi40_dsex", "OBVdiv+RSI<40+DSEX", w)

        # Liquidity grab
        if liq_grab: add_event("liq_grab", "Liquidity grab", w)
        if liq_grab and rsi_v < 40: add_event("liq_grab_rsi40", "LiqGrab + RSI<40", w)

        # FVG
        if has_fvg: add_event("fvg", "FVG pullback", w)
        if has_fvg and green: add_event("fvg_green", "FVG + green", w)
        if has_fvg and rsi_v < 40: add_event("fvg_rsi40", "FVG + RSI<40", w)

        # Volume spike
        if vol_ratio > 2.5 and green: add_event("vol_spike_green", "Vol spike + green", w)
        if vol_ratio > 2.5 and green and rsi_v < 40: add_event("vol_spike_rsi40", "VolSpike+green+RSI<40", w)

        # MA aligned
        if ma_aligned: add_event("ma_aligned", "MA aligned (trend)", w)
        if ma_aligned and rsi_v < 40: add_event("ma_rsi40", "MA aligned + RSI<40", w)

        # CMF
        if cmf_v > 0 and cmf_streak >= 5: add_event("cmf5", "CMF positive 5+ days", w)
        if cmf_v < -0.2: add_event("cmf_neg", "CMF < -0.2", w)

        # BB
        if bb_v < 0: add_event("bb_below", "Below BB lower band", w)
        if bb_v < 0 and dsex_weak: add_event("bb_below_dsex", "Below BB + DSEX weak", w)

        # 52w high
        if at_52w_high: add_event("52w_high", "52w high break", w)
        if at_52w_high and cmf_v > 0: add_event("52w_cmf", "52w high + CMF+", w)

        # MEGA COMBOS
        if rsi_v < 30 and dsex_weak and at_support: add_event("mega1", "RSI<30+DSEX+Support", w)
        if rsi_v < 30 and dsex_weak and green: add_event("mega2", "RSI<30+DSEX+Green", w)
        if rsi_v < 35 and dsex_weak and at_support and green: add_event("mega3", "RSI<35+DSEX+Sup+Green", w)
        if chg_5d < -7 and dsex_weak and rsi_v < 35: add_event("mega4", "Drop7%+DSEX+RSI<35", w)
        if rsi_v < 30 and red_days >= 2 and dsex_weak: add_event("mega5", "RSI<30+2red+DSEX", w)
        if rsi_v < 25 and dsex_weak: add_event("mega6", "RSI<25+DSEX weak", w)
        if bb_v < 0 and rsi_v < 30 and dsex_weak: add_event("mega7", "BB<0+RSI<30+DSEX", w)
        if obv_div and at_support and rsi_v < 40: add_event("mega8", "OBVdiv+Support+RSI<40", w)
        if liq_grab and dsex_weak: add_event("mega9", "LiqGrab + DSEX weak", w)
        if hammer and at_support and rsi_v < 40: add_event("mega10", "Hammer+Support+RSI<40", w)

    if stock_count % 50 == 0:
        print(f"  Processed {stock_count} stocks...")

print(f"\nProcessed {stock_count} stocks\n")

# === RESULTS ===
print("=" * 75)
print("ULTIMATE RANKING: HIGH reaches +3% between Day 3 and Day 5")
print("(The REAL metric for DSE T+2 traders)")
print("=" * 75)

rows = []
for key, wins in results.items():
    if len(wins) < 20:
        rows.append({"name": strategy_names[key], "n": len(wins), "hit": 0, "note": "too few"})
        continue
    hit = sum(wins) / len(wins) * 100
    rows.append({"name": strategy_names[key], "n": len(wins), "hit": hit})

rows.sort(key=lambda x: -x.get("hit", 0))

print(f"\n{'Rank':>4} {'Strategy':<40s} {'Events':>7s} {'Hit +3%':>8s}")
print("-" * 65)
for rank, r in enumerate(rows, 1):
    if "note" in r:
        print(f"{rank:4d} {r['name']:<40s} {r['n']:7d}    {r['note']}")
    else:
        marker = " ***" if r["hit"] >= 80 else " **" if r["hit"] >= 70 else " *" if r["hit"] >= 60 else ""
        print(f"{rank:4d} {r['name']:<40s} {r['n']:7d} {r['hit']:7.0f}%{marker}")
