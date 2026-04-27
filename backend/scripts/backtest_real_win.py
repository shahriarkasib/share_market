#!/usr/bin/env python3
"""
REAL WIN metric: Close is +2%+ on Day 3 OR Day 4 OR Day 5.
NO DSEX filter — must work in any market condition.
Test every hypothesis.
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
conn.close()
print(f"Loaded {len(df)} rows, {df.symbol.nunique()} stocks\n")


def real_win(close, entry_price, entry_idx):
    """CLOSE is +2%+ on Day 3, 4, or 5."""
    if entry_idx + 5 >= len(close):
        return None
    for d in [3, 4, 5]:
        if (close.iloc[entry_idx + d] - entry_price) / entry_price * 100 >= 2:
            return True
    return False


results = {}
names = {}

def add(key, name, win):
    if key not in results:
        results[key] = []
        names[key] = name
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
    plus_di = adx_obj.adx_pos()
    minus_di = adx_obj.adx_neg()
    ema9 = EMAIndicator(close, window=9).ema_indicator()
    ema21 = EMAIndicator(close, window=21).ema_indicator()
    ema50 = EMAIndicator(close, window=50).ema_indicator()
    obv = OnBalanceVolumeIndicator(close, volume).on_balance_volume()
    bb = BollingerBands(close, window=20, window_dev=2)
    bb_pct = bb.bollinger_pband()
    vol_avg = volume.rolling(20).mean()

    obv_slope = obv.rolling(10).apply(lambda x: np.polyfit(range(len(x)), x, 1)[0] if len(x) > 1 else 0, raw=True)
    price_slope = close.rolling(10).apply(lambda x: np.polyfit(range(len(x)), x, 1)[0] if len(x) > 1 else 0, raw=True)

    cmf_streak = pd.Series(0, index=close.index, dtype=int)
    for j in range(1, len(cmf)):
        if pd.notna(cmf.iloc[j]) and cmf.iloc[j] > 0:
            cmf_streak.iloc[j] = cmf_streak.iloc[j-1] + 1

    # Swing points for ICT
    swing_highs = []
    swing_lows = []
    for i in range(3, len(sdf) - 3):
        if high.iloc[i] == max(high.iloc[i-3:i+4]):
            swing_highs.append((i, float(high.iloc[i])))
        if low.iloc[i] == min(low.iloc[i-3:i+4]):
            swing_lows.append((i, float(low.iloc[i])))

    for i in range(60, len(sdf) - 5):
        c = close.iloc[i]
        o = open_.iloc[i]
        h = high.iloc[i]
        l = low.iloc[i]
        v = volume.iloc[i]
        c_prev = close.iloc[i-1]

        rsi_v = rsi.iloc[i] if pd.notna(rsi.iloc[i]) else 50
        cmf_v = cmf.iloc[i] if pd.notna(cmf.iloc[i]) else 0
        adx_v = adx.iloc[i] if pd.notna(adx.iloc[i]) else 0
        pdi = plus_di.iloc[i] if pd.notna(plus_di.iloc[i]) else 0
        mdi = minus_di.iloc[i] if pd.notna(minus_di.iloc[i]) else 0
        e9 = ema9.iloc[i] if pd.notna(ema9.iloc[i]) else 0
        e21 = ema21.iloc[i] if pd.notna(ema21.iloc[i]) else 0
        e50 = ema50.iloc[i] if pd.notna(ema50.iloc[i]) else 0
        va = vol_avg.iloc[i] if pd.notna(vol_avg.iloc[i]) else v
        bb_v = bb_pct.iloc[i] if pd.notna(bb_pct.iloc[i]) else 0.5
        obv_s = obv_slope.iloc[i] if pd.notna(obv_slope.iloc[i]) else 0
        price_s = price_slope.iloc[i] if pd.notna(price_slope.iloc[i]) else 0
        cs = int(cmf_streak.iloc[i])

        green = c > o
        body = abs(c - o)
        total_range = h - l
        lower_shadow = min(c, o) - l
        upper_shadow = h - max(c, o)
        vol_ratio = v / va if va > 0 else 1
        ma_aligned = e9 > e21 > e50 and c > e9
        bullish_di = pdi > mdi

        red_days = 0
        for j in range(1, min(6, i)):
            if close.iloc[i-j] < close.iloc[i-j-1]:
                red_days += 1
            else:
                break

        chg_5d = (c - close.iloc[i-5]) / close.iloc[i-5] * 100 if i >= 5 else 0

        # Support
        at_support = False
        sup_touches = 0
        for sl_i, sl_p in swing_lows:
            if sl_i >= i:
                break
            if sl_i < i - 60:
                continue
            count = sum(1 for si2, sp2 in swing_lows if abs(sp2 - sl_p) / sl_p * 100 < 1.5 and si2 < i)
            if abs(l - sl_p) / sl_p * 100 < 2 and count >= 2:
                at_support = True
                sup_touches = count
                break

        # Hammer
        hammer = lower_shadow > body * 2 if body > 0 and total_range > 0 else False

        # Engulfing
        engulf = (c > o and c_prev < open_.iloc[i-1] and body > abs(c_prev - open_.iloc[i-1])) if i > 0 else False

        # OBV divergence
        obv_div = price_s < 0 and obv_s > 0

        # Liquidity grab
        recent_low = low.iloc[max(0,i-5):i].min()
        liq_grab = l < recent_low * 0.99 and c > recent_low

        # FVG
        has_fvg = False
        if i >= 3:
            for j in range(i-15, i-2):
                if j >= 2 and high.iloc[j-2] < low.iloc[j]:
                    if l <= low.iloc[j] and c >= high.iloc[j-2]:
                        has_fvg = True
                        break

        # ChoCh + BOS
        choch_bos = False
        recent_sh = [(idx, p) for idx, p in swing_highs if i-30 < idx < i]
        recent_sl = [(idx, p) for idx, p in swing_lows if i-30 < idx < i]
        if len(recent_sh) >= 2 and len(recent_sl) >= 2:
            if recent_sl[-1][1] < recent_sl[-2][1]:  # downtrend (LL)
                lh_price = recent_sh[-1][1]
                if c > lh_price:  # broke above LH = ChoCh
                    if h > recent_sh[-1][1]:  # new HH = BOS
                        choch_bos = True

        # 52w high
        h52w = high.iloc[max(0,i-252):i].max() if i >= 252 else high.iloc[:i].max()
        at_52w_high = c > h52w and c_prev <= h52w

        w = real_win(close, c, i)

        # === BASELINE ===
        add("baseline", "Random entry", w)

        # === SINGLE FACTORS ===
        if rsi_v < 25: add("rsi25", "RSI < 25", w)
        if rsi_v < 30: add("rsi30", "RSI < 30", w)
        if rsi_v < 35: add("rsi35", "RSI < 35", w)
        if rsi_v < 40: add("rsi40", "RSI < 40", w)
        if red_days >= 3: add("3red", "3+ red days", w)
        if red_days >= 4: add("4red", "4+ red days", w)
        if chg_5d < -5: add("drop5", "5d drop >5%", w)
        if chg_5d < -7: add("drop7", "5d drop >7%", w)
        if chg_5d < -10: add("drop10", "5d drop >10%", w)
        if bb_v < 0: add("bb0", "Below BB lower", w)
        if bb_v < -0.2: add("bb_neg", "BB < -0.2", w)
        if obv_div: add("obv_div", "OBV divergence", w)
        if at_support and sup_touches >= 3: add("sup3t", "Support 3T", w)
        if liq_grab: add("liq", "Liquidity grab", w)
        if has_fvg: add("fvg", "FVG pullback", w)
        if choch_bos: add("choch_bos", "ChoCh + BOS", w)
        if hammer: add("hammer", "Hammer", w)
        if engulf: add("engulf", "Bullish engulfing", w)
        if at_52w_high: add("52w", "52w high break", w)
        if ma_aligned: add("ma", "MA aligned", w)
        if cmf_v > 0 and cs >= 5: add("cmf5", "CMF positive 5d+", w)
        if vol_ratio > 2: add("vol2x", "Volume > 2x avg", w)
        if adx_v > 25 and bullish_di: add("adx25_bdi", "ADX>25 + bullish DI", w)

        # === COMBOS (no DSEX) ===
        if rsi_v < 30 and red_days >= 3: add("rsi30_3red", "RSI<30 + 3red", w)
        if rsi_v < 30 and chg_5d < -7: add("rsi30_drop7", "RSI<30 + drop>7%", w)
        if rsi_v < 35 and chg_5d < -5: add("rsi35_drop5", "RSI<35 + drop>5%", w)
        if rsi_v < 30 and bb_v < 0: add("rsi30_bb", "RSI<30 + below BB", w)
        if rsi_v < 35 and at_support and sup_touches >= 3: add("rsi35_sup", "RSI<35 + Support 3T", w)
        if rsi_v < 40 and at_support and green: add("rsi40_sup_grn", "RSI<40 + Sup + Green", w)
        if rsi_v < 35 and hammer: add("rsi35_ham", "RSI<35 + Hammer", w)
        if rsi_v < 30 and liq_grab: add("rsi30_liq", "RSI<30 + LiqGrab", w)
        if rsi_v < 35 and obv_div: add("rsi35_obv", "RSI<35 + OBV div", w)
        if choch_bos and rsi_v < 50: add("choch_rsi50", "ChoCh+BOS + RSI<50", w)
        if choch_bos and vol_ratio > 1.5: add("choch_vol", "ChoCh+BOS + Vol>1.5x", w)
        if choch_bos and green: add("choch_grn", "ChoCh+BOS + Green", w)
        if at_52w_high and cmf_v > 0: add("52w_cmf", "52w high + CMF+", w)
        if at_52w_high and vol_ratio > 1.5: add("52w_vol", "52w high + Vol>1.5x", w)
        if has_fvg and green and rsi_v < 50: add("fvg_grn_rsi", "FVG + Green + RSI<50", w)
        if red_days >= 3 and rsi_v < 35 and bb_v < 0: add("3red_rsi35_bb", "3red+RSI<35+BB<0", w)
        if chg_5d < -10 and rsi_v < 30: add("drop10_rsi30", "Drop10%+RSI<30", w)
        if liq_grab and green: add("liq_grn", "LiqGrab + Green", w)
        if liq_grab and green and vol_ratio > 1.3: add("liq_grn_vol", "LiqGrab+Green+Vol", w)
        if hammer and at_support and vol_ratio > 1.3: add("ham_sup_vol", "Hammer+Support+Vol", w)
        if engulf and rsi_v < 40: add("eng_rsi40", "Engulfing+RSI<40", w)
        if ma_aligned and cmf_v > 0 and cs >= 5 and rsi_v < 60: add("trend_cmf", "MA+CMF5d+RSI<60", w)
        if adx_v > 25 and bullish_di and rsi_v < 45: add("adx_di_rsi", "ADX>25+BullDI+RSI<45", w)
        if rsi_v < 30 and vol_ratio > 1.5: add("rsi30_vol", "RSI<30 + Vol>1.5x", w)
        if rsi_v < 25 and red_days >= 2: add("rsi25_2red", "RSI<25 + 2red", w)
        if bb_v < -0.2 and red_days >= 2: add("bb_neg_2red", "BB<-0.2 + 2red", w)
        if chg_5d < -7 and obv_div: add("drop7_obv", "Drop7%+OBVdiv", w)
        if rsi_v < 30 and at_support and green: add("rsi30_sup_grn", "RSI<30+Sup+Green", w)

    if stock_count % 50 == 0:
        print(f"  Processed {stock_count} stocks...")

print(f"\nProcessed {stock_count} stocks\n")

# === RESULTS ===
print("=" * 70)
print("REAL WIN: Close +2%+ on Day 3, 4, or 5 — NO DSEX FILTER")
print("=" * 70)

rows = []
for key, wins in results.items():
    if len(wins) < 30:
        rows.append({"name": names[key], "n": len(wins), "wr": 0, "note": f"few({len(wins)})"})
        continue
    wr = sum(wins) / len(wins) * 100
    rows.append({"name": names[key], "n": len(wins), "wr": wr})

rows.sort(key=lambda x: -x.get("wr", 0))

print(f"\n{'Rank':>4} {'Strategy':<40s} {'Events':>8s} {'Win Rate':>9s}")
print("-" * 65)
for rank, r in enumerate(rows, 1):
    if "note" in r:
        print(f"{rank:4d} {r['name']:<40s} {r['n']:8d}     {r['note']}")
    else:
        marker = " ***" if r["wr"] >= 55 else " **" if r["wr"] >= 50 else " *" if r["wr"] >= 45 else ""
        print(f"{rank:4d} {r['name']:<40s} {r['n']:8d} {r['wr']:8.1f}%{marker}")
