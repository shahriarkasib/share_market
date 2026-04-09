#!/usr/bin/env python3
"""
Comprehensive DSE Factor Study — 6 months backtest.
Tests 15+ hypotheses across all A+B stocks and finds what actually works.
"""

import warnings; warnings.filterwarnings("ignore")
import sys, os
import pandas as pd
import numpy as np
import psycopg2

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from config import DATABASE_URL

from ta.volume import ChaikinMoneyFlowIndicator, OnBalanceVolumeIndicator
from ta.trend import EMAIndicator, SMAIndicator, MACD, ADXIndicator
from ta.momentum import RSIIndicator
from ta.volatility import BollingerBands

conn = psycopg2.connect(DATABASE_URL)

print("Loading data...")
df = pd.read_sql(
    "SELECT dp.symbol, dp.date, dp.open, dp.high, dp.low, dp.close, dp.volume, f.category "
    "FROM daily_prices dp JOIN fundamentals f ON dp.symbol = f.symbol "
    "WHERE dp.date >= '2025-10-01' AND dp.close > 0 AND f.category IN ('A','B') "
    "AND dp.volume > 0 "
    "ORDER BY dp.symbol, dp.date",
    conn,
)
df["date"] = pd.to_datetime(df["date"])
print(f"Loaded {len(df)} rows, {df.symbol.nunique()} stocks")

# Load DSEX for market regime
dsex = pd.read_sql(
    "SELECT date, dsex_index as close FROM dsex_history WHERE dsex_index > 0 ORDER BY date",
    conn,
)
dsex["date"] = pd.to_datetime(dsex["date"])
dsex = dsex.set_index("date").sort_index()
dsex["dsex_chg"] = dsex["close"].pct_change() * 100
dsex["dsex_up"] = dsex["dsex_chg"] > 0
conn.close()


def fwd_returns(close, i, max_days=10):
    """Get forward returns from position i."""
    fwd = {}
    for d in [1, 2, 3, 5, 10]:
        if i + d < len(close):
            fwd[f"ret_{d}d"] = (close.iloc[i + d] - close.iloc[i]) / close.iloc[i] * 100
        else:
            fwd[f"ret_{d}d"] = None
    return fwd


def report(name, events, note=""):
    """Print results for a hypothesis."""
    if len(events) == 0:
        return
    rdf = pd.DataFrame(events)
    n = len(rdf)
    print(f"\n{'='*70}")
    print(f"HYPOTHESIS: {name}")
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
        avg = valid.mean()
        med = valid.median()
        marker = " <<<" if wr >= 60 and d == 5 else ""
        print(f"  Day {d:2d}: Win {win}/{total} ({wr:.0f}%), Avg {avg:+.2f}%, Median {med:+.2f}%{marker}")
    return rdf


# Precompute indicators per stock
print("\nComputing indicators for all stocks...\n")

all_events = {
    "macd_cross_bull": [],
    "macd_cross_bear": [],
    "cmf_flip_positive": [],
    "cmf_flip_negative": [],
    "golden_cross": [],
    "death_cross": [],
    "bb_squeeze_break_up": [],
    "bb_squeeze_break_down": [],
    "volume_spike_green": [],
    "volume_spike_red": [],
    "gap_up_hold": [],
    "gap_up_fade": [],
    "gap_down_bounce": [],
    "ema21_bounce": [],
    "ema50_bounce": [],
    "sma200_bounce": [],
    "hammer_at_low": [],
    "engulfing_bullish": [],
    "shooting_star_at_high": [],
    "three_red_bounce": [],
    "three_green_sell": [],
    "obv_divergence_bull": [],
    "dsex_up_buy": [],
    "dsex_down_buy": [],
    "rsi_oversold_bounce": [],
    "rsi_overbought_sell": [],
    "adx_trend_breakout": [],
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
    dates = sdf["date"]

    # Indicators
    ema21 = EMAIndicator(close, window=21).ema_indicator()
    ema50 = EMAIndicator(close, window=50).ema_indicator()
    sma200 = SMAIndicator(close, window=200).sma_indicator() if len(sdf) >= 200 else pd.Series(np.nan, index=close.index)
    sma50 = SMAIndicator(close, window=50).sma_indicator()
    rsi = RSIIndicator(close, window=14).rsi()
    macd_obj = MACD(close, window_slow=26, window_fast=12, window_sign=9)
    macd_line = macd_obj.macd()
    macd_signal = macd_obj.macd_signal()
    macd_hist = macd_obj.macd_diff()
    cmf = ChaikinMoneyFlowIndicator(high, low, close, volume, window=20).chaikin_money_flow()
    obv = OnBalanceVolumeIndicator(close, volume).on_balance_volume()
    adx_obj = ADXIndicator(high, low, close, window=14)
    adx = adx_obj.adx()
    bb = BollingerBands(close, window=20, window_dev=2)
    bb_width = (bb.bollinger_hband() - bb.bollinger_lband()) / bb.bollinger_mavg() * 100
    bb_upper = bb.bollinger_hband()
    bb_lower = bb.bollinger_lband()
    vol_avg = volume.rolling(20).mean()

    for i in range(60, len(sdf) - 10):
        c = close.iloc[i]
        c_prev = close.iloc[i - 1]
        dt = dates.iloc[i]

        # Get DSEX regime for this date
        dsex_row = dsex.loc[:dt].iloc[-1] if dt in dsex.index or len(dsex.loc[:dt]) > 0 else None
        dsex_is_up = bool(dsex_row["dsex_up"]) if dsex_row is not None else None

        fwd = fwd_returns(close, i)
        base = {"symbol": symbol, "date": str(dt.date()), "price": c, **fwd}

        # === 1. MACD CROSSOVER ===
        if pd.notna(macd_line.iloc[i]) and pd.notna(macd_signal.iloc[i]):
            if macd_line.iloc[i] > macd_signal.iloc[i] and macd_line.iloc[i-1] <= macd_signal.iloc[i-1]:
                all_events["macd_cross_bull"].append(base)
            if macd_line.iloc[i] < macd_signal.iloc[i] and macd_line.iloc[i-1] >= macd_signal.iloc[i-1]:
                all_events["macd_cross_bear"].append(base)

        # === 2. CMF FLIP ===
        if pd.notna(cmf.iloc[i]) and pd.notna(cmf.iloc[i-1]):
            if cmf.iloc[i] > 0 and cmf.iloc[i-1] <= 0:
                all_events["cmf_flip_positive"].append(base)
            if cmf.iloc[i] < 0 and cmf.iloc[i-1] >= 0:
                all_events["cmf_flip_negative"].append(base)

        # === 3. GOLDEN/DEATH CROSS ===
        if pd.notna(sma50.iloc[i]) and pd.notna(sma200.iloc[i]):
            if sma50.iloc[i] > sma200.iloc[i] and sma50.iloc[i-1] <= sma200.iloc[i-1]:
                all_events["golden_cross"].append(base)
            if sma50.iloc[i] < sma200.iloc[i] and sma50.iloc[i-1] >= sma200.iloc[i-1]:
                all_events["death_cross"].append(base)

        # === 4. BOLLINGER SQUEEZE BREAKOUT ===
        if pd.notna(bb_width.iloc[i]) and i >= 5:
            # Squeeze = bb_width at 20-bar low
            recent_widths = bb_width.iloc[i-20:i].dropna()
            if len(recent_widths) > 10:
                is_squeeze = bb_width.iloc[i-1] <= recent_widths.quantile(0.1)
                if is_squeeze and pd.notna(bb_upper.iloc[i]):
                    if c > bb_upper.iloc[i]:
                        all_events["bb_squeeze_break_up"].append(base)
                    elif c < bb_lower.iloc[i]:
                        all_events["bb_squeeze_break_down"].append(base)

        # === 5. VOLUME SPIKE ===
        if pd.notna(vol_avg.iloc[i]) and vol_avg.iloc[i] > 0:
            vr = volume.iloc[i] / vol_avg.iloc[i]
            if vr > 2.5:
                if c > c_prev:
                    all_events["volume_spike_green"].append(base)
                else:
                    all_events["volume_spike_red"].append(base)

        # === 6. GAP UP/DOWN ===
        o = open_.iloc[i]
        gap_pct = (o - c_prev) / c_prev * 100 if c_prev > 0 else 0
        if gap_pct > 2:
            if c > o:
                all_events["gap_up_hold"].append(base)
            else:
                all_events["gap_up_fade"].append(base)
        elif gap_pct < -2:
            if c > o:  # closed green despite gap down
                all_events["gap_down_bounce"].append(base)

        # === 7. EMA BOUNCE ===
        for ema_name, ema_series in [("ema21", ema21), ("ema50", ema50), ("sma200", sma200)]:
            if pd.notna(ema_series.iloc[i]):
                ema_val = ema_series.iloc[i]
                # Low touched EMA but closed above = bounce
                if low.iloc[i] <= ema_val * 1.005 and c > ema_val and c > o:
                    all_events[f"{ema_name}_bounce"].append(base)

        # === 8. CANDLESTICK PATTERNS ===
        body = abs(c - o)
        total_range = high.iloc[i] - low.iloc[i]
        lower_shadow = min(c, o) - low.iloc[i]
        upper_shadow = high.iloc[i] - max(c, o)

        if total_range > 0:
            # Hammer at 20-day low
            if lower_shadow > body * 2 and body / total_range < 0.35:
                if low.iloc[i] <= low.iloc[i-20:i].min() * 1.02:
                    all_events["hammer_at_low"].append(base)

            # Bullish engulfing
            if c > o and c_prev < open_.iloc[i-1]:
                prev_body = abs(c_prev - open_.iloc[i-1])
                if body > prev_body and o < c_prev and c > open_.iloc[i-1]:
                    all_events["engulfing_bullish"].append(base)

            # Shooting star at 20-day high
            if upper_shadow > body * 2 and body / total_range < 0.35:
                if high.iloc[i] >= high.iloc[i-20:i].max() * 0.98:
                    all_events["shooting_star_at_high"].append(base)

        # === 9. CONSECUTIVE DAYS ===
        if i >= 3:
            three_red = all(close.iloc[i-j] < close.iloc[i-j-1] for j in range(3))
            three_green = all(close.iloc[i-j] > close.iloc[i-j-1] for j in range(3))
            if three_red:
                all_events["three_red_bounce"].append(base)
            if three_green:
                all_events["three_green_sell"].append(base)

        # === 10. OBV DIVERGENCE ===
        if i >= 20 and pd.notna(obv.iloc[i]):
            # Bullish div: price lower low but OBV higher low
            price_ll = close.iloc[i] < close.iloc[i-10:i].min()
            obv_hl = obv.iloc[i] > obv.iloc[i-10:i].min()
            if price_ll and obv_hl:
                all_events["obv_divergence_bull"].append(base)

        # === 11. DSEX REGIME ===
        if dsex_is_up is not None:
            if dsex_is_up:
                all_events["dsex_up_buy"].append(base)
            else:
                all_events["dsex_down_buy"].append(base)

        # === 12. RSI EXTREMES ===
        if pd.notna(rsi.iloc[i]):
            if rsi.iloc[i] < 30:
                all_events["rsi_oversold_bounce"].append(base)
            elif rsi.iloc[i] > 70:
                all_events["rsi_overbought_sell"].append(base)

        # === 13. ADX TREND BREAKOUT ===
        if pd.notna(adx.iloc[i]) and pd.notna(adx.iloc[i-1]):
            if adx.iloc[i] > 25 and adx.iloc[i-1] <= 25 and c > c_prev:
                all_events["adx_trend_breakout"].append(base)

print(f"Processed {stock_count} stocks\n")

# === REPORT ALL ===
print("=" * 70)
print("COMPREHENSIVE DSE FACTOR STUDY — 6 MONTH BACKTEST")
print("=" * 70)
print(f"Period: Oct 2025 - Apr 2026 | Stocks: {stock_count} A+B category")
print()
print("LEGEND: <<< = 5-day win rate >= 60% (actionable edge)")
print()

# Organize by category
categories = {
    "TREND SIGNALS": [
        ("MACD Bullish Cross (line crosses above signal)", "macd_cross_bull"),
        ("MACD Bearish Cross (line crosses below signal)", "macd_cross_bear"),
        ("Golden Cross (SMA50 > SMA200)", "golden_cross"),
        ("Death Cross (SMA50 < SMA200)", "death_cross"),
        ("ADX Trend Breakout (ADX crosses 25 + green candle)", "adx_trend_breakout"),
    ],
    "MONEY FLOW": [
        ("CMF Flips Positive (crosses 0 from below)", "cmf_flip_positive"),
        ("CMF Flips Negative (crosses 0 from above)", "cmf_flip_negative"),
        ("OBV Bullish Divergence (price lower low, OBV higher low)", "obv_divergence_bull"),
    ],
    "VOLATILITY & BREAKOUT": [
        ("BB Squeeze Breakout UP (squeeze then break upper band)", "bb_squeeze_break_up"),
        ("BB Squeeze Breakout DOWN (squeeze then break lower band)", "bb_squeeze_break_down"),
        ("Volume Spike + Green Candle (>2.5x avg volume, close up)", "volume_spike_green"),
        ("Volume Spike + Red Candle (>2.5x avg volume, close down)", "volume_spike_red"),
    ],
    "GAP ANALYSIS": [
        ("Gap Up + Held (>2% gap, closed above open)", "gap_up_hold"),
        ("Gap Up + Faded (>2% gap, closed below open)", "gap_up_fade"),
        ("Gap Down + Bounced (>2% gap down, closed green)", "gap_down_bounce"),
    ],
    "SUPPORT BOUNCE (EMA)": [
        ("EMA21 Bounce (low touched EMA21, closed above + green)", "ema21_bounce"),
        ("EMA50 Bounce (low touched EMA50, closed above + green)", "ema50_bounce"),
        ("SMA200 Bounce (low touched SMA200, closed above + green)", "sma200_bounce"),
    ],
    "CANDLESTICK PATTERNS": [
        ("Hammer at 20-day Low", "hammer_at_low"),
        ("Bullish Engulfing", "engulfing_bullish"),
        ("Shooting Star at 20-day High", "shooting_star_at_high"),
    ],
    "MEAN REVERSION": [
        ("3 Consecutive Red Days (buy the dip?)", "three_red_bounce"),
        ("3 Consecutive Green Days (sell the pop?)", "three_green_sell"),
        ("RSI < 30 Oversold Bounce", "rsi_oversold_bounce"),
        ("RSI > 70 Overbought Sell", "rsi_overbought_sell"),
    ],
    "MARKET REGIME": [
        ("Buy on DSEX Green Day", "dsex_up_buy"),
        ("Buy on DSEX Red Day", "dsex_down_buy"),
    ],
}

all_results = {}
for cat_name, items in categories.items():
    print(f"\n{'#'*70}")
    print(f"# {cat_name}")
    print(f"{'#'*70}")
    for desc, key in items:
        events = all_events[key]
        rdf = report(desc, events)
        if rdf is not None:
            v5 = rdf["ret_5d"].dropna()
            if len(v5) > 0:
                wr = (v5 > 0).sum() / len(v5) * 100
                all_results[key] = {"desc": desc, "n": len(rdf), "wr_5d": wr, "avg_5d": v5.mean()}

# === FINAL RANKING ===
print(f"\n{'='*70}")
print("FINAL RANKING — ALL FACTORS BY 5-DAY WIN RATE")
print(f"{'='*70}\n")

ranked = sorted(all_results.items(), key=lambda x: -x[1]["wr_5d"])
print(f"{'Rank':>4} {'Factor':<55} {'Events':>7} {'WinRate':>8} {'AvgRet':>8}")
print("-" * 85)
for rank, (key, info) in enumerate(ranked, 1):
    marker = " ***" if info["wr_5d"] >= 60 else ""
    print(f"{rank:4d} {info['desc'][:55]:<55} {info['n']:7d} {info['wr_5d']:7.0f}% {info['avg_5d']:+7.2f}%{marker}")

print()
print("*** = 60%+ win rate (statistically significant edge)")
