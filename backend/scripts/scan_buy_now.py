#!/usr/bin/env python3
"""Scan for stocks matching proven buy setups RIGHT NOW."""

import warnings; warnings.filterwarnings("ignore")
import sys, os
import pandas as pd
import numpy as np
import psycopg2
import psycopg2.extras

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from config import DATABASE_URL

conn = psycopg2.connect(DATABASE_URL, cursor_factory=psycopg2.extras.RealDictCursor)
cur = conn.cursor()

print("=" * 70)
print("DSE PROVEN BUY SETUPS — LIVE SCAN")
print("Based on 6-month backtest of 277 stocks")
print("=" * 70)

# Get all data in one query
cur.execute("""
    SELECT lp.symbol, lp.ltp, lp.change_pct, lp.volume,
           si.rsi_14, si.cmf_20, si.cmf_pos_streak, si.cmf_neg_streak,
           si.adx_14, si.vol_ratio, si.chg_5d, si.chg_20d,
           si.obv_slope_10d, si.price_slope_10d, si.macd_hist,
           ps.support_levels, ps.resistance_levels, ps.pivot_daily,
           ps.candle_pattern, ps.candle_confirmed, ps.swing_structure,
           ps.mean_reversion_score, ps.fib_levels,
           f.category, f.sector, f.pe_ratio
    FROM live_prices lp
    JOIN stock_indicators si ON lp.symbol = si.symbol AND si.timeframe = 'daily'
      AND si.date = (SELECT MAX(date) FROM stock_indicators WHERE timeframe = 'daily')
    JOIN price_structure ps ON lp.symbol = ps.symbol
      AND ps.date = (SELECT MAX(date) FROM price_structure)
    JOIN fundamentals f ON lp.symbol = f.symbol
    WHERE lp.ltp > 0 AND f.category IN ('A','B') AND lp.volume > 10000
""")
rows = cur.fetchall()
conn.close()

setup1 = []  # Support + RSI < 40 (79%)
setup1_best = []  # Support + RSI < 40 + bullish candle (83%)
setup2 = []  # RSI < 30 (74%)
setup3 = []  # Dropped 5%+ in 5 days (proxy for 3 red days) (63%)
setup4 = []  # OBV divergence (61%)

for r in rows:
    ltp = float(r["ltp"])
    rsi = float(r["rsi_14"] or 50)
    cmf = float(r["cmf_20"] or 0)
    chg5d = float(r["chg_5d"] or 0)
    obv_slope = float(r["obv_slope_10d"] or 0)
    price_slope = float(r["price_slope_10d"] or 0)
    sups = r["support_levels"] or []
    ress = r["resistance_levels"] or []
    pivot = r["pivot_daily"] or {}
    candle = r["candle_pattern"] or ""
    confirmed = r["candle_confirmed"] or False
    mr = r["mean_reversion_score"] or 0
    vol = int(r["volume"] or 0)

    p_r1 = pivot.get("r1", "")
    p_s1 = pivot.get("s1", "")

    # Check if at support (3+ touches, within 2%)
    at_support = False
    sup_info = ""
    for s in sups[:3]:
        dist = abs(ltp - s["price"]) / ltp * 100
        if dist < 2 and s["touches"] >= 3:
            at_support = True
            sup_info = f'{s["price"]}({s["touches"]}T)'
            break

    nearest_res = f'{ress[0]["price"]}({ress[0]["touches"]}T)' if ress else "none"
    bullish_candle = candle in ("HAMMER", "BULLISH_ENGULFING", "BULLISH_MARUBOZU", "DRAGONFLY_DOJI", "BULLISH_HARAMI", "INVERTED_HAMMER")

    info = {
        "symbol": r["symbol"],
        "ltp": ltp,
        "rsi": rsi,
        "cmf": cmf,
        "chg5d": chg5d,
        "vol": vol,
        "support": sup_info,
        "resistance": nearest_res,
        "r1": p_r1,
        "s1": p_s1,
        "candle": candle,
        "confirmed": confirmed,
        "mr": mr,
        "sector": r["sector"] or "",
        "cat": r["category"],
    }

    # Setup 1: Support + RSI < 40
    if at_support and rsi < 40:
        setup1.append(info)
        if bullish_candle and confirmed:
            setup1_best.append(info)

    # Setup 2: RSI < 30
    if rsi < 30:
        setup2.append(info)

    # Setup 3: Dropped 5%+ in 5 days
    if chg5d < -5:
        setup3.append(info)

    # Setup 4: OBV divergence (price falling, OBV rising)
    if price_slope < 0 and obv_slope > 0:
        setup4.append(info)


def print_stocks(stocks, show_candle=False):
    if not stocks:
        print("  None found.\n")
        return
    for s in sorted(stocks, key=lambda x: x["rsi"]):
        candle_str = f" Candle={s['candle']}" if show_candle and s["candle"] else ""
        print(f"  {s['symbol']:12s} ৳{s['ltp']:7.1f} RSI={s['rsi']:5.1f} CMF={s['cmf']:+.3f} "
              f"5D={s['chg5d']:+.1f}% Sup={s['support']} Res={s['resistance']} "
              f"R1={s['r1']} MR={s['mr']}{candle_str}")


# === REPORT ===

print(f"\n{'='*70}")
print(f"SETUP 1: Strong Support + RSI < 40  |  79% win rate, +3.36% in 5 days")
print(f"{'='*70}")
print(f"Found: {len(setup1)} stocks")
print_stocks(setup1, show_candle=True)

if setup1_best:
    print(f"\n  *** BEST COMBO (83% win): Support + RSI<40 + Bullish Candle Confirmed ***")
    print_stocks(setup1_best, show_candle=True)

print(f"\n{'='*70}")
print(f"SETUP 2: RSI < 30 Deeply Oversold  |  74% win rate, +4.04% in 5 days")
print(f"{'='*70}")
print(f"Found: {len(setup2)} stocks")
print_stocks(setup2)

print(f"\n{'='*70}")
print(f"SETUP 3: Dropped 5%+ in 5 Days (Mean Reversion)  |  63% win, +1.79%")
print(f"{'='*70}")
print(f"Found: {len(setup3)} stocks")
print_stocks(sorted(setup3, key=lambda x: x["chg5d"]))

print(f"\n{'='*70}")
print(f"SETUP 4: OBV Divergence (price down, OBV up)  |  61% win, +1.42%")
print(f"{'='*70}")
print(f"Found: {len(setup4)} stocks")
# Only show with RSI < 50 (more likely to bounce)
setup4_filtered = [s for s in setup4 if s["rsi"] < 50]
print(f"Filtered (RSI < 50): {len(setup4_filtered)}")
print_stocks(setup4_filtered)

# === MULTI-SETUP OVERLAP ===
print(f"\n{'='*70}")
print(f"STOCKS APPEARING IN MULTIPLE SETUPS (highest conviction)")
print(f"{'='*70}")

all_symbols = {}
for setup_name, stocks in [("Sup+RSI40", setup1), ("RSI30", setup2), ("3RedDays", setup3), ("OBVDiv", setup4_filtered)]:
    for s in stocks:
        sym = s["symbol"]
        if sym not in all_symbols:
            all_symbols[sym] = {"info": s, "setups": []}
        all_symbols[sym]["setups"].append(setup_name)

multi = {k: v for k, v in all_symbols.items() if len(v["setups"]) >= 2}
if multi:
    for sym, data in sorted(multi.items(), key=lambda x: -len(x[1]["setups"])):
        s = data["info"]
        setups = " + ".join(data["setups"])
        print(f"  {sym:12s} ৳{s['ltp']:7.1f} RSI={s['rsi']:5.1f} CMF={s['cmf']:+.3f} [{setups}]")
else:
    print("  None found with multiple setups overlap.")

print()
