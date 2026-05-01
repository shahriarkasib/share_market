#!/usr/bin/env python3
"""Walk the backtest engine forward and report the BUY signals that fired
on the LAST N bars of each stock — i.e. "what could we have bought yesterday
and today?".

Uses the same patterns + filters as backtest_smc_support_combo.py so the
output reflects the deployed live recommender.
"""
from __future__ import annotations
import sys, os
from collections import defaultdict
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

import pandas as pd
from data.repository import read_historical_for_symbol
from scripts.backtest_smc_support_combo import (
    detect_fvgs_validated, fvg_mitigation_state, find_swings,
    detect_structure_events, is_uptrend, is_consolidating,
    is_trendy, detect_support_levels, TICK,
)


def find_recent_signals(symbol: str, df: pd.DataFrame, last_n_bars: int = 2):
    """Find 2-bar buy patterns in the last N bars.

    Sourav's rule: prior bar (T-1) wicked into a fresh bullish FVG,
    today (T) closes green = entry confirmed. Same logic for support taps.
    Reports the entry on bar T (today).
    """
    if len(df) < 80:
        return []
    o = df["open"].astype(float)
    h = df["high"].astype(float)
    l = df["low"].astype(float)
    c = df["close"].astype(float)
    v = df["volume"].astype(float) if "volume" in df.columns else None

    signals = []
    n = len(df)
    for t in range(max(60, n - last_n_bars), n):
        # Today must be green (confirmation candle)
        if float(c.iloc[t]) <= float(o.iloc[t]):
            continue
        # Yesterday must exist
        if t == 0:
            continue
        prev_t = t - 1

        swings_t = find_swings(h.iloc[: t + 1], l.iloc[: t + 1], n=3)
        events_t = detect_structure_events(swings_t)
        if not is_uptrend(events_t, t):
            continue
        if is_consolidating(df, t):
            continue
        if not is_trendy(df, t):
            continue

        prev_low = float(l.iloc[prev_t])
        bar_close = float(c.iloc[t])

        # FVG state as of yesterday (the touch day, not today)
        fvgs = detect_fvgs_validated(o, h, l, c, v, end_idx=prev_t)
        fvgs = fvg_mitigation_state(fvgs, l, end_idx=prev_t)
        recent_fvgs = [f for f in fvgs if f["idx"] >= prev_t - 60]
        fresh_bull_fvgs = [
            f for f in recent_fvgs
            if not f["mitigated"] and f["top"] < bar_close
        ]

        pattern, entry_fvg, support_used = None, None, None
        supports = detect_support_levels(h, l, end_idx=prev_t, lookback=60, min_touches=2)

        # 2-bar touch test: yesterday's low entered the FVG (any depth, even
        # boundary). Today's green confirmation supplies the bias filter.
        def _touched(f):
            # Any tag of the FVG zone (low <= top) AND not far below bottom
            return prev_low <= f["top"] and prev_low >= f["bottom"] * 0.97

        # CONFLUENCE first (fresh FVG + multi-touch support overlap)
        for f in fresh_bull_fvgs:
            if not _touched(f):
                continue
            for s in supports:
                if f["bottom"] * 0.98 <= s["price"] <= f["top"] * 1.02:
                    pattern, entry_fvg, support_used = "CONFLUENCE", f, s
                    break
            if pattern:
                break

        if pattern is None:
            for f in fresh_bull_fvgs:
                if _touched(f):
                    pattern, entry_fvg = "FRESH_FVG", f
                    break

        if pattern is None and len([f for f in recent_fvgs if not f["mitigated"]]) == 0:
            for s in supports:
                if abs(prev_low - s["price"]) / s["price"] <= 0.02:
                    pattern, support_used = "SUPPORT", s
                    break

        if pattern is None:
            continue

        entry = bar_close
        if entry_fvg:
            stop = max(entry_fvg["bottom"] - TICK, prev_low - TICK)
        else:
            stop = prev_low - TICK

        risk_pct = (entry - stop) / entry
        if risk_pct < 0.01 or risk_pct > 0.08:
            continue

        target1 = round(entry * 1.05, 2)
        target2 = round(entry * 1.10, 2)

        signals.append({
            "symbol": symbol,
            "date": df["date"].iloc[t].strftime("%Y-%m-%d"),
            "touch_date": df["date"].iloc[prev_t].strftime("%Y-%m-%d"),
            "pattern": pattern,
            "entry": round(entry, 2),
            "stop": round(stop, 2),
            "target1": target1,
            "target2": target2,
            "risk_pct": round(risk_pct * 100, 2),
            "rr_t1": round((target1 - entry) / (entry - stop), 2),
            "fvg_zone": f"{entry_fvg['bottom']:.1f}-{entry_fvg['top']:.1f}" if entry_fvg else None,
            "support_touches": support_used.get("touches") if support_used else None,
        })
    return signals


def main():
    from database import get_connection
    conn = get_connection()
    rows = conn.execute(
        "SELECT symbol FROM fundamentals WHERE category = 'A' ORDER BY symbol"
    ).fetchall()
    conn.close()
    symbols = [r[0] for r in rows]

    by_date = defaultdict(list)
    for sym in symbols:
        try:
            df = read_historical_for_symbol(sym, min_rows=200)
            if df is None or df.empty:
                continue
            df = df.sort_values("date").reset_index(drop=True).tail(400).reset_index(drop=True)
            sigs = find_recent_signals(sym, df, last_n_bars=5)
            for s in sigs:
                by_date[s["date"]].append(s)
        except Exception:
            continue

    if not by_date:
        print("No signals fired in the last 5 bars.")
        return

    for date in sorted(by_date.keys(), reverse=True):
        sigs = by_date[date]
        # Sort: CONFLUENCE first, then by best R/R
        order = {"CONFLUENCE": 0, "FRESH_FVG": 1, "SUPPORT": 2}
        sigs.sort(key=lambda s: (order.get(s["pattern"], 9), -s["rr_t1"]))
        print(f"\n{'=' * 90}")
        print(f"  📅 {date}  —  {len(sigs)} BUY signals")
        print('=' * 90)
        print(f"  {'symbol':<12} {'pattern':<11} {'entry':>7} {'stop':>7} {'T1':>7} {'risk%':>6} {'R/R':>5}  zone / extra")
        for s in sigs:
            extra = ""
            if s["pattern"] == "CONFLUENCE":
                extra = f"FVG {s['fvg_zone']} + {s['support_touches']}-touch support"
            elif s["pattern"] == "FRESH_FVG":
                extra = f"FVG {s['fvg_zone']}"
            elif s["pattern"] == "SUPPORT":
                extra = f"{s['support_touches']}-touch support"
            print(f"  {s['symbol']:<12} {s['pattern']:<11} {s['entry']:>7.2f} {s['stop']:>7.2f} "
                  f"{s['target1']:>7.2f} {s['risk_pct']:>5.1f}% {s['rr_t1']:>5.2f}  {extra}")


if __name__ == "__main__":
    main()
