#!/usr/bin/env python3
"""Audit + remove phantom bars from daily_prices.

A phantom bar = a row dated on a non-market day (Fri/Sat for DSE) OR a
row where open/close are suspiciously equal to the previous bar's close
(suggesting it was fabricated by a buggy live-bar appender).

Usage:
  ./venv/bin/python3 scripts/audit_phantom_bars.py            # report only
  ./venv/bin/python3 scripts/audit_phantom_bars.py --delete   # delete them
"""
from __future__ import annotations
import argparse
import sys
import os
from datetime import datetime, timedelta

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from database import get_connection


def is_dse_trading_day(d) -> bool:
    """Sun-Thu in Bangladesh = trading days. Mon=0, Sun=6."""
    return d.weekday() in (6, 0, 1, 2, 3)


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--delete", action="store_true", help="actually delete; default is dry-run")
    ap.add_argument("--days-back", type=int, default=14, help="check last N days")
    args = ap.parse_args()

    conn = get_connection()
    cutoff = (datetime.utcnow() - timedelta(days=args.days_back)).date()

    # 1. Bars dated on non-market days (Fri/Sat)
    rows = conn.execute(
        "SELECT symbol, date, open, high, low, close, volume "
        "FROM daily_prices WHERE date >= %s ORDER BY date DESC, symbol",
        (cutoff,),
    ).fetchall()

    bad_weekend = []
    bad_phantom = []
    for r in rows:
        d = r["date"]
        if hasattr(d, "date"):
            d = d.date()
        if not is_dse_trading_day(d):
            bad_weekend.append((r["symbol"], str(d), float(r["open"] or 0),
                                float(r["high"] or 0), float(r["low"] or 0),
                                float(r["close"] or 0)))

    # 2. Phantom-pattern: open == close == previous-bar close (very suspicious)
    by_symbol: dict = {}
    for r in rows:
        by_symbol.setdefault(r["symbol"], []).append(r)
    for sym, bars in by_symbol.items():
        bars_sorted = sorted(bars, key=lambda x: x["date"])
        for i in range(1, len(bars_sorted)):
            cur = bars_sorted[i]
            prev = bars_sorted[i - 1]
            try:
                cur_o = float(cur["open"] or 0)
                cur_c = float(cur["close"] or 0)
                prev_c = float(prev["close"] or 0)
                cur_h = float(cur["high"] or 0)
                cur_l = float(cur["low"] or 0)
                prev_h = float(prev["high"] or 0)
                prev_l = float(prev["low"] or 0)
            except Exception:
                continue
            # Pattern: open == close == prev close AND high/low match prev day
            if (cur_o == cur_c == prev_c and prev_c > 0 and
                cur_h == prev_h and cur_l == prev_l):
                bad_phantom.append((sym, str(cur["date"]), cur_o, cur_h, cur_l, cur_c))

    print(f"Audit period: last {args.days_back} days, from {cutoff}")
    print(f"Total bars examined: {len(rows)}")
    print()
    print(f"⚠ {len(bad_weekend)} bars on non-market days (Fri/Sat)")
    for b in bad_weekend[:25]:
        print(f"  {b[0]:<12} {b[1]} O:{b[2]:.1f} H:{b[3]:.1f} L:{b[4]:.1f} C:{b[5]:.1f}")
    if len(bad_weekend) > 25:
        print(f"  ... +{len(bad_weekend) - 25} more")
    print()
    print(f"⚠ {len(bad_phantom)} phantom bars (open=close=prev_close + same H/L as prev)")
    for b in bad_phantom[:25]:
        print(f"  {b[0]:<12} {b[1]} O:{b[2]:.1f} H:{b[3]:.1f} L:{b[4]:.1f} C:{b[5]:.1f}")
    if len(bad_phantom) > 25:
        print(f"  ... +{len(bad_phantom) - 25} more")

    if args.delete and (bad_weekend or bad_phantom):
        print()
        print("DELETING phantom rows...")
        for b in bad_weekend + bad_phantom:
            conn.execute(
                "DELETE FROM daily_prices WHERE symbol = %s AND date = %s",
                (b[0], b[1]),
            )
        conn.commit()
        print(f"deleted {len(bad_weekend) + len(bad_phantom)} rows")
    elif bad_weekend or bad_phantom:
        print()
        print("Run with --delete to remove these rows")
    conn.close()


if __name__ == "__main__":
    main()
