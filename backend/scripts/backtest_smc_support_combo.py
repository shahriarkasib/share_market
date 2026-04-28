#!/usr/bin/env python3
"""
Backtest Sourav's SMC + Support combo strategy on 2 years of DSE data.

Two entry patterns:
  A) FRESH_FVG: today's low touches a fresh (unmitigated) bullish FVG AND
     today closes green (close > open) → BUY at close
  B) SUPPORT: all recent bullish FVGs are mitigated AND today's low touches
     a multi-touch support level (>=2 prior touches in last 60 bars) AND
     today closes green → BUY at close

Exit rules (per trade):
  - Stop loss: low of entry candle - 1 tick (0.10 BDT)
  - Target 1: +5%   (take 50%)
  - Target 2: +10%  (take rest)
  - Time stop: 15 trading days
  - Trail stop after T1 hit: stop moves to entry (breakeven)

Walk-forward: at each bar t, signals only use data up to t (no lookahead).

Usage:
  cd backend && ./venv/bin/python3 scripts/backtest_smc_support_combo.py \
      [--symbols GP,LOVELLO,...] [--days 730] [--min-trades 5]
"""
from __future__ import annotations

import argparse
import sys
import os
from collections import defaultdict
from datetime import timedelta

# Add parent dir to path
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

import pandas as pd
from data.repository import read_historical_for_symbol


# ─────────────────────────────────────────────────────────────────────────
#  Lightweight SMC primitives — duplicated from api.smc_chart.py to keep
#  the backtest standalone and explicitly walk-forward (no lookahead).
# ─────────────────────────────────────────────────────────────────────────

def find_swings(h, l, n=3):
    swings = []
    for i in range(n, len(h) - n):
        if float(h.iloc[i]) == float(h.iloc[max(0, i - n):i + n + 1].max()):
            swings.append({"idx": i, "type": "high", "price": float(h.iloc[i])})
        if float(l.iloc[i]) == float(l.iloc[max(0, i - n):i + n + 1].min()):
            swings.append({"idx": i, "type": "low", "price": float(l.iloc[i])})
    return swings


def detect_fvgs_validated(o, h, l, c, v, end_idx):
    """SMC-grade FVGs computed using only bars [0..end_idx]. Returns list with
    type/top/bottom/idx/start_idx/valid."""
    fvgs = []
    if end_idx < 2:
        return fvgs
    for i in range(2, end_idx + 1):
        # Bullish FVG: c[i-2].high < c[i].low
        if float(h.iloc[i - 2]) < float(l.iloc[i]):
            mo, mc = float(o.iloc[i - 1]), float(c.iloc[i - 1])
            mh, ml = float(h.iloc[i - 1]), float(l.iloc[i - 1])
            top, bot = float(l.iloc[i]), float(h.iloc[i - 2])
            size_pct = (top - bot) / bot * 100 if bot > 0 else 0
            mid_range = max(mh - ml, 1e-9)
            f1 = mc > mo
            f2 = (mc - mo) / mid_range >= 0.40 if f1 else False
            f3 = size_pct >= 0.30
            f4 = True
            if v is not None:
                lo = max(0, i - 21)
                hi = i - 1
                if hi > lo:
                    med = float(v.iloc[lo:hi].median() or 0)
                    if med > 0:
                        f4 = float(v.iloc[i - 1]) >= med * 0.9
            quality = sum([f1, f2, f3, f4])
            if quality >= 3:
                fvgs.append({
                    "type": "bullish", "idx": i - 1, "start_idx": i - 2,
                    "top": top, "bottom": bot,
                })
    return fvgs


def fvg_mitigation_state(fvgs, l_series, end_idx):
    """For each bullish FVG, decide if mitigated by any close beyond bottom
    in bars [fvg.idx+1 .. end_idx]."""
    out = []
    for f in fvgs:
        mitigated = False
        for j in range(f["idx"] + 1, end_idx + 1):
            if float(l_series.iloc[j]) < f["bottom"]:
                mitigated = True
                break
        out.append({**f, "mitigated": mitigated})
    return out


def detect_structure_events(swings):
    """Walk-forward BOS/ChoCh detection — same as api.smc_chart but standalone."""
    events = []
    trend = None
    last_sh = None
    last_sl = None
    for sw in swings:
        if sw["type"] == "high":
            if last_sh is not None:
                if sw["price"] > last_sh["price"]:
                    if trend == "up":
                        events.append({"idx": sw["idx"], "type": "bullish_BOS"})
                    elif trend == "down":
                        events.append({"idx": sw["idx"], "type": "bullish_ChoCh"})
                        trend = "up"
                    else:
                        trend = "up"
                elif sw["price"] < last_sh["price"] and trend is None:
                    trend = "down"
            last_sh = sw
        elif sw["type"] == "low":
            if last_sl is not None:
                if sw["price"] < last_sl["price"]:
                    if trend == "down":
                        events.append({"idx": sw["idx"], "type": "bearish_BOS"})
                    elif trend == "up":
                        events.append({"idx": sw["idx"], "type": "bearish_ChoCh"})
                        trend = "down"
                    else:
                        trend = "down"
                elif sw["price"] > last_sl["price"] and trend is None:
                    trend = "up"
            last_sl = sw
    return events


def is_uptrend(events, t, lookback_bars=20):
    """True when latest event is bullish AND no bearish event in last `lookback_bars`."""
    if not events:
        return False
    latest = events[-1]
    if not latest["type"].startswith("bullish"):
        return False
    # No bearish events in recent window
    for ev in events:
        if ev["idx"] >= t - lookback_bars and ev["type"].startswith("bearish"):
            return False
    return True


def is_consolidating(df, t, window=20, range_threshold=0.05):
    """True when last `window` bars cover < range_threshold of price (flat range)."""
    start = max(0, t - window + 1)
    rh = float(df["high"].iloc[start:t + 1].max())
    rl = float(df["low"].iloc[start:t + 1].min())
    cp = float(df["close"].iloc[t])
    if cp <= 0 or rh <= rl:
        return True
    return (rh - rl) / cp < range_threshold


def calc_adx(df, end_idx, period=14):
    """Wilder ADX(14) computed using bars [0..end_idx]. Returns float or None."""
    if end_idx < period * 2:
        return None
    h = df["high"].iloc[: end_idx + 1].astype(float).values
    l = df["low"].iloc[: end_idx + 1].astype(float).values
    c = df["close"].iloc[: end_idx + 1].astype(float).values
    n = len(h)
    tr = [0.0] * n
    plus_dm = [0.0] * n
    minus_dm = [0.0] * n
    for i in range(1, n):
        tr[i] = max(h[i] - l[i], abs(h[i] - c[i - 1]), abs(l[i] - c[i - 1]))
        up = h[i] - h[i - 1]
        down = l[i - 1] - l[i]
        plus_dm[i] = up if (up > down and up > 0) else 0
        minus_dm[i] = down if (down > up and down > 0) else 0

    # Wilder smoothing
    def wilder(arr, p):
        out = [0.0] * len(arr)
        first = sum(arr[1:p + 1])
        out[p] = first
        for i in range(p + 1, len(arr)):
            out[i] = out[i - 1] - (out[i - 1] / p) + arr[i]
        return out

    atr = wilder(tr, period)
    pdms = wilder(plus_dm, period)
    mdms = wilder(minus_dm, period)
    dx = [0.0] * n
    for i in range(period, n):
        if atr[i] == 0:
            continue
        pdi = 100 * pdms[i] / atr[i]
        mdi = 100 * mdms[i] / atr[i]
        s = pdi + mdi
        if s > 0:
            dx[i] = 100 * abs(pdi - mdi) / s
    # ADX = Wilder MA of DX
    adx = [0.0] * n
    if 2 * period < n:
        adx[2 * period] = sum(dx[period + 1: 2 * period + 1]) / period
        for i in range(2 * period + 1, n):
            adx[i] = (adx[i - 1] * (period - 1) + dx[i]) / period
    return adx[end_idx] if adx[end_idx] > 0 else None


def is_trendy(df, t, adx_min=25, return_min=0.0, range_min=0.08):
    """Trendiness gate: ADX>25, 90-bar return positive, 20-day range > 8%.
    All three guard against trading in flat/choppy/declining markets where
    FVG strategies have no edge."""
    adx = calc_adx(df, t)
    if adx is None or adx < adx_min:
        return False
    # 90-bar return must be positive (we only buy in uptrends)
    lookback90 = max(0, t - 90)
    ret_90 = (float(df["close"].iloc[t]) - float(df["close"].iloc[lookback90])) / float(df["close"].iloc[lookback90])
    if ret_90 < return_min:
        return False
    # 20-day range as % of price
    start20 = max(0, t - 20)
    rh = float(df["high"].iloc[start20: t + 1].max())
    rl = float(df["low"].iloc[start20: t + 1].min())
    cp = float(df["close"].iloc[t])
    if cp <= 0 or (rh - rl) / cp < range_min:
        return False
    return True


def detect_support_levels(h, l, end_idx, lookback=60, min_touches=2, tick=0.10):
    """Cluster swing lows from the last `lookback` bars; return support levels
    with >= min_touches. Each level is the median of its cluster."""
    if end_idx < 5:
        return []
    start = max(0, end_idx - lookback + 1)
    swings = find_swings(h.iloc[start:end_idx + 1], l.iloc[start:end_idx + 1], n=2)
    lows = [s["price"] for s in swings if s["type"] == "low"]
    if not lows:
        return []
    lows.sort()
    clusters = []
    current = [lows[0]]
    for px in lows[1:]:
        if (px - current[-1]) / current[-1] < 0.015:  # within 1.5%
            current.append(px)
        else:
            clusters.append(current)
            current = [px]
    clusters.append(current)
    levels = []
    for cl in clusters:
        if len(cl) >= min_touches:
            mid = sum(cl) / len(cl)
            levels.append({"price": round(mid, 2), "touches": len(cl)})
    return levels


# ─────────────────────────────────────────────────────────────────────────
#  Backtest engine
# ─────────────────────────────────────────────────────────────────────────

TICK = 0.10
TARGET1_PCT = 0.05
TARGET2_PCT = 0.10
TIME_STOP_DAYS = 15


def simulate_trade(df, entry_idx, entry_price, stop_loss):
    """Walk forward from entry_idx+1 until exit. Returns dict with P&L."""
    n = len(df)
    target1 = entry_price * (1 + TARGET1_PCT)
    target2 = entry_price * (1 + TARGET2_PCT)
    half_size = 0.5
    remaining = 1.0
    realized_pl_pct = 0.0
    t1_hit = False
    exit_idx = None
    exit_reason = None

    for j in range(entry_idx + 1, min(n, entry_idx + 1 + TIME_STOP_DAYS)):
        bar_high = float(df["high"].iloc[j])
        bar_low = float(df["low"].iloc[j])

        # Stop check first (conservative)
        if bar_low <= stop_loss:
            realized_pl_pct += remaining * ((stop_loss - entry_price) / entry_price * 100)
            exit_idx = j
            exit_reason = "stop"
            remaining = 0
            break

        # T1 hit
        if not t1_hit and bar_high >= target1:
            realized_pl_pct += half_size * TARGET1_PCT * 100
            remaining -= half_size
            t1_hit = True
            stop_loss = entry_price  # move to breakeven

        # T2 hit on remaining
        if remaining > 0 and bar_high >= target2:
            realized_pl_pct += remaining * TARGET2_PCT * 100
            exit_idx = j
            exit_reason = "target2"
            remaining = 0
            break

    if remaining > 0:
        # Time stop or end of data → close at last close
        last_idx = min(n - 1, entry_idx + TIME_STOP_DAYS)
        last_close = float(df["close"].iloc[last_idx])
        realized_pl_pct += remaining * ((last_close - entry_price) / entry_price * 100)
        exit_idx = last_idx
        exit_reason = "time" if last_idx == entry_idx + TIME_STOP_DAYS else "end"

    return {
        "entry_idx": entry_idx,
        "exit_idx": exit_idx,
        "exit_reason": exit_reason,
        "entry_price": round(entry_price, 2),
        "stop_loss": round(stop_loss, 2),
        "pl_pct": round(realized_pl_pct, 2),
        "win": realized_pl_pct > 0,
        "t1_hit": t1_hit,
    }


def backtest_symbol(symbol: str, df: pd.DataFrame, warmup: int = 60,
                     require_uptrend: bool = True, skip_consolidation: bool = True,
                     require_trendy: bool = True):
    """Walk forward over df, generate signals, simulate each trade.

    `require_uptrend`: only enter when latest structure event is bullish AND
       no bearish event in last 20 bars.
    `skip_consolidation`: skip when last 20 bars range < 5% (flat market).
    Both filter rationale: FVGs have edge in trending markets, not in
    accumulation / consolidation where they're random noise.
    """
    if len(df) < warmup + 30:
        return []

    o = df["open"].astype(float)
    h = df["high"].astype(float)
    l = df["low"].astype(float)
    c = df["close"].astype(float)
    v = df["volume"].astype(float) if "volume" in df.columns else None

    trades = []
    in_trade_until = -1  # avoid overlapping trades

    for t in range(warmup, len(df) - 5):
        if t <= in_trade_until:
            continue

        is_green = float(c.iloc[t]) > float(o.iloc[t])
        if not is_green:
            continue

        # Trend / consolidation gates — FVGs only work in clean uptrends
        swings_t = find_swings(h.iloc[: t + 1], l.iloc[: t + 1], n=3)
        events_t = detect_structure_events(swings_t)
        if require_uptrend and not is_uptrend(events_t, t):
            continue
        if skip_consolidation and is_consolidating(df, t):
            continue
        if require_trendy and not is_trendy(df, t):
            continue

        bar_low = float(l.iloc[t])
        bar_close = float(c.iloc[t])

        # State as of bar t (no lookahead)
        fvgs = detect_fvgs_validated(o, h, l, c, v, end_idx=t)
        fvgs = fvg_mitigation_state(fvgs, l, end_idx=t)
        # Only consider FVGs in the last 60 bars
        recent_fvgs = [f for f in fvgs if f["idx"] >= t - 60]

        fresh_bull_fvgs = [
            f for f in recent_fvgs
            if not f["mitigated"] and f["top"] < bar_close
        ]

        # ─── Pattern detection (priority: CONFLUENCE > FRESH_FVG > SUPPORT) ───
        pattern = None
        entry_fvg = None
        support_used = None

        # CONFLUENCE: fresh FVG zone overlaps a prominent multi-touch support
        # Today's low taps both — strongest setup
        supports = detect_support_levels(h, l, end_idx=t, lookback=60, min_touches=2)
        for f in fresh_bull_fvgs:
            if not (bar_low <= f["top"] and bar_low >= f["bottom"] * 0.985):
                continue
            for s in supports:
                # Support level falls within or near (±2%) the FVG zone
                in_fvg = f["bottom"] * 0.98 <= s["price"] <= f["top"] * 1.02
                if in_fvg:
                    pattern = "CONFLUENCE"
                    entry_fvg = f
                    support_used = s
                    break
            if pattern:
                break

        # FRESH_FVG only (no overlapping support)
        if pattern is None:
            for f in fresh_bull_fvgs:
                if bar_low <= f["top"] and bar_low >= f["bottom"] * 0.985:
                    pattern = "FRESH_FVG"
                    entry_fvg = f
                    break

        # SUPPORT only (all FVGs mitigated, but support holds)
        if pattern is None and len([f for f in recent_fvgs if not f["mitigated"]]) == 0:
            for s in supports:
                if abs(bar_low - s["price"]) / s["price"] <= 0.02:
                    pattern = "SUPPORT"
                    support_used = s
                    break

        if pattern is None:
            continue

        entry_price = bar_close
        if pattern == "FRESH_FVG":
            stop_loss = max(entry_fvg["bottom"] - TICK, bar_low - TICK)
        else:
            stop_loss = bar_low - TICK

        # Sanity: skip if stop is too tight (<1%) or too wide (>8%)
        risk_pct = (entry_price - stop_loss) / entry_price
        if risk_pct < 0.01 or risk_pct > 0.08:
            continue

        result = simulate_trade(df, t, entry_price, stop_loss)
        result.update({
            "symbol": symbol,
            "pattern": pattern,
            "entry_date": df["date"].iloc[t].strftime("%Y-%m-%d"),
            "exit_date": df["date"].iloc[result["exit_idx"]].strftime("%Y-%m-%d"),
            "fvg_size": entry_fvg["top"] - entry_fvg["bottom"] if entry_fvg else None,
            "support_touches": support_used["touches"] if support_used else None,
        })
        trades.append(result)
        in_trade_until = result["exit_idx"]

    return trades


def aggregate(trades):
    if not trades:
        return None
    wins = [t for t in trades if t["win"]]
    losses = [t for t in trades if not t["win"]]
    total = len(trades)
    win_rate = len(wins) / total * 100
    avg_win = sum(t["pl_pct"] for t in wins) / len(wins) if wins else 0
    avg_loss = sum(t["pl_pct"] for t in losses) / len(losses) if losses else 0
    expectancy = (win_rate / 100) * avg_win + (1 - win_rate / 100) * avg_loss
    profit_factor = (
        sum(t["pl_pct"] for t in wins) / abs(sum(t["pl_pct"] for t in losses))
        if losses and sum(t["pl_pct"] for t in losses) != 0
        else float("inf")
    )
    return {
        "trades": total,
        "wins": len(wins),
        "losses": len(losses),
        "win_rate": round(win_rate, 1),
        "avg_win_pct": round(avg_win, 2),
        "avg_loss_pct": round(avg_loss, 2),
        "expectancy_pct": round(expectancy, 2),
        "profit_factor": round(profit_factor, 2) if profit_factor != float("inf") else None,
        "total_pl_pct": round(sum(t["pl_pct"] for t in trades), 1),
    }


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--symbols", default="", help="comma-separated; empty = all DSE A-cat")
    ap.add_argument("--days", type=int, default=730)
    ap.add_argument("--min-trades", type=int, default=3,
                    help="minimum trades per symbol to include in per-symbol report")
    ap.add_argument("--top-n", type=int, default=20)
    ap.add_argument("--no-uptrend-filter", action="store_true",
                    help="disable uptrend gate (trade in any structure)")
    ap.add_argument("--no-consolidation-filter", action="store_true",
                    help="disable consolidation skip")
    ap.add_argument("--no-trendiness-filter", action="store_true",
                    help="disable ADX trendiness gate")
    args = ap.parse_args()

    if args.symbols:
        symbols = [s.strip().upper() for s in args.symbols.split(",")]
    else:
        from database import get_connection
        conn = get_connection()
        rows = conn.execute(
            "SELECT symbol FROM fundamentals WHERE category = 'A' ORDER BY symbol"
        ).fetchall()
        conn.close()
        symbols = [r[0] for r in rows]

    print(f"Backtesting {len(symbols)} symbols, {args.days} days lookback")
    print("=" * 80)

    all_trades = []
    per_symbol = {}
    by_pattern = defaultdict(list)

    for i, sym in enumerate(symbols, 1):
        try:
            df = read_historical_for_symbol(sym, min_rows=args.days // 2)
            if df is None or df.empty:
                continue
            df = df.sort_values("date").reset_index(drop=True)
            df = df.tail(args.days).reset_index(drop=True)
            trades = backtest_symbol(
                sym, df,
                require_uptrend=not args.no_uptrend_filter,
                skip_consolidation=not args.no_consolidation_filter,
                require_trendy=not args.no_trendiness_filter,
            )
            if trades:
                all_trades.extend(trades)
                per_symbol[sym] = trades
                for t in trades:
                    by_pattern[t["pattern"]].append(t)
            if i % 25 == 0:
                print(f"  ... processed {i}/{len(symbols)}")
        except Exception as e:
            print(f"  {sym} error: {e}")
            continue

    # ─── Overall ───
    print()
    print("─" * 80)
    print("OVERALL  (all stocks, all patterns)")
    print("─" * 80)
    overall = aggregate(all_trades)
    if overall:
        for k, v in overall.items():
            print(f"  {k:>16}: {v}")
    else:
        print("  (no trades generated)")

    # ─── By pattern ───
    print()
    print("─" * 80)
    print("BY PATTERN")
    print("─" * 80)
    for pat in ("CONFLUENCE", "FRESH_FVG", "SUPPORT"):
        agg = aggregate(by_pattern.get(pat, []))
        print(f"\n{pat}:")
        if agg:
            for k, v in agg.items():
                print(f"  {k:>16}: {v}")
        else:
            print("  (no trades)")

    # ─── Top symbols ───
    print()
    print("─" * 80)
    print(f"TOP {args.top_n} SYMBOLS BY WIN RATE  (min {args.min_trades} trades)")
    print("─" * 80)
    sym_aggs = []
    for sym, trades in per_symbol.items():
        if len(trades) >= args.min_trades:
            agg = aggregate(trades)
            agg["symbol"] = sym
            sym_aggs.append(agg)
    sym_aggs.sort(key=lambda x: (-x["win_rate"], -x["trades"]))

    print(f"\n  {'symbol':<12} {'trades':>6} {'win%':>6} {'avg_win':>8} {'avg_loss':>9} {'total_pl':>9}")
    for a in sym_aggs[: args.top_n]:
        print(f"  {a['symbol']:<12} {a['trades']:>6} {a['win_rate']:>6.1f} "
              f"{a['avg_win_pct']:>8.2f} {a['avg_loss_pct']:>9.2f} {a['total_pl_pct']:>9.1f}")

    print()
    print(f"Backtest done.  Total trades: {len(all_trades)}  "
          f"Symbols with trades: {len(per_symbol)}")


if __name__ == "__main__":
    main()
