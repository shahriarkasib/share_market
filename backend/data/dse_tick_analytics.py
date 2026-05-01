"""DSE tick analytics — true cumulative delta + footprint from `dse_ticks`.

Mirror of nasdaq_trading.tick_analytics for DSE. Powered by the LankaBD
tape scraper + Lee-Ready side classification.
"""
from __future__ import annotations

import os
import sys
from typing import Optional

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from database import get_connection


def has_ticks(symbol: str, since_hours: int = 24) -> bool:
    try:
        conn = get_connection()
        row = conn.execute(
            "SELECT COUNT(*) FROM dse_ticks WHERE symbol = ? "
            "AND ts >= NOW() - INTERVAL '? hours'".replace("?", "%s", 2),
            (symbol.upper(), since_hours),
        ).fetchone()
        conn.close()
        return (row[0] or 0) > 0
    except Exception:
        return False


def cumulative_delta(symbol: str, since_hours: int = 24) -> Optional[dict]:
    try:
        conn = get_connection()
        rows = conn.execute(
            """SELECT date_trunc('minute', ts) AS bucket,
                      SUM(CASE WHEN side='B' THEN size ELSE 0 END) AS buy_vol,
                      SUM(CASE WHEN side='S' THEN size ELSE 0 END) AS sell_vol,
                      SUM(size) AS total
               FROM dse_ticks
               WHERE symbol = %s AND ts >= NOW() - %s::interval
               GROUP BY bucket ORDER BY bucket""",
            (symbol.upper(), f"{since_hours} hours"),
        ).fetchall()
        conn.close()
        if not rows:
            return None

        cum = 0
        series = []
        for r in rows:
            buy_v = int(r["buy_vol"] or 0)
            sell_v = int(r["sell_vol"] or 0)
            delta = buy_v - sell_v
            cum += delta
            series.append({
                "time": r["bucket"].isoformat(),
                "buy_vol": buy_v, "sell_vol": sell_v,
                "delta": delta, "cumulative": cum,
                "color": "#26a69a" if delta >= 0 else "#ef5350",
            })

        last = series[-15:] if len(series) >= 15 else series
        return {
            "source": "ticks",
            "since_hours": since_hours,
            "buckets": len(series),
            "total_buy": sum(s["buy_vol"] for s in series),
            "total_sell": sum(s["sell_vol"] for s in series),
            "cumulative": cum,
            "last_15min_delta": sum(s["delta"] for s in last),
            "series": series,
        }
    except Exception:
        return None


def footprint(symbol: str, bar_seconds: int = 300, lookback_hours: int = 6,
              n_price_buckets: int = 25) -> Optional[dict]:
    try:
        conn = get_connection()
        rows = conn.execute(
            """SELECT FLOOR(EXTRACT(EPOCH FROM ts) / %s)::BIGINT AS bar_idx,
                      price, side, size
               FROM dse_ticks
               WHERE symbol = %s AND ts >= NOW() - %s::interval
               ORDER BY ts""",
            (bar_seconds, symbol.upper(), f"{lookback_hours} hours"),
        ).fetchall()
        conn.close()
        if not rows:
            return None

        prices = [float(r["price"]) for r in rows]
        if not prices:
            return None
        p_lo, p_hi = min(prices), max(prices)
        if p_hi <= p_lo:
            return None
        bin_w = (p_hi - p_lo) / n_price_buckets

        cells: dict = {}
        bar_meta: dict = {}
        for r in rows:
            bar_idx = int(r["bar_idx"])
            px = float(r["price"])
            side = r["side"]
            sz = int(r["size"])
            bucket = min(int((px - p_lo) / bin_w), n_price_buckets - 1)
            key = (bar_idx, bucket)
            cells.setdefault(key, {"buy": 0, "sell": 0})
            if side == "B": cells[key]["buy"] += sz
            elif side == "S": cells[key]["sell"] += sz
            bm = bar_meta.setdefault(bar_idx, {
                "open": (bar_idx * bar_seconds),
                "low": px, "high": px,
            })
            bm["low"] = min(bm["low"], px)
            bm["high"] = max(bm["high"], px)

        from datetime import datetime, timezone
        bars = []
        for bi in sorted(bar_meta):
            m = bar_meta[bi]
            cell_list = []
            tb = ts_ = 0
            for b in range(n_price_buckets):
                if (bi, b) in cells:
                    cc = cells[(bi, b)]
                    px = round(p_lo + (b + 0.5) * bin_w, 2)
                    cell_list.append({
                        "price": px,
                        "buy": cc["buy"], "sell": cc["sell"],
                        "delta": cc["buy"] - cc["sell"],
                        "total": cc["buy"] + cc["sell"],
                    })
                    tb += cc["buy"]; ts_ += cc["sell"]
            poc = max(cell_list, key=lambda c: c["total"])["price"] if cell_list else None
            bars.append({
                "open": datetime.fromtimestamp(m["open"], tz=timezone.utc).isoformat(),
                "low": round(m["low"], 2), "high": round(m["high"], 2),
                "buy": tb, "sell": ts_, "delta": tb - ts_, "poc": poc,
                "cells": cell_list,
            })

        return {
            "source": "ticks",
            "bar_seconds": bar_seconds,
            "lookback_hours": lookback_hours,
            "price_range": {"low": round(p_lo, 2), "high": round(p_hi, 2)},
            "bin_width": round(bin_w, 4),
            "bars": bars[-30:],
        }
    except Exception:
        return None


def get_tick_order_flow(symbol: str) -> Optional[dict]:
    if not has_ticks(symbol):
        return None
    return {
        "cumulative_delta": cumulative_delta(symbol, since_hours=24),
        "footprint": footprint(symbol, bar_seconds=300, lookback_hours=6),
    }
