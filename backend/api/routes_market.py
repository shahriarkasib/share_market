"""Market overview API routes."""

import math
from fastapi import APIRouter
from data.fetcher import DSEDataFetcher
from data.cache import cache
from database import get_connection
from api.schemas import MarketSummaryResponse, StockPriceResponse
from config import CACHE_TTL_LIVE_PRICES
from datetime import datetime


def _clean_nan(records: list) -> list:
    """Replace NaN/inf values with None for JSON serialization."""
    cleaned = []
    for rec in records:
        cleaned.append(
            {
                k: (
                    None
                    if isinstance(v, float) and (math.isnan(v) or math.isinf(v))
                    else v
                )
                for k, v in rec.items()
            }
        )
    return cleaned


router = APIRouter()
fetcher = DSEDataFetcher()


@router.get("/summary", response_model=MarketSummaryResponse)
async def get_market_summary():
    """Get DSEX index and market statistics."""
    cached = cache.get("market_summary")
    if cached:
        return cached

    conn = get_connection()

    # Try market_summary table first (written by scheduler)
    row = conn.execute("SELECT * FROM market_summary WHERE id = 1").fetchone()
    summary = dict(row) if row else None

    # If missing or DSEX is zero, enrich from dsex_history and live_prices
    if not summary or not summary.get("dsex_index"):
        summary = summary or {
            "dsex_index": 0, "dsex_change": 0, "dsex_change_pct": 0,
            "total_volume": 0, "total_value": 0, "total_trade": 0,
            "advances": 0, "declines": 0, "unchanged": 0,
            "market_status": "CLOSED",
        }
        # Fill DSEX from dsex_history
        hist = conn.execute(
            "SELECT * FROM dsex_history ORDER BY date DESC LIMIT 2"
        ).fetchall()
        if hist:
            latest = dict(hist[0])
            summary["dsex_index"] = latest.get("dsex_index", 0)
            summary["total_volume"] = latest.get("total_volume", 0)
            summary["total_value"] = latest.get("total_value", 0)
            summary["total_trade"] = latest.get("total_trade", 0)
            if len(hist) > 1:
                prev = dict(hist[1])
                prev_dsex = prev.get("dsex_index", 0)
                if prev_dsex > 0:
                    change = summary["dsex_index"] - prev_dsex
                    summary["dsex_change"] = round(change, 2)
                    summary["dsex_change_pct"] = round(change / prev_dsex * 100, 2)

        # Fill advances/declines from live_prices
        adv = conn.execute("SELECT COUNT(*) FROM live_prices WHERE change_pct > 0").fetchone()[0]
        dec = conn.execute("SELECT COUNT(*) FROM live_prices WHERE change_pct < 0").fetchone()[0]
        unch = conn.execute("SELECT COUNT(*) FROM live_prices WHERE change_pct = 0 AND trade_count > 0").fetchone()[0]
        summary["advances"] = adv
        summary["declines"] = dec
        summary["unchanged"] = unch

        # Determine market status from time
        import pytz
        from datetime import time as dtime
        dse_tz = pytz.timezone("Asia/Dhaka")
        now = datetime.now(dse_tz)
        market_days = [6, 0, 1, 2, 3]  # Sun-Thu
        if now.weekday() in market_days and dtime(10, 0) <= now.time() <= dtime(14, 30):
            summary["market_status"] = "OPEN"
        else:
            summary["market_status"] = "CLOSED"

    # Always enrich from live_prices aggregates (bdshare summary can be stale)
    live_agg = conn.execute(
        "SELECT COALESCE(SUM(volume),0) as vol, COALESCE(SUM(value),0) as val, "
        "COALESCE(SUM(trade_count),0) as trades, "
        "SUM(CASE WHEN change_pct > 0 THEN 1 ELSE 0 END) as adv, "
        "SUM(CASE WHEN change_pct < 0 THEN 1 ELSE 0 END) as dec, "
        "SUM(CASE WHEN change_pct = 0 AND trade_count > 0 THEN 1 ELSE 0 END) as unch "
        "FROM live_prices WHERE ltp > 0"
    ).fetchone()
    if live_agg:
        live_vol = int(live_agg["vol"])
        live_val = float(live_agg["val"])
        live_trades = int(live_agg["trades"])
        # Prefer live totals when DB value is zero or live is larger
        if live_vol > 0 and (not summary.get("total_volume") or live_vol > summary["total_volume"]):
            summary["total_volume"] = live_vol
        if live_val > 0 and (not summary.get("total_value") or summary["total_value"] == 0):
            summary["total_value"] = live_val
        if live_trades > 0 and (not summary.get("total_trade") or live_trades > summary["total_trade"]):
            summary["total_trade"] = live_trades
        # Always use live adv/dec/unch
        summary["advances"] = int(live_agg["adv"])
        summary["declines"] = int(live_agg["dec"])
        summary["unchanged"] = int(live_agg["unch"])

    conn.close()

    updated = summary.get("updated_at") or datetime.now()
    summary["last_updated"] = str(updated) if not isinstance(updated, str) else updated
    # Strip keys not in response model
    summary.pop("id", None)
    summary.pop("updated_at", None)
    cache.set("market_summary", summary, CACHE_TTL_LIVE_PRICES)
    return summary


@router.get("/movers")
async def get_top_movers(type: str = "gainers", limit: int = 20):
    """Get top gainers or losers."""
    cached = cache.get(f"movers_{type}_{limit}")
    if cached:
        return cached

    movers = fetcher.get_top_movers(limit)
    result = _clean_nan(movers.get(type, []))

    cache.set(f"movers_{type}_{limit}", result, CACHE_TTL_LIVE_PRICES)
    return result


@router.get("/all-prices")
async def get_all_prices(category: str = None):
    """Get live prices for all stocks. Optional ?category=A filter."""
    cache_key = f"all_prices_{category}" if category else "all_prices"
    cached = cache.get(cache_key)
    if cached:
        return cached

    conn = get_connection()
    if category:
        rows = conn.execute("""
            SELECT lp.*, f.category FROM live_prices lp
            LEFT JOIN fundamentals f ON lp.symbol = f.symbol
            WHERE f.category = ?
            ORDER BY lp.value DESC
        """, (category.upper(),)).fetchall()
    else:
        rows = conn.execute("SELECT * FROM live_prices").fetchall()
    conn.close()

    if rows:
        result = _clean_nan([dict(r) for r in rows])
        cache.set(cache_key, result, CACHE_TTL_LIVE_PRICES)
        return result

    if not category:
        df = fetcher.get_live_prices()
        if not df.empty:
            result = _clean_nan(df.to_dict("records"))
            cache.set(cache_key, result, CACHE_TTL_LIVE_PRICES)
            return result

    return []


@router.get("/dsex-chart")
async def get_dsex_chart():
    """Get DSEX index history formatted for charting."""
    cached = cache.get("dsex_chart")
    if cached:
        return cached

    conn = get_connection()
    rows = conn.execute(
        "SELECT date, dsex_index, total_volume, total_value FROM dsex_history ORDER BY date"
    ).fetchall()
    conn.close()

    result = [
        {
            "date": r["date"],
            "value": r["dsex_index"],
            "volume": r["total_volume"] or 0,
            "turnover": r["total_value"] or 0,
        }
        for r in rows if r["dsex_index"] and r["dsex_index"] > 0
    ]

    cache.set("dsex_chart", result, 3600)
    return result


@router.get("/index-history")
async def get_index_history():
    """Get DSEX index daily history for charting."""
    cached = cache.get("dsex_history")
    if cached:
        return cached

    conn = get_connection()
    rows = conn.execute(
        "SELECT date, dsex_index, total_volume, total_trade FROM dsex_history ORDER BY date"
    ).fetchall()
    conn.close()

    if not rows:
        # Seed from bdshare
        _seed_dsex_history()
        conn = get_connection()
        rows = conn.execute(
            "SELECT date, dsex_index, total_volume, total_trade FROM dsex_history ORDER BY date"
        ).fetchall()
        conn.close()

    result = [
        {
            "date": r["date"],
            "dsex": r["dsex_index"],
            "volume": r["total_volume"],
            "trade": r["total_trade"],
        }
        for r in rows
    ]

    cache.set("dsex_history", result, 3600)
    return result


def _seed_dsex_history():
    """Fetch DSEX history from bdshare and store in DB."""
    try:
        from bdshare import market_summary
        import warnings
        import pandas as pd

        with warnings.catch_warnings():
            warnings.simplefilter("ignore")
            data = market_summary()

        if data is None or (isinstance(data, pd.DataFrame) and data.empty):
            return

        conn = get_connection()
        for _, row in data.iterrows():
            date_str = row.get("Date", "")
            if not date_str:
                continue
            # Convert DD-MM-YYYY to YYYY-MM-DD
            try:
                dt = datetime.strptime(date_str, "%d-%m-%Y")
                iso_date = dt.strftime("%Y-%m-%d")
            except (ValueError, TypeError):
                continue

            dsex = float(row.get("DSEX Index", 0) or 0)
            dses = float(row.get("DSES Index", 0) or 0)
            ds30 = float(row.get("DS30 Index", 0) or 0)
            volume = int(row.get("Total Volume", 0) or 0)
            value = float(row.get("Total Value (mn)", 0) or 0)
            trade = int(row.get("Total Trade", 0) or 0)

            if dsex > 0:
                conn.execute(
                    """INSERT INTO dsex_history
                       (date, dsex_index, dses_index, ds30_index, total_volume, total_value, total_trade)
                       VALUES (?, ?, ?, ?, ?, ?, ?)
                       ON CONFLICT (date) DO UPDATE SET
                         dsex_index = EXCLUDED.dsex_index, dses_index = EXCLUDED.dses_index,
                         ds30_index = EXCLUDED.ds30_index, total_volume = EXCLUDED.total_volume,
                         total_value = EXCLUDED.total_value, total_trade = EXCLUDED.total_trade""",
                    (iso_date, dsex, dses, ds30, volume, value, trade),
                )

        conn.commit()
        conn.close()
    except Exception as e:
        import logging
        logging.getLogger(__name__).error(f"Failed to seed DSEX history: {e}")


@router.get("/sectors")
async def get_sector_performance():
    """Get sector-wise performance with aggregated metrics per sector."""
    cached = cache.get("sector_performance")
    if cached:
        return cached

    conn = get_connection()
    rows = conn.execute("""
        SELECT f.sector, lp.symbol, lp.ltp, lp.change_pct, lp.volume, lp.value, lp.trade_count
        FROM fundamentals f
        JOIN live_prices lp ON f.symbol = lp.symbol
        WHERE f.sector IS NOT NULL AND lp.ltp > 0
        ORDER BY f.sector, lp.value DESC
    """).fetchall()
    conn.close()

    if not rows:
        return []

    from collections import defaultdict
    sectors: dict = defaultdict(lambda: {
        "stocks": [], "advances": 0, "declines": 0, "unchanged": 0,
        "total_turnover": 0, "total_volume": 0, "total_trades": 0,
        "change_pcts": [],
    })

    for r in rows:
        s = sectors[r["sector"]]
        chg = r["change_pct"] or 0
        s["stocks"].append({"symbol": r["symbol"], "change_pct": chg, "ltp": r["ltp"]})
        s["change_pcts"].append(chg)
        s["total_turnover"] += r["value"] or 0
        s["total_volume"] += r["volume"] or 0
        s["total_trades"] += r["trade_count"] or 0
        if chg > 0:
            s["advances"] += 1
        elif chg < 0:
            s["declines"] += 1
        else:
            s["unchanged"] += 1

    result = []
    for sector_name, data in sorted(sectors.items()):
        pcts = data["change_pcts"]
        stocks = data["stocks"]
        avg_chg = sum(pcts) / len(pcts) if pcts else 0
        top_gainer = max(stocks, key=lambda x: x["change_pct"]) if stocks else None
        top_loser = min(stocks, key=lambda x: x["change_pct"]) if stocks else None
        result.append({
            "sector": sector_name,
            "stock_count": len(stocks),
            "advances": data["advances"],
            "declines": data["declines"],
            "unchanged": data["unchanged"],
            "avg_change_pct": round(avg_chg, 2),
            "total_turnover": data["total_turnover"],
            "total_volume": data["total_volume"],
            "total_trades": data["total_trades"],
            "top_gainer": {"symbol": top_gainer["symbol"], "change_pct": top_gainer["change_pct"]} if top_gainer else None,
            "top_loser": {"symbol": top_loser["symbol"], "change_pct": top_loser["change_pct"]} if top_loser else None,
        })

    result.sort(key=lambda x: x["total_turnover"], reverse=True)
    cache.set("sector_performance", result, CACHE_TTL_LIVE_PRICES)
    return result


@router.get("/sectors/{sector_name}")
async def get_sector_detail(sector_name: str):
    """Get all stocks in a specific sector with prices and signal info."""
    conn = get_connection()
    rows = conn.execute("""
        SELECT lp.*, f.sector, f.company_name as fname
        FROM live_prices lp
        JOIN fundamentals f ON lp.symbol = f.symbol
        WHERE f.sector = ?
        ORDER BY lp.value DESC
    """, (sector_name,)).fetchall()
    conn.close()

    if not rows:
        return {"sector": sector_name, "stocks": []}

    result = _clean_nan([dict(r) for r in rows])
    return {"sector": sector_name, "stocks": result}


@router.get("/most-active")
async def get_most_active(tab: str = "gainers", limit: int = 20):
    """Unified most-active endpoint: gainers, losers, volume, turnover."""
    cache_key = f"most_active_{tab}_{limit}"
    cached = cache.get(cache_key)
    if cached:
        return cached

    conn = get_connection()
    order_map = {
        "gainers": "lp.change_pct DESC",
        "losers": "lp.change_pct ASC",
        "volume": "lp.volume DESC",
        "turnover": "lp.value DESC",
    }
    order = order_map.get(tab, "lp.change_pct DESC")

    rows = conn.execute(f"""
        SELECT lp.*, f.sector, f.company_name as fname
        FROM live_prices lp
        LEFT JOIN fundamentals f ON lp.symbol = f.symbol
        WHERE lp.ltp > 0 AND lp.trade_count > 0
        ORDER BY {order}
        LIMIT ?
    """, (limit,)).fetchall()
    conn.close()

    result = _clean_nan([dict(r) for r in rows])
    cache.set(cache_key, result, CACHE_TTL_LIVE_PRICES)
    return result


@router.get("/heatmap")
async def get_heatmap_data(size_by: str = "turnover"):
    """Get hierarchical heatmap data grouped by sector."""
    cache_key = f"heatmap_{size_by}"
    cached = cache.get(cache_key)
    if cached:
        return cached

    size_col_map = {"turnover": "lp.value", "volume": "lp.volume", "trades": "lp.trade_count"}
    size_col = size_col_map.get(size_by, "lp.value")

    conn = get_connection()
    rows = conn.execute(f"""
        SELECT f.sector, lp.symbol, lp.ltp, lp.change_pct,
               {size_col} as size_value, lp.volume, lp.value, lp.trade_count
        FROM fundamentals f
        JOIN live_prices lp ON f.symbol = lp.symbol
        WHERE f.sector IS NOT NULL AND lp.ltp > 0 AND lp.trade_count > 0
        ORDER BY f.sector, {size_col} DESC
    """).fetchall()
    conn.close()

    from collections import defaultdict
    sector_groups: dict = defaultdict(lambda: {"stocks": [], "total_size": 0})

    for r in rows:
        g = sector_groups[r["sector"]]
        size_val = r["size_value"] or 0
        g["stocks"].append({
            "symbol": r["symbol"],
            "change_pct": r["change_pct"] or 0,
            "size_value": size_val,
            "ltp": r["ltp"],
            "volume": r["volume"],
        })
        g["total_size"] += size_val

    result = []
    for sector_name, data in sector_groups.items():
        if data["total_size"] > 0:
            pcts = [s["change_pct"] for s in data["stocks"]]
            result.append({
                "sector": sector_name,
                "stocks": data["stocks"],
                "total_size": data["total_size"],
                "avg_change_pct": round(sum(pcts) / len(pcts), 2) if pcts else 0,
            })

    result.sort(key=lambda x: x["total_size"], reverse=True)
    cache.set(cache_key, result, CACHE_TTL_LIVE_PRICES)
    return result
