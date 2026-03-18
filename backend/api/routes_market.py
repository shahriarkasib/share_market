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

    # DB first (fast, synced every 5 min by scheduler)
    summary = None
    conn = get_connection()
    row = conn.execute("SELECT * FROM market_summary WHERE id = 1").fetchone()
    conn.close()
    if row:
        summary = dict(row)

    # Fallback to live scrape if DB empty
    if not summary or not summary.get("dsex_index"):
        try:
            summary = fetcher.get_market_summary()
        except Exception as e:
            import logging
            logging.getLogger(__name__).warning(f"Fetcher market summary failed: {e}")

    # Last resort: empty defaults
    if not summary or not summary.get("dsex_index"):
        summary = summary or {
            "dsex_index": 0, "dsex_change": 0, "dsex_change_pct": 0,
            "total_volume": 0, "total_value": 0, "total_trade": 0,
            "advances": 0, "declines": 0, "unchanged": 0,
            "market_status": "CLOSED",
        }

    summary["last_updated"] = str(summary.pop("updated_at", None) or datetime.now())
    summary.pop("id", None)
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


@router.get("/matrix")
async def get_matrix_data():
    """Enriched stock data for the Matrix page — prices + analysis + AI signals."""
    cached = cache.get("matrix_data")
    if cached:
        return cached

    conn = get_connection()

    # Live prices
    price_rows = conn.execute(
        "SELECT symbol, ltp, change, change_pct, open, high, low, close_prev, "
        "volume, value, trade_count, bid_ask_ratio FROM live_prices WHERE ltp > 0"
    ).fetchall()
    prices = {r["symbol"]: dict(r) for r in price_rows}

    # Fundamentals (sector, category)
    fund_rows = conn.execute(
        "SELECT symbol, sector, category FROM fundamentals"
    ).fetchall()
    fund_map = {r["symbol"]: dict(r) for r in fund_rows}

    # Latest daily analysis
    latest_row = conn.execute("SELECT MAX(date) FROM daily_analysis").fetchone()
    latest_date = str(latest_row[0]) if latest_row and latest_row[0] else None

    analysis_map = {}
    if latest_date:
        a_rows = conn.execute(
            "SELECT symbol, action, score, rsi, stoch_rsi, macd_status, "
            "entry_low, entry_high, sl, t1, t2, risk_pct, reward_pct, "
            "bb_pct, vol_ratio, support, resistance "
            "FROM daily_analysis WHERE date = ?", (latest_date,)
        ).fetchall()
        analysis_map = {r["symbol"]: dict(r) for r in a_rows}

    # LLM + Judge analysis
    llm_map = {}
    judge_map = {}
    if latest_date:
        llm_date_row = conn.execute("SELECT MAX(date) FROM llm_daily_analysis").fetchone()
        ai_date = str(llm_date_row[0]) if llm_date_row and llm_date_row[0] else latest_date

        llm_rows = conn.execute(
            "SELECT symbol, action, confidence, stage, entry_direction, conviction "
            "FROM llm_daily_analysis WHERE date = ?", (ai_date,)
        ).fetchall()
        llm_map = {r["symbol"]: dict(r) for r in llm_rows}

        judge_rows = conn.execute(
            "SELECT symbol, final_action, final_confidence, agreement, score "
            "FROM judge_daily_analysis WHERE date = ?", (ai_date,)
        ).fetchall()
        judge_map = {r["symbol"]: dict(r) for r in judge_rows}

    conn.close()

    # Build enriched result
    result = []
    for sym, p in prices.items():
        fund = fund_map.get(sym, {})
        a = analysis_map.get(sym, {})
        llm = llm_map.get(sym, {})
        judge = judge_map.get(sym, {})

        ai_action = judge.get("final_action") or llm.get("action") or ""
        ai_confidence = judge.get("final_confidence") or llm.get("confidence") or ""
        ai_score = float(judge.get("score") or llm.get("score") or a.get("score") or 0)

        # Composite ranking score: higher = better buy opportunity
        # Weights: AI score (40%), RSI inversed (20%), risk-reward (20%), change momentum (20%)
        rsi_val = float(a.get("rsi") or 50)
        risk_pct = abs(float(a.get("risk_pct") or 0))
        reward_pct = float(a.get("reward_pct") or 0)
        rr = reward_pct / risk_pct if risk_pct > 0 else 0

        # Normalize components (0-100 scale)
        score_norm = max(0, min(100, ai_score))  # already 0-100
        rsi_norm = max(0, min(100, 100 - rsi_val))  # lower RSI = better for buying
        rr_norm = max(0, min(100, rr * 25))  # R:R of 4 = 100
        # Prefer negative change (dip buying) but not too negative
        chg = float(p.get("change_pct") or 0)
        momentum_norm = max(0, min(100, 50 - chg * 10))  # -5% change = 100, +5% = 0

        composite = (score_norm * 0.4 + rsi_norm * 0.2 + rr_norm * 0.2 + momentum_norm * 0.2)

        result.append({
            **p,
            "sector": fund.get("sector") or "",
            "category": fund.get("category") or "",
            "algo_action": a.get("action") or "",
            "ai_action": ai_action,
            "ai_confidence": ai_confidence,
            "score": ai_score,
            "rsi": float(a.get("rsi") or 0),
            "stoch_rsi": float(a.get("stoch_rsi") or 0),
            "macd_status": a.get("macd_status") or "",
            "bb_pct": float(a.get("bb_pct") or 0),
            "vol_ratio": float(a.get("vol_ratio") or 0),
            "entry_low": a.get("entry_low"),
            "entry_high": a.get("entry_high"),
            "sl": a.get("sl"),
            "t1": a.get("t1"),
            "t2": a.get("t2"),
            "risk_pct": float(a.get("risk_pct") or 0),
            "reward_pct": reward_pct,
            "support": float(a.get("support") or 0),
            "resistance": float(a.get("resistance") or 0),
            "entry_direction": llm.get("entry_direction") or "",
            "conviction": llm.get("conviction") or "",
            "stage": llm.get("stage") or "",
            "composite_score": round(composite, 1),
            "bid_ask_ratio": float(p.get("bid_ask_ratio") or 0),
        })

    cache.set("matrix_data", result, 300)  # 5 min cache
    return result


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

    cache.set("dsex_history", result, 600)
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
