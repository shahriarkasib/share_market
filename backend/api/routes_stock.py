"""Individual stock API routes."""

import math
from datetime import datetime
from fastapi import APIRouter, HTTPException, Query
from data.fetcher import DSEDataFetcher
from data.cache import cache
from data.repository import read_historical_for_symbol
from database import get_connection
from api.schemas import StockPriceResponse, OHLCVResponse
from config import CACHE_TTL_LIVE_PRICES, CACHE_TTL_HISTORICAL
import pandas as pd
import pytz


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


@router.get("/{symbol}")
async def get_stock_price(symbol: str):
    """Get live price for a specific stock."""
    symbol = symbol.upper()

    cached = cache.get(f"stock_{symbol}")
    if cached:
        return cached

    # Read from DB first (fast)
    conn = get_connection()
    row = conn.execute(
        "SELECT * FROM live_prices WHERE symbol = ?", (symbol,)
    ).fetchone()
    conn.close()

    if row:
        result = dict(row)
        result = {k: (None if isinstance(v, float) and pd.isna(v) else v) for k, v in result.items()}
        cache.set(f"stock_{symbol}", result, CACHE_TTL_LIVE_PRICES)
        return result

    # Fallback to live fetch
    df = fetcher.get_live_prices()
    if df.empty:
        raise HTTPException(status_code=404, detail="No market data available")

    stock = df[df["symbol"] == symbol]
    if stock.empty:
        raise HTTPException(status_code=404, detail=f"Stock {symbol} not found")

    result = stock.iloc[0].to_dict()
    result = {k: (None if pd.isna(v) else v) for k, v in result.items()}

    cache.set(f"stock_{symbol}", result, CACHE_TTL_LIVE_PRICES)
    return result


@router.get("/{symbol}/history")
async def get_stock_history(symbol: str, period: str = "3m"):
    """Get historical OHLCV data for charting.
    Fetches from DB first; if not enough bars, fetches from bdshare and caches."""
    symbol = symbol.upper()

    cache_key = f"history_{symbol}_{period}"
    cached = cache.get(cache_key)
    if cached:
        return cached

    period_days = {
        "1w": 7, "2w": 14, "1m": 30, "3m": 90,
        "6m": 180, "1y": 365, "2y": 730, "3y": 1095,
    }
    days = period_days.get(period, 90)

    # Read from local DB first (fast)
    df = read_historical_for_symbol(symbol, min_rows=days)

    # If DB has fewer rows than requested, try fetching more from bdshare
    if len(df) < days * 0.6:  # Allow 60% tolerance (weekends/holidays)
        df_remote = _fetch_and_store_history(symbol, days)
        if not df_remote.empty and len(df_remote) > len(df):
            df = df_remote

    if df.empty:
        return []

    # Ensure required columns exist
    required = ["date", "open", "high", "low", "close", "volume"]
    for col in required:
        if col not in df.columns:
            df[col] = 0

    df["date"] = pd.to_datetime(df["date"]).dt.strftime("%Y-%m-%d")
    result = df[required].to_dict("records")

    ttl = CACHE_TTL_LIVE_PRICES if days <= 30 else CACHE_TTL_HISTORICAL
    cache.set(cache_key, result, ttl)
    return result


def _fetch_and_store_history(symbol: str, days: int) -> pd.DataFrame:
    """Fetch longer history from bdshare and store in DB."""
    from datetime import datetime, timedelta
    from data.repository import bulk_insert_daily_prices
    import logging
    logger = logging.getLogger(__name__)

    try:
        from bdshare import get_historical_data
        end_date = datetime.now().strftime("%Y-%m-%d")
        start_date = (datetime.now() - timedelta(days=days + 30)).strftime("%Y-%m-%d")

        logger.info(f"Fetching extended history for {symbol}: {start_date} to {end_date}")
        df = get_historical_data(start=start_date, end=end_date, code=symbol)
        if df is None or df.empty:
            return pd.DataFrame()

        # Normalize: bdshare returns index=date, columns include 'ltp', 'close', etc.
        df = df.reset_index()
        if "ltp" in df.columns and "close" in df.columns:
            df = df.drop(columns=["close"])
        col_map = {"ltp": "close", "trade": "trade_count"}
        df = df.rename(columns={k: v for k, v in col_map.items() if k in df.columns})
        for col in ["open", "high", "low", "close", "volume", "value"]:
            if col in df.columns:
                df[col] = pd.to_numeric(df[col], errors="coerce")

        # Store in DB (upsert handles duplicates)
        if not df.empty:
            inserted = bulk_insert_daily_prices(df)
            logger.info(f"Stored {inserted} rows for {symbol}")

        # Re-read from DB to get clean sorted data
        return read_historical_for_symbol(symbol, min_rows=days)

    except Exception as e:
        logger.error(f"Extended history fetch failed for {symbol}: {e}")
        return pd.DataFrame()


@router.get("/{symbol}/indicators")
async def get_stock_indicators(symbol: str):
    """Get computed technical indicators for a stock."""
    symbol = symbol.upper()

    cached = cache.get(f"indicators_{symbol}")
    if cached:
        return cached

    # Read from local DB (fast, no HTTP)
    df = read_historical_for_symbol(symbol, min_rows=120)
    if df.empty or len(df) < 20:
        raise HTTPException(status_code=404, detail=f"Insufficient data for {symbol}")

    from analysis.indicators import TechnicalIndicators
    ti = TechnicalIndicators(df)
    indicators = ti.compute_all()

    latest = indicators.iloc[-1]
    result = {}
    for col in indicators.columns:
        val = latest[col]
        if pd.notna(val):
            result[col] = float(val) if isinstance(val, (int, float)) else str(val)

    cache.set(f"indicators_{symbol}", result, 300)
    return result


@router.get("/{symbol}/intraday")
async def get_intraday_snapshots(
    symbol: str,
    date: str = Query(default=None, description="Date in YYYY-MM-DD format (default: today)"),
):
    """Get 5-minute intraday snapshots for a stock on a given day."""
    symbol = symbol.upper()
    dse_tz = pytz.timezone("Asia/Dhaka")

    if date is None:
        date = datetime.now(dse_tz).strftime("%Y-%m-%d")

    conn = get_connection()
    rows = conn.execute(
        """SELECT ts, ltp, open, high, low, volume, value, trade_count
           FROM intraday_snapshots
           WHERE symbol = ? AND ts::date = ?::date
           ORDER BY ts""",
        (symbol, date),
    ).fetchall()
    conn.close()

    if not rows:
        return []

    return [dict(r) for r in rows]


@router.get("/{symbol}/peers")
async def get_stock_peers(symbol: str, limit: int = 8):
    """Get peer stocks from the same sector."""
    symbol = symbol.upper()

    conn = get_connection()
    # Find the sector for this symbol
    row = conn.execute(
        "SELECT sector FROM fundamentals WHERE symbol = ?", (symbol,)
    ).fetchone()
    if not row or not row["sector"]:
        conn.close()
        return {"sector": None, "peers": []}

    sector = row["sector"]
    # Get other stocks in the same sector
    peers = conn.execute("""
        SELECT lp.symbol, lp.ltp, lp.change_pct, lp.volume, lp.value,
               f.company_name
        FROM live_prices lp
        JOIN fundamentals f ON lp.symbol = f.symbol
        WHERE f.sector = ? AND lp.symbol != ? AND lp.ltp > 0
        ORDER BY lp.value DESC
        LIMIT ?
    """, (sector, symbol, limit)).fetchall()
    conn.close()

    result = [dict(p) for p in peers]
    return {"sector": sector, "peers": _clean_nan(result)}
