"""DSE Trading Assistant - FastAPI Backend."""

import sys
import os
import logging

# Add backend directory to path
sys.path.insert(0, os.path.dirname(__file__))

from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from contextlib import asynccontextmanager
from database import init_database, get_connection
from jobs.scheduler import setup_scheduler, fetch_live_prices
from config import CORS_ORIGINS, API_PREFIX

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
)
logger = logging.getLogger(__name__)


def _normalize_hist_df(df):
    """Normalize a bdshare historical DataFrame for our schema."""
    import pandas as pd
    df = df.reset_index()
    # bdshare returns both 'ltp' and 'close' columns — drop 'close' first, then rename 'ltp' → 'close'
    if "ltp" in df.columns and "close" in df.columns:
        df = df.drop(columns=["close"])
    col_map = {"ltp": "close", "trade": "trade_count"}
    df = df.rename(columns={k: v for k, v in col_map.items() if k in df.columns})
    for col in ["open", "high", "low", "close", "volume", "value"]:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors="coerce")
    return df


async def _bulk_load_historical():
    """One-time bulk download of historical OHLCV data from bdshare."""
    from data.repository import bulk_insert_daily_prices
    from database import get_connection
    from datetime import datetime, timedelta
    import pandas as pd

    end_date = datetime.now().strftime("%Y-%m-%d")
    start_date = (datetime.now() - timedelta(days=120)).strftime("%Y-%m-%d")

    try:
        from bdshare import get_historical_data

        # Try batch download first (faster if DSE archive endpoint is up)
        logger.info(f"Attempting batch historical download {start_date} to {end_date}...")
        try:
            df_all = get_historical_data(start=start_date, end=end_date)
            if df_all is not None and not df_all.empty:
                df_all = _normalize_hist_df(df_all)
                inserted = bulk_insert_daily_prices(df_all)
                logger.info(f"Batch loaded {inserted} rows into daily_prices")
                return
        except Exception as e:
            logger.warning(f"Batch download failed: {e}, trying per-stock...")

        # Fallback: fetch top stocks individually
        conn = get_connection()
        rows = conn.execute(
            "SELECT symbol FROM live_prices ORDER BY value DESC LIMIT 100"
        ).fetchall()
        conn.close()

        if not rows:
            logger.warning("No live prices for per-stock historical fetch")
            return

        symbols = [r["symbol"] for r in rows]
        logger.info(f"Fetching history for {len(symbols)} stocks individually...")

        total = 0
        for i, sym in enumerate(symbols):
            try:
                df = get_historical_data(start=start_date, end=end_date, code=sym)
                if df is not None and not df.empty:
                    df = _normalize_hist_df(df)
                    total += bulk_insert_daily_prices(df)
            except Exception:
                pass
            if (i + 1) % 20 == 0:
                logger.info(f"  Progress: {i + 1}/{len(symbols)}, {total} rows")

        logger.info(f"Per-stock load complete: {total} rows")

    except Exception as e:
        logger.error(f"Bulk historical load failed: {e}")


@asynccontextmanager
async def lifespan(app: FastAPI):
    """Application startup and shutdown."""
    logger.info("Starting DSE Trading Assistant...")
    init_database()
    logger.info("Database initialized")

    # 1. Check if we need a bulk historical load (first run)
    from data.repository import get_daily_prices_count, load_signals_from_db

    count = get_daily_prices_count()
    if count == 0:
        logger.info("First run: bulk loading historical data...")
        await _bulk_load_historical()
    else:
        logger.info(f"Found {count} rows in daily_prices, skipping bulk load")

    # 2. Seed sector mapping if empty
    from data.sector_scraper import scrape_sector_mapping
    conn_check = get_connection()
    sector_count = conn_check.execute("SELECT COUNT(*) FROM fundamentals WHERE sector IS NOT NULL").fetchone()[0]
    conn_check.close()
    if sector_count == 0:
        logger.info("Seeding sector mapping from DSE...")
        try:
            scrape_sector_mapping()
        except Exception as e:
            logger.warning(f"Sector seeding failed (non-critical): {e}")
    else:
        logger.info(f"Found {sector_count} stocks with sector data")

    # 3. Load signals from DB into cache (instant startup)
    from data.cache import cache
    from config import CACHE_TTL_SIGNALS

    db_signals = load_signals_from_db()
    if db_signals:
        cache.set("all_signals", db_signals, CACHE_TTL_SIGNALS * 2)
        logger.info(f"Loaded {len(db_signals)} signals from DB into cache")

    # 4. Start scheduler
    scheduler = setup_scheduler()
    scheduler.start()
    logger.info("Background scheduler started")

    # 5. Fetch initial live data
    logger.info("Fetching initial market data...")
    await fetch_live_prices()

    # 6. Start background signal recomputation (non-blocking)
    from api.routes_signals import start_background_computation

    logger.info("Starting background signal recomputation...")
    start_background_computation()

    yield

    scheduler.shutdown()
    logger.info("Scheduler shut down")


app = FastAPI(
    title="DSE Trading Assistant",
    description="Bangladesh Stock Market analysis and trading signals",
    version="1.0.0",
    lifespan=lifespan,
)

# CORS for frontend
app.add_middleware(
    CORSMiddleware,
    allow_origins=CORS_ORIGINS,
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# Import and mount routers
from api.routes_market import router as market_router
from api.routes_stock import router as stock_router
from api.routes_signals import router as signals_router
from api.routes_screener import router as screener_router
from api.routes_watchlist import router as watchlist_router
from api.routes_portfolio import router as portfolio_router

app.include_router(market_router, prefix=f"{API_PREFIX}/market", tags=["Market"])
app.include_router(stock_router, prefix=f"{API_PREFIX}/stock", tags=["Stock"])
app.include_router(signals_router, prefix=f"{API_PREFIX}/signals", tags=["Signals"])
app.include_router(screener_router, prefix=f"{API_PREFIX}/screener", tags=["Screener"])
app.include_router(watchlist_router, prefix=f"{API_PREFIX}/watchlist", tags=["Watchlist"])
app.include_router(portfolio_router, prefix=f"{API_PREFIX}/portfolio", tags=["Portfolio"])


@app.get("/")
async def root():
    return {
        "name": "DSE Trading Assistant",
        "version": "1.0.0",
        "docs": "/docs",
        "status": "running",
    }


@app.get("/health")
async def health():
    return {"status": "healthy"}
