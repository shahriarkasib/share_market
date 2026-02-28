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


def _seed_sectors_from_json():
    """Load sector mapping from bundled static JSON file."""
    import json
    json_path = os.path.join(os.path.dirname(__file__), "data", "dse_sectors.json")
    try:
        with open(json_path) as f:
            sectors = json.load(f)
        conn = get_connection()
        total = 0
        for sector_name, symbols in sectors.items():
            conn.execute(
                "INSERT INTO sectors (name, stock_count, updated_at) VALUES (?, ?, CURRENT_TIMESTAMP) ON CONFLICT (name) DO UPDATE SET stock_count = EXCLUDED.stock_count, updated_at = CURRENT_TIMESTAMP",
                (sector_name, len(symbols)),
            )
            for sym in symbols:
                conn.execute(
                    """INSERT INTO fundamentals (symbol, sector, updated_at)
                       VALUES (?, ?, CURRENT_TIMESTAMP)
                       ON CONFLICT(symbol) DO UPDATE SET sector = excluded.sector, updated_at = CURRENT_TIMESTAMP""",
                    (sym, sector_name),
                )
                total += 1
        conn.commit()
        conn.close()
        logger.info(f"Seeded {total} stocks across {len(sectors)} sectors from JSON")
    except Exception as e:
        logger.error(f"Static sector seeding failed: {e}")


def _seed_dsex_history():
    """Seed DSEX index history from bdshare market_summary (works from any region)."""
    try:
        from bdshare import market_summary
        from datetime import datetime
        import warnings
        import pandas as pd

        with warnings.catch_warnings():
            warnings.simplefilter("ignore")
            data = market_summary()

        if data is None or (isinstance(data, pd.DataFrame) and data.empty):
            return

        conn = get_connection()
        count = 0
        for _, row in data.iterrows():
            date_str = row.get("Date", "")
            if not date_str:
                continue
            try:
                dt = datetime.strptime(date_str, "%d-%m-%Y")
                iso_date = dt.strftime("%Y-%m-%d")
            except (ValueError, TypeError):
                continue
            dsex = float(row.get("DSEX Index", 0) or 0)
            if dsex > 0:
                conn.execute(
                    """INSERT INTO dsex_history
                       (date, dsex_index, dses_index, ds30_index, total_volume, total_value, total_trade)
                       VALUES (?, ?, ?, ?, ?, ?, ?)
                       ON CONFLICT (date) DO UPDATE SET
                         dsex_index = EXCLUDED.dsex_index, dses_index = EXCLUDED.dses_index,
                         ds30_index = EXCLUDED.ds30_index, total_volume = EXCLUDED.total_volume,
                         total_value = EXCLUDED.total_value, total_trade = EXCLUDED.total_trade""",
                    (iso_date, dsex,
                     float(row.get("DSES Index", 0) or 0),
                     float(row.get("DS30 Index", 0) or 0),
                     int(row.get("Total Volume", 0) or 0),
                     float(row.get("Total Value (mn)", 0) or 0),
                     int(row.get("Total Trade", 0) or 0)),
                )
                count += 1
        conn.commit()
        conn.close()
        logger.info(f"Seeded {count} DSEX history rows from bdshare")
    except Exception as e:
        logger.error(f"DSEX history seeding failed: {e}")


def _run_background_init():
    """Run heavy initialization tasks in a background thread."""
    import threading
    import time

    def _init():
        try:
            from datetime import datetime
            # 0. Fetch live prices (don't block startup)
            import asyncio
            logger.info("Background: fetching live prices...")
            try:
                loop = asyncio.new_event_loop()
                loop.run_until_complete(fetch_live_prices())
                loop.close()
            except Exception as e:
                logger.warning(f"Background live price fetch failed: {e}")

            from data.repository import get_daily_prices_count
            count = get_daily_prices_count()

            # 1. Bulk historical load if needed
            if count == 0:
                logger.info("Background: bulk loading historical data...")
                import asyncio
                loop = asyncio.new_event_loop()
                loop.run_until_complete(_bulk_load_historical())
                loop.close()
            else:
                logger.info(f"Background: found {count} rows in daily_prices")

            # 2. Seed sector mapping if empty — use static JSON (DSE scraper unreliable)
            conn_check = get_connection()
            sector_count = conn_check.execute(
                "SELECT COUNT(*) FROM fundamentals WHERE sector IS NOT NULL"
            ).fetchone()[0]
            conn_check.close()
            if sector_count == 0:
                logger.info("Background: seeding sectors from static JSON...")
                _seed_sectors_from_json()

            # 2b. Seed DSEX history if empty
            conn_dsex = get_connection()
            dsex_count = conn_dsex.execute("SELECT COUNT(*) FROM dsex_history").fetchone()[0]
            conn_dsex.close()
            if dsex_count == 0:
                logger.info("Background: seeding DSEX history from bdshare...")
                _seed_dsex_history()

            # 2c. Scrape DSE categories if mostly missing
            from data.repository import get_category_count, save_stock_categories
            cat_count = get_category_count()
            if cat_count < 50:
                logger.info(f"Background: scraping DSE categories ({cat_count} existing)...")
                try:
                    conn_sym = get_connection()
                    all_syms = [r["symbol"] for r in conn_sym.execute(
                        "SELECT symbol FROM live_prices"
                    ).fetchall()]
                    conn_sym.close()
                    if all_syms:
                        from data.fetcher import DSEDataFetcher
                        cats = DSEDataFetcher.scrape_all_categories(all_syms)
                        if cats:
                            save_stock_categories(cats)
                            logger.info(f"Scraped {len(cats)} categories from DSE")
                except Exception as e:
                    logger.error(f"Category scraping failed: {e}")

            # 3. Compute signals only if missing or stale (>4 hours old)
            from data.cache import cache
            from data.repository import load_signals_from_db
            all_signals = cache.get("all_signals")
            if not all_signals:
                # Cache miss — try loading from DB
                db_sigs = load_signals_from_db()
                if db_sigs:
                    from config import CACHE_TTL_SIGNALS
                    cache.set("all_signals", db_sigs, CACHE_TTL_SIGNALS * 2)
                    all_signals = db_sigs
                    logger.info(f"Background: loaded {len(db_sigs)} signals from DB into cache")

            # Check staleness — only recompute if signals are missing or >4 hours old
            should_compute = False
            if not all_signals:
                should_compute = True
                logger.info("Background: no signals found, will compute")
            else:
                # Check age of signals in DB
                conn_sig = get_connection()
                age_row = conn_sig.execute(
                    "SELECT created_at FROM signals ORDER BY created_at DESC LIMIT 1"
                ).fetchone()
                conn_sig.close()
                if age_row and age_row["created_at"]:
                    try:
                        created = datetime.fromisoformat(age_row["created_at"])
                        age_hours = (datetime.now() - created).total_seconds() / 3600
                        if age_hours > 4:
                            should_compute = True
                            logger.info(f"Background: signals are {age_hours:.1f}h old, will recompute")
                        else:
                            logger.info(f"Background: signals are {age_hours:.1f}h old, skipping recompute")
                    except (ValueError, TypeError):
                        should_compute = True

            if should_compute:
                logger.info("Background: starting signal computation...")
                from api.routes_signals import start_background_computation
                start_background_computation()

            logger.info("Background initialization complete")
        except Exception as e:
            logger.error(f"Background init error: {e}", exc_info=True)

    t = threading.Thread(target=_init, daemon=True, name="bg-init")
    t.start()
    return t


@asynccontextmanager
async def lifespan(app: FastAPI):
    """Application startup and shutdown."""
    logger.info("Starting DSE Trading Assistant...")
    init_database()
    logger.info("Database initialized")

    # Load cached signals from DB (instant)
    from data.repository import load_signals_from_db
    from data.cache import cache
    from config import CACHE_TTL_SIGNALS

    db_signals = load_signals_from_db()
    if db_signals:
        cache.set("all_signals", db_signals, CACHE_TTL_SIGNALS * 2)
        logger.info(f"Loaded {len(db_signals)} signals from DB into cache")

    # Start scheduler
    scheduler = setup_scheduler()
    scheduler.start()
    logger.info("Background scheduler started")

    # Run ALL data fetching in background so server starts instantly
    # (live prices, historical load, sectors, signals)
    _run_background_init()

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
from api.routes_analysis import router as analysis_router

app.include_router(market_router, prefix=f"{API_PREFIX}/market", tags=["Market"])
app.include_router(stock_router, prefix=f"{API_PREFIX}/stock", tags=["Stock"])
app.include_router(signals_router, prefix=f"{API_PREFIX}/signals", tags=["Signals"])
app.include_router(screener_router, prefix=f"{API_PREFIX}/screener", tags=["Screener"])
app.include_router(watchlist_router, prefix=f"{API_PREFIX}/watchlist", tags=["Watchlist"])
app.include_router(portfolio_router, prefix=f"{API_PREFIX}/portfolio", tags=["Portfolio"])
app.include_router(analysis_router, prefix=f"{API_PREFIX}/analysis", tags=["Analysis"])


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
    from data.repository import get_daily_prices_count
    conn = get_connection()
    sector_count = conn.execute(
        "SELECT COUNT(*) FROM fundamentals WHERE sector IS NOT NULL"
    ).fetchone()[0]
    live_count = conn.execute("SELECT COUNT(*) FROM live_prices").fetchone()[0]
    conn.close()
    return {
        "status": "healthy",
        "daily_prices": get_daily_prices_count(),
        "live_prices": live_count,
        "sectors": sector_count,
    }


@app.post("/api/v1/admin/init")
async def trigger_init():
    """Manually trigger background data initialization."""
    _run_background_init()
    return {"status": "initialization started in background"}


@app.post("/api/v1/admin/seed-prices")
async def seed_prices(payload: dict):
    """Accept bulk daily_prices data as JSON and insert into DB.

    Expected payload: {"rows": [{"symbol":..., "date":..., "open":..., ...}, ...]}
    """
    from data.repository import bulk_insert_daily_prices
    import pandas as pd

    rows = payload.get("rows", [])
    if not rows:
        return {"status": "no data", "inserted": 0}

    df = pd.DataFrame(rows)
    inserted = bulk_insert_daily_prices(df)
    logger.info(f"Seeded {inserted} daily_prices rows via admin endpoint")
    return {"status": "ok", "inserted": inserted}


@app.post("/api/v1/admin/seed-sectors")
async def seed_sectors(payload: dict):
    """Accept sector mapping data and insert into DB.

    Expected payload: {"sectors": {"Bank": [{"symbol": "...", "company_name": "..."}, ...], ...}}
    """
    conn = get_connection()
    total = 0
    for sector_name, stocks in payload.get("sectors", {}).items():
        conn.execute(
            "INSERT INTO sectors (name, stock_count, updated_at) VALUES (?, ?, CURRENT_TIMESTAMP) ON CONFLICT (name) DO UPDATE SET stock_count = EXCLUDED.stock_count, updated_at = CURRENT_TIMESTAMP",
            (sector_name, len(stocks)),
        )
        for s in stocks:
            symbol = s if isinstance(s, str) else s.get("symbol", "")
            company_name = "" if isinstance(s, str) else s.get("company_name", "")
            conn.execute(
                """INSERT INTO fundamentals (symbol, company_name, sector, updated_at)
                   VALUES (?, ?, ?, CURRENT_TIMESTAMP)
                   ON CONFLICT(symbol) DO UPDATE SET
                     sector = excluded.sector,
                     company_name = COALESCE(NULLIF(excluded.company_name, ''), fundamentals.company_name),
                     updated_at = CURRENT_TIMESTAMP""",
                (symbol, company_name, sector_name),
            )
            total += 1
    conn.commit()
    conn.close()
    logger.info(f"Seeded {total} stocks across {len(payload.get('sectors', {}))} sectors")
    return {"status": "ok", "stocks": total, "sectors": len(payload.get("sectors", {}))}


@app.post("/api/v1/admin/seed-dsex")
async def seed_dsex(payload: dict):
    """Accept DSEX index history data and insert into DB.

    Expected payload: {"rows": [{"date":..., "dsex_index":..., ...}, ...]}
    """
    rows = payload.get("rows", [])
    if not rows:
        return {"status": "no data", "inserted": 0}

    conn = get_connection()
    inserted = 0
    for r in rows:
        try:
            conn.execute(
                """INSERT INTO dsex_history
                   (date, dsex_index, dses_index, ds30_index, total_volume, total_value, total_trade)
                   VALUES (?, ?, ?, ?, ?, ?, ?)
                   ON CONFLICT (date) DO UPDATE SET
                     dsex_index = EXCLUDED.dsex_index, dses_index = EXCLUDED.dses_index,
                     ds30_index = EXCLUDED.ds30_index, total_volume = EXCLUDED.total_volume,
                     total_value = EXCLUDED.total_value, total_trade = EXCLUDED.total_trade""",
                (
                    r.get("date", ""),
                    float(r.get("dsex_index", 0) or 0),
                    float(r.get("dses_index", 0) or 0),
                    float(r.get("ds30_index", 0) or 0),
                    int(r.get("total_volume", 0) or 0),
                    float(r.get("total_value", 0) or 0),
                    int(r.get("total_trade", 0) or 0),
                ),
            )
            inserted += 1
        except Exception as e:
            logger.error(f"DSEX seed error: {e}")
    conn.commit()
    conn.close()
    logger.info(f"Seeded {inserted} dsex_history rows")
    return {"status": "ok", "inserted": inserted}
