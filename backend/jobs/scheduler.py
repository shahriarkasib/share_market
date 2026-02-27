"""Background job scheduler for periodic data fetching and signal computation."""

import logging
import threading
from apscheduler.schedulers.asyncio import AsyncIOScheduler
from apscheduler.triggers.cron import CronTrigger
from apscheduler.triggers.interval import IntervalTrigger
from data.fetcher import DSEDataFetcher
from data.cache import cache
from database import get_connection
from datetime import datetime
import pytz
import pandas as pd

logger = logging.getLogger(__name__)
fetcher = DSEDataFetcher()

DSE_TZ = pytz.timezone("Asia/Dhaka")


async def fetch_live_prices():
    """Fetch and cache live prices from DSE."""
    try:
        logger.info("Fetching live prices...")
        df = fetcher.get_live_prices()
        if df.empty:
            logger.warning("No live prices returned")
            return

        # Update live_prices table
        conn = get_connection()
        for _, row in df.iterrows():
            try:
                conn.execute(
                    """INSERT OR REPLACE INTO live_prices
                       (symbol, ltp, high, low, open, close_prev, change, change_pct,
                        volume, value, trade_count, updated_at)
                       VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)""",
                    (
                        row.get("symbol", ""),
                        row.get("ltp", 0),
                        row.get("high", 0),
                        row.get("low", 0),
                        row.get("open", 0),
                        row.get("close_prev", 0),
                        row.get("change", 0),
                        row.get("change_pct", 0),
                        int(row.get("volume", 0)),
                        row.get("value", 0),
                        int(row.get("trade_count", 0)),
                        datetime.now(DSE_TZ).isoformat(),
                    ),
                )
            except Exception as e:
                logger.error(f"Error saving price for {row.get('symbol')}: {e}")

        conn.commit()
        conn.close()

        # Clear price caches
        cache.delete("all_prices")
        cache.delete("market_summary")
        for key in list(cache._cache.keys()):
            if key.startswith("movers_") or key.startswith("stock_"):
                cache.delete(key)

        logger.info(f"Updated prices for {len(df)} stocks")

    except Exception as e:
        logger.error(f"Price fetch job failed: {e}")


async def sync_daily_prices_from_live():
    """Copy today's live prices into daily_prices table for historical record."""
    try:
        from data.repository import upsert_today_prices

        today_str = datetime.now(DSE_TZ).strftime("%Y-%m-%d")

        conn = get_connection()
        rows = conn.execute("SELECT * FROM live_prices").fetchall()
        conn.close()

        if not rows:
            return

        df = pd.DataFrame([dict(r) for r in rows])
        upsert_today_prices(df, today_str)
        logger.info(f"Synced {len(df)} live prices to daily_prices for {today_str}")
    except Exception as e:
        logger.error(f"Daily price sync failed: {e}")


async def compute_signals():
    """Recompute trading signals from local DB data."""
    try:
        logger.info("Triggering signal recomputation...")
        from api.routes_signals import _compute_all_signals_background

        thread = threading.Thread(
            target=_compute_all_signals_background, daemon=True
        )
        thread.start()
    except Exception as e:
        logger.error(f"Signal computation job failed: {e}")


async def sync_market_summary():
    """Sync market summary data. Never overwrite good data with zeroes."""
    try:
        summary = fetcher.get_market_summary()

        # Don't overwrite with zeroes — keep the last known good data
        if summary.get("dsex_index", 0) == 0:
            logger.info("Market summary returned zero DSEX, skipping DB write")
            cache.delete("market_summary")
            return

        conn = get_connection()
        conn.execute(
            """INSERT OR REPLACE INTO market_summary
               (id, dsex_index, dsex_change, dsex_change_pct, total_volume,
                total_value, total_trade, advances, declines, unchanged,
                market_status, updated_at)
               VALUES (1, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)""",
            (
                summary.get("dsex_index", 0),
                summary.get("dsex_change", 0),
                summary.get("dsex_change_pct", 0),
                summary.get("total_volume", 0),
                summary.get("total_value", 0),
                summary.get("total_trade", 0),
                summary.get("advances", 0),
                summary.get("declines", 0),
                summary.get("unchanged", 0),
                summary.get("market_status", "UNKNOWN"),
                datetime.now(DSE_TZ).isoformat(),
            ),
        )
        conn.commit()
        conn.close()
        cache.delete("market_summary")
    except Exception as e:
        logger.error(f"Market summary sync failed: {e}")


async def market_data_pipeline():
    """Full pipeline: fetch live → sync to daily → recompute signals → sync summary."""
    await fetch_live_prices()
    await sync_daily_prices_from_live()
    await compute_signals()
    await sync_market_summary()


def setup_scheduler() -> AsyncIOScheduler:
    """Configure and return the background scheduler."""
    scheduler = AsyncIOScheduler(timezone="Asia/Dhaka")

    # Full pipeline every 5 minutes during market hours (Sun-Thu 10:00-14:30)
    scheduler.add_job(
        market_data_pipeline,
        trigger=CronTrigger(
            day_of_week="sun,mon,tue,wed,thu",
            hour="10-14",
            minute="*/5",
            timezone="Asia/Dhaka",
        ),
        id="market_pipeline",
        name="Market data pipeline",
        replace_existing=True,
    )

    # Fallback: fetch prices every 30 minutes off-hours
    scheduler.add_job(
        fetch_live_prices,
        trigger=IntervalTrigger(minutes=30),
        id="fetch_prices_fallback",
        name="Fallback price fetch",
        replace_existing=True,
    )

    return scheduler
