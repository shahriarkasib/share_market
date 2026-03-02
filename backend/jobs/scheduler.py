"""Background job scheduler — lightweight pipeline that never blocks requests."""

import logging
import threading
import math
from collections import defaultdict
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

# Lock to prevent overlapping heavy refreshes
_heavy_refresh_lock = threading.Lock()


# ─── FAST JOBS (run every 5 min, <10s total) ─────────────────────────

async def fast_pipeline():
    """Fast pipeline: fetch prices + summary, update lightweight caches.

    Target: <10 seconds. Never blocks user requests.
    """
    try:
        await _fetch_live_prices()
        await _sync_market_summary()
        await _sync_dsex_history()
        _refresh_fast_caches()
        logger.info("Fast pipeline done")
    except Exception as e:
        logger.error(f"Fast pipeline failed: {e}")


async def _fetch_live_prices():
    """Fetch and store live prices from DSE."""
    try:
        df = fetcher.get_live_prices()
        if df.empty:
            return

        conn = get_connection()
        now_ts = datetime.now(DSE_TZ)

        for _, row in df.iterrows():
            try:
                conn.execute(
                    """INSERT INTO live_prices
                       (symbol, ltp, high, low, open, close_prev, change, change_pct,
                        volume, value, trade_count, updated_at)
                       VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                       ON CONFLICT (symbol) DO UPDATE SET
                         ltp = EXCLUDED.ltp, high = EXCLUDED.high, low = EXCLUDED.low,
                         open = EXCLUDED.open, close_prev = EXCLUDED.close_prev,
                         change = EXCLUDED.change, change_pct = EXCLUDED.change_pct,
                         volume = EXCLUDED.volume, value = EXCLUDED.value,
                         trade_count = EXCLUDED.trade_count, updated_at = EXCLUDED.updated_at""",
                    (
                        row.get("symbol", ""), row.get("ltp", 0),
                        row.get("high", 0), row.get("low", 0),
                        row.get("open", 0), row.get("close_prev", 0),
                        row.get("change", 0), row.get("change_pct", 0),
                        int(row.get("volume", 0)), row.get("value", 0),
                        int(row.get("trade_count", 0)), now_ts.isoformat(),
                    ),
                )
            except Exception as e:
                logger.error(f"Price save {row.get('symbol')}: {e}")

        conn.commit()

        # Intraday snapshots
        snap_ts = now_ts.replace(second=0, microsecond=0).isoformat()
        for _, row in df.iterrows():
            try:
                conn.execute(
                    """INSERT INTO intraday_snapshots
                       (symbol, ts, ltp, open, high, low, volume, value, trade_count)
                       VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
                       ON CONFLICT (symbol, ts) DO NOTHING""",
                    (
                        row.get("symbol", ""), snap_ts,
                        row.get("ltp", 0), row.get("open", 0),
                        row.get("high", 0), row.get("low", 0),
                        int(row.get("volume", 0)), row.get("value", 0),
                        int(row.get("trade_count", 0)),
                    ),
                )
            except Exception:
                pass

        conn.commit()
        conn.close()
        logger.info(f"Updated prices for {len(df)} stocks")
    except Exception as e:
        logger.error(f"Price fetch failed: {e}")


async def _sync_market_summary():
    """Sync market summary. Skip if DSEX is zero."""
    try:
        summary = fetcher.get_market_summary()
        if summary.get("dsex_index", 0) == 0:
            return

        conn = get_connection()
        conn.execute(
            """INSERT INTO market_summary
               (id, dsex_index, dsex_change, dsex_change_pct, total_volume,
                total_value, total_trade, advances, declines, unchanged,
                market_status, updated_at)
               VALUES (1, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
               ON CONFLICT (id) DO UPDATE SET
                 dsex_index = EXCLUDED.dsex_index, dsex_change = EXCLUDED.dsex_change,
                 dsex_change_pct = EXCLUDED.dsex_change_pct, total_volume = EXCLUDED.total_volume,
                 total_value = EXCLUDED.total_value, total_trade = EXCLUDED.total_trade,
                 advances = EXCLUDED.advances, declines = EXCLUDED.declines,
                 unchanged = EXCLUDED.unchanged, market_status = EXCLUDED.market_status,
                 updated_at = EXCLUDED.updated_at""",
            (
                summary.get("dsex_index", 0), summary.get("dsex_change", 0),
                summary.get("dsex_change_pct", 0), summary.get("total_volume", 0),
                summary.get("total_value", 0), summary.get("total_trade", 0),
                summary.get("advances", 0), summary.get("declines", 0),
                summary.get("unchanged", 0), summary.get("market_status", "UNKNOWN"),
                datetime.now(DSE_TZ).isoformat(),
            ),
        )
        conn.commit()
        conn.close()
        cache.delete("market_summary")
    except Exception as e:
        logger.error(f"Market summary sync failed: {e}")


async def _sync_dsex_history():
    """Upsert today's DSEX into dsex_history for the chart."""
    try:
        conn = get_connection()
        row = conn.execute("SELECT * FROM market_summary WHERE id = 1").fetchone()
        if not row or not row["dsex_index"] or row["dsex_index"] <= 0:
            conn.close()
            return

        today_str = datetime.now(DSE_TZ).strftime("%Y-%m-%d")
        conn.execute(
            """INSERT INTO dsex_history (date, dsex_index, total_volume, total_value, total_trade)
               VALUES (?, ?, ?, ?, ?)
               ON CONFLICT (date) DO UPDATE SET
                 dsex_index = EXCLUDED.dsex_index, total_volume = EXCLUDED.total_volume,
                 total_value = EXCLUDED.total_value, total_trade = EXCLUDED.total_trade""",
            (today_str, row["dsex_index"], row["total_volume"], row["total_value"], row["total_trade"]),
        )
        conn.commit()
        conn.close()
        cache.delete("dsex_history")
    except Exception as e:
        logger.error(f"DSEX history sync failed: {e}")


def _refresh_fast_caches():
    """Refresh only lightweight caches from DB (already written by pipeline)."""
    CACHE_TTL = 600
    now_dhaka = datetime.now(DSE_TZ)

    def _safe_float(v):
        if v is None:
            return None
        f = float(v)
        return None if (math.isnan(f) or math.isinf(f)) else f

    # 1. All prices (already in DB from _fetch_live_prices)
    try:
        conn = get_connection()
        rows = conn.execute("SELECT * FROM live_prices").fetchall()
        conn.close()
        if rows:
            all_prices = []
            for r in rows:
                d = dict(r)
                for k, v in d.items():
                    if isinstance(v, float) and (math.isnan(v) or math.isinf(v)):
                        d[k] = None
                all_prices.append(d)
            cache.set("all_prices", all_prices, CACHE_TTL)
    except Exception as e:
        logger.error(f"Fast cache prices failed: {e}")

    # 2. Market summary from DB
    try:
        conn = get_connection()
        ms_row = conn.execute("SELECT * FROM market_summary WHERE id = 1").fetchone()
        conn.close()
        if ms_row:
            summary = dict(ms_row)
            summary["last_updated"] = str(summary.pop("updated_at", None) or now_dhaka.isoformat())
            summary.pop("id", None)
            if summary.get("dsex_index", 0) > 0:
                cache.set("market_summary", summary, CACHE_TTL)
    except Exception:
        pass

    # 3. Most active tabs (lightweight — 4 small queries)
    try:
        conn = get_connection()
        for tab, order in [("gainers", "lp.change_pct DESC"), ("losers", "lp.change_pct ASC"),
                           ("volume", "lp.volume DESC"), ("turnover", "lp.value DESC")]:
            tab_rows = conn.execute(f"""
                SELECT lp.*, f.sector, f.company_name as fname
                FROM live_prices lp LEFT JOIN fundamentals f ON lp.symbol = f.symbol
                WHERE lp.ltp > 0 AND lp.trade_count > 0
                ORDER BY {order} LIMIT 20
            """).fetchall()
            tab_data = []
            for r in tab_rows:
                d = dict(r)
                for k, v in d.items():
                    if isinstance(v, float) and (math.isnan(v) or math.isinf(v)):
                        d[k] = None
                tab_data.append(d)
            cache.set(f"most_active_{tab}_20", tab_data, CACHE_TTL)
        conn.close()
    except Exception as e:
        logger.error(f"Fast cache most-active failed: {e}")


# ─── HEAVY JOBS (run every 10 min, in background thread) ─────────────

async def heavy_refresh():
    """Rebuild expensive caches in a background thread so we never block requests."""
    if not _heavy_refresh_lock.acquire(blocking=False):
        logger.info("Heavy refresh already running, skipping")
        return
    try:
        thread = threading.Thread(target=_heavy_refresh_sync, daemon=True)
        thread.start()
    except Exception as e:
        _heavy_refresh_lock.release()
        logger.error(f"Heavy refresh launch failed: {e}")


def _heavy_refresh_sync():
    """Run all expensive cache rebuilds (signals, heatmap, sectors, analysis, tracker)."""
    CACHE_TTL = 900  # 15 min — heavy refresh runs every 10
    try:
        now_dhaka = datetime.now(DSE_TZ)
        today = now_dhaka.strftime("%Y-%m-%d")

        # Find latest analysis date
        try:
            conn_dt = get_connection()
            latest_row = conn_dt.execute("SELECT MAX(date) FROM daily_analysis").fetchone()
            conn_dt.close()
            analysis_date = str(latest_row[0]) if latest_row and latest_row[0] else today
        except Exception:
            analysis_date = today

        # 1. Signals + summary (the biggest one)
        try:
            from api.routes_signals import _get_signals
            signals = _get_signals()
            if signals:
                strong_buy = sum(1 for s in signals if s["signal_type"] == "STRONG_BUY")
                buy = sum(1 for s in signals if s["signal_type"] == "BUY")
                hold = sum(1 for s in signals if s["signal_type"] == "HOLD")
                sell = sum(1 for s in signals if s["signal_type"] == "SELL")
                strong_sell = sum(1 for s in signals if s["signal_type"] == "STRONG_SELL")
                total = len(signals)
                bullish = strong_buy + buy
                bearish = sell + strong_sell
                sentiment = "BULLISH" if total > 0 and bullish / total > 0.5 else (
                    "BEARISH" if total > 0 and bearish / total > 0.5 else "NEUTRAL")
                cache.set("signals_summary", {
                    "total_stocks": total,
                    "strong_buy_count": strong_buy, "buy_count": buy,
                    "hold_count": hold, "sell_count": sell,
                    "strong_sell_count": strong_sell,
                    "market_sentiment": sentiment,
                    "last_updated": now_dhaka.isoformat(),
                    "is_computing": False,
                }, CACHE_TTL)
        except Exception as e:
            logger.error(f"Heavy: signals failed: {e}")

        # 2. Heatmap
        try:
            conn = get_connection()
            heatmap_rows = conn.execute("""
                SELECT f.sector, lp.symbol, lp.ltp, lp.change_pct,
                       lp.value as size_value, lp.volume, lp.value, lp.trade_count
                FROM fundamentals f
                JOIN live_prices lp ON f.symbol = lp.symbol
                WHERE f.sector IS NOT NULL AND lp.ltp > 0 AND lp.trade_count > 0
                ORDER BY f.sector, lp.value DESC
            """).fetchall()
            conn.close()
            sector_groups = defaultdict(lambda: {"stocks": [], "total_size": 0})
            for r in heatmap_rows:
                g = sector_groups[r["sector"]]
                sz = r["size_value"] or 0
                g["stocks"].append({
                    "symbol": r["symbol"], "change_pct": r["change_pct"] or 0,
                    "size_value": sz, "ltp": r["ltp"], "volume": r["volume"],
                })
                g["total_size"] += sz
            heatmap = []
            for sn, data in sector_groups.items():
                if data["total_size"] > 0:
                    pcts = [s["change_pct"] for s in data["stocks"]]
                    heatmap.append({
                        "sector": sn, "stocks": data["stocks"],
                        "total_size": data["total_size"],
                        "avg_change_pct": round(sum(pcts) / len(pcts), 2) if pcts else 0,
                    })
            heatmap.sort(key=lambda x: x["total_size"], reverse=True)
            cache.set("heatmap_turnover", heatmap, CACHE_TTL)
        except Exception as e:
            logger.error(f"Heavy: heatmap failed: {e}")

        # 3. Sector performance
        try:
            conn = get_connection()
            sec_rows = conn.execute("""
                SELECT f.sector, lp.symbol, lp.ltp, lp.change_pct, lp.volume, lp.value, lp.trade_count
                FROM fundamentals f
                JOIN live_prices lp ON f.symbol = lp.symbol
                WHERE f.sector IS NOT NULL AND lp.ltp > 0
                ORDER BY f.sector, lp.value DESC
            """).fetchall()
            conn.close()
            sectors = defaultdict(lambda: {
                "stocks": [], "advances": 0, "declines": 0, "unchanged": 0,
                "total_turnover": 0, "total_volume": 0, "total_trades": 0, "change_pcts": [],
            })
            for r in sec_rows:
                s = sectors[r["sector"]]
                chg = r["change_pct"] or 0
                s["stocks"].append({"symbol": r["symbol"], "change_pct": chg, "ltp": r["ltp"]})
                s["change_pcts"].append(chg)
                s["total_turnover"] += r["value"] or 0
                s["total_volume"] += r["volume"] or 0
                s["total_trades"] += r["trade_count"] or 0
                if chg > 0: s["advances"] += 1
                elif chg < 0: s["declines"] += 1
                else: s["unchanged"] += 1
            sec_result = []
            for sn, data in sorted(sectors.items()):
                pcts = data["change_pcts"]
                stocks = data["stocks"]
                avg_chg = sum(pcts) / len(pcts) if pcts else 0
                top_g = max(stocks, key=lambda x: x["change_pct"]) if stocks else None
                top_l = min(stocks, key=lambda x: x["change_pct"]) if stocks else None
                sec_result.append({
                    "sector": sn, "stock_count": len(stocks),
                    "advances": data["advances"], "declines": data["declines"],
                    "unchanged": data["unchanged"], "avg_change_pct": round(avg_chg, 2),
                    "total_turnover": data["total_turnover"], "total_volume": data["total_volume"],
                    "total_trades": data["total_trades"],
                    "top_gainer": {"symbol": top_g["symbol"], "change_pct": top_g["change_pct"]} if top_g else None,
                    "top_loser": {"symbol": top_l["symbol"], "change_pct": top_l["change_pct"]} if top_l else None,
                })
            sec_result.sort(key=lambda x: x["total_turnover"], reverse=True)
            cache.set("sector_performance", sec_result, CACHE_TTL)
        except Exception as e:
            logger.error(f"Heavy: sectors failed: {e}")

        # 4. Analysis daily
        try:
            from analysis.daily_report import load_daily_analysis
            analysis = load_daily_analysis(date_str=analysis_date)
            if analysis:
                grouped = {}
                for a in analysis:
                    act = a.get("action", "UNKNOWN")
                    grouped[act] = grouped.get(act, 0) + 1
                cache.set(f"analysis_daily_{analysis_date}_all", {
                    "date": analysis_date, "count": len(analysis),
                    "summary": grouped, "analysis": analysis,
                }, CACHE_TTL)
        except Exception as e:
            logger.error(f"Heavy: analysis failed: {e}")

        # 5. Live tracker
        try:
            from api.routes_analysis import _compute_status, _STATUS_PRIORITY, _is_market_open

            def _safe(v):
                if v is None: return None
                f = float(v)
                return None if (math.isnan(f) or math.isinf(f)) else f

            conn = get_connection()
            tracker_rows = conn.execute("""
                SELECT da.symbol, da.action, da.entry_low, da.entry_high, da.sl, da.t1, da.t2,
                       da.score, da.category, da.entry_start, da.entry_end,
                       da.exit_t1_by, da.exit_t2_by, da.hold_days_t1, da.hold_days_t2,
                       da.reasoning, da.rsi, da.stoch_rsi, da.macd_status,
                       da.risk_pct, da.reward_pct,
                       lp.ltp AS live_ltp, lp.change_pct AS live_change_pct,
                       lp.volume AS live_volume, lp.high AS live_high, lp.low AS live_low,
                       lp.updated_at AS price_updated_at,
                       f.sector
                FROM daily_analysis da
                JOIN live_prices lp ON da.symbol = lp.symbol
                LEFT JOIN fundamentals f ON da.symbol = f.symbol
                WHERE da.date = %s AND da.action LIKE 'BUY%%'
            """, (analysis_date,)).fetchall()

            stocks = []
            for r in tracker_rows:
                ltp = float(r["live_ltp"] or 0)
                el, eh = float(r["entry_low"] or 0), float(r["entry_high"] or 0)
                sl, t1, t2 = float(r["sl"] or 0), float(r["t1"] or 0), float(r["t2"] or 0)
                if ltp <= 0: continue
                status, dist_pct = _compute_status(ltp, el, eh, sl, t1, t2)
                stocks.append({
                    "symbol": r["symbol"], "action": r["action"],
                    "category": r["category"] or "", "sector": r["sector"] or "",
                    "score": _safe(r["score"]) or 0,
                    "entry_low": el, "entry_high": eh, "sl": sl, "t1": t1, "t2": t2,
                    "entry_start": str(r["entry_start"]) if r["entry_start"] else None,
                    "entry_end": str(r["entry_end"]) if r["entry_end"] else None,
                    "exit_t1_by": str(r["exit_t1_by"]) if r["exit_t1_by"] else None,
                    "exit_t2_by": str(r["exit_t2_by"]) if r["exit_t2_by"] else None,
                    "hold_days_t1": r["hold_days_t1"], "hold_days_t2": r["hold_days_t2"],
                    "reasoning": r["reasoning"] or "",
                    "rsi": _safe(r["rsi"]) or 0, "stoch_rsi": _safe(r["stoch_rsi"]) or 0,
                    "macd_status": r["macd_status"] or "",
                    "risk_pct": _safe(r["risk_pct"]) or 0, "reward_pct": _safe(r["reward_pct"]) or 0,
                    "live_ltp": round(ltp, 1), "live_change_pct": _safe(r["live_change_pct"]) or 0,
                    "live_volume": int(r["live_volume"] or 0),
                    "live_high": _safe(r["live_high"]) or 0, "live_low": _safe(r["live_low"]) or 0,
                    "status": status, "distance_pct": dist_pct,
                })
            stocks.sort(key=lambda s: (_STATUS_PRIORITY.get(s["status"], 9), -s["score"]))

            updated_at = None
            if stocks:
                ts_row = conn.execute("SELECT MAX(updated_at) FROM live_prices").fetchone()
                if ts_row and ts_row[0]:
                    updated_at = str(ts_row[0])
            conn.close()
            cache.set(f"live_tracker_{analysis_date}", {
                "date": analysis_date, "market_status": _is_market_open(),
                "updated_at": updated_at, "count": len(stocks), "stocks": stocks,
            }, CACHE_TTL)
        except Exception as e:
            logger.error(f"Heavy: live-tracker failed: {e}")

        logger.info("Heavy cache refresh complete")
    except Exception as e:
        logger.error(f"Heavy refresh failed: {e}")
    finally:
        _heavy_refresh_lock.release()


# ─── DAILY PRICE SYNC (every 15 min, lightweight) ────────────────────

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


# ─── DSEX BACKFILL (startup only) ────────────────────────────────────

def backfill_dsex_history():
    """Fill gaps in dsex_history using bdshare + market_summary. Runs at startup."""
    try:
        conn = get_connection()
        last_row = conn.execute("SELECT MAX(date) AS last_date FROM dsex_history").fetchone()
        last_date = str(last_row["last_date"]) if last_row and last_row["last_date"] else None
        conn.close()

        if not last_date:
            return

        filled = 0
        # bdshare backfill
        try:
            from bdshare import market_summary as bdshare_summary
            import warnings
            with warnings.catch_warnings():
                warnings.simplefilter("ignore")
                data = bdshare_summary()
            if data is not None and not data.empty:
                conn = get_connection()
                for _, row in data.iterrows():
                    date_str = str(row.get("date", ""))[:10]
                    if date_str <= last_date:
                        continue
                    dsex = row.get("dsex_index") or row.get("DSEX")
                    if not dsex or float(dsex) <= 0:
                        continue
                    conn.execute(
                        """INSERT INTO dsex_history (date, dsex_index, total_volume, total_value, total_trade)
                           VALUES (?, ?, ?, ?, ?)
                           ON CONFLICT (date) DO UPDATE SET
                             dsex_index = EXCLUDED.dsex_index, total_volume = EXCLUDED.total_volume,
                             total_value = EXCLUDED.total_value, total_trade = EXCLUDED.total_trade""",
                        (date_str, float(dsex), int(row.get("total_volume", 0) or 0),
                         float(row.get("total_value", 0) or 0), int(row.get("total_trade", 0) or 0)),
                    )
                    filled += 1
                conn.commit()
                conn.close()
        except Exception as e:
            logger.warning(f"bdshare backfill failed (non-fatal): {e}")

        # Upsert latest market_summary
        try:
            conn = get_connection()
            ms = conn.execute("SELECT * FROM market_summary WHERE id = 1").fetchone()
            if ms and ms["dsex_index"] and ms["dsex_index"] > 0:
                today_str = datetime.now(DSE_TZ).strftime("%Y-%m-%d")
                conn.execute(
                    """INSERT INTO dsex_history (date, dsex_index, total_volume, total_value, total_trade)
                       VALUES (?, ?, ?, ?, ?)
                       ON CONFLICT (date) DO UPDATE SET
                         dsex_index = EXCLUDED.dsex_index, total_volume = EXCLUDED.total_volume,
                         total_value = EXCLUDED.total_value, total_trade = EXCLUDED.total_trade""",
                    (today_str, ms["dsex_index"], ms["total_volume"], ms["total_value"], ms["total_trade"]),
                )
                conn.commit()
                filled += 1
            conn.close()
        except Exception as e:
            logger.warning(f"market_summary upsert to dsex_history failed: {e}")

        if filled:
            cache.delete("dsex_history")
            logger.info(f"Backfilled {filled} days into dsex_history")
    except Exception as e:
        logger.error(f"DSEX backfill failed: {e}")


# ─── OTHER JOBS ───────────────────────────────────────────────────────

async def run_post_market_analysis():
    """Run daily analysis after market close."""
    try:
        logger.info("Triggering post-market daily analysis...")
        from analysis.daily_report import run_daily_analysis
        thread = threading.Thread(target=run_daily_analysis, daemon=True)
        thread.start()
        cache.delete("all_signals")
        cache.delete("signals_summary")
        cache.delete("suggestions")
    except Exception as e:
        logger.error(f"Post-market analysis failed: {e}")


async def run_live_scanner():
    """Run intraday live scanner."""
    try:
        from analysis.live_scanner import run_live_scan
        thread = threading.Thread(target=run_live_scan, daemon=True)
        thread.start()
    except Exception as e:
        logger.error(f"Live scanner failed: {e}")


async def verify_scan_decisions():
    """Verify past scan decisions against actual outcomes."""
    try:
        from analysis.live_scanner import verify_past_decisions
        thread = threading.Thread(target=verify_past_decisions, daemon=True)
        thread.start()
    except Exception as e:
        logger.error(f"Decision verification failed: {e}")


async def cleanup_intraday_snapshots():
    """Delete intraday snapshots older than 7 days."""
    try:
        conn = get_connection()
        conn.execute("DELETE FROM intraday_snapshots WHERE ts < NOW() - INTERVAL '7 days'")
        conn.commit()
        conn.close()
        logger.info("Cleaned up old intraday snapshots")
    except Exception as e:
        logger.error(f"Intraday cleanup failed: {e}")


# ─── BACKWARD COMPAT (used by main.py startup) ───────────────────────

async def refresh_all_caches():
    """Startup cache warm — runs fast caches then kicks off heavy in background."""
    _refresh_fast_caches()
    await heavy_refresh()


# ─── SCHEDULER SETUP ─────────────────────────────────────────────────

def setup_scheduler() -> AsyncIOScheduler:
    """Configure background scheduler with staggered jobs."""
    scheduler = AsyncIOScheduler(timezone="Asia/Dhaka")

    # FAST pipeline: prices + summary + lightweight caches (every 5 min, market hours)
    scheduler.add_job(
        fast_pipeline,
        trigger=CronTrigger(
            day_of_week="sun,mon,tue,wed,thu",
            hour="10-14", minute="*/5",
            timezone="Asia/Dhaka",
        ),
        id="fast_pipeline",
        name="Fast: prices + summary",
        replace_existing=True,
    )

    # HEAVY refresh: signals, heatmap, sectors, analysis (every 10 min, in bg thread)
    scheduler.add_job(
        heavy_refresh,
        trigger=CronTrigger(
            day_of_week="sun,mon,tue,wed,thu",
            hour="10-14", minute="2,12,22,32,42,52",
            timezone="Asia/Dhaka",
        ),
        id="heavy_refresh",
        name="Heavy: signals + analysis (bg thread)",
        replace_existing=True,
    )

    # Daily price sync (every 15 min during market hours)
    scheduler.add_job(
        sync_daily_prices_from_live,
        trigger=CronTrigger(
            day_of_week="sun,mon,tue,wed,thu",
            hour="10-14", minute="5,20,35,50",
            timezone="Asia/Dhaka",
        ),
        id="daily_price_sync",
        name="Sync live to daily_prices",
        replace_existing=True,
    )

    # Off-hours: just fast caches every 10 min (for deploy warmup)
    scheduler.add_job(
        _refresh_fast_caches_async,
        trigger=IntervalTrigger(minutes=10),
        id="offhours_cache",
        name="Off-hours cache refresh",
        replace_existing=True,
    )

    # Cleanup old intraday snapshots
    scheduler.add_job(
        cleanup_intraday_snapshots,
        trigger=CronTrigger(hour=0, minute=30, timezone="Asia/Dhaka"),
        id="cleanup_intraday",
        name="Cleanup old intraday snapshots",
        replace_existing=True,
    )

    # Post-market daily analysis (15:00 BST)
    scheduler.add_job(
        run_post_market_analysis,
        trigger=CronTrigger(
            day_of_week="sun,mon,tue,wed,thu",
            hour=15, minute=0, timezone="Asia/Dhaka",
        ),
        id="daily_analysis",
        name="Post-market daily analysis",
        replace_existing=True,
    )

    # Live intraday scanner (every 5 min during market)
    scheduler.add_job(
        run_live_scanner,
        trigger=CronTrigger(
            day_of_week="sun,mon,tue,wed,thu",
            hour="9-14", minute="*/5",
            timezone="Asia/Dhaka",
        ),
        id="live_scanner",
        name="Live intraday scanner",
        replace_existing=True,
    )

    # Verify past scan decisions
    scheduler.add_job(
        verify_scan_decisions,
        trigger=CronTrigger(
            day_of_week="sun,mon,tue,wed,thu",
            hour=15, minute=30, timezone="Asia/Dhaka",
        ),
        id="verify_decisions",
        name="Verify past scan decisions",
        replace_existing=True,
    )

    return scheduler


async def _refresh_fast_caches_async():
    """Async wrapper for off-hours fast cache refresh."""
    _refresh_fast_caches()
