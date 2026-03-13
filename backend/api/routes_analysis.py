"""Daily analysis API routes."""

import io
import json
import logging
import math
import os
import threading
from datetime import datetime, time as dtime

import pytz
from fastapi import APIRouter, HTTPException, Query
from fastapi.responses import StreamingResponse

from analysis.daily_report import (
    generate_daily_analysis,
    get_available_dates,
    load_daily_analysis,
    run_daily_analysis,
    save_daily_analysis,
)
from config import MARKET_DAYS, CACHE_TTL_LIVE_PRICES
from data.cache import cache
from database import get_connection

logger = logging.getLogger(__name__)
router = APIRouter()
DSE_TZ = pytz.timezone("Asia/Dhaka")
_analysis_lock = threading.Lock()


def _is_market_open() -> str:
    """Return 'OPEN', 'PRE_MARKET', or 'CLOSED'."""
    now = datetime.now(DSE_TZ)
    if now.weekday() not in MARKET_DAYS:
        return "CLOSED"
    t = now.time()
    if dtime(9, 30) <= t < dtime(10, 0):
        return "PRE_MARKET"
    if dtime(10, 0) <= t <= dtime(14, 30):
        return "OPEN"
    return "CLOSED"


_STATUS_PRIORITY = {
    "SL_HIT": 0, "ENTRY_ZONE": 1, "APPROACHING": 2,
    "BELOW_ENTRY": 3, "T1_HIT": 4, "T2_HIT": 5, "WATCHING": 6,
}


def _compute_status(ltp: float, entry_low: float, entry_high: float,
                    sl: float, t1: float, t2: float) -> tuple[str, float]:
    """Compute tracking status and distance % from entry zone midpoint."""
    entry_mid = (entry_low + entry_high) / 2 if entry_low > 0 else ltp
    dist_pct = round((ltp - entry_mid) / entry_mid * 100, 1) if entry_mid > 0 else 0

    if sl > 0 and ltp <= sl:
        return "SL_HIT", dist_pct
    if t2 > 0 and ltp >= t2:
        return "T2_HIT", dist_pct
    if t1 > 0 and ltp >= t1:
        return "T1_HIT", dist_pct
    if entry_low > 0 and entry_high > 0 and entry_low <= ltp <= entry_high:
        return "ENTRY_ZONE", dist_pct
    if entry_low > 0 and ltp < entry_low and (sl <= 0 or ltp > sl):
        return "BELOW_ENTRY", dist_pct
    if entry_high > 0 and ltp <= entry_high * 1.02:
        return "APPROACHING", dist_pct
    return "WATCHING", dist_pct

_running = False


@router.get("/daily")
async def get_daily_analysis_api(
    date: str = Query(default=None, description="Date YYYY-MM-DD (default: today)"),
    action: str = Query(default=None, description="Filter: BUY, HOLD, AVOID"),
):
    """Get daily analysis for a specific date."""
    if not date:
        # Use latest analysis date, not today (today's analysis may not exist yet)
        try:
            conn = get_connection()
            latest = conn.execute("SELECT MAX(date) FROM daily_analysis").fetchone()
            conn.close()
            date = str(latest[0]) if latest and latest[0] else datetime.now(DSE_TZ).strftime("%Y-%m-%d")
        except Exception:
            date = datetime.now(DSE_TZ).strftime("%Y-%m-%d")

    cache_key = f"analysis_daily_{date}_{action or 'all'}"
    cached = cache.get(cache_key)
    if cached:
        return cached

    # Thundering-herd lock: only one thread runs the expensive query
    with _analysis_lock:
        cached = cache.get(cache_key)
        if cached:
            return cached

        results = load_daily_analysis(date_str=date, action_filter=action)
        if not results:
            return {"date": date, "count": 0, "analysis": [], "message": "No analysis for this date"}

        # Group by action
        grouped = {}
        for r in results:
            act = r.get("action", "UNKNOWN")
            grouped.setdefault(act, []).append(r)

        result = {
            "date": date,
            "count": len(results),
            "summary": {k: len(v) for k, v in grouped.items()},
            "analysis": results,
        }
        cache.set(cache_key, result, 1800)  # 30 min — data changes once/day
        return result


@router.get("/dates")
async def get_analysis_dates():
    """List dates that have daily analysis."""
    dates = get_available_dates()
    return {"dates": dates}


@router.get("/excel")
async def download_analysis_excel(
    date: str = Query(default=None, description="Date YYYY-MM-DD (default: today)"),
):
    """Download daily analysis as Excel file."""
    if not date:
        date = datetime.now(DSE_TZ).strftime("%Y-%m-%d")

    results = load_daily_analysis(date_str=date)
    if not results:
        raise HTTPException(status_code=404, detail=f"No analysis for {date}")

    from analysis.excel_generator import generate_analysis_excel

    buf = io.BytesIO()
    generate_analysis_excel(results, buf)
    buf.seek(0)

    filename = f"DSE_Analysis_{date}.xlsx"
    return StreamingResponse(
        buf,
        media_type="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
        headers={"Content-Disposition": f'attachment; filename="{filename}"'},
    )


@router.post("/trigger")
async def trigger_analysis():
    """Manually trigger daily analysis computation."""
    global _running
    if _running:
        return {"status": "already_running"}

    def _run():
        global _running
        _running = True
        try:
            run_daily_analysis()
        finally:
            _running = False

    thread = threading.Thread(target=_run, daemon=True)
    thread.start()
    return {"status": "started", "message": "Analysis running in background"}


@router.get("/status")
async def analysis_status():
    """Check if analysis is currently running."""
    return {"running": _running}


@router.get("/summary")
async def get_analysis_summary(
    date: str = Query(default=None),
):
    """Get summary counts by action for a date."""
    if not date:
        date = datetime.now(DSE_TZ).strftime("%Y-%m-%d")

    conn = get_connection()
    rows = conn.execute(
        "SELECT action, COUNT(*) as count FROM daily_analysis WHERE date = ? GROUP BY action ORDER BY count DESC",
        (date,),
    ).fetchall()
    total = conn.execute(
        "SELECT COUNT(*) FROM daily_analysis WHERE date = ?", (date,)
    ).fetchone()[0]
    conn.close()

    return {
        "date": date,
        "total": total,
        "by_action": {r["action"]: r["count"] for r in rows},
    }


@router.get("/live-tracker")
async def live_tracker(
    date: str = Query(default=None, description="Analysis date (default: latest)"),
):
    """Compare daily analysis levels against live prices in real-time."""
    if not date:
        try:
            conn = get_connection()
            latest = conn.execute("SELECT MAX(date) FROM daily_analysis").fetchone()
            conn.close()
            date = str(latest[0]) if latest and latest[0] else datetime.now(DSE_TZ).strftime("%Y-%m-%d")
        except Exception:
            date = datetime.now(DSE_TZ).strftime("%Y-%m-%d")

    cache_key = f"live_tracker_{date}"
    cached = cache.get(cache_key)
    if cached:
        return cached

    conn = get_connection()
    rows = conn.execute(
        """SELECT da.symbol, da.action, da.entry_low, da.entry_high, da.sl, da.t1, da.t2,
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
           WHERE da.date = ?
             AND da.action LIKE 'BUY%%'
        """,
        (date,),
    ).fetchall()
    conn.close()

    stocks = []
    for r in rows:
        ltp = float(r["live_ltp"] or 0)
        entry_low = float(r["entry_low"] or 0)
        entry_high = float(r["entry_high"] or 0)
        sl = float(r["sl"] or 0)
        t1 = float(r["t1"] or 0)
        t2 = float(r["t2"] or 0)

        if ltp <= 0:
            continue

        status, dist_pct = _compute_status(ltp, entry_low, entry_high, sl, t1, t2)

        def _safe(v):
            if v is None:
                return None
            f = float(v)
            if math.isnan(f) or math.isinf(f):
                return None
            return f

        stocks.append({
            "symbol": r["symbol"],
            "action": r["action"],
            "category": r["category"] or "",
            "sector": r["sector"] or "",
            "score": _safe(r["score"]) or 0,
            "entry_low": entry_low,
            "entry_high": entry_high,
            "sl": sl,
            "t1": t1,
            "t2": t2,
            "entry_start": str(r["entry_start"]) if r["entry_start"] else None,
            "entry_end": str(r["entry_end"]) if r["entry_end"] else None,
            "exit_t1_by": str(r["exit_t1_by"]) if r["exit_t1_by"] else None,
            "exit_t2_by": str(r["exit_t2_by"]) if r["exit_t2_by"] else None,
            "hold_days_t1": r["hold_days_t1"],
            "hold_days_t2": r["hold_days_t2"],
            "reasoning": r["reasoning"] or "",
            "rsi": _safe(r["rsi"]) or 0,
            "stoch_rsi": _safe(r["stoch_rsi"]) or 0,
            "macd_status": r["macd_status"] or "",
            "risk_pct": _safe(r["risk_pct"]) or 0,
            "reward_pct": _safe(r["reward_pct"]) or 0,
            "live_ltp": round(ltp, 1),
            "live_change_pct": _safe(r["live_change_pct"]) or 0,
            "live_volume": int(r["live_volume"] or 0),
            "live_high": _safe(r["live_high"]) or 0,
            "live_low": _safe(r["live_low"]) or 0,
            "status": status,
            "distance_pct": dist_pct,
        })

    # Sort by status priority, then score desc
    stocks.sort(key=lambda s: (_STATUS_PRIORITY.get(s["status"], 9), -s["score"]))

    # Get latest price update time
    updated_at = None
    if stocks:
        conn2 = get_connection()
        ts_row = conn2.execute(
            "SELECT MAX(updated_at) FROM live_prices"
        ).fetchone()
        conn2.close()
        if ts_row and ts_row[0]:
            updated_at = str(ts_row[0])

    result = {
        "date": date,
        "market_status": _is_market_open(),
        "updated_at": updated_at,
        "count": len(stocks),
        "stocks": stocks,
    }
    cache.set(cache_key, result, CACHE_TTL_LIVE_PRICES)
    return result


@router.get("/live-scan")
async def get_live_scan():
    """Get the latest live scan results (market depth + buy signal analysis)."""
    from analysis.live_scanner import get_latest_scan
    scan = get_latest_scan()
    if not scan.get("timestamp"):
        return {"timestamp": None, "results": [], "summary": {}, "total": 0,
                "message": "No scan results yet. Scanner runs every 5 min during market hours (9:55-14:30)."}
    return scan


@router.get("/live-scan/excel")
async def download_live_scan_excel(
    date: str = Query(default=None, description="Date YYYY-MM-DD (default: today)"),
):
    """Download the live scan Excel file for a date."""
    from analysis.live_scanner import get_scan_excel_path

    filepath = get_scan_excel_path(date)
    if not filepath:
        d = date or datetime.now(DSE_TZ).strftime("%Y-%m-%d")
        raise HTTPException(status_code=404, detail=f"No live scan Excel for {d}")

    filename = os.path.basename(filepath)
    return StreamingResponse(
        open(filepath, "rb"),
        media_type="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
        headers={"Content-Disposition": f'attachment; filename="{filename}"'},
    )


@router.post("/live-scan/trigger")
async def trigger_live_scan():
    """Manually trigger a live scan (for testing outside market hours)."""
    from analysis.live_scanner import run_live_scan
    import threading

    def _run():
        return run_live_scan()

    thread = threading.Thread(target=_run, daemon=True)
    thread.start()
    return {"status": "started", "message": "Live scan triggered"}


@router.get("/llm-scan")
async def get_llm_scan(
    date: str = Query(default=None, description="Date YYYY-MM-DD (default: today)"),
):
    """Get latest LLM analysis results."""
    if not date:
        date = datetime.now(DSE_TZ).strftime("%Y-%m-%d")

    conn = get_connection()

    # Get the latest scan_time for this date
    ts_row = conn.execute(
        "SELECT MAX(scan_time) as latest FROM llm_scan_results WHERE date = %s",
        (date,),
    ).fetchone()

    if not ts_row or not ts_row["latest"]:
        conn.close()
        return {
            "date": date,
            "scan_time": None,
            "market_outlook": None,
            "top_picks": [],
            "message": "No LLM analysis for this date. Run the scanner on the GCP VM.",
        }

    latest = ts_row["latest"]

    # Load market overview
    overview_row = conn.execute(
        """SELECT recommendation, reasoning, key_insights, risk_factors
           FROM llm_scan_results
           WHERE date = %s AND scan_time = %s AND analysis_type = 'market_overview'
           LIMIT 1""",
        (date, latest),
    ).fetchone()

    market_outlook = None
    if overview_row:
        market_outlook = {
            "sentiment": overview_row["recommendation"],
            "summary": overview_row["reasoning"],
            "key_insights": json.loads(overview_row["key_insights"]) if overview_row["key_insights"] else {},
            "key_risks": json.loads(overview_row["risk_factors"]) if overview_row["risk_factors"] else [],
        }

    # Load stock picks
    pick_rows = conn.execute(
        """SELECT symbol, recommendation, confidence, reasoning, key_insights, risk_factors
           FROM llm_scan_results
           WHERE date = %s AND scan_time = %s AND analysis_type = 'stock_pick'
           ORDER BY
             CASE confidence
               WHEN 'HIGH' THEN 0
               WHEN 'MEDIUM' THEN 1
               WHEN 'LOW' THEN 2
               ELSE 3
             END""",
        (date, latest),
    ).fetchall()

    top_picks = []
    for r in pick_rows:
        insights = json.loads(r["key_insights"]) if r["key_insights"] else {}
        risks = json.loads(r["risk_factors"]) if r["risk_factors"] else []
        top_picks.append({
            "symbol": r["symbol"],
            "recommendation": r["recommendation"],
            "confidence": r["confidence"],
            "reasoning": r["reasoning"],
            "entry_strategy": insights.get("entry_strategy", ""),
            "risk_note": risks[0] if risks else "",
        })

    # Check how many scans today
    count_row = conn.execute(
        """SELECT COUNT(DISTINCT scan_time) as cnt FROM llm_scan_results
           WHERE date = %s AND analysis_type = 'market_overview'""",
        (date,),
    ).fetchone()

    conn.close()

    return {
        "date": date,
        "scan_time": str(latest),
        "scan_count": count_row["cnt"] if count_row else 0,
        "market_outlook": market_outlook,
        "top_picks": top_picks,
    }


@router.get("/llm-scan/history")
async def get_llm_scan_history(
    date: str = Query(default=None, description="Date YYYY-MM-DD"),
):
    """Get all LLM scan times for a date (to see how analysis evolved)."""
    if not date:
        date = datetime.now(DSE_TZ).strftime("%Y-%m-%d")

    conn = get_connection()
    rows = conn.execute(
        """SELECT DISTINCT scan_time, recommendation as sentiment
           FROM llm_scan_results
           WHERE date = %s AND analysis_type = 'market_overview'
           ORDER BY scan_time DESC""",
        (date,),
    ).fetchall()
    conn.close()

    return {
        "date": date,
        "scans": [{"time": str(r["scan_time"]), "sentiment": r["sentiment"]} for r in rows],
    }


@router.get("/decision-accuracy")
async def get_decision_accuracy_api(
    days: int = Query(default=30, description="Look back N days"),
):
    """Get accuracy stats for past scan decisions (backtesting)."""
    from analysis.live_scanner import get_decision_accuracy
    return get_decision_accuracy(days)


@router.post("/verify-decisions")
async def trigger_decision_verification():
    """Manually trigger verification of past scan decisions."""
    from analysis.live_scanner import verify_past_decisions
    import threading
    thread = threading.Thread(target=verify_past_decisions, daemon=True)
    thread.start()
    return {"status": "started", "message": "Verifying past decisions..."}


@router.get("/buy-radar")
async def get_buy_radar(categories: str = "A", exclude_sectors: str = ""):
    """Buy Radar — shows stocks approaching buy zone with per-indicator readiness.

    Query params:
      categories: comma-separated list of categories (default "A", options: A,B,Z,ALL)
      exclude_sectors: comma-separated sector keywords to exclude (e.g. "bank,insurance")
    """
    import pandas as pd
    from analysis.indicators import TechnicalIndicators

    cache_key = f"buy_radar_{categories}_{exclude_sectors}"
    cached = cache.get(cache_key)
    if cached:
        return cached

    # Try pre-computed radar first (instant load)
    if categories.strip().upper() == "A" and not exclude_sectors:
        try:
            precomp_conn = get_connection()
            row = precomp_conn.execute(
                "SELECT data_json FROM radar_precomputed "
                "WHERE category = 'A' ORDER BY date DESC LIMIT 1"
            ).fetchone()
            precomp_conn.close()
            if row and row["data_json"]:
                import json as _json
                result = _json.loads(row["data_json"])
                cache.set(cache_key, result, ttl=1800)
                return result
        except Exception:
            pass  # Fall through to live computation

    conn = get_connection()

    # Get symbols with sector info based on category filter
    cat_list = [c.strip().upper() for c in categories.split(",") if c.strip()]
    if "ALL" in cat_list:
        rows = conn.execute(
            "SELECT symbol, sector, category FROM fundamentals"
        ).fetchall()
    else:
        placeholders = ",".join(["?"] * len(cat_list))
        rows = conn.execute(
            f"SELECT symbol, sector, category FROM fundamentals WHERE category IN ({placeholders})",
            cat_list,
        ).fetchall()
    a_cat = {r["symbol"]: {"sector": r["sector"] or "", "category": r["category"] or "A"} for r in rows}

    # Optional sector filter (user-specified only, no default halal filter)
    if exclude_sectors:
        skip_sectors = {es.strip().lower() for es in exclude_sectors.split(",") if es.strip()}
        filtered = {
            s: info["sector"] for s, info in a_cat.items()
            if not (info["sector"] and any(k in info["sector"].lower() for k in skip_sectors))
        }
    else:
        filtered = {s: info["sector"] for s, info in a_cat.items()}
    # Keep category info for response
    cat_map = {s: info["category"] for s, info in a_cat.items()}

    # Get latest analysis data for entry/exit levels
    latest_row = conn.execute("SELECT MAX(date) FROM daily_analysis").fetchone()
    latest_date = str(latest_row[0]) if latest_row and latest_row[0] else None

    analysis_map = {}
    if latest_date:
        arows = conn.execute(
            "SELECT symbol, ltp, action, score, entry_low, entry_high, sl, t1, t2, "
            "macd_status FROM daily_analysis WHERE date = ?",
            (latest_date,),
        ).fetchall()
        analysis_map = {r["symbol"]: dict(r) for r in arows}

    # ── Load LLM + Judge AI analysis ──
    llm_map: dict[str, dict] = {}
    judge_map: dict[str, dict] = {}
    ai_date = latest_date
    ai_stale_days = 0  # How many days old the AI analysis is
    if ai_date:
        # Try latest LLM date (may differ from algo date)
        llm_date_row = conn.execute("SELECT MAX(date) FROM llm_daily_analysis").fetchone()
        if llm_date_row and llm_date_row[0]:
            ai_date = str(llm_date_row[0])
            from datetime import date as _date
            try:
                ai_d = _date.fromisoformat(ai_date)
                algo_d = _date.fromisoformat(latest_date)
                ai_stale_days = (algo_d - ai_d).days
            except Exception:
                pass
        logger.info(f"Radar: AI date={ai_date}, algo date={latest_date}, stale={ai_stale_days}d")

        llm_rows = conn.execute(
            "SELECT symbol, action, confidence, reasoning, wait_for, wait_days, "
            "score, risk_factors, catalysts, how_to_buy, volume_rule, "
            "entry_low, entry_high, sl, t1, t2, stage, stage_reasoning, "
            "expected_return_1w, expected_return_2w, expected_return_1m, downside_risk, "
            "dsex_dependency, if_dsex_drops, if_dsex_rises, dsex_outlook "
            "FROM llm_daily_analysis WHERE date = ?", (ai_date,),
        ).fetchall()
        llm_map = {r["symbol"]: dict(r) for r in llm_rows}

        judge_rows = conn.execute(
            "SELECT symbol, final_action, final_confidence, agreement, "
            "reasoning, key_risk, algo_strengths, llm_strengths, "
            "entry_low, entry_high, sl, t1, t2, score "
            "FROM judge_daily_analysis WHERE date = ?", (ai_date,),
        ).fetchall()
        judge_map = {r["symbol"]: dict(r) for r in judge_rows}

    # ── Market Context: DSEX state for adaptive thresholds ──
    dsex_row = conn.execute(
        "SELECT dsex_index FROM dsex_history ORDER BY date DESC LIMIT 10"
    ).fetchall()
    market_ctx = {"regime": "NEUTRAL", "dsex": 0, "dsex_rsi": 50, "adjustment": 1.0}
    if len(dsex_row) >= 10:
        dsex_prices = [float(r["dsex_index"]) for r in reversed(dsex_row)]
        dsex_now = dsex_prices[-1]
        market_ctx["dsex"] = dsex_now

        # Simple RSI for DSEX
        deltas = [dsex_prices[i] - dsex_prices[i-1] for i in range(1, len(dsex_prices))]
        gains = [d for d in deltas if d > 0]
        losses = [-d for d in deltas if d < 0]
        avg_g = sum(gains) / max(len(gains), 1)
        avg_l = sum(losses) / max(len(losses), 1)
        dsex_rsi = 100 - (100 / (1 + avg_g / max(avg_l, 0.01)))
        market_ctx["dsex_rsi"] = round(dsex_rsi, 1)

        # Market regime determines threshold adjustment
        if dsex_rsi < 30:
            market_ctx["regime"] = "OVERSOLD"
            market_ctx["adjustment"] = 1.3  # Easier to qualify (market is cheap)
        elif dsex_rsi < 40:
            market_ctx["regime"] = "WEAK"
            market_ctx["adjustment"] = 1.15
        elif dsex_rsi > 70:
            market_ctx["regime"] = "OVERBOUGHT"
            market_ctx["adjustment"] = 0.7  # Harder to qualify (market stretched)
        elif dsex_rsi > 60:
            market_ctx["regime"] = "HEATED"
            market_ctx["adjustment"] = 0.85
        else:
            market_ctx["regime"] = "NEUTRAL"
            market_ctx["adjustment"] = 1.0

    # Market volume analysis
    try:
        ms_row = conn.execute(
            "SELECT total_volume, total_value, total_trade, advances, declines "
            "FROM market_summary ORDER BY updated_at DESC LIMIT 1"
        ).fetchone()
        if ms_row:
            # total_value is in millions BDT, convert to crore (÷10)
            market_ctx["total_value_cr"] = round(float(ms_row["total_value"] or 0) / 10, 1)
            market_ctx["total_volume"] = int(ms_row["total_volume"] or 0)
            market_ctx["total_trades"] = int(ms_row["total_trade"] or 0)
            market_ctx["advances"] = int(ms_row["advances"] or 0)
            market_ctx["declines"] = int(ms_row["declines"] or 0)

            # Volume verdict
            val_cr = market_ctx["total_value_cr"]
            if val_cr < 400:
                market_ctx["volume_verdict"] = "VERY_LOW"
            elif val_cr < 700:
                market_ctx["volume_verdict"] = "LOW"
            elif val_cr < 1200:
                market_ctx["volume_verdict"] = "NORMAL"
            elif val_cr < 2000:
                market_ctx["volume_verdict"] = "HIGH"
            else:
                market_ctx["volume_verdict"] = "VERY_HIGH"

            # Breadth ratio (advances vs declines)
            adv = market_ctx["advances"]
            dec = market_ctx["declines"]
            total_ad = adv + dec
            if total_ad > 0:
                market_ctx["breadth_pct"] = round(adv / total_ad * 100, 0)
            else:
                market_ctx["breadth_pct"] = 50

            # Interpret: positive index + low volume = weak rally (sellers absent, not buyers strong)
            dsex_chg = dsex_prices[-1] - dsex_prices[-2] if len(dsex_prices) >= 2 else 0
            market_ctx["dsex_change"] = round(dsex_chg, 1)
            if dsex_chg > 0 and val_cr < 500:
                market_ctx["signal"] = "Weak rally — low conviction, wait for volume confirmation"
            elif dsex_chg > 0 and val_cr > 1000:
                market_ctx["signal"] = "Strong rally — high volume confirms buying"
            elif dsex_chg < 0 and val_cr > 1000:
                market_ctx["signal"] = "Heavy selling — avoid new entries"
            elif dsex_chg < 0 and val_cr < 500:
                market_ctx["signal"] = "Quiet pullback — possible accumulation opportunity"
            else:
                market_ctx["signal"] = "Normal activity — follow individual stock signals"
    except Exception as e:
        logger.warning(f"Market volume ctx error: {e}")

    logger.info(f"Radar market context: DSEX RSI={market_ctx['dsex_rsi']}, "
                f"regime={market_ctx['regime']}, adj={market_ctx['adjustment']}")

    # Load price history for all stocks (batch)
    price_rows = conn.execute(
        "SELECT symbol, date, open, high, low, close, volume "
        "FROM daily_prices ORDER BY symbol, date"
    ).fetchall()
    conn.close()

    if not price_rows:
        return {"date": latest_date, "count": 0, "stages": {}, "stocks": []}

    # Group by symbol
    all_df = pd.DataFrame([dict(r) for r in price_rows])
    for col in ["open", "high", "low", "close", "volume"]:
        all_df[col] = pd.to_numeric(all_df[col], errors="coerce")

    stocks = []
    for sym, group in all_df.groupby("symbol"):
        if sym not in filtered or len(group) < 30:
            continue

        df = group.sort_values("date").reset_index(drop=True)
        df = df.tail(60).reset_index(drop=True)

        try:
            ti = TechnicalIndicators(df)
            full_df = ti.compute_all()
            if len(full_df) < 5:
                continue
            ind = ti.get_latest_indicators()
        except Exception:
            continue

        close = ind.get("close") or 0
        volume = ind.get("volume") or 0
        vol_sma = ind.get("volume_sma_20") or 1

        # ── Extract indicators for display only ──
        rsi = ind.get("rsi_14") or 50
        mfi = ind.get("mfi_14") or 50
        cmf = ind.get("cmf_20") or 0
        macd_hist = ind.get("macd_histogram") or 0
        stoch_k = ind.get("stoch_k") or 50
        bb_upper = ind.get("bb_upper") or 0
        bb_lower = ind.get("bb_lower") or 0
        vol_ratio = ind.get("volume_ratio") or 0

        bb_range = bb_upper - bb_lower
        bb_pct = ((close - bb_lower) / bb_range * 100) if bb_range > 0 else 50

        # ════════════════════════════════════════
        #  LLM-POWERED RADAR
        #  Stage, score, and signals all come from Claude's analysis
        # ════════════════════════════════════════
        llm = llm_map.get(sym, {})
        judge = judge_map.get(sym, {})

        # Skip stocks with no LLM analysis
        if not llm and not judge:
            continue

        # Use judge final_action if available, else LLM action
        ai_action = judge.get("final_action") or llm.get("action") or ""
        ai_confidence = (judge.get("final_confidence") or
                         llm.get("confidence") or "")
        ai_reasoning_text = llm.get("reasoning") or ""
        ai_wait_for = llm.get("wait_for") or ""
        ai_how_to_buy = llm.get("how_to_buy") or ""
        ai_key_risk = judge.get("key_risk") or ""
        ai_catalysts = llm.get("catalysts") or []
        ai_risk_factors = llm.get("risk_factors") or []

        # Overall score comes directly from LLM (judge overrides if available)
        overall = float(judge.get("score") or llm.get("score") or 0)

        # Stage comes directly from LLM
        action_upper = ai_action.upper()
        llm_stage = (llm.get("stage") or "").upper().replace(" ", "_")
        valid_stages = {"ENTRY_ZONE", "READY", "APPROACHING", "BUILDING", "WATCHING", "TOO_LATE"}

        # Check recent rally from price history
        prices_close = df["close"].tolist()
        ret_5d_pct = 0
        if len(prices_close) >= 6:
            p5 = prices_close[-6]
            if p5 > 0:
                ret_5d_pct = (close / p5 - 1) * 100

        if llm_stage in valid_stages:
            if llm_stage == "TOO_LATE":
                stage = "WATCHING"
            elif llm_stage in ("ENTRY_ZONE", "READY"):
                # Sanity check: demote if score is low or stock already rallied
                if overall < 40 and ret_5d_pct > 5:
                    stage = "APPROACHING"  # Already moved, not ready
                elif overall < 30:
                    stage = "APPROACHING"  # Score too low for READY
                elif "HOLD" in action_upper or "WAIT" in action_upper:
                    stage = "APPROACHING"  # LLM action contradicts READY
                else:
                    stage = llm_stage
            else:
                stage = llm_stage
        else:
            # No valid stage from LLM — infer from action/score
            if overall >= 60 and "BUY" in action_upper:
                stage = "ENTRY_ZONE"
            elif overall >= 45 and "BUY" in action_upper:
                stage = "READY"
            elif overall >= 35 and "BUY" in action_upper:
                stage = "APPROACHING"
            elif overall >= 25:
                stage = "BUILDING"
            else:
                stage = "WATCHING"

        # Red flags from LLM risk_factors
        red_flags = []
        if isinstance(ai_risk_factors, list):
            red_flags = ai_risk_factors[:5]
        if "SELL" in action_upper or "AVOID" in action_upper:
            red_flags.append(f"AI says {ai_action}")

        # Signals from LLM catalysts
        all_signals = []
        if isinstance(ai_catalysts, list):
            all_signals = ai_catalysts[:4]
        all_signals.append(f"AI: {ai_action} ({ai_confidence})")
        if llm.get("wait_days"):
            all_signals.append(f"Wait: {llm['wait_days']}")

        # Entry/exit: prefer judge > LLM > algo
        algo_data = analysis_map.get(sym, {})
        ai_entry_low = (judge.get("entry_low") or llm.get("entry_low") or
                        algo_data.get("entry_low"))
        ai_entry_high = (judge.get("entry_high") or llm.get("entry_high") or
                         algo_data.get("entry_high"))
        ai_sl = judge.get("sl") or llm.get("sl") or algo_data.get("sl")
        ai_t1 = judge.get("t1") or llm.get("t1") or algo_data.get("t1")
        ai_t2 = judge.get("t2") or llm.get("t2") or algo_data.get("t2")

        ready_count = sum(1 for v in [
            llm.get("stage") in ("ENTRY_ZONE", "READY"),
            "BUY" in action_upper and "AVOID" not in action_upper,
            ai_confidence and ai_confidence.upper() == "HIGH",
            judge.get("agreement"),
            overall >= 40,
        ] if v)

        # ── Build response object ──
        ret_5d = round(ret_5d_pct, 1)

        a = analysis_map.get(sym, {})

        stocks.append({
            "symbol": sym,
            "price": round(close, 1),
            "sector": filtered[sym][:25],
            "category": cat_map.get(sym, "A"),
            "stage": stage,
            "overall_readiness": round(overall, 0),
            "ready_count": ready_count,
            "ret_5d": ret_5d,
            "volume": int(volume),
            "vol_ratio": round(vol_ratio, 1),
            "indicators": {
                "rsi":       {"value": round(rsi, 1),       "readiness": round(overall, 0)},
                "mfi":       {"value": round(mfi, 1),       "readiness": round(overall, 0)},
                "cmf":       {"value": round(cmf, 3),       "readiness": round(overall, 0)},
                "macd":      {"value": round(macd_hist, 2), "readiness": round(overall, 0)},
                "stoch_rsi": {"value": round(stoch_k, 1),   "readiness": round(overall, 0)},
                "bb_pct":    {"value": round(bb_pct, 1),    "readiness": round(overall, 0)},
            },
            # Layer scores — all driven by AI now
            "layers": {
                "leading":    round(overall, 0),
                "confirming": round(overall, 0),
                "money_flow": round(overall, 0),
                "positioning": round(overall, 0),
                "ai_verdict": round(overall, 0),
            },
            "signals": all_signals[:6],
            "red_flags": red_flags,
            # Entry/exit from AI (judge > LLM > algo)
            "entry_low": ai_entry_low,
            "entry_high": ai_entry_high,
            "sl": ai_sl,
            "t1": ai_t1,
            "t2": ai_t2,
            "action": ai_action,
            "score": overall,
            # AI context fields
            "ai_action": ai_action,
            "ai_confidence": ai_confidence,
            "ai_reasoning": ai_reasoning_text,
            "ai_how_to_buy": ai_how_to_buy,
            "ai_key_risk": ai_key_risk,
            "ai_wait_for": ai_wait_for,
            "ai_catalysts": ai_catalysts if isinstance(ai_catalysts, list) else [],
            "ai_risk_factors": ai_risk_factors if isinstance(ai_risk_factors, list) else [],
            "ai_signals": [f"AI: {ai_action}"],
            "stage_reasoning": llm.get("stage_reasoning") or "",
            # Profit estimation
            "expected_return_1w": llm.get("expected_return_1w"),
            "expected_return_2w": llm.get("expected_return_2w"),
            "expected_return_1m": llm.get("expected_return_1m"),
            "downside_risk": llm.get("downside_risk"),
            # DSEX analysis
            "dsex_dependency": llm.get("dsex_dependency") or "",
            "if_dsex_drops": llm.get("if_dsex_drops") or "",
            "if_dsex_rises": llm.get("if_dsex_rises") or "",
            "dsex_outlook": llm.get("dsex_outlook") or "",
        })

    # ── Save today's snapshots & load history ──
    stage_order = {"ENTRY_ZONE": 0, "READY": 1, "APPROACHING": 2, "BUILDING": 3, "WATCHING": 4}
    today_symbols = {s["symbol"] for s in stocks}

    conn2 = get_connection()
    try:
        # Save snapshots for today (upsert)
        for s in stocks:
            ind_json = json.dumps(s["indicators"])
            conn2.execute(
                """INSERT INTO radar_snapshots (date, symbol, stage, readiness, ready_count, price, indicators_json)
                   VALUES (?, ?, ?, ?, ?, ?, ?)
                   ON CONFLICT (symbol, date) DO UPDATE SET
                     stage = EXCLUDED.stage, readiness = EXCLUDED.readiness,
                     ready_count = EXCLUDED.ready_count, price = EXCLUDED.price,
                     indicators_json = EXCLUDED.indicators_json""",
                (latest_date, s["symbol"], s["stage"], s["overall_readiness"],
                 s["ready_count"], s["price"], ind_json),
            )
        conn2.execute("COMMIT")

        # Load last 10 days of history for all current symbols
        hist_rows = conn2.execute(
            """SELECT symbol, date, stage, readiness, price
               FROM radar_snapshots
               WHERE date >= (CURRENT_DATE - INTERVAL '10 days')
               ORDER BY symbol, date""",
        ).fetchall()

        # Detect removed stocks: in yesterday's snapshot but not in today's
        yesterday_rows = conn2.execute(
            """SELECT symbol, stage, readiness, price
               FROM radar_snapshots
               WHERE date = (SELECT MAX(date) FROM radar_snapshots WHERE date < ?)""",
            (latest_date,),
        ).fetchall()

        # Prune old snapshots (>30 days)
        conn2.execute("DELETE FROM radar_snapshots WHERE date < CURRENT_DATE - INTERVAL '30 days'")
        conn2.execute("COMMIT")
    except Exception as e:
        logger.error(f"Radar snapshot error: {e}")
        hist_rows = []
        yesterday_rows = []
    finally:
        conn2.close()

    # Build history lookup: {symbol: [{date, stage, readiness, price}, ...]}
    history: dict[str, list[dict]] = {}
    for r in hist_rows:
        sym = r["symbol"]
        if sym not in history:
            history[sym] = []
        history[sym].append({
            "date": str(r["date"]),
            "stage": r["stage"],
            "readiness": float(r["readiness"] or 0),
            "price": float(r["price"] or 0),
        })

    # Enrich each stock with tracking data
    for s in stocks:
        sym = s["symbol"]
        hist = history.get(sym, [])

        if len(hist) <= 1:
            # First time on radar
            s["is_new"] = True
            s["days_on_radar"] = 1
            s["first_seen"] = latest_date
            s["entry_price"] = s["price"]
            s["price_change_pct"] = 0
            s["stage_history"] = [s["stage"]]
            s["trend"] = "STABLE"
        else:
            s["is_new"] = False
            s["days_on_radar"] = len(hist)
            s["first_seen"] = hist[0]["date"]
            s["entry_price"] = hist[0]["price"]
            entry_p = hist[0]["price"]
            s["price_change_pct"] = round(
                ((s["price"] - entry_p) / entry_p * 100) if entry_p > 0 else 0, 1
            )
            s["stage_history"] = [h["stage"] for h in hist]

            # Trend: compare first vs current stage + readiness
            first_ord = stage_order.get(hist[0]["stage"], 4)
            curr_ord = stage_order.get(s["stage"], 4)
            readiness_delta = s["overall_readiness"] - hist[0]["readiness"]

            if curr_ord < first_ord or readiness_delta >= 10:
                s["trend"] = "IMPROVING"
            elif curr_ord > first_ord or readiness_delta <= -10:
                s["trend"] = "DETERIORATING"
            else:
                s["trend"] = "STABLE"

    # Build removed stocks list
    removed = []
    yesterday_map = {r["symbol"]: dict(r) for r in yesterday_rows}
    for sym, info in yesterday_map.items():
        if sym not in today_symbols:
            # Determine reason
            reason = "Lost momentum"
            removed.append({
                "symbol": sym,
                "last_stage": info["stage"],
                "last_price": float(info["price"] or 0),
                "last_readiness": float(info["readiness"] or 0),
                "reason": reason,
                "removed_date": latest_date,
                "days_tracked": len(history.get(sym, [])),
            })

    # Sort stocks: stage priority, then overall readiness desc
    stocks.sort(key=lambda s: (stage_order.get(s["stage"], 5), -s["overall_readiness"]))

    # Count per stage
    stage_counts: dict[str, int] = {}
    for s in stocks:
        stage_counts[s["stage"]] = stage_counts.get(s["stage"], 0) + 1

    # Load DSEX forecast if available
    dsex_forecast = None
    try:
        fc_conn = get_connection()
        fc_row = fc_conn.execute(
            "SELECT forecast, sentiment, support, resistance, expected_direction, "
            "confidence, key_factors, scenario_bull, scenario_bear, scenario_base "
            "FROM dsex_forecast ORDER BY date DESC LIMIT 1"
        ).fetchone()
        fc_conn.close()
        if fc_row:
            dsex_forecast = dict(fc_row)
    except Exception:
        pass

    result = {
        "date": latest_date,
        "count": len(stocks),
        "stages": stage_counts,
        "market_ctx": market_ctx,
        "dsex_forecast": dsex_forecast,
        "stocks": stocks,
        "removed": removed,
    }

    cache.set(cache_key, result, ttl=1800)  # 30 min cache
    return result
