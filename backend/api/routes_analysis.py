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
