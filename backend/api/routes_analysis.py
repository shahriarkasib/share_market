"""Daily analysis API routes."""

import io
import json
import logging
import math
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
from config import MARKET_DAYS
from database import get_connection

logger = logging.getLogger(__name__)
router = APIRouter()
DSE_TZ = pytz.timezone("Asia/Dhaka")


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

    results = load_daily_analysis(date_str=date, action_filter=action)
    if not results:
        return {"date": date, "count": 0, "analysis": [], "message": "No analysis for this date"}

    # Group by action
    grouped = {}
    for r in results:
        act = r.get("action", "UNKNOWN")
        grouped.setdefault(act, []).append(r)

    return {
        "date": date,
        "count": len(results),
        "summary": {k: len(v) for k, v in grouped.items()},
        "analysis": results,
    }


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
    conn = get_connection()

    # Use provided date or latest available
    if not date:
        row = conn.execute(
            "SELECT MAX(date) FROM daily_analysis"
        ).fetchone()
        date = str(row[0]) if row and row[0] else datetime.now(DSE_TZ).strftime("%Y-%m-%d")

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

    return {
        "date": date,
        "market_status": _is_market_open(),
        "updated_at": updated_at,
        "count": len(stocks),
        "stocks": stocks,
    }
