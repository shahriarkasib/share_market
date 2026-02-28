"""Daily analysis API routes."""

import io
import json
import logging
import threading
from datetime import datetime

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
from database import get_connection

logger = logging.getLogger(__name__)
router = APIRouter()
DSE_TZ = pytz.timezone("Asia/Dhaka")

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
