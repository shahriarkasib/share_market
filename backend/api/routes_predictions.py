"""Prediction tracker and LLM/Judge analysis API routes."""

import json
from datetime import datetime

import pytz
from fastapi import APIRouter, Query

from database import get_connection

DSE_TZ = pytz.timezone("Asia/Dhaka")

router = APIRouter()


@router.get("/tracker")
async def get_prediction_tracker(
    date: str = Query(default=None, description="Prediction date YYYY-MM-DD"),
    symbol: str = Query(default=None, description="Filter by symbol"),
    source: str = Query(default=None, description="algo, llm, or judge"),
    action: str = Query(default=None, description="Filter by predicted action"),
    outcome: str = Query(default=None, description="CORRECT, WRONG, PARTIAL, PENDING"),
    limit: int = Query(default=100, le=500),
):
    """Get prediction tracker entries with filters."""
    conn = get_connection()

    clauses = []
    params = []

    if date:
        clauses.append("pt.date = %s")
        params.append(date)
    if symbol:
        clauses.append("pt.symbol = %s")
        params.append(symbol.upper())
    if source:
        clauses.append("pt.source = %s")
        params.append(source)
    if action:
        clauses.append("pt.action LIKE %s")
        params.append(f"%{action}%")
    if outcome:
        clauses.append("pt.outcome = %s")
        params.append(outcome)

    where = ("WHERE " + " AND ".join(clauses)) if clauses else ""
    params.append(limit)

    rows = conn.execute(f"""
        SELECT pt.*, f.sector
        FROM prediction_tracker pt
        LEFT JOIN fundamentals f ON pt.symbol = f.symbol
        {where}
        ORDER BY pt.date DESC, pt.score DESC
        LIMIT %s
    """, tuple(params)).fetchall()
    conn.close()

    return {
        "count": len(rows),
        "predictions": [dict(r) for r in rows],
    }


@router.get("/accuracy")
async def get_accuracy_comparison(
    period: str = Query(default="30d", description="7d, 30d, or 90d"),
):
    """Compare algo vs LLM vs judge accuracy side-by-side."""
    conn = get_connection()
    rows = conn.execute("""
        SELECT * FROM accuracy_summary
        WHERE period = %s
        ORDER BY date DESC, source
        LIMIT 3
    """, (period,)).fetchall()
    conn.close()

    if not rows:
        return {
            "period": period,
            "data": [],
            "message": "No accuracy data yet. Run predictions for a few days first.",
        }

    return {
        "period": period,
        "date": str(rows[0]["date"]) if rows else None,
        "data": [dict(r) for r in rows],
    }


@router.get("/accuracy/history")
async def get_accuracy_history(
    source: str = Query(default=None, description="algo, llm, judge (all if empty)"),
    days: int = Query(default=30, le=90),
):
    """Get daily accuracy trend for charting."""
    conn = get_connection()

    if source:
        rows = conn.execute("""
            SELECT date, source, accuracy_pct, avg_return_pct,
                   buy_accuracy_pct, t1_hit_rate, sl_hit_rate, total_predictions
            FROM accuracy_summary
            WHERE source = %s AND period = '30d'
              AND date > CURRENT_DATE - INTERVAL '%s days'
            ORDER BY date
        """, (source, days)).fetchall()
    else:
        rows = conn.execute("""
            SELECT date, source, accuracy_pct, avg_return_pct,
                   buy_accuracy_pct, t1_hit_rate, sl_hit_rate, total_predictions
            FROM accuracy_summary
            WHERE period = '30d'
              AND date > CURRENT_DATE - INTERVAL '%s days'
            ORDER BY date, source
        """, (days,)).fetchall()

    conn.close()
    return {"days": days, "history": [dict(r) for r in rows]}


@router.get("/stock/{symbol}")
async def get_stock_prediction_history(
    symbol: str,
    limit: int = Query(default=30, le=100),
):
    """Get prediction history for a stock across all sources."""
    conn = get_connection()
    rows = conn.execute("""
        SELECT pt.*, f.sector
        FROM prediction_tracker pt
        LEFT JOIN fundamentals f ON pt.symbol = f.symbol
        WHERE pt.symbol = %s
        ORDER BY pt.date DESC, pt.source
        LIMIT %s
    """, (symbol.upper(), limit)).fetchall()
    conn.close()

    # Group by date
    by_date = {}
    for r in rows:
        d = str(r["date"])
        if d not in by_date:
            by_date[d] = {}
        by_date[d][r["source"]] = dict(r)

    return {
        "symbol": symbol.upper(),
        "count": len(rows),
        "by_date": by_date,
        "raw": [dict(r) for r in rows],
    }


@router.get("/llm-analysis")
async def get_llm_daily_analysis(
    date: str = Query(default=None, description="Date YYYY-MM-DD"),
    action: str = Query(default=None, description="Filter by action"),
    symbol: str = Query(default=None, description="Filter by symbol"),
):
    """Get LLM daily analysis results with judge verdicts."""
    if not date:
        date = datetime.now(DSE_TZ).strftime("%Y-%m-%d")

    conn = get_connection()

    # Check if we have LLM data for this date
    count_row = conn.execute(
        "SELECT COUNT(*) as cnt FROM llm_daily_analysis WHERE date = %s",
        (date,),
    ).fetchone()

    if not count_row or count_row["cnt"] == 0:
        conn.close()
        return {
            "date": date,
            "count": 0,
            "analysis": [],
            "message": "No LLM analysis for this date. Run llm_daily_analyzer.py on the GCP VM.",
        }

    clauses = ["la.date = %s"]
    params = [date]

    if action:
        clauses.append("la.action LIKE %s")
        params.append(f"%{action}%")
    if symbol:
        clauses.append("la.symbol = %s")
        params.append(symbol.upper())

    where = " AND ".join(clauses)

    rows = conn.execute(f"""
        SELECT la.symbol, la.action, la.confidence, la.reasoning, la.wait_for,
               la.wait_days, la.entry_low, la.entry_high, la.sl, la.t1, la.t2,
               la.risk_factors, la.catalysts, la.score,
               ja.algo_action, ja.llm_action, ja.final_action, ja.final_confidence,
               ja.agreement, ja.reasoning AS judge_reasoning,
               ja.algo_strengths, ja.llm_strengths, ja.key_risk,
               f.sector
        FROM llm_daily_analysis la
        LEFT JOIN judge_daily_analysis ja ON la.date = ja.date AND la.symbol = ja.symbol
        LEFT JOIN fundamentals f ON la.symbol = f.symbol
        WHERE {where}
        ORDER BY la.score DESC
    """, tuple(params)).fetchall()
    conn.close()

    results = []
    for r in rows:
        item = dict(r)
        # Parse JSON fields
        for field in ("risk_factors", "catalysts"):
            if item.get(field):
                try:
                    item[field] = json.loads(item[field])
                except (json.JSONDecodeError, TypeError):
                    pass
        results.append(item)

    return {
        "date": date,
        "count": len(results),
        "analysis": results,
    }


@router.get("/judge-analysis")
async def get_judge_analysis(
    date: str = Query(default=None, description="Date YYYY-MM-DD"),
    disagreement_only: bool = Query(default=False, description="Show only disagreements"),
):
    """Get judge verdicts comparing algo vs LLM."""
    if not date:
        date = datetime.now(DSE_TZ).strftime("%Y-%m-%d")

    conn = get_connection()

    extra = " AND ja.agreement = false" if disagreement_only else ""

    rows = conn.execute(f"""
        SELECT ja.*, f.sector
        FROM judge_daily_analysis ja
        LEFT JOIN fundamentals f ON ja.symbol = f.symbol
        WHERE ja.date = %s {extra}
        ORDER BY ja.score DESC
    """, (date,)).fetchall()
    conn.close()

    total = len(rows)
    agree = sum(1 for r in rows if r["agreement"])

    return {
        "date": date,
        "count": total,
        "agreements": agree,
        "disagreements": total - agree,
        "agreement_pct": round(agree / total * 100, 1) if total > 0 else 0,
        "verdicts": [dict(r) for r in rows],
    }


@router.get("/dates")
async def get_prediction_dates():
    """Get all dates that have prediction data."""
    conn = get_connection()
    rows = conn.execute("""
        SELECT DISTINCT date, source, COUNT(*) as count
        FROM prediction_tracker
        GROUP BY date, source
        ORDER BY date DESC
        LIMIT 90
    """).fetchall()
    conn.close()

    by_date = {}
    for r in rows:
        d = str(r["date"])
        if d not in by_date:
            by_date[d] = {}
        by_date[d][r["source"]] = r["count"]

    return {"dates": by_date}
