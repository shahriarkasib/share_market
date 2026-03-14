"""API routes for floor detection — indicator-floor approach."""

from typing import Optional

from fastapi import APIRouter, Query

from analysis.floor_detector import compute_floor_table, get_available_dates

router = APIRouter()


@router.get("")
async def floor_table(
    months: int = Query(default=6, ge=1, le=24),
    as_of: Optional[str] = Query(default=None, description="ISO date for historical replay"),
):
    """Floor detection table for all A-category stocks."""
    result = compute_floor_table(lookback_months=months, as_of_date=as_of)
    return {"stocks": result, "lookback_months": months, "as_of": as_of}


@router.get("/dates")
async def available_dates():
    """Available trading dates for historical replay."""
    return {"dates": get_available_dates()}
