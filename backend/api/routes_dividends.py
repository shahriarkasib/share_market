"""Dividend / record-date impact analysis API routes."""

import logging

from fastapi import APIRouter, Query

from analysis.dividend_analyzer import (
    analyze_record_date_impact,
    find_post_dividend_opportunities,
    get_upcoming_record_dates,
)
from data.cache import cache

logger = logging.getLogger(__name__)
router = APIRouter()


@router.get("/impact/{symbol}")
async def record_date_impact(symbol: str):
    """Historical price behavior around record dates for a stock."""
    key = f"div_impact:{symbol.upper()}"
    cached = cache.get(key)
    if cached:
        return cached
    result = analyze_record_date_impact(symbol)
    cache.set(key, result, 1800)
    return result


@router.get("/opportunities")
async def post_dividend_opportunities(days: int = Query(default=7)):
    """Find post-dividend buying opportunities (oversold + accumulation)."""
    key = f"div_opps:{days}"
    cached = cache.get(key)
    if cached:
        return cached
    result = {"opportunities": find_post_dividend_opportunities(days)}
    cache.set(key, result, 1800)
    return result


@router.get("/upcoming")
async def upcoming_records(days: int = Query(default=60)):
    """Upcoming record dates enriched with price and historical drop data."""
    return {"upcoming": get_upcoming_record_dates(days)}
