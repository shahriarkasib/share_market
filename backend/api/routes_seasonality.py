"""Seasonality API routes.

Exposes monthly/weekly historical performance patterns for sectors and
stocks, plus a current-month outlook endpoint.
"""

from typing import Optional

from fastapi import APIRouter, Query

from analysis.seasonality import (
    month_outlook,
    monthly_sector_performance,
    monthly_stock_performance,
    sector_yearly_detail,
    stock_yearly_detail,
    weekly_performance,
)
from data.cache import cache

router = APIRouter()


@router.get("/monthly/sectors")
async def monthly_sectors(year: int = Query(default=0)):
    """Average return per sector for each calendar month. year=0 means overall."""
    yr = year if year >= 2010 else None
    key = f"seasonality_monthly_sectors_{yr or 'all'}"
    cached = cache.get(key)
    if cached:
        return cached
    result = monthly_sector_performance(year=yr)
    cache.set(key, result, 3600)
    return result


@router.get("/monthly/stocks")
async def monthly_stocks(
    category: str = Query(default="A"),
    year: int = Query(default=0),
    sector: Optional[str] = Query(default=None),
):
    """Per-stock monthly pattern. year=0 means overall. sector filters by sector."""
    yr = year if year >= 2010 else None
    key = f"seasonality_monthly_stocks_{category}_{yr or 'all'}_{sector or 'all'}"
    cached = cache.get(key)
    if cached:
        return cached
    result = monthly_stock_performance(category=category, year=yr, sector=sector)
    cache.set(key, result, 3600)
    return result


@router.get("/monthly/sectors/yearly")
async def sectors_yearly():
    """Per-sector per-year per-month returns for expandable heatmap."""
    cached = cache.get("seasonality_sectors_yearly")
    if cached:
        return cached
    result = sector_yearly_detail()
    cache.set("seasonality_sectors_yearly", result, 3600)
    return result


@router.get("/monthly/stocks/yearly")
async def stocks_yearly(category: str = Query(default="A")):
    """Per-stock per-year per-month returns for expandable stock patterns table."""
    key = f"seasonality_stocks_yearly_{category}"
    cached = cache.get(key)
    if cached:
        return cached
    result = stock_yearly_detail(category=category)
    cache.set(key, result, 3600)
    return result


@router.get("/weekly")
async def weekly(weeks: int = Query(default=12)):
    """Sector-level weekly returns for the last N weeks."""
    key = f"seasonality_weekly_{weeks}"
    cached = cache.get(key)
    if cached:
        return cached
    result = {"weeks": weekly_performance(weeks)}
    cache.set(key, result, 3600)
    return result


@router.get("/outlook")
async def outlook(month: int = Query(default=0, ge=0, le=12)):
    """Historical outlook for a calendar month (0 = current month)."""
    m = month if month >= 1 else None
    key = f"seasonality_outlook_{m or 'current'}"
    cached = cache.get(key)
    if cached:
        return cached
    result = month_outlook(m)
    cache.set(key, result, 3600)
    return result
