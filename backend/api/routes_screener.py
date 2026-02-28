"""Stock screener API routes."""

from fastapi import APIRouter, Query
from typing import Optional
from api.routes_signals import _get_signals

router = APIRouter()


@router.get("")
async def screen_stocks(
    rsi_min: Optional[float] = Query(None),
    rsi_max: Optional[float] = Query(None),
    volume_min: Optional[float] = Query(None, description="Min volume ratio vs 20-day avg"),
    price_min: Optional[float] = Query(None),
    price_max: Optional[float] = Query(None),
    signal_type: Optional[str] = Query(None, description="BUY, SELL, STRONG_BUY, etc."),
    t2_safe: Optional[bool] = Query(None, description="Filter by T+2 safety"),
    min_expected_return: Optional[float] = Query(None, description="Min expected return %"),
    max_risk_score: Optional[float] = Query(None, description="Max risk score 0-100"),
    trend: Optional[str] = Query(None, description="STRONG_UP, UP, SIDEWAYS, DOWN, STRONG_DOWN"),
    max_hold_days: Optional[int] = Query(None, description="Max recommended hold days"),
    sort_by: str = Query("confidence", description="confidence, short_term_score, ltp, change_pct, risk_reward, rsi, expected_return, risk_score"),
    limit: int = Query(50, ge=1, le=200),
):
    """Screen stocks based on technical filters and T+2 prediction data."""
    signals = _get_signals()

    filtered = []
    for s in signals:
        indicators = s.get("indicators", {})

        # RSI filter
        rsi = indicators.get("rsi")
        if rsi_min is not None and (rsi is None or rsi < rsi_min):
            continue
        if rsi_max is not None and (rsi is None or rsi > rsi_max):
            continue

        # Volume filter
        vol = indicators.get("volume_signal")
        if volume_min is not None:
            vol_label_to_ratio = {
                "SURGE": 3.0, "HIGH": 1.5, "NORMAL": 1.0,
                "LOW": 0.5, "VERY_LOW": 0.2, "UNKNOWN": 0,
            }
            est_ratio = vol_label_to_ratio.get(vol, 0)
            if est_ratio < volume_min:
                continue

        # Price filter
        ltp = s.get("ltp", 0)
        if price_min is not None and ltp < price_min:
            continue
        if price_max is not None and ltp > price_max:
            continue

        # Signal type filter
        if signal_type is not None and s["signal_type"] != signal_type.upper():
            continue

        # T+2 safety filter
        if t2_safe is not None and s.get("t2_safe", False) != t2_safe:
            continue

        # Expected return filter
        if min_expected_return is not None:
            if s.get("expected_return_pct", 0) < min_expected_return:
                continue

        # Risk score filter
        if max_risk_score is not None:
            if s.get("risk_score", 50) > max_risk_score:
                continue

        # Trend filter
        if trend is not None and s.get("trend_strength", "SIDEWAYS") != trend.upper():
            continue

        # Hold days filter
        if max_hold_days is not None:
            if s.get("hold_days", 0) > max_hold_days:
                continue

        filtered.append(s)

    # Sort
    sort_map = {
        "confidence": ("confidence", True),
        "short_term_score": ("short_term_score", True),
        "ltp": ("ltp", True),
        "change_pct": ("change_pct", True),
        "risk_reward": ("risk_reward_ratio", True),
        "expected_return": ("expected_return_pct", True),
        "risk_score": ("risk_score", False),  # Lower risk = better
    }
    if sort_by == "rsi":
        filtered.sort(key=lambda x: (x.get("indicators", {}).get("rsi") or 0), reverse=True)
    elif sort_by in sort_map:
        key, rev = sort_map[sort_by]
        filtered.sort(key=lambda x: x.get(key, 0), reverse=rev)

    return {
        "stocks": filtered[:limit],
        "total_count": len(filtered),
        "filters_applied": {
            "rsi_min": rsi_min, "rsi_max": rsi_max,
            "volume_min": volume_min,
            "price_min": price_min, "price_max": price_max,
            "signal_type": signal_type,
            "t2_safe": t2_safe,
            "min_expected_return": min_expected_return,
            "max_risk_score": max_risk_score,
            "trend": trend,
            "max_hold_days": max_hold_days,
        },
    }
