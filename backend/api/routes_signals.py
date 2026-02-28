"""Trading signals API routes - adapter over daily_analysis table."""

import json
import math
import logging
from datetime import datetime, date
from fastapi import APIRouter

from analysis.daily_report import load_daily_analysis
from data.cache import cache
from data.repository import (
    get_active_holdings,
    save_signal_history,
    backfill_signal_accuracy,
    get_signal_history_for_symbol,
    get_signal_accuracy_report,
)
from config import CACHE_TTL_SIGNALS
from database import get_connection

logger = logging.getLogger(__name__)

router = APIRouter()

# ── Action mapping from daily_analysis actions to signal types ──

_ACTION_MAP = {
    "BUY (strong)": "STRONG_BUY",
    "BUY": "BUY",
    "BUY on pullback": "BUY",
    "BUY on dip": "BUY",
    "BUY (wait for MACD cross)": "BUY",
    "HOLD/WAIT": "HOLD",
    "SELL/AVOID": "SELL",
    "AVOID": "STRONG_SELL",
}


# ── Adapter: daily_analysis row -> StockSignal shape ──


def _safe_float(val, default=0.0):
    """Return float or default if None/NaN/Inf."""
    if val is None:
        return default
    try:
        f = float(val)
        return default if math.isnan(f) or math.isinf(f) else f
    except (ValueError, TypeError):
        return default


def _analysis_to_signal(a: dict, live_map: dict) -> dict:
    """Map a daily_analysis row to the StockSignal dict shape."""
    symbol = a["symbol"]
    action = a.get("action", "HOLD/WAIT")
    signal_type = _ACTION_MAP.get(action, "HOLD")

    score = _safe_float(a.get("score"), 0)
    confidence = min(score / 100.0, 1.0) if score > 0 else 0.0

    entry_low = _safe_float(a.get("entry_low"))
    entry_high = _safe_float(a.get("entry_high"))
    sl = _safe_float(a.get("sl"))
    t1 = _safe_float(a.get("t1"))
    t2 = _safe_float(a.get("t2"))
    vol_entry = _safe_float(a.get("vol_entry"))

    risk_pct = _safe_float(a.get("risk_pct"))
    reward_pct = _safe_float(a.get("reward_pct"))
    risk_reward = round(reward_pct / risk_pct, 2) if risk_pct > 0 else 0.0

    # LTP: prefer live, fall back to analysis row
    live = live_map.get(symbol, {})
    ltp = _safe_float(live.get("ltp")) or _safe_float(a.get("ltp"))
    change_pct = _safe_float(live.get("change_pct"))
    company_name = live.get("company_name") or symbol

    # Timing
    if signal_type in ("STRONG_BUY", "BUY"):
        if entry_high > 0 and ltp <= entry_high:
            timing = "BUY_NOW"
        elif entry_low > 0 and ltp < entry_low:
            timing = "WAIT_FOR_DIP"
        else:
            timing = "ACCUMULATE"
    elif signal_type in ("SELL", "STRONG_SELL"):
        timing = "SELL_NOW"
    else:
        timing = "HOLD_TIGHT"

    # Indicators
    rsi = a.get("rsi")
    macd_status = a.get("macd_status")
    bb_pct = _safe_float(a.get("bb_pct"), 50)
    vol_ratio = _safe_float(a.get("vol_ratio"), 1.0)
    stoch_rsi = a.get("stoch_rsi")

    if bb_pct > 80:
        bb_position = "UPPER"
    elif bb_pct < 20:
        bb_position = "LOWER"
    else:
        bb_position = "MIDDLE"

    if vol_ratio > 2:
        volume_signal = "SURGE"
    elif vol_ratio > 1.5:
        volume_signal = "HIGH"
    elif vol_ratio > 0.8:
        volume_signal = "NORMAL"
    elif vol_ratio > 0.5:
        volume_signal = "LOW"
    else:
        volume_signal = "VERY_LOW"

    indicators = {
        "rsi": rsi,
        "macd_signal": macd_status,
        "bb_position": bb_position,
        "volume_signal": volume_signal,
        "momentum_3d": None,
        "stoch_k": stoch_rsi,
    }

    # Prediction data
    pred = a.get("prediction_json")
    if not isinstance(pred, dict):
        pred = {}

    predicted_prices = pred.get("predicted_prices", {})
    daily_ranges = pred.get("daily_ranges", {})
    price_range_next_3d = pred.get("price_range_next_3d", {})
    support_level = _safe_float(pred.get("support_level")) or _safe_float(a.get("support"))
    resistance_level = _safe_float(pred.get("resistance_level")) or _safe_float(a.get("resistance"))
    t2_safe = pred.get("t2_safe", False)
    risk_score = _safe_float(pred.get("risk_score"), 50)
    expected_return_pct = _safe_float(pred.get("expected_return_pct"))

    # trend_strength
    if "trend_strength" in pred:
        trend_strength = pred["trend_strength"]
    else:
        trend_50d = _safe_float(a.get("trend_50d"))
        if trend_50d > 10:
            trend_strength = "STRONG_UP"
        elif trend_50d > 3:
            trend_strength = "UP"
        elif trend_50d > -3:
            trend_strength = "SIDEWAYS"
        elif trend_50d > -10:
            trend_strength = "DOWN"
        else:
            trend_strength = "STRONG_DOWN"

    # volatility_level
    if "volatility_level" in pred:
        volatility_level = pred["volatility_level"]
    else:
        volatility = _safe_float(a.get("volatility"), 2.0)
        if volatility < 1.5:
            volatility_level = "LOW"
        elif volatility < 3:
            volatility_level = "MEDIUM"
        else:
            volatility_level = "HIGH"

    # hold_days
    if "hold_days" in pred:
        hold_days = pred["hold_days"]
    else:
        hold_days = a.get("hold_days_t1") or 0

    # entry_strategy
    if "entry_strategy" in pred:
        entry_strategy = pred["entry_strategy"]
    else:
        parts = []
        if entry_low > 0:
            parts.append(f"Entry: {entry_low:.1f}-{entry_high:.1f}")
        if vol_entry > 0:
            parts.append(f"Vol entry: {vol_entry:.0f}")
        entry_strategy = " | ".join(parts) if parts else ""

    # exit_strategy
    if "exit_strategy" in pred:
        exit_strategy = pred["exit_strategy"]
    else:
        parts = []
        if t1 > 0:
            parts.append(f"T1: {t1:.1f}")
        if t2 > 0:
            parts.append(f"T2: {t2:.1f}")
        if sl > 0:
            parts.append(f"SL: {sl:.1f}")
        exit_strategy = " | ".join(parts) if parts else ""

    return {
        "symbol": symbol,
        "company_name": company_name,
        "ltp": ltp,
        "change_pct": change_pct,
        "action": action,
        "signal_type": signal_type,
        "confidence": round(confidence, 3),
        "short_term_score": score,
        "long_term_score": round(score * 0.8, 1),
        "target_price": t1,
        "stop_loss": sl,
        "risk_reward_ratio": risk_reward,
        "reasoning": a.get("reasoning", ""),
        "timing": timing,
        "indicators": indicators,
        "predicted_prices": predicted_prices,
        "daily_ranges": daily_ranges,
        "price_range_next_3d": price_range_next_3d,
        "support_level": support_level,
        "resistance_level": resistance_level,
        "trend_strength": trend_strength,
        "volatility_level": volatility_level,
        "t2_safe": t2_safe,
        "risk_score": risk_score,
        "expected_return_pct": expected_return_pct,
        "hold_days": hold_days,
        "entry_strategy": entry_strategy,
        "exit_strategy": exit_strategy,
        "created_at": datetime.now().isoformat(),
    }


# ── Data loading ──


def _get_signals() -> list:
    """Get signals from cache, falling back to daily_analysis."""
    cached = cache.get("all_signals")
    if cached is not None:
        return cached

    analysis = load_daily_analysis()  # Latest date, all actions
    if not analysis:
        return []

    # Enrich with live prices
    conn = get_connection()
    live_rows = conn.execute(
        "SELECT symbol, ltp, change_pct, company_name FROM live_prices"
    ).fetchall()
    conn.close()
    live_map = {r["symbol"]: dict(r) for r in live_rows}

    signals = [_analysis_to_signal(a, live_map) for a in analysis]
    cache.set("all_signals", signals, CACHE_TTL_SIGNALS * 2)
    return signals


# ── Endpoints ──


@router.get("/top")
async def get_top_signals(type: str = "all", limit: int = 20):
    """Get top-ranked trading signals. Returns cached/DB results instantly."""
    signals = _get_signals()

    if type == "buy":
        signals = [
            s for s in signals if s["signal_type"] in ("STRONG_BUY", "BUY")
        ]
        signals.sort(key=lambda x: x["short_term_score"], reverse=True)
    elif type == "sell":
        signals = [
            s for s in signals if s["signal_type"] in ("STRONG_SELL", "SELL")
        ]
        signals.sort(key=lambda x: x["short_term_score"])
    else:
        signals.sort(key=lambda x: abs(x["short_term_score"]), reverse=True)

    return signals[:limit]


@router.get("/summary")
async def get_signals_summary():
    """Get overall market signal summary."""
    cached = cache.get("signals_summary")
    if cached:
        return cached

    signals = _get_signals()

    strong_buy = sum(1 for s in signals if s["signal_type"] == "STRONG_BUY")
    buy = sum(1 for s in signals if s["signal_type"] == "BUY")
    hold = sum(1 for s in signals if s["signal_type"] == "HOLD")
    sell = sum(1 for s in signals if s["signal_type"] == "SELL")
    strong_sell = sum(1 for s in signals if s["signal_type"] == "STRONG_SELL")

    total = len(signals)
    bullish = strong_buy + buy
    bearish = sell + strong_sell

    if total > 0:
        if bullish / total > 0.5:
            sentiment = "BULLISH"
        elif bearish / total > 0.5:
            sentiment = "BEARISH"
        else:
            sentiment = "NEUTRAL"
    else:
        sentiment = "NEUTRAL"

    result = {
        "total_stocks": total,
        "strong_buy_count": strong_buy,
        "buy_count": buy,
        "hold_count": hold,
        "sell_count": sell,
        "strong_sell_count": strong_sell,
        "market_sentiment": sentiment,
        "last_updated": datetime.now().isoformat(),
        "is_computing": False,
    }

    if total > 0:
        cache.set("signals_summary", result, CACHE_TTL_SIGNALS)
    return result


@router.get("/status")
async def get_computation_status():
    """Check signal status (no background computation needed)."""
    signals = _get_signals()
    return {
        "is_computing": False,
        "total_signals": len(signals),
        "last_computed": None,
    }


@router.get("/suggestions")
async def get_suggestions():
    """Get top 5 entry picks (not in portfolio) and exit alerts for portfolio holdings."""
    cached = cache.get("suggestions")
    if cached:
        return cached

    signals = _get_signals()
    if not signals:
        return {"entry": [], "exit": []}

    # ---- Entry suggestions ----
    holdings = get_active_holdings()
    portfolio_symbols = {h["symbol"] for h in holdings}

    entry_candidates = []
    for s in signals:
        if s["signal_type"] not in ("STRONG_BUY", "BUY"):
            continue
        if s["symbol"] in portfolio_symbols:
            continue

        # Composite score: short_term_score * confidence * safety_bonus
        score_base = s.get("short_term_score", 0) or 0
        confidence = s.get("confidence", 0) or 0
        risk = s.get("risk_score", 50) or 50
        t2_bonus = 1.5 if s.get("t2_safe", False) else 1.0
        score = score_base * confidence * (1 - risk / 200.0) * t2_bonus
        entry_candidates.append({**s, "_score": score})

    entry_candidates.sort(key=lambda x: x["_score"], reverse=True)
    entry = []
    for c in entry_candidates[:5]:
        c.pop("_score", None)
        entry.append(c)

    # ---- Exit suggestions ----
    exit_alerts = []
    signals_by_symbol = {s["symbol"]: s for s in signals}

    for h in holdings:
        symbol = h["symbol"]
        reasons = []
        signal = signals_by_symbol.get(symbol)

        # Check maturity
        mat_date = h.get("maturity_date", "")
        is_mature = False
        if mat_date:
            try:
                is_mature = date.fromisoformat(mat_date) <= date.today()
            except (ValueError, TypeError):
                pass

        if not is_mature:
            continue  # Can't sell yet (T+2 not passed)

        # Check signal
        if signal and signal["signal_type"] in ("SELL", "STRONG_SELL"):
            reasons.append(f"Signal: {signal['signal_type']}")

        # Check target reached
        if signal:
            current = signal.get("ltp", 0)
            target = signal.get("target_price", 0)
            stop = signal.get("stop_loss", 0)
            buy_price = h.get("buy_price", 0)

            if target > 0 and current >= target:
                reasons.append(f"Target reached ({current:.1f} >= {target:.1f})")

            if stop > 0 and current <= stop:
                reasons.append(f"Stop loss hit ({current:.1f} <= {stop:.1f})")

            # Calculate P&L
            if buy_price > 0 and current > 0:
                pnl_pct = (current - buy_price) / buy_price * 100
            else:
                pnl_pct = 0
        else:
            pnl_pct = 0

        if reasons:
            exit_alerts.append({
                "holding": h,
                "signal": signal,
                "reasons": reasons,
                "pnl_pct": round(pnl_pct, 2),
            })

    # Sort by number of reasons (more urgent first)
    exit_alerts.sort(key=lambda x: len(x["reasons"]), reverse=True)

    result = {"entry": entry, "exit": exit_alerts}
    cache.set("suggestions", result, CACHE_TTL_SIGNALS)
    return result


@router.get("/accuracy")
async def get_accuracy():
    """Get signal accuracy report -- how well did past predictions perform?"""
    cached = cache.get("signal_accuracy")
    if cached:
        return cached

    report = get_signal_accuracy_report()
    if report.get("total_verified", 0) > 0:
        cache.set("signal_accuracy", report, CACHE_TTL_SIGNALS)
    return report


@router.get("/history/{symbol}")
async def get_signal_history(symbol: str, limit: int = 30):
    """Get historical signal decisions for a specific stock."""
    return get_signal_history_for_symbol(symbol, limit)


@router.get("/{symbol}")
async def get_stock_signal(symbol: str):
    """Get detailed signal for a specific stock."""
    symbol = symbol.upper()

    all_signals = _get_signals()
    for s in all_signals:
        if s["symbol"] == symbol:
            return s

    # Not found in daily analysis -- return minimal HOLD
    return {
        "symbol": symbol,
        "signal_type": "HOLD",
        "reasoning": "No analysis available for this stock",
        "confidence": 0,
        "short_term_score": 0,
        "long_term_score": 0,
        "timing": "HOLD_TIGHT",
        "indicators": {},
    }
