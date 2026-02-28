"""Portfolio holdings API — track buys, P&L, maturity, sell recommendations."""

import logging
from datetime import date, datetime
from fastapi import APIRouter, HTTPException
from pydantic import BaseModel
from typing import Optional

from database import get_connection
from data.repository import (
    insert_holding, get_active_holdings, get_holding_by_id,
    update_holding_sell, delete_holding,
)
from data.cache import cache
from analysis.t2_scorer import T2Scorer

logger = logging.getLogger(__name__)
router = APIRouter()
t2_scorer = T2Scorer()


# ---- Request models ----

class HoldingCreate(BaseModel):
    symbol: str
    quantity: int
    buy_price: float
    buy_date: str  # YYYY-MM-DD
    notes: Optional[str] = None


class HoldingSell(BaseModel):
    sell_price: float
    sell_date: str  # YYYY-MM-DD
    quantity: int


# ---- Helpers ----

def _get_live_price(symbol: str) -> float:
    """Get current LTP from live_prices table."""
    try:
        conn = get_connection()
        row = conn.execute(
            "SELECT ltp FROM live_prices WHERE symbol = ?", (symbol,)
        ).fetchone()
        conn.close()
        return row["ltp"] if row and row["ltp"] else 0
    except Exception:
        return 0


def _get_signal_for_symbol(symbol: str) -> dict | None:
    """Get cached signal for a symbol (from daily analysis adapter)."""
    from api.routes_signals import _get_signals
    all_signals = _get_signals()
    for s in all_signals:
        if s.get("symbol") == symbol:
            return s
    return None


def _enrich_holding(h: dict) -> dict:
    """Add live price, P&L, maturity status, and sell recommendation."""
    symbol = h["symbol"]
    current_price = _get_live_price(symbol)
    buy_price = h["buy_price"]
    quantity = h["quantity"] - (h.get("sell_quantity") or 0)

    unrealized_pnl = (current_price - buy_price) * quantity if current_price else 0
    unrealized_pnl_pct = (
        ((current_price - buy_price) / buy_price * 100) if buy_price and current_price else 0
    )

    maturity_date = h.get("maturity_date", "")
    today = date.today().isoformat()
    is_mature = maturity_date <= today if maturity_date else False

    # Generate sell recommendation
    signal = _get_signal_for_symbol(symbol)
    sell_rec = _sell_recommendation(is_mature, signal, current_price, buy_price, maturity_date)

    return {
        "id": h["id"],
        "symbol": symbol,
        "quantity": h["quantity"],
        "remaining_quantity": quantity,
        "buy_price": buy_price,
        "buy_date": h["buy_date"],
        "maturity_date": maturity_date,
        "is_mature": is_mature,
        "current_price": round(current_price, 2),
        "unrealized_pnl": round(unrealized_pnl, 2),
        "unrealized_pnl_pct": round(unrealized_pnl_pct, 2),
        "sell_recommendation": sell_rec,
        "signal_type": signal.get("signal_type") if signal else None,
        "status": h["status"],
        "notes": h.get("notes"),
    }


def _sell_recommendation(is_mature: bool, signal: dict | None,
                         current: float, buy_price: float,
                         maturity_date: str) -> str:
    """Generate actionable sell recommendation."""
    if not is_mature:
        return f"Matures on {maturity_date} — hold until then"

    if not signal:
        if current > buy_price:
            return "Mature — consider taking profit"
        return "Mature — monitor for exit"

    sig_type = signal.get("signal_type", "HOLD")
    target = signal.get("target_price", 0)
    stop = signal.get("stop_loss", 0)
    hold_days = signal.get("hold_days", 0)
    expected = signal.get("expected_return_pct", 0)

    if sig_type in ("SELL", "STRONG_SELL"):
        return "Sell now — sell signal active"

    if target and current >= target:
        return f"Target reached (৳{target:.1f}) — take profit"

    if stop and current <= stop:
        return f"Stop loss hit (৳{stop:.1f}) — cut losses"

    if sig_type in ("BUY", "STRONG_BUY") and expected > 0:
        return f"Hold — expected +{expected:.1f}% in {hold_days} days"

    if current > buy_price:
        pnl_pct = (current - buy_price) / buy_price * 100
        return f"Profitable (+{pnl_pct:.1f}%) — trail stop or hold"

    return "Hold — waiting for better exit"


# ---- Endpoints ----

@router.get("")
async def get_holdings():
    """Get all active holdings with P&L and recommendations."""
    holdings = get_active_holdings()
    return [_enrich_holding(h) for h in holdings]


@router.post("")
async def add_holding(req: HoldingCreate):
    """Record a new stock purchase."""
    try:
        buy_date = date.fromisoformat(req.buy_date)
    except ValueError:
        raise HTTPException(400, "Invalid buy_date format. Use YYYY-MM-DD")

    maturity = t2_scorer.compute_maturity_date(buy_date)
    holding_id = insert_holding(
        symbol=req.symbol,
        quantity=req.quantity,
        buy_price=req.buy_price,
        buy_date=req.buy_date,
        maturity_date=maturity.isoformat(),
        notes=req.notes,
    )

    return {
        "id": holding_id,
        "symbol": req.symbol.upper(),
        "quantity": req.quantity,
        "buy_price": req.buy_price,
        "buy_date": req.buy_date,
        "maturity_date": maturity.isoformat(),
        "message": f"Holding recorded. Matures on {maturity.isoformat()}",
    }


@router.post("/{holding_id}/sell")
async def record_sell(holding_id: int, req: HoldingSell):
    """Record a (partial or full) sale."""
    holding = get_holding_by_id(holding_id)
    if not holding:
        raise HTTPException(404, "Holding not found")

    remaining = holding["quantity"] - (holding.get("sell_quantity") or 0)
    if req.quantity > remaining:
        raise HTTPException(400, f"Cannot sell {req.quantity}, only {remaining} remaining")

    update_holding_sell(holding_id, req.sell_price, req.sell_date, req.quantity)

    pnl = (req.sell_price - holding["buy_price"]) * req.quantity
    return {
        "holding_id": holding_id,
        "sold_quantity": req.quantity,
        "sell_price": req.sell_price,
        "realized_pnl": round(pnl, 2),
        "message": f"Sold {req.quantity} shares for ৳{pnl:+.2f} P&L",
    }


@router.get("/summary")
async def get_portfolio_summary():
    """Portfolio summary: total invested, current value, P&L."""
    holdings = get_active_holdings()
    today = date.today().isoformat()

    total_invested = 0.0
    current_value = 0.0
    mature_count = 0
    at_risk = 0

    for h in holdings:
        qty = h["quantity"] - (h.get("sell_quantity") or 0)
        if qty <= 0:
            continue
        total_invested += h["buy_price"] * qty
        ltp = _get_live_price(h["symbol"])
        current_value += ltp * qty

        if h.get("maturity_date", "") <= today:
            mature_count += 1

        # Check if stop loss is hit
        signal = _get_signal_for_symbol(h["symbol"])
        if signal and ltp <= signal.get("stop_loss", 0):
            at_risk += 1

    total_pnl = current_value - total_invested
    total_pnl_pct = (total_pnl / total_invested * 100) if total_invested else 0

    return {
        "total_invested": round(total_invested, 2),
        "current_value": round(current_value, 2),
        "total_pnl": round(total_pnl, 2),
        "total_pnl_pct": round(total_pnl_pct, 2),
        "active_holdings": len(holdings),
        "mature_holdings": mature_count,
        "at_risk_holdings": at_risk,
    }


@router.get("/alerts")
async def get_portfolio_alerts():
    """Actionable alerts for held positions."""
    holdings = get_active_holdings()
    today = date.today().isoformat()
    alerts = []

    for h in holdings:
        qty = h["quantity"] - (h.get("sell_quantity") or 0)
        if qty <= 0:
            continue

        symbol = h["symbol"]
        ltp = _get_live_price(symbol)
        signal = _get_signal_for_symbol(symbol)
        maturity = h.get("maturity_date", "")
        is_mature = maturity <= today if maturity else False

        # Maturity alert
        if is_mature and maturity == today:
            alerts.append({
                "symbol": symbol,
                "alert_type": "MATURITY",
                "message": f"{symbol} matured today — you can now sell",
                "urgency": "MEDIUM",
                "holding_id": h["id"],
            })

        if not signal or not ltp:
            continue

        # Stop loss alert
        stop = signal.get("stop_loss", 0)
        if stop and ltp <= stop and is_mature:
            alerts.append({
                "symbol": symbol,
                "alert_type": "STOP_LOSS",
                "message": f"{symbol} hit stop loss ৳{stop:.1f} — consider selling",
                "urgency": "HIGH",
                "holding_id": h["id"],
            })

        # Target reached alert
        target = signal.get("target_price", 0)
        if target and ltp >= target and is_mature:
            alerts.append({
                "symbol": symbol,
                "alert_type": "TARGET_REACHED",
                "message": f"{symbol} reached target ৳{target:.1f} — take profit",
                "urgency": "HIGH",
                "holding_id": h["id"],
            })

        # Signal changed to sell
        sig_type = signal.get("signal_type", "HOLD")
        if sig_type in ("SELL", "STRONG_SELL") and is_mature:
            alerts.append({
                "symbol": symbol,
                "alert_type": "SIGNAL_CHANGE",
                "message": f"{symbol} signal changed to {sig_type} — sell recommended",
                "urgency": "HIGH",
                "holding_id": h["id"],
            })

    # Sort by urgency (HIGH first)
    urgency_order = {"HIGH": 0, "MEDIUM": 1, "LOW": 2}
    alerts.sort(key=lambda a: urgency_order.get(a["urgency"], 2))
    return alerts


@router.delete("/{holding_id}")
async def remove_holding(holding_id: int):
    """Delete a holding record."""
    holding = get_holding_by_id(holding_id)
    if not holding:
        raise HTTPException(404, "Holding not found")
    delete_holding(holding_id)
    return {"message": f"Holding #{holding_id} deleted"}
