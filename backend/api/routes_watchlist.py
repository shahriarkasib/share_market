"""Watchlist API routes."""

from fastapi import APIRouter, HTTPException
from pydantic import BaseModel
from typing import Optional
from database import get_connection
from datetime import datetime

router = APIRouter()


class WatchlistAdd(BaseModel):
    symbol: str
    notes: Optional[str] = None


@router.get("")
async def get_watchlist():
    """Get all watchlist items."""
    conn = get_connection()
    items = conn.execute(
        "SELECT id, symbol, added_at, notes FROM watchlist ORDER BY added_at DESC"
    ).fetchall()
    conn.close()
    return [dict(item) for item in items]


@router.post("")
async def add_to_watchlist(item: WatchlistAdd):
    """Add a stock to watchlist."""
    conn = get_connection()
    try:
        conn.execute(
            "INSERT INTO watchlist (symbol, notes) VALUES (?, ?)",
            (item.symbol.upper(), item.notes)
        )
        conn.commit()
        return {"message": f"{item.symbol.upper()} added to watchlist"}
    except Exception as e:
        if "UNIQUE" in str(e):
            raise HTTPException(status_code=400, detail="Already in watchlist")
        raise
    finally:
        conn.close()


@router.delete("/{symbol}")
async def remove_from_watchlist(symbol: str):
    """Remove a stock from watchlist."""
    conn = get_connection()
    result = conn.execute(
        "DELETE FROM watchlist WHERE symbol = ?", (symbol.upper(),)
    )
    conn.commit()
    if result.rowcount == 0:
        conn.close()
        raise HTTPException(status_code=404, detail="Not in watchlist")
    conn.close()
    return {"message": f"{symbol.upper()} removed from watchlist"}
