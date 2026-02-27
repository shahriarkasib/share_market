"""Pydantic response models for the API."""

from pydantic import BaseModel
from typing import Optional
from datetime import datetime


class MarketSummaryResponse(BaseModel):
    dsex_index: float = 0
    dsex_change: float = 0
    dsex_change_pct: float = 0
    total_volume: int = 0
    total_value: float = 0
    total_trade: int = 0
    advances: int = 0
    declines: int = 0
    unchanged: int = 0
    market_status: str = "UNKNOWN"
    last_updated: Optional[str] = None


class StockPriceResponse(BaseModel):
    symbol: str
    company_name: Optional[str] = None
    ltp: float = 0
    change: float = 0
    change_pct: float = 0
    open: float = 0
    high: float = 0
    low: float = 0
    close_prev: float = 0
    volume: int = 0
    value: float = 0
    trade_count: int = 0


class StockSignalResponse(BaseModel):
    symbol: str
    company_name: Optional[str] = None
    ltp: float = 0
    change_pct: float = 0
    signal_type: str  # STRONG_BUY, BUY, HOLD, SELL, STRONG_SELL
    confidence: float = 0  # 0.0 to 1.0
    short_term_score: float = 0  # -100 to +100
    long_term_score: float = 0
    target_price: float = 0
    stop_loss: float = 0
    risk_reward_ratio: float = 0
    reasoning: str = ""
    timing: str = "HOLD"  # BUY_NOW, WAIT_FOR_DIP, ACCUMULATE, SELL_NOW, HOLD_TIGHT
    indicators: dict = {}
    created_at: Optional[str] = None


class OHLCVResponse(BaseModel):
    date: str
    open: float
    high: float
    low: float
    close: float
    volume: int = 0


class SignalsSummaryResponse(BaseModel):
    total_stocks: int = 0
    strong_buy_count: int = 0
    buy_count: int = 0
    hold_count: int = 0
    sell_count: int = 0
    strong_sell_count: int = 0
    market_sentiment: str = "NEUTRAL"  # BULLISH, NEUTRAL, BEARISH
    last_updated: Optional[str] = None


class WatchlistItemResponse(BaseModel):
    id: int
    symbol: str
    added_at: str
    notes: Optional[str] = None
    ltp: Optional[float] = None
    change_pct: Optional[float] = None
    signal_type: Optional[str] = None
    confidence: Optional[float] = None


class ScreenerFilterRequest(BaseModel):
    rsi_min: Optional[float] = None
    rsi_max: Optional[float] = None
    pe_min: Optional[float] = None
    pe_max: Optional[float] = None
    volume_min: Optional[float] = None  # ratio vs 20-day avg
    price_min: Optional[float] = None
    price_max: Optional[float] = None
    signal_type: Optional[str] = None
    sector: Optional[str] = None
    sort_by: str = "confidence"  # confidence, short_term_score, volume_ratio
    limit: int = 50
