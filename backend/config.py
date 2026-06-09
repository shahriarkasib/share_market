"""DSE Trading Assistant - Configuration constants."""

import os
import pytz
from datetime import time

# DSE Market Configuration
DSE_TIMEZONE = pytz.timezone("Asia/Dhaka")
MARKET_OPEN_TIME = time(10, 0)   # 10:00 AM BST
MARKET_CLOSE_TIME = time(14, 30)  # 2:30 PM BST
MARKET_DAYS = [6, 0, 1, 2, 3]    # Sun=6, Mon=0, Tue=1, Wed=2, Thu=3

# ─── Signal Quality Filters ────────────────────────────────────────────────
# Sectors / symbols where the system's signals don't translate to profit
# (mostly mean-reverting or low-liquidity). Excluded from default Live
# Signals view but still tracked in DB for backtests.
SIGNAL_EXCLUDED_SECTORS = {
    'Insurance', 'Life Insurance', 'Mutual Funds', 'Bank',
}
SIGNAL_EXCLUDED_SYMBOLS = {
    'BATBC',  # known low-quality signal source per backtest
}
# Patterns: anything ending in MF (mutual fund) or *INS* (insurance) is junk
SIGNAL_EXCLUDED_PATTERNS = ('MF', 'INS', 'BANK')


def is_signal_quality_symbol(symbol: str, sector: str | None = None) -> bool:
    """Return True if a symbol should appear in the curated Live Signals view."""
    if not symbol:
        return False
    if symbol in SIGNAL_EXCLUDED_SYMBOLS:
        return False
    s = (sector or '').strip()
    if s in SIGNAL_EXCLUDED_SECTORS:
        return False
    # Suffix-based heuristics
    for suffix in ('MF',):
        if symbol.endswith(suffix):
            return False
    # Insurance company patterns: most end in INS
    if 'INS' in symbol and symbol not in ('TITASGAS',):
        return False
    # Bank patterns: most contain BANK
    if 'BANK' in symbol and symbol not in ('LANKABAFIN',):  # LANKABA-FIN not bank
        return False
    return True


# ─── Holiday Calendar Overrides ────────────────────────────────────────────
# DSE publishes special schedules for Eid + national holidays.
# Use ISO format "YYYY-MM-DD".
#
# DSE_HOLIDAYS: Dates that fall on normal trading days (Sun-Thu) but are
# CLOSED due to holidays. The scheduler will skip these.
#
# DSE_SPECIAL_TRADING_DAYS: Dates that fall on Fri/Sat but are EXCEPTIONALLY
# OPEN (e.g., Eid make-up days). The scheduler will trade these.
DSE_HOLIDAYS = {
    # Eid-ul-Adha 2026 closure (May 25-31)
    "2026-05-25", "2026-05-26", "2026-05-27", "2026-05-28", "2026-05-29",
    # May 30-31 already non-trading (Fri-Sat) but listed for clarity
    "2026-05-30", "2026-05-31",
}
DSE_SPECIAL_TRADING_DAYS = {
    # Eid week make-up days — Sat May 23 trades to compensate for the closure
    "2026-05-23",
    # Sunday May 24 is already a normal trading day (no override needed but
    # listed here for documentation)
}


def is_dse_trading_day(d) -> bool:
    """Return True if `d` (a date or datetime) is a DSE trading day.

    Logic:
      1. If date is in DSE_SPECIAL_TRADING_DAYS → trading day (override)
      2. If date is in DSE_HOLIDAYS → NOT trading day (override)
      3. Else fall back to weekday check (MARKET_DAYS: Sun-Thu)
    """
    iso = d.strftime("%Y-%m-%d") if hasattr(d, "strftime") else str(d)
    if iso in DSE_SPECIAL_TRADING_DAYS:
        return True
    if iso in DSE_HOLIDAYS:
        return False
    return d.weekday() in MARKET_DAYS

# Data Refresh Configuration
REFRESH_INTERVAL_SECONDS = 300  # 5 minutes
HISTORICAL_DAYS = 365  # 1 year of history for indicators

# Signal Thresholds (tuned for DSE's lower volatility vs global markets)
STRONG_BUY_THRESHOLD = 40
BUY_THRESHOLD = 15
SELL_THRESHOLD = -15
STRONG_SELL_THRESHOLD = -40

# Volume Filter - minimum daily traded value for signal consideration
# bdshare reports value in millions BDT, so 0.5 = 5 lakh BDT
MIN_DAILY_VALUE = 0.5

# Database — Local PostgreSQL on GCP VM
DATABASE_URL = os.getenv(
    "DATABASE_URL",
    "postgresql://postgres:dse_local_2026@127.0.0.1:5432/dse_trading",
)
DATABASE_URL_DIRECT = DATABASE_URL  # Same connection, no pgbouncer needed

# API Configuration
API_PREFIX = "/api/v1"
_cors_env = os.getenv("CORS_ORIGINS", "")
CORS_ORIGINS = (
    [o.strip() for o in _cors_env.split(",") if o.strip()]
    if _cors_env
    else ["http://localhost:5173", "http://127.0.0.1:5173"]
)

# Cache TTLs (seconds)
# Backend refreshes ALL caches every 5 min, so TTLs are just safety nets.
# Set to 600s (10 min) = 2x refresh interval, so caches never go cold.
CACHE_TTL_LIVE_PRICES = 600
CACHE_TTL_SIGNALS = 600
CACHE_TTL_INDICATORS = 600
CACHE_TTL_FUNDAMENTALS = 3600
CACHE_TTL_HISTORICAL = 86400

# Short-term indicator weights
SHORT_TERM_WEIGHTS = {
    "rsi": 0.15,
    "macd": 0.15,
    "ema_crossover": 0.15,
    "volume": 0.15,
    "bollinger": 0.10,
    "support_resistance": 0.10,
    "candlestick": 0.10,
    "price_momentum": 0.10,
}

# ---- Prediction Configuration ----
# Statistical methods: ARMA(p,q) + GARCH(1,1) + Bootstrap Monte Carlo
# Ensemble: inverse-variance weighting (Timmermann 2006)
PREDICTION_DAYS = [2, 3, 4, 5, 6, 7]
SR_PIVOT_WINDOW = 5
SR_CLUSTER_PCT = 0.015  # 1.5% bandwidth for clustering S/R levels

# ---- T+2 Settlement Configuration ----
T2_SETTLEMENT_DAYS = 2
T2_MIN_RETURN_PCT = 0.15  # minimum 0.15% expected return to be "safe"
T2_RISK_BASE = 50
T2_RISK_UPTREND_BONUS = -15
T2_RISK_HIGH_VOL_PENALTY = 20
T2_RISK_NEAR_RESISTANCE_PENALTY = 15
T2_RISK_NEAR_SUPPORT_BONUS = -10
T2_RISK_NEGATIVE_T2_PENALTY = 20
T2_RISK_VOLUME_BONUS = -10

# Long-term indicator weights
LONG_TERM_WEIGHTS = {
    "sma_50_trend": 0.15,
    "macd_weekly": 0.10,
    "rsi_monthly": 0.10,
    "pe_ratio": 0.15,
    "eps_growth": 0.15,
    "dividend_yield": 0.10,
    "sector_strength": 0.10,
    "volume_trend": 0.15,
}
