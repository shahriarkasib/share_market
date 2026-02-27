"""DSE Trading Assistant - Configuration constants."""

import os
import pytz
from datetime import time

# DSE Market Configuration
DSE_TIMEZONE = pytz.timezone("Asia/Dhaka")
MARKET_OPEN_TIME = time(10, 0)   # 10:00 AM BST
MARKET_CLOSE_TIME = time(14, 30)  # 2:30 PM BST
MARKET_DAYS = [6, 0, 1, 2, 3]    # Sun=6, Mon=0, Tue=1, Wed=2, Thu=3

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

# Database
DATABASE_PATH = "dse_trading.db"

# API Configuration
API_PREFIX = "/api/v1"
_cors_env = os.getenv("CORS_ORIGINS", "")
CORS_ORIGINS = (
    [o.strip() for o in _cors_env.split(",") if o.strip()]
    if _cors_env
    else ["http://localhost:5173", "http://127.0.0.1:5173"]
)

# Cache TTLs (seconds)
CACHE_TTL_LIVE_PRICES = 60
CACHE_TTL_SIGNALS = 300
CACHE_TTL_INDICATORS = 300
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
