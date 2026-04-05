#!/usr/bin/env python3
"""
Compute technical indicators for all stocks using the `ta` library.
Populates the stock_indicators table with daily, weekly, and monthly timeframes.

Usage:
    ./venv/bin/python3 scripts/compute_indicators.py [--symbols ACMELAB,GP] [--timeframe daily]
"""

import sys
import os
import argparse
import logging
import warnings
from datetime import datetime, timedelta

import numpy as np
import pandas as pd
import psycopg2
import psycopg2.extras

# Suppress pandas SQLAlchemy warning — we use psycopg2 directly, which works fine
warnings.filterwarnings("ignore", message=".*pandas only supports SQLAlchemy.*")

# Add parent dir for config import
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from config import DATABASE_URL

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%H:%M:%S",
)
log = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Database helpers
# ---------------------------------------------------------------------------

def get_conn():
    return psycopg2.connect(DATABASE_URL)


def load_daily_prices(conn, symbol: str, lookback_days: int = 2800) -> pd.DataFrame:
    """Load OHLCV data for a symbol.

    lookback_days=2800 (~11 years) to cover full history in DB.
    Extra rows needed for SMA200 warm-up and monthly aggregation.
    close > 0 filter excludes garbage rows for suspended stocks.
    """
    sql = """
        SELECT date, open, high, low, close, volume
        FROM daily_prices
        WHERE symbol = %s AND date >= CURRENT_DATE - INTERVAL '%s days'
          AND close > 0 AND high > 0 AND low > 0
        ORDER BY date ASC
    """
    df = pd.read_sql(sql, conn, params=(symbol, lookback_days))
    if df.empty:
        return df
    df["date"] = pd.to_datetime(df["date"])
    df = df.set_index("date").sort_index()
    df = df[~df.index.duplicated(keep="last")]
    return df


def aggregate_weekly(daily: pd.DataFrame) -> pd.DataFrame:
    """Aggregate daily OHLCV to weekly (DSE: Sun-Thu week)."""
    weekly = daily.resample("W-WED").agg({
        "open": "first",
        "high": "max",
        "low": "min",
        "close": "last",
        "volume": "sum",
    }).dropna(subset=["close"])
    return weekly


def aggregate_monthly(daily: pd.DataFrame) -> pd.DataFrame:
    """Aggregate daily OHLCV to monthly."""
    monthly = daily.resample("ME").agg({
        "open": "first",
        "high": "max",
        "low": "min",
        "close": "last",
        "volume": "sum",
    }).dropna(subset=["close"])
    return monthly


# ---------------------------------------------------------------------------
# Indicator computation using `ta` library
# ---------------------------------------------------------------------------

def compute_indicators(df: pd.DataFrame, timeframe: str = "daily") -> pd.DataFrame:
    """Compute all technical indicators on an OHLCV DataFrame.

    Minimum 14 rows required (for RSI-14). Indicators with longer windows
    will simply be NaN for early rows, which is correct.
    """
    if len(df) < 14:
        return pd.DataFrame()

    close = df["close"]
    high = df["high"]
    low = df["low"]
    volume = df["volume"].astype(float)
    open_ = df["open"]

    # Skip stocks with zero volume across the board (fully suspended)
    if volume.sum() == 0:
        return pd.DataFrame()

    result = pd.DataFrame(index=df.index)
    result["open"] = open_
    result["high"] = high
    result["low"] = low
    result["close"] = close
    result["volume"] = df["volume"]

    # --- Moving Averages ---
    from ta.trend import EMAIndicator, SMAIndicator

    result["ema_9"] = EMAIndicator(close, window=9).ema_indicator()
    result["ema_21"] = EMAIndicator(close, window=21).ema_indicator()
    if len(df) >= 50:
        result["ema_50"] = EMAIndicator(close, window=50).ema_indicator()
        result["sma_50"] = SMAIndicator(close, window=50).sma_indicator()
    if len(df) >= 200:
        result["sma_200"] = SMAIndicator(close, window=200).sma_indicator()
        result["ema_200"] = EMAIndicator(close, window=200).ema_indicator()

    # --- RSI ---
    from ta.momentum import RSIIndicator, StochRSIIndicator, WilliamsRIndicator

    result["rsi_14"] = RSIIndicator(close, window=14).rsi()

    # Stochastic RSI (needs >14 rows to avoid internal indexing error in ta lib)
    if len(df) > 20:
        try:
            stoch_rsi = StochRSIIndicator(close, window=14, smooth1=3, smooth2=3)
            result["stoch_rsi_k"] = stoch_rsi.stochrsi_k() * 100
            result["stoch_rsi_d"] = stoch_rsi.stochrsi_d() * 100
        except (IndexError, ValueError):
            pass  # Not enough data for StochRSI

    # Williams %R
    result["williams_r_14"] = WilliamsRIndicator(high, low, close, lbp=14).williams_r()

    # --- MACD ---
    from ta.trend import MACD

    macd = MACD(close, window_slow=26, window_fast=12, window_sign=9)
    result["macd_line"] = macd.macd()
    result["macd_signal"] = macd.macd_signal()
    result["macd_hist"] = macd.macd_diff()

    # --- Money Flow ---
    from ta.volume import ChaikinMoneyFlowIndicator, MFIIndicator, OnBalanceVolumeIndicator

    if len(df) >= 20:
        result["cmf_20"] = ChaikinMoneyFlowIndicator(high, low, close, volume, window=20).chaikin_money_flow()
    result["mfi_14"] = MFIIndicator(high, low, close, volume, window=14).money_flow_index()
    result["obv"] = OnBalanceVolumeIndicator(close, volume).on_balance_volume()

    # --- ADX / DI ---
    from ta.trend import ADXIndicator

    try:
        adx = ADXIndicator(high, low, close, window=14)
        result["adx_14"] = adx.adx()
        result["plus_di_14"] = adx.adx_pos()
        result["minus_di_14"] = adx.adx_neg()
    except (IndexError, ValueError):
        pass

    # --- Bollinger Bands ---
    from ta.volatility import BollingerBands

    if len(df) >= 20:
        bb = BollingerBands(close, window=20, window_dev=2)
        result["bb_upper"] = bb.bollinger_hband()
        result["bb_middle"] = bb.bollinger_mavg()
        result["bb_lower"] = bb.bollinger_lband()
        result["bb_pct"] = bb.bollinger_pband()
        bb_width = (result["bb_upper"] - result["bb_lower"])
        result["bb_width_pct"] = np.where(
            result["bb_middle"] > 0,
            bb_width / result["bb_middle"] * 100,
            np.nan,
        )

    # --- ATR ---
    from ta.volatility import AverageTrueRange

    atr = AverageTrueRange(high, low, close, window=14)
    result["atr_14"] = atr.average_true_range()
    result["atr_pct"] = np.where(close > 0, result["atr_14"] / close * 100, np.nan)

    # --- Ichimoku ---
    from ta.trend import IchimokuIndicator

    if len(df) >= 52:
        ichi = IchimokuIndicator(high, low, window1=9, window2=26, window3=52)
        result["ichi_tenkan"] = ichi.ichimoku_conversion_line()
        result["ichi_kijun"] = ichi.ichimoku_base_line()
        result["ichi_senkou_a"] = ichi.ichimoku_a()
        result["ichi_senkou_b"] = ichi.ichimoku_b()

    # --- Volume derived ---
    if len(df) >= 20:
        result["avg_volume_20"] = volume.rolling(20).mean()
        avg_vol = result["avg_volume_20"]
        result["vol_ratio"] = np.where(avg_vol > 0, volume / avg_vol, np.nan)

    # Up/Down volume ratio (10-day)
    if len(df) >= 11:
        green = close > close.shift(1)
        red = close < close.shift(1)
        up_vol_10 = volume.where(green, 0).rolling(10).sum()
        dn_vol_10 = volume.where(red, 0).rolling(10).sum()
        result["up_down_vol_ratio"] = np.where(dn_vol_10 > 0, up_vol_10 / dn_vol_10, np.nan)

    # --- Price changes ---
    result["chg_5d"] = close.pct_change(5) * 100
    result["chg_10d"] = close.pct_change(10) * 100
    result["chg_20d"] = close.pct_change(20) * 100

    # --- Slopes (linear regression over N days) ---
    def rolling_slope(series, window):
        """Linear regression slope over rolling window."""
        x = np.arange(window, dtype=float)
        x_mean = x.mean()
        x_var = ((x - x_mean) ** 2).sum()
        if x_var == 0:
            return pd.Series(np.nan, index=series.index)
        slopes = series.rolling(window).apply(
            lambda y: ((x * (y - y.mean())).sum()) / x_var,
            raw=True,
        )
        return slopes

    if "macd_hist" in result.columns:
        result["macd_hist_slope"] = result["macd_hist"].diff(3) / 3
    if len(df) >= 10:
        result["obv_slope_10d"] = rolling_slope(result.get("obv", pd.Series(dtype=float)), 10)
        result["price_slope_10d"] = rolling_slope(close, 10)
        if "cmf_20" in result.columns:
            result["cmf_slope_10d"] = rolling_slope(result["cmf_20"], 10)

    # --- Swing analysis ---
    if len(df) >= 20:
        result["swing_low_20d"] = low.rolling(20).min()
        swing = result["swing_low_20d"]
        result["pct_from_swing_low"] = np.where(swing > 0, (close - swing) / swing * 100, np.nan)
        min_idx = low.rolling(20).apply(lambda x: x.argmin(), raw=True)
        result["days_since_swing_low"] = (20 - 1 - min_idx).astype("Int64")

    # --- CMF streak ---
    if "cmf_20" in result.columns:
        cmf = result["cmf_20"]
        pos_streak = pd.Series(0, index=df.index, dtype=int)
        neg_streak = pd.Series(0, index=df.index, dtype=int)
        for i in range(1, len(cmf)):
            if pd.notna(cmf.iloc[i]):
                if cmf.iloc[i] > 0:
                    pos_streak.iloc[i] = pos_streak.iloc[i - 1] + 1
                    neg_streak.iloc[i] = 0
                elif cmf.iloc[i] < 0:
                    neg_streak.iloc[i] = neg_streak.iloc[i - 1] + 1
                    pos_streak.iloc[i] = 0
        result["cmf_pos_streak"] = pos_streak
        result["cmf_neg_streak"] = neg_streak

    # --- MA alignment flags ---
    has_ema9 = "ema_9" in result.columns
    has_ema21 = "ema_21" in result.columns
    has_ema50 = "ema_50" in result.columns
    has_sma200 = "sma_200" in result.columns
    has_sma50 = "sma_50" in result.columns

    if has_ema9 and has_ema21 and has_ema50 and has_sma200:
        result["ma_aligned"] = (
            (result["ema_9"] > result["ema_21"])
            & (result["ema_21"] > result["ema_50"])
            & (result["ema_50"] > result["sma_200"])
        )

    if has_sma50 and has_sma200:
        sma50 = result["sma_50"]
        sma200 = result["sma_200"]
        result["golden_cross"] = (sma50 > sma200) & (sma50.shift(1) <= sma200.shift(1))
        result["death_cross"] = (sma50 < sma200) & (sma50.shift(1) >= sma200.shift(1))

    # --- Support / Resistance (20-day low/high) ---
    if len(df) >= 20:
        result["support"] = low.rolling(20).min()
        result["resistance"] = high.rolling(20).max()

    return result


# ---------------------------------------------------------------------------
# Store results
# ---------------------------------------------------------------------------

INDICATOR_COLUMNS = [
    "open", "high", "low", "close", "volume",
    "ema_9", "ema_21", "ema_50", "sma_50", "sma_200", "ema_200",
    "rsi_14", "stoch_rsi_k", "stoch_rsi_d", "williams_r_14",
    "macd_line", "macd_signal", "macd_hist",
    "cmf_20", "mfi_14", "obv",
    "adx_14", "plus_di_14", "minus_di_14",
    "bb_upper", "bb_middle", "bb_lower", "bb_pct", "bb_width_pct",
    "atr_14", "atr_pct",
    "ichi_tenkan", "ichi_kijun", "ichi_senkou_a", "ichi_senkou_b",
    "avg_volume_20", "vol_ratio", "up_down_vol_ratio",
    "chg_5d", "chg_10d", "chg_20d",
    "macd_hist_slope", "obv_slope_10d", "price_slope_10d", "cmf_slope_10d",
    "swing_low_20d", "pct_from_swing_low", "days_since_swing_low",
    "cmf_pos_streak", "cmf_neg_streak",
    "ma_aligned", "golden_cross", "death_cross",
    "support", "resistance",
]


def store_indicators(conn, symbol: str, timeframe: str, indicators: pd.DataFrame, last_n_days: int = 500):
    """Upsert computed indicators into stock_indicators table."""
    if len(indicators) > last_n_days:
        indicators = indicators.iloc[-last_n_days:]

    if indicators.empty:
        return 0

    cur = conn.cursor()
    cols = ["symbol", "date", "timeframe"] + INDICATOR_COLUMNS
    placeholders = ", ".join(["%s"] * len(cols))
    conflict_updates = ", ".join(
        f"{c} = EXCLUDED.{c}" for c in INDICATOR_COLUMNS
    )

    sql = f"""
        INSERT INTO stock_indicators ({', '.join(cols)})
        VALUES ({placeholders})
        ON CONFLICT (symbol, date, timeframe)
        DO UPDATE SET {conflict_updates}, computed_at = NOW()
    """

    rows = []
    for dt, row in indicators.iterrows():
        vals = [symbol, dt.date(), timeframe]
        for col in INDICATOR_COLUMNS:
            v = row.get(col)
            if v is None or (isinstance(v, float) and np.isnan(v)):
                vals.append(None)
            elif pd.isna(v):
                vals.append(None)
            elif isinstance(v, (np.bool_, bool)):
                vals.append(bool(v))
            elif isinstance(v, (np.integer,)):
                vals.append(int(v))
            elif isinstance(v, (np.floating, float)):
                vals.append(round(float(v), 6))
            else:
                vals.append(v)
        rows.append(vals)

    psycopg2.extras.execute_batch(cur, sql, rows, page_size=200)
    conn.commit()
    cur.close()
    return len(rows)


# ---------------------------------------------------------------------------
# Main pipeline
# ---------------------------------------------------------------------------

def process_symbol(conn, symbol: str, timeframes: list[str]) -> dict:
    """Compute and store indicators for one symbol across timeframes."""
    stats = {}

    daily = load_daily_prices(conn, symbol, lookback_days=2800)
    if len(daily) < 30:
        log.warning(f"{symbol}: only {len(daily)} daily rows, skipping")
        return stats

    for tf in timeframes:
        if tf == "daily":
            ohlcv = daily
            store_days = 500
        elif tf == "weekly":
            ohlcv = aggregate_weekly(daily)
            store_days = 104
        elif tf == "monthly":
            ohlcv = aggregate_monthly(daily)
            store_days = 36  # 3 years of monthly
        else:
            continue

        if len(ohlcv) < 14:
            log.debug(f"{symbol}/{tf}: only {len(ohlcv)} rows, skipping")
            continue

        try:
            indicators = compute_indicators(ohlcv, tf)
        except Exception as e:
            log.debug(f"{symbol}/{tf}: compute error: {e}")
            continue
        if indicators.empty:
            continue

        n = store_indicators(conn, symbol, tf, indicators, last_n_days=store_days)
        stats[tf] = n

    return stats


def get_active_symbols(conn, categories: list[str] = None) -> list[str]:
    """Get symbols that have recent price data."""
    sql = """
        SELECT DISTINCT dp.symbol
        FROM daily_prices dp
        JOIN fundamentals f ON dp.symbol = f.symbol
        WHERE dp.date >= CURRENT_DATE - INTERVAL '10 days'
          AND dp.close > 0
    """
    if categories:
        sql += " AND f.category IN %s"
        params = (tuple(categories),)
    else:
        params = None

    cur = conn.cursor()
    cur.execute(sql, params)
    symbols = [row["symbol"] if isinstance(row, dict) else row[0] for row in cur.fetchall()]
    cur.close()
    return sorted(symbols)


def main():
    parser = argparse.ArgumentParser(description="Compute stock indicators")
    parser.add_argument("--symbols", type=str, help="Comma-separated symbols (default: all active)")
    parser.add_argument("--timeframe", type=str, default="daily,weekly,monthly",
                        help="Comma-separated timeframes (default: daily,weekly,monthly)")
    parser.add_argument("--categories", type=str, default="A,B",
                        help="Stock categories to process (default: A,B)")
    parser.add_argument("--clean", action="store_true",
                        help="Delete existing indicators before computing (fresh start)")
    args = parser.parse_args()

    timeframes = [t.strip() for t in args.timeframe.split(",")]

    conn = get_conn()

    if args.clean:
        cur = conn.cursor()
        cur.execute("DELETE FROM stock_indicators")
        conn.commit()
        cur.close()
        log.info("Cleared all existing indicator data")

    if args.symbols:
        symbols = [s.strip() for s in args.symbols.split(",")]
    else:
        categories = [c.strip() for c in args.categories.split(",")]
        symbols = get_active_symbols(conn, categories)

    log.info(f"Processing {len(symbols)} symbols, timeframes: {timeframes}")

    success = 0
    failed = 0
    total_rows = 0

    for i, symbol in enumerate(symbols, 1):
        try:
            stats = process_symbol(conn, symbol, timeframes)
            if stats:
                n = sum(stats.values())
                total_rows += n
                success += 1
                if i % 25 == 0 or i == len(symbols):
                    log.info(f"[{i}/{len(symbols)}] {symbol}: {stats}")
            else:
                log.debug(f"[{i}/{len(symbols)}] {symbol}: no data")
        except Exception as e:
            failed += 1
            log.error(f"[{i}/{len(symbols)}] {symbol}: {e}")
            conn.rollback()

    conn.close()
    log.info(f"Done. {success} stocks, {total_rows} rows inserted, {failed} failed")


if __name__ == "__main__":
    main()
