"""Technical indicator computation — matches lankabd.com standard settings.

Indicator settings (matching lankabd / TradingView defaults):
  RSI(14)             — Wilder's smoothed RSI
  StochRSI(14,14,3,3) — Stochastic of RSI, K=3 smoothed, D=3 smoothed
  MACD(12,26,close,9) — Standard MACD
  Bollinger Bands(20,2)
  Stochastic(14,3,3)  — Price-based stochastic oscillator
  ATR(14)
  ADX(14)             — Average Directional Index (trend strength)
  MFI(14)             — Money Flow Index (volume-weighted RSI)
  A/D Line            — Accumulation/Distribution
  CMF(20)             — Chaikin Money Flow
  Williams %R(14)     — Overbought/oversold oscillator
  VWAP                — Volume Weighted Average Price
"""

import pandas as pd
import numpy as np
import logging

logger = logging.getLogger(__name__)


class TechnicalIndicators:
    """Computes technical indicators on OHLCV DataFrames."""

    def __init__(self, df: pd.DataFrame):
        """
        df must have columns: date, open, high, low, close, volume
        Sorted by date ascending. Minimum 20 rows recommended.
        """
        self.df = df.copy()
        # Ensure numeric
        for col in ["open", "high", "low", "close", "volume"]:
            if col in self.df.columns:
                self.df[col] = pd.to_numeric(self.df[col], errors="coerce")

        # Filter out non-trading days (record dates, halts) where close=0
        # These corrupt all indicator calculations
        self.df = self.df[self.df["close"] > 0].reset_index(drop=True)

    def compute_all(self) -> pd.DataFrame:
        """Compute all indicators and return enriched DataFrame."""
        df = self.df

        try:
            # EMA (Exponential Moving Average)
            df["ema_9"] = df["close"].ewm(span=9, adjust=False).mean()
            df["ema_21"] = df["close"].ewm(span=21, adjust=False).mean()
            df["sma_50"] = df["close"].rolling(window=50).mean()

            # RSI(14) — Wilder's smoothing
            df["rsi_14"] = self._compute_rsi(df["close"], 14)

            # StochRSI(14,14,3,3) — Stochastic of RSI (matches lankabd)
            rsi_series = df["rsi_14"]
            rsi_low14 = rsi_series.rolling(window=14).min()
            rsi_high14 = rsi_series.rolling(window=14).max()
            rsi_range = rsi_high14 - rsi_low14
            stochrsi_raw = ((rsi_series - rsi_low14) / rsi_range.replace(0, np.nan)) * 100
            df["stoch_k"] = stochrsi_raw.rolling(window=3).mean()   # K line (blue)
            df["stoch_d"] = df["stoch_k"].rolling(window=3).mean()  # D line (red)

            # MACD(12,26,close,9)
            ema12 = df["close"].ewm(span=12, adjust=False).mean()
            ema26 = df["close"].ewm(span=26, adjust=False).mean()
            df["macd"] = ema12 - ema26
            df["macd_signal"] = df["macd"].ewm(span=9, adjust=False).mean()
            df["macd_histogram"] = df["macd"] - df["macd_signal"]

            # Bollinger Bands(20,2)
            sma20 = df["close"].rolling(window=20).mean()
            std20 = df["close"].rolling(window=20).std()
            df["bb_upper"] = sma20 + (std20 * 2)
            df["bb_middle"] = sma20
            df["bb_lower"] = sma20 - (std20 * 2)

            # Stochastic Oscillator(14,3,3) — price-based (kept separately)
            low14 = df["low"].rolling(window=14).min()
            high14 = df["high"].rolling(window=14).max()
            price_range = (high14 - low14).replace(0, np.nan)
            raw_k = ((df["close"] - low14) / price_range) * 100
            df["slow_k"] = raw_k.rolling(window=3).mean()   # %K smoothed
            df["slow_d"] = df["slow_k"].rolling(window=3).mean()  # %D

            # ATR(14)
            df["atr_14"] = self._compute_atr(df, 14)

            # Volume indicators
            df["volume_sma_20"] = df["volume"].rolling(window=20).mean()
            df["volume_ratio"] = df["volume"] / df["volume_sma_20"]

            # OBV (On-Balance Volume)
            df["obv"] = self._compute_obv(df)

            # Price momentum (3-day and 5-day returns)
            df["momentum_3d"] = df["close"].pct_change(3) * 100
            df["momentum_5d"] = df["close"].pct_change(5) * 100

            # ── NEW INDICATORS ──

            # ADX(14) — Average Directional Index (trend strength)
            df["adx_14"], df["plus_di"], df["minus_di"] = self._compute_adx(df, 14)

            # MFI(14) — Money Flow Index (volume-weighted RSI)
            df["mfi_14"] = self._compute_mfi(df, 14)

            # A/D Line — Accumulation/Distribution
            clv = ((df["close"] - df["low"]) - (df["high"] - df["close"])) / \
                  (df["high"] - df["low"]).replace(0, np.nan)
            df["ad_line"] = (clv * df["volume"]).cumsum()

            # CMF(20) — Chaikin Money Flow
            mfv = clv * df["volume"]
            df["cmf_20"] = mfv.rolling(20).sum() / df["volume"].rolling(20).sum()

            # Williams %R(14)
            df["williams_r"] = ((high14 - df["close"]) / price_range) * -100

            # VWAP (rolling 20-day approximation for daily data)
            typical = (df["high"] + df["low"] + df["close"]) / 3
            df["vwap_20"] = (typical * df["volume"]).rolling(20).sum() / \
                            df["volume"].rolling(20).sum()

        except Exception as e:
            logger.error(f"Error computing indicators: {e}")

        return df

    def get_latest_indicators(self) -> dict:
        """Compute all indicators and return the latest values as dict."""
        df = self.compute_all()
        if df.empty:
            return {}

        latest = df.iloc[-1]
        prev = df.iloc[-2] if len(df) > 1 else latest

        result = {}
        indicator_cols = [
            "ema_9", "ema_21", "sma_50", "rsi_14",
            "macd", "macd_signal", "macd_histogram",
            "bb_upper", "bb_middle", "bb_lower",
            "stoch_k", "stoch_d",
            "slow_k", "slow_d",
            "atr_14",
            "volume_sma_20", "volume_ratio", "obv",
            "momentum_3d", "momentum_5d",
            "adx_14", "plus_di", "minus_di",
            "mfi_14", "ad_line", "cmf_20",
            "williams_r", "vwap_20",
        ]

        for col in indicator_cols:
            if col in df.columns:
                val = latest[col]
                result[col] = round(float(val), 4) if pd.notna(val) else None

        # Add previous values needed for crossover detection
        result["prev_ema_9"] = round(float(prev["ema_9"]), 4) if pd.notna(prev.get("ema_9")) else None
        result["prev_ema_21"] = round(float(prev["ema_21"]), 4) if pd.notna(prev.get("ema_21")) else None
        result["prev_macd_histogram"] = round(float(prev["macd_histogram"]), 4) if pd.notna(prev.get("macd_histogram")) else None

        # Current price info
        result["close"] = round(float(latest["close"]), 2) if pd.notna(latest.get("close")) else None
        result["volume"] = int(latest["volume"]) if pd.notna(latest.get("volume")) else 0

        return result

    @staticmethod
    def _compute_rsi(series: pd.Series, period: int = 14) -> pd.Series:
        """Compute RSI."""
        delta = series.diff()
        gain = delta.where(delta > 0, 0.0)
        loss = (-delta).where(delta < 0, 0.0)

        avg_gain = gain.rolling(window=period, min_periods=period).mean()
        avg_loss = loss.rolling(window=period, min_periods=period).mean()

        # Use exponential smoothing after initial SMA
        for i in range(period, len(series)):
            avg_gain.iloc[i] = (avg_gain.iloc[i-1] * (period - 1) + gain.iloc[i]) / period
            avg_loss.iloc[i] = (avg_loss.iloc[i-1] * (period - 1) + loss.iloc[i]) / period

        rs = avg_gain / avg_loss
        rsi = 100 - (100 / (1 + rs))
        return rsi

    @staticmethod
    def _compute_atr(df: pd.DataFrame, period: int = 14) -> pd.Series:
        """Compute Average True Range."""
        high = df["high"]
        low = df["low"]
        close = df["close"].shift(1)

        tr1 = high - low
        tr2 = (high - close).abs()
        tr3 = (low - close).abs()

        tr = pd.concat([tr1, tr2, tr3], axis=1).max(axis=1)
        atr = tr.rolling(window=period).mean()
        return atr

    @staticmethod
    def _compute_obv(df: pd.DataFrame) -> pd.Series:
        """Compute On-Balance Volume."""
        obv = [0]
        for i in range(1, len(df)):
            if df["close"].iloc[i] > df["close"].iloc[i-1]:
                obv.append(obv[-1] + df["volume"].iloc[i])
            elif df["close"].iloc[i] < df["close"].iloc[i-1]:
                obv.append(obv[-1] - df["volume"].iloc[i])
            else:
                obv.append(obv[-1])
        return pd.Series(obv, index=df.index)

    @staticmethod
    def _compute_adx(df: pd.DataFrame, period: int = 14):
        """Compute ADX (Average Directional Index) with +DI and -DI."""
        high = df["high"]
        low = df["low"]
        close = df["close"]

        # Directional movement
        up_move = high - high.shift(1)
        down_move = low.shift(1) - low
        plus_dm = up_move.where((up_move > down_move) & (up_move > 0), 0.0)
        minus_dm = down_move.where((down_move > up_move) & (down_move > 0), 0.0)

        # True Range
        tr1 = high - low
        tr2 = (high - close.shift(1)).abs()
        tr3 = (low - close.shift(1)).abs()
        tr = pd.concat([tr1, tr2, tr3], axis=1).max(axis=1)

        # Wilder's smoothing (same as ATR smoothing)
        atr = tr.rolling(period).mean()
        plus_dm_smooth = plus_dm.rolling(period).mean()
        minus_dm_smooth = minus_dm.rolling(period).mean()

        # Wilder's EMA after initial SMA
        for i in range(period, len(df)):
            atr.iloc[i] = (atr.iloc[i - 1] * (period - 1) + tr.iloc[i]) / period
            plus_dm_smooth.iloc[i] = (plus_dm_smooth.iloc[i - 1] * (period - 1) + plus_dm.iloc[i]) / period
            minus_dm_smooth.iloc[i] = (minus_dm_smooth.iloc[i - 1] * (period - 1) + minus_dm.iloc[i]) / period

        # +DI / -DI
        plus_di = (plus_dm_smooth / atr.replace(0, np.nan)) * 100
        minus_di = (minus_dm_smooth / atr.replace(0, np.nan)) * 100

        # DX and ADX
        di_sum = plus_di + minus_di
        dx = ((plus_di - minus_di).abs() / di_sum.replace(0, np.nan)) * 100
        adx = dx.rolling(period).mean()
        for i in range(period * 2, len(df)):
            if pd.notna(adx.iloc[i - 1]):
                adx.iloc[i] = (adx.iloc[i - 1] * (period - 1) + dx.iloc[i]) / period

        return adx, plus_di, minus_di

    @staticmethod
    def _compute_mfi(df: pd.DataFrame, period: int = 14) -> pd.Series:
        """Compute Money Flow Index (volume-weighted RSI)."""
        typical_price = (df["high"] + df["low"] + df["close"]) / 3
        raw_money_flow = typical_price * df["volume"]

        # Positive / negative money flow
        positive = raw_money_flow.where(typical_price > typical_price.shift(1), 0.0)
        negative = raw_money_flow.where(typical_price < typical_price.shift(1), 0.0)

        pos_sum = positive.rolling(period).sum()
        neg_sum = negative.rolling(period).sum()

        mfi = 100 - (100 / (1 + pos_sum / neg_sum.replace(0, np.nan)))
        return mfi
