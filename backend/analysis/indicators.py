"""Technical indicator computation using pandas-ta."""

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

    def compute_all(self) -> pd.DataFrame:
        """Compute all indicators and return enriched DataFrame."""
        df = self.df

        try:
            # EMA (Exponential Moving Average)
            df["ema_9"] = df["close"].ewm(span=9, adjust=False).mean()
            df["ema_21"] = df["close"].ewm(span=21, adjust=False).mean()
            df["sma_50"] = df["close"].rolling(window=50).mean()

            # RSI (Relative Strength Index)
            df["rsi_14"] = self._compute_rsi(df["close"], 14)

            # MACD
            ema12 = df["close"].ewm(span=12, adjust=False).mean()
            ema26 = df["close"].ewm(span=26, adjust=False).mean()
            df["macd"] = ema12 - ema26
            df["macd_signal"] = df["macd"].ewm(span=9, adjust=False).mean()
            df["macd_histogram"] = df["macd"] - df["macd_signal"]

            # Bollinger Bands
            sma20 = df["close"].rolling(window=20).mean()
            std20 = df["close"].rolling(window=20).std()
            df["bb_upper"] = sma20 + (std20 * 2)
            df["bb_middle"] = sma20
            df["bb_lower"] = sma20 - (std20 * 2)

            # Stochastic Oscillator
            low14 = df["low"].rolling(window=14).min()
            high14 = df["high"].rolling(window=14).max()
            df["stoch_k"] = ((df["close"] - low14) / (high14 - low14) * 100)
            df["stoch_d"] = df["stoch_k"].rolling(window=3).mean()

            # ATR (Average True Range)
            df["atr_14"] = self._compute_atr(df, 14)

            # Volume indicators
            df["volume_sma_20"] = df["volume"].rolling(window=20).mean()
            df["volume_ratio"] = df["volume"] / df["volume_sma_20"]

            # OBV (On-Balance Volume)
            df["obv"] = self._compute_obv(df)

            # Price momentum (3-day and 5-day returns)
            df["momentum_3d"] = df["close"].pct_change(3) * 100
            df["momentum_5d"] = df["close"].pct_change(5) * 100

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
            "stoch_k", "stoch_d", "atr_14",
            "volume_sma_20", "volume_ratio", "obv",
            "momentum_3d", "momentum_5d",
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
