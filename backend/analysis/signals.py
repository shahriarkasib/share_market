"""Core signal generation engine with weighted scoring and T+2 prediction."""

import pandas as pd
import numpy as np
import logging
from analysis.indicators import TechnicalIndicators
from analysis.predictor import PricePredictor
from analysis.t2_scorer import T2Scorer
from config import (
    SHORT_TERM_WEIGHTS, STRONG_BUY_THRESHOLD, BUY_THRESHOLD,
    SELL_THRESHOLD, STRONG_SELL_THRESHOLD
)

logger = logging.getLogger(__name__)


def _to_native(obj):
    """Recursively convert numpy types to Python native types."""
    if isinstance(obj, dict):
        return {k: _to_native(v) for k, v in obj.items()}
    elif isinstance(obj, (list, tuple)):
        return [_to_native(v) for v in obj]
    elif isinstance(obj, (np.bool_,)):
        return bool(obj)
    elif isinstance(obj, (np.integer,)):
        return int(obj)
    elif isinstance(obj, (np.floating,)):
        return round(float(obj), 4)
    return obj


class SignalGenerator:
    """
    Generates BUY/SELL/HOLD signals with confidence scores.
    Each indicator votes +1 (buy) to -1 (sell). Weighted sum produces score.
    """

    def generate_signal(self, symbol: str, df: pd.DataFrame,
                        live_price: dict = None) -> dict:
        """
        Generate a trading signal for a stock.

        Args:
            symbol: Stock trading code
            df: Historical OHLCV DataFrame (min 30 rows)
            live_price: Current live price data dict

        Returns:
            Signal dict with all fields
        """
        if df is None or df.empty or len(df) < 20:
            return self._empty_signal(symbol, "Insufficient data")

        # Compute indicators
        ti = TechnicalIndicators(df)
        indicators = ti.get_latest_indicators()

        if not indicators or indicators.get("close") is None:
            return self._empty_signal(symbol, "Could not compute indicators")

        # Run prediction engine (pass indicators for regression method)
        predictor = PricePredictor(df, indicators=indicators)
        predictions = predictor.predict()

        # Score each indicator
        scores = {}
        scores["rsi"] = self._score_rsi(indicators.get("rsi_14"))
        scores["macd"] = self._score_macd(
            indicators.get("macd"),
            indicators.get("macd_signal"),
            indicators.get("macd_histogram"),
            indicators.get("prev_macd_histogram"),
        )
        scores["ema_crossover"] = self._score_ema_crossover(
            indicators.get("ema_9"),
            indicators.get("ema_21"),
            indicators.get("prev_ema_9"),
            indicators.get("prev_ema_21"),
        )
        scores["volume"] = self._score_volume(indicators.get("volume_ratio"))
        scores["bollinger"] = self._score_bollinger(
            indicators.get("close"),
            indicators.get("bb_lower"),
            indicators.get("bb_middle"),
            indicators.get("bb_upper"),
        )
        scores["price_momentum"] = self._score_momentum(
            indicators.get("momentum_3d"),
            indicators.get("momentum_5d"),
        )
        # Support/resistance from prediction engine
        scores["support_resistance"] = self._score_support_resistance(
            indicators.get("close", 0),
            predictions.get("support_level"),
            predictions.get("resistance_level"),
            indicators.get("atr_14", 0),
        )
        scores["candlestick"] = 0.0

        # Calculate weighted short-term score
        raw_score = 0
        total_weight = 0
        for key, weight in SHORT_TERM_WEIGHTS.items():
            if key in scores and scores[key] is not None:
                raw_score += scores[key] * weight
                total_weight += weight

        # Normalize to -100 to +100 range
        if total_weight > 0:
            short_term_score = round((raw_score / total_weight) * 100, 1)
        else:
            short_term_score = 0

        # Determine signal type
        signal_type = self._classify_signal(short_term_score)

        # Calculate confidence (based on indicator agreement)
        confidence = self._calculate_confidence(scores)

        # Calculate target and stop loss
        close = indicators.get("close", 0)
        atr = indicators.get("atr_14", 0)
        target_price, stop_loss, rr_ratio = self._calculate_target_stoploss(
            close, atr, signal_type
        )

        # T+2 scoring
        t2_scorer = T2Scorer()
        t2_result = t2_scorer.score(
            predictions=predictions,
            current_price=close,
            atr=atr,
            signal_type=signal_type,
            stop_loss=stop_loss,
            volume_ratio=indicators.get("volume_ratio"),
        )

        # Determine timing (with T+2 override)
        timing = self._determine_timing(signal_type, indicators)
        if signal_type in ("STRONG_BUY", "BUY") and not t2_result.get("t2_safe", False):
            timing = "WAIT_FOR_DIP"

        # Generate reasoning
        reasoning = self._generate_reasoning(signal_type, confidence, indicators, scores)
        if signal_type in ("STRONG_BUY", "BUY") and not t2_result.get("t2_safe", False):
            reasoning += " T+2 Warning: price may drop before you can sell."

        # Build indicator summary for display
        indicator_summary = {
            "rsi": round(indicators.get("rsi_14", 0), 1) if indicators.get("rsi_14") else None,
            "macd_signal": self._macd_label(indicators),
            "bb_position": self._bb_label(indicators),
            "ema_crossover": self._ema_label(indicators),
            "volume_signal": self._volume_label(indicators.get("volume_ratio")),
            "momentum_3d": round(indicators.get("momentum_3d", 0), 2) if indicators.get("momentum_3d") else None,
            "stoch_k": round(indicators.get("stoch_k", 0), 1) if indicators.get("stoch_k") else None,
        }

        ltp = live_price.get("ltp", close) if live_price else close
        change_pct = live_price.get("change_pct", 0) if live_price else 0
        company_name = live_price.get("company_name", symbol) if live_price else symbol

        return _to_native({
            "symbol": symbol,
            "company_name": company_name,
            "ltp": round(ltp, 2),
            "change_pct": round(change_pct, 2),
            "signal_type": signal_type,
            "confidence": round(confidence, 2),
            "short_term_score": short_term_score,
            "long_term_score": 0,
            "target_price": round(target_price, 2),
            "stop_loss": round(stop_loss, 2),
            "risk_reward_ratio": round(rr_ratio, 2),
            "reasoning": reasoning,
            "timing": timing,
            "indicators": indicator_summary,
            "created_at": pd.Timestamp.now().isoformat(),
            # Prediction fields
            "predicted_prices": predictions.get("predicted_prices", {}),
            "expected_return_pct": t2_result.get("expected_return_pct", 0),
            "hold_days": t2_result.get("hold_days", 0),
            "entry_strategy": t2_result.get("entry_strategy", ""),
            "exit_strategy": t2_result.get("exit_strategy", ""),
            "support_level": predictions.get("support_level", 0),
            "resistance_level": predictions.get("resistance_level", 0),
            "trend_strength": predictions.get("trend_strength", "SIDEWAYS"),
            "volatility_level": predictions.get("volatility_level", "MEDIUM"),
            "t2_safe": t2_result.get("t2_safe", False),
            "price_range_next_3d": predictions.get("price_range_next_3d", {}),
            "daily_ranges": predictions.get("daily_ranges", {}),
            "risk_score": t2_result.get("risk_score", 50),
            "t2_maturity_date": t2_result.get("t2_maturity_date", ""),
        })

    def _score_rsi(self, rsi: float) -> float:
        """Score RSI: oversold = buy, overbought = sell."""
        if rsi is None:
            return 0.0
        if rsi < 25:
            return 1.0
        elif rsi < 30:
            return 0.7
        elif rsi < 40:
            return 0.3
        elif rsi <= 60:
            return 0.0
        elif rsi <= 70:
            return -0.3
        elif rsi <= 80:
            return -0.7
        else:
            return -1.0

    def _score_macd(self, macd, signal, histogram, prev_histogram) -> float:
        """Score MACD crossover and histogram direction."""
        if any(v is None for v in [macd, signal, histogram]):
            return 0.0

        score = 0.0

        # Crossover detection
        if prev_histogram is not None:
            if prev_histogram <= 0 and histogram > 0:  # Bullish crossover
                score = 0.8
            elif prev_histogram >= 0 and histogram < 0:  # Bearish crossover
                score = -0.8
            elif histogram > 0:
                score = 0.3 if histogram > prev_histogram else 0.1
            elif histogram < 0:
                score = -0.3 if histogram < prev_histogram else -0.1

        return score

    def _score_ema_crossover(self, ema9, ema21, prev_ema9, prev_ema21) -> float:
        """Score EMA 9/21 crossover."""
        if any(v is None for v in [ema9, ema21]):
            return 0.0

        if prev_ema9 is not None and prev_ema21 is not None:
            # Golden cross
            if prev_ema9 <= prev_ema21 and ema9 > ema21:
                return 1.0
            # Death cross
            if prev_ema9 >= prev_ema21 and ema9 < ema21:
                return -1.0

        # Current position
        if ema9 > ema21:
            spread = (ema9 - ema21) / ema21 * 100
            return min(0.5, spread * 0.1)
        else:
            spread = (ema21 - ema9) / ema21 * 100
            return max(-0.5, -spread * 0.1)

    def _score_volume(self, volume_ratio) -> float:
        """Score volume relative to 20-day average."""
        if volume_ratio is None:
            return 0.0
        if volume_ratio > 3.0:
            return 0.8  # Volume surge (direction needs price context)
        elif volume_ratio > 2.0:
            return 0.5
        elif volume_ratio > 1.5:
            return 0.3
        elif volume_ratio > 0.8:
            return 0.0
        elif volume_ratio > 0.5:
            return -0.2
        else:
            return -0.5  # Very low volume, avoid

    def _score_bollinger(self, close, lower, middle, upper) -> float:
        """Score price position relative to Bollinger Bands."""
        if any(v is None for v in [close, lower, middle, upper]):
            return 0.0

        band_width = upper - lower
        if band_width == 0:
            return 0.0

        position = (close - lower) / band_width  # 0 = at lower, 1 = at upper

        if position < 0:  # Below lower band
            return 0.8
        elif position < 0.15:
            return 0.5
        elif position < 0.4:
            return 0.2
        elif position <= 0.6:
            return 0.0
        elif position <= 0.85:
            return -0.2
        elif position <= 1.0:
            return -0.5
        else:  # Above upper band
            return -0.8

    def _score_momentum(self, momentum_3d, momentum_5d) -> float:
        """Score short-term price momentum."""
        if momentum_3d is None:
            return 0.0

        # 3-day momentum weighted more for short-term
        score = 0.0
        if momentum_3d > 5:
            score = 0.5  # Strong positive but may be overextended
        elif momentum_3d > 2:
            score = 0.7
        elif momentum_3d > 0:
            score = 0.3
        elif momentum_3d > -2:
            score = -0.3
        elif momentum_3d > -5:
            score = -0.5
        else:
            score = -0.3  # Oversold bounce potential

        return score

    @staticmethod
    def _score_support_resistance(close, support, resistance, atr) -> float:
        """Score based on proximity to support/resistance levels."""
        if not close or not atr or atr == 0:
            return 0.0

        if support and support > 0:
            dist_support = (close - support) / atr
            if dist_support < 0.5:
                return 0.7  # Very close to support — bullish bounce
            elif dist_support < 1.0:
                return 0.3

        if resistance and resistance > 0:
            dist_resistance = (resistance - close) / atr
            if dist_resistance < 0.5:
                return -0.7  # Very close to resistance — bearish rejection
            elif dist_resistance < 1.0:
                return -0.3

        return 0.0

    def _classify_signal(self, score: float) -> str:
        """Classify score into signal type."""
        if score >= STRONG_BUY_THRESHOLD:
            return "STRONG_BUY"
        elif score >= BUY_THRESHOLD:
            return "BUY"
        elif score <= STRONG_SELL_THRESHOLD:
            return "STRONG_SELL"
        elif score <= SELL_THRESHOLD:
            return "SELL"
        else:
            return "HOLD"

    def _calculate_confidence(self, scores: dict) -> float:
        """Calculate confidence based on indicator agreement."""
        values = [v for v in scores.values() if v is not None and v != 0]
        if not values:
            return 0.0

        # Count agreement
        positive = sum(1 for v in values if v > 0)
        negative = sum(1 for v in values if v < 0)
        total = len(values)

        # Confidence = how much indicators agree
        agreement = max(positive, negative) / total
        avg_strength = sum(abs(v) for v in values) / total

        confidence = agreement * avg_strength
        return min(confidence, 1.0)

    def _calculate_target_stoploss(self, close: float, atr: float,
                                     signal_type: str) -> tuple:
        """Calculate target price and stop loss using ATR."""
        if close == 0 or atr is None or atr == 0:
            return close, close, 0

        if signal_type in ("STRONG_BUY", "BUY"):
            target = close + (2.0 * atr)
            stop = close - (1.0 * atr)
        elif signal_type in ("STRONG_SELL", "SELL"):
            target = close - (2.0 * atr)
            stop = close + (1.0 * atr)
        else:
            target = close + (1.0 * atr)
            stop = close - (1.0 * atr)

        risk = abs(close - stop)
        reward = abs(target - close)
        rr_ratio = reward / risk if risk > 0 else 0

        return target, stop, rr_ratio

    def _determine_timing(self, signal_type: str, indicators: dict) -> str:
        """Determine timing advice."""
        rsi = indicators.get("rsi_14", 50)
        volume_ratio = indicators.get("volume_ratio", 1)

        if signal_type in ("STRONG_BUY", "BUY"):
            if rsi and rsi < 30 and volume_ratio and volume_ratio > 1.5:
                return "BUY_NOW"
            elif rsi and rsi < 40:
                return "BUY_NOW"
            else:
                return "WAIT_FOR_DIP"
        elif signal_type in ("STRONG_SELL", "SELL"):
            return "SELL_NOW"
        else:
            return "HOLD_TIGHT"

    def _generate_reasoning(self, signal_type, confidence, indicators, scores) -> str:
        """Generate human-readable reasoning."""
        parts = []

        rsi = indicators.get("rsi_14")
        if rsi is not None:
            if rsi < 30:
                parts.append(f"RSI at {rsi:.1f} (oversold zone)")
            elif rsi > 70:
                parts.append(f"RSI at {rsi:.1f} (overbought zone)")
            else:
                parts.append(f"RSI at {rsi:.1f}")

        macd_h = indicators.get("macd_histogram")
        prev_h = indicators.get("prev_macd_histogram")
        if macd_h is not None and prev_h is not None:
            if prev_h <= 0 and macd_h > 0:
                parts.append("Bullish MACD crossover detected")
            elif prev_h >= 0 and macd_h < 0:
                parts.append("Bearish MACD crossover detected")

        vol_ratio = indicators.get("volume_ratio")
        if vol_ratio is not None:
            if vol_ratio > 2.0:
                parts.append(f"Volume surge at {vol_ratio:.1f}x average")
            elif vol_ratio < 0.5:
                parts.append("Very low volume - caution")

        ema9 = indicators.get("ema_9")
        ema21 = indicators.get("ema_21")
        if ema9 is not None and ema21 is not None:
            if ema9 > ema21:
                parts.append("EMA 9 above EMA 21 (bullish trend)")
            else:
                parts.append("EMA 9 below EMA 21 (bearish trend)")

        mom = indicators.get("momentum_3d")
        if mom is not None:
            if mom > 3:
                parts.append(f"Strong 3-day momentum (+{mom:.1f}%)")
            elif mom < -3:
                parts.append(f"Weak 3-day momentum ({mom:.1f}%)")

        if not parts:
            parts.append("Mixed signals - limited directional clarity")

        return f"{signal_type} ({confidence:.0%} confidence). " + ". ".join(parts) + "."

    def _macd_label(self, indicators: dict) -> str:
        h = indicators.get("macd_histogram")
        prev_h = indicators.get("prev_macd_histogram")
        if h is None:
            return "NEUTRAL"
        if prev_h is not None and prev_h <= 0 and h > 0:
            return "BULLISH_CROSSOVER"
        if prev_h is not None and prev_h >= 0 and h < 0:
            return "BEARISH_CROSSOVER"
        return "BULLISH" if h > 0 else "BEARISH"

    def _bb_label(self, indicators: dict) -> str:
        close = indicators.get("close")
        lower = indicators.get("bb_lower")
        upper = indicators.get("bb_upper")
        if any(v is None for v in [close, lower, upper]):
            return "UNKNOWN"
        bw = upper - lower
        if bw == 0:
            return "UNKNOWN"
        pos = (close - lower) / bw
        if pos < 0.1:
            return "BELOW_LOWER"
        elif pos < 0.3:
            return "NEAR_LOWER"
        elif pos < 0.7:
            return "MIDDLE"
        elif pos < 0.9:
            return "NEAR_UPPER"
        else:
            return "ABOVE_UPPER"

    def _ema_label(self, indicators: dict) -> str:
        ema9 = indicators.get("ema_9")
        ema21 = indicators.get("ema_21")
        prev9 = indicators.get("prev_ema_9")
        prev21 = indicators.get("prev_ema_21")
        if any(v is None for v in [ema9, ema21]):
            return "NONE"
        if prev9 is not None and prev21 is not None:
            if prev9 <= prev21 and ema9 > ema21:
                return "GOLDEN_CROSS"
            if prev9 >= prev21 and ema9 < ema21:
                return "DEATH_CROSS"
        return "BULLISH" if ema9 > ema21 else "BEARISH"

    def _volume_label(self, volume_ratio) -> str:
        if volume_ratio is None:
            return "UNKNOWN"
        if volume_ratio > 3.0:
            return "SURGE"
        elif volume_ratio > 1.5:
            return "HIGH"
        elif volume_ratio > 0.8:
            return "NORMAL"
        elif volume_ratio > 0.5:
            return "LOW"
        else:
            return "VERY_LOW"

    def _empty_signal(self, symbol: str, reason: str) -> dict:
        return {
            "symbol": symbol,
            "company_name": symbol,
            "ltp": 0,
            "change_pct": 0,
            "signal_type": "HOLD",
            "confidence": 0,
            "short_term_score": 0,
            "long_term_score": 0,
            "target_price": 0,
            "stop_loss": 0,
            "risk_reward_ratio": 0,
            "reasoning": reason,
            "timing": "HOLD_TIGHT",
            "indicators": {},
            "created_at": pd.Timestamp.now().isoformat(),
            "predicted_prices": {},
            "expected_return_pct": 0,
            "hold_days": 0,
            "entry_strategy": "",
            "exit_strategy": "",
            "support_level": 0,
            "resistance_level": 0,
            "trend_strength": "SIDEWAYS",
            "volatility_level": "MEDIUM",
            "t2_safe": False,
            "price_range_next_3d": {},
            "risk_score": 50,
            "t2_maturity_date": "",
        }
