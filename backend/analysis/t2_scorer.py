"""T+2 settlement-aware scoring for DSE trading signals."""

import logging
from datetime import date, timedelta
from config import (
    MARKET_DAYS, T2_SETTLEMENT_DAYS, T2_MIN_RETURN_PCT,
    T2_RISK_BASE, T2_RISK_UPTREND_BONUS, T2_RISK_HIGH_VOL_PENALTY,
    T2_RISK_NEAR_RESISTANCE_PENALTY, T2_RISK_NEAR_SUPPORT_BONUS,
    T2_RISK_NEGATIVE_T2_PENALTY, T2_RISK_VOLUME_BONUS,
)

logger = logging.getLogger(__name__)


class T2Scorer:
    """
    DSE T+2 settlement rule:
    If you buy on Day 0, you can first sell on Day 2 (2 trading days later).
    Trading days: Sun-Thu (Bangladesh). Fri/Sat are off.

    This class evaluates whether buying is safe given T+2 lock-in,
    computes risk scores, and generates entry/exit strategies.
    """

    def score(
        self,
        predictions: dict,
        current_price: float,
        atr: float,
        signal_type: str,
        stop_loss: float,
        volume_ratio: float | None = None,
    ) -> dict:
        """
        Compute T+2 safety, risk score, hold days, and strategies.

        Args:
            predictions: Output from PricePredictor.predict()
            current_price: Current stock price (LTP)
            atr: Average True Range (14-period)
            signal_type: STRONG_BUY, BUY, HOLD, SELL, STRONG_SELL
            stop_loss: Pre-calculated stop loss price
            volume_ratio: Current volume / 20-day average (optional)
        """
        pred_prices = predictions.get("predicted_prices", {})
        price_range = predictions.get("price_range_next_3d", {})
        support = predictions.get("support_level", 0)
        resistance = predictions.get("resistance_level", 0)
        trend = predictions.get("trend_strength", "SIDEWAYS")
        volatility = predictions.get("volatility_level", "MEDIUM")

        day_2 = pred_prices.get("day_2", current_price)
        day_3 = pred_prices.get("day_3", current_price)
        range_min = price_range.get("min", current_price)

        # ---- T+2 Safety ----
        t2_safe = self._check_t2_safety(
            current_price, day_2, day_3, range_min, stop_loss
        )

        # ---- Expected Return ----
        expected_return = (
            ((day_2 - current_price) / current_price * 100)
            if current_price > 0
            else 0
        )

        # ---- Hold Days (find optimal sell day) ----
        hold_days = self._find_optimal_hold_days(pred_prices, current_price)

        # ---- Best predicted return (at optimal hold day) ----
        if hold_days > 0:
            best_price = pred_prices.get(f"day_{hold_days}", current_price)
            best_return = (
                (best_price - current_price) / current_price * 100
                if current_price > 0
                else 0
            )
        else:
            best_return = expected_return

        # ---- Risk Score ----
        risk_score = self._compute_risk_score(
            trend, volatility, current_price, support, resistance,
            atr, day_2, volume_ratio,
        )

        # ---- Entry Strategy ----
        entry_strategy = self._entry_strategy(
            t2_safe, signal_type, current_price, support, atr, trend
        )

        # ---- Exit Strategy ----
        exit_strategy = self._exit_strategy(
            hold_days, pred_prices, current_price, resistance, atr, signal_type
        )

        # ---- Maturity Date ----
        maturity = self.compute_maturity_date()

        return {
            "t2_safe": t2_safe,
            "expected_return_pct": round(best_return, 2),
            "hold_days": hold_days,
            "entry_strategy": entry_strategy,
            "exit_strategy": exit_strategy,
            "risk_score": round(risk_score, 1),
            "t2_maturity_date": maturity.isoformat(),
        }

    # ------------------------------------------------------------------ #

    def _check_t2_safety(
        self, current: float, day_2: float, day_3: float,
        range_min: float, stop_loss: float,
    ) -> bool:
        """T+2 safe if price expected higher on Day 2+ and return worth the risk."""
        if current <= 0:
            return False

        ret_pct = (day_2 - current) / current * 100

        return (
            day_2 > current                          # Price up on Day 2
            and day_3 > current                      # Momentum sustains past Day 2
            and ret_pct >= T2_MIN_RETURN_PCT          # Worth the risk (>0.15%)
        )

    def _find_optimal_hold_days(
        self, pred_prices: dict, current: float
    ) -> int:
        """Find the day (2-7) where predicted price is maximized.
        Always returns at least day 2 (minimum T+2 settlement)."""
        best_day = 2  # minimum hold is T+2
        best_price = pred_prices.get("day_2", current)

        for key, price in pred_prices.items():
            if not key.startswith("day_"):
                continue
            day_num = int(key.split("_")[1])
            if day_num < 2:
                continue
            if price > best_price:
                best_price = price
                best_day = day_num

        return best_day

    def _compute_risk_score(
        self, trend: str, volatility: str, current: float,
        support: float, resistance: float, atr: float,
        day_2: float, volume_ratio: float | None,
    ) -> float:
        """Risk score 0-100. Higher = riskier."""
        risk = T2_RISK_BASE

        # Trend bonus/penalty
        if trend in ("STRONG_UP", "UP"):
            risk += T2_RISK_UPTREND_BONUS
        elif trend in ("STRONG_DOWN", "DOWN"):
            risk -= T2_RISK_UPTREND_BONUS  # penalty (adds risk)

        # Volatility
        if volatility == "HIGH":
            risk += T2_RISK_HIGH_VOL_PENALTY
        elif volatility == "LOW":
            risk -= 10

        # Proximity to resistance (risky — may reverse)
        if resistance and atr > 0:
            dist = abs(current - resistance) / atr
            if dist < 1.0:
                risk += T2_RISK_NEAR_RESISTANCE_PENALTY

        # Proximity to support (safer — has floor)
        if support and atr > 0:
            dist = abs(current - support) / atr
            if dist < 1.0:
                risk += T2_RISK_NEAR_SUPPORT_BONUS

        # Day 2 below current = risky
        if current > 0 and day_2 < current:
            risk += T2_RISK_NEGATIVE_T2_PENALTY

        # Volume confirmation bonus
        if volume_ratio and volume_ratio > 1.5:
            risk += T2_RISK_VOLUME_BONUS

        return max(0, min(100, risk))

    def _entry_strategy(
        self, t2_safe: bool, signal_type: str, current: float,
        support: float, atr: float, trend: str,
    ) -> str:
        """Generate human-readable entry strategy with specific price levels."""
        if signal_type in ("SELL", "STRONG_SELL"):
            return f"Avoid — sell signal active (current ৳{current:.1f})"

        if trend in ("STRONG_DOWN", "DOWN") and not t2_safe:
            if support and support > 0:
                return f"Wait for reversal near support ৳{support:.1f}"
            return f"Avoid — downtrend, wait for ৳{current * 0.97:.1f} or lower"

        if not t2_safe:
            if support and support > 0 and support < current:
                entry = support + (current - support) * 0.3
                return f"Buy on dip to ৳{entry:.1f} (near support ৳{support:.1f})"
            entry = current * 0.985
            return f"Wait for dip to ৳{entry:.1f} — T+2 risk at current price"

        if signal_type == "STRONG_BUY":
            return f"Buy at ৳{current:.1f} (market open) — strong signal"

        if support and atr > 0 and (current - support) < atr * 0.5:
            return f"Buy at ৳{current:.1f} — near support ৳{support:.1f}"

        if atr > 0:
            dip_target = current - atr * 0.3
            return f"Buy at ৳{current:.1f} or better on dip to ৳{dip_target:.1f}"

        return f"Buy at ৳{current:.1f} (market open)"

    def _exit_strategy(
        self, hold_days: int, pred_prices: dict, current: float,
        resistance: float, atr: float, signal_type: str,
    ) -> str:
        """Generate human-readable exit strategy."""
        if signal_type in ("SELL", "STRONG_SELL"):
            return "Sell at market open"

        target = pred_prices.get(f"day_{hold_days}", current)
        target_ret = (target - current) / current * 100 if current > 0 else 0

        # If predicted target is below current price, warn
        if target_ret < 0:
            if resistance and resistance > current:
                return f"Sell near resistance ৳{resistance:.1f} if price rebounds"
            return f"Exit at ৳{target:.1f} on Day {hold_days} — limited upside"

        # If resistance is close to target, use resistance as exit
        if resistance and atr > 0 and abs(target - resistance) < atr:
            return f"Sell near resistance ৳{resistance:.1f} around Day {hold_days}"

        if hold_days <= 3:
            return f"Sell at ৳{target:.1f} on Day {hold_days} (+{target_ret:.1f}%)"

        if atr > 0:
            trail = atr * 1.5
            return f"Hold {hold_days}d, target ৳{target:.1f} (+{target_ret:.1f}%), trail stop ৳{trail:.1f}"

        return f"Sell at ৳{target:.1f} on Day {hold_days} (+{target_ret:.1f}%)"

    # ------------------------------------------------------------------ #

    @staticmethod
    def compute_maturity_date(buy_date: date | None = None) -> date:
        """Calculate T+2 maturity date, skipping Fri(4) and Sat(5)."""
        if buy_date is None:
            buy_date = date.today()

        trading_days_counted = 0
        current = buy_date
        while trading_days_counted < T2_SETTLEMENT_DAYS:
            current += timedelta(days=1)
            # MARKET_DAYS uses Python's weekday(): Mon=0..Sun=6
            if current.weekday() in MARKET_DAYS:
                trading_days_counted += 1
        return current
