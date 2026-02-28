"""Daily post-market analysis engine (v2).

Screens ALL stocks (A/B/Z categories), uses composite scoring with
multi-day trends, candlestick patterns, volume divergence, and
historical accuracy feedback. Runs after market close (15:00 BST).
"""

import json
import logging
import math
from datetime import datetime, timedelta

import numpy as np
import pandas as pd
import pytz

from analysis.indicators import TechnicalIndicators
from data.repository import (
    get_all_symbols_with_category,
    get_bulk_signal_accuracy,
    read_all_historical_grouped,
    read_historical_for_symbol,
)
from database import get_connection

logger = logging.getLogger(__name__)
DSE_TZ = pytz.timezone("Asia/Dhaka")

# Action priority for sorting
ACTION_ORDER = {
    "BUY (strong)": 0,
    "BUY": 1,
    "BUY on pullback": 2,
    "BUY on dip": 3,
    "BUY (wait for MACD cross)": 4,
    "HOLD/WAIT": 5,
    "SELL/AVOID": 6,
    "AVOID": 7,
}


# ════════════════════════════════════════════════════════════
#  MAIN PIPELINE
# ════════════════════════════════════════════════════════════


def generate_daily_analysis() -> list[dict]:
    """Run full daily analysis on all eligible stocks (A/B/Z categories).

    Returns list of analysis dicts sorted by action priority then score.
    """
    logger.info("Starting daily analysis v2...")

    # 1. Load live prices
    conn = get_connection()
    live_rows = conn.execute(
        "SELECT * FROM live_prices WHERE ltp > 0"
    ).fetchall()
    conn.close()

    if not live_rows:
        logger.warning("No live prices available for daily analysis")
        return []

    live_prices = {r["symbol"]: dict(r) for r in live_rows}
    logger.info(f"Loaded {len(live_prices)} live prices")

    # 2. Get ALL symbols with their category (A/B/Z)
    symbol_categories = get_all_symbols_with_category()
    logger.info(f"Total symbols with categories: {len(symbol_categories)}")

    # 3. Filter: min turnover only (no category filter)
    MIN_TURNOVER = 0.5  # millions BDT
    eligible = {}
    for sym, lp in live_prices.items():
        turnover = lp.get("value", 0) or 0
        if turnover < MIN_TURNOVER:
            continue
        eligible[sym] = lp

    logger.info(f"Eligible stocks (turnover >= {MIN_TURNOVER}M): {len(eligible)}")

    # 4. Load all historical data in one batch
    all_history = read_all_historical_grouped(min_rows_per_symbol=30)

    # 5. Batch-load historical accuracy for all symbols
    all_accuracy = get_bulk_signal_accuracy(list(eligible.keys()))

    # 6. Analyze each stock
    results = []
    for sym, lp in eligible.items():
        try:
            df = all_history.get(sym)
            if df is None or len(df) < 30:
                continue
            category = symbol_categories.get(sym, "Z")
            accuracy = all_accuracy.get(sym, {})
            analysis = _analyze_stock(sym, df, lp, category, accuracy)
            if analysis:
                results.append(analysis)
        except Exception as e:
            logger.error(f"Analysis failed for {sym}: {e}")

    logger.info(f"Analyzed {len(results)} stocks")

    # 7. Sort by action priority then score
    results.sort(key=lambda x: (ACTION_ORDER.get(x["action"], 9), -x.get("score", 0)))

    return results


# ════════════════════════════════════════════════════════════
#  STOCK ANALYSIS
# ════════════════════════════════════════════════════════════


def _analyze_stock(symbol: str, df: pd.DataFrame, live: dict,
                   category: str = "A", accuracy: dict | None = None) -> dict | None:
    """Analyze a single stock and return structured analysis dict."""
    ltp = live.get("ltp", 0)
    if ltp <= 0:
        return None

    # Compute indicators
    ti = TechnicalIndicators(df)
    ind_df = ti.compute_all()
    if ind_df.empty:
        return None

    latest = ind_df.iloc[-1]
    prev = ind_df.iloc[-2] if len(ind_df) > 1 else latest

    # Extract indicator values safely
    rsi = _safe(latest.get("rsi_14"))
    stoch_k = _safe(latest.get("stoch_k"))
    macd_line = _safe(latest.get("macd"))
    macd_signal_val = _safe(latest.get("macd_signal"))
    macd_hist = _safe(latest.get("macd_histogram"))
    prev_macd_hist = _safe(prev.get("macd_histogram"))
    ema9 = _safe(latest.get("ema_9"))
    ema21 = _safe(latest.get("ema_21"))
    sma50 = _safe(latest.get("sma_50"))
    bb_lower = _safe(latest.get("bb_lower"))
    bb_upper = _safe(latest.get("bb_upper"))
    atr = _safe(latest.get("atr_14"))
    vol_ratio = _safe(latest.get("volume_ratio"))
    avg_vol = _safe(latest.get("volume_sma_20"))

    # Derived metrics
    above_sma50 = ltp > sma50 if sma50 > 0 else False
    bb_range = bb_upper - bb_lower if bb_upper > bb_lower else 1
    bb_pct = (ltp - bb_lower) / bb_range if bb_range > 0 else 0.5
    atr_pct = round(atr / ltp * 100, 2) if ltp > 0 and atr > 0 else 0

    # MACD crossover detection
    macd_cross_bull = prev_macd_hist <= 0 and macd_hist > 0 if prev_macd_hist is not None else False
    macd_cross_bear = prev_macd_hist >= 0 and macd_hist < 0 if prev_macd_hist is not None else False
    macd_converging = (
        macd_hist < 0
        and prev_macd_hist is not None
        and prev_macd_hist < macd_hist
        and abs(macd_hist) < abs(macd_line) * 0.5
    )

    # Historical stats (last 50 days)
    close_series = ind_df["close"].dropna()
    n = min(50, len(close_series))
    if n < 10:
        return None

    recent = close_series.tail(n)
    returns = recent.pct_change().dropna()
    volatility = round(float(returns.std() * 100), 2)
    max_dd = _max_drawdown(recent)
    trend_50d = round(float((recent.iloc[-1] / recent.iloc[0] - 1) * 100), 1)

    # Win/bounce rate
    up_days = int((returns > 0).sum())
    total_days = len(returns)
    win_rate = round(up_days / total_days * 100, 1) if total_days > 0 else 50

    # Bounce from lower BB
    near_bb_lower = ind_df["close"] <= ind_df["bb_lower"] * 1.02
    if near_bb_lower.sum() > 0:
        next_day_up = (ind_df["close"].shift(-1) > ind_df["close"])[near_bb_lower]
        bounce_rate = round(float(next_day_up.mean() * 100), 1) if len(next_day_up) > 0 else 50
    else:
        bounce_rate = 50.0

    # Support / resistance
    support = round(float(recent.tail(20).min()), 1)
    resistance = round(float(recent.tail(20).max()), 1)

    # Volume analysis (last 5 days)
    last5 = ind_df.tail(5)
    up_vol = int(last5[last5["close"] >= last5["open"]]["volume"].sum())
    dn_vol = int(last5[last5["close"] < last5["open"]]["volume"].sum())
    last_vol = int(latest.get("volume", 0))

    # Recent changes
    chg_5d = round(float((close_series.iloc[-1] / close_series.iloc[-min(6, n)] - 1) * 100), 1) if n > 5 else 0
    chg_10d = round(float((close_series.iloc[-1] / close_series.iloc[-min(11, n)] - 1) * 100), 1) if n > 10 else 0
    chg_20d = round(float((close_series.iloc[-1] / close_series.iloc[-min(21, n)] - 1) * 100), 1) if n > 20 else 0

    # Low/high ranges
    low_5d = round(float(ind_df.tail(5)["low"].min()), 1)
    high_5d = round(float(ind_df.tail(5)["high"].max()), 1)

    # ─── NEW: Multi-day trend analysis ───
    multi_day = _analyze_multi_day_trend(ind_df)

    # ─── NEW: Candlestick patterns ───
    candle_patterns = _detect_candlestick_patterns(ind_df)

    # ─── NEW: Volume-price divergence ───
    volume_pattern = _analyze_volume_pattern(ind_df)

    # ─── NEW: Historical accuracy ───
    history_score = _get_historical_accuracy_score(accuracy or {})

    # ─── Classification (composite scoring v2) ───
    action, reasoning, wait_days, score = _classify_stock_v2(
        ltp=ltp, rsi=rsi, stoch_rsi=stoch_k, macd_hist=macd_hist,
        macd_cross_bull=macd_cross_bull, macd_cross_bear=macd_cross_bear,
        macd_converging=macd_converging, above_sma50=above_sma50,
        bb_pct=bb_pct, trend_50d=trend_50d, volatility=volatility,
        max_dd=max_dd, vol_ratio=vol_ratio, bounce_rate=bounce_rate,
        atr_pct=atr_pct, ema9=ema9, ema21=ema21, sma50=sma50,
        bb_lower=bb_lower, support=support, resistance=resistance,
        macd_line=macd_line, macd_signal=macd_signal_val,
        multi_day=multi_day, candle_patterns=candle_patterns,
        volume_pattern=volume_pattern, history_score=history_score,
    )

    # ─── Entry / Exit computation ───
    entry_low, entry_high, sl, t1, t2 = _compute_entry_exit(
        action=action, ltp=ltp, atr=atr, bb_lower=bb_lower,
        ema21=ema21, low_5d=low_5d, support=support,
    )

    # Risk / reward
    entry_mid = (entry_low + entry_high) / 2 if entry_low > 0 else ltp
    risk_pct = round(abs(entry_mid - sl) / entry_mid * 100, 1) if entry_mid > 0 and sl > 0 else 0
    avg_target = (t1 + t2) / 2 if t1 > 0 and t2 > 0 else t1
    reward_pct = round((avg_target - entry_mid) / entry_mid * 100, 1) if entry_mid > 0 and avg_target > 0 else 0

    # Volume entry threshold
    vol_entry = f">{int(avg_vol * 0.5):,}" if avg_vol > 0 else ""

    # Concrete entry/exit timing
    timing = _compute_timing(
        action=action, atr=atr, ltp=ltp,
        entry_mid=entry_mid, t1=t1, t2=t2,
        volatility=volatility, score=score,
    )

    # Entry scenarios
    scenarios = _generate_scenarios(
        symbol=symbol, action=action, entry_low=entry_low, entry_high=entry_high,
        sl=sl, bb_lower=bb_lower, support=support, vol_entry=vol_entry,
    )

    # Last 5 days OHLCV
    last_5_data = []
    for _, row in ind_df.tail(5).iterrows():
        last_5_data.append([
            str(row["date"])[:10] if pd.notna(row.get("date")) else "",
            round(float(row["open"]), 1),
            round(float(row["high"]), 1),
            round(float(row["low"]), 1),
            round(float(row["close"]), 1),
            int(row["volume"]),
        ])

    macd_status = (
        "BULL cross" if macd_cross_bull
        else "BEAR cross" if macd_cross_bear
        else "Converging" if macd_converging
        else "Bullish" if macd_hist > 0
        else "Bearish"
    )

    # ─── Price predictions (BUY-type only — expensive) ───
    prediction_data = {}
    if action.startswith("BUY"):
        try:
            from analysis.predictor import PricePredictor
            from analysis.t2_scorer import T2Scorer

            _sig_map = {"BUY (strong)": "STRONG_BUY", "BUY": "BUY",
                        "BUY on pullback": "BUY", "BUY on dip": "BUY",
                        "BUY (wait for MACD cross)": "BUY"}
            pred = PricePredictor(df).predict()
            t2_result = T2Scorer().score(
                predictions=pred, current_price=ltp, atr=atr or 0,
                signal_type=_sig_map.get(action, "BUY"),
                stop_loss=sl, volume_ratio=vol_ratio,
            )
            prediction_data = {
                "predicted_prices": pred.get("predicted_prices", {}),
                "daily_ranges": pred.get("daily_ranges", {}),
                "price_range_next_3d": pred.get("price_range_next_3d", {}),
                "support_level": pred.get("support_level", 0),
                "resistance_level": pred.get("resistance_level", 0),
                "trend_strength": pred.get("trend_strength", "SIDEWAYS"),
                "volatility_level": pred.get("volatility_level", "MEDIUM"),
                "t2_safe": t2_result.get("t2_safe", False),
                "risk_score": t2_result.get("risk_score", 50),
                "expected_return_pct": round(t2_result.get("expected_return_pct", 0), 2),
                "hold_days": t2_result.get("hold_days", 0),
                "entry_strategy": t2_result.get("entry_strategy", ""),
                "exit_strategy": t2_result.get("exit_strategy", ""),
            }
        except Exception as e:
            logger.warning(f"Prediction failed for {symbol}: {e}")

    return {
        "symbol": symbol,
        "category": category,
        "ltp": ltp,
        "action": action,
        "reasoning": reasoning,
        "score": score,
        "entry_low": entry_low,
        "entry_high": entry_high,
        "sl": sl,
        "t1": t1,
        "t2": t2,
        "risk_pct": risk_pct,
        "reward_pct": reward_pct,
        "rsi": round(rsi, 1) if rsi else 0,
        "stoch_rsi": round(stoch_k, 1) if stoch_k else 0,
        "macd_line": round(macd_line, 3) if macd_line else 0,
        "macd_signal": round(macd_signal_val, 3) if macd_signal_val else 0,
        "macd_hist": round(macd_hist, 3) if macd_hist else 0,
        "macd_status": macd_status,
        "macd_cross_bull": bool(macd_cross_bull),
        "macd_cross_bear": bool(macd_cross_bear),
        "macd_converging": bool(macd_converging),
        "bb_pct": round(bb_pct, 3),
        "bb_lower": round(bb_lower, 1),
        "bb_upper": round(bb_upper, 1),
        "atr": round(atr, 1),
        "atr_pct": atr_pct,
        "volatility": volatility,
        "max_dd": round(max_dd, 1),
        "ema9": round(ema9, 1) if ema9 else 0,
        "ema21": round(ema21, 1) if ema21 else 0,
        "sma50": round(sma50, 1) if sma50 else 0,
        "support": support,
        "resistance": resistance,
        "trend_50d": trend_50d,
        "avg_vol": int(avg_vol),
        "last_vol": last_vol,
        "vol_ratio": round(vol_ratio, 2) if vol_ratio else 0,
        "avg_turnover": round(float(avg_vol * ltp), 0) if avg_vol > 0 else 0,
        "up_vol_5": up_vol,
        "dn_vol_5": dn_vol,
        "above_sma50": above_sma50,
        "win_rate": win_rate,
        "bounce_rate": bounce_rate,
        "chg_5d": chg_5d,
        "chg_10d": chg_10d,
        "chg_20d": chg_20d,
        "wait_days": wait_days,
        "vol_entry": vol_entry,
        "entry_start": timing["entry_start"],
        "entry_end": timing["entry_end"],
        "exit_t1_by": timing["exit_t1_by"],
        "exit_t2_by": timing["exit_t2_by"],
        "hold_days_t1": timing["hold_days_t1"],
        "hold_days_t2": timing["hold_days_t2"],
        "scenarios_json": json.dumps(scenarios),
        "last_5_json": json.dumps(last_5_data),
        "prediction_json": json.dumps(prediction_data) if prediction_data else None,
    }


# ════════════════════════════════════════════════════════════
#  MULTI-DAY TREND ANALYSIS
# ════════════════════════════════════════════════════════════


def _analyze_multi_day_trend(ind_df: pd.DataFrame) -> dict:
    """Analyze 3-5 day trends in RSI, MACD, StochRSI, Volume, BB position."""
    n = len(ind_df)
    if n < 5:
        return {"rsi_trend": 0, "macd_trend": 0, "stoch_trend": 0,
                "volume_trend": 0, "bb_trend": 0}

    last5 = ind_df.tail(5)
    last3 = ind_df.tail(3)

    # --- RSI 3-day trend ---
    rsi_vals = last3["rsi_14"].dropna().values
    rsi_trend = 0.0
    if len(rsi_vals) >= 3:
        rsi_slope = float(rsi_vals[-1] - rsi_vals[0])
        current_rsi = float(rsi_vals[-1])
        if rsi_slope < -5 and current_rsi < 45:
            rsi_trend = 0.6  # RSI falling rapidly toward oversold
        elif rsi_slope < -3 and current_rsi < 50:
            rsi_trend = 0.3
        elif rsi_slope > 0 and current_rsi < 40:
            rsi_trend = 0.5  # RSI recovering from oversold
        elif rsi_slope > 5 and current_rsi > 60:
            rsi_trend = -0.3  # RSI rising rapidly = already moved

    # --- MACD histogram 3-day trend ---
    macd_hist_vals = last3["macd_histogram"].dropna().values
    macd_trend = 0.0
    if len(macd_hist_vals) >= 3:
        diffs = np.diff(macd_hist_vals)
        if all(d > 0 for d in diffs):
            # 3 consecutive improving bars
            macd_trend = 0.7 if float(macd_hist_vals[-1]) < 0 else 0.3
        elif all(d < 0 for d in diffs):
            macd_trend = -0.5
        elif diffs[-1] > 0 and float(macd_hist_vals[-1]) < 0:
            macd_trend = 0.4  # Last bar improving from negative

    # --- StochRSI direction ---
    stoch_vals = last3["stoch_k"].dropna().values
    stoch_trend = 0.0
    if len(stoch_vals) >= 3:
        sv = [float(v) for v in stoch_vals]
        if sv[0] < 20 and sv[-1] > sv[0]:
            stoch_trend = 0.7  # Crossing up from oversold
        elif sv[-1] > 80 and sv[-1] < sv[0]:
            stoch_trend = -0.5  # Turning down from overbought
        elif sv[0] > 80 and all(sv[i] >= sv[i + 1] for i in range(len(sv) - 1)):
            stoch_trend = -0.6  # Stuck high and falling

    # --- Volume accumulation (5 days) ---
    volume_trend = 0.0
    up_days_vol = 0.0
    dn_days_vol = 0.0
    for _, row in last5.iterrows():
        v = float(row.get("volume", 0))
        if float(row["close"]) >= float(row["open"]):
            up_days_vol += v
        else:
            dn_days_vol += v
    total_vol = up_days_vol + dn_days_vol
    if total_vol > 0:
        up_vol_ratio = up_days_vol / total_vol
        if up_vol_ratio > 0.7:
            volume_trend = 0.6
        elif up_vol_ratio > 0.6:
            volume_trend = 0.3
        elif up_vol_ratio < 0.3:
            volume_trend = -0.5
        elif up_vol_ratio < 0.4:
            volume_trend = -0.3

    # --- BB position trend ---
    bb_trend = 0.0
    if "bb_lower" in last3.columns and "bb_upper" in last3.columns:
        bb_positions = []
        for _, row in last3.iterrows():
            bb_range = float(row.get("bb_upper", 0)) - float(row.get("bb_lower", 0))
            if bb_range > 0:
                bb_positions.append((float(row["close"]) - float(row["bb_lower"])) / bb_range)
        if len(bb_positions) >= 3:
            if bb_positions[-1] < 0.2 and bb_positions[-1] < bb_positions[0]:
                bb_trend = 0.5
            elif bb_positions[-1] > 0.8 and bb_positions[-1] > bb_positions[0]:
                bb_trend = -0.4

    return {
        "rsi_trend": rsi_trend,
        "macd_trend": macd_trend,
        "stoch_trend": stoch_trend,
        "volume_trend": volume_trend,
        "bb_trend": bb_trend,
    }


# ════════════════════════════════════════════════════════════
#  CANDLESTICK PATTERN DETECTION
# ════════════════════════════════════════════════════════════


def _detect_candlestick_patterns(ind_df: pd.DataFrame) -> dict:
    """Detect candlestick patterns from last 3 candles."""
    n = len(ind_df)
    if n < 3:
        return {"pattern": "none", "score": 0.0}

    c1, c2, c3 = ind_df.iloc[-3], ind_df.iloc[-2], ind_df.iloc[-1]

    o3, h3, l3, cl3 = float(c3["open"]), float(c3["high"]), float(c3["low"]), float(c3["close"])
    o2, h2, l2, cl2 = float(c2["open"]), float(c2["high"]), float(c2["low"]), float(c2["close"])
    o1, h1, l1, cl1 = float(c1["open"]), float(c1["high"]), float(c1["low"]), float(c1["close"])

    body3 = abs(cl3 - o3)
    body2 = abs(cl2 - o2)
    range3 = h3 - l3 if h3 > l3 else 0.001
    range2 = h2 - l2 if h2 > l2 else 0.001

    # --- Doji: body < 0.3% of price ---
    if cl3 > 0 and body3 / cl3 < 0.003:
        midpoint = (h3 + l3) / 2
        if cl3 >= midpoint:
            return {"pattern": "doji_bullish", "score": 0.4}
        else:
            return {"pattern": "doji_bearish", "score": -0.3}

    # --- Hammer: long lower shadow > 2x body ---
    lower_shadow3 = min(o3, cl3) - l3
    upper_shadow3 = h3 - max(o3, cl3)
    if body3 > 0 and lower_shadow3 > 2 * body3 and upper_shadow3 < body3 * 0.5:
        if cl3 > o3:
            return {"pattern": "hammer", "score": 0.6}
        else:
            return {"pattern": "hammer_red", "score": 0.3}

    # --- Shooting star: long upper shadow > 2x body ---
    if body3 > 0 and upper_shadow3 > 2 * body3 and lower_shadow3 < body3 * 0.5:
        if cl2 > o2 and cl3 < o3:
            return {"pattern": "shooting_star", "score": -0.5}
        else:
            return {"pattern": "inverted_hammer", "score": 0.3}

    # --- Bullish engulfing ---
    if cl2 < o2 and cl3 > o3:
        if o3 <= cl2 and cl3 >= o2:
            return {"pattern": "bullish_engulfing", "score": 0.7}

    # --- Bearish engulfing ---
    if cl2 > o2 and cl3 < o3:
        if o3 >= cl2 and cl3 <= o2:
            return {"pattern": "bearish_engulfing", "score": -0.7}

    # --- Morning star (3-candle) ---
    if cl1 < o1 and body2 / range2 < 0.3 and cl3 > o3:
        if cl3 > (o1 + cl1) / 2:
            return {"pattern": "morning_star", "score": 0.8}

    # --- Evening star (3-candle) ---
    if cl1 > o1 and body2 / range2 < 0.3 and cl3 < o3:
        if cl3 < (o1 + cl1) / 2:
            return {"pattern": "evening_star", "score": -0.8}

    return {"pattern": "none", "score": 0.0}


# ════════════════════════════════════════════════════════════
#  VOLUME-PRICE DIVERGENCE
# ════════════════════════════════════════════════════════════


def _analyze_volume_pattern(ind_df: pd.DataFrame) -> dict:
    """Analyze volume-price relationship for divergence signals."""
    n = len(ind_df)
    if n < 10:
        return {"signal": "neutral", "score": 0.0}

    last5 = ind_df.tail(5)
    latest = ind_df.iloc[-1]
    prev = ind_df.iloc[-2]

    close_chg = float(latest["close"]) - float(prev["close"])
    vol_ratio = float(latest.get("volume_ratio", 1.0))

    # Price down + volume spike = possible accumulation
    if close_chg < 0 and vol_ratio > 2.0:
        return {"signal": "accumulation_spike", "score": 0.5}

    # Price up + low volume = weak rally
    if close_chg > 0 and vol_ratio < 0.5:
        return {"signal": "weak_rally", "score": -0.3}

    # OBV divergence (10-day)
    if "obv" in ind_df.columns and n >= 10:
        obv_last10 = ind_df.tail(10)["obv"].values
        price_last10 = ind_df.tail(10)["close"].values
        if len(obv_last10) >= 10:
            price_chg_10d = (float(price_last10[-1]) - float(price_last10[0])) / float(price_last10[0]) * 100
            obv_chg_10d = float(obv_last10[-1]) - float(obv_last10[0])

            if abs(price_chg_10d) < 2 and obv_chg_10d > 0:
                return {"signal": "hidden_accumulation", "score": 0.4}
            elif abs(price_chg_10d) < 2 and obv_chg_10d < 0:
                return {"signal": "hidden_distribution", "score": -0.3}
            elif price_chg_10d > 3 and obv_chg_10d < 0:
                return {"signal": "bearish_divergence", "score": -0.5}
            elif price_chg_10d < -3 and obv_chg_10d > 0:
                return {"signal": "bullish_divergence", "score": 0.6}

    # Up-volume ratio (5 days)
    up_vol = float(last5[last5["close"] >= last5["open"]]["volume"].sum())
    total_vol = float(last5["volume"].sum())
    if total_vol > 0:
        ratio = up_vol / total_vol
        if ratio > 0.7:
            return {"signal": "strong_accumulation", "score": 0.4}
        elif ratio < 0.3:
            return {"signal": "strong_distribution", "score": -0.4}

    return {"signal": "neutral", "score": 0.0}


# ════════════════════════════════════════════════════════════
#  HISTORICAL ACCURACY FEEDBACK
# ════════════════════════════════════════════════════════════


def _get_historical_accuracy_score(accuracy: dict) -> dict:
    """Convert historical accuracy data into a scoring component."""
    total = accuracy.get("total_signals", 0)

    if total < 3:
        return {"score": 0.0, "reason": "", "confidence_modifier": 0}

    buy_win_rate = accuracy.get("buy_win_rate", 0.5)
    target_hit_rate = accuracy.get("target_hit_rate", 0.5)
    avg_return = accuracy.get("avg_return", 0)

    score = 0.0
    if buy_win_rate > 0.7:
        score += 0.5
    elif buy_win_rate > 0.6:
        score += 0.3
    elif buy_win_rate < 0.3:
        score -= 0.5
    elif buy_win_rate < 0.4:
        score -= 0.3

    if target_hit_rate > 0.6:
        score += 0.3
    elif target_hit_rate < 0.2:
        score -= 0.3

    if avg_return > 2.0:
        score += 0.2
    elif avg_return < -2.0:
        score -= 0.2

    score = max(-1.0, min(1.0, score))
    reason = f"history({total}sig, {buy_win_rate:.0%}win, {target_hit_rate:.0%}target)"

    return {"score": score, "reason": reason, "confidence_modifier": score * 10}


# ════════════════════════════════════════════════════════════
#  COMPOSITE SCORING CLASSIFIER (v2)
# ════════════════════════════════════════════════════════════


def _classify_stock_v2(
    *, ltp, rsi, stoch_rsi, macd_hist, macd_cross_bull, macd_cross_bear,
    macd_converging, above_sma50, bb_pct, trend_50d, volatility, max_dd,
    vol_ratio, bounce_rate, atr_pct, ema9, ema21, sma50,
    bb_lower, support, resistance, macd_line, macd_signal,
    multi_day, candle_patterns, volume_pattern, history_score,
) -> tuple[str, str, str, float]:
    """Composite scoring classification engine.

    9 weighted dimensions, each -1.0 to +1.0.
    Composite score mapped to -100..+100 for action classification.
    """
    rsi = rsi or 50
    stoch_rsi = stoch_rsi or 50

    # ── DIM 1: RSI position + 3d trend (weight 0.15) ──
    if rsi < 25:
        rsi_score = 1.0
    elif rsi < 35:
        rsi_score = 0.7
    elif rsi < 45:
        rsi_score = 0.3
    elif rsi <= 55:
        rsi_score = 0.0
    elif rsi <= 65:
        rsi_score = -0.2
    elif rsi <= 75:
        rsi_score = -0.6
    else:
        rsi_score = -1.0
    rsi_score = rsi_score * 0.6 + multi_day.get("rsi_trend", 0) * 0.4

    # ── DIM 2: StochRSI + direction (weight 0.12) ──
    if stoch_rsi < 15:
        stoch_score = 1.0
    elif stoch_rsi < 25:
        stoch_score = 0.7
    elif stoch_rsi < 40:
        stoch_score = 0.3
    elif stoch_rsi <= 60:
        stoch_score = 0.0
    elif stoch_rsi <= 75:
        stoch_score = -0.3
    elif stoch_rsi <= 85:
        stoch_score = -0.6
    else:
        stoch_score = -1.0
    stoch_score = stoch_score * 0.6 + multi_day.get("stoch_trend", 0) * 0.4

    # ── DIM 3: MACD crossover + hist trend (weight 0.15) ──
    if macd_cross_bull:
        macd_score = 0.9
    elif macd_converging:
        macd_score = 0.5
    elif macd_cross_bear:
        macd_score = -0.8
    elif macd_hist > 0:
        macd_score = 0.2
    else:
        macd_score = -0.3
    macd_score = macd_score * 0.6 + multi_day.get("macd_trend", 0) * 0.4

    # ── DIM 4: Bollinger position (weight 0.10) ──
    if bb_pct < 0.05:
        bb_score = 0.9
    elif bb_pct < 0.15:
        bb_score = 0.6
    elif bb_pct < 0.35:
        bb_score = 0.2
    elif bb_pct <= 0.65:
        bb_score = 0.0
    elif bb_pct <= 0.85:
        bb_score = -0.3
    elif bb_pct <= 0.95:
        bb_score = -0.6
    else:
        bb_score = -0.9
    bb_score = bb_score * 0.7 + multi_day.get("bb_trend", 0) * 0.3

    # ── DIM 5: Trend (SMA50 + EMA alignment) (weight 0.12) ──
    trend_score = 0.0
    if above_sma50:
        if ema9 > ema21 > sma50:
            trend_score = 0.7  # Perfect EMA stacking
        elif ema9 > ema21:
            trend_score = 0.5
        else:
            trend_score = 0.3
    else:
        if ema9 < ema21 < sma50:
            trend_score = -0.7
        elif ema9 < ema21:
            trend_score = -0.5
        else:
            trend_score = -0.3
    if trend_50d > 10:
        trend_score = min(1.0, trend_score + 0.2)
    elif trend_50d < -10:
        trend_score = max(-1.0, trend_score - 0.2)

    # ── DIM 6: Volume + accumulation (weight 0.12) ──
    if vol_ratio > 3.0:
        vol_score = 0.7
    elif vol_ratio > 2.0:
        vol_score = 0.5
    elif vol_ratio > 1.2:
        vol_score = 0.2
    elif vol_ratio > 0.8:
        vol_score = 0.0
    elif vol_ratio > 0.5:
        vol_score = -0.2
    else:
        vol_score = -0.5
    vol_score = (vol_score * 0.5
                 + volume_pattern.get("score", 0) * 0.3
                 + multi_day.get("volume_trend", 0) * 0.2)

    # ── DIM 7: Candlestick patterns (weight 0.08) ──
    candle_score = candle_patterns.get("score", 0.0)

    # ── DIM 8: Pullback-in-uptrend (weight 0.08) ──
    pullback_score = 0.0
    if above_sma50 and trend_50d > 0:
        if 40 <= rsi <= 60 and 25 <= stoch_rsi <= 70:
            pullback_score += 0.4
            if macd_converging or (macd_hist < 0 and multi_day.get("macd_trend", 0) > 0):
                pullback_score += 0.3
            if bb_pct < 0.4:
                pullback_score += 0.2
            if vol_ratio > 0.8:
                pullback_score += 0.1
        pullback_score = min(1.0, pullback_score)
    elif not above_sma50 and trend_50d > -3:
        if rsi < 40 and stoch_rsi < 30:
            pullback_score = 0.2

    # ── DIM 9: Historical accuracy (weight 0.08) ──
    hist_score = history_score.get("score", 0.0)

    # ════ COMPOSITE SCORE ════
    composite = (
        rsi_score * 0.15
        + stoch_score * 0.12
        + macd_score * 0.15
        + bb_score * 0.10
        + trend_score * 0.12
        + vol_score * 0.12
        + candle_score * 0.08
        + pullback_score * 0.08
        + hist_score * 0.08
    )
    score = round(composite * 100, 1)

    # ════ BUILD REASONING ════
    dimension_scores = [
        ("RSI", rsi_score, f"RSI {rsi:.1f}"),
        ("StochRSI", stoch_score, f"StochRSI {stoch_rsi:.1f}"),
        ("MACD", macd_score,
         f"MACD {'BULL cross' if macd_cross_bull else 'converging' if macd_converging else 'BEAR cross' if macd_cross_bear else 'bullish' if macd_hist > 0 else 'bearish'}"),
        ("BB", bb_score, f"BB at {bb_pct * 100:.0f}%"),
        ("Trend", trend_score,
         f"{'Above' if above_sma50 else 'Below'} SMA50 ({sma50:.1f}), 50d {trend_50d}%"),
        ("Volume", vol_score, f"Vol {vol_ratio:.1f}x, {volume_pattern.get('signal', '')}"),
        ("Candle", candle_score, candle_patterns.get("pattern", "none").replace("_", " ").title()),
        ("Pullback", pullback_score, "pullback setup" if pullback_score > 0.3 else ""),
        ("History", hist_score, history_score.get("reason", "")),
    ]
    top = sorted(dimension_scores, key=lambda x: abs(x[1]), reverse=True)

    # ════ ACTION CLASSIFICATION ════
    if score > 55:
        action = "BUY (strong)"
        bullish = [s[2] for s in top if s[1] > 0.3][:4]
        reason_str = " + ".join(bullish) + ". Strong multi-signal confirmation."
        wait = "NOW (Sun/Mon)"

    elif score > 35:
        action = "BUY"
        bullish = [s[2] for s in top if s[1] > 0.2][:3]
        reason_str = " + ".join(bullish) + ". Good setup with confirmation signals."
        wait = "NOW (Sun/Mon)"

    elif score > 20 and pullback_score > 0.4:
        action = "BUY on pullback"
        reason_str = (
            f"Uptrend pullback: Above SMA50 ({sma50:.1f}), RSI {rsi:.1f}, "
            f"StochRSI {stoch_rsi:.1f}. "
            f"MACD {'improving' if macd_converging or multi_day.get('macd_trend', 0) > 0 else 'flat'}. "
            f"Entry on dip to BB lower ({bb_lower:.1f}) or support ({support})."
        )
        wait = "1-3 days"

    elif score > 15 and bb_pct < 0.35 and above_sma50:
        action = "BUY on dip"
        reason_str = (
            f"BB at {bb_pct * 100:.0f}% in uptrend. RSI {rsi:.1f}, StochRSI {stoch_rsi:.1f}. "
            f"Bounce rate {bounce_rate}%. Set limit buy at {bb_lower:.1f}-{support}."
        )
        wait = "1-5 days"

    elif score > 5 and macd_converging:
        action = "BUY (wait for MACD cross)"
        reason_str = (
            f"MACD converging (hist={macd_hist:.3f}). RSI {rsi:.1f}, StochRSI {stoch_rsi:.1f}. "
            f"{'Above' if above_sma50 else 'Below'} SMA50. Wait for MACD bull cross."
        )
        wait = "3-7 days"

    elif score > -15:
        action = "HOLD/WAIT"
        pos = [s[2] for s in top if s[1] > 0.1 and s[2]][:2]
        neg = [s[2] for s in top if s[1] < -0.1 and s[2]][:2]
        parts = []
        if pos:
            parts.append("Positives: " + ", ".join(pos))
        if neg:
            parts.append("Negatives: " + ", ".join(neg))
        reason_str = ". ".join(parts) + ". No clear entry trigger. Watch for StochRSI<20 + MACD cross."
        wait = "5-15 days"

    elif score > -35:
        action = "SELL/AVOID"
        bearish = [s[2] for s in top if s[1] < -0.2 and s[2]][:3]
        reason_str = " + ".join(bearish) + ". Bearish signals dominate. Wait for correction."
        wait = "10-20 days"

    else:
        action = "AVOID"
        bearish = [s[2] for s in top if s[1] < -0.3 and s[2]][:3]
        reason_str = " + ".join(bearish) + ". Multiple bearish confirmations."
        wait = "15-30 days"

    # Append history insight
    hist_reason = history_score.get("reason", "")
    if hist_reason and hist_score != 0:
        modifier = "boosted" if hist_score > 0 else "reduced"
        reason_str += f" Signal {modifier} by {hist_reason}."

    # Append candlestick pattern
    if candle_patterns.get("pattern", "none") != "none":
        pattern_name = candle_patterns["pattern"].replace("_", " ").title()
        reason_str += f" Candle: {pattern_name}."

    return action, reason_str, wait, score


# ════════════════════════════════════════════════════════════
#  ENTRY / EXIT / SCENARIOS
# ════════════════════════════════════════════════════════════


def _compute_entry_exit(*, action, ltp, atr, bb_lower, ema21, low_5d, support):
    """Compute entry range, stop loss, and targets."""
    atr = atr or ltp * 0.02

    if "BUY" in action and "AVOID" not in action:
        entry_low = round(max(bb_lower, low_5d - atr * 0.2), 1)
        entry_high = round(min(ema21, ltp + atr * 0.3), 1)
        if entry_low > entry_high:
            entry_low, entry_high = entry_high, entry_low
        sl = round(entry_low - atr * 1.5, 1)
        t1 = round(entry_high + atr * 1.5, 1)
        t2 = round(entry_high + atr * 2.5, 1)
    elif "HOLD" in action or "WAIT" in action:
        pullback = round(min(ema21, bb_lower + atr * 0.5), 1)
        entry_low = round(min(pullback, low_5d - atr * 0.2), 1)
        entry_high = round(pullback + atr * 0.3, 1)
        if entry_low > entry_high:
            entry_low, entry_high = entry_high, entry_low
        sl = round(entry_low - atr * 1.5, 1)
        t1 = round(entry_high + atr * 1.5, 1)
        t2 = round(entry_high + atr * 2.5, 1)
    else:
        ideal = round(min(bb_lower, support + atr * 0.5), 1)
        entry_low = ideal
        entry_high = round(ideal + atr * 0.3, 1)
        sl = round(ideal - atr * 1.5, 1)
        t1 = round(ideal + atr * 1.0, 1)
        t2 = round(ideal + atr * 2.0, 1)

    return entry_low, entry_high, sl, t1, t2


def _next_trading_day(dt: datetime, n: int = 1) -> datetime:
    """Advance `n` DSE trading days (skip Fri/Sat)."""
    count = 0
    while count < n:
        dt += timedelta(days=1)
        if dt.weekday() not in (4, 5):  # 4=Fri, 5=Sat
            count += 1
    return dt


def _compute_timing(*, action: str, atr: float, ltp: float,
                    entry_mid: float, t1: float, t2: float,
                    volatility: float, score: float) -> dict:
    """Compute concrete entry and exit date windows.

    Returns dict with entry_start, entry_end, exit_t1_by, exit_t2_by
    as YYYY-MM-DD strings.
    """
    today = datetime.now(DSE_TZ).date()
    base = datetime.combine(today, datetime.min.time())

    # ── Entry window (trading days) ──
    # Based on action urgency
    if "strong" in action.lower() or action == "BUY":
        entry_start_days, entry_end_days = 0, 1  # next 1 trading day
    elif "pullback" in action.lower():
        entry_start_days, entry_end_days = 1, 3
    elif "dip" in action.lower():
        entry_start_days, entry_end_days = 1, 5
    elif "MACD" in action:
        entry_start_days, entry_end_days = 3, 7
    elif "HOLD" in action or "WAIT" in action:
        entry_start_days, entry_end_days = 5, 15
    elif "SELL" in action:
        entry_start_days, entry_end_days = 10, 20
    else:  # AVOID
        entry_start_days, entry_end_days = 15, 30

    entry_start = _next_trading_day(base, max(entry_start_days, 1))
    entry_end = _next_trading_day(base, entry_end_days)

    # ── Exit timing: estimate days to reach targets ──
    # Use ATR-based projection: price moves ~0.5-1.0 ATR per day on avg
    daily_move = max(atr * 0.5, ltp * 0.005)  # conservative: 0.5 ATR/day

    dist_t1 = abs(t1 - entry_mid) if t1 > 0 and entry_mid > 0 else atr * 1.5
    dist_t2 = abs(t2 - entry_mid) if t2 > 0 and entry_mid > 0 else atr * 2.5

    days_to_t1 = max(2, min(30, round(dist_t1 / daily_move)))
    days_to_t2 = max(3, min(60, round(dist_t2 / daily_move)))

    # High-volatility stocks reach faster
    if volatility and volatility > 3:
        days_to_t1 = max(2, int(days_to_t1 * 0.7))
        days_to_t2 = max(3, int(days_to_t2 * 0.7))

    # Entry midpoint for exit calculation
    entry_mid_day = max(entry_start_days, 1)
    exit_t1 = _next_trading_day(base, entry_mid_day + days_to_t1)
    exit_t2 = _next_trading_day(base, entry_mid_day + days_to_t2)

    return {
        "entry_start": entry_start.strftime("%Y-%m-%d"),
        "entry_end": entry_end.strftime("%Y-%m-%d"),
        "exit_t1_by": exit_t1.strftime("%Y-%m-%d"),
        "exit_t2_by": exit_t2.strftime("%Y-%m-%d"),
        "hold_days_t1": days_to_t1,
        "hold_days_t2": days_to_t2,
    }


def _generate_scenarios(*, symbol, action, entry_low, entry_high, sl, bb_lower, support, vol_entry):
    """Generate 3 entry scenarios."""
    el, eh = entry_low, entry_high
    return [
        {
            "name": f"Flat/Green Open ({el}-{eh})",
            "steps": [
                f"Wait 15 min. Check volume > {vol_entry}.",
                f"If volume strong + price holds above {el} → BUY at market.",
                f"If volume weak → place limit order at {el}.",
                f"Set stop loss at {sl} immediately.",
            ],
        },
        {
            "name": f"Small Dip 1-2% to {round(el * 0.98, 1)}-{el}",
            "steps": [
                "IDEAL entry — better price, same targets.",
                "Wait for green 5-min candle to confirm reversal.",
                f"Buy at {round(el * 0.99, 1)}. Stop loss stays at {sl}.",
                f"If holds BB lower ({round(bb_lower, 1)}), add more.",
            ],
        },
        {
            "name": f"Gap Down >2% below {round(el * 0.97, 1)}",
            "steps": [
                "DO NOT buy immediately. Wait 30-60 min.",
                f"If holds above support ({support}) with volume → enter 50% size.",
                f"If breaks support {support} → SKIP entirely.",
                "Market panic days: keep cash, buy nothing.",
            ],
        },
    ]


# ════════════════════════════════════════════════════════════
#  PERSISTENCE
# ════════════════════════════════════════════════════════════


def save_daily_analysis(analysis: list[dict], date_str: str | None = None):
    """Persist daily analysis to database."""
    if not date_str:
        date_str = datetime.now(DSE_TZ).strftime("%Y-%m-%d")

    conn = get_connection()

    # Ensure new columns exist (migration-safe)
    for col, ctype in [
        ("category", "TEXT"),
        ("entry_start", "DATE"),
        ("entry_end", "DATE"),
        ("exit_t1_by", "DATE"),
        ("exit_t2_by", "DATE"),
        ("hold_days_t1", "INTEGER"),
        ("hold_days_t2", "INTEGER"),
        ("prediction_json", "TEXT"),
    ]:
        try:
            conn.execute(f"ALTER TABLE daily_analysis ADD COLUMN IF NOT EXISTS {col} {ctype}")
            conn.commit()
        except Exception:
            conn.rollback()

    saved = 0
    for a in analysis:
        try:
            conn.execute(
                """INSERT INTO daily_analysis
                   (date, symbol, action, reasoning, entry_low, entry_high, sl, t1, t2,
                    risk_pct, reward_pct, rsi, stoch_rsi, macd_line, macd_signal, macd_hist,
                    macd_status, bb_pct, atr, atr_pct, volatility, max_dd, support, resistance,
                    trend_50d, avg_vol, vol_ratio, wait_days, vol_entry,
                    entry_start, entry_end, exit_t1_by, exit_t2_by, hold_days_t1, hold_days_t2,
                    scenarios_json, last_5_json, ltp, score, category, prediction_json)
                   VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?,
                           ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                   ON CONFLICT (date, symbol) DO UPDATE SET
                     action=EXCLUDED.action, reasoning=EXCLUDED.reasoning,
                     entry_low=EXCLUDED.entry_low, entry_high=EXCLUDED.entry_high,
                     sl=EXCLUDED.sl, t1=EXCLUDED.t1, t2=EXCLUDED.t2,
                     risk_pct=EXCLUDED.risk_pct, reward_pct=EXCLUDED.reward_pct,
                     rsi=EXCLUDED.rsi, stoch_rsi=EXCLUDED.stoch_rsi,
                     macd_line=EXCLUDED.macd_line, macd_signal=EXCLUDED.macd_signal,
                     macd_hist=EXCLUDED.macd_hist, macd_status=EXCLUDED.macd_status,
                     bb_pct=EXCLUDED.bb_pct, atr=EXCLUDED.atr, atr_pct=EXCLUDED.atr_pct,
                     volatility=EXCLUDED.volatility, max_dd=EXCLUDED.max_dd,
                     support=EXCLUDED.support, resistance=EXCLUDED.resistance,
                     trend_50d=EXCLUDED.trend_50d, avg_vol=EXCLUDED.avg_vol,
                     vol_ratio=EXCLUDED.vol_ratio, wait_days=EXCLUDED.wait_days,
                     vol_entry=EXCLUDED.vol_entry,
                     entry_start=EXCLUDED.entry_start, entry_end=EXCLUDED.entry_end,
                     exit_t1_by=EXCLUDED.exit_t1_by, exit_t2_by=EXCLUDED.exit_t2_by,
                     hold_days_t1=EXCLUDED.hold_days_t1, hold_days_t2=EXCLUDED.hold_days_t2,
                     scenarios_json=EXCLUDED.scenarios_json,
                     last_5_json=EXCLUDED.last_5_json, ltp=EXCLUDED.ltp, score=EXCLUDED.score,
                     category=EXCLUDED.category, prediction_json=EXCLUDED.prediction_json""",
                (
                    date_str, a["symbol"], a["action"], a["reasoning"],
                    a["entry_low"], a["entry_high"], a["sl"], a["t1"], a["t2"],
                    a["risk_pct"], a["reward_pct"], a["rsi"], a["stoch_rsi"],
                    a["macd_line"], a["macd_signal"], a["macd_hist"], a["macd_status"],
                    a["bb_pct"], a["atr"], a["atr_pct"], a["volatility"], a["max_dd"],
                    a["support"], a["resistance"], a["trend_50d"],
                    a["avg_vol"], a["vol_ratio"], a["wait_days"], a["vol_entry"],
                    a["entry_start"], a["entry_end"], a["exit_t1_by"], a["exit_t2_by"],
                    a["hold_days_t1"], a["hold_days_t2"],
                    a["scenarios_json"], a["last_5_json"], a["ltp"], a.get("score", 0),
                    a.get("category", ""), a.get("prediction_json"),
                ),
            )
            saved += 1
        except Exception as e:
            logger.error(f"Save daily analysis error for {a['symbol']}: {e}")

    conn.commit()
    conn.close()
    logger.info(f"Saved daily analysis for {saved} stocks on {date_str}")


def load_daily_analysis(date_str: str | None = None, action_filter: str | None = None) -> list[dict]:
    """Load daily analysis from DB, enriched with sector/category from fundamentals."""
    if not date_str:
        date_str = datetime.now(DSE_TZ).strftime("%Y-%m-%d")

    conn = get_connection()
    sql = """SELECT da.*, f.sector, f.category AS fund_category
             FROM daily_analysis da
             LEFT JOIN fundamentals f ON da.symbol = f.symbol
             WHERE da.date = ?"""
    params = [date_str]

    if action_filter:
        sql += " AND da.action LIKE ?"
        params.append(f"%{action_filter}%")

    sql += " ORDER BY da.score DESC"
    rows = conn.execute(sql, params).fetchall()
    conn.close()

    results = []
    for r in rows:
        d = dict(r)
        # Use fundamentals category as fallback
        if not d.get("category") and d.get("fund_category"):
            d["category"] = d.pop("fund_category")
        else:
            d.pop("fund_category", None)
        # Convert date/datetime to strings
        if hasattr(d.get("date"), "isoformat"):
            d["date"] = str(d["date"])
        if hasattr(d.get("created_at"), "isoformat"):
            d["created_at"] = d["created_at"].isoformat()
        # Sanitize NaN/Inf floats
        for k, v in d.items():
            if isinstance(v, float) and (math.isnan(v) or math.isinf(v)):
                d[k] = None
        # Parse JSON fields
        for jfield in ("scenarios_json", "last_5_json", "prediction_json"):
            if d.get(jfield):
                try:
                    d[jfield] = json.loads(d[jfield])
                except (json.JSONDecodeError, TypeError):
                    pass
        results.append(d)
    return results


def get_available_dates() -> list[str]:
    """Get list of dates that have analysis."""
    conn = get_connection()
    rows = conn.execute(
        "SELECT DISTINCT date FROM daily_analysis ORDER BY date DESC LIMIT 30"
    ).fetchall()
    conn.close()
    return [str(r["date"]) for r in rows]


def run_daily_analysis():
    """Full pipeline: generate + save + log. Called by scheduler."""
    try:
        logger.info("Running daily analysis pipeline v2...")
        analysis = generate_daily_analysis()
        if not analysis:
            logger.warning("Daily analysis produced no results")
            return

        today = datetime.now(DSE_TZ).strftime("%Y-%m-%d")
        save_daily_analysis(analysis, today)

        counts = {}
        for a in analysis:
            act = a["action"]
            counts[act] = counts.get(act, 0) + 1
        logger.info(f"Daily analysis complete: {len(analysis)} stocks — {counts}")

    except Exception as e:
        logger.error(f"Daily analysis pipeline failed: {e}")


# ─── Helpers ───

def _safe(val) -> float:
    """Safe float extraction from pandas."""
    if val is None or (isinstance(val, float) and (np.isnan(val) or np.isinf(val))):
        return 0.0
    try:
        return float(val)
    except (ValueError, TypeError):
        return 0.0


def _max_drawdown(series: pd.Series) -> float:
    """Compute max drawdown percentage."""
    peak = series.expanding().max()
    dd = (series - peak) / peak * 100
    return abs(float(dd.min())) if len(dd) > 0 else 0.0


if __name__ == "__main__":
    import sys
    logging.basicConfig(level=logging.INFO)
    results = generate_daily_analysis()
    print(f"\nTotal: {len(results)} stocks analyzed")
    for r in results[:30]:
        print(f"  {r['symbol']:15s} [{r.get('category','?'):1s}] {r['action']:25s} "
              f"LTP={r['ltp']:>8.1f}  Entry={r['entry_low']}-{r['entry_high']}  "
              f"Score={r.get('score', 0):.1f}")
