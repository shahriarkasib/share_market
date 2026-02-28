"""Daily post-market analysis engine.

Screens all A-category stocks, classifies BUY/WAIT/AVOID,
computes entry/exit/SL/targets, and generates rule-based reasoning.
Runs automatically after market close (15:00 BST) via scheduler.
"""

import json
import logging
from datetime import datetime

import numpy as np
import pandas as pd
import pytz

from analysis.indicators import TechnicalIndicators
from data.repository import (
    get_a_category_symbols,
    read_all_historical_grouped,
    read_historical_for_symbol,
)
from database import get_connection

logger = logging.getLogger(__name__)
DSE_TZ = pytz.timezone("Asia/Dhaka")


def generate_daily_analysis() -> list[dict]:
    """Run full daily analysis on all eligible stocks.

    Returns list of analysis dicts sorted by action priority then score.
    """
    logger.info("Starting daily analysis...")

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

    # 2. Get A-category symbols
    a_symbols = set(get_a_category_symbols())
    logger.info(f"A-category symbols: {len(a_symbols)}")

    # 3. Filter: A-category + min turnover
    # bdshare reports value in millions BDT — 0.5 = 5 lakh
    MIN_TURNOVER = 0.5
    eligible = {}
    for sym, lp in live_prices.items():
        if sym not in a_symbols:
            continue
        turnover = lp.get("value", 0) or 0
        if turnover < MIN_TURNOVER:
            continue
        eligible[sym] = lp

    logger.info(f"Eligible stocks (A-cat + turnover): {len(eligible)}")

    # 4. Load all historical data in one batch
    all_history = read_all_historical_grouped(min_rows_per_symbol=30)

    # 5. Analyze each stock
    results = []
    for sym, lp in eligible.items():
        try:
            df = all_history.get(sym)
            if df is None or len(df) < 30:
                continue
            analysis = _analyze_stock(sym, df, lp)
            if analysis:
                results.append(analysis)
        except Exception as e:
            logger.error(f"Analysis failed for {sym}: {e}")

    logger.info(f"Analyzed {len(results)} stocks")

    # 6. Sort by action priority then score
    action_order = {
        "BUY": 0,
        "BUY (wait for MACD cross)": 1,
        "BUY on dip": 2,
        "HOLD/WAIT": 3,
        "SELL/AVOID": 4,
        "AVOID": 5,
    }
    results.sort(key=lambda x: (action_order.get(x["action"], 9), -x.get("score", 0)))

    return results


def _analyze_stock(symbol: str, df: pd.DataFrame, live: dict) -> dict | None:
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
    macd_signal = _safe(latest.get("macd_signal"))
    macd_hist = _safe(latest.get("macd_histogram"))
    prev_macd_hist = _safe(prev.get("macd_histogram"))
    ema9 = _safe(latest.get("ema_9"))
    ema21 = _safe(latest.get("ema_21"))
    sma50 = _safe(latest.get("sma_50"))
    bb_lower = _safe(latest.get("bb_lower"))
    bb_upper = _safe(latest.get("bb_upper"))
    bb_mid = _safe(latest.get("bb_middle"))
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

    # Support / resistance (from recent price action)
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

    # ─── Classification ───
    action, reasoning, wait_days, score = _classify_stock(
        ltp=ltp, rsi=rsi, stoch_rsi=stoch_k, macd_hist=macd_hist,
        macd_cross_bull=macd_cross_bull, macd_cross_bear=macd_cross_bear,
        macd_converging=macd_converging, above_sma50=above_sma50,
        bb_pct=bb_pct, trend_50d=trend_50d, volatility=volatility,
        max_dd=max_dd, vol_ratio=vol_ratio, bounce_rate=bounce_rate,
        atr_pct=atr_pct, ema9=ema9, ema21=ema21, sma50=sma50,
        bb_lower=bb_lower, support=support, resistance=resistance,
        macd_line=macd_line, macd_signal=macd_signal,
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

    return {
        "symbol": symbol,
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
        "macd_signal": round(macd_signal, 3) if macd_signal else 0,
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
        "scenarios_json": json.dumps(scenarios),
        "last_5_json": json.dumps(last_5_data),
    }


def _classify_stock(
    *, ltp, rsi, stoch_rsi, macd_hist, macd_cross_bull, macd_cross_bear,
    macd_converging, above_sma50, bb_pct, trend_50d, volatility, max_dd,
    vol_ratio, bounce_rate, atr_pct, ema9, ema21, sma50,
    bb_lower, support, resistance, macd_line, macd_signal,
) -> tuple[str, str, str, float]:
    """Classify stock into action category with reasoning and score."""
    rsi = rsi or 50
    stoch_rsi = stoch_rsi or 50

    # Composite score for ranking within category
    score = 0.0

    # ─── AVOID: overbought or broken trend ───
    if rsi > 75 or stoch_rsi > 90:
        reason = f"RSI {rsi:.1f} extremely overbought"
        if stoch_rsi > 90:
            reason += f", StochRSI {stoch_rsi:.1f} maxed out"
        reason += ". Must correct before entry. Wait for RSI<60 + StochRSI<30."
        return "AVOID", reason, "15-30 days", -rsi

    if rsi > 70 or (stoch_rsi > 85 and rsi > 60):
        reason = f"RSI {rsi:.1f}, StochRSI {stoch_rsi:.1f} — overbought zone. "
        if macd_cross_bear:
            reason += "MACD bearish cross confirms selling pressure."
            return "SELL/AVOID", reason, "10-20 days", -(rsi + stoch_rsi) / 2
        reason += "Wait for pullback to RSI<60."
        return "SELL/AVOID", reason, "10-20 days", -(rsi + stoch_rsi) / 2

    if not above_sma50 and trend_50d < -5:
        reason = f"Below SMA50 ({sma50:.1f}) with {trend_50d}% downtrend. "
        reason += "Broken trend — wait for price to reclaim SMA50."
        return "AVOID", reason, "15-30 days", trend_50d - 50

    # ─── BUY: oversold with bullish signals ───
    if rsi < 50 and stoch_rsi < 30 and above_sma50:
        if macd_cross_bull:
            reason = f"MACD bullish crossover + RSI {rsi:.1f} + StochRSI {stoch_rsi:.1f} oversold. "
            reason += f"Above SMA50 ({sma50:.1f}), trend {trend_50d}%. Strong entry setup."
            score = 100 - rsi + (30 - stoch_rsi) + (20 if bounce_rate > 55 else 0)
            return "BUY", reason, "NOW (Sun/Mon)", score

        if macd_converging:
            reason = f"MACD converging toward bullish cross. RSI {rsi:.1f}, StochRSI {stoch_rsi:.1f}. "
            reason += f"BB at {bb_pct*100:.0f}%. Wait for MACD to actually cross, then buy."
            score = 80 - rsi + (30 - stoch_rsi)
            return "BUY (wait for MACD cross)", reason, "2-5 days", score

        if bb_pct < 0.2:
            reason = f"Near BB lower ({bb_pct*100:.0f}%). RSI {rsi:.1f}, StochRSI {stoch_rsi:.1f}. "
            reason += f"Bounce rate {bounce_rate}%. Buy on next green candle confirmation."
            score = 70 - rsi + (1 - bb_pct) * 30
            return "BUY", reason, "NOW (Sun/Mon)", score

    # ─── BUY on dip ───
    if rsi < 60 and above_sma50 and trend_50d > 0:
        if stoch_rsi < 20:
            reason = f"StochRSI {stoch_rsi:.1f} deeply oversold in uptrend ({trend_50d}%). "
            reason += f"RSI {rsi:.1f} healthy. MACD {'bullish' if macd_hist > 0 else 'bearish'}. "
            reason += f"Buy on dip to BB lower ({bb_lower:.1f}) or support ({support})."
            score = 60 + (20 - stoch_rsi) + trend_50d * 0.5
            return "BUY on dip", reason, "1-5 days", score

        if bb_pct < 0.35 and bounce_rate > 50:
            reason = f"BB at {bb_pct*100:.0f}% in uptrend. RSI {rsi:.1f}, StochRSI {stoch_rsi:.1f}. "
            reason += f"Bounce rate {bounce_rate}%. Set limit buy at {bb_lower:.1f}-{support}."
            score = 50 + (1 - bb_pct) * 20 + bounce_rate * 0.2
            return "BUY on dip", reason, "1-5 days", score

    # ─── WAIT for MACD ───
    if above_sma50 and macd_converging and rsi < 65:
        reason = f"MACD converging (hist={macd_hist:.3f}, improving). RSI {rsi:.1f}, StochRSI {stoch_rsi:.1f}. "
        reason += "Trend intact above SMA50. Wait for MACD bullish crossover to confirm entry."
        score = 40 + (65 - rsi) * 0.5
        return "BUY (wait for MACD cross)", reason, "3-7 days", score

    # ─── HOLD/WAIT: uptrend but not oversold enough ───
    if above_sma50:
        macd_label = (
            "BULL cross" if macd_cross_bull
            else "BEAR cross" if macd_cross_bear
            else "Converging" if macd_converging
            else "Bullish" if macd_hist > 0
            else "Bearish"
        )
        if rsi > 65 or stoch_rsi > 65:
            wait = "7-15 days"
            reason = f"RSI {rsi:.1f}, StochRSI {stoch_rsi:.1f} — elevated, not oversold. "
            reason += f"MACD {macd_label}. Above SMA50 but need RSI<60 + StochRSI<30 pullback. "
            reason += f"Target entry at EMA21 ({ema21:.1f}) or BB lower ({bb_lower:.1f})."
        elif macd_cross_bear:
            wait = "7-14 days"
            reason = f"MACD bearish cross — selling momentum. RSI {rsi:.1f}, StochRSI {stoch_rsi:.1f}. "
            reason += f"Wait for MACD to turn neutral then bullish. Entry at BB lower ({bb_lower:.1f})."
        else:
            wait = "5-10 days"
            reason = f"RSI {rsi:.1f}, StochRSI {stoch_rsi:.1f}, MACD {macd_label}. "
            reason += f"Uptrend intact (above SMA50 {sma50:.1f}) but no clear entry trigger. "
            reason += f"Watch for StochRSI<20 bounce + MACD bull cross."
        score = 20 - abs(rsi - 50) * 0.3
        return "HOLD/WAIT", reason, wait, score

    # ─── Default: no signal ───
    reason = f"Below SMA50 ({sma50:.1f}). RSI {rsi:.1f}, StochRSI {stoch_rsi:.1f}. "
    reason += "No bullish signal. Wait for trend reversal."
    return "AVOID", reason, "15-30 days", -30


def _compute_entry_exit(*, action, ltp, atr, bb_lower, ema21, low_5d, support):
    """Compute entry range, stop loss, and targets."""
    atr = atr or ltp * 0.02  # fallback 2% of price

    if "BUY" in action and "AVOID" not in action:
        # Entry near BB lower or recent low
        entry_low = round(max(bb_lower, low_5d - atr * 0.2), 1)
        entry_high = round(min(ema21, ltp + atr * 0.3), 1)
        if entry_low > entry_high:
            entry_low, entry_high = entry_high, entry_low
        sl = round(entry_low - atr * 1.5, 1)
        t1 = round(entry_high + atr * 1.5, 1)
        t2 = round(entry_high + atr * 2.5, 1)
    elif "HOLD" in action or "WAIT" in action:
        # Conservative: entry on deeper pullback
        pullback = round(min(ema21, bb_lower + atr * 0.5), 1)
        entry_low = round(min(pullback, low_5d - atr * 0.2), 1)
        entry_high = round(pullback + atr * 0.3, 1)
        if entry_low > entry_high:
            entry_low, entry_high = entry_high, entry_low
        sl = round(entry_low - atr * 1.5, 1)
        t1 = round(entry_high + atr * 1.5, 1)
        t2 = round(entry_high + atr * 2.5, 1)
    else:
        # AVOID: show "if corrects to X" levels
        ideal = round(min(bb_lower, support + atr * 0.5), 1)
        entry_low = ideal
        entry_high = round(ideal + atr * 0.3, 1)
        sl = round(ideal - atr * 1.5, 1)
        t1 = round(ideal + atr * 1.0, 1)
        t2 = round(ideal + atr * 2.0, 1)

    return entry_low, entry_high, sl, t1, t2


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
            "name": f"Small Dip 1-2% to {round(el*0.98,1)}-{el}",
            "steps": [
                "IDEAL entry — better price, same targets.",
                "Wait for green 5-min candle to confirm reversal.",
                f"Buy at {round(el*0.99,1)}. Stop loss stays at {sl}.",
                f"If holds BB lower ({round(bb_lower,1)}), add more.",
            ],
        },
        {
            "name": f"Gap Down >2% below {round(el*0.97,1)}",
            "steps": [
                "DO NOT buy immediately. Wait 30-60 min.",
                f"If holds above support ({support}) with volume → enter 50% size.",
                f"If breaks support {support} → SKIP entirely.",
                "Market panic days: keep cash, buy nothing.",
            ],
        },
    ]


def save_daily_analysis(analysis: list[dict], date_str: str | None = None):
    """Persist daily analysis to database."""
    if not date_str:
        date_str = datetime.now(DSE_TZ).strftime("%Y-%m-%d")

    conn = get_connection()
    saved = 0
    for a in analysis:
        try:
            conn.execute(
                """INSERT INTO daily_analysis
                   (date, symbol, action, reasoning, entry_low, entry_high, sl, t1, t2,
                    risk_pct, reward_pct, rsi, stoch_rsi, macd_line, macd_signal, macd_hist,
                    macd_status, bb_pct, atr, atr_pct, volatility, max_dd, support, resistance,
                    trend_50d, avg_vol, vol_ratio, wait_days, vol_entry,
                    scenarios_json, last_5_json, ltp, score)
                   VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?,
                           ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
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
                     vol_entry=EXCLUDED.vol_entry, scenarios_json=EXCLUDED.scenarios_json,
                     last_5_json=EXCLUDED.last_5_json, ltp=EXCLUDED.ltp, score=EXCLUDED.score""",
                (
                    date_str, a["symbol"], a["action"], a["reasoning"],
                    a["entry_low"], a["entry_high"], a["sl"], a["t1"], a["t2"],
                    a["risk_pct"], a["reward_pct"], a["rsi"], a["stoch_rsi"],
                    a["macd_line"], a["macd_signal"], a["macd_hist"], a["macd_status"],
                    a["bb_pct"], a["atr"], a["atr_pct"], a["volatility"], a["max_dd"],
                    a["support"], a["resistance"], a["trend_50d"],
                    a["avg_vol"], a["vol_ratio"], a["wait_days"], a["vol_entry"],
                    a["scenarios_json"], a["last_5_json"], a["ltp"], a.get("score", 0),
                ),
            )
            saved += 1
        except Exception as e:
            logger.error(f"Save daily analysis error for {a['symbol']}: {e}")

    conn.commit()
    conn.close()
    logger.info(f"Saved daily analysis for {saved} stocks on {date_str}")


def load_daily_analysis(date_str: str | None = None, action_filter: str | None = None) -> list[dict]:
    """Load daily analysis from DB."""
    if not date_str:
        date_str = datetime.now(DSE_TZ).strftime("%Y-%m-%d")

    conn = get_connection()
    sql = "SELECT * FROM daily_analysis WHERE date = ?"
    params = [date_str]

    if action_filter:
        sql += " AND action LIKE ?"
        params.append(f"%{action_filter}%")

    sql += " ORDER BY score DESC"
    rows = conn.execute(sql, params).fetchall()
    conn.close()

    results = []
    for r in rows:
        d = dict(r)
        # Convert date/datetime to strings for JSON serialization
        if hasattr(d.get("date"), "isoformat"):
            d["date"] = str(d["date"])
        if hasattr(d.get("created_at"), "isoformat"):
            d["created_at"] = d["created_at"].isoformat()
        # Sanitize NaN/Inf floats (not JSON-serializable)
        import math
        for k, v in d.items():
            if isinstance(v, float) and (math.isnan(v) or math.isinf(v)):
                d[k] = None
        # Parse JSON fields
        for jfield in ("scenarios_json", "last_5_json"):
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
        logger.info("Running daily analysis pipeline...")
        analysis = generate_daily_analysis()
        if not analysis:
            logger.warning("Daily analysis produced no results")
            return

        today = datetime.now(DSE_TZ).strftime("%Y-%m-%d")
        save_daily_analysis(analysis, today)

        # Summary
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
    # Allow running standalone for testing
    import sys
    logging.basicConfig(level=logging.INFO)
    results = generate_daily_analysis()
    print(f"\nTotal: {len(results)} stocks analyzed")
    for r in results[:20]:
        print(f"  {r['symbol']:15s} {r['action']:25s} LTP={r['ltp']:>8.1f}  Entry={r['entry_low']}-{r['entry_high']}  Score={r.get('score',0):.0f}")
