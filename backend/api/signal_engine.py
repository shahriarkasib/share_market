"""Composite signal engine — combines ALL trading methodologies with
**regime-aware weighting** (no fixed weights — different strategies work in
different market conditions) and **entry-distance classification** (so we
distinguish BUY-NOW from BUY-LIMIT-on-pullback from STALE).

Methodologies combined:
  1. SMC (FVG + BOS + Order Blocks)               weight 25
  2. Order Flow (absorption, cumulative delta)     weight 15
  3. Multi-Timeframe Confluence (D + W bias)       weight 15
  4. Liquidity Sweep (failed-breakout reversal)    weight 10
  5. Volume Profile (POC / value area position)    weight  5
  6. BB Squeeze + Volatility Breakout              weight  5
  7. Sector Relative Strength                      weight 10
  8. Fibonacci Confluence (zone at key Fib level)  weight  5
  9. Wyckoff Phase (accumulation, markup, etc.)    weight 10
                                                   total  100

Returns:
  composite_score: 0-100
  signal_level:    NONE | WATCH | BUY | STRONG_BUY
  risk_score:      1 (lowest) — 5 (highest)
  active_signals:  list of methodology names firing
  reasons:         human-readable explanations
"""
from __future__ import annotations

from typing import Optional

import pandas as pd


# ───────────────────────────────────────────────────────────────────────
#  Individual methodology detectors that aren't already in smc_chart.py
# ───────────────────────────────────────────────────────────────────────

def detect_mtf_confluence(df: pd.DataFrame) -> dict:
    """Check daily + weekly bias alignment.

    Daily bias: last 20-bar slope. Weekly bias: resample to weekly, last 8-week slope.
    Returns:
        {
          "daily_bias": "up"|"down"|"flat",
          "weekly_bias": "up"|"down"|"flat",
          "aligned": bool,            # both same direction
          "score": 0-100              # 100 if both up, 50 if mixed, 0 if both down
        }
    """
    if len(df) < 60:
        return {"daily_bias": "flat", "weekly_bias": "flat", "aligned": False, "score": 0}

    closes = df["close"].astype(float)
    # Daily: 20-bar slope
    daily_chg = (closes.iloc[-1] - closes.iloc[-20]) / closes.iloc[-20] * 100
    daily = "up" if daily_chg > 2 else ("down" if daily_chg < -2 else "flat")

    # Weekly: resample
    if "date" in df.columns:
        wf = df.set_index("date")["close"].resample("W").last().dropna()
        if len(wf) >= 8:
            weekly_chg = (wf.iloc[-1] - wf.iloc[-8]) / wf.iloc[-8] * 100
            weekly = "up" if weekly_chg > 4 else ("down" if weekly_chg < -4 else "flat")
        else:
            weekly = "flat"
    else:
        weekly = "flat"

    aligned = daily == weekly == "up"
    if daily == "up" and weekly == "up":
        score = 100
    elif daily == "up" or weekly == "up":
        score = 60
    elif daily == "flat" and weekly == "flat":
        score = 30
    else:
        score = 0

    return {"daily_bias": daily, "weekly_bias": weekly, "aligned": aligned, "score": score}


def detect_liquidity_sweep(df: pd.DataFrame, swings: list) -> dict:
    """Detect wick that takes out a recent swing high/low and reverses.

    Bullish sweep: today's low < recent swing low AND close > swing low (reclaim).
    Bearish sweep: today's high > recent swing high AND close < swing high (rejection).
    """
    if len(df) < 5 or not swings:
        return {"detected": False, "type": None, "score": 0}

    last_bar = df.iloc[-1]
    h, l, c = float(last_bar["high"]), float(last_bar["low"]), float(last_bar["close"])

    # Recent swings (last 30 bars)
    recent = [s for s in swings if s["idx"] >= len(df) - 30]
    swing_lows = [s["price"] for s in recent if s["type"] == "low"]
    swing_highs = [s["price"] for s in recent if s["type"] == "high"]

    if swing_lows:
        nearest_low = min(swing_lows, key=lambda x: abs(x - l))
        if l < nearest_low * 0.999 and c > nearest_low * 1.002:
            return {
                "detected": True, "type": "bullish_sweep",
                "swept_level": round(nearest_low, 2),
                "score": 100,
            }
    if swing_highs:
        nearest_high = max(swing_highs, key=lambda x: -abs(x - h))
        if h > nearest_high * 1.001 and c < nearest_high * 0.998:
            return {
                "detected": True, "type": "bearish_sweep",
                "swept_level": round(nearest_high, 2),
                "score": 0,  # bearish — counts negatively for buy signal
            }
    return {"detected": False, "type": None, "score": 50}


def detect_bb_squeeze(df: pd.DataFrame, period: int = 20) -> dict:
    """BB Squeeze: bandwidth compressed below 50% of its 100-bar mean.
    Breakout: close > upper band on volume = bullish breakout."""
    if len(df) < 100:
        return {"squeezing": False, "broke_out": False, "score": 50}
    closes = df["close"].astype(float)
    mean = closes.rolling(period).mean()
    std = closes.rolling(period).std()
    upper = mean + 2 * std
    lower = mean - 2 * std
    bandwidth = (upper - lower) / mean * 100
    avg_bw = bandwidth.iloc[-100:].mean()
    cur_bw = bandwidth.iloc[-1]
    squeezing = cur_bw < avg_bw * 0.5

    last_close = closes.iloc[-1]
    last_upper = upper.iloc[-1]
    broke_out = last_close > last_upper and bandwidth.iloc[-2] < avg_bw * 0.6

    if broke_out:
        score = 100
    elif squeezing:
        score = 70  # squeeze = potential setup
    else:
        score = 50
    return {
        "squeezing": bool(squeezing),
        "broke_out": bool(broke_out),
        "bandwidth": round(float(cur_bw), 2),
        "avg_bandwidth": round(float(avg_bw), 2),
        "score": score,
    }


def detect_volume_profile_position(current_price: float, vp: Optional[dict]) -> dict:
    """Where price sits in the volume profile.
    Above POC = bullish flow; below VAL = oversold; inside VA = balanced."""
    if vp is None:
        return {"zone": "unknown", "score": 50}
    poc = vp.get("poc")
    vah = vp.get("vah")
    val = vp.get("val")
    if poc is None:
        return {"zone": "unknown", "score": 50}

    if current_price > vah:
        zone, score = "above_value", 75  # bullish but extended
    elif current_price < val:
        zone, score = "below_value", 60  # oversold — buy zone
    elif current_price > poc:
        zone, score = "upper_value", 70
    else:
        zone, score = "lower_value", 65
    return {"zone": zone, "score": score, "poc": poc, "vah": vah, "val": val}


def detect_fib_confluence(zone_price: float, swing_high: float, swing_low: float) -> dict:
    """Is `zone_price` near a key Fibonacci retracement of the recent swing?"""
    if swing_high <= swing_low or zone_price <= 0:
        return {"matched": False, "score": 50}
    fib_levels = {0.236: "23.6%", 0.382: "38.2%", 0.5: "50%",
                  0.618: "61.8%", 0.786: "78.6%"}
    rng = swing_high - swing_low
    for r, label in fib_levels.items():
        fib_px = swing_high - rng * r
        if abs(zone_price - fib_px) / zone_price <= 0.012:
            return {
                "matched": True, "level": label, "level_price": round(fib_px, 2),
                "score": 90 if r in (0.5, 0.618, 0.786) else 70,
            }
    return {"matched": False, "score": 50}


def compute_sector_rs(symbol: str, df: pd.DataFrame, conn) -> dict:
    """60-day relative return vs sector average. RS rank 0-100."""
    try:
        if len(df) < 60:
            return {"rs_rank": 50, "score": 50}
        own_ret = (float(df["close"].iloc[-1]) - float(df["close"].iloc[-60])) / float(df["close"].iloc[-60]) * 100

        row = conn.execute(
            "SELECT sector FROM fundamentals WHERE symbol = ? LIMIT 1",
            (symbol.upper(),),
        ).fetchone()
        if not row or not row[0]:
            return {"rs_rank": 50, "score": 50, "note": "no sector"}
        sector = row[0]

        peers = conn.execute(
            "SELECT symbol FROM fundamentals WHERE sector = ? AND symbol != ?",
            (sector, symbol.upper()),
        ).fetchall()
        peer_returns = []
        for p in peers[:30]:
            pr = conn.execute(
                "SELECT close FROM daily_prices WHERE symbol = ? "
                "ORDER BY date DESC LIMIT 60",
                (p[0],),
            ).fetchall()
            if len(pr) >= 60:
                first = float(pr[-1]["close"])
                last = float(pr[0]["close"])
                if first > 0:
                    peer_returns.append((last - first) / first * 100)
        if not peer_returns:
            return {"rs_rank": 50, "score": 50}
        rank = sum(1 for r in peer_returns if own_ret > r) / len(peer_returns) * 100
        score = round(rank)
        return {
            "rs_rank": round(rank),
            "score": score,
            "own_60d_return": round(own_ret, 1),
            "sector": sector,
            "peers_compared": len(peer_returns),
        }
    except Exception as e:
        return {"rs_rank": 50, "score": 50, "error": str(e)}


# ───────────────────────────────────────────────────────────────────────
#  Composite scoring orchestrator
# ───────────────────────────────────────────────────────────────────────

# Regime-aware weights — different strategies work in different markets.
# Weights per regime sum to 100. Regime detected from ADX + ATR + range %.
REGIME_WEIGHTS = {
    "TRENDING_UP": {
        # Trends → SMC + MTF + Wyckoff lead. Mean-reversion noise.
        "smc": 25, "mtf": 20, "order_flow": 15, "wyckoff": 15,
        "sector_rs": 10, "fib_confluence": 5, "liquidity_sweep": 5,
        "volume_profile": 3, "bb_squeeze": 2,
    },
    "TRENDING_DOWN": {
        # Avoid in downtrends — lower max scores. SMC tells you to stay out.
        "smc": 30, "mtf": 25, "order_flow": 15, "wyckoff": 10,
        "sector_rs": 10, "fib_confluence": 3, "liquidity_sweep": 3,
        "volume_profile": 2, "bb_squeeze": 2,
    },
    "SIDEWAYS": {
        # Mean reversion + range trading dominate. SMC FVGs are noise here.
        "volume_profile": 20, "order_flow": 18, "liquidity_sweep": 15,
        "bb_squeeze": 12, "fib_confluence": 10, "sector_rs": 10,
        "smc": 8, "wyckoff": 5, "mtf": 2,
    },
    "VOLATILE_EXPANSION": {
        # Volatility breakout regime — BB squeeze + Order Flow lead.
        "bb_squeeze": 22, "order_flow": 20, "liquidity_sweep": 15,
        "smc": 13, "mtf": 10, "sector_rs": 10,
        "volume_profile": 5, "wyckoff": 3, "fib_confluence": 2,
    },
}


def detect_market_regime(df: pd.DataFrame, current_adx: Optional[float]) -> str:
    """Classify the regime from ADX + 20-day return + ATR expansion."""
    if len(df) < 30:
        return "SIDEWAYS"
    closes = df["close"].astype(float)
    highs = df["high"].astype(float)
    lows = df["low"].astype(float)

    # 20-day return + slope
    ret_20 = (closes.iloc[-1] - closes.iloc[-20]) / closes.iloc[-20] * 100

    # ATR expansion: today's ATR vs 20-bar avg ATR
    tr = pd.concat([
        highs - lows,
        (highs - closes.shift(1)).abs(),
        (lows - closes.shift(1)).abs(),
    ], axis=1).max(axis=1)
    atr_now = tr.iloc[-5:].mean()
    atr_avg = tr.iloc[-30:-5].mean() if len(tr) >= 30 else atr_now
    atr_expanding = atr_now > atr_avg * 1.4 if atr_avg > 0 else False

    # Decision tree
    if atr_expanding and current_adx and current_adx < 30:
        return "VOLATILE_EXPANSION"
    if current_adx is not None and current_adx >= 25:
        return "TRENDING_UP" if ret_20 > 0 else "TRENDING_DOWN"
    return "SIDEWAYS"


def classify_entry_distance(entry: Optional[float], current_price: float) -> dict:
    """Classify how tradeable the entry is RIGHT NOW.

    Returns:
      action_type: BUY_NOW | BUY_LIMIT | SETUP | STALE | NO_ENTRY
      distance_pct: how far below current (positive = below)
      label: human-readable
    """
    if entry is None or entry <= 0 or current_price <= 0:
        return {"action_type": "NO_ENTRY", "distance_pct": None,
                "label": "No entry zone"}
    distance_pct = (current_price - entry) / current_price * 100  # positive = entry below current

    if abs(distance_pct) <= 1.5:
        return {"action_type": "BUY_NOW", "distance_pct": round(distance_pct, 2),
                "label": "BUY NOW (price at entry)"}
    if -1.5 <= distance_pct <= 6:
        return {"action_type": "BUY_LIMIT", "distance_pct": round(distance_pct, 2),
                "label": f"BUY LIMIT — pullback {round(distance_pct,1)}% to entry"}
    if 6 < distance_pct <= 12:
        return {"action_type": "SETUP", "distance_pct": round(distance_pct, 2),
                "label": f"Setup zone — {round(distance_pct,1)}% below"}
    if distance_pct > 12:
        return {"action_type": "STALE", "distance_pct": round(distance_pct, 2),
                "label": f"Stale — {round(distance_pct,1)}% below (old level)"}
    # entry above current = breakout zone
    if distance_pct < -1.5:
        return {"action_type": "BREAKOUT_PENDING", "distance_pct": round(distance_pct, 2),
                "label": f"Breakout pending — needs +{round(abs(distance_pct),1)}% rise"}
    return {"action_type": "BUY_NOW", "distance_pct": round(distance_pct, 2), "label": "BUY"}


def compute_risk_score(analysis: dict, vp_pos: dict, vwap_data: Optional[dict],
                       current_price: float) -> int:
    """1=lowest risk, 5=highest risk."""
    risk = 1
    # Stop loss too far = high risk
    if analysis.get("entry") and analysis.get("stop_loss"):
        stop_pct = abs(analysis["entry"] - analysis["stop_loss"]) / analysis["entry"] * 100
        if stop_pct > 5: risk += 1
        if stop_pct > 8: risk += 1
    # Premium zone = higher risk
    if vp_pos.get("zone") == "above_value": risk += 1
    # Below VWAP -2σ = riskier (catching falling knife)
    if vwap_data and current_price < vwap_data.get("lower_2sd", current_price):
        risk += 1
    # Low confidence = higher risk
    if analysis.get("confidence") == "LOW": risk += 1
    return min(5, max(1, risk))


def compute_composite_signal(chart_data: dict, conn=None) -> dict:
    """Take a full smc_chart response and produce a composite signal.

    Args:
        chart_data: the dict returned by api.smc_chart.get_smc_chart()
        conn: DB connection for sector RS lookup (optional)
    """
    symbol = chart_data.get("symbol")
    current_price = chart_data.get("current_price", 0)
    analysis = chart_data.get("analysis", {})
    order_flow = chart_data.get("order_flow") or {}
    accumulation = chart_data.get("accumulation") or {}
    structure = chart_data.get("structure", [])
    fvgs = chart_data.get("fvgs", [])

    scores: dict[str, int] = {}
    reasons: list[str] = []
    active: list[str] = []

    # 1. SMC — score from existing analysis
    bias = analysis.get("bias")
    confidence = analysis.get("confidence")
    action = analysis.get("action", "")
    if bias == "BULLISH" and "BUY" in action:
        if confidence == "HIGH":
            scores["smc"] = 100
        elif confidence == "MEDIUM":
            scores["smc"] = 70
        else:
            scores["smc"] = 40
        active.append("SMC")
        reasons.append(f"SMC: {action} ({confidence})")
    elif bias == "BULLISH":
        scores["smc"] = 50
    else:
        scores["smc"] = 0

    # 2. Order Flow
    of_score = 50
    absorption = order_flow.get("absorption") or {}
    if absorption.get("absorbed"):
        of_score = 90
        active.append("Absorption")
        reasons.append(f"Order flow: institutional absorption (strength {int(absorption.get('strength', 0) * 100)}%)")
    elif absorption.get("strength", 0) >= 0.5:
        of_score = 70
    vd = order_flow.get("volume_delta") or {}
    if vd.get("delta_5d", 0) > 0:
        of_score = max(of_score, 65)
    obi = order_flow.get("orderbook_imbalance") or {}
    if obi.get("imbalance", 0) > 0.1:
        of_score = max(of_score, 80)
        active.append("OB Imbalance")
        reasons.append(f"Order book leaning buy-side (+{obi.get('imbalance_pct')}%)")
    scores["order_flow"] = of_score

    # 3. Multi-Timeframe Confluence
    df_proxy = pd.DataFrame(chart_data.get("candles", []))
    if not df_proxy.empty:
        df_proxy["date"] = pd.to_datetime(df_proxy["time"])
        mtf = detect_mtf_confluence(df_proxy)
        scores["mtf"] = mtf["score"]
        if mtf["aligned"]:
            active.append("MTF Aligned")
            reasons.append(f"Multi-TF: 1D {mtf['daily_bias']} + 1W {mtf['weekly_bias']} aligned ✅")
    else:
        scores["mtf"] = 50
        mtf = {"score": 50}

    # 4. Liquidity Sweep
    sweep = detect_liquidity_sweep(df_proxy, [])  # we don't pass swings; simple wick check below
    if not df_proxy.empty:
        last = df_proxy.iloc[-1]
        prev_low = df_proxy["low"].iloc[-30:-1].min() if len(df_proxy) > 30 else None
        if prev_low and float(last["low"]) < prev_low * 0.999 and float(last["close"]) > prev_low * 1.002:
            sweep = {"detected": True, "type": "bullish_sweep", "swept_level": prev_low, "score": 95}
            active.append("Liquidity Sweep")
            reasons.append(f"Bullish sweep: wicked below ৳{round(prev_low,2)} and reclaimed")
        else:
            sweep = {"detected": False, "score": 50}
    scores["liquidity_sweep"] = sweep["score"]

    # 5. Volume Profile position
    vp = order_flow.get("volume_profile")
    vp_pos = detect_volume_profile_position(current_price, vp)
    scores["volume_profile"] = vp_pos["score"]
    if vp_pos.get("zone") == "below_value":
        active.append("Below Value")
        reasons.append(f"Below VAL ৳{vp_pos.get('val')} = oversold of value")

    # 6. BB Squeeze
    bb = detect_bb_squeeze(df_proxy) if not df_proxy.empty else {"score": 50, "broke_out": False, "squeezing": False}
    scores["bb_squeeze"] = bb["score"]
    if bb.get("broke_out"):
        active.append("BB Breakout")
        reasons.append("Bollinger breakout from squeeze")
    elif bb.get("squeezing"):
        active.append("BB Squeeze")
        reasons.append(f"BB squeeze (bandwidth {bb.get('bandwidth')}% vs avg {bb.get('avg_bandwidth')}%)")

    # 7. Sector RS
    rs = compute_sector_rs(symbol, df_proxy, conn) if conn is not None else {"score": 50, "rs_rank": 50}
    scores["sector_rs"] = rs["score"]
    if rs["rs_rank"] >= 70:
        active.append(f"RS {rs['rs_rank']}")
        reasons.append(f"Sector RS: {rs['rs_rank']}/100 (top tier)")

    # 8. Fib Confluence
    fib = {"matched": False, "score": 50}
    if analysis.get("entry"):
        # Use 60-bar swing
        try:
            sub = df_proxy.iloc[-60:]
            sh = float(sub["high"].max())
            sl = float(sub["low"].min())
            fib = detect_fib_confluence(analysis["entry"], sh, sl)
            if fib["matched"]:
                active.append(f"Fib {fib['level']}")
                reasons.append(f"Entry sits at Fib {fib['level']} retracement")
        except Exception:
            pass
    scores["fib_confluence"] = fib["score"]

    # 9. Wyckoff phase
    wy_score = 50
    if accumulation.get("phase") == "ACCUMULATION":
        wy_score = 80
        if accumulation.get("bias") == "bullish":
            wy_score = 90
        active.append("Wyckoff Accum")
        reasons.append(f"Wyckoff: {accumulation.get('phase')} ({accumulation.get('confidence')})")
    elif accumulation.get("phase") == "DISTRIBUTION":
        wy_score = 20
    scores["wyckoff"] = wy_score

    # ─── Detect market regime + apply regime-specific weights ───
    regime = detect_market_regime(df_proxy, analysis.get("adx"))
    weights = REGIME_WEIGHTS[regime]
    weighted = sum(scores.get(k, 50) * weights.get(k, 0) for k in weights) / 100
    composite_score = round(weighted)

    # ─── Per-strategy votes (each gets BUY/HOLD/AVOID independently) ───
    def _vote(score: int) -> str:
        if score >= 70: return "BUY"
        if score >= 50: return "HOLD"
        return "AVOID"
    votes = {k: {"score": scores.get(k, 50), "vote": _vote(scores.get(k, 50)),
                 "weight_in_regime": weights.get(k, 0)} for k in weights}

    # ─── Entry-distance classification (the KDSALTD fix) ───
    entry_class = classify_entry_distance(analysis.get("entry"), current_price)

    # ─── Signal level: combine composite score with action_type to avoid
    # ranking aspirational "buy below current" zones as STRONG_BUY when price
    # hasn't actually retraced yet.
    if entry_class["action_type"] == "STALE":
        signal_level = "WATCH"  # cap stale signals to WATCH max
    elif composite_score >= 80 and entry_class["action_type"] in ("BUY_NOW", "BUY_LIMIT"):
        signal_level = "STRONG_BUY"
    elif composite_score >= 65 and entry_class["action_type"] in ("BUY_NOW", "BUY_LIMIT"):
        signal_level = "BUY"
    elif composite_score >= 50:
        signal_level = "WATCH"
    else:
        signal_level = "NONE"

    risk = compute_risk_score(analysis, vp_pos, order_flow.get("vwap"), current_price)

    return {
        "symbol": symbol,
        "current_price": current_price,
        "composite_score": composite_score,
        "signal_level": signal_level,
        "risk_score": risk,
        "regime": regime,
        "scores_by_method": scores,
        "votes": votes,
        "active_signals": active,
        "reasons": reasons[:6],
        "entry": analysis.get("entry"),
        "stop_loss": analysis.get("stop_loss"),
        "target1": analysis.get("target1"),
        "target2": analysis.get("target2"),
        "risk_reward": analysis.get("risk_reward"),
        "bias": analysis.get("bias"),
        "entry_class": entry_class,
        "action_type": entry_class["action_type"],
        "entry_distance_pct": entry_class["distance_pct"],
    }
