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
# vsa + wyckoff_events are NEW — high-edge triggers, especially in chop.
REGIME_WEIGHTS = {
    "TRENDING_UP": {
        "smc": 22, "mtf": 17, "order_flow": 13, "wyckoff": 8,
        "wyckoff_events": 10, "vsa": 8,
        "sector_rs": 8, "fib_confluence": 5, "liquidity_sweep": 5,
        "volume_profile": 3, "bb_squeeze": 1,
    },
    "TRENDING_DOWN": {
        "smc": 25, "mtf": 20, "order_flow": 13, "wyckoff": 5,
        "wyckoff_events": 8, "vsa": 8,
        "sector_rs": 8, "fib_confluence": 5, "liquidity_sweep": 5,
        "volume_profile": 2, "bb_squeeze": 1,
    },
    "SIDEWAYS": {
        # VSA + Wyckoff events shine here — institutional footprints in chop
        "volume_profile": 18, "order_flow": 15, "liquidity_sweep": 13,
        "vsa": 12, "bb_squeeze": 10, "wyckoff_events": 8,
        "fib_confluence": 8, "sector_rs": 8, "smc": 5,
        "wyckoff": 2, "mtf": 1,
    },
    "VOLATILE_EXPANSION": {
        "bb_squeeze": 18, "order_flow": 17, "liquidity_sweep": 13,
        "vsa": 10, "wyckoff_events": 10, "smc": 10,
        "mtf": 8, "sector_rs": 8, "volume_profile": 4,
        "wyckoff": 1, "fib_confluence": 1,
    },
}


def score_vsa_events(vsa_events: list, n_bars: int) -> tuple[int, str]:
    """Score VSA events from last 3 bars. Returns (score 0-100, note)."""
    if not vsa_events:
        return 50, ""
    recent = [e for e in vsa_events if e.get("idx", 0) >= n_bars - 3]
    if not recent:
        return 50, ""

    bull_strong = {"SELLING_CLIMAX", "SPRING"}
    bull_weak = {"NO_SUPPLY", "STOPPING_VOLUME"}
    bear_strong = {"BUYING_CLIMAX", "UPTHRUST"}
    bear_weak = {"NO_DEMAND"}

    score = 50
    notes: list[str] = []
    for e in recent:
        t = e.get("type", "")
        bias = e.get("bias")
        if t in bull_strong and bias == "bullish":
            score += 25; notes.append(t)
        elif t in bull_weak and bias == "bullish":
            score += 12; notes.append(t)
        elif t in bear_strong and bias == "bearish":
            score -= 25; notes.append(t)
        elif t in bear_weak and bias == "bearish":
            score -= 10
        elif t == "STOPPING_VOLUME" and bias == "bullish":
            score += 15; notes.append(t)
    return max(0, min(100, score)), ", ".join(notes[:2])


def score_wyckoff_events(wyckoff_events: list, n_bars: int) -> tuple[int, str]:
    """Score Wyckoff Spring/SOS/UTAD in last 5 bars. Returns (score, note)."""
    if not wyckoff_events:
        return 50, ""
    recent = [e for e in wyckoff_events if e.get("idx", 0) >= n_bars - 5]
    if not recent:
        return 50, ""
    score = 50
    notes: list[str] = []
    for e in recent:
        t = e.get("type", "")
        bias = e.get("bias")
        if t == "WYCKOFF_SPRING" and bias == "bullish":
            score = max(score, 90); notes.append("SPRING")
        elif t == "WYCKOFF_SOS" and bias == "bullish":
            score = max(score, 85); notes.append("SOS")
        elif t == "WYCKOFF_UTAD" and bias == "bearish":
            score = min(score, 15); notes.append("UTAD")
    return score, ", ".join(notes)


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


def classify_stock_state(df: pd.DataFrame, fvgs: list, current_price: float,
                          analysis: dict) -> dict:
    """Lifecycle-aware classification — answers "where is this stock RIGHT NOW
    in its setup → trigger → run cycle?"

    States:
      BUY_NOW          today tagged a fresh bullish FVG + closed green
      RECENT_TRIGGER   tagged 1-3 days ago + still near zone (re-entry possible)
      MISSED_ENTRY     tagged 2-7 days ago + price already moved >2% above FVG
      RUNNING          tagged >7 days ago, trend continuing (hold, don't chase)
      BUY_LIMIT        entry 1.5-6% below current, no recent tag
      SETUP_DEEP       entry 6-12% below current, just a watch zone
      STALE            entry >12% below current
      BREAKOUT_PENDING entry above current — wait for break
      AVOID            bearish bias / whipsaw / structure broken
      WAITING          bullish bias but no clear entry zone
    """
    if df.empty:
        return {"state": "WAITING", "label": "no data", "days_since_trigger": None}

    bias = analysis.get("bias")
    if bias in ("BEARISH", "WHIPSAW"):
        return {"state": "AVOID", "label": f"avoid ({bias.lower()})",
                "days_since_trigger": None}

    closes = df["close"].astype(float).values
    highs = df["high"].astype(float).values
    lows = df["low"].astype(float).values
    opens = df["open"].astype(float).values
    n = len(df)

    # Look back up to 14 trading days for FVG tags. CRITICAL: include
    # mitigated FVGs too — a "tag and reject" pattern marks the FVG as
    # mitigated, but THAT IS the signal we want to detect (CCJ case).
    bull_fvgs = [f for f in fvgs if f.get("type") == "bullish"
                 and f.get("top", 0) < current_price * 1.20]

    triggered_idx = None  # bar index of MOST RECENT FVG tag with confirmation
    triggered_fvg = None
    confirmed_idx = None  # bar index of green-confirmation after tag

    # Walk NEWEST → OLDEST so we find the *latest* tag-and-confirm pattern.
    # We want the "current" lifecycle state, not history. An old tag should
    # only matter if no newer one exists.
    for i in range(n - 1, max(0, n - 14) - 1, -1):
        bar_low = lows[i]
        for f in bull_fvgs:
            if bar_low <= f["top"] and bar_low >= f["bottom"] * 0.97:
                # Look for green confirmation on same bar or any of next 3
                conf = None
                for j in range(i, min(n, i + 4)):
                    if closes[j] > opens[j]:
                        conf = j
                        break
                if conf is not None:
                    triggered_idx = i
                    triggered_fvg = f
                    confirmed_idx = conf
                    break
        if triggered_idx is not None:
            break

    days_since = (n - 1 - confirmed_idx) if confirmed_idx is not None else None

    # Distance from current to FVG top
    fvg_dist_pct = None
    if triggered_fvg:
        fvg_top = triggered_fvg["top"]
        fvg_dist_pct = (current_price - fvg_top) / fvg_top * 100

    # ─── State decision ───
    if confirmed_idx is not None:
        if days_since == 0:
            state = "BUY_NOW"
            label = "BUY NOW — today tagged FVG + green close"
        elif days_since <= 3 and fvg_dist_pct is not None and fvg_dist_pct < 2:
            state = "RECENT_TRIGGER"
            label = f"Triggered {days_since}d ago — re-entry near FVG still possible"
        elif days_since <= 7 and fvg_dist_pct is not None and fvg_dist_pct >= 2:
            state = "MISSED_ENTRY"
            label = f"Missed entry — triggered {days_since}d ago, price +{fvg_dist_pct:.1f}% above zone"
        elif days_since > 7:
            state = "RUNNING"
            label = f"Running — triggered {days_since}d ago, trend continuing"
        else:
            state = "RECENT_TRIGGER"
            label = f"Triggered {days_since}d ago"
    else:
        # No recent trigger — fall back to entry-distance classifier
        ed = classify_entry_distance(analysis.get("entry"), current_price)
        if ed["action_type"] == "BUY_NOW":
            state = "BUY_LIMIT"  # no real trigger yet, pretend it's a limit
            label = "Entry near current — limit order setup"
        elif ed["action_type"] == "BUY_LIMIT":
            state = "BUY_LIMIT"
            label = ed["label"]
        elif ed["action_type"] == "SETUP":
            state = "SETUP_DEEP"
            label = ed["label"]
        elif ed["action_type"] == "STALE":
            state = "STALE"
            label = ed["label"]
        elif ed["action_type"] == "BREAKOUT_PENDING":
            state = "BREAKOUT_PENDING"
            label = ed["label"]
        else:
            state = "WAITING"
            label = "Bullish bias but no clear entry"

    return {
        "state": state,
        "label": label,
        "days_since_trigger": days_since,
        "fvg_distance_pct": round(fvg_dist_pct, 2) if fvg_dist_pct is not None else None,
        "triggered_at_bar": triggered_idx,
        "confirmed_at_bar": confirmed_idx,
    }


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

    # 10. VSA — institutional footprint signals (Tom Williams)
    vsa_events = chart_data.get("vsa_events", []) or []
    n_bars = len(chart_data.get("candles", []))
    vsa_score, vsa_note = score_vsa_events(vsa_events, n_bars)
    scores["vsa"] = vsa_score
    if vsa_note and vsa_score >= 70:
        active.append(f"VSA {vsa_note}")
        reasons.append(f"VSA: {vsa_note} (last 3 bars)")
    elif vsa_note and vsa_score <= 30:
        reasons.append(f"⚠ VSA bearish: {vsa_note}")

    # 11. Wyckoff Events — Spring / SOS / UTAD entry triggers
    wyckoff_events = chart_data.get("wyckoff_events", []) or []
    we_score, we_note = score_wyckoff_events(wyckoff_events, n_bars)
    scores["wyckoff_events"] = we_score
    if we_note and we_score >= 80:
        active.append(f"Wyckoff {we_note}")
        reasons.append(f"💎 Wyckoff: {we_note} — strongest entry trigger")
    elif we_note and we_score <= 20:
        reasons.append(f"⚠ Wyckoff: {we_note} (bearish)")

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

    # ─── Lifecycle state classification (CCJ-style "missed entry" fix) ───
    state_info = classify_stock_state(df_proxy, fvgs, current_price, analysis)
    # Entry-distance classification kept for backwards compat
    entry_class = classify_entry_distance(analysis.get("entry"), current_price)

    # ─── Strategy agreement requirement ───
    # User insight: a BUY shouldn't fire just from a high score. Multiple
    # methodologies must AGREE. Count BUY votes + weighted agreement.
    buy_votes = sum(1 for v in votes.values() if v["vote"] == "BUY")
    avoid_votes = sum(1 for v in votes.values() if v["vote"] == "AVOID")
    # Weighted: % of total weight where BUY vote was cast
    total_weight = sum(v["weight_in_regime"] for v in votes.values())
    weighted_buy = sum(v["weight_in_regime"] for v in votes.values() if v["vote"] == "BUY")
    weighted_buy_pct = (weighted_buy / total_weight * 100) if total_weight > 0 else 0

    # ─── Signal level: combine composite score + lifecycle state + agreement ───
    state = state_info["state"]
    if state in ("STALE", "AVOID"):
        signal_level = "AVOID" if state == "AVOID" else "WATCH"
    elif state == "MISSED_ENTRY":
        signal_level = "WATCH"
    elif state == "RUNNING":
        signal_level = "WATCH"
    elif state == "BUY_NOW" and composite_score >= 65:
        signal_level = "STRONG_BUY" if composite_score >= 80 else "BUY"
    elif state == "RECENT_TRIGGER" and composite_score >= 65:
        signal_level = "BUY"
    elif state == "BUY_LIMIT" and composite_score >= 65:
        signal_level = "BUY"
    elif composite_score >= 50:
        signal_level = "WATCH"
    else:
        signal_level = "NONE"

    # Agreement gate: STRONG_BUY needs >=6 strategies BUY OR >=60% weighted.
    # BUY needs >=5 BUY votes OR >=45% weighted. Otherwise demote.
    if signal_level == "STRONG_BUY" and (buy_votes < 6 and weighted_buy_pct < 60):
        signal_level = "BUY"
        reasons.append(f"⚠ Demoted from STRONG_BUY: only {buy_votes}/9 strategies agree ({weighted_buy_pct:.0f}% weighted)")
    if signal_level == "BUY" and (buy_votes < 5 and weighted_buy_pct < 45):
        signal_level = "WATCH"
        reasons.append(f"⚠ Demoted from BUY: only {buy_votes}/9 strategies agree ({weighted_buy_pct:.0f}% weighted) — needs broader confirmation")

    risk = compute_risk_score(analysis, vp_pos, order_flow.get("vwap"), current_price)

    # ─── T+2 friendliness: is this trade likely to resolve in 1-3 days?
    # For DSE retail with T+2 settlement, only certain setups are tradeable.
    state = state_info["state"]
    t2_reasons = []
    t2_friendly = True
    # Hard exclusions
    if state in ("STALE", "AVOID", "WAITING", "BREAKOUT_PENDING", "SETUP_DEEP", "RUNNING"):
        t2_friendly = False
        t2_reasons.append(f"state={state} not actionable in 2 days")
    if state == "MISSED_ENTRY":
        t2_friendly = False
        t2_reasons.append("already past entry — chase risk")
    # Quality conditions for "yes T+2"
    adx = analysis.get("adx") or 0
    if adx < 25:
        t2_friendly = False
        t2_reasons.append(f"ADX {adx} < 25 = no trend = whipsaw risk")
    if in_extreme_premium:
        t2_friendly = False
        t2_reasons.append("extreme premium = pullback likely within 2d")
    if overhead_bear_fvg is not None:
        t2_friendly = False
        t2_reasons.append("overhead supply within 3% = ceiling")
    if analysis.get("bias") in ("WHIPSAW", "BEARISH"):
        t2_friendly = False
        t2_reasons.append("bias not bullish")
    # Bonus boosters (still need base conditions met)
    t2_bonus = []
    if state == "BUY_NOW":
        t2_bonus.append("BUY_NOW today")
    if vsa_score >= 70:
        t2_bonus.append(f"VSA bullish trigger ({vsa_note})")
    if we_score >= 80:
        t2_bonus.append(f"Wyckoff {we_note}")
    if order_flow.get("absorption", {}).get("absorbed"):
        t2_bonus.append("buyer absorption")

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
        "action_type": state_info["state"],  # use lifecycle state as action_type
        "state_label": state_info["label"],
        "days_since_trigger": state_info["days_since_trigger"],
        "fvg_distance_pct": state_info.get("fvg_distance_pct"),
        "entry_distance_pct": entry_class["distance_pct"],
        "t_plus_2_friendly": bool(t2_friendly),
        "t_plus_2_reasons": t2_reasons,
        "t_plus_2_bonuses": t2_bonus,
        "buy_votes": buy_votes,
        "weighted_buy_pct": round(weighted_buy_pct, 1),
        "total_strategies": len(votes),
    }
