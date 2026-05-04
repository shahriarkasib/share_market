"""Per-methodology signal generation.

We have detection for 14+ trading methodologies bundled into one composite
score. This module ALSO emits a per-method signal so the user can trade
their preferred system (SMC, Wyckoff, RSI, Harmonic, etc.) without noise
from the others.

Each method function takes the chart_data dict from get_smc_chart and
returns a signal dict with:
    method, signal (BUY/WATCH/AVOID/NONE), entry, entry_zone_low/high,
    stop_loss, target1, confidence, reason, trigger_date, bars_since_trigger,
    max_profit_since_pct, max_drawdown_since_pct, bucket.

Bucket logic (mirrors live_signals_tracker.derive_bucket):
    IN_ZONE       — price inside / barely above (≤1.5%) entry zone
    WATCHING      — 1.5-8% above zone, set buy limit
    MISSED        — triggered ≥2d ago, ≥3% max profit since, but didn't buy
    WRONG_TRIGGER — triggered ≥2d ago, zone broke (>3% drawdown after entry)
    STALE         — no actionable signal
"""
from __future__ import annotations

from typing import Optional
import pandas as pd


# ─── helpers ────────────────────────────────────────────────────────


def _scan_zone_trigger(zone_low: float, zone_high: float, df: pd.DataFrame,
                        lookback: int = 120) -> Optional[dict]:
    """Walk back through candles to find when price LAST entered the zone.
    Returns trigger_date, bars_since_hit, max_profit_pct, max_drawdown_pct."""
    if zone_low is None or zone_high is None or df is None or len(df) == 0:
        return None
    try:
        n = len(df)
        start = max(0, n - lookback)
        hit_idx = None
        for i in range(n - 1, start - 1, -1):
            lo = float(df["low"].iloc[i])
            if lo <= zone_high:
                hit_idx = i
                break
        if hit_idx is None:
            return None
        hit_date = df["date"].iloc[hit_idx]
        hit_str = hit_date.strftime("%Y-%m-%d") if hasattr(hit_date, "strftime") else str(hit_date)
        slice_after = df.iloc[hit_idx:n]
        max_high = float(slice_after["high"].max())
        min_low = float(slice_after["low"].min())
        mid = (zone_low + zone_high) / 2
        return {
            "trigger_date": hit_str,
            "bars_since_trigger": int((n - 1) - hit_idx),
            "max_profit_since_pct": round((max_high - mid) / mid * 100, 1) if mid > 0 else 0,
            "max_drawdown_since_pct": round((min_low - zone_high) / zone_high * 100, 1) if zone_high > 0 else 0,
        }
    except Exception:
        return None


SIGNAL_TYPES = {
    # PULLBACK = buy at/below the zone (price has retraced to support)
    "SMC": "PULLBACK",
    "VSA": "PULLBACK",
    "WYCKOFF": "PULLBACK",
    "HARMONIC": "PULLBACK",
    "FIBONACCI": "PULLBACK",
    "SUPPORT_RESISTANCE": "PULLBACK",
    "CANDLE_PATTERN": "PULLBACK",
    "MOVING_AVG": "PULLBACK",
    # BREAKOUT = buy ABOVE the level (price has cleared resistance)
    "CHART_PATTERN": "BREAKOUT",
    "BOLLINGER": "BREAKOUT",
    "ICHIMOKU": "BREAKOUT",
    "ELLIOTT": "BREAKOUT",
    # MOMENTUM = buy near current (signal is "now is the moment")
    "ORDER_FLOW": "MOMENTUM",
    "RSI_MACD": "MOMENTUM",
    "OBV_MFI": "MOMENTUM",
}


def _classify_bucket(current_price: float, zone_low: float, zone_high: float,
                      trigger: Optional[dict],
                      signal_type: str = "PULLBACK") -> str:
    """Bucket classification varies by signal type:

    PULLBACK (SMC FVG, Fib, S/R, Wyckoff, VSA, Candle@support, MA pullback):
        Buy AT or BELOW the zone. Above zone = wait for pullback.

    BREAKOUT (Chart Patterns, Bollinger squeeze, Ichimoku break, Elliott break):
        Buy ABOVE the level (price has cleared resistance). Below = setup
        not triggered yet = WATCHING.

    MOMENTUM (Order Flow, RSI/MACD, OBV/MFI):
        Buy near current price. Zone is current ± buffer. Both sides fine.
    """
    if current_price is None or zone_low is None or zone_high is None:
        return "STALE"
    bars_ago = (trigger or {}).get("bars_since_trigger", 0)
    max_profit = (trigger or {}).get("max_profit_since_pct", 0)
    max_drawdown = (trigger or {}).get("max_drawdown_since_pct", 0)
    triggered_in_past = bars_ago >= 2
    delivered_profit = max_profit >= 3.0
    zone_broke = max_drawdown < -3.0

    inside = zone_low <= current_price <= zone_high
    pct_above = (current_price - zone_high) / zone_high * 100 if zone_high > 0 else 999
    pct_below = (zone_low - current_price) / zone_low * 100 if zone_low > 0 else 999

    if signal_type == "BREAKOUT":
        if inside or (0 < pct_above <= 1.0):
            return "IN_ZONE"
        if 1.0 < pct_above <= 2.5:
            return "IN_ZONE"
        if 2.5 < pct_above <= 8.0:
            return "WATCHING"  # already broke out by more than 2.5% — slight chase
        if pct_above > 8.0:
            if triggered_in_past and delivered_profit:
                return "MISSED"
            return "MISSED"
        # Below level — setup pending, NOT a buy
        if pct_below <= 5.0:
            return "WATCHING"
        return "STALE"

    if signal_type == "MOMENTUM":
        if inside or pct_above <= 2.0 or pct_below <= 5.0:
            return "IN_ZONE"
        if pct_above <= 8.0:
            return "WATCHING"
        if triggered_in_past and delivered_profit:
            return "MISSED"
        return "MISSED"

    # PULLBACK (default)
    if inside:
        return "IN_ZONE"
    if current_price < zone_low:
        if triggered_in_past and zone_broke:
            return "WRONG_TRIGGER"
        return "IN_ZONE"
    if triggered_in_past and delivered_profit:
        return "MISSED"
    if pct_above <= 1.5:
        return "IN_ZONE"
    if pct_above <= 8.0:
        return "WATCHING"
    return "MISSED"


def _empty_signal(method: str, symbol: str, current_price: Optional[float] = None,
                   reason: str = "no setup") -> dict:
    return {
        "method": method,
        "symbol": symbol,
        "signal": "NONE",
        "entry": None, "entry_zone_low": None, "entry_zone_high": None,
        "stop_loss": None, "target1": None,
        "confidence": "LOW",
        "reason": reason,
        "trigger_date": None, "bars_since_trigger": None,
        "max_profit_since_pct": None, "max_drawdown_since_pct": None,
        "bucket": "STALE",
        "current_price": current_price,
    }


def _build_signal(method: str, symbol: str, current_price: float,
                   zone_low: float, zone_high: float,
                   stop_loss: Optional[float], target1: Optional[float],
                   confidence: str, reason: str,
                   df: pd.DataFrame, signal_level: str = "BUY") -> dict:
    """Common builder — runs trigger scan + bucket classification.
    Bucket logic depends on the method's signal type (BREAKOUT vs PULLBACK
    vs MOMENTUM). Without that distinction, breakout setups (chart patterns,
    Bollinger squeeze, etc.) get falsely flagged as IN_ZONE when price is
    still BELOW the breakout level — i.e., the setup hasn't triggered yet.
    """
    trigger = _scan_zone_trigger(zone_low, zone_high, df)
    sig_type = SIGNAL_TYPES.get(method, "PULLBACK")
    bucket = _classify_bucket(current_price, zone_low, zone_high, trigger, signal_type=sig_type)
    entry_mid = round((zone_low + zone_high) / 2, 2)
    return {
        "method": method,
        "symbol": symbol,
        "signal": signal_level,
        "entry": entry_mid,
        "entry_zone_low": zone_low,
        "entry_zone_high": zone_high,
        "stop_loss": stop_loss,
        "target1": target1,
        "confidence": confidence,
        "reason": reason,
        "trigger_date": (trigger or {}).get("trigger_date"),
        "bars_since_trigger": (trigger or {}).get("bars_since_trigger"),
        "max_profit_since_pct": (trigger or {}).get("max_profit_since_pct"),
        "max_drawdown_since_pct": (trigger or {}).get("max_drawdown_since_pct"),
        "bucket": bucket,
        "current_price": current_price,
    }


# ─── per-method signal functions ────────────────────────────────────


def signal_smc(chart: dict, df: pd.DataFrame) -> dict:
    """SMC: bullish bias + fresh FVG below price + (ideally) confluence with support."""
    sym = chart.get("symbol") or ""
    cp = chart.get("current_price")
    a = chart.get("analysis") or {}
    if a.get("bias") != "BULLISH":
        return _empty_signal("SMC", sym, cp, "bias not bullish")
    confluence = a.get("confluence") or {}
    fvgs = chart.get("fvg_zones") or chart.get("fvgs") or []
    fresh_below = [f for f in fvgs if f.get("type") == "bullish" and not f.get("mitigated")
                    and f.get("top") and float(f["top"]) < (cp or 0)]
    if confluence and confluence.get("top"):
        zlow = float(confluence["bottom"]); zhigh = float(confluence["top"])
        reason = f"Bullish FVG ৳{zlow}-{zhigh} + {confluence.get('support_touches', 0)}-touch support (CONFLUENCE)"
        conf = "HIGH"
    elif fresh_below:
        f = sorted(fresh_below, key=lambda x: -float(x["top"]))[0]
        zlow = float(f["bottom"]); zhigh = float(f["top"])
        reason = f"Bullish FVG retest ৳{zlow}-{zhigh}"
        conf = "MEDIUM"
    else:
        return _empty_signal("SMC", sym, cp, "no fresh FVG below")
    stop = a.get("stop_loss") or round(zlow * 0.97, 2)
    target = a.get("target1")
    return _build_signal("SMC", sym, cp, zlow, zhigh, stop, target, conf, reason, df)


def signal_order_flow(chart: dict, df: pd.DataFrame) -> dict:
    """Order Flow: buyer absorption today + positive 5d delta + price near POC."""
    sym = chart.get("symbol") or ""
    cp = chart.get("current_price")
    of = chart.get("order_flow") or {}
    abs_data = of.get("absorption") or {}
    vd = of.get("volume_delta") or {}
    vp = of.get("volume_profile") or {}
    obi = of.get("orderbook_imbalance") or {}

    absorbed = bool(abs_data.get("absorbed"))
    delta_5d = float(vd.get("delta_5d") or 0)
    poc = float(vp.get("poc") or 0)
    obi_pct = float(obi.get("imbalance_pct") or 0)

    score = sum([absorbed, delta_5d > 0, poc and cp and cp >= poc * 0.99, obi_pct > 5])
    if score < 2 or cp is None:
        return _empty_signal("ORDER_FLOW", sym, cp, f"only {score}/4 flow signals")

    # Entry zone = current ± 0.5% (this is a momentum signal, not a pullback signal)
    zlow = round(cp * 0.99, 2)
    zhigh = round(cp * 1.005, 2)
    parts = []
    if absorbed: parts.append("buyer absorption")
    if delta_5d > 0: parts.append(f"5d Δ +{int(delta_5d):,}")
    if poc and cp >= poc * 0.99: parts.append(f"price ≥ POC ৳{poc}")
    if obi_pct > 5: parts.append(f"order book +{obi_pct:.0f}%")
    reason = "Order flow: " + ", ".join(parts)
    conf = "HIGH" if score >= 3 else "MEDIUM"
    stop = round(cp * 0.95, 2)
    target = round(cp * 1.08, 2)
    return _build_signal("ORDER_FLOW", sym, cp, zlow, zhigh, stop, target, conf, reason, df)


def signal_vsa(chart: dict, df: pd.DataFrame) -> dict:
    """VSA: stopping volume or no supply on recent bar."""
    sym = chart.get("symbol") or ""
    cp = chart.get("current_price")
    events = chart.get("vsa_events") or []
    bullish = [e for e in events if e.get("type") in ("stopping_volume", "no_supply", "buying_climax", "spring")]
    if not bullish:
        return _empty_signal("VSA", sym, cp, "no bullish VSA events")
    latest = bullish[-1]
    bar_low = float(latest.get("low") or latest.get("price") or 0)
    bar_high = float(latest.get("high") or latest.get("price") or 0)
    if bar_low <= 0 or bar_high <= 0:
        return _empty_signal("VSA", sym, cp, "VSA event missing bar OHLC")
    zlow = round(bar_low, 2); zhigh = round(bar_high, 2)
    reason = f"VSA {latest.get('type','').replace('_',' ')} on {latest.get('date','recent')}"
    stop = round(bar_low * 0.98, 2)
    target = round(zhigh * 1.06, 2)
    return _build_signal("VSA", sym, cp, zlow, zhigh, stop, target, "MEDIUM", reason, df)


def signal_wyckoff(chart: dict, df: pd.DataFrame) -> dict:
    """Wyckoff: Spring or Sign of Strength (SOS) confirmed."""
    sym = chart.get("symbol") or ""
    cp = chart.get("current_price")
    events = chart.get("wyckoff_events") or []
    bullish = [e for e in events if e.get("type") in ("spring", "sos", "lps", "test")]
    if not bullish:
        return _empty_signal("WYCKOFF", sym, cp, "no Wyckoff bullish events")
    latest = bullish[-1]
    bar_low = float(latest.get("low") or latest.get("price") or 0)
    bar_high = float(latest.get("high") or latest.get("price") or 0)
    if bar_low <= 0:
        return _empty_signal("WYCKOFF", sym, cp, "Wyckoff event missing OHLC")
    zlow = round(bar_low, 2); zhigh = round(bar_high if bar_high > bar_low else bar_low * 1.01, 2)
    reason = f"Wyckoff {latest.get('type','').upper()} on {latest.get('date','recent')}"
    stop = round(bar_low * 0.97, 2)
    target = round(zhigh * 1.10, 2)
    return _build_signal("WYCKOFF", sym, cp, zlow, zhigh, stop, target, "MEDIUM", reason, df)


def signal_harmonic(chart: dict, df: pd.DataFrame) -> dict:
    """Harmonic: completed bullish XABCD pattern at PRZ (D point)."""
    sym = chart.get("symbol") or ""
    cp = chart.get("current_price")
    patterns = chart.get("harmonic_patterns") or []
    bull = [p for p in patterns if p.get("bias") == "bullish"]
    if not bull:
        return _empty_signal("HARMONIC", sym, cp, "no bullish harmonic")
    p = bull[-1]
    d = p.get("D") or p.get("d_point") or {}
    d_price = float(d.get("price") if isinstance(d, dict) else (d or 0))
    if d_price <= 0:
        # try prz_low/high
        prz_lo = p.get("prz_low") or p.get("D_price")
        if prz_lo:
            d_price = float(prz_lo)
    if d_price <= 0:
        return _empty_signal("HARMONIC", sym, cp, "D point missing")
    zlow = round(d_price * 0.995, 2); zhigh = round(d_price * 1.005, 2)
    reason = f"Bullish {p.get('type', 'Harmonic')} pattern @ ৳{d_price}"
    stop = round(d_price * 0.97, 2)
    target = round(d_price * 1.10, 2)
    return _build_signal("HARMONIC", sym, cp, zlow, zhigh, stop, target, "MEDIUM", reason, df)


def signal_fibonacci(chart: dict, df: pd.DataFrame) -> dict:
    """Fibonacci: price at 61.8% or 78.6% retrace of recent dealing range.
    Only fire when price is currently near the Golden Pocket (within 5%)."""
    sym = chart.get("symbol") or ""
    cp = chart.get("current_price")
    fib = chart.get("fib_dealing_range") or {}
    if not fib.get("valid"):
        return _empty_signal("FIBONACCI", sym, cp, "no valid dealing range")
    swing_low = float(fib.get("swing_low") or 0)
    swing_high = float(fib.get("swing_high") or 0)
    if swing_high <= swing_low or cp is None:
        return _empty_signal("FIBONACCI", sym, cp, "invalid swing")
    rng = swing_high - swing_low
    fib_618 = swing_high - rng * 0.618
    fib_786 = swing_high - rng * 0.786
    zlow = round(min(fib_618, fib_786), 2); zhigh = round(max(fib_618, fib_786), 2)
    # Quality gate: current must be within 5% of the Golden Pocket
    mid = (zlow + zhigh) / 2
    if mid > 0 and abs(cp - mid) / mid > 0.05:
        return _empty_signal("FIBONACCI", sym, cp,
                              f"Golden Pocket ৳{zlow}-{zhigh} too far from ৳{cp}")
    reason = f"Golden Pocket ৳{zlow}-{zhigh} (61.8-78.6 of leg ৳{swing_low}-৳{swing_high})"
    stop = round(swing_low * 0.99, 2)
    target = round(swing_high, 2)
    return _build_signal("FIBONACCI", sym, cp, zlow, zhigh, stop, target, "MEDIUM", reason, df)


def signal_elliott(chart: dict, df: pd.DataFrame) -> dict:
    """Elliott Wave: contracting triangle E-point or wave 4 complete."""
    sym = chart.get("symbol") or ""
    cp = chart.get("current_price")
    et = chart.get("elliott_triangle") or {}
    if not et.get("valid"):
        return _empty_signal("ELLIOTT", sym, cp, "no Elliott pattern")
    bias = et.get("bias", "")
    if "bullish" not in bias.lower():
        return _empty_signal("ELLIOTT", sym, cp, "not bullish bias")
    breakout_up = float(et.get("breakout_up_target") or 0)
    points = et.get("points") or []
    e_pt = next((p for p in points if p.get("label", "").upper() == "E"), None)
    if not e_pt:
        return _empty_signal("ELLIOTT", sym, cp, "no E point")
    e_price = float(e_pt.get("price") or 0)
    zlow = round(e_price * 0.99, 2); zhigh = round(e_price * 1.01, 2)
    reason = f"Elliott {et.get('kind', '')} triangle, E ৳{e_price}, breakout ৳{breakout_up}"
    stop = round(e_price * 0.95, 2)
    target = round(breakout_up, 2) if breakout_up else round(e_price * 1.10, 2)
    return _build_signal("ELLIOTT", sym, cp, zlow, zhigh, stop, target, "MEDIUM", reason, df)


def signal_ichimoku(chart: dict, df: pd.DataFrame) -> dict:
    """Ichimoku: above cloud + bullish TK cross — full system bullish.
    Treated as BREAKOUT: only fire when ALL bullish conditions are aligned
    AND price recently broke above the cloud (not perpetually above)."""
    sym = chart.get("symbol") or ""
    cp = chart.get("current_price")
    ich = chart.get("ichimoku") or {}
    if not ich or cp is None:
        return _empty_signal("ICHIMOKU", sym, cp, "no Ichimoku data")
    sig = ich.get("signal", "")
    if "above_cloud_bullish" not in sig:
        return _empty_signal("ICHIMOKU", sym, cp, f"signal: {sig or 'none'}")
    kijun = float(ich.get("kijun") or 0)
    senkou_a = float(ich.get("senkou_a") or 0)
    senkou_b = float(ich.get("senkou_b") or 0)
    cloud_top = max(senkou_a, senkou_b) if senkou_a and senkou_b else kijun
    if cloud_top <= 0:
        return _empty_signal("ICHIMOKU", sym, cp, "cloud values missing")
    # Quality gate: TK bullish cross required (not just "above cloud forever")
    if ich.get("tk_cross") != "bullish":
        return _empty_signal("ICHIMOKU", sym, cp, "no bullish TK cross")
    # Recently broke above cloud — verify by checking last 30 bars
    recent_n = min(30, len(df))
    recent_lows = df["low"].iloc[-recent_n:].astype(float)
    cloud_break_recent = bool((recent_lows < cloud_top).any())
    if not cloud_break_recent:
        return _empty_signal("ICHIMOKU", sym, cp, "above cloud >30 bars (stale)")
    # BREAKOUT entry: at the cloud_top (re-test of cloud after break)
    zlow = round(cloud_top * 0.998, 2); zhigh = round(cloud_top * 1.005, 2)
    reason = f"Above cloud + TK bullish cross — cloud retest @ ৳{cloud_top:.1f}"
    stop = round(senkou_b * 0.99, 2) if senkou_b else round(cloud_top * 0.95, 2)
    target = round(cp * 1.10, 2)
    return _build_signal("ICHIMOKU", sym, cp, zlow, zhigh, stop, target, "MEDIUM", reason, df)


def signal_rsi_macd(chart: dict, df: pd.DataFrame) -> dict:
    """RSI/MACD: bullish divergence OR oversold reversal + MACD bullish."""
    sym = chart.get("symbol") or ""
    cp = chart.get("current_price")
    rsi = chart.get("rsi") or []
    macd = chart.get("macd") or {}
    if not rsi or len(rsi) < 14:
        return _empty_signal("RSI_MACD", sym, cp, "no RSI history")
    last_rsi = float(rsi[-1].get("value") or 50)
    macd_hist = macd.get("histogram") or []
    if not macd_hist:
        return _empty_signal("RSI_MACD", sym, cp, "no MACD")
    last_hist = float(macd_hist[-1].get("value") or 0)
    prev_hist = float(macd_hist[-2].get("value") or 0) if len(macd_hist) >= 2 else 0
    macd_turning_up = last_hist > prev_hist
    if last_rsi < 35 and macd_turning_up:
        reason = f"RSI oversold {last_rsi:.0f} + MACD turning up"
        conf = "HIGH"
    elif last_rsi < 45 and macd_turning_up:
        reason = f"RSI {last_rsi:.0f} + MACD turning up"
        conf = "MEDIUM"
    else:
        return _empty_signal("RSI_MACD", sym, cp, f"RSI {last_rsi:.0f} no setup")
    # Momentum signal: entry zone = current ± 1%
    if cp is None:
        return _empty_signal("RSI_MACD", sym, cp, "no price")
    zlow = round(cp * 0.99, 2); zhigh = round(cp * 1.01, 2)
    stop = round(cp * 0.95, 2)
    target = round(cp * 1.08, 2)
    return _build_signal("RSI_MACD", sym, cp, zlow, zhigh, stop, target, conf, reason, df)


def signal_bollinger(chart: dict, df: pd.DataFrame) -> dict:
    """Bollinger: squeeze + breakout above upper band (TIGHT criteria).
    Only fire on REAL squeeze breakouts, not "price near upper band"."""
    sym = chart.get("symbol") or ""
    cp = chart.get("current_price")
    bb = chart.get("bollinger_bands") or {}
    upper = bb.get("upper") or []; lower = bb.get("lower") or []; middle = bb.get("middle") or []
    if len(upper) < 21 or cp is None:
        return _empty_signal("BOLLINGER", sym, cp, "BB history short")
    last_upper = float(upper[-1].get("value") or 0)
    last_lower = float(lower[-1].get("value") or 0)
    last_mid = float(middle[-1].get("value") or 0)
    if not (last_upper and last_lower and last_mid):
        return _empty_signal("BOLLINGER", sym, cp, "BB values missing")
    bandwidth = (last_upper - last_lower) / last_mid * 100 if last_mid else 0
    bw_20 = []
    for i in range(max(0, len(upper) - 20), len(upper)):
        u = float(upper[i].get("value") or 0); l_ = float(lower[i].get("value") or 0); m = float(middle[i].get("value") or 0)
        if m: bw_20.append((u - l_) / m * 100)
    avg_bw = sum(bw_20) / len(bw_20) if bw_20 else bandwidth
    # Real squeeze: bandwidth in bottom 30% of recent 20 days
    is_squeeze = bandwidth < avg_bw * 0.70
    # BREAKOUT signal: only fire when price ACTUALLY closes above upper band
    just_broke_out = cp > last_upper * 1.001
    if just_broke_out and is_squeeze:
        reason = f"BB squeeze breakout — close ৳{cp:.1f} above upper ৳{last_upper:.1f}"
        zlow = round(last_upper * 0.998, 2); zhigh = round(last_upper * 1.005, 2)
        conf = "HIGH"
    elif just_broke_out:
        reason = f"Close above upper ৳{last_upper:.1f} — momentum (no squeeze)"
        zlow = round(last_upper * 0.998, 2); zhigh = round(last_upper * 1.005, 2)
        conf = "MEDIUM"
    else:
        return _empty_signal("BOLLINGER", sym, cp, "no breakout above upper")
    stop = round(last_mid * 0.99, 2)
    target = round(cp + (last_upper - last_lower), 2)  # measured-move target
    return _build_signal("BOLLINGER", sym, cp, zlow, zhigh, stop, target, conf, reason, df)


def signal_chart_patterns(chart: dict, df: pd.DataFrame) -> dict:
    """Chart Patterns: bullish pattern (cup&handle, double bottom, ascending triangle).
    BREAKOUT signal: buy AFTER price clears the neckline. Below neckline = setup
    pending. Patterns must be recent (right rim within last 60 bars)."""
    sym = chart.get("symbol") or ""
    cp = chart.get("current_price")
    patterns = chart.get("chart_patterns") or []
    bull = [p for p in patterns if p.get("bias") == "bullish"]
    if not bull:
        return _empty_signal("CHART_PATTERN", sym, cp, "no bullish patterns")
    n = len(df)
    # Filter: pattern must be RECENT (right rim / detection idx within last 60 bars)
    def _is_recent(p):
        for k in ("right_rim_idx", "p2_idx", "handle_end_idx", "idx", "detected_idx"):
            v = p.get(k)
            if isinstance(v, (int, float)):
                return n - int(v) <= 60
        return True  # if no idx available, accept
    recent = [p for p in bull if _is_recent(p)]
    if not recent:
        return _empty_signal("CHART_PATTERN", sym, cp, "patterns too old (>60 bars)")
    p = recent[-1]
    neckline = float(p.get("neckline") or 0)
    target = float(p.get("target") or 0)
    if neckline <= 0:
        return _empty_signal("CHART_PATTERN", sym, cp, "neckline missing")
    # Sanity check: neckline must be within ±15% of current — anything wider
    # is a stale pattern that no longer reflects current price action.
    if cp and abs(cp - neckline) / neckline > 0.15:
        return _empty_signal("CHART_PATTERN", sym, cp, f"neckline ৳{neckline} too far from ৳{cp}")
    # BREAKOUT entry zone = neckline (the trigger level), tight ±0.5%
    zlow = round(neckline * 0.995, 2); zhigh = round(neckline * 1.005, 2)
    reason = f"{p.get('type', 'Pattern').replace('_', ' ').title()} — break above neckline ৳{neckline}"
    stop = round(neckline * 0.95, 2)
    return _build_signal("CHART_PATTERN", sym, cp, zlow, zhigh, stop, target, "MEDIUM", reason, df)


def signal_candle_patterns(chart: dict, df: pd.DataFrame) -> dict:
    """Candlestick: hammer/engulfing/morning star at recent support."""
    sym = chart.get("symbol") or ""
    cp = chart.get("current_price")
    cps = chart.get("candle_patterns") or []
    bull = [p for p in cps if p.get("bias") == "bullish" and p.get("strength", 0) >= 60]
    if not bull:
        return _empty_signal("CANDLE_PATTERN", sym, cp, "no strong bullish candles")
    p = bull[-1]
    bar_low = float(p.get("price_low") or 0)
    bar_high = float(p.get("price_high") or 0)
    if bar_low <= 0:
        return _empty_signal("CANDLE_PATTERN", sym, cp, "candle data missing")
    zlow = round(bar_low, 2); zhigh = round(bar_high, 2)
    reason = f"{p.get('type', 'Candle').replace('_',' ').title()} — {p.get('description','')}"
    stop = round(bar_low * 0.98, 2)
    target = round(zhigh * 1.06, 2)
    conf = "HIGH" if p.get("strength", 0) >= 80 else "MEDIUM"
    return _build_signal("CANDLE_PATTERN", sym, cp, zlow, zhigh, stop, target, conf, reason, df)


def signal_moving_avg(chart: dict, df: pd.DataFrame) -> dict:
    """Moving Average: EMA stack bullish (20>50>200) + price pullback to EMA20/50."""
    sym = chart.get("symbol") or ""
    cp = chart.get("current_price")
    mas = chart.get("moving_averages") or {}
    if not mas or cp is None:
        return _empty_signal("MOVING_AVG", sym, cp, "no MA data")

    def _last(key):
        s = mas.get(key) or []
        if not s: return None
        return float(s[-1].get("value") if isinstance(s[-1], dict) else (s[-1] or 0))

    ema20 = _last("ema_20") or _last("EMA20") or _last("ma20")
    ema50 = _last("ema_50") or _last("EMA50") or _last("ma50")
    ema200 = _last("ema_200") or _last("EMA200") or _last("ma200") or _last("sma_200")
    if not (ema20 and ema50):
        return _empty_signal("MOVING_AVG", sym, cp, "EMA20/50 missing")
    bullish_stack = ema20 > ema50 and (not ema200 or ema50 > ema200)
    if not bullish_stack:
        return _empty_signal("MOVING_AVG", sym, cp, "EMA stack not bullish")
    # Entry = pullback to EMA20 or EMA50
    zlow = round(min(ema20, ema50) * 0.99, 2)
    zhigh = round(max(ema20, ema50) * 1.005, 2)
    reason = f"EMA stack bullish (20>{ema50:.1f}>{ema200:.1f}) — pullback to EMA20-50"
    stop = round(min(ema20, ema50) * 0.95, 2)
    target = round(cp * 1.08, 2) if cp > zhigh else round(zhigh * 1.06, 2)
    return _build_signal("MOVING_AVG", sym, cp, zlow, zhigh, stop, target, "MEDIUM", reason, df)


def signal_support_resistance(chart: dict, df: pd.DataFrame) -> dict:
    """S/R: bouncing from multi-touch support.
    PULLBACK signal — actionable when price is at or just above the support.
    Filters: support must be within 8% of current, ≥3 touches (was 2)."""
    sym = chart.get("symbol") or ""
    cp = chart.get("current_price")
    sr = chart.get("support_resistance") or []
    if cp is None:
        return _empty_signal("SUPPORT_RESISTANCE", sym, cp, "no price")
    # Stricter filter: ≥3 touches, support within 10% below current
    sup = [r for r in sr if r.get("role") == "support"
           and r.get("touches", 0) >= 3
           and float(r.get("price", 0)) < cp
           and (cp - float(r.get("price", 0))) / cp <= 0.10]
    if not sup:
        return _empty_signal("SUPPORT_RESISTANCE", sym, cp, "no nearby ≥3-touch support")
    s = sorted(sup, key=lambda r: -float(r.get("price", 0)))[0]
    s_px = float(s.get("price"))
    touches = int(s.get("touches", 3))
    zlow = round(s_px * 0.995, 2); zhigh = round(s_px * 1.005, 2)
    reason = f"{touches}-touch support @ ৳{s_px}"
    stop = round(s_px * 0.97, 2)
    target = round(cp * 1.06, 2) if cp > s_px else round(s_px * 1.08, 2)
    conf = "HIGH" if touches >= 5 else "MEDIUM"
    return _build_signal("SUPPORT_RESISTANCE", sym, cp, zlow, zhigh, stop, target, conf, reason, df)


def signal_obv_mfi(chart: dict, df: pd.DataFrame) -> dict:
    """OBV/MFI: OBV bullish divergence + MFI <30 (oversold money flow)."""
    sym = chart.get("symbol") or ""
    cp = chart.get("current_price")
    obv = chart.get("obv") or {}
    mfi = chart.get("mfi") or {}
    obv_div = obv.get("divergence")
    obv_trend = obv.get("trend")
    mfi_val = float(mfi.get("current") or 50)
    bullish_obv = obv_div == "bullish" or obv_trend == "rising"
    oversold_mfi = mfi_val < 35
    if not (bullish_obv and oversold_mfi) and not (obv_div == "bullish" and mfi_val < 50):
        return _empty_signal("OBV_MFI", sym, cp, f"OBV {obv_trend or 'flat'}, MFI {mfi_val:.0f}")
    if cp is None:
        return _empty_signal("OBV_MFI", sym, cp, "no price")
    zlow = round(cp * 0.99, 2); zhigh = round(cp * 1.01, 2)
    parts = []
    if obv_div == "bullish": parts.append("OBV bullish divergence")
    if obv_trend == "rising": parts.append("OBV rising")
    if oversold_mfi: parts.append(f"MFI oversold {mfi_val:.0f}")
    reason = "Money flow: " + ", ".join(parts)
    stop = round(cp * 0.95, 2)
    target = round(cp * 1.08, 2)
    conf = "HIGH" if obv_div == "bullish" and oversold_mfi else "MEDIUM"
    return _build_signal("OBV_MFI", sym, cp, zlow, zhigh, stop, target, conf, reason, df)


# ─── Registry ───────────────────────────────────────────────────────


METHODS = {
    "SMC": signal_smc,
    "ORDER_FLOW": signal_order_flow,
    "VSA": signal_vsa,
    "WYCKOFF": signal_wyckoff,
    "HARMONIC": signal_harmonic,
    "FIBONACCI": signal_fibonacci,
    "ELLIOTT": signal_elliott,
    "ICHIMOKU": signal_ichimoku,
    "RSI_MACD": signal_rsi_macd,
    "BOLLINGER": signal_bollinger,
    "CHART_PATTERN": signal_chart_patterns,
    "CANDLE_PATTERN": signal_candle_patterns,
    "MOVING_AVG": signal_moving_avg,
    "SUPPORT_RESISTANCE": signal_support_resistance,
    "OBV_MFI": signal_obv_mfi,
}


def compute_all_method_signals(chart: dict, df: pd.DataFrame) -> list[dict]:
    """Run every methodology. Returns one signal dict per method (15 total)."""
    out = []
    for name, fn in METHODS.items():
        try:
            sig = fn(chart, df)
        except Exception as e:
            sig = _empty_signal(name, chart.get("symbol") or "", chart.get("current_price"),
                                 reason=f"error: {type(e).__name__}")
        out.append(sig)
    return out
