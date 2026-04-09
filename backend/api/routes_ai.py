"""V2 AI Analysis API endpoints.

Serves data from ai_analysis, stock_indicators, and market_analysis tables.
"""

from fastapi import APIRouter, Query
from database import get_connection
import json

router = APIRouter()


@router.get("/stocks")
async def get_ai_stocks(
    signal: str = Query(None, description="Filter by signal: BUY, SELL, HOLD, WATCH, AVOID"),
    category: str = Query(None, description="Filter by category: A, B"),
):
    """Get AI analysis for all stocks, optionally filtered by signal."""
    conn = get_connection()
    try:
        where_clauses = ["a.date = (SELECT MAX(date) FROM ai_analysis)"]
        params = []

        if signal:
            where_clauses.append("a.overall_signal = %s")
            params.append(signal.upper())
        if category:
            where_clauses.append("f.category = %s")
            params.append(category.upper())

        where_sql = " AND ".join(where_clauses)

        result = conn.execute(f"""
            SELECT
                a.symbol, a.date, a.overall_signal, a.signal_strength, a.confidence,
                a.classification, a.position_type,
                a.score_overall, a.score_money_flow, a.score_momentum,
                a.score_price_action, a.score_volatility, a.score_fundamentals, a.score_news,
                a.one_liner,
                a.entry_low, a.entry_high, a.stop_loss, a.stop_loss_method,
                a.target_1, a.target_2, a.for_new_buyer, a.for_holder,
                lp.ltp, lp.change_pct, lp.volume,
                f.sector, f.category, f.eps_ttm, f.pe_ratio, f.dividend_yield_pct,
                f.high_52w, f.low_52w,
                si.rsi_14, si.cmf_20, si.cmf_pos_streak, si.cmf_neg_streak,
                si.adx_14, si.macd_hist, si.ma_aligned, si.atr_pct, si.vol_ratio,
                si.chg_5d, si.chg_20d, si.support, si.resistance
            FROM ai_analysis a
            LEFT JOIN live_prices lp ON a.symbol = lp.symbol
            LEFT JOIN fundamentals f ON a.symbol = f.symbol
            LEFT JOIN stock_indicators si ON a.symbol = si.symbol
                AND si.timeframe = 'daily'
                AND si.date = (SELECT MAX(date) FROM stock_indicators WHERE timeframe = 'daily')
            WHERE {where_sql}
            ORDER BY a.score_overall DESC NULLS LAST
        """, tuple(params) if params else None)

        rows = result.fetchall()
        conn.close()

        stocks = []
        for r in rows:
            stocks.append({
                "symbol": r["symbol"],
                "date": str(r["date"]),
                "overall_signal": r["overall_signal"],
                "signal_strength": r["signal_strength"],
                "confidence": r["confidence"],
                "classification": r["classification"],
                "position_type": r["position_type"],
                "score_overall": r["score_overall"],
                "score_money_flow": r["score_money_flow"],
                "score_momentum": r["score_momentum"],
                "score_price_action": r["score_price_action"],
                "score_volatility": r["score_volatility"],
                "score_fundamentals": r["score_fundamentals"],
                "score_news": r["score_news"],
                "one_liner": r["one_liner"],
                "entry_low": r["entry_low"],
                "entry_high": r["entry_high"],
                "stop_loss": r["stop_loss"],
                "stop_loss_method": r["stop_loss_method"],
                "target_1": r["target_1"],
                "target_2": r["target_2"],
                "for_new_buyer": r["for_new_buyer"],
                "for_holder": r["for_holder"],
                "ltp": r["ltp"],
                "change_pct": r["change_pct"],
                "volume": r["volume"],
                "sector": r["sector"],
                "category": r["category"],
                "eps_ttm": r["eps_ttm"],
                "pe_ratio": r["pe_ratio"],
                "dividend_yield_pct": r["dividend_yield_pct"],
                "high_52w": r["high_52w"],
                "low_52w": r["low_52w"],
                "rsi_14": _round(r["rsi_14"], 1),
                "cmf_20": _round(r["cmf_20"], 3),
                "cmf_pos_streak": r["cmf_pos_streak"],
                "cmf_neg_streak": r["cmf_neg_streak"],
                "adx_14": _round(r["adx_14"], 1),
                "macd_hist": _round(r["macd_hist"], 3),
                "ma_aligned": r["ma_aligned"],
                "atr_pct": _round(r["atr_pct"], 2),
                "vol_ratio": _round(r["vol_ratio"], 2),
                "chg_5d": _round(r["chg_5d"], 1),
                "chg_20d": _round(r["chg_20d"], 1),
                "support": r["support"],
                "resistance": r["resistance"],
            })

        return {"stocks": stocks, "count": len(stocks)}

    except Exception as e:
        conn.close()
        return {"stocks": [], "count": 0, "error": str(e)}


@router.get("/stocks/{symbol}")
async def get_ai_stock_detail(symbol: str):
    """Get full AI analysis for a single stock."""
    conn = get_connection()
    try:
        result = conn.execute("""
            SELECT a.*, lp.ltp, lp.change_pct, lp.volume,
                   f.sector, f.category, f.eps_ttm, f.pe_ratio,
                   f.dividend_yield_pct, f.high_52w, f.low_52w
            FROM ai_analysis a
            LEFT JOIN live_prices lp ON a.symbol = lp.symbol
            LEFT JOIN fundamentals f ON a.symbol = f.symbol
            WHERE a.symbol = %s
            ORDER BY a.date DESC LIMIT 1
        """, (symbol.upper(),))
        row = result.fetchone()
        conn.close()

        if not row:
            return {"error": "No analysis found", "symbol": symbol}

        analysis_json = row["analysis_json"]
        if isinstance(analysis_json, str):
            analysis_json = json.loads(analysis_json)

        return {
            "symbol": row["symbol"],
            "date": str(row["date"]),
            "overall_signal": row["overall_signal"],
            "signal_strength": row["signal_strength"],
            "confidence": row["confidence"],
            "classification": row["classification"],
            "position_type": row["position_type"],
            "scores": {
                "overall": row["score_overall"],
                "money_flow": row["score_money_flow"],
                "momentum": row["score_momentum"],
                "price_action": row["score_price_action"],
                "volatility": row["score_volatility"],
                "fundamentals": row["score_fundamentals"],
                "news": row["score_news"],
            },
            "one_liner": row["one_liner"],
            "entry_low": row["entry_low"],
            "entry_high": row["entry_high"],
            "stop_loss": row["stop_loss"],
            "stop_loss_method": row["stop_loss_method"],
            "target_1": row["target_1"],
            "target_2": row["target_2"],
            "for_new_buyer": row["for_new_buyer"],
            "for_holder": row["for_holder"],
            "ltp": row["ltp"],
            "change_pct": row["change_pct"],
            "sector": row["sector"],
            "category": row["category"],
            "pe_ratio": row["pe_ratio"],
            "dividend_yield_pct": row["dividend_yield_pct"],
            "high_52w": row["high_52w"],
            "low_52w": row["low_52w"],
            "analysis": analysis_json,
        }

    except Exception as e:
        conn.close()
        return {"error": str(e)}


@router.get("/market")
async def get_ai_market():
    """Get market-level analysis and regime."""
    conn = get_connection()
    try:
        # Market summary (live)
        ms = conn.execute("SELECT * FROM market_summary WHERE id = 1").fetchone()

        # DSEX trend from recent history
        dsex = conn.execute("""
            SELECT date, dsex_index FROM dsex_history
            WHERE dsex_index > 0
            ORDER BY date DESC LIMIT 10
        """).fetchall()

        # Market analysis from AI (if available)
        ma = conn.execute("""
            SELECT * FROM market_analysis
            ORDER BY date DESC LIMIT 1
        """).fetchone()

        conn.close()

        dsex_data = []
        for d in (dsex or []):
            dsex_data.append({"date": str(d["date"]), "dsex": d["dsex_index"]})

        # Signal distribution
        conn2 = get_connection()
        dist = conn2.execute("""
            SELECT overall_signal, COUNT(*) as cnt
            FROM ai_analysis
            WHERE date = (SELECT MAX(date) FROM ai_analysis)
            GROUP BY overall_signal
        """).fetchall()
        conn2.close()

        signal_dist = {r["overall_signal"]: r["cnt"] for r in (dist or [])}

        return {
            "dsex": ms["dsex_index"] if ms else None,
            "dsex_change": ms["dsex_change"] if ms else None,
            "dsex_change_pct": ms["dsex_change_pct"] if ms else None,
            "advances": ms["advances"] if ms else 0,
            "declines": ms["declines"] if ms else 0,
            "unchanged": ms["unchanged"] if ms else 0,
            "turnover_cr": round((ms["total_value"] or 0) / 100, 1) if ms else 0,
            "market_status": ms["market_status"] if ms else "UNKNOWN",
            "regime": ma["regime"] if ma else None,
            "ai_summary": ma["ai_summary"] if ma else None,
            "is_good_day_to_buy": ma["is_good_day_to_buy"] if ma else None,
            "signal_distribution": signal_dist,
            "dsex_history": dsex_data,
        }

    except Exception as e:
        return {"error": str(e)}


@router.get("/summary/{symbol}")
async def get_stock_summary(symbol: str):
    """Auto-generated structural analysis for a stock — like a human analyst post.

    No AI call needed — pure data formatting from price_structure + stock_indicators.
    """
    conn = get_connection()
    try:
        # Load all data
        ps = conn.execute("""
            SELECT * FROM price_structure WHERE symbol = %s
            ORDER BY date DESC LIMIT 1
        """, (symbol.upper(),)).fetchone()

        si = conn.execute("""
            SELECT * FROM stock_indicators WHERE symbol = %s AND timeframe = 'daily'
            ORDER BY date DESC LIMIT 1
        """, (symbol.upper(),)).fetchone()

        lp = conn.execute("""
            SELECT ltp, change_pct, volume, open, high, low, close_prev
            FROM live_prices WHERE symbol = %s
        """, (symbol.upper(),)).fetchone()

        f = conn.execute("""
            SELECT sector, category, pe_ratio, eps_ttm, dividend_yield_pct, high_52w, low_52w
            FROM fundamentals WHERE symbol = %s
        """, (symbol.upper(),)).fetchone()

        # Load DSEX structure for market context
        dsex_ps = conn.execute("""
            SELECT swing_structure, pivot_daily, mean_reversion_score
            FROM price_structure WHERE symbol = 'DSEX'
            ORDER BY date DESC LIMIT 1
        """).fetchone()

        dsex_ms = conn.execute("""
            SELECT dsex_index, dsex_change, dsex_change_pct, advances, declines
            FROM market_summary WHERE id = 1
        """).fetchone()

        conn.close()

        if not ps or not si or not lp:
            return {"symbol": symbol.upper(), "summary": "Insufficient data for analysis.", "sections": {}}

        ltp = float(lp["ltp"] or 0)
        change_pct = float(lp["change_pct"] or 0)
        volume = int(lp["volume"] or 0)
        prev_close = float(lp["close_prev"] or 0)

        rsi = _round(si["rsi_14"], 1)
        cmf = _round(si["cmf_20"], 3)
        cmf_pos = si.get("cmf_pos_streak", 0) or 0
        cmf_neg = si.get("cmf_neg_streak", 0) or 0
        adx = _round(si["adx_14"], 1)
        ma_aligned = si.get("ma_aligned", False)
        macd_hist = _round(si["macd_hist"], 3)
        vol_ratio = _round(si["vol_ratio"], 2)
        ema9 = _round(si.get("ema_9"), 1)
        ema21 = _round(si.get("ema_21"), 1)
        ema50 = _round(si.get("ema_50"), 1)
        sma200 = _round(si.get("sma_200"), 1)
        chg5d = _round(si.get("chg_5d"), 1)
        chg20d = _round(si.get("chg_20d"), 1)
        atr_pct = _round(si.get("atr_pct"), 2)

        swing = ps.get("swing_structure", "UNKNOWN")
        candle = ps.get("candle_pattern")
        candle_conf = ps.get("candle_confirmed", False)
        ema_sup = ps.get("ema_support")
        ema_res = ps.get("ema_resistance")
        mr_score = ps.get("mean_reversion_score", 0)
        pivot = ps.get("pivot_daily") or {}
        wpivot = ps.get("pivot_weekly") or {}
        sr_sup = ps.get("support_levels") or []
        sr_res = ps.get("resistance_levels") or []
        fib = ps.get("fib_levels") or {}
        gaps = ps.get("unfilled_gaps") or []
        swings = ps.get("swings_json") or []

        high_52w = float(f["high_52w"] or 0) if f else 0
        low_52w = float(f["low_52w"] or 0) if f else 0
        sector = f["sector"] if f else None
        category = f["category"] if f else None
        pe = f["pe_ratio"] if f else None

        # --- Build sections ---
        sections = {}

        # 1. Structure & Trend
        structure_lines = []
        swing_desc = {
            "UPTREND": "making higher highs and higher lows — bullish structure",
            "DOWNTREND": "making lower highs and lower lows — bearish structure",
            "HIGHER_LOWS": "forming higher lows — accumulation pattern",
            "LOWER_HIGHS": "forming lower highs — distribution pattern",
            "CONTRACTING": "contracting range — squeeze forming, breakout imminent",
            "EXPANDING": "expanding range — increased volatility",
        }.get(swing, "no clear swing pattern")
        structure_lines.append(f"Price is {swing_desc}.")

        if ma_aligned:
            structure_lines.append(f"All moving averages aligned bullishly (EMA9 {ema9} > EMA21 {ema21} > EMA50 {ema50}).")
        else:
            ma_parts = []
            if ema9 and ema21:
                if ema9 > ema21:
                    ma_parts.append("short-term bullish (EMA9 > EMA21)")
                else:
                    ma_parts.append("short-term bearish (EMA9 < EMA21)")
            if sma200:
                if ltp > sma200:
                    ma_parts.append(f"above SMA200 ({sma200})")
                else:
                    ma_parts.append(f"below SMA200 ({sma200})")
            if ma_parts:
                structure_lines.append("MAs: " + ", ".join(ma_parts) + ".")

        if ema_sup:
            structure_lines.append(f"{ema_sup} acting as dynamic support — price bouncing off it.")
        if ema_res:
            structure_lines.append(f"{ema_res} acting as dynamic resistance — price being rejected.")

        if adx:
            if adx > 30:
                structure_lines.append(f"ADX {adx} — strong trend in place.")
            elif adx > 20:
                structure_lines.append(f"ADX {adx} — moderate trend developing.")
            else:
                structure_lines.append(f"ADX {adx} — no clear trend, choppy conditions.")

        sections["structure"] = " ".join(structure_lines)

        # 2. Key Levels
        levels_lines = []
        p = pivot.get("p")
        r1 = pivot.get("r1")
        r2 = pivot.get("r2")
        s1 = pivot.get("s1")
        s2 = pivot.get("s2")
        if p:
            if ltp > float(r1 or 0):
                levels_lines.append(f"Trading above Pivot R1 ({r1}) — bullish positioning. Next target R2 at {r2}.")
            elif ltp > float(p):
                levels_lines.append(f"Above pivot ({p}), targeting R1 at {r1}.")
            elif ltp > float(s1 or 0):
                levels_lines.append(f"Between Pivot ({p}) and S1 ({s1}) — neutral zone.")
            else:
                levels_lines.append(f"Below S1 ({s1}) — bearish. Watch S2 at {s2}.")

        if sr_sup:
            nearest_sup = sr_sup[0]
            levels_lines.append(f"Nearest support: {nearest_sup['price']} ({nearest_sup['touches']} historical touches, {nearest_sup['strength']}).")
        if sr_res:
            nearest_res = sr_res[0]
            levels_lines.append(f"Nearest resistance: {nearest_res['price']} ({nearest_res['touches']} historical touches, {nearest_res['strength']}).")

        if fib:
            fib_ret = fib.get("retracement", {})
            fib_ext = fib.get("extension", {})
            fib_trend = fib.get("trend", "")
            if fib_trend == "UP" and fib_ext:
                ext_1 = fib_ext.get("1.0")
                ext_1618 = fib_ext.get("1.618")
                if ext_1 and ltp > float(ext_1):
                    levels_lines.append(f"Broke above Fib 1.0 extension ({ext_1}). Next Fib target: 1.618 at {ext_1618}.")
                elif ext_1:
                    levels_lines.append(f"Fib 1.0 extension target: {ext_1}. Stretch: 1.618 at {ext_1618}.")
            elif fib_trend == "DOWN" and fib_ret:
                r_382 = fib_ret.get("0.382")
                r_618 = fib_ret.get("0.618")
                if r_382:
                    levels_lines.append(f"In downtrend. Fib retracement levels: 0.382 at {r_382}, 0.618 at {r_618}.")

        sections["key_levels"] = " ".join(levels_lines)

        # 3. Momentum & Flow
        momentum_lines = []
        if rsi:
            if rsi > 70:
                momentum_lines.append(f"RSI {rsi} — overbought. Pullback risk.")
            elif rsi > 55:
                momentum_lines.append(f"RSI {rsi} — bullish momentum with room to run.")
            elif rsi > 40:
                momentum_lines.append(f"RSI {rsi} — neutral zone.")
            else:
                momentum_lines.append(f"RSI {rsi} — oversold. Bounce potential.")

        if cmf:
            if cmf > 0.1 and cmf_pos > 5:
                momentum_lines.append(f"CMF +{cmf} positive for {cmf_pos} consecutive days — strong institutional accumulation.")
            elif cmf > 0 and cmf_pos > 0:
                momentum_lines.append(f"CMF +{cmf} ({cmf_pos} days positive) — mild buying.")
            elif cmf < -0.1 and cmf_neg > 5:
                momentum_lines.append(f"CMF {cmf} negative for {cmf_neg} consecutive days — distribution.")
            elif cmf < 0:
                momentum_lines.append(f"CMF {cmf} ({cmf_neg} days negative) — selling pressure.")

        if macd_hist:
            if macd_hist > 0:
                momentum_lines.append(f"MACD histogram positive ({macd_hist}) — bullish momentum.")
            else:
                momentum_lines.append(f"MACD histogram negative ({macd_hist}) — bearish momentum.")

        if vol_ratio:
            if vol_ratio > 2:
                momentum_lines.append(f"Volume {vol_ratio}x average — very high activity.")
            elif vol_ratio > 1.3:
                momentum_lines.append(f"Volume {vol_ratio}x average — above normal.")
            elif vol_ratio < 0.5:
                momentum_lines.append(f"Volume {vol_ratio}x average — very low activity.")

        sections["momentum"] = " ".join(momentum_lines)

        # 4. Candlestick & Pattern
        if candle:
            conf_str = "volume-confirmed" if candle_conf else "unconfirmed"
            pattern_map = {
                "HAMMER": "Hammer pattern detected — bullish reversal signal at support",
                "BULLISH_ENGULFING": "Bullish engulfing — strong reversal signal",
                "BEARISH_ENGULFING": "Bearish engulfing — strong sell signal",
                "SHOOTING_STAR": "Shooting star — bearish reversal at resistance",
                "DOJI": "Doji — indecision, watch for next candle direction",
                "BULLISH_MARUBOZU": "Bullish marubozu — strong buyer dominance, no shadows",
                "BEARISH_MARUBOZU": "Bearish marubozu — strong seller dominance",
                "GRAVESTONE_DOJI": "Gravestone doji — bearish reversal signal, sellers pushed price back down",
                "DRAGONFLY_DOJI": "Dragonfly doji — bullish reversal signal, buyers defended the low",
                "BULLISH_HARAMI": "Bullish harami — potential reversal forming",
                "BEARISH_HARAMI": "Bearish harami — potential reversal forming",
                "INVERTED_HAMMER": "Inverted hammer — bullish signal if confirmed next session",
            }
            desc = pattern_map.get(candle, candle)
            sections["candle"] = f"{desc} ({conf_str})."
        else:
            sections["candle"] = "No significant candlestick pattern on the last candle."

        # 5. Gaps
        if gaps:
            gap_lines = []
            for g in gaps[:3]:
                gap_lines.append(f"{g['type']} gap at {g['gap_low']}-{g['gap_high']} from {g['date']} (unfilled — may act as magnet)")
            sections["gaps"] = ". ".join(gap_lines) + "."
        else:
            sections["gaps"] = "No unfilled gaps."

        # 6. Market Context
        ctx_lines = []
        if dsex_ms:
            dsex = dsex_ms["dsex_index"]
            dsex_chg = dsex_ms["dsex_change_pct"]
            adv = dsex_ms["advances"]
            dec = dsex_ms["declines"]
            if dsex_chg > 0:
                ctx_lines.append(f"DSEX {dsex} (+{dsex_chg}%), {adv} advances vs {dec} declines — market supportive.")
            else:
                ctx_lines.append(f"DSEX {dsex} ({dsex_chg}%), {adv} advances vs {dec} declines — market weak.")
        if dsex_ps:
            dsex_swing = dsex_ps.get("swing_structure")
            dsex_pivot = dsex_ps.get("pivot_daily") or {}
            dp = dsex_pivot.get("p")
            if dsex_swing:
                ctx_lines.append(f"DSEX structure: {dsex_swing}.")
            if dp:
                ctx_lines.append(f"DSEX pivot: {dp}.")
        sections["market_context"] = " ".join(ctx_lines)

        # 7. Risk
        risk_lines = []
        if atr_pct:
            risk_lines.append(f"ATR {atr_pct}% — expected daily range. T+2 max risk ~{round(atr_pct * 2, 1)}%.")
        if high_52w and ltp > 0:
            pct_from_high = round((ltp - high_52w) / high_52w * 100, 1)
            pct_from_low = round((ltp - low_52w) / low_52w * 100, 1) if low_52w else 0
            risk_lines.append(f"52W range: {low_52w}-{high_52w}. Currently {pct_from_high}% from high, +{pct_from_low}% from low.")
        sections["risk"] = " ".join(risk_lines)

        # 8. Action Summary
        action_lines = []
        if mr_score >= 60 and rsi and rsi < 40:
            action_lines.append(f"Bounce setup — mean reversion score {mr_score}/100 at support with RSI {rsi}.")
        if swing == "UPTREND" and ma_aligned:
            action_lines.append("Uptrend intact — look for pullbacks to EMA as entry.")
        elif swing == "DOWNTREND":
            action_lines.append("Downtrend — avoid fresh entries unless at strong support with reversal candle.")
        if candle in ("HAMMER", "BULLISH_ENGULFING", "DRAGONFLY_DOJI", "BULLISH_MARUBOZU") and candle_conf:
            action_lines.append("Bullish candle confirmed — supports entry if at support level.")
        if candle in ("SHOOTING_STAR", "BEARISH_ENGULFING", "GRAVESTONE_DOJI", "BEARISH_MARUBOZU") and candle_conf:
            action_lines.append("Bearish candle confirmed — consider taking profit or tightening stop.")

        # Targets from pivot
        if p and r1:
            if ltp > float(r1):
                action_lines.append(f"Target: R2 at {r2}. SL: R1 at {r1} (now support).")
            elif ltp > float(p):
                action_lines.append(f"Target: R1 at {r1}. SL: Pivot at {p}.")
            elif s1:
                action_lines.append(f"Target: Pivot at {p}. SL: S1 at {s1}.")

        sections["action"] = " ".join(action_lines) if action_lines else "No clear actionable setup. Wait for price to reach a key level."

        # --- Build full summary ---
        title = f"{symbol.upper()} at ৳{ltp} ({'+' if change_pct > 0 else ''}{change_pct}%)"
        if sector:
            title += f" | {sector}"
        if category:
            title += f" | Cat {category}"

        full_summary = f"""{title}

Structure: {sections['structure']}

Key Levels: {sections['key_levels']}

Momentum: {sections['momentum']}

Pattern: {sections['candle']}

{f"Gaps: {sections['gaps']}" if gaps else ""}

Market: {sections['market_context']}

Risk: {sections['risk']}

Action: {sections['action']}""".strip()

        return {
            "symbol": symbol.upper(),
            "ltp": ltp,
            "change_pct": change_pct,
            "summary": full_summary,
            "sections": sections,
            "data": {
                "swing_structure": swing,
                "pivot_daily": pivot,
                "pivot_weekly": wpivot,
                "support_levels": sr_sup,
                "resistance_levels": sr_res,
                "fib_levels": fib,
                "candle_pattern": candle,
                "candle_confirmed": candle_conf,
                "ema_support": ema_sup,
                "ema_resistance": ema_res,
                "mean_reversion_score": mr_score,
                "rsi": rsi,
                "cmf": cmf,
                "cmf_pos_streak": cmf_pos,
                "cmf_neg_streak": cmf_neg,
                "adx": adx,
                "ma_aligned": ma_aligned,
                "vol_ratio": vol_ratio,
            },
        }

    except Exception as e:
        conn.close()
        return {"symbol": symbol.upper(), "summary": f"Error: {e}", "sections": {}}


@router.get("/alerts")
async def get_live_alerts(symbol: str = None):
    """Get live trading alerts for today."""
    conn = get_connection()
    try:
        if symbol:
            result = conn.execute(
                "SELECT * FROM live_alerts WHERE date = CURRENT_DATE AND symbol = %s ORDER BY time DESC",
                (symbol.upper(),),
            )
        else:
            result = conn.execute(
                "SELECT * FROM live_alerts WHERE date = CURRENT_DATE ORDER BY time DESC LIMIT 100"
            )
        rows = result.fetchall()
        conn.close()

        alerts = []
        for r in rows:
            alerts.append({
                "id": r["id"],
                "symbol": r["symbol"],
                "time": str(r["time"]),
                "alert_type": r["alert_type"],
                "severity": r["severity"],
                "price": r["price"],
                "level_name": r["level_name"],
                "level_price": r["level_price"],
                "message": r["message"],
                "extra": r["extra"],
            })
        return {"alerts": alerts, "count": len(alerts)}
    except Exception as e:
        conn.close()
        return {"alerts": [], "count": 0, "error": str(e)}


@router.get("/live-signals")
async def get_live_signals():
    """Live intraday opening signals — compare current LTP to yesterday's close.

    Shows gap direction, volume pace, and momentum for all stocks.
    Updates every time live prices refresh (every 5 min during trading).
    """
    conn = get_connection()
    try:
        rows = conn.execute("""
            SELECT
                lp.symbol,
                lp.ltp,
                lp.open,
                lp.close_prev,
                lp.change_pct,
                lp.volume,
                lp.high,
                lp.low,
                f.category,
                -- Yesterday's daily data for comparison
                si.close as prev_close,
                si.volume as prev_volume,
                si.avg_volume_20,
                si.rsi_14,
                si.cmf_20,
                si.atr_14,
                -- Price structure
                ps.pivot_daily,
                ps.candle_pattern as yesterday_candle,
                ps.swing_structure,
                ps.support_levels,
                ps.resistance_levels,
                ps.mean_reversion_score
            FROM live_prices lp
            LEFT JOIN fundamentals f ON lp.symbol = f.symbol
            LEFT JOIN stock_indicators si ON lp.symbol = si.symbol
                AND si.timeframe = 'daily'
                AND si.date = (SELECT MAX(date) FROM stock_indicators WHERE timeframe = 'daily')
            LEFT JOIN price_structure ps ON lp.symbol = ps.symbol
                AND ps.date = (SELECT MAX(date) FROM price_structure)
            WHERE lp.ltp > 0 AND f.category IN ('A', 'B')
            ORDER BY ABS(lp.change_pct) DESC
        """).fetchall()
        conn.close()

        signals = []
        for r in rows:
            ltp = float(r["ltp"] or 0)
            open_price = float(r["open"] or 0)
            prev_close = float(r["close_prev"] or r["prev_close"] or 0)
            volume = int(r["volume"] or 0)
            avg_vol = float(r["avg_volume_20"] or 0)
            high = float(r["high"] or 0)
            low = float(r["low"] or 0)
            change_pct = float(r["change_pct"] or 0)

            if prev_close == 0 or ltp == 0:
                continue

            # Gap analysis
            gap_pct = (open_price - prev_close) / prev_close * 100 if prev_close > 0 else 0
            if gap_pct > 1:
                gap_type = "GAP_UP"
            elif gap_pct < -1:
                gap_type = "GAP_DOWN"
            else:
                gap_type = "FLAT"

            # Intraday candle shape
            if ltp > open_price:
                body = "BULLISH"
            elif ltp < open_price:
                body = "BEARISH"
            else:
                body = "DOJI"

            # Shadow analysis
            upper_shadow = high - max(ltp, open_price) if high > 0 else 0
            lower_shadow = min(ltp, open_price) - low if low > 0 else 0
            body_size = abs(ltp - open_price)
            total_range = high - low if high > low else 0.01

            shadow_signal = None
            if total_range > 0:
                if upper_shadow > body_size * 2 and lower_shadow < body_size * 0.5:
                    shadow_signal = "SELLING_PRESSURE"  # long upper shadow
                elif lower_shadow > body_size * 2 and upper_shadow < body_size * 0.5:
                    shadow_signal = "BUYING_SUPPORT"  # long lower shadow (hammer-like)
                elif upper_shadow > body_size * 1.5 and lower_shadow > body_size * 1.5:
                    shadow_signal = "INDECISION"  # doji-like

            # Volume pace
            # Estimate expected volume at this time of day (proportional)
            # Market hours: 10:00-14:30 = 270 minutes
            vol_ratio = volume / avg_vol if avg_vol > 0 else 0

            if vol_ratio > 2:
                vol_signal = "VERY_HIGH"
            elif vol_ratio > 1.3:
                vol_signal = "HIGH"
            elif vol_ratio > 0.7:
                vol_signal = "NORMAL"
            else:
                vol_signal = "LOW"

            # Momentum verdict
            momentum = "NEUTRAL"
            if change_pct > 3 and vol_ratio > 1.3 and body == "BULLISH":
                momentum = "STRONG_BULLISH"
            elif change_pct > 1 and body == "BULLISH":
                momentum = "BULLISH"
            elif change_pct < -3 and vol_ratio > 1.3 and body == "BEARISH":
                momentum = "STRONG_BEARISH"
            elif change_pct < -1 and body == "BEARISH":
                momentum = "BEARISH"
            elif gap_type == "GAP_UP" and ltp < open_price:
                momentum = "GAP_FADE"
            elif gap_type == "GAP_DOWN" and ltp > open_price:
                momentum = "GAP_FILL_UP"

            # Pivot context
            pivot = r["pivot_daily"] or {}
            pivot_p = pivot.get("p")
            pivot_r1 = pivot.get("r1")
            pivot_s1 = pivot.get("s1")

            pivot_position = None
            if pivot_p:
                if ltp > float(pivot_r1 or 999999):
                    pivot_position = "ABOVE_R1"
                elif ltp > float(pivot_p):
                    pivot_position = "ABOVE_PIVOT"
                elif ltp > float(pivot_s1 or 0):
                    pivot_position = "BELOW_PIVOT"
                else:
                    pivot_position = "BELOW_S1"

            signals.append({
                "symbol": r["symbol"],
                "category": r["category"],
                "ltp": ltp,
                "open": open_price,
                "prev_close": prev_close,
                "change_pct": round(change_pct, 2),
                "high": high,
                "low": low,
                "volume": volume,
                "gap_type": gap_type,
                "gap_pct": round(gap_pct, 2),
                "body": body,
                "shadow_signal": shadow_signal,
                "vol_ratio": round(vol_ratio, 2),
                "vol_signal": vol_signal,
                "momentum": momentum,
                "pivot_p": pivot_p,
                "pivot_r1": pivot_r1,
                "pivot_s1": pivot_s1,
                "pivot_position": pivot_position,
                "swing_structure": r["swing_structure"],
                "yesterday_candle": r["yesterday_candle"],
                "rsi": _round(r["rsi_14"], 1),
                "cmf": _round(r["cmf_20"], 3),
                "mean_reversion_score": r["mean_reversion_score"],
            })

        return {"signals": signals, "count": len(signals)}

    except Exception as e:
        conn.close()
        return {"signals": [], "count": 0, "error": str(e)}


def _round(v, decimals):
    if v is None:
        return None
    return round(float(v), decimals)
