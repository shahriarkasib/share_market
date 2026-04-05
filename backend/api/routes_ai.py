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


def _round(v, decimals):
    if v is None:
        return None
    return round(float(v), decimals)
