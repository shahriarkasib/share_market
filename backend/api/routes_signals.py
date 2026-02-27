"""Trading signals API routes - fast, cache-first design with DB persistence."""

import threading
import math
from fastapi import APIRouter, BackgroundTasks
from data.cache import cache
from data.repository import (
    read_all_historical_grouped,
    save_signals_to_db,
    load_signals_from_db,
    read_historical_for_symbol,
    get_active_holdings,
    save_signal_history,
    backfill_signal_accuracy,
    get_signal_history_for_symbol,
    get_signal_accuracy_report,
)
from analysis.signals import SignalGenerator
from config import CACHE_TTL_SIGNALS, MIN_DAILY_VALUE
from database import get_connection
from datetime import datetime, date
import logging

logger = logging.getLogger(__name__)

router = APIRouter()
signal_gen = SignalGenerator()

# Track computation state
_computing = False
_last_compute_time = None


def _clean_nan_dict(d: dict) -> dict:
    """Replace NaN/inf with None in a dict."""
    return {
        k: (
            None
            if isinstance(v, float) and (math.isnan(v) or math.isinf(v))
            else v
        )
        for k, v in d.items()
    }


def _get_signals() -> list:
    """Get signals from cache, falling back to DB."""
    signals = cache.get("all_signals")
    if signals is not None:
        return signals
    signals = load_signals_from_db()
    if signals:
        cache.set("all_signals", signals, CACHE_TTL_SIGNALS * 2)
    return signals or []


@router.get("/top")
async def get_top_signals(type: str = "all", limit: int = 20):
    """Get top-ranked trading signals. Returns cached/DB results instantly."""
    signals = _get_signals()

    if type == "buy":
        signals = [
            s for s in signals if s["signal_type"] in ("STRONG_BUY", "BUY")
        ]
        signals.sort(key=lambda x: x["short_term_score"], reverse=True)
    elif type == "sell":
        signals = [
            s for s in signals if s["signal_type"] in ("STRONG_SELL", "SELL")
        ]
        signals.sort(key=lambda x: x["short_term_score"])
    else:
        signals.sort(key=lambda x: abs(x["short_term_score"]), reverse=True)

    return signals[:limit]


@router.get("/summary")
async def get_signals_summary():
    """Get overall market signal summary."""
    cached = cache.get("signals_summary")
    if cached:
        return cached

    signals = _get_signals()

    strong_buy = sum(1 for s in signals if s["signal_type"] == "STRONG_BUY")
    buy = sum(1 for s in signals if s["signal_type"] == "BUY")
    hold = sum(1 for s in signals if s["signal_type"] == "HOLD")
    sell = sum(1 for s in signals if s["signal_type"] == "SELL")
    strong_sell = sum(1 for s in signals if s["signal_type"] == "STRONG_SELL")

    total = len(signals)
    bullish = strong_buy + buy
    bearish = sell + strong_sell

    if total > 0:
        if bullish / total > 0.5:
            sentiment = "BULLISH"
        elif bearish / total > 0.5:
            sentiment = "BEARISH"
        else:
            sentiment = "NEUTRAL"
    else:
        sentiment = "NEUTRAL"

    result = {
        "total_stocks": total,
        "strong_buy_count": strong_buy,
        "buy_count": buy,
        "hold_count": hold,
        "sell_count": sell,
        "strong_sell_count": strong_sell,
        "market_sentiment": sentiment,
        "last_updated": datetime.now().isoformat(),
        "is_computing": _computing,
    }

    if total > 0:
        cache.set("signals_summary", result, CACHE_TTL_SIGNALS)
    return result


@router.get("/status")
async def get_computation_status():
    """Check if signal computation is in progress."""
    signals = _get_signals()
    return {
        "is_computing": _computing,
        "total_signals": len(signals),
        "last_computed": _last_compute_time.isoformat()
        if _last_compute_time
        else None,
    }


@router.post("/recompute")
async def trigger_recompute(background_tasks: BackgroundTasks):
    """Manually trigger signal recomputation."""
    if _computing:
        return {"message": "Already computing"}
    background_tasks.add_task(_compute_all_signals_background)
    return {"message": "Computation started"}


@router.get("/suggestions")
async def get_suggestions():
    """Get top 5 entry picks (not in portfolio) and exit alerts for portfolio holdings."""
    cached = cache.get("suggestions")
    if cached:
        return cached

    signals = _get_signals()
    if not signals:
        return {"entry": [], "exit": []}

    # ---- Entry suggestions ----
    # Get portfolio symbols to exclude
    holdings = get_active_holdings()
    portfolio_symbols = {h["symbol"] for h in holdings}

    # Filter: BUY/STRONG_BUY, not in portfolio
    # Prefer t2_safe, but include all BUY signals ranked by composite score
    entry_candidates = []
    for s in signals:
        if s["signal_type"] not in ("STRONG_BUY", "BUY"):
            continue
        if s["symbol"] in portfolio_symbols:
            continue

        # Composite score: short_term_score * confidence * safety_bonus
        score_base = s.get("short_term_score", 0) or 0
        confidence = s.get("confidence", 0) or 0
        risk = s.get("risk_score", 50) or 50
        t2_bonus = 1.5 if s.get("t2_safe", False) else 1.0
        score = score_base * confidence * (1 - risk / 200.0) * t2_bonus
        entry_candidates.append({**s, "_score": score})

    entry_candidates.sort(key=lambda x: x["_score"], reverse=True)
    entry = []
    for c in entry_candidates[:5]:
        c.pop("_score", None)
        entry.append(c)

    # ---- Exit suggestions ----
    exit_alerts = []
    signals_by_symbol = {s["symbol"]: s for s in signals}

    for h in holdings:
        symbol = h["symbol"]
        reasons = []
        signal = signals_by_symbol.get(symbol)

        # Check maturity
        mat_date = h.get("maturity_date", "")
        is_mature = False
        if mat_date:
            try:
                is_mature = date.fromisoformat(mat_date) <= date.today()
            except (ValueError, TypeError):
                pass

        if not is_mature:
            continue  # Can't sell yet (T+2 not passed)

        # Check signal
        if signal and signal["signal_type"] in ("SELL", "STRONG_SELL"):
            reasons.append(f"Signal: {signal['signal_type']}")

        # Check target reached
        if signal:
            current = signal.get("ltp", 0)
            target = signal.get("target_price", 0)
            stop = signal.get("stop_loss", 0)
            buy_price = h.get("buy_price", 0)

            if target > 0 and current >= target:
                reasons.append(f"Target reached (৳{current:.1f} >= ৳{target:.1f})")

            if stop > 0 and current <= stop:
                reasons.append(f"Stop loss hit (৳{current:.1f} <= ৳{stop:.1f})")

            # Calculate P&L
            if buy_price > 0 and current > 0:
                pnl_pct = (current - buy_price) / buy_price * 100
            else:
                pnl_pct = 0
        else:
            pnl_pct = 0

        if reasons:
            exit_alerts.append({
                "holding": h,
                "signal": signal,
                "reasons": reasons,
                "pnl_pct": round(pnl_pct, 2),
            })

    # Sort by number of reasons (more urgent first)
    exit_alerts.sort(key=lambda x: len(x["reasons"]), reverse=True)

    result = {"entry": entry, "exit": exit_alerts}
    cache.set("suggestions", result, CACHE_TTL_SIGNALS)
    return result


@router.get("/accuracy")
async def get_accuracy():
    """Get signal accuracy report — how well did past predictions perform?"""
    cached = cache.get("signal_accuracy")
    if cached:
        return cached

    report = get_signal_accuracy_report()
    if report.get("total_verified", 0) > 0:
        cache.set("signal_accuracy", report, CACHE_TTL_SIGNALS)
    return report


@router.get("/history/{symbol}")
async def get_signal_history(symbol: str, limit: int = 30):
    """Get historical signal decisions for a specific stock."""
    return get_signal_history_for_symbol(symbol, limit)


@router.get("/{symbol}")
async def get_stock_signal(symbol: str):
    """Get detailed signal for a specific stock."""
    symbol = symbol.upper()

    # Check cached/DB signals first
    all_signals = _get_signals()
    for s in all_signals:
        if s["symbol"] == symbol:
            return s

    # Compute on-demand from local DB
    cached = cache.get(f"signal_{symbol}")
    if cached:
        return cached

    # Read from live_prices DB
    conn = get_connection()
    row = conn.execute(
        "SELECT * FROM live_prices WHERE symbol = ?", (symbol,)
    ).fetchone()
    conn.close()
    live_price = dict(row) if row else None

    df_hist = read_historical_for_symbol(symbol)
    if df_hist.empty or len(df_hist) < 20:
        return {"symbol": symbol, "signal_type": "HOLD", "reasoning": "Insufficient data"}

    signal = signal_gen.generate_signal(symbol, df_hist, live_price)
    cache.set(f"signal_{symbol}", signal, CACHE_TTL_SIGNALS)
    return signal


def _compute_all_signals_background():
    """Compute signals for top stocks from LOCAL DB data (fast)."""
    global _computing, _last_compute_time

    if _computing:
        return

    _computing = True
    try:
        logger.info("=== Starting signal computation from local DB ===")

        # 1. Get live prices from DB (not external API)
        conn = get_connection()
        rows = conn.execute("SELECT * FROM live_prices").fetchall()
        conn.close()

        if not rows:
            logger.warning("No live prices in DB for signal computation")
            return

        import pandas as pd

        df_live = pd.DataFrame([dict(r) for r in rows])

        # 2. Filter by liquidity
        if "value" in df_live.columns:
            df_live["value"] = pd.to_numeric(df_live["value"], errors="coerce")
            df_live = df_live[df_live["value"] >= MIN_DAILY_VALUE]

        # 3. Top 100 by trading value
        df_live = df_live.sort_values("value", ascending=False)
        top_symbols = df_live["symbol"].head(100).tolist()

        logger.info(f"Computing signals for {len(top_symbols)} stocks...")

        # 4. Read historical data from LOCAL DB (fast!)
        all_hist = read_all_historical_grouped(min_rows_per_symbol=20)
        logger.info(f"Loaded history for {len(all_hist)} symbols from DB")

        # 5. Generate signals
        signals = []
        processed = 0
        for symbol in top_symbols:
            try:
                stock_live = df_live[df_live["symbol"] == symbol].iloc[0].to_dict()
                stock_live = _clean_nan_dict(stock_live)

                if symbol not in all_hist or len(all_hist[symbol]) < 20:
                    continue

                df_hist = all_hist[symbol]
                signal = signal_gen.generate_signal(symbol, df_hist, stock_live)
                signals.append(signal)
                processed += 1

                if processed % 20 == 0:
                    logger.info(f"  Processed {processed}/{len(top_symbols)} stocks")

            except Exception as e:
                logger.error(f"Error computing signal for {symbol}: {e}")
                continue

        # 6. Cache + persist to DB
        cache.set("all_signals", signals, CACHE_TTL_SIGNALS * 2)
        cache.delete("signals_summary")
        save_signals_to_db(signals)

        # 7. Save daily snapshot to signal_history (append-only for accuracy tracking)
        save_signal_history(signals)

        # 8. Backfill accuracy for older entries
        try:
            backfill_signal_accuracy()
        except Exception as e:
            logger.error(f"Accuracy backfill error: {e}")

        _last_compute_time = datetime.now()
        logger.info(f"=== Computed {len(signals)} signals ({processed} processed) ===")

        # Log top signals
        buy_signals = [
            s
            for s in signals
            if s["signal_type"] in ("STRONG_BUY", "BUY")
        ]
        buy_signals.sort(key=lambda x: x["short_term_score"], reverse=True)
        if buy_signals:
            logger.info("Top buy signals:")
            for s in buy_signals[:5]:
                logger.info(
                    f"  {s['symbol']}: {s['signal_type']} score={s['short_term_score']} conf={s['confidence']}"
                )

    except Exception as e:
        logger.error(f"Signal computation failed: {e}")
    finally:
        _computing = False


def start_background_computation():
    """Start signal computation in a background thread."""
    thread = threading.Thread(
        target=_compute_all_signals_background, daemon=True
    )
    thread.start()
