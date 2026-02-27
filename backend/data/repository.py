"""Repository for persisting and reading daily_prices, signals, and holdings from SQLite."""

import json
import logging
import math
from datetime import datetime
import pandas as pd
from database import get_connection

logger = logging.getLogger(__name__)


# ======================== daily_prices ========================


def get_daily_prices_count() -> int:
    conn = get_connection()
    count = conn.execute("SELECT COUNT(*) FROM daily_prices").fetchone()[0]
    conn.close()
    return count


def bulk_insert_daily_prices(df: pd.DataFrame) -> int:
    """Insert historical OHLCV DataFrame into daily_prices (INSERT OR IGNORE)."""
    if df.empty:
        return 0

    conn = get_connection()
    inserted = 0
    for _, row in df.iterrows():
        try:
            conn.execute(
                """INSERT OR IGNORE INTO daily_prices
                   (symbol, date, open, high, low, close, volume, value, trade_count)
                   VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)""",
                (
                    row.get("symbol", ""),
                    str(row.get("date", ""))[:10],
                    _safe_float(row.get("open")),
                    _safe_float(row.get("high")),
                    _safe_float(row.get("low")),
                    _safe_float(row.get("close")),
                    int(row.get("volume", 0) or 0),
                    _safe_float(row.get("value")),
                    int(row.get("trade_count", 0) or 0),
                ),
            )
            inserted += 1
        except Exception as e:
            logger.error(f"Insert error for {row.get('symbol')}: {e}")
    conn.commit()
    conn.close()
    return inserted


def upsert_today_prices(df: pd.DataFrame, today_str: str):
    """INSERT OR REPLACE today's rows from live prices into daily_prices."""
    if df.empty:
        return
    conn = get_connection()
    for _, row in df.iterrows():
        try:
            conn.execute(
                """INSERT OR REPLACE INTO daily_prices
                   (symbol, date, open, high, low, close, volume, value, trade_count)
                   VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)""",
                (
                    row.get("symbol", ""),
                    today_str,
                    _safe_float(row.get("open")),
                    _safe_float(row.get("high")),
                    _safe_float(row.get("low")),
                    _safe_float(row.get("ltp", row.get("close", 0))),
                    int(row.get("volume", 0) or 0),
                    _safe_float(row.get("value")),
                    int(row.get("trade_count", 0) or 0),
                ),
            )
        except Exception as e:
            logger.error(f"Upsert error for {row.get('symbol')}: {e}")
    conn.commit()
    conn.close()


def read_historical_for_symbol(symbol: str, min_rows: int = 120) -> pd.DataFrame:
    """Read the last N daily prices for a symbol from DB."""
    conn = get_connection()
    rows = conn.execute(
        """SELECT date, open, high, low, close, volume, value, trade_count
           FROM daily_prices WHERE symbol = ?
           ORDER BY date DESC LIMIT ?""",
        (symbol, min_rows),
    ).fetchall()
    conn.close()

    if not rows:
        return pd.DataFrame()

    df = pd.DataFrame([dict(r) for r in rows])
    df["date"] = pd.to_datetime(df["date"])
    for col in ["open", "high", "low", "close", "volume"]:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors="coerce")
    return df.sort_values("date").reset_index(drop=True)


def read_all_historical_grouped(min_rows_per_symbol: int = 20) -> dict:
    """Read all daily_prices grouped by symbol. Returns {symbol: DataFrame}."""
    conn = get_connection()
    rows = conn.execute(
        "SELECT symbol, date, open, high, low, close, volume FROM daily_prices ORDER BY symbol, date"
    ).fetchall()
    conn.close()

    if not rows:
        return {}

    df = pd.DataFrame([dict(r) for r in rows])
    df["date"] = pd.to_datetime(df["date"])
    for col in ["open", "high", "low", "close", "volume"]:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors="coerce")

    result = {}
    for sym, group in df.groupby("symbol"):
        if len(group) >= min_rows_per_symbol:
            result[sym] = group.sort_values("date").reset_index(drop=True)
    return result


# ======================== signals ========================


def save_signals_to_db(signals: list[dict]):
    """Replace all signals in DB with new batch."""
    conn = get_connection()
    conn.execute("DELETE FROM signals")

    # Ensure prediction_json column exists
    _ensure_prediction_column(conn)

    now = datetime.now().isoformat()
    for s in signals:
        try:
            ind = s.get("indicators", {})
            # Pack prediction fields into JSON (convert numpy types)
            prediction_data = _make_json_safe({
                "predicted_prices": s.get("predicted_prices", {}),
                "expected_return_pct": s.get("expected_return_pct", 0),
                "hold_days": s.get("hold_days", 0),
                "entry_strategy": s.get("entry_strategy", ""),
                "exit_strategy": s.get("exit_strategy", ""),
                "trend_strength": s.get("trend_strength", "SIDEWAYS"),
                "volatility_level": s.get("volatility_level", "MEDIUM"),
                "t2_safe": s.get("t2_safe", False),
                "price_range_next_3d": s.get("price_range_next_3d", {}),
                "risk_score": s.get("risk_score", 50),
                "t2_maturity_date": s.get("t2_maturity_date", ""),
            })
            conn.execute(
                """INSERT INTO signals
                   (symbol, company_name, ltp, change_pct, signal_type, confidence,
                    short_term_score, long_term_score, rsi, macd_signal, bb_position,
                    ema_crossover, volume_signal, support_level, resistance_level,
                    target_price, stop_loss, risk_reward_ratio, reasoning, timing,
                    prediction_json, created_at)
                   VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)""",
                (
                    s.get("symbol"),
                    s.get("company_name"),
                    s.get("ltp", 0),
                    s.get("change_pct", 0),
                    s.get("signal_type", "HOLD"),
                    s.get("confidence", 0),
                    s.get("short_term_score", 0),
                    s.get("long_term_score", 0),
                    ind.get("rsi"),
                    ind.get("macd_signal"),
                    ind.get("bb_position"),
                    ind.get("ema_crossover"),
                    ind.get("volume_signal"),
                    s.get("support_level", 0),
                    s.get("resistance_level", 0),
                    s.get("target_price", 0),
                    s.get("stop_loss", 0),
                    s.get("risk_reward_ratio", 0),
                    s.get("reasoning", ""),
                    s.get("timing", "HOLD_TIGHT"),
                    json.dumps(prediction_data),
                    now,
                ),
            )
        except Exception as e:
            logger.error(f"Save signal error for {s.get('symbol')}: {e}")
    conn.commit()
    conn.close()
    logger.info(f"Saved {len(signals)} signals to DB")


def load_signals_from_db() -> list[dict]:
    """Load latest signals from DB in the same dict format as cache."""
    conn = get_connection()

    # Check if prediction_json column exists
    has_prediction = _has_column(conn, "signals", "prediction_json")
    has_support = _has_column(conn, "signals", "support_level")

    cols = """symbol, company_name, ltp, change_pct, signal_type, confidence,
              short_term_score, long_term_score, rsi, macd_signal, bb_position,
              ema_crossover, volume_signal, target_price, stop_loss,
              risk_reward_ratio, reasoning, timing, created_at"""
    if has_support:
        cols += ", support_level, resistance_level"
    if has_prediction:
        cols += ", prediction_json"

    rows = conn.execute(
        f"SELECT {cols} FROM signals ORDER BY ABS(short_term_score) DESC"
    ).fetchall()
    conn.close()

    signals = []
    for r in rows:
        r = dict(r)
        signal = {
            "symbol": r["symbol"],
            "company_name": r["company_name"] or r["symbol"],
            "ltp": r["ltp"] or 0,
            "change_pct": r["change_pct"] or 0,
            "signal_type": r["signal_type"],
            "confidence": r["confidence"] or 0,
            "short_term_score": r["short_term_score"] or 0,
            "long_term_score": r["long_term_score"] or 0,
            "target_price": r["target_price"] or 0,
            "stop_loss": r["stop_loss"] or 0,
            "risk_reward_ratio": r["risk_reward_ratio"] or 0,
            "reasoning": r["reasoning"] or "",
            "timing": r["timing"] or "HOLD_TIGHT",
            "indicators": {
                "rsi": r["rsi"],
                "macd_signal": r["macd_signal"],
                "bb_position": r["bb_position"],
                "ema_crossover": r["ema_crossover"],
                "volume_signal": r["volume_signal"],
                "momentum_3d": None,
                "stoch_k": None,
            },
            "created_at": r["created_at"],
            "support_level": r.get("support_level", 0) or 0,
            "resistance_level": r.get("resistance_level", 0) or 0,
        }

        # Merge prediction data from JSON
        pred_json = r.get("prediction_json")
        if pred_json:
            try:
                pred = json.loads(pred_json)
                signal.update(pred)
            except (json.JSONDecodeError, TypeError):
                pass

        # Ensure prediction fields always exist
        signal.setdefault("predicted_prices", {})
        signal.setdefault("expected_return_pct", 0)
        signal.setdefault("hold_days", 0)
        signal.setdefault("entry_strategy", "")
        signal.setdefault("exit_strategy", "")
        signal.setdefault("trend_strength", "SIDEWAYS")
        signal.setdefault("volatility_level", "MEDIUM")
        signal.setdefault("t2_safe", False)
        signal.setdefault("price_range_next_3d", {})
        signal.setdefault("risk_score", 50)
        signal.setdefault("t2_maturity_date", "")

        signals.append(signal)
    return signals


# ======================== signal_history ========================


def save_signal_history(signals: list[dict]):
    """Save daily signal snapshot to history (append-only, one per symbol per day)."""
    conn = get_connection()
    today = datetime.now().strftime("%Y-%m-%d")
    saved = 0
    for s in signals:
        try:
            pp = s.get("predicted_prices", {})
            conn.execute(
                """INSERT OR REPLACE INTO signal_history
                   (symbol, date, signal_type, ltp, target_price, stop_loss,
                    confidence, short_term_score, predicted_day2, predicted_day3,
                    predicted_day5, predicted_day7, expected_return_pct, reasoning)
                   VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)""",
                (
                    s.get("symbol"),
                    today,
                    s.get("signal_type", "HOLD"),
                    s.get("ltp", 0),
                    s.get("target_price", 0),
                    s.get("stop_loss", 0),
                    s.get("confidence", 0),
                    s.get("short_term_score", 0),
                    pp.get("day_2"),
                    pp.get("day_3"),
                    pp.get("day_5"),
                    pp.get("day_7"),
                    s.get("expected_return_pct", 0),
                    s.get("reasoning", ""),
                ),
            )
            saved += 1
        except Exception as e:
            logger.error(f"Signal history save error for {s.get('symbol')}: {e}")
    conn.commit()
    conn.close()
    logger.info(f"Saved {saved} signal history entries for {today}")


def backfill_signal_accuracy():
    """Compare past predictions with actual prices and update accuracy columns."""
    conn = get_connection()
    # Get history entries that haven't been verified yet (actual_day2 is NULL)
    rows = conn.execute(
        """SELECT id, symbol, date, ltp, target_price, stop_loss,
                  predicted_day2, predicted_day3, predicted_day5, predicted_day7
           FROM signal_history
           WHERE actual_day2 IS NULL AND date < date('now', '-2 days')
           ORDER BY date LIMIT 500"""
    ).fetchall()

    if not rows:
        conn.close()
        return 0

    updated = 0
    for r in rows:
        r = dict(r)
        symbol = r["symbol"]
        sig_date = r["date"]
        entry_price = r["ltp"] or 0

        # Get actual prices for day+2, day+3, day+5, day+7
        prices = conn.execute(
            """SELECT date, close FROM daily_prices
               WHERE symbol = ? AND date > ? ORDER BY date LIMIT 7""",
            (symbol, sig_date),
        ).fetchall()

        if len(prices) < 2:
            continue

        actual = {}
        for i, p in enumerate(prices):
            day_num = i + 1
            if day_num in (2, 3, 5, 7):
                actual[day_num] = p["close"]

        # Check if target or stop was hit within 7 days
        target = r["target_price"] or 0
        stop = r["stop_loss"] or 0
        target_hit = 0
        stop_hit = 0
        max_price = 0
        min_price = float("inf")

        for p in prices:
            close = p["close"] or 0
            if close > max_price:
                max_price = close
            if close < min_price:
                min_price = close
            if target > 0 and close >= target:
                target_hit = 1
            if stop > 0 and close <= stop:
                stop_hit = 1

        # Calculate actual return (day 7 vs entry)
        actual_return = 0
        last_actual = actual.get(7) or actual.get(5) or actual.get(3) or actual.get(2)
        if entry_price > 0 and last_actual:
            actual_return = (last_actual - entry_price) / entry_price * 100

        conn.execute(
            """UPDATE signal_history SET
                  actual_day2 = ?, actual_day3 = ?, actual_day5 = ?, actual_day7 = ?,
                  target_hit = ?, stop_hit = ?, actual_return_pct = ?
               WHERE id = ?""",
            (
                actual.get(2), actual.get(3), actual.get(5), actual.get(7),
                target_hit, stop_hit, round(actual_return, 2),
                r["id"],
            ),
        )
        updated += 1

    conn.commit()
    conn.close()
    logger.info(f"Backfilled accuracy for {updated} signal history entries")
    return updated


def get_signal_history_for_symbol(symbol: str, limit: int = 30) -> list[dict]:
    """Get signal history for a specific symbol."""
    conn = get_connection()
    rows = conn.execute(
        """SELECT * FROM signal_history
           WHERE symbol = ? ORDER BY date DESC LIMIT ?""",
        (symbol.upper(), limit),
    ).fetchall()
    conn.close()
    return [dict(r) for r in rows]


def get_signal_accuracy_report() -> dict:
    """Generate accuracy report from signal history."""
    conn = get_connection()

    # Overall stats
    total = conn.execute(
        "SELECT COUNT(*) FROM signal_history WHERE actual_day2 IS NOT NULL"
    ).fetchone()[0]

    if total == 0:
        conn.close()
        return {"total_verified": 0, "message": "No verified signals yet. Accuracy data will appear after 2+ trading days."}

    # By signal type
    by_type = conn.execute(
        """SELECT signal_type,
                  COUNT(*) as count,
                  AVG(actual_return_pct) as avg_return,
                  SUM(CASE WHEN target_hit = 1 THEN 1 ELSE 0 END) as targets_hit,
                  SUM(CASE WHEN stop_hit = 1 THEN 1 ELSE 0 END) as stops_hit,
                  SUM(CASE WHEN actual_return_pct > 0 THEN 1 ELSE 0 END) as profitable
           FROM signal_history
           WHERE actual_day2 IS NOT NULL
           GROUP BY signal_type"""
    ).fetchall()

    # Overall profitable
    profitable = conn.execute(
        """SELECT COUNT(*) FROM signal_history
           WHERE actual_day2 IS NOT NULL
             AND ((signal_type IN ('BUY', 'STRONG_BUY') AND actual_return_pct > 0)
               OR (signal_type IN ('SELL', 'STRONG_SELL') AND actual_return_pct < 0))"""
    ).fetchone()[0]

    # Best and worst calls
    best = conn.execute(
        """SELECT symbol, date, signal_type, ltp, actual_return_pct, target_hit
           FROM signal_history WHERE actual_day2 IS NOT NULL
           ORDER BY actual_return_pct DESC LIMIT 5"""
    ).fetchall()

    worst = conn.execute(
        """SELECT symbol, date, signal_type, ltp, actual_return_pct, stop_hit
           FROM signal_history WHERE actual_day2 IS NOT NULL
           ORDER BY actual_return_pct ASC LIMIT 5"""
    ).fetchall()

    # Recent accuracy (last 7 days that have verification)
    recent = conn.execute(
        """SELECT date,
                  COUNT(*) as signals,
                  AVG(actual_return_pct) as avg_return,
                  SUM(CASE WHEN target_hit = 1 THEN 1 ELSE 0 END) as targets_hit
           FROM signal_history
           WHERE actual_day2 IS NOT NULL
           GROUP BY date ORDER BY date DESC LIMIT 7"""
    ).fetchall()

    conn.close()

    return {
        "total_verified": total,
        "correct_direction": profitable,
        "accuracy_pct": round(profitable / total * 100, 1) if total > 0 else 0,
        "by_signal_type": [dict(r) for r in by_type],
        "best_calls": [dict(r) for r in best],
        "worst_calls": [dict(r) for r in worst],
        "recent_daily": [dict(r) for r in recent],
    }


# ======================== holdings ========================


def insert_holding(symbol: str, quantity: int, buy_price: float,
                   buy_date: str, maturity_date: str, notes: str = None) -> int:
    """Insert a new holding and return its ID."""
    conn = get_connection()
    cursor = conn.execute(
        """INSERT INTO holdings (symbol, quantity, buy_price, buy_date, maturity_date, notes)
           VALUES (?, ?, ?, ?, ?, ?)""",
        (symbol.upper(), quantity, buy_price, buy_date, maturity_date, notes),
    )
    conn.commit()
    holding_id = cursor.lastrowid
    conn.close()
    return holding_id


def get_active_holdings() -> list[dict]:
    """Get all ACTIVE holdings."""
    conn = get_connection()
    rows = conn.execute(
        """SELECT id, symbol, quantity, buy_price, buy_date, maturity_date,
                  sell_price, sell_date, sell_quantity, status, notes, created_at
           FROM holdings WHERE status = 'ACTIVE' ORDER BY buy_date DESC"""
    ).fetchall()
    conn.close()
    return [dict(r) for r in rows]


def get_holding_by_id(holding_id: int) -> dict | None:
    """Get a single holding by ID."""
    conn = get_connection()
    row = conn.execute(
        "SELECT * FROM holdings WHERE id = ?", (holding_id,)
    ).fetchone()
    conn.close()
    return dict(row) if row else None


def update_holding_sell(holding_id: int, sell_price: float,
                        sell_date: str, quantity: int):
    """Record a sell on a holding (full or partial)."""
    conn = get_connection()
    holding = conn.execute(
        "SELECT quantity, sell_quantity FROM holdings WHERE id = ?", (holding_id,)
    ).fetchone()

    if not holding:
        conn.close()
        return

    total_qty = holding["quantity"]
    already_sold = holding["sell_quantity"] or 0
    new_sold = already_sold + quantity

    if new_sold >= total_qty:
        status = "SOLD"
    else:
        status = "PARTIAL"

    conn.execute(
        """UPDATE holdings SET sell_price = ?, sell_date = ?,
           sell_quantity = ?, status = ? WHERE id = ?""",
        (sell_price, sell_date, new_sold, status, holding_id),
    )
    conn.commit()
    conn.close()


def delete_holding(holding_id: int):
    """Delete a holding record."""
    conn = get_connection()
    conn.execute("DELETE FROM holdings WHERE id = ?", (holding_id,))
    conn.commit()
    conn.close()


# ======================== helpers ========================


def _safe_float(val) -> float:
    """Convert to float, returning 0.0 for NaN/None."""
    if val is None:
        return 0.0
    try:
        f = float(val)
        return 0.0 if math.isnan(f) or math.isinf(f) else f
    except (ValueError, TypeError):
        return 0.0


def _make_json_safe(obj):
    """Convert numpy types to native Python types for JSON serialization."""
    import numpy as np
    if isinstance(obj, dict):
        return {k: _make_json_safe(v) for k, v in obj.items()}
    elif isinstance(obj, (list, tuple)):
        return [_make_json_safe(v) for v in obj]
    elif isinstance(obj, (np.bool_,)):
        return bool(obj)
    elif isinstance(obj, (np.integer,)):
        return int(obj)
    elif isinstance(obj, (np.floating,)):
        return float(obj)
    return obj


def _ensure_prediction_column(conn):
    """Add prediction_json column to signals table if it doesn't exist."""
    if not _has_column(conn, "signals", "prediction_json"):
        try:
            conn.execute("ALTER TABLE signals ADD COLUMN prediction_json TEXT")
        except Exception:
            pass  # Column already exists or table issue


def _has_column(conn, table: str, column: str) -> bool:
    """Check if a column exists in a table."""
    try:
        info = conn.execute(f"PRAGMA table_info({table})").fetchall()
        return any(row["name"] == column for row in info)
    except Exception:
        return False
