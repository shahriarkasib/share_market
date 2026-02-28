"""PostgreSQL database setup and connection management (Supabase).

Provides a compatibility wrapper so existing code using sqlite3-style
conn.execute(sql, (?,?,...)) works unchanged with psycopg2.
"""

import re
import logging
import psycopg2
import psycopg2.extras
import psycopg2.pool
from config import DATABASE_URL, DATABASE_URL_DIRECT

logger = logging.getLogger(__name__)

# Connection pool (lazy-initialized)
_pool: psycopg2.pool.ThreadedConnectionPool | None = None


def _get_pool() -> psycopg2.pool.ThreadedConnectionPool:
    """Get or create the connection pool."""
    global _pool
    if _pool is None or _pool.closed:
        _pool = psycopg2.pool.ThreadedConnectionPool(
            minconn=1,
            maxconn=10,
            dsn=DATABASE_URL,
            cursor_factory=psycopg2.extras.RealDictCursor,
        )
    return _pool


def _convert_placeholders(sql: str) -> str:
    """Convert sqlite3 '?' placeholders to psycopg2 '%s' placeholders.

    Skips '?' inside string literals (single quotes).
    """
    result = []
    in_string = False
    for char in sql:
        if char == "'" and not in_string:
            in_string = True
            result.append(char)
        elif char == "'" and in_string:
            in_string = False
            result.append(char)
        elif char == "?" and not in_string:
            result.append("%s")
        else:
            result.append(char)
    return "".join(result)


class DictRow(dict):
    """A dict that also supports integer indexing like sqlite3.Row.

    row["column_name"] and row[0] both work.  dict(row) also works.
    """

    def __init__(self, data):
        super().__init__(data)
        self._keys = list(data.keys())

    def __getitem__(self, key):
        if isinstance(key, int):
            return super().__getitem__(self._keys[key])
        return super().__getitem__(key)


def _wrap_row(row):
    """Wrap a RealDictRow into a DictRow supporting integer indexing."""
    if row is None:
        return None
    return DictRow(row)


class PgCursor:
    """Wrapper around psycopg2 cursor that supports dict(row) like sqlite3.Row."""

    def __init__(self, cursor):
        self._cursor = cursor

    def fetchall(self):
        rows = self._cursor.fetchall()
        return [DictRow(r) for r in rows]

    def fetchone(self):
        return _wrap_row(self._cursor.fetchone())

    def __iter__(self):
        return (DictRow(r) for r in self._cursor)

    @property
    def rowcount(self):
        return self._cursor.rowcount


class PgConnection:
    """Wrapper around psycopg2 connection mimicking sqlite3 Connection API.

    - Converts ? placeholders to %s
    - Returns PgCursor wrapping RealDictCursor
    - Manages pool return on close()
    """

    def __init__(self, conn):
        self._conn = conn

    def execute(self, sql: str, params=None) -> PgCursor:
        sql = _convert_placeholders(sql)
        cursor = self._conn.cursor()
        try:
            cursor.execute(sql, params)
        except Exception:
            self._conn.rollback()
            raise
        return PgCursor(cursor)

    def executescript(self, sql: str):
        """Execute multiple SQL statements separated by semicolons."""
        cursor = self._conn.cursor()
        # Split by semicolon, filter empty
        statements = [s.strip() for s in sql.split(";") if s.strip()]
        for stmt in statements:
            try:
                cursor.execute(stmt)
            except Exception as e:
                self._conn.rollback()
                logger.error(f"DDL error: {e} | statement: {stmt[:100]}")
                raise
        self._conn.commit()

    def commit(self):
        self._conn.commit()

    def rollback(self):
        self._conn.rollback()

    def close(self):
        try:
            _get_pool().putconn(self._conn)
        except Exception:
            pass


def get_connection() -> PgConnection:
    """Get a PostgreSQL connection from the pool."""
    pool = _get_pool()
    conn = pool.getconn()
    conn.autocommit = False
    return PgConnection(conn)


def init_database():
    """Create all tables if they don't exist (uses direct connection for DDL)."""
    conn = psycopg2.connect(DATABASE_URL_DIRECT)
    conn.autocommit = True
    cursor = conn.cursor()

    # Execute each CREATE TABLE separately (PostgreSQL DDL)
    statements = [
        """CREATE TABLE IF NOT EXISTS daily_prices (
            id SERIAL PRIMARY KEY,
            symbol TEXT NOT NULL,
            date DATE NOT NULL,
            open DOUBLE PRECISION,
            high DOUBLE PRECISION,
            low DOUBLE PRECISION,
            close DOUBLE PRECISION,
            volume BIGINT,
            value DOUBLE PRECISION,
            trade_count INTEGER,
            UNIQUE(symbol, date)
        )""",
        "CREATE INDEX IF NOT EXISTS idx_daily_symbol_date ON daily_prices(symbol, date)",

        """CREATE TABLE IF NOT EXISTS live_prices (
            symbol TEXT PRIMARY KEY,
            company_name TEXT,
            ltp DOUBLE PRECISION,
            high DOUBLE PRECISION,
            low DOUBLE PRECISION,
            open DOUBLE PRECISION,
            close_prev DOUBLE PRECISION,
            change DOUBLE PRECISION,
            change_pct DOUBLE PRECISION,
            volume BIGINT,
            value DOUBLE PRECISION,
            trade_count INTEGER,
            updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )""",

        """CREATE TABLE IF NOT EXISTS signals (
            id SERIAL PRIMARY KEY,
            symbol TEXT NOT NULL,
            company_name TEXT,
            ltp DOUBLE PRECISION,
            change_pct DOUBLE PRECISION,
            signal_type TEXT NOT NULL,
            confidence DOUBLE PRECISION,
            short_term_score DOUBLE PRECISION,
            long_term_score DOUBLE PRECISION,
            rsi DOUBLE PRECISION,
            macd_signal TEXT,
            bb_position TEXT,
            ema_crossover TEXT,
            volume_signal TEXT,
            support_level DOUBLE PRECISION,
            resistance_level DOUBLE PRECISION,
            pattern TEXT,
            target_price DOUBLE PRECISION,
            stop_loss DOUBLE PRECISION,
            risk_reward_ratio DOUBLE PRECISION,
            reasoning TEXT,
            timing TEXT,
            prediction_json TEXT,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )""",
        "CREATE INDEX IF NOT EXISTS idx_signals_symbol ON signals(symbol)",
        "CREATE INDEX IF NOT EXISTS idx_signals_type ON signals(signal_type)",

        """CREATE TABLE IF NOT EXISTS fundamentals (
            symbol TEXT PRIMARY KEY,
            company_name TEXT,
            sector TEXT,
            category TEXT,
            pe_ratio DOUBLE PRECISION,
            eps DOUBLE PRECISION,
            book_value DOUBLE PRECISION,
            market_cap DOUBLE PRECISION,
            dividend_yield DOUBLE PRECISION,
            year_high DOUBLE PRECISION,
            year_low DOUBLE PRECISION,
            updated_at TIMESTAMP
        )""",

        """CREATE TABLE IF NOT EXISTS holdings (
            id SERIAL PRIMARY KEY,
            symbol TEXT NOT NULL,
            quantity INTEGER NOT NULL,
            buy_price DOUBLE PRECISION NOT NULL,
            buy_date DATE NOT NULL,
            maturity_date DATE NOT NULL,
            sell_price DOUBLE PRECISION,
            sell_date DATE,
            sell_quantity INTEGER DEFAULT 0,
            status TEXT DEFAULT 'ACTIVE',
            notes TEXT,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )""",
        "CREATE INDEX IF NOT EXISTS idx_holdings_symbol ON holdings(symbol)",
        "CREATE INDEX IF NOT EXISTS idx_holdings_status ON holdings(status)",

        """CREATE TABLE IF NOT EXISTS watchlist (
            id SERIAL PRIMARY KEY,
            symbol TEXT NOT NULL UNIQUE,
            added_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            notes TEXT
        )""",

        """CREATE TABLE IF NOT EXISTS market_summary (
            id INTEGER PRIMARY KEY CHECK (id = 1),
            dsex_index DOUBLE PRECISION,
            dsex_change DOUBLE PRECISION,
            dsex_change_pct DOUBLE PRECISION,
            total_volume BIGINT,
            total_value DOUBLE PRECISION,
            total_trade INTEGER,
            advances INTEGER,
            declines INTEGER,
            unchanged INTEGER,
            market_status TEXT,
            updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )""",

        """CREATE TABLE IF NOT EXISTS dsex_history (
            date DATE PRIMARY KEY,
            dsex_index DOUBLE PRECISION,
            dses_index DOUBLE PRECISION,
            ds30_index DOUBLE PRECISION,
            total_volume BIGINT,
            total_value DOUBLE PRECISION,
            total_trade INTEGER
        )""",

        """CREATE TABLE IF NOT EXISTS signal_history (
            id SERIAL PRIMARY KEY,
            symbol TEXT NOT NULL,
            date DATE NOT NULL,
            signal_type TEXT NOT NULL,
            ltp DOUBLE PRECISION,
            target_price DOUBLE PRECISION,
            stop_loss DOUBLE PRECISION,
            confidence DOUBLE PRECISION,
            short_term_score DOUBLE PRECISION,
            predicted_day2 DOUBLE PRECISION,
            predicted_day3 DOUBLE PRECISION,
            predicted_day5 DOUBLE PRECISION,
            predicted_day7 DOUBLE PRECISION,
            expected_return_pct DOUBLE PRECISION,
            reasoning TEXT,
            actual_day2 DOUBLE PRECISION,
            actual_day3 DOUBLE PRECISION,
            actual_day5 DOUBLE PRECISION,
            actual_day7 DOUBLE PRECISION,
            target_hit INTEGER DEFAULT 0,
            stop_hit INTEGER DEFAULT 0,
            actual_return_pct DOUBLE PRECISION,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            UNIQUE(symbol, date)
        )""",
        "CREATE INDEX IF NOT EXISTS idx_signal_history_symbol ON signal_history(symbol)",
        "CREATE INDEX IF NOT EXISTS idx_signal_history_date ON signal_history(date)",

        """CREATE TABLE IF NOT EXISTS sectors (
            id SERIAL PRIMARY KEY,
            name TEXT NOT NULL UNIQUE,
            stock_count INTEGER DEFAULT 0,
            updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )""",

        """CREATE TABLE IF NOT EXISTS intraday_snapshots (
            id SERIAL PRIMARY KEY,
            symbol TEXT NOT NULL,
            ts TIMESTAMP NOT NULL,
            ltp DOUBLE PRECISION,
            open DOUBLE PRECISION,
            high DOUBLE PRECISION,
            low DOUBLE PRECISION,
            volume BIGINT,
            value DOUBLE PRECISION,
            trade_count INTEGER,
            UNIQUE(symbol, ts)
        )""",
        "CREATE INDEX IF NOT EXISTS idx_intraday_sym_ts ON intraday_snapshots(symbol, ts)",

        """CREATE TABLE IF NOT EXISTS daily_analysis (
            id SERIAL PRIMARY KEY,
            date DATE NOT NULL,
            symbol TEXT NOT NULL,
            action TEXT NOT NULL,
            reasoning TEXT,
            entry_low DOUBLE PRECISION,
            entry_high DOUBLE PRECISION,
            sl DOUBLE PRECISION,
            t1 DOUBLE PRECISION,
            t2 DOUBLE PRECISION,
            risk_pct DOUBLE PRECISION,
            reward_pct DOUBLE PRECISION,
            rsi DOUBLE PRECISION,
            stoch_rsi DOUBLE PRECISION,
            macd_line DOUBLE PRECISION,
            macd_signal DOUBLE PRECISION,
            macd_hist DOUBLE PRECISION,
            macd_status TEXT,
            bb_pct DOUBLE PRECISION,
            atr DOUBLE PRECISION,
            atr_pct DOUBLE PRECISION,
            volatility DOUBLE PRECISION,
            max_dd DOUBLE PRECISION,
            support DOUBLE PRECISION,
            resistance DOUBLE PRECISION,
            trend_50d DOUBLE PRECISION,
            avg_vol BIGINT,
            vol_ratio DOUBLE PRECISION,
            wait_days TEXT,
            vol_entry TEXT,
            entry_start DATE,
            entry_end DATE,
            exit_t1_by DATE,
            exit_t2_by DATE,
            hold_days_t1 INTEGER,
            hold_days_t2 INTEGER,
            scenarios_json TEXT,
            last_5_json TEXT,
            ltp DOUBLE PRECISION,
            score DOUBLE PRECISION,
            category TEXT,
            created_at TIMESTAMP DEFAULT NOW(),
            UNIQUE(date, symbol)
        )""",
        "CREATE INDEX IF NOT EXISTS idx_daily_analysis_date ON daily_analysis(date)",

        """CREATE TABLE IF NOT EXISTS llm_scan_results (
            id SERIAL PRIMARY KEY,
            date DATE NOT NULL,
            scan_time TIMESTAMP NOT NULL,
            analysis_type TEXT NOT NULL,
            symbol TEXT,
            recommendation TEXT,
            confidence TEXT,
            reasoning TEXT,
            key_insights TEXT,
            risk_factors TEXT,
            raw_response TEXT,
            created_at TIMESTAMP DEFAULT NOW(),
            UNIQUE(date, scan_time, analysis_type, symbol)
        )""",
        "CREATE INDEX IF NOT EXISTS idx_llm_scan_date ON llm_scan_results(date)",

        """CREATE TABLE IF NOT EXISTS scan_decisions (
            id SERIAL PRIMARY KEY,
            date DATE NOT NULL,
            scan_time TIMESTAMP NOT NULL,
            symbol TEXT NOT NULL,
            recommendation TEXT NOT NULL,
            live_ltp DOUBLE PRECISION,
            entry_low DOUBLE PRECISION,
            entry_high DOUBLE PRECISION,
            sl DOUBLE PRECISION,
            t1 DOUBLE PRECISION,
            t2 DOUBLE PRECISION,
            status TEXT,
            buy_sell_ratio DOUBLE PRECISION,
            t2_risk TEXT,
            score DOUBLE PRECISION,
            rsi DOUBLE PRECISION,
            macd_status TEXT,
            reasoning TEXT,
            actual_t1 DOUBLE PRECISION,
            actual_t2 DOUBLE PRECISION,
            actual_t3 DOUBLE PRECISION,
            actual_t5 DOUBLE PRECISION,
            actual_t7 DOUBLE PRECISION,
            return_t2_pct DOUBLE PRECISION,
            outcome TEXT,
            sl_hit_day INTEGER,
            t1_hit_day INTEGER,
            t2_hit_day INTEGER,
            created_at TIMESTAMP DEFAULT NOW(),
            UNIQUE(date, scan_time, symbol)
        )""",
        "CREATE INDEX IF NOT EXISTS idx_scan_decisions_date ON scan_decisions(date)",
        "CREATE INDEX IF NOT EXISTS idx_scan_decisions_symbol ON scan_decisions(symbol)",
        "CREATE INDEX IF NOT EXISTS idx_scan_decisions_outcome ON scan_decisions(outcome)",
    ]

    for stmt in statements:
        try:
            cursor.execute(stmt)
        except Exception as e:
            logger.error(f"DDL error: {e}")

    cursor.close()
    conn.close()
    logger.info("PostgreSQL tables initialized")
