"""SQLite database setup and connection management."""

import sqlite3
import os
from config import DATABASE_PATH

DB_PATH = os.path.join(os.path.dirname(__file__), DATABASE_PATH)


def get_connection() -> sqlite3.Connection:
    """Get a SQLite connection with row factory."""
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA journal_mode=WAL")
    conn.execute("PRAGMA foreign_keys=ON")
    return conn


def init_database():
    """Create all tables if they don't exist."""
    conn = get_connection()
    cursor = conn.cursor()

    cursor.executescript("""
        -- Daily OHLCV data for technical analysis
        CREATE TABLE IF NOT EXISTS daily_prices (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            symbol TEXT NOT NULL,
            date DATE NOT NULL,
            open REAL,
            high REAL,
            low REAL,
            close REAL,
            volume INTEGER,
            value REAL,
            trade_count INTEGER,
            UNIQUE(symbol, date)
        );
        CREATE INDEX IF NOT EXISTS idx_daily_symbol_date ON daily_prices(symbol, date);

        -- Latest intraday snapshot (overwritten every 5 minutes)
        CREATE TABLE IF NOT EXISTS live_prices (
            symbol TEXT PRIMARY KEY,
            company_name TEXT,
            ltp REAL,
            high REAL,
            low REAL,
            open REAL,
            close_prev REAL,
            change REAL,
            change_pct REAL,
            volume INTEGER,
            value REAL,
            trade_count INTEGER,
            updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        );

        -- Computed trading signals
        CREATE TABLE IF NOT EXISTS signals (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            symbol TEXT NOT NULL,
            company_name TEXT,
            ltp REAL,
            change_pct REAL,
            signal_type TEXT NOT NULL,
            confidence REAL,
            short_term_score REAL,
            long_term_score REAL,
            rsi REAL,
            macd_signal TEXT,
            bb_position TEXT,
            ema_crossover TEXT,
            volume_signal TEXT,
            support_level REAL,
            resistance_level REAL,
            pattern TEXT,
            target_price REAL,
            stop_loss REAL,
            risk_reward_ratio REAL,
            reasoning TEXT,
            timing TEXT,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            UNIQUE(symbol, created_at)
        );
        CREATE INDEX IF NOT EXISTS idx_signals_symbol ON signals(symbol);
        CREATE INDEX IF NOT EXISTS idx_signals_type ON signals(signal_type);

        -- Company fundamentals
        CREATE TABLE IF NOT EXISTS fundamentals (
            symbol TEXT PRIMARY KEY,
            company_name TEXT,
            sector TEXT,
            pe_ratio REAL,
            eps REAL,
            book_value REAL,
            market_cap REAL,
            dividend_yield REAL,
            year_high REAL,
            year_low REAL,
            updated_at TIMESTAMP
        );

        -- Portfolio holdings
        CREATE TABLE IF NOT EXISTS holdings (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            symbol TEXT NOT NULL,
            quantity INTEGER NOT NULL,
            buy_price REAL NOT NULL,
            buy_date DATE NOT NULL,
            maturity_date DATE NOT NULL,
            sell_price REAL,
            sell_date DATE,
            sell_quantity INTEGER DEFAULT 0,
            status TEXT DEFAULT 'ACTIVE',
            notes TEXT,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        );
        CREATE INDEX IF NOT EXISTS idx_holdings_symbol ON holdings(symbol);
        CREATE INDEX IF NOT EXISTS idx_holdings_status ON holdings(status);

        -- User watchlist
        CREATE TABLE IF NOT EXISTS watchlist (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            symbol TEXT NOT NULL UNIQUE,
            added_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            notes TEXT
        );

        -- Market summary cache
        CREATE TABLE IF NOT EXISTS market_summary (
            id INTEGER PRIMARY KEY CHECK (id = 1),
            dsex_index REAL,
            dsex_change REAL,
            dsex_change_pct REAL,
            total_volume INTEGER,
            total_value REAL,
            total_trade INTEGER,
            advances INTEGER,
            declines INTEGER,
            unchanged INTEGER,
            market_status TEXT,
            updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        );

        -- DSEX index daily history
        CREATE TABLE IF NOT EXISTS dsex_history (
            date DATE PRIMARY KEY,
            dsex_index REAL,
            dses_index REAL,
            ds30_index REAL,
            total_volume INTEGER,
            total_value REAL,
            total_trade INTEGER
        );

        -- Signal history — append-only daily snapshots for accuracy tracking
        CREATE TABLE IF NOT EXISTS signal_history (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            symbol TEXT NOT NULL,
            date DATE NOT NULL,
            signal_type TEXT NOT NULL,
            ltp REAL,
            target_price REAL,
            stop_loss REAL,
            confidence REAL,
            short_term_score REAL,
            predicted_day2 REAL,
            predicted_day3 REAL,
            predicted_day5 REAL,
            predicted_day7 REAL,
            expected_return_pct REAL,
            reasoning TEXT,
            -- Filled in later when we verify accuracy
            actual_day2 REAL,
            actual_day3 REAL,
            actual_day5 REAL,
            actual_day7 REAL,
            target_hit INTEGER DEFAULT 0,
            stop_hit INTEGER DEFAULT 0,
            actual_return_pct REAL,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            UNIQUE(symbol, date)
        );
        CREATE INDEX IF NOT EXISTS idx_signal_history_symbol ON signal_history(symbol);
        CREATE INDEX IF NOT EXISTS idx_signal_history_date ON signal_history(date);

        -- Sector reference table
        CREATE TABLE IF NOT EXISTS sectors (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            name TEXT NOT NULL UNIQUE,
            stock_count INTEGER DEFAULT 0,
            updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        );
    """)

    conn.commit()
    conn.close()
