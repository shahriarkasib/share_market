-- Migration 001: Create V2 tables
-- Run: sudo -u postgres psql -d dse_trading -f 001_create_new_tables.sql

BEGIN;

-- ============================================================
-- 1. stock_indicators — computed technical indicators per timeframe
-- Replaces the indicator columns from daily_analysis
-- Computed by pandas_ta, pure math, no AI
-- ============================================================
CREATE TABLE IF NOT EXISTS stock_indicators (
    symbol TEXT NOT NULL,
    date DATE NOT NULL,
    timeframe TEXT NOT NULL DEFAULT 'daily',  -- 'daily' | 'weekly' | 'monthly'

    -- Price (for weekly/monthly aggregates)
    open DOUBLE PRECISION,
    high DOUBLE PRECISION,
    low DOUBLE PRECISION,
    close DOUBLE PRECISION,
    volume BIGINT,

    -- Moving Averages
    ema_9 DOUBLE PRECISION,
    ema_21 DOUBLE PRECISION,
    ema_50 DOUBLE PRECISION,
    sma_50 DOUBLE PRECISION,
    sma_200 DOUBLE PRECISION,
    ema_200 DOUBLE PRECISION,

    -- Oscillators
    rsi_14 DOUBLE PRECISION,
    stoch_rsi_k DOUBLE PRECISION,
    stoch_rsi_d DOUBLE PRECISION,
    williams_r_14 DOUBLE PRECISION,

    -- MACD
    macd_line DOUBLE PRECISION,
    macd_signal DOUBLE PRECISION,
    macd_hist DOUBLE PRECISION,

    -- Money Flow
    cmf_20 DOUBLE PRECISION,
    mfi_14 DOUBLE PRECISION,
    obv DOUBLE PRECISION,

    -- Trend
    adx_14 DOUBLE PRECISION,
    plus_di_14 DOUBLE PRECISION,
    minus_di_14 DOUBLE PRECISION,

    -- Bollinger Bands
    bb_upper DOUBLE PRECISION,
    bb_middle DOUBLE PRECISION,
    bb_lower DOUBLE PRECISION,
    bb_pct DOUBLE PRECISION,
    bb_width_pct DOUBLE PRECISION,

    -- Volatility
    atr_14 DOUBLE PRECISION,
    atr_pct DOUBLE PRECISION,

    -- Ichimoku
    ichi_tenkan DOUBLE PRECISION,
    ichi_kijun DOUBLE PRECISION,
    ichi_senkou_a DOUBLE PRECISION,
    ichi_senkou_b DOUBLE PRECISION,

    -- Volume derived
    avg_volume_20 BIGINT,
    vol_ratio DOUBLE PRECISION,
    up_down_vol_ratio DOUBLE PRECISION,

    -- Price derived
    chg_5d DOUBLE PRECISION,
    chg_10d DOUBLE PRECISION,
    chg_20d DOUBLE PRECISION,

    -- Slopes (for divergence detection)
    macd_hist_slope DOUBLE PRECISION,
    obv_slope_10d DOUBLE PRECISION,
    price_slope_10d DOUBLE PRECISION,
    cmf_slope_10d DOUBLE PRECISION,

    -- Swing analysis
    swing_low_20d DOUBLE PRECISION,
    pct_from_swing_low DOUBLE PRECISION,
    days_since_swing_low INTEGER,

    -- CMF streak tracking
    cmf_pos_streak INTEGER DEFAULT 0,
    cmf_neg_streak INTEGER DEFAULT 0,

    -- MA alignment flags
    ma_aligned BOOLEAN DEFAULT FALSE,
    golden_cross BOOLEAN DEFAULT FALSE,
    death_cross BOOLEAN DEFAULT FALSE,

    -- Support / Resistance
    support DOUBLE PRECISION,
    resistance DOUBLE PRECISION,

    computed_at TIMESTAMPTZ DEFAULT NOW(),

    PRIMARY KEY (symbol, date, timeframe)
);

CREATE INDEX IF NOT EXISTS idx_si_date_tf ON stock_indicators (date, timeframe);
CREATE INDEX IF NOT EXISTS idx_si_symbol_tf ON stock_indicators (symbol, timeframe);


-- ============================================================
-- 2. ai_analysis — Claude AI analysis results per stock per date
-- Replaces llm_daily_analysis + judge_daily_analysis + signal parts of daily_analysis
-- ============================================================
CREATE TABLE IF NOT EXISTS ai_analysis (
    symbol TEXT NOT NULL,
    date DATE NOT NULL,

    -- Extracted fields for fast queries (no JSON parsing needed)
    overall_signal TEXT NOT NULL,          -- BUY | HOLD | SELL | AVOID | WATCH
    signal_strength TEXT,                  -- STRONG | MEDIUM | WEAK
    confidence TEXT,                       -- HIGH | MEDIUM | LOW
    classification TEXT,                   -- ENTRY_ZONE | READY | APPROACHING | BUILDING | WATCHING
    position_type TEXT,                    -- STRONG_TREND | TREND | EMERGING | RANGE | CHOPPY
    score_overall INTEGER,                 -- 0-100
    score_money_flow INTEGER,
    score_momentum INTEGER,
    score_price_action INTEGER,
    score_volatility INTEGER,
    score_fundamentals INTEGER,
    score_news INTEGER,
    one_liner TEXT,                        -- quick summary for matrix/dashboard

    -- Action fields (extracted for quick access)
    entry_low DOUBLE PRECISION,
    entry_high DOUBLE PRECISION,
    stop_loss DOUBLE PRECISION,
    stop_loss_method TEXT,
    target_1 DOUBLE PRECISION,
    target_2 DOUBLE PRECISION,
    for_new_buyer TEXT,
    for_holder TEXT,

    -- Full structured analysis (complete JSON from Claude)
    analysis_json JSONB NOT NULL,

    -- Metadata
    model TEXT DEFAULT 'claude-opus',
    created_at TIMESTAMPTZ DEFAULT NOW(),

    PRIMARY KEY (symbol, date)
);

CREATE INDEX IF NOT EXISTS idx_ai_date ON ai_analysis (date);
CREATE INDEX IF NOT EXISTS idx_ai_signal ON ai_analysis (overall_signal, date);
CREATE INDEX IF NOT EXISTS idx_ai_score ON ai_analysis (score_overall DESC, date);
CREATE INDEX IF NOT EXISTS idx_ai_classification ON ai_analysis (classification, date);


-- ============================================================
-- 3. market_analysis — daily market-level analysis from Claude
-- Replaces market_summary for AI content
-- ============================================================
CREATE TABLE IF NOT EXISTS market_analysis (
    date DATE PRIMARY KEY,

    -- DSEX metrics
    dsex_close DOUBLE PRECISION,
    dsex_change_pct DOUBLE PRECISION,
    regime TEXT,                           -- TRENDING_UP | TRENDING_DOWN | RANGING | CHOPPY
    regime_multiplier DOUBLE PRECISION,

    -- Breadth
    advances INTEGER,
    declines INTEGER,
    unchanged INTEGER,
    turnover_cr DOUBLE PRECISION,

    -- AI output
    is_good_day_to_buy BOOLEAN,
    ai_summary TEXT,                      -- 2-3 sentences

    -- Full analysis
    analysis_json JSONB,

    -- Global context
    sp500_change_pct DOUBLE PRECISION,
    oil_price DOUBLE PRECISION,
    usd_bdt DOUBLE PRECISION,

    created_at TIMESTAMPTZ DEFAULT NOW()
);

COMMIT;

-- Verify
SELECT 'stock_indicators' as tbl, COUNT(*) FROM stock_indicators
UNION ALL SELECT 'ai_analysis', COUNT(*) FROM ai_analysis
UNION ALL SELECT 'market_analysis', COUNT(*) FROM market_analysis;
