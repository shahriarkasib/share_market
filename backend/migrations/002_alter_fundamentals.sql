-- Migration 002: Add real financial data columns to fundamentals
-- Run: sudo -u postgres psql -d dse_trading -f 002_alter_fundamentals.sql

BEGIN;

-- Add financial columns (all currently NULL — will be populated by scraper)
ALTER TABLE fundamentals ADD COLUMN IF NOT EXISTS company_name TEXT;
ALTER TABLE fundamentals ADD COLUMN IF NOT EXISTS eps_ttm DOUBLE PRECISION;
ALTER TABLE fundamentals ADD COLUMN IF NOT EXISTS pe_ratio DOUBLE PRECISION;
ALTER TABLE fundamentals ADD COLUMN IF NOT EXISTS nav_per_share DOUBLE PRECISION;
ALTER TABLE fundamentals ADD COLUMN IF NOT EXISTS dividend_per_share DOUBLE PRECISION;
ALTER TABLE fundamentals ADD COLUMN IF NOT EXISTS dividend_yield_pct DOUBLE PRECISION;
ALTER TABLE fundamentals ADD COLUMN IF NOT EXISTS last_dividend_date DATE;
ALTER TABLE fundamentals ADD COLUMN IF NOT EXISTS debt_equity DOUBLE PRECISION;
ALTER TABLE fundamentals ADD COLUMN IF NOT EXISTS current_ratio DOUBLE PRECISION;
ALTER TABLE fundamentals ADD COLUMN IF NOT EXISTS roe_pct DOUBLE PRECISION;
ALTER TABLE fundamentals ADD COLUMN IF NOT EXISTS revenue_ttm BIGINT;
ALTER TABLE fundamentals ADD COLUMN IF NOT EXISTS revenue_growth_pct DOUBLE PRECISION;
ALTER TABLE fundamentals ADD COLUMN IF NOT EXISTS net_profit_ttm BIGINT;
ALTER TABLE fundamentals ADD COLUMN IF NOT EXISTS net_margin_pct DOUBLE PRECISION;
ALTER TABLE fundamentals ADD COLUMN IF NOT EXISTS free_cash_flow BIGINT;
ALTER TABLE fundamentals ADD COLUMN IF NOT EXISTS market_cap BIGINT;
ALTER TABLE fundamentals ADD COLUMN IF NOT EXISTS shares_outstanding BIGINT;
ALTER TABLE fundamentals ADD COLUMN IF NOT EXISTS high_52w DOUBLE PRECISION;
ALTER TABLE fundamentals ADD COLUMN IF NOT EXISTS low_52w DOUBLE PRECISION;

-- Populate EPS and NAV from corporate_events (parse from text)
-- This is a best-effort extraction — will need manual cleanup
UPDATE fundamentals f SET
    eps_ttm = sub.eps,
    nav_per_share = sub.nav
FROM (
    SELECT DISTINCT ON (symbol)
        symbol,
        CASE
            WHEN title ~ 'EPS was Tk\. ([0-9.-]+)'
            THEN (regexp_match(title, 'EPS was Tk\. ([0-9.-]+)'))[1]::DOUBLE PRECISION * 2
            ELSE NULL
        END as eps,
        CASE
            WHEN title ~ 'NAV per share was Tk\. ([0-9.]+)'
            THEN (regexp_match(title, 'NAV per share was Tk\. ([0-9.]+)'))[1]::DOUBLE PRECISION
            ELSE NULL
        END as nav
    FROM corporate_events
    WHERE title LIKE '%EPS%' AND date >= '2026-01-01'
    ORDER BY symbol, date DESC
) sub
WHERE f.symbol = sub.symbol AND sub.eps IS NOT NULL;

-- Populate dividend from corporate_events
UPDATE fundamentals f SET
    dividend_per_share = sub.div_pct * 0.10,  -- face value 10 BDT
    dividend_yield_pct = CASE WHEN lp.ltp > 0 THEN (sub.div_pct * 0.10 / lp.ltp * 100) ELSE NULL END,
    last_dividend_date = sub.div_date
FROM (
    SELECT DISTINCT ON (symbol)
        symbol,
        CASE
            WHEN details ~ 'Cash (\d+)%'
            THEN (regexp_match(details, 'Cash (\d+)%'))[1]::DOUBLE PRECISION
            ELSE NULL
        END as div_pct,
        date as div_date
    FROM corporate_events
    WHERE (details LIKE '%Cash%' AND details LIKE '%Dividend%')
    ORDER BY symbol, date DESC
) sub
JOIN live_prices lp ON f.symbol = lp.symbol
WHERE f.symbol = sub.symbol AND sub.div_pct IS NOT NULL;

-- Populate 52-week high/low from daily_prices
UPDATE fundamentals f SET
    high_52w = sub.h,
    low_52w = sub.l
FROM (
    SELECT symbol, MAX(high) as h, MIN(low) as l
    FROM daily_prices
    WHERE date >= CURRENT_DATE - INTERVAL '365 days' AND high > 0 AND low > 0
    GROUP BY symbol
) sub
WHERE f.symbol = sub.symbol;

-- Populate market cap estimate
UPDATE fundamentals f SET
    pe_ratio = CASE WHEN eps_ttm > 0 THEN ROUND((lp.ltp / eps_ttm)::numeric, 1) ELSE NULL END
FROM live_prices lp
WHERE f.symbol = lp.symbol AND f.eps_ttm IS NOT NULL AND f.eps_ttm > 0;

COMMIT;

-- Verify
SELECT symbol, eps_ttm, pe_ratio, nav_per_share, dividend_yield_pct, high_52w, low_52w
FROM fundamentals
WHERE eps_ttm IS NOT NULL
ORDER BY pe_ratio ASC NULLS LAST
LIMIT 10;
