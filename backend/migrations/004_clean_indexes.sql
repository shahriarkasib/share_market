-- Migration 004: Clean duplicate indexes
-- Run: sudo -u postgres psql -d dse_trading -f 004_clean_indexes.sql

BEGIN;

-- daily_prices has 4 indexes doing the same thing:
-- dp_sym_date (UNIQUE on symbol,date) — KEEP (PK equivalent)
-- idx_daily_symbol_date (on symbol,date) — DROP (duplicate of dp_sym_date)
-- idx_dp_date (on date) — KEEP (useful for date-range queries)
-- idx_dp_symbol (on symbol) — DROP (covered by dp_sym_date)
DROP INDEX IF EXISTS idx_daily_symbol_date;
DROP INDEX IF EXISTS idx_dp_symbol;

-- daily_analysis has duplicate date indexes:
-- idx_da_date and idx_daily_analysis_date — keep one
DROP INDEX IF EXISTS idx_daily_analysis_date;

-- radar_snapshots had duplicates (table may be dropped already)
DROP INDEX IF EXISTS idx_radar_snapshots_date;
DROP INDEX IF EXISTS idx_radar_snapshots_symbol;

COMMIT;

-- Verify remaining indexes
SELECT tablename, indexname
FROM pg_indexes
WHERE schemaname = 'public' AND tablename IN ('daily_prices', 'daily_analysis')
ORDER BY tablename, indexname;
