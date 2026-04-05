-- Migration 003: Drop unused tables
-- Run: sudo -u postgres psql -d dse_trading -f 003_drop_unused_tables.sql
-- IMPORTANT: Run AFTER backup!

BEGIN;

-- Tables confirmed unused (0 rows or no code references)
DROP TABLE IF EXISTS intraday_alerts CASCADE;
DROP TABLE IF EXISTS signal_patterns CASCADE;
DROP TABLE IF EXISTS llm_scan_results CASCADE;

-- Tables replaced by V2 system
DROP TABLE IF EXISTS orderbook_snapshots CASCADE;    -- 84 MB, B/A now in live_prices
DROP TABLE IF EXISTS intraday_snapshots CASCADE;     -- 36 MB, old live scanner
DROP TABLE IF EXISTS scan_decisions CASCADE;         -- old depth scanner tracking
DROP TABLE IF EXISTS radar_precomputed CASCADE;      -- replaced by ai_analysis
DROP TABLE IF EXISTS radar_snapshots CASCADE;        -- replaced by ai_analysis history

-- Old signal tables (keeping signals + signal_history for now until backend code updated)
-- DROP TABLE IF EXISTS signals CASCADE;             -- KEEP: actively used by repository.py
-- DROP TABLE IF EXISTS signal_history CASCADE;      -- KEEP: actively used by repository.py

-- Chat (separate concern, can be dropped if not needed)
DROP TABLE IF EXISTS chat_users CASCADE;

COMMIT;

-- Verify what remains
SELECT table_name, pg_size_pretty(pg_total_relation_size(quote_ident(table_name))) as size
FROM information_schema.tables
WHERE table_schema = 'public' AND table_type = 'BASE TABLE'
ORDER BY pg_total_relation_size(quote_ident(table_name)) DESC;
