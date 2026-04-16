-- Reverse the partial indexes added by 000024.up.sql. Both DROP statements
-- use IF EXISTS + CONCURRENTLY so the rollback is safe to run repeatedly
-- and does not block writers on the affected tables.

DROP INDEX CONCURRENTLY IF EXISTS idx_reconciliation_sources_fetcher_conn;

DROP INDEX CONCURRENTLY IF EXISTS idx_extraction_requests_eligible_for_bridge;
