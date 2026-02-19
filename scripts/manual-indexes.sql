-- =============================================================================
-- Manual Pre-Deployment Index Creation Script
-- =============================================================================
--
-- IMPORTANT: Run this script BEFORE deploying to production.
--
-- These indexes cannot be created via migrations because:
-- 1. golang-migrate runs migrations in transactions
-- 2. CREATE INDEX CONCURRENTLY cannot execute within a transaction
-- 3. PostgreSQL raises: "CREATE INDEX CONCURRENTLY cannot run inside a transaction block"
--
-- Benefits of CONCURRENTLY:
-- - Does not lock writes on the table during index creation
-- - Safe for production databases with live traffic
-- - May take longer but doesn't block operations
--
-- Failed concurrent builds can leave INVALID indexes (indisvalid = false).
-- If a CREATE INDEX CONCURRENTLY fails, drop the invalid index before retrying:
--   DROP INDEX CONCURRENTLY <index_name>;
--   CREATE INDEX CONCURRENTLY IF NOT EXISTS <index_name> ON <table>(...);
-- Verify validity via pg_index.indisvalid and retry with backoff or during low traffic windows.
--
-- Usage:
--   psql -h <host> -U <user> -d <database> -f scripts/manual-indexes.sql
--
-- For multi-tenant deployments, run this for each tenant schema.
-- Either set the session search_path before execution:
--   SET search_path TO tenant_schema;
-- Or schema-qualify table names (e.g., tenant_schema.exceptions).
-- Repeat per tenant schema.
--
-- =============================================================================

-- Exception reason index (from baseline 000001_release_0_1_0)
-- Enables efficient filtering by exception reason
CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_exceptions_reason 
    ON exceptions(reason);

-- Exception resolution type index (from baseline 000001_release_0_1_0)
-- Partial index for resolved exceptions only
CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_exceptions_resolution_type 
    ON exceptions(resolution_type) 
    WHERE resolution_type IS NOT NULL;

-- Outbox retry index (from baseline 000001_release_0_1_0)
-- Optimizes failed event retrieval for retry processing
CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_outbox_events_failed_retry 
    ON outbox_events(status, updated_at, attempts) 
    WHERE status = 'FAILED';

-- =============================================================================
-- Verification queries (run after index creation)
-- =============================================================================

-- Check that all indexes were created and are valid:
-- SELECT
--   cls.relname AS indexname,
--   pg_get_indexdef(cls.oid) AS indexdef,
--   idx.indisvalid AS is_valid
-- FROM pg_class cls
-- JOIN pg_index idx ON idx.indexrelid = cls.oid
-- JOIN pg_class tbl ON idx.indrelid = tbl.oid
-- WHERE tbl.relname IN ('exceptions', 'outbox_events')
--   AND cls.relname IN (
--     'idx_exceptions_reason',
--     'idx_exceptions_resolution_type',
--     'idx_outbox_events_failed_retry'
--   );
