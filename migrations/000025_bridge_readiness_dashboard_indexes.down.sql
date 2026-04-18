-- Rollback the bridge readiness dashboard partial indexes.
DROP INDEX CONCURRENTLY IF EXISTS idx_extraction_requests_bridge_unlinked_by_created;
DROP INDEX CONCURRENTLY IF EXISTS idx_extraction_requests_bridge_failed;
DROP INDEX CONCURRENTLY IF EXISTS idx_extraction_requests_bridge_ready;
