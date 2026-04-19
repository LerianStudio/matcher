-- Adds a composite index on audit_logs that covers the dominant query shape
-- issued by /audit-logs endpoints and governance archival lookups:
--   WHERE tenant_id = ? AND entity_type = ? AND entity_id = ?
--   ORDER BY created_at DESC, id DESC
--
-- Prior indexes in 000001_init_schema:
--   idx_audit_logs_tenant   (tenant_id)
--   idx_audit_logs_entity   (entity_type, entity_id)
--   idx_audit_logs_created  (created_at)
-- do not cover tenant_id AND the ordering columns together, forcing a heap
-- scan + sort on large partitions. The new composite covers the predicate
-- and ordering in a single index walk.
--
-- CONCURRENTLY avoids blocking ingest writes on the hot audit_logs table.
-- IF NOT EXISTS makes the migration safe to retry; paired with the symmetric
-- DROP INDEX CONCURRENTLY IF EXISTS in the .down.sql.
CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_audit_logs_tenant_entity_created
  ON audit_logs (tenant_id, entity_type, entity_id, created_at DESC, id DESC);
