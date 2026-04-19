-- DROP INDEX CONCURRENTLY is rejected on indexes belonging to partitioned
-- parents (same SQLSTATE 0A000 as CREATE INDEX CONCURRENTLY). Use plain
-- DROP INDEX IF EXISTS, mirroring the up.sql, so the rollback is safe to
-- run repeatedly.
DROP INDEX IF EXISTS idx_audit_logs_tenant_entity_created;
