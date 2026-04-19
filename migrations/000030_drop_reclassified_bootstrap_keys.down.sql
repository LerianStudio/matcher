-- Rollback: intentionally irreversible.
--
-- The up migration DELETEs orphan systemplane rows for keys that v5 no
-- longer honours. The original values are not preserved anywhere the
-- rollback could restore them from, and restoring stale bootstrap-only
-- values would re-introduce the very drift the up migration cleans up.
--
-- If an operator needs the old admin-API values, recover them from
-- system.runtime_history (pre-v5) or from application audit logs. The
-- rollback here is a no-op rather than a DROP so that chained down
-- migrations remain safe to run.

SELECT 1;
