-- Rollback is intentionally blocked.
-- Migration 000022 removes legacy rate schema and references that cannot be
-- reconstructed safely. Recreating the old schema shape would falsely imply
-- rollback succeeded while historical relationships remain unrecoverable.

SELECT current_setting(
    'migration_000022_rollback_blocked_legacy_rate_schema_is_non_reversible'
);
