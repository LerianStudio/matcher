-- Phase 2 of source-side cutover (enforcement):
-- After operators have backfilled side assignments (or reset the environment),
-- this migration enforces NOT NULL + CHECK constraint.
--
-- Blocks if any source still has a NULL side. Operators must either:
-- 1. Reset the environment, or
-- 2. UPDATE reconciliation_sources SET side = 'LEFT' WHERE ... (per context topology)
SELECT CASE
    WHEN EXISTS (
        SELECT 1
        FROM reconciliation_sources
        WHERE side IS NULL
    ) THEN current_setting(
        'migration_000018_blocked_backfill_source_side_assignments_before_enforcement'
    )
    ELSE 'ok'
END;

ALTER TABLE reconciliation_sources
    ALTER COLUMN side SET NOT NULL,
    ADD CONSTRAINT chk_reconciliation_sources_side
        CHECK (side IN ('LEFT', 'RIGHT'));
