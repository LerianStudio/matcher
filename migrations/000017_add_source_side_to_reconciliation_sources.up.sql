-- Pre-launch hard cutover:
-- Matching now requires every source to declare an explicit LEFT/RIGHT side.
-- This migration intentionally refuses to guess sides for existing rows.
-- Operators must reset internal data or explicitly backfill side assignments
-- before this migration can be applied.
SELECT CASE
    WHEN EXISTS (SELECT 1 FROM reconciliation_sources) THEN current_setting(
        'migration_000017_blocked_reset_or_backfill_source_side_assignments_before_cutover'
    )
    ELSE 'ok'
END;

ALTER TABLE reconciliation_sources
    ADD COLUMN side TEXT NOT NULL,
    ADD CONSTRAINT chk_reconciliation_sources_side
        CHECK (side IN ('LEFT', 'RIGHT'));

CREATE INDEX idx_reconciliation_sources_context_side
    ON reconciliation_sources (context_id, side);
