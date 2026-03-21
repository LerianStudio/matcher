-- Rollback note:
-- This removes all explicit source side assignments introduced by migration 000017.
-- Side data is not reconstructed after the column is dropped.
DROP INDEX IF EXISTS idx_reconciliation_sources_context_side;

ALTER TABLE reconciliation_sources
    DROP CONSTRAINT IF EXISTS chk_reconciliation_sources_side,
    DROP COLUMN IF EXISTS side;
