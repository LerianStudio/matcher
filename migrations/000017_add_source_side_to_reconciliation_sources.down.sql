-- Rollback note:
-- This removes the side column and index introduced by migration 000017.
-- Side data is not reconstructed after the column is dropped.
DROP INDEX IF EXISTS idx_reconciliation_sources_context_side;

ALTER TABLE reconciliation_sources
    DROP COLUMN IF EXISTS side;
