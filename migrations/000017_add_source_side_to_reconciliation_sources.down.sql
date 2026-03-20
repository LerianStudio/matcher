DROP INDEX IF EXISTS idx_reconciliation_sources_context_side;

ALTER TABLE reconciliation_sources
    DROP CONSTRAINT IF EXISTS chk_reconciliation_sources_side,
    DROP COLUMN IF EXISTS side;
