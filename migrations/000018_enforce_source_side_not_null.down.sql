-- Rollback note:
-- Removes the NOT NULL constraint and CHECK constraint on side.
-- The column itself remains (from 000017) but becomes nullable again.
ALTER TABLE reconciliation_sources
    DROP CONSTRAINT IF EXISTS chk_reconciliation_sources_side,
    ALTER COLUMN side DROP NOT NULL;
