-- Remove the unique constraint on reconciliation context name.
DROP INDEX IF EXISTS idx_reconciliation_contexts_name_unique;
