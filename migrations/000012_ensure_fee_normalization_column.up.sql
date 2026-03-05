-- Ensure legacy environments also have context fee normalization column.
--
-- Why this exists:
-- Some long-lived databases may have run version 000002 before fee_normalization
-- was introduced in that migration file. Since migrate tracks only version number,
-- those environments do not re-run 000002 and can miss this column.
--
-- This migration is idempotent and safe for fresh environments.
ALTER TABLE reconciliation_contexts
    ADD COLUMN IF NOT EXISTS fee_normalization TEXT;
