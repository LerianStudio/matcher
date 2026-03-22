-- Pre-launch hard cutover cleanup:
-- The legacy source-level fee_schedule_id column is removed only after operators
-- have cleared or migrated those bindings during the fee-rule cutover.
ALTER TABLE reconciliation_sources
    DROP COLUMN IF EXISTS fee_schedule_id;
