ALTER TABLE match_fee_variances
    DROP COLUMN IF EXISTS fee_schedule_item_id,
    DROP COLUMN IF EXISTS fee_schedule_id;

ALTER TABLE reconciliation_contexts
    DROP COLUMN IF EXISTS fee_normalization;

ALTER TABLE reconciliation_sources
    DROP COLUMN IF EXISTS fee_schedule_id;

DROP TABLE IF EXISTS fee_schedule_items;
DROP TABLE IF EXISTS fee_schedules;
