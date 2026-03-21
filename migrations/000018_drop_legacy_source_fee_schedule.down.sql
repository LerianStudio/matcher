-- Rollback note:
-- This restores the legacy column shape only. Historical fee_schedule_id values
-- are not reconstructed automatically after the column was dropped.
ALTER TABLE reconciliation_sources
    ADD COLUMN fee_schedule_id UUID REFERENCES fee_schedules(id);
