-- Restore source-level fee schedule attachment.
ALTER TABLE reconciliation_sources ADD COLUMN fee_schedule_id UUID REFERENCES fee_schedules(id);

-- Drop fee rules table and its indexes (indexes dropped automatically with table).
DROP TABLE IF EXISTS fee_rules;
