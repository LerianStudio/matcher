ALTER TABLE reconciliation_sources
    ADD COLUMN fee_schedule_id UUID REFERENCES fee_schedules(id);
