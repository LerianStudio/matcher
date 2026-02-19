-- Revert audit_logs.changes from JSON back to JSONB.
-- WARNING: This re-enables JSONB normalization which breaks hash chain
-- verification for any records created after the up migration.

ALTER TABLE audit_logs ALTER COLUMN changes TYPE JSONB USING changes::JSONB;
