-- Rollback Release Baseline Migration
-- WARNING: This drops all Matcher schema objects and data.

DROP TABLE IF EXISTS actor_mapping;
DROP TABLE IF EXISTS archive_metadata;

DROP RULE IF EXISTS audit_logs_no_delete ON audit_logs;
DROP RULE IF EXISTS audit_logs_no_update ON audit_logs;

DROP INDEX IF EXISTS idx_audit_logs_tenant_seq_uq;
-- CASCADE removes partitions and partition indexes
DROP TABLE IF EXISTS audit_logs CASCADE;
DROP TABLE IF EXISTS audit_log_chain_state;
DROP TABLE IF EXISTS exceptions;
DROP TABLE IF EXISTS match_fee_variances;
DROP TABLE IF EXISTS adjustments;
DROP TABLE IF EXISTS export_jobs;
DROP TABLE IF EXISTS outbox_events;
DROP TABLE IF EXISTS match_items;
DROP TABLE IF EXISTS match_groups;
DROP TABLE IF EXISTS match_runs;
DROP TABLE IF EXISTS transactions;
DROP TABLE IF EXISTS ingestion_jobs;
DROP TABLE IF EXISTS match_rules;
DROP TABLE IF EXISTS field_maps;
DROP TABLE IF EXISTS reconciliation_sources;
DROP TABLE IF EXISTS reconciliation_contexts;
DROP TABLE IF EXISTS rates;

DROP TYPE IF EXISTS adjustment_type;
DROP TYPE IF EXISTS export_report_type;
DROP TYPE IF EXISTS export_format;
DROP TYPE IF EXISTS export_job_status;
DROP TYPE IF EXISTS outbox_event_status;
DROP TYPE IF EXISTS external_system_type;
DROP TYPE IF EXISTS exception_status;
DROP TYPE IF EXISTS exception_severity;
DROP TYPE IF EXISTS match_group_status;
DROP TYPE IF EXISTS match_run_status;
DROP TYPE IF EXISTS match_run_mode;
DROP TYPE IF EXISTS transaction_status;
DROP TYPE IF EXISTS transaction_extraction_status;
DROP TYPE IF EXISTS ingestion_job_status;
DROP TYPE IF EXISTS match_rule_type;
DROP TYPE IF EXISTS reconciliation_source_type;
DROP TYPE IF EXISTS reconciliation_context_status;
DROP TYPE IF EXISTS reconciliation_context_type;
