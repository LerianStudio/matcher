-- Matcher Release Baseline Schema
-- Consolidated migration for initial production release.

-- Enum types
CREATE TYPE reconciliation_context_type AS ENUM ('1:1', '1:N', 'N:M');
CREATE TYPE reconciliation_context_status AS ENUM ('ACTIVE', 'PAUSED');
CREATE TYPE reconciliation_source_type AS ENUM ('LEDGER', 'BANK', 'GATEWAY', 'CUSTOM');
CREATE TYPE match_rule_type AS ENUM ('EXACT', 'TOLERANCE', 'DATE_LAG');
CREATE TYPE ingestion_job_status AS ENUM ('QUEUED', 'PROCESSING', 'COMPLETED', 'FAILED');
CREATE TYPE transaction_extraction_status AS ENUM ('PENDING', 'COMPLETE', 'FAILED');
CREATE TYPE transaction_status AS ENUM ('UNMATCHED', 'MATCHED', 'IGNORED', 'PENDING_REVIEW');
CREATE TYPE match_run_mode AS ENUM ('DRY_RUN', 'COMMIT');
CREATE TYPE match_run_status AS ENUM ('PROCESSING', 'COMPLETED', 'FAILED');
CREATE TYPE match_group_status AS ENUM ('PROPOSED', 'CONFIRMED', 'REJECTED');
CREATE TYPE exception_severity AS ENUM ('LOW', 'MEDIUM', 'HIGH', 'CRITICAL');
CREATE TYPE exception_status AS ENUM ('OPEN', 'ASSIGNED', 'RESOLVED');
CREATE TYPE external_system_type AS ENUM ('JIRA', 'SERVICENOW', 'WEBHOOK');
CREATE TYPE outbox_event_status AS ENUM ('PENDING', 'PROCESSING', 'PUBLISHED', 'FAILED', 'INVALID');
CREATE TYPE export_job_status AS ENUM ('QUEUED', 'RUNNING', 'SUCCEEDED', 'FAILED', 'EXPIRED', 'CANCELED');
CREATE TYPE export_format AS ENUM ('CSV', 'JSON', 'XML', 'PDF');
CREATE TYPE export_report_type AS ENUM ('MATCHED', 'UNMATCHED', 'SUMMARY', 'VARIANCE');
CREATE TYPE adjustment_type AS ENUM (
    'BANK_FEE',
    'FX_DIFFERENCE',
    'ROUNDING',
    'WRITE_OFF',
    'MISCELLANEOUS'
);

-- Rates table: owned by matching, stored per-tenant schema
CREATE TABLE rates (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    tenant_id UUID NOT NULL,
    name VARCHAR(100) NOT NULL,
    currency VARCHAR(3) NOT NULL,
    structure_type TEXT NOT NULL, -- FLAT|PERCENTAGE|TIERED
    structure JSONB NOT NULL DEFAULT '{}'::jsonb,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    CONSTRAINT uq_rates_tenant_name UNIQUE (tenant_id, name)
);

-- Reconciliation Contexts table
CREATE TABLE reconciliation_contexts (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    tenant_id UUID NOT NULL,
    name VARCHAR(100) NOT NULL,
    type reconciliation_context_type NOT NULL,
    interval VARCHAR(100) NOT NULL,
    status reconciliation_context_status NOT NULL DEFAULT 'ACTIVE',
    rate_id UUID REFERENCES rates(id),
    fee_tolerance_abs DECIMAL(20, 8) NOT NULL DEFAULT 0,
    fee_tolerance_pct DECIMAL(20, 10) NOT NULL DEFAULT 0,
    created_at TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT NOW(),
    CONSTRAINT uq_context_tenant_name UNIQUE (tenant_id, name)
);

-- Reconciliation Sources table
CREATE TABLE reconciliation_sources (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    context_id UUID NOT NULL REFERENCES reconciliation_contexts(id) ON DELETE CASCADE,
    name VARCHAR(50) NOT NULL,
    type reconciliation_source_type NOT NULL,
    config JSONB NOT NULL DEFAULT '{}',
    created_at TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT NOW()
);

-- Field Maps table
CREATE TABLE field_maps (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    context_id UUID NOT NULL REFERENCES reconciliation_contexts(id) ON DELETE CASCADE,
    source_id UUID NOT NULL REFERENCES reconciliation_sources(id) ON DELETE CASCADE,
    mapping JSONB NOT NULL DEFAULT '{}',
    version INTEGER NOT NULL DEFAULT 1,
    created_at TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT NOW()
);

-- Match Rules table
CREATE TABLE match_rules (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    context_id UUID NOT NULL REFERENCES reconciliation_contexts(id) ON DELETE CASCADE,
    priority INTEGER NOT NULL CHECK (priority > 0),
    type match_rule_type NOT NULL,
    config JSONB NOT NULL DEFAULT '{}',
    created_at TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT NOW(),
    CONSTRAINT uq_rule_context_priority UNIQUE (context_id, priority)
);

-- Ingestion Jobs table
CREATE TABLE ingestion_jobs (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    context_id UUID NOT NULL REFERENCES reconciliation_contexts(id) ON DELETE CASCADE,
    source_id UUID NOT NULL REFERENCES reconciliation_sources(id) ON DELETE CASCADE,
    status ingestion_job_status NOT NULL DEFAULT 'QUEUED',
    started_at TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT NOW(),
    completed_at TIMESTAMP WITH TIME ZONE,
    metadata JSONB DEFAULT '{}',
    created_at TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT NOW()
);

-- Transactions table
CREATE TABLE transactions (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    ingestion_job_id UUID NOT NULL REFERENCES ingestion_jobs(id) ON DELETE CASCADE,
    source_id UUID NOT NULL REFERENCES reconciliation_sources(id) ON DELETE CASCADE,
    external_id VARCHAR(255) NOT NULL,
    amount DECIMAL(20, 8) NOT NULL,
    currency VARCHAR(3) NOT NULL,
    amount_base DECIMAL(20, 8),
    base_currency VARCHAR(3),
    fx_rate DECIMAL(20, 10),
    fx_rate_source VARCHAR(100),
    fx_rate_effective_date DATE,
    extraction_status transaction_extraction_status NOT NULL DEFAULT 'PENDING',
    date TIMESTAMP WITH TIME ZONE NOT NULL,
    description TEXT,
    status transaction_status NOT NULL DEFAULT 'UNMATCHED',
    metadata JSONB DEFAULT '{}',
    created_at TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT NOW(),
    CONSTRAINT uq_transaction_source_external UNIQUE (source_id, external_id)
);

-- Match Runs table
CREATE TABLE match_runs (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    context_id UUID NOT NULL REFERENCES reconciliation_contexts(id) ON DELETE CASCADE,
    mode match_run_mode NOT NULL,
    status match_run_status NOT NULL DEFAULT 'PROCESSING',
    started_at TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT NOW(),
    completed_at TIMESTAMP WITH TIME ZONE,
    stats JSONB DEFAULT '{}',
    failure_reason TEXT,
    created_at TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT NOW()
);

-- Match Groups table
CREATE TABLE match_groups (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    context_id UUID NOT NULL REFERENCES reconciliation_contexts(id) ON DELETE CASCADE,
    run_id UUID NOT NULL REFERENCES match_runs(id) ON DELETE CASCADE,
    rule_id UUID NOT NULL REFERENCES match_rules(id) ON DELETE CASCADE,
    confidence INTEGER NOT NULL CHECK (confidence >= 0 AND confidence <= 100),
    status match_group_status NOT NULL DEFAULT 'PROPOSED',
    rejected_reason TEXT,
    confirmed_at TIMESTAMP WITH TIME ZONE,
    created_at TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT NOW()
);

-- Match Items table
CREATE TABLE match_items (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    match_group_id UUID NOT NULL REFERENCES match_groups(id) ON DELETE CASCADE,
    transaction_id UUID NOT NULL REFERENCES transactions(id) ON DELETE CASCADE,
    allocated_amount DECIMAL(20, 8) NOT NULL,
    allocated_currency VARCHAR(3) NOT NULL,
    expected_amount DECIMAL(20, 8) NOT NULL DEFAULT 0,
    allow_partial BOOLEAN NOT NULL DEFAULT FALSE,
    created_at TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT NOW()
);

-- Exceptions table
CREATE TABLE exceptions (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    transaction_id UUID NOT NULL UNIQUE REFERENCES transactions(id) ON DELETE CASCADE,
    severity exception_severity NOT NULL DEFAULT 'MEDIUM',
    status exception_status NOT NULL DEFAULT 'OPEN',
    reason TEXT,
    metadata JSONB NOT NULL DEFAULT '{}'::jsonb,
    version BIGINT NOT NULL DEFAULT 1,
    resolution_type VARCHAR(50),
    resolution_reason VARCHAR(100),
    external_system external_system_type,
    external_issue_id VARCHAR(255),
    assigned_to VARCHAR(255),
    due_at TIMESTAMP WITH TIME ZONE,
    resolution_notes TEXT,
    created_at TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT NOW()
);

-- Per-tenant sequence counter for audit log hash chain ordering
CREATE TABLE audit_log_chain_state (
    tenant_id UUID PRIMARY KEY,
    next_seq BIGINT NOT NULL DEFAULT 1
);

-- Audit Logs table (append-only, range-partitioned by created_at monthly)
-- PK is (id, created_at) because PostgreSQL requires the partition key in all
-- unique constraints on partitioned tables.
CREATE TABLE audit_logs (
    id UUID NOT NULL DEFAULT gen_random_uuid(),
    tenant_id UUID NOT NULL,
    entity_type VARCHAR(50) NOT NULL,
    entity_id UUID NOT NULL,
    action VARCHAR(50) NOT NULL,
    actor_id VARCHAR(255),
    changes JSONB NOT NULL DEFAULT '{}',
    created_at TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT NOW(),
    tenant_seq BIGINT,
    prev_hash BYTEA,
    record_hash BYTEA,
    hash_version SMALLINT NOT NULL DEFAULT 1,
    CONSTRAINT audit_logs_pkey PRIMARY KEY (id, created_at),
    CONSTRAINT audit_logs_prev_hash_len CHECK (prev_hash IS NULL OR octet_length(prev_hash) = 32),
    CONSTRAINT audit_logs_record_hash_len CHECK (record_hash IS NULL OR octet_length(record_hash) = 32)
) PARTITION BY RANGE (created_at);

-- Initial monthly partitions (no DEFAULT partition -- incompatible with DETACH PARTITION CONCURRENTLY)
CREATE TABLE audit_logs_2026_02 PARTITION OF audit_logs
    FOR VALUES FROM ('2026-02-01') TO ('2026-03-01');

CREATE TABLE audit_logs_2026_03 PARTITION OF audit_logs
    FOR VALUES FROM ('2026-03-01') TO ('2026-04-01');

CREATE TABLE audit_logs_2026_04 PARTITION OF audit_logs
    FOR VALUES FROM ('2026-04-01') TO ('2026-05-01');

COMMENT ON TABLE audit_log_chain_state IS 'Per-tenant sequence counter for audit log hash chain ordering';
COMMENT ON TABLE audit_logs IS 'Append-only audit log, range-partitioned by created_at (monthly granularity)';
COMMENT ON COLUMN audit_logs.tenant_seq IS 'Monotonic sequence per tenant for deterministic hash chain ordering';
COMMENT ON COLUMN audit_logs.prev_hash IS 'SHA-256 hash of the previous audit log entry (32 bytes). Genesis records use 32 zero bytes.';
COMMENT ON COLUMN audit_logs.record_hash IS 'SHA-256 hash of prev_hash concatenated with canonicalized record content (32 bytes)';
COMMENT ON COLUMN audit_logs.hash_version IS 'Version of the canonicalization/hashing scheme for future compatibility';

-- Enforce append-only audit_logs
-- NOTE: Our migration runner splits statements naively on semicolons (even inside $$...$$),
-- so we cannot define PL/pgSQL trigger functions here. We use RULEs that fail fast
-- (instead of DO INSTEAD NOTHING) to prevent accidental UPDATE/DELETE.
-- Post-deploy: run scripts/post-deploy-audit-logs-append-only.sql to replace
-- these RULEs with a trigger that raises a clear exception message.
CREATE OR REPLACE RULE audit_logs_no_update AS
    ON UPDATE TO audit_logs
    DO INSTEAD (SELECT pg_sleep(-1));

CREATE OR REPLACE RULE audit_logs_no_delete AS
    ON DELETE TO audit_logs
    DO INSTEAD (SELECT pg_sleep(-1));

-- Outbox events table for transactional event publishing
CREATE TABLE outbox_events (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    event_type VARCHAR(100) NOT NULL,
    aggregate_id UUID NOT NULL,
    payload JSONB NOT NULL,
    status outbox_event_status NOT NULL DEFAULT 'PENDING',
    attempts INTEGER NOT NULL DEFAULT 0,
    published_at TIMESTAMP WITH TIME ZONE,
    last_error TEXT,
    created_at TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT NOW()
);

-- Match fee variances table
CREATE TABLE match_fee_variances (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    context_id UUID NOT NULL REFERENCES reconciliation_contexts(id),
    run_id UUID NOT NULL REFERENCES match_runs(id),
    match_group_id UUID NOT NULL REFERENCES match_groups(id) ON DELETE CASCADE,
    transaction_id UUID NOT NULL REFERENCES transactions(id) ON DELETE CASCADE,
    rate_id UUID NOT NULL REFERENCES rates(id),

    currency VARCHAR(3) NOT NULL,
    expected_fee_amount DECIMAL(20, 8) NOT NULL,
    actual_fee_amount DECIMAL(20, 8) NOT NULL,
    delta DECIMAL(20, 8) NOT NULL,
    tolerance_abs DECIMAL(20, 8) NOT NULL,
    tolerance_percent DECIMAL(20, 10) NOT NULL,
    variance_type TEXT NOT NULL, -- MATCH|UNDERCHARGE|OVERCHARGE

    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

-- Export Jobs table
CREATE TABLE export_jobs (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    tenant_id UUID NOT NULL,
    context_id UUID NOT NULL REFERENCES reconciliation_contexts(id) ON DELETE CASCADE,
    report_type export_report_type NOT NULL,
    format export_format NOT NULL,
    filter JSONB NOT NULL DEFAULT '{}',
    status export_job_status NOT NULL DEFAULT 'QUEUED',
    records_written BIGINT NOT NULL DEFAULT 0,
    bytes_written BIGINT NOT NULL DEFAULT 0,
    file_key TEXT,
    file_name TEXT,
    sha256 TEXT,
    error TEXT,
    attempts INTEGER NOT NULL DEFAULT 0,
    next_retry_at TIMESTAMP WITH TIME ZONE,
    created_at TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT NOW(),
    started_at TIMESTAMP WITH TIME ZONE,
    finished_at TIMESTAMP WITH TIME ZONE,
    expires_at TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT (NOW() + INTERVAL '7 days'),
    updated_at TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT NOW()
);

-- Adjustments table for balancing journal entries
CREATE TABLE adjustments (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    context_id UUID NOT NULL,
    match_group_id UUID,
    transaction_id UUID,
    type adjustment_type NOT NULL,
    direction VARCHAR(6) NOT NULL,
    amount NUMERIC(20, 8) NOT NULL,
    currency VARCHAR(3) NOT NULL,
    description VARCHAR(500) NOT NULL,
    reason TEXT NOT NULL,
    created_by VARCHAR(255) NOT NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),

    CONSTRAINT chk_adjustments_target CHECK (match_group_id IS NOT NULL OR transaction_id IS NOT NULL),
    CONSTRAINT adjustments_direction_check CHECK (direction IN ('DEBIT', 'CREDIT')),
    CONSTRAINT fk_adjustments_context FOREIGN KEY (context_id) REFERENCES reconciliation_contexts(id) ON DELETE CASCADE,
    CONSTRAINT fk_adjustments_match_group FOREIGN KEY (match_group_id) REFERENCES match_groups(id) ON DELETE CASCADE,
    CONSTRAINT fk_adjustments_transaction FOREIGN KEY (transaction_id) REFERENCES transactions(id) ON DELETE CASCADE
);

-- Indexes for common queries
CREATE INDEX idx_rates_tenant ON rates(tenant_id);

CREATE INDEX idx_contexts_tenant ON reconciliation_contexts(tenant_id);
CREATE INDEX idx_contexts_rate ON reconciliation_contexts(rate_id);

CREATE INDEX idx_sources_context ON reconciliation_sources(context_id);

CREATE INDEX idx_field_maps_source ON field_maps(source_id);
CREATE INDEX idx_field_maps_context ON field_maps(context_id);

CREATE INDEX idx_rules_context ON match_rules(context_id);

CREATE INDEX idx_jobs_context ON ingestion_jobs(context_id);
CREATE INDEX idx_jobs_status ON ingestion_jobs(status);

CREATE INDEX idx_transactions_job ON transactions(ingestion_job_id);
CREATE INDEX idx_transactions_source ON transactions(source_id);
CREATE INDEX idx_transactions_status ON transactions(status);
CREATE INDEX idx_transactions_date ON transactions(date);

CREATE INDEX idx_match_runs_context ON match_runs(context_id);

CREATE INDEX idx_match_groups_run ON match_groups(run_id);

CREATE INDEX idx_match_items_group ON match_items(match_group_id);
CREATE INDEX idx_match_items_transaction ON match_items(transaction_id);

CREATE INDEX idx_exceptions_status ON exceptions(status);
CREATE INDEX idx_exceptions_due ON exceptions(due_at);
CREATE INDEX IF NOT EXISTS idx_exceptions_reason ON exceptions(reason);
CREATE INDEX IF NOT EXISTS idx_exceptions_resolution_type ON exceptions(resolution_type) WHERE resolution_type IS NOT NULL;

CREATE INDEX idx_audit_logs_tenant ON audit_logs(tenant_id);
CREATE INDEX idx_audit_logs_entity ON audit_logs(entity_type, entity_id);
CREATE INDEX idx_audit_logs_created ON audit_logs(created_at);
-- Unique index must include created_at (partition key) for partitioned tables
CREATE UNIQUE INDEX idx_audit_logs_tenant_seq_uq ON audit_logs(tenant_id, tenant_seq, created_at) WHERE tenant_seq IS NOT NULL;

CREATE INDEX idx_outbox_events_status_created ON outbox_events(status, created_at);
CREATE INDEX idx_outbox_events_aggregate_id ON outbox_events(aggregate_id);
CREATE INDEX idx_outbox_events_event_type ON outbox_events(event_type);
CREATE INDEX IF NOT EXISTS idx_outbox_events_failed_retry ON outbox_events(status, updated_at, attempts)
    WHERE status = 'FAILED';

CREATE INDEX idx_match_fee_variances_group ON match_fee_variances(match_group_id);
CREATE INDEX idx_match_fee_variances_run ON match_fee_variances(context_id, run_id);
CREATE INDEX idx_match_fee_variances_tx ON match_fee_variances(transaction_id);

CREATE INDEX idx_export_jobs_tenant_id ON export_jobs(tenant_id);
CREATE INDEX idx_export_jobs_context_id ON export_jobs(context_id);
CREATE INDEX idx_export_jobs_status ON export_jobs(status);
CREATE INDEX idx_export_jobs_created_at ON export_jobs(created_at DESC);
CREATE INDEX idx_export_jobs_expires_at ON export_jobs(expires_at) WHERE status = 'SUCCEEDED';
CREATE INDEX idx_export_jobs_tenant_status ON export_jobs(tenant_id, status, created_at DESC);
CREATE INDEX idx_export_jobs_retry ON export_jobs(status, next_retry_at, created_at)
    WHERE status = 'QUEUED';

CREATE INDEX idx_adjustments_context_id ON adjustments(context_id);
CREATE INDEX idx_adjustments_match_group_id ON adjustments(match_group_id) WHERE match_group_id IS NOT NULL;
CREATE INDEX idx_adjustments_transaction_id ON adjustments(transaction_id) WHERE transaction_id IS NOT NULL;
CREATE INDEX idx_adjustments_type ON adjustments(context_id, type);

COMMENT ON TABLE export_jobs IS 'Tracks async export job lifecycle for large report exports';
COMMENT ON COLUMN export_jobs.filter IS 'Original filter parameters as JSON (date_from, date_to, source_id, etc.)';
COMMENT ON COLUMN export_jobs.file_key IS 'Object storage key/path for the generated file';
COMMENT ON COLUMN export_jobs.expires_at IS 'When the export file will be deleted (default 7 days)';
COMMENT ON COLUMN export_jobs.attempts IS 'Number of processing attempts (incremented each time job is claimed)';
COMMENT ON COLUMN export_jobs.next_retry_at IS 'When the job can be retried (NULL means immediately eligible)';

COMMENT ON TABLE adjustments IS 'Balancing journal entries to resolve variances between matched transactions';
COMMENT ON COLUMN adjustments.type IS 'Category of the adjustment (BANK_FEE, FX_DIFFERENCE, ROUNDING, WRITE_OFF, MISCELLANEOUS)';
COMMENT ON COLUMN adjustments.amount IS 'Adjustment amount (can be positive or negative)';
COMMENT ON COLUMN adjustments.reason IS 'Reason for creating this adjustment';
COMMENT ON COLUMN adjustments.created_by IS 'User or system that created the adjustment';
COMMENT ON COLUMN adjustments.direction IS 'Journal entry direction: DEBIT or CREDIT';

-- Archive Metadata table
-- Tracks which audit_logs partitions have been archived, their object storage
-- location, checksum, and lifecycle state machine status.
CREATE TABLE archive_metadata (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    tenant_id UUID NOT NULL,
    partition_name VARCHAR(100) NOT NULL,
    date_range_start TIMESTAMP WITH TIME ZONE NOT NULL,
    date_range_end TIMESTAMP WITH TIME ZONE NOT NULL,
    row_count BIGINT NOT NULL DEFAULT 0,
    archive_key VARCHAR(500),
    checksum VARCHAR(128),
    compressed_size_bytes BIGINT,
    storage_class VARCHAR(50),
    status VARCHAR(20) NOT NULL DEFAULT 'PENDING',
    error_message TEXT,
    archived_at TIMESTAMP WITH TIME ZONE,
    created_at TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT NOW()
);

CREATE INDEX idx_archive_metadata_tenant ON archive_metadata(tenant_id);
CREATE INDEX idx_archive_metadata_status ON archive_metadata(status);
CREATE UNIQUE INDEX idx_archive_metadata_tenant_partition ON archive_metadata(tenant_id, partition_name);

COMMENT ON TABLE archive_metadata IS 'Tracks audit log partition archival state machine: PENDING -> EXPORTING -> EXPORTED -> UPLOADING -> UPLOADED -> VERIFYING -> VERIFIED -> DETACHING -> COMPLETE';
COMMENT ON COLUMN archive_metadata.partition_name IS 'PostgreSQL partition table name (e.g., audit_logs_2026_02)';
COMMENT ON COLUMN archive_metadata.archive_key IS 'Object storage key/path for the archived partition data';
COMMENT ON COLUMN archive_metadata.checksum IS 'SHA-256 checksum of the archived data for integrity verification';
COMMENT ON COLUMN archive_metadata.compressed_size_bytes IS 'Size of the compressed archive in bytes';
COMMENT ON COLUMN archive_metadata.storage_class IS 'Object storage class (e.g., GLACIER, DEEP_ARCHIVE)';
COMMENT ON COLUMN archive_metadata.status IS 'Current archival lifecycle state';
COMMENT ON COLUMN archive_metadata.error_message IS 'Error details if archival failed at any step (preserved for retry diagnosis)';
COMMENT ON COLUMN archive_metadata.archived_at IS 'Timestamp when archival completed successfully';

-- Actor Mapping table
-- Mutable indirection layer for GDPR pseudonymization. Maps opaque actor IDs
-- to PII (display name, email). On right-to-erasure: delete the mapping row,
-- the audit log remains intact with an unresolvable actor ID.
-- NOTE: This table is intentionally NOT append-only (no RULEs) since it must
-- support UPDATE (pseudonymize) and DELETE (right-to-erasure).
CREATE TABLE actor_mapping (
    actor_id VARCHAR(255) PRIMARY KEY,
    display_name VARCHAR(255),
    email VARCHAR(255),
    created_at TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT NOW()
);

CREATE INDEX idx_actor_mapping_email ON actor_mapping(email) WHERE email IS NOT NULL;

COMMENT ON TABLE actor_mapping IS 'GDPR-compliant actor identity mapping. Mutable: supports pseudonymization (UPDATE) and right-to-erasure (DELETE)';
COMMENT ON COLUMN actor_mapping.actor_id IS 'Opaque identifier matching audit_logs.actor_id';
COMMENT ON COLUMN actor_mapping.display_name IS 'Human-readable name. Set to [REDACTED] on pseudonymization';
COMMENT ON COLUMN actor_mapping.email IS 'Email address. Set to [REDACTED] on pseudonymization';
