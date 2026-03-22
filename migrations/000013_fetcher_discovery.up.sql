-- Fetcher Service Discovery tables
-- Stores discovered database connections, schemas, and extraction requests
-- for the optional Fetcher integration.

CREATE TABLE fetcher_connections (
    id                UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    fetcher_conn_id   TEXT NOT NULL,
    config_name       TEXT NOT NULL,
    database_type     TEXT NOT NULL,
    host              TEXT NOT NULL DEFAULT '',
    port              INTEGER NOT NULL DEFAULT 0,
    database_name     TEXT NOT NULL DEFAULT '',
    product_name      TEXT NOT NULL DEFAULT '',
    status            TEXT NOT NULL DEFAULT 'UNKNOWN',
    last_seen_at      TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    schema_discovered BOOLEAN NOT NULL DEFAULT FALSE,
    created_at        TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at        TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    CONSTRAINT uq_fetcher_connections_fetcher_id UNIQUE (fetcher_conn_id),
    CONSTRAINT chk_fetcher_connections_status CHECK (status IN ('AVAILABLE', 'UNREACHABLE', 'UNKNOWN')),
    CONSTRAINT chk_fetcher_connections_fetcher_conn_id_nonempty CHECK (btrim(fetcher_conn_id) <> ''),
    CONSTRAINT chk_fetcher_connections_config_name_nonempty CHECK (btrim(config_name) <> ''),
    CONSTRAINT chk_fetcher_connections_database_type_nonempty CHECK (btrim(database_type) <> ''),
    CONSTRAINT chk_fetcher_connections_port_range CHECK (port BETWEEN 0 AND 65535)
);

CREATE TABLE discovered_schemas (
    id              UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    connection_id   UUID NOT NULL REFERENCES fetcher_connections(id) ON DELETE CASCADE,
    table_name      TEXT NOT NULL,
    columns         JSONB NOT NULL DEFAULT '[]',
    discovered_at   TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    CONSTRAINT uq_discovered_schemas_conn_table UNIQUE (connection_id, table_name),
    CONSTRAINT chk_discovered_schemas_columns_array CHECK (jsonb_typeof(columns) = 'array')
);

-- extraction_requests tracks data extraction jobs submitted to Fetcher.
-- ingestion_job_id is NULLABLE to decouple extraction lifecycle from ingestion:
--   - Extractions can be triggered independently (e.g., by scheduler) before any ingestion job exists
--   - The ingestion_job_id is set later when extracted data is actually imported
--   - This allows extraction requests to exist without requiring a pre-existing ingestion job
CREATE TABLE extraction_requests (
    id               UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    connection_id    UUID NOT NULL REFERENCES fetcher_connections(id) ON DELETE RESTRICT,
    ingestion_job_id UUID REFERENCES ingestion_jobs(id) ON DELETE SET NULL,
    fetcher_job_id   TEXT,
    tables           JSONB NOT NULL DEFAULT '{}',
    start_date       TEXT,
    end_date         TEXT,
    filters          JSONB,
    status           TEXT NOT NULL DEFAULT 'PENDING',
    result_path      TEXT,
    error_message    TEXT,
    created_at       TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at       TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    CONSTRAINT chk_extraction_requests_status CHECK (status IN ('PENDING', 'SUBMITTED', 'EXTRACTING', 'COMPLETE', 'FAILED', 'CANCELLED')),
    CONSTRAINT chk_extraction_requests_tables_object CHECK (jsonb_typeof(tables) = 'object'),
    CONSTRAINT chk_extraction_requests_filters_object CHECK (filters IS NULL OR jsonb_typeof(filters) = 'object'),
    CONSTRAINT chk_extraction_requests_fetcher_job_id_required CHECK (
        status IN ('PENDING', 'FAILED', 'CANCELLED')
        OR (fetcher_job_id IS NOT NULL AND btrim(fetcher_job_id) <> '')
    )
);

-- Index on connection_id for extraction lookups and parent-restriction checks.
CREATE INDEX idx_extraction_requests_connection_id ON extraction_requests (connection_id);
