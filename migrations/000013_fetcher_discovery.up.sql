-- Fetcher Service Discovery tables
-- Stores discovered database connections, schemas, and extraction requests
-- for the optional Fetcher integration.

CREATE TABLE IF NOT EXISTS fetcher_connections (
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
    CONSTRAINT uq_fetcher_connections_fetcher_id UNIQUE (fetcher_conn_id)
);

CREATE INDEX IF NOT EXISTS idx_fetcher_connections_status ON fetcher_connections (status);

CREATE TABLE IF NOT EXISTS discovered_schemas (
    id              UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    connection_id   UUID NOT NULL REFERENCES fetcher_connections(id) ON DELETE CASCADE,
    table_name      TEXT NOT NULL,
    columns         JSONB NOT NULL DEFAULT '[]',
    discovered_at   TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    CONSTRAINT uq_discovered_schemas_conn_table UNIQUE (connection_id, table_name)
);

CREATE INDEX IF NOT EXISTS idx_discovered_schemas_conn_id ON discovered_schemas (connection_id);

-- extraction_requests tracks data extraction jobs submitted to Fetcher.
-- ingestion_job_id is NULLABLE to decouple extraction lifecycle from ingestion:
--   - Extractions can be triggered independently (e.g., by scheduler) before any ingestion job exists
--   - The ingestion_job_id is set later when extracted data is actually imported
--   - This allows extraction requests to exist without requiring a pre-existing ingestion job
CREATE TABLE IF NOT EXISTS extraction_requests (
    id               UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    connection_id    UUID NOT NULL REFERENCES fetcher_connections(id) ON DELETE RESTRICT,
    ingestion_job_id UUID,
    fetcher_job_id   TEXT,
    tables           JSONB NOT NULL DEFAULT '{}',
    filters          JSONB,
    status           TEXT NOT NULL DEFAULT 'PENDING',
    result_path      TEXT,
    error_message    TEXT,
    created_at       TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at       TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

-- Index on ingestion_job_id (nullable columns can be indexed and NULLs are excluded from lookups)
CREATE INDEX IF NOT EXISTS idx_extraction_requests_connection_id ON extraction_requests (connection_id);
CREATE INDEX IF NOT EXISTS idx_extraction_requests_job ON extraction_requests (ingestion_job_id) WHERE ingestion_job_id IS NOT NULL;
CREATE INDEX IF NOT EXISTS idx_extraction_requests_status ON extraction_requests (status);
