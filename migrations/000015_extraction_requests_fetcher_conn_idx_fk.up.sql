-- Add index and foreign key for extraction_requests.fetcher_conn_id.
-- The index accelerates lookups by fetcher connection (e.g., listing all
-- extractions for a given connection). The FK enforces referential integrity
-- against the fetcher_connections table's unique fetcher_conn_id column.

CREATE INDEX IF NOT EXISTS idx_extraction_requests_fetcher_conn_id
    ON extraction_requests (fetcher_conn_id);

ALTER TABLE extraction_requests
    ADD CONSTRAINT fk_extraction_requests_fetcher_conn_id
    FOREIGN KEY (fetcher_conn_id)
    REFERENCES fetcher_connections (fetcher_conn_id)
    ON DELETE RESTRICT
    NOT VALID;

-- NOTE:
-- The FK is created as NOT VALID to keep rollout backward-compatible when
-- legacy orphan rows might exist. New writes are still checked immediately.
-- Run `ALTER TABLE extraction_requests VALIDATE CONSTRAINT fk_extraction_requests_fetcher_conn_id;`
-- after orphan remediation to enforce referential integrity for historical rows.
