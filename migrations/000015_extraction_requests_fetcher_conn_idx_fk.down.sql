-- Reverse: drop FK constraint and index on extraction_requests.fetcher_conn_id.

ALTER TABLE extraction_requests
    DROP CONSTRAINT IF EXISTS fk_extraction_requests_fetcher_conn_id;

DROP INDEX IF EXISTS idx_extraction_requests_fetcher_conn_id;
