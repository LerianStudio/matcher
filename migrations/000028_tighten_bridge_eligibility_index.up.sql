-- Tighten idx_extraction_requests_eligible_for_bridge to match T-005 query predicate.
-- Prior migration 000024 omitted `bridge_last_error IS NULL` from the partial index.
-- The T-005 query now includes it, leaving the index carrying terminal rows as dead
-- heap entries. This migration drops and recreates with the tightened predicate.
DROP INDEX CONCURRENTLY IF EXISTS idx_extraction_requests_eligible_for_bridge;

CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_extraction_requests_eligible_for_bridge
  ON extraction_requests (updated_at ASC)
  WHERE status = 'COMPLETE'
    AND ingestion_job_id IS NULL
    AND bridge_last_error IS NULL;
