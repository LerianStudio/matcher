-- Reverse the tightened predicate introduced by 000028.up.sql by restoring the
-- looser predicate from 000024 (status='COMPLETE' AND ingestion_job_id IS NULL,
-- without the bridge_last_error IS NULL clause). Keeps the rollback safe to run
-- repeatedly and non-blocking via CONCURRENTLY + IF EXISTS.
DROP INDEX CONCURRENTLY IF EXISTS idx_extraction_requests_eligible_for_bridge;

CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_extraction_requests_eligible_for_bridge
  ON extraction_requests (updated_at ASC)
  WHERE status = 'COMPLETE'
    AND ingestion_job_id IS NULL;
