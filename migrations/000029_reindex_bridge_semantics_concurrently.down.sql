-- Reverse 000029 by restoring the pre-000029 state: drop the concurrent
-- indexes and recreate them with the original plain CREATE INDEX statements
-- from 000026/000027. This rollback reintroduces the exclusive-lock behavior
-- on rebuild, matching the pre-migration state for history integrity.

-- From 000026: failed-bridge drilldown index.
DROP INDEX CONCURRENTLY IF EXISTS idx_extraction_requests_bridge_failed_class;

CREATE INDEX IF NOT EXISTS idx_extraction_requests_bridge_failed_class
    ON extraction_requests (created_at, id)
    WHERE bridge_last_error IS NOT NULL AND bridge_failed_at IS NOT NULL;

-- From 000026: ingestion short-circuit index.
DROP INDEX CONCURRENTLY IF EXISTS idx_ingestion_jobs_metadata_extraction_id;

CREATE INDEX IF NOT EXISTS idx_ingestion_jobs_metadata_extraction_id
    ON ingestion_jobs ((metadata->>'extractionId'))
    WHERE metadata->>'extractionId' IS NOT NULL AND status = 'COMPLETED';

-- From 000027: custody retention sweep index.
DROP INDEX CONCURRENTLY IF EXISTS idx_extraction_requests_custody_pending_cleanup;

CREATE INDEX IF NOT EXISTS idx_extraction_requests_custody_pending_cleanup
    ON extraction_requests (updated_at)
    WHERE custody_deleted_at IS NULL
      AND (bridge_last_error IS NOT NULL OR ingestion_job_id IS NOT NULL);
