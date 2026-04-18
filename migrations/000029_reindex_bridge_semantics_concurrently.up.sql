-- Reindex the partial indexes added by 000026 (bridge failure semantics) and
-- 000027 (custody deletion marker) using CREATE INDEX CONCURRENTLY. The
-- originals used plain CREATE INDEX, which takes an exclusive write lock on
-- the target table for the duration of the build. On large extraction_requests
-- / ingestion_jobs tables this blocks the bridge worker + ingestion traffic.
--
-- Migrations 000024/000025 correctly used CONCURRENTLY, while 000026/000027 are
-- the outliers. This migration drops and recreates each index with the same name
-- and predicate, non-blocking.
--
-- Naming and predicates preserved EXACTLY as defined in 000026/000027 so
-- downstream queries continue to match without planner surprises.

-- From 000026: failed-bridge drilldown index.
DROP INDEX CONCURRENTLY IF EXISTS idx_extraction_requests_bridge_failed_class;

CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_extraction_requests_bridge_failed_class
    ON extraction_requests (created_at, id)
    WHERE bridge_last_error IS NOT NULL AND bridge_failed_at IS NOT NULL;

-- From 000026: ingestion short-circuit index.
DROP INDEX CONCURRENTLY IF EXISTS idx_ingestion_jobs_metadata_extraction_id;

CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_ingestion_jobs_metadata_extraction_id
    ON ingestion_jobs ((metadata->>'extractionId'))
    WHERE metadata->>'extractionId' IS NOT NULL AND status = 'COMPLETED';

-- From 000027: custody retention sweep index.
DROP INDEX CONCURRENTLY IF EXISTS idx_extraction_requests_custody_pending_cleanup;

CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_extraction_requests_custody_pending_cleanup
    ON extraction_requests (updated_at)
    WHERE custody_deleted_at IS NULL
      AND (bridge_last_error IS NOT NULL OR ingestion_job_id IS NOT NULL);
