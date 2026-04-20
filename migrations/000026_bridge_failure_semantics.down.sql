-- Reverse T-005 bridge failure semantics. Drops indexes first so the
-- column drop does not cascade-walk the index trees.

DROP INDEX IF EXISTS idx_ingestion_jobs_metadata_extraction_id;
DROP INDEX IF EXISTS idx_extraction_requests_bridge_failed_class;

ALTER TABLE extraction_requests
    DROP COLUMN IF EXISTS bridge_failed_at,
    DROP COLUMN IF EXISTS bridge_last_error_message,
    DROP COLUMN IF EXISTS bridge_last_error,
    DROP COLUMN IF EXISTS bridge_attempts;
