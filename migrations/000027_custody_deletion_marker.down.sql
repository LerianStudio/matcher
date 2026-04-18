-- Rollback for migration 000027 (custody deletion marker).

DROP INDEX IF EXISTS idx_extraction_requests_custody_pending_cleanup;

ALTER TABLE extraction_requests
    DROP COLUMN IF EXISTS custody_deleted_at;
