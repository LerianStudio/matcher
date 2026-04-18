-- T-006 Custody Retention Convergence: persist custody deletion marker.
--
-- The retention sweep (custody_retention_worker) and the bridge orchestrator's
-- happy-path cleanupCustody hook both delete custody objects. Without a
-- persisted "already deleted" marker, the retention query's LATE-LINKED
-- predicate (ingestion_job_id IS NOT NULL AND updated_at < now - grace) would
-- match every linked extraction older than grace forever — causing an
-- unbounded history re-scan and no-op S3 Delete calls on already-absent keys
-- on every sweep cycle.
--
-- This migration adds a terminal marker column so both code paths can record
-- "custody is gone" once, and the retention query can short-circuit subsequent
-- sweeps on the same row (converges to idle after one successful delete).
--
--   custody_deleted_at : UTC timestamp when the custody object was deleted
--                        (either via the orchestrator's happy-path hook or the
--                        retention worker's sweep). NULL while the custody
--                        object may still exist in object storage.
--
-- Indexes:
--   1. idx_extraction_requests_custody_pending_cleanup — supports the
--      retention sweep's "still needs cleanup" query. Partial predicate
--      keeps the index tight: once custody_deleted_at is set, the row drops
--      out of the index and stops re-appearing in sweep candidates.
--
-- Migration is non-destructive: existing rows get custody_deleted_at=NULL by
-- default. Existing terminally-failed or late-linked rows will be swept once
-- (and marked), then converge to idle.

ALTER TABLE extraction_requests
    ADD COLUMN IF NOT EXISTS custody_deleted_at TIMESTAMPTZ;

-- Retention-sweep index. Partial predicate mirrors the FindBridgeRetention
-- Candidates WHERE clause so the sweep drains the relevant rows in O(log n)
-- and — crucially — stops returning rows that have already been cleaned up.
CREATE INDEX IF NOT EXISTS idx_extraction_requests_custody_pending_cleanup
    ON extraction_requests (updated_at)
    WHERE custody_deleted_at IS NULL
      AND (bridge_last_error IS NOT NULL OR ingestion_job_id IS NOT NULL);
