-- T-005 Retry-Safe Failure And Staleness Control: bridge failure semantics.
--
-- Adds the columns the bridge worker needs to persist a failure classification
-- for an extraction without conflating it with the discovery-layer status. The
-- discovery status (PENDING/SUBMITTED/EXTRACTING/COMPLETE/FAILED/CANCELLED)
-- describes the upstream Fetcher pipeline, while the bridge_* columns describe the
-- Matcher-side bridging pipeline (retrieve → verify → custody → ingest →
-- link). Keeping the two state machines separate preserves the invariant that
-- a COMPLETE+unlinked extraction is the only candidate for the bridge worker
-- to pick up — the worker's filter just adds AND bridge_last_error IS NULL.
--
--   bridge_attempts          : monotonically increases each time the worker
--                              touches this extraction. Capped at the
--                              configured max attempts, beyond which the worker
--                              upgrades the failure to terminal.
--   bridge_last_error        : sentinel-coded class string when terminal
--                              (e.g. "artifact_not_found"). NULL while the
--                              extraction is still retryable. Presence of a
--                              non-NULL value EXCLUDES the row from
--                              FindEligibleForBridge (P2 fix from T-005).
--   bridge_last_error_message: human-readable detail for support diagnosis.
--                              May contain operator-targeted hints (e.g. "404
--                              from Fetcher GET /artifacts/...") but never
--                              client-side PII.
--   bridge_failed_at         : UTC timestamp the terminal failure was
--                              recorded. Used by the readiness projection's
--                              "failed" bucket and by the support drilldown.
--
-- Indexes:
--   1. idx_extraction_requests_bridge_failed_class — supports the readiness
--      drilldown's failed bucket. Partial: only rows that actually failed
--      (sentinel value present + timestamp set) end up indexed.
--   2. idx_ingestion_jobs_metadata_extraction_id — supports the P1
--      orphan-job short-circuit. JSONB expression index over the metadata
--      key the bridge orchestrator now stamps on every IngestTrustedContent
--      call. Partial predicate matches the FindLatestByExtractionID query
--      shape exactly (extractionId stamp present AND status='COMPLETED'),
--      so the index size mirrors the lookup selectivity. The status filter
--      is essential: without it, a FAILED retry remnant would short-circuit
--      a Tick-2 retry into linking against a failed job (Polish Fix 1).
--
-- Migration is non-destructive: existing rows get bridge_attempts=0 and
-- bridge_last_error=NULL via column defaults, leaving the eligibility query
-- shape unchanged for already-bridged or in-flight extractions.

ALTER TABLE extraction_requests
    ADD COLUMN IF NOT EXISTS bridge_attempts INTEGER NOT NULL DEFAULT 0,
    ADD COLUMN IF NOT EXISTS bridge_last_error VARCHAR(64),
    ADD COLUMN IF NOT EXISTS bridge_last_error_message TEXT,
    ADD COLUMN IF NOT EXISTS bridge_failed_at TIMESTAMPTZ;

-- Failed-bridge drilldown index. Partial predicate matches the readiness
-- query's failed-bucket filter: extractions whose bridge step is terminally
-- failed are addressable in O(log n) for the dashboard.
CREATE INDEX IF NOT EXISTS idx_extraction_requests_bridge_failed_class
    ON extraction_requests (created_at, id)
    WHERE bridge_last_error IS NOT NULL AND bridge_failed_at IS NOT NULL;

-- Ingestion short-circuit index (P1). Looks up jobs by extraction_id stamped
-- in metadata so IngestTrustedContent can detect an existing job without a
-- full table scan. Partial keeps the index size proportional to bridge usage.
CREATE INDEX IF NOT EXISTS idx_ingestion_jobs_metadata_extraction_id
    ON ingestion_jobs ((metadata->>'extractionId'))
    WHERE metadata->>'extractionId' IS NOT NULL AND status = 'COMPLETED';
