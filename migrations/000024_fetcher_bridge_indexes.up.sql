-- Partial indexes that keep the bridge worker's hot-path queries off
-- sequential scans. Both indexes are deliberately tiny because their
-- WHERE clauses match only the row populations the worker actually
-- traverses.
--
-- T-003 polish: the bridge worker runs two queries per tick per tenant.
--   1. FindEligibleForBridge: WHERE status = 'COMPLETE' AND
--                             ingestion_job_id IS NULL ORDER BY updated_at ASC.
--   2. BridgeSourceResolverAdapter.ResolveSourceForConnection: JSONB
--      lookup on reconciliation_sources.config->>'connection_id' filtered
--      to FETCHER-typed sources only.
--
-- Without these partial indexes, the planner falls back to seq scans
-- once the extraction_requests / reconciliation_sources tables grow
-- past trivial sizes, and the bridge worker's per-tick cost becomes
-- linear in total row count.

-- Index for bridge worker's eligibility query.
-- Predicate matches FindEligibleForBridge: status='COMPLETE' AND ingestion_job_id IS NULL.
-- Partial index stays tiny because it only includes unlinked-COMPLETE rows.
CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_extraction_requests_eligible_for_bridge
  ON extraction_requests (updated_at ASC)
  WHERE status = 'COMPLETE' AND ingestion_job_id IS NULL;

-- Index for BridgeSourceResolverAdapter.ResolveSourceForConnection JSONB lookup.
-- Partial index on FETCHER sources only; expression index makes O(log n).
CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_reconciliation_sources_fetcher_conn
  ON reconciliation_sources ((config->>'connection_id'))
  WHERE type = 'FETCHER';
