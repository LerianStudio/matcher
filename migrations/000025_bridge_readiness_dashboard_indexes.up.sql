-- Bridge readiness dashboard partial indexes (T-004 polish).
--
-- The bridge readiness drilldown (GET /v1/discovery/extractions/bridge/candidates)
-- partitions extraction_requests into four buckets and orders each page by
-- (created_at ASC, id ASC) for keyset pagination. Migration 000024 only
-- accelerates the worker's eligibility query, which orders by updated_at and
-- targets COMPLETE+unlinked rows. The dashboard needs three more indexes:
--
--   1. ready: COMPLETE+linked rows accumulate forever — without an index
--      every drilldown forces a sequential scan over the success archive.
--   2. failed: FAILED/CANCELLED rows accumulate too. Smaller population, but
--      drilldown pagination still needs an ordered access path.
--   3. pending/stale: T-003's index is on updated_at, but the drilldown
--      orders by created_at — a created_at-keyed partial index removes the
--      sort step on every page.
--
-- All three are partial indexes whose WHERE clauses match exactly the
-- partition predicates in extraction_readiness_queries.go so the planner can
-- skip the table data entirely for the ORDER BY + LIMIT portion of each query.

-- Ready partition: COMPLETE extractions linked to an ingestion job. Forever-
-- growing partition, so without an index each drilldown scans the full table.
CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_extraction_requests_bridge_ready
  ON extraction_requests (created_at, id)
  WHERE status = 'COMPLETE' AND ingestion_job_id IS NOT NULL;

-- Failed partition: FAILED + CANCELLED extractions. Smaller than the ready
-- partition but still needs ordered access for cursor pagination.
CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_extraction_requests_bridge_failed
  ON extraction_requests (created_at, id)
  WHERE status IN ('FAILED', 'CANCELLED');

-- Pending/stale drilldown: COMPLETE+unlinked rows ordered by created_at.
-- T-003's idx_extraction_requests_eligible_for_bridge orders by updated_at
-- because the worker drains oldest-update-first, but the dashboard orders by
-- created_at so it matches operator intuition (oldest extraction first).
CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_extraction_requests_bridge_unlinked_by_created
  ON extraction_requests (created_at, id)
  WHERE status = 'COMPLETE' AND ingestion_job_id IS NULL;
