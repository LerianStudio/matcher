-- Enforces uniqueness of Fetcher connection_id across reconciliation_sources
-- of type 'FETCHER'. A single Fetcher connection must map to exactly one
-- reconciliation source -- a misconfiguration that splits a connection across
-- two sources would otherwise cause BridgeSourceResolverAdapter to silently
-- pick the oldest row (ORDER BY created_at ASC LIMIT 1) without surfacing
-- the ambiguity.
--
-- The partial predicate (WHERE type='FETCHER') limits enforcement to the
-- Fetcher adapter -- other source types may have no connection_id or share
-- keys in their config blobs legitimately. The expression on
-- (config->>'connection_id') matches the runtime lookup in
-- internal/shared/adapters/cross/bridge_source_resolver.go.
--
-- CONCURRENTLY avoids blocking ingest writes on the hot
-- reconciliation_sources table. IF NOT EXISTS makes the migration safe to
-- retry and is paired with the symmetric DROP INDEX CONCURRENTLY IF EXISTS
-- in the .down.sql.
-- (Comment terminators intentionally avoid the SQL statement terminator byte
-- because golang-migrate's multi-statement splitter is not comment-aware.)
CREATE UNIQUE INDEX CONCURRENTLY IF NOT EXISTS idx_reconciliation_sources_fetcher_connection
  ON reconciliation_sources ((config->>'connection_id'))
  WHERE type = 'FETCHER';
