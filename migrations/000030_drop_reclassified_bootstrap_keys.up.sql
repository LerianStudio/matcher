-- Migration: Drop orphan rows for systemplane keys that v5 reclassified as
-- bootstrap-only (admin writes silently ignored at runtime).
--
-- Background: the lib-commons v5 migration removed the following keys from
-- matcherKeyDefs because every consumer is wired once at startup. Operators
-- who set them via the v4 admin API retained persisted rows that the v5
-- runtime never reads. Deleting them now prevents operator confusion the
-- next time they inspect /system/matcher and see a value the process does
-- not honour.
--
-- Keys (namespace='matcher'):
--   app.log_level                       (logger is initialised once)
--   tenancy.default_tenant_id           (buildTenantExtractor wires once)
--   tenancy.default_tenant_slug         (buildTenantExtractor wires once)
--   auth.enabled                        (auth middleware chain fixed at boot)
--   auth.host                           (auth client constructed once)
--   auth.token_secret                   (auth client constructed once)
--   outbox.retry_window_sec             (dispatcher timing fixed at boot)
--   outbox.dispatch_interval_sec        (dispatcher timing fixed at boot)
--
-- Structure: the original guard used a PL/pgSQL DO block, but golang-migrate
-- v4's multi-statement parser (used by this project with
-- MultiStatementEnabled=true) splits naively on the statement terminator byte
-- and treats the inner terminators in a DO block as statement boundaries.
-- That produces "unterminated dollar-quoted string" errors at apply time.
--
-- Comments in this file intentionally avoid the statement terminator byte for
-- the same reason -- the splitter is not comment-aware either.
--
-- Replacement: two plain top-level statements the splitter handles cleanly.
--   1. CREATE TABLE IF NOT EXISTS public.systemplane_entries(...) -- no-op on
--      upgrades (the table already exists), proto-creates the table on
--      fresh deploys so the following DELETE does not fail on a missing
--      relation. The schema mirrors lib-commons v5's
--      systemplane/internal/postgres.ensureSchema so its later CREATE TABLE
--      IF NOT EXISTS is a no-op too.
--   2. DELETE FROM public.systemplane_entries -- idempotent on both paths
--      (removes orphan rows on upgrades, matches zero rows on fresh deploys).

CREATE TABLE IF NOT EXISTS public.systemplane_entries (
    namespace  TEXT NOT NULL,
    key        TEXT NOT NULL,
    value      JSONB NOT NULL,
    updated_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_by TEXT NOT NULL DEFAULT '',
    PRIMARY KEY (namespace, key)
);

DELETE FROM public.systemplane_entries
 WHERE namespace = 'matcher'
   AND key       IN (
        'app.log_level',
        'tenancy.default_tenant_id',
        'tenancy.default_tenant_slug',
        'auth.enabled',
        'auth.host',
        'auth.token_secret',
        'outbox.retry_window_sec',
        'outbox.dispatch_interval_sec'
   );
