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
-- The systemplane v5 table is created lazily by the Client at startup, so
-- the delete is wrapped in an existence check — first-time deploys (where
-- no rows ever existed) become a no-op.

DO $$
BEGIN
    IF EXISTS (
        SELECT 1
        FROM   information_schema.tables
        WHERE  table_schema = 'public'
          AND  table_name   = 'systemplane_entries'
    ) THEN
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
    END IF;
END$$;
