-- =============================================================================
-- Post-Deployment Audit Logs Append-Only Enforcement
-- =============================================================================
--
-- Run this AFTER baseline migrations to replace RULE-based guards with a
-- trigger that raises a clear exception message on UPDATE/DELETE attempts.
--
-- Usage:
--   psql -h <host> -U <user> -d <database> -f scripts/post-deploy-audit-logs-append-only.sql
--
-- For multi-tenant deployments, run this per tenant schema:
--   SET search_path TO tenant_schema;
--
-- =============================================================================

BEGIN;

DROP TRIGGER IF EXISTS audit_logs_append_only_guard ON audit_logs;
DROP FUNCTION IF EXISTS audit_logs_append_only();

CREATE OR REPLACE FUNCTION audit_logs_append_only()
RETURNS trigger
LANGUAGE plpgsql
AS $$
BEGIN
    RAISE EXCEPTION 'audit_logs table is append-only (operation: %)', TG_OP;
END;
$$;

CREATE TRIGGER audit_logs_append_only_guard
BEFORE UPDATE OR DELETE ON audit_logs
FOR EACH ROW EXECUTE FUNCTION audit_logs_append_only();

DROP RULE IF EXISTS audit_logs_no_update ON audit_logs;
DROP RULE IF EXISTS audit_logs_no_delete ON audit_logs;

COMMIT;
