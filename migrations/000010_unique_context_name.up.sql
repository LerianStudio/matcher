-- Add UNIQUE constraint on reconciliation context name within each tenant schema.
-- This provides database-level defense-in-depth for the application-level uniqueness
-- check in CreateContext/UpdateContext. Prevents TOCTOU race conditions where two
-- concurrent requests could both pass the FindByName check and create duplicates.
--
-- The constraint is scoped to the tenant schema (each tenant has its own schema
-- with its own reconciliation_contexts table, so names are unique per-tenant).
CREATE UNIQUE INDEX IF NOT EXISTS idx_reconciliation_contexts_name_unique
    ON reconciliation_contexts (name);
