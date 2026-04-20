-- Add schema and user_name columns to fetcher_connections table.
-- Stores the logical schema (e.g., "public", "tenant_schema") and username used
-- when Fetcher connects to the source database. Required for proper schema
-- qualification during extraction job submission.

ALTER TABLE fetcher_connections
    ADD COLUMN schema    TEXT NOT NULL DEFAULT '',
    ADD COLUMN user_name TEXT NOT NULL DEFAULT '';
