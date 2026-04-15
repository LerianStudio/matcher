ALTER TABLE fetcher_connections
    DROP COLUMN IF EXISTS schema,
    DROP COLUMN IF EXISTS user_name;
