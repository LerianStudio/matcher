-- Convert external_system column from enum to VARCHAR(255) to support
-- arbitrary external system names in callback processing.
-- The previous enum ('JIRA', 'SERVICENOW', 'WEBHOOK') did not include
-- 'MANUAL' and other valid routing targets, causing UPDATE failures
-- when callbacks set external_system to an unlisted value.

ALTER TABLE exceptions
    ALTER COLUMN external_system TYPE VARCHAR(255)
    USING external_system::TEXT;

DROP TYPE IF EXISTS external_system_type;
