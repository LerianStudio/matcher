-- Restore external_system_type enum and revert column type.
-- Rollback must fail if rows contain values not representable in the original enum.
-- Silently coercing them would corrupt historical provenance.

DO $$
BEGIN
    IF EXISTS (
        SELECT 1
        FROM exceptions
        WHERE external_system IS NOT NULL
          AND external_system NOT IN ('JIRA', 'SERVICENOW', 'WEBHOOK')
    ) THEN
        RAISE EXCEPTION
            'cannot rollback 000021_external_system_to_varchar: unsupported external_system values exist';
    END IF;
END $$;

CREATE TYPE external_system_type AS ENUM ('JIRA', 'SERVICENOW', 'WEBHOOK');

ALTER TABLE exceptions
    ALTER COLUMN external_system TYPE external_system_type
    USING external_system::external_system_type;
