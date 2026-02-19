-- PostgreSQL does not support removing values from an existing ENUM type
-- with a simple ALTER TYPE ... DROP VALUE. To roll back, we must:
-- 0. Record which rows were REVOKED (audit preservation).
-- 1. Update any rows that use 'REVOKED' back to 'REJECTED'.
-- 2. Recreate the enum without the 'REVOKED' value.
-- 3. Re-apply it to the column.

-- Step 0: Preserve audit trail of REVOKED rows before status rollback.
-- REVOKED is semantically distinct from REJECTED (confirmed-then-undone vs never-confirmed).
-- This table records the original REVOKED state so the intent is recoverable after rollback.
CREATE TABLE IF NOT EXISTS match_group_status_rollback_audit (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    match_group_id UUID NOT NULL,
    context_id UUID NOT NULL,
    run_id UUID NOT NULL,
    confirmed_at TIMESTAMP WITH TIME ZONE,
    previous_status TEXT NOT NULL DEFAULT 'REVOKED',
    rolled_back_to TEXT NOT NULL DEFAULT 'REJECTED',
    rollback_migration VARCHAR(100) NOT NULL DEFAULT '000008_match_group_status_revoked',
    created_at TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT NOW()
);

COMMENT ON TABLE match_group_status_rollback_audit IS
    'Preserves match groups that held REVOKED status before migration 000008 rollback mapped them to REJECTED';

CREATE INDEX IF NOT EXISTS idx_mg_rollback_audit_group
    ON match_group_status_rollback_audit(match_group_id);

INSERT INTO match_group_status_rollback_audit
    (match_group_id, context_id, run_id, confirmed_at, previous_status, rolled_back_to)
SELECT id, context_id, run_id, confirmed_at, status::TEXT, 'REJECTED'
FROM match_groups
WHERE status = 'REVOKED';

-- Step 1: Migrate existing REVOKED rows to REJECTED.
UPDATE match_groups SET status = 'REJECTED' WHERE status = 'REVOKED';

-- Step 2: Recreate the type without REVOKED.
ALTER TABLE match_groups ALTER COLUMN status TYPE TEXT;
DROP TYPE IF EXISTS match_group_status;
CREATE TYPE match_group_status AS ENUM ('PROPOSED', 'CONFIRMED', 'REJECTED');
ALTER TABLE match_groups ALTER COLUMN status TYPE match_group_status USING status::match_group_status;
