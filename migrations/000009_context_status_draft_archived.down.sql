-- PostgreSQL does not support removing values from an existing ENUM type
-- with a simple ALTER TYPE ... DROP VALUE. To roll back, we must:
-- 0. Record which rows used DRAFT or ARCHIVED (audit preservation).
-- 1. Update any rows that use 'DRAFT' to 'ACTIVE' and 'ARCHIVED' to 'PAUSED'.
-- 2. Recreate the enum without DRAFT and ARCHIVED values.
-- 3. Re-apply it to the column.

-- Step 0: Preserve audit trail of DRAFT/ARCHIVED rows before status rollback.
CREATE TABLE IF NOT EXISTS context_status_rollback_audit (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    context_id UUID NOT NULL,
    tenant_id UUID NOT NULL,
    previous_status TEXT NOT NULL,
    rolled_back_to TEXT NOT NULL,
    rollback_migration VARCHAR(100) NOT NULL DEFAULT '000009_context_status_draft_archived',
    created_at TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT NOW()
);

COMMENT ON TABLE context_status_rollback_audit IS
    'Preserves contexts that held DRAFT or ARCHIVED status before migration 000009 rollback mapped them to ACTIVE or PAUSED';

CREATE INDEX IF NOT EXISTS idx_ctx_rollback_audit_context
    ON context_status_rollback_audit(context_id);

INSERT INTO context_status_rollback_audit
    (context_id, tenant_id, previous_status, rolled_back_to)
SELECT id, tenant_id, status::TEXT, CASE WHEN status = 'DRAFT' THEN 'ACTIVE' ELSE 'PAUSED' END
FROM reconciliation_contexts
WHERE status IN ('DRAFT', 'ARCHIVED');

-- Step 1: Migrate existing DRAFT rows to ACTIVE and ARCHIVED rows to PAUSED.
UPDATE reconciliation_contexts SET status = 'ACTIVE' WHERE status = 'DRAFT';
UPDATE reconciliation_contexts SET status = 'PAUSED' WHERE status = 'ARCHIVED';

-- Step 2: Restore default to ACTIVE before recreating the type.
ALTER TABLE reconciliation_contexts ALTER COLUMN status SET DEFAULT 'ACTIVE';

-- Step 3: Recreate the type without DRAFT and ARCHIVED.
ALTER TABLE reconciliation_contexts ALTER COLUMN status TYPE TEXT;
DROP TYPE IF EXISTS reconciliation_context_status;
CREATE TYPE reconciliation_context_status AS ENUM ('ACTIVE', 'PAUSED');
ALTER TABLE reconciliation_contexts ALTER COLUMN status TYPE reconciliation_context_status USING status::reconciliation_context_status;
