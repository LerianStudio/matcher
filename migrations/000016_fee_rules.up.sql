-- Fee rules: per-transaction fee schedule resolution based on field predicates.
-- Attached to reconciliation context with a side indicator (LEFT/RIGHT/ANY).
CREATE TABLE fee_rules (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    context_id UUID NOT NULL REFERENCES reconciliation_contexts(id) ON DELETE CASCADE,
    side VARCHAR(10) NOT NULL CHECK (side IN ('LEFT', 'RIGHT', 'ANY')),
    fee_schedule_id UUID NOT NULL,
    name VARCHAR(100) NOT NULL,
    priority INTEGER NOT NULL,
    predicates JSONB NOT NULL DEFAULT '[]'::jsonb,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    CONSTRAINT fk_fee_rules_fee_schedule FOREIGN KEY (fee_schedule_id) REFERENCES fee_schedules(id) ON DELETE RESTRICT,
    CONSTRAINT uq_fee_rules_context_priority UNIQUE (context_id, priority),
    CONSTRAINT uq_fee_rules_context_name UNIQUE (context_id, name)
);

-- Primary lookup path: all rules for a context, ordered by priority.
CREATE INDEX idx_fee_rules_context ON fee_rules(context_id);

-- Referential integrity check path: which rules reference a given schedule.
CREATE INDEX idx_fee_rules_schedule ON fee_rules(fee_schedule_id);

-- Keep the legacy source-level fee schedule attachment during rollout.
-- Cleanup happens in a later contract migration after cutover is validated.
