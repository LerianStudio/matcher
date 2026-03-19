-- Fee rules: per-transaction fee schedule resolution based on field predicates.
-- Attached to reconciliation context with a side indicator (LEFT/RIGHT/ANY).
CREATE TABLE fee_rules (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    context_id UUID NOT NULL REFERENCES reconciliation_contexts(id) ON DELETE CASCADE,
    side VARCHAR(10) NOT NULL CHECK (side IN ('LEFT', 'RIGHT', 'ANY')),
    fee_schedule_id UUID NOT NULL REFERENCES fee_schedules(id) ON DELETE RESTRICT,
    name VARCHAR(100) NOT NULL,
    priority INTEGER NOT NULL DEFAULT 0,
    predicates JSONB NOT NULL DEFAULT '[]'::jsonb,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    CONSTRAINT uq_fee_rules_context_priority UNIQUE (context_id, priority),
    CONSTRAINT uq_fee_rules_context_name UNIQUE (context_id, name)
);

-- Primary lookup path: all rules for a context, ordered by priority.
CREATE INDEX idx_fee_rules_context ON fee_rules(context_id);

-- Referential integrity check path: which rules reference a given schedule.
CREATE INDEX idx_fee_rules_schedule ON fee_rules(fee_schedule_id);

-- Remove source-level fee schedule attachment (replaced by context-level fee rules).
ALTER TABLE reconciliation_sources DROP COLUMN IF EXISTS fee_schedule_id;
