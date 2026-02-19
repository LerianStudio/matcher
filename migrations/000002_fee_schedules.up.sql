-- Fee Schedules
CREATE TABLE fee_schedules (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    tenant_id UUID NOT NULL,
    name VARCHAR(100) NOT NULL,
    currency VARCHAR(3) NOT NULL,
    application_order TEXT NOT NULL DEFAULT 'PARALLEL',
    rounding_scale INTEGER NOT NULL DEFAULT 2,
    rounding_mode TEXT NOT NULL DEFAULT 'HALF_UP',
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    CONSTRAINT uq_fee_schedules_tenant_name UNIQUE (tenant_id, name)
);
CREATE INDEX idx_fee_schedules_tenant ON fee_schedules(tenant_id);

-- Fee Schedule Items
CREATE TABLE fee_schedule_items (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    fee_schedule_id UUID NOT NULL REFERENCES fee_schedules(id) ON DELETE CASCADE,
    name VARCHAR(100) NOT NULL,
    priority INTEGER NOT NULL DEFAULT 0,
    structure_type TEXT NOT NULL,
    structure_data JSONB NOT NULL DEFAULT '{}'::jsonb,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    CONSTRAINT uq_fee_schedule_item_priority UNIQUE (fee_schedule_id, priority)
);
CREATE INDEX idx_fee_schedule_items_schedule ON fee_schedule_items(fee_schedule_id);

-- Per-source fee schedule reference
ALTER TABLE reconciliation_sources
    ADD COLUMN fee_schedule_id UUID REFERENCES fee_schedules(id);

-- Context-level normalization mode
ALTER TABLE reconciliation_contexts
    ADD COLUMN fee_normalization TEXT;

-- Fee variance tracking extension
ALTER TABLE match_fee_variances
    ADD COLUMN fee_schedule_id UUID REFERENCES fee_schedules(id),
    ADD COLUMN fee_schedule_item_id UUID;
