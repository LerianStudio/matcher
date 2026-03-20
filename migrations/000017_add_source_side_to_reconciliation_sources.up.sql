ALTER TABLE reconciliation_sources
    ADD COLUMN side TEXT,
    ADD CONSTRAINT chk_reconciliation_sources_side
        CHECK (side IS NULL OR side IN ('LEFT', 'RIGHT'));

CREATE INDEX idx_reconciliation_sources_context_side
    ON reconciliation_sources (context_id, side);
