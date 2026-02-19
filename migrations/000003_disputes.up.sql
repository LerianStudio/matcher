-- Disputes table: tracks dispute lifecycle for exceptions
CREATE TABLE disputes (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    exception_id UUID NOT NULL REFERENCES exceptions(id) ON DELETE RESTRICT,
    category TEXT NOT NULL,
    state TEXT NOT NULL DEFAULT 'DRAFT',
    description TEXT NOT NULL,
    opened_by VARCHAR(255) NOT NULL,
    resolution TEXT,
    reopen_reason TEXT,
    evidence JSONB NOT NULL DEFAULT '[]'::jsonb,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    CONSTRAINT chk_disputes_category CHECK (category IN ('BANK_FEE_ERROR', 'UNRECOGNIZED_CHARGE', 'DUPLICATE_TRANSACTION', 'AMOUNT_MISMATCH', 'OTHER')),
    CONSTRAINT chk_disputes_state CHECK (state IN ('DRAFT', 'OPEN', 'PENDING_EVIDENCE', 'WON', 'LOST'))
);

CREATE INDEX idx_disputes_exception_id ON disputes(exception_id);
CREATE INDEX idx_disputes_state ON disputes(state);
CREATE INDEX idx_disputes_created_at ON disputes(created_at);

COMMENT ON TABLE disputes IS 'Dispute lifecycle tracking linked to exceptions';
COMMENT ON COLUMN disputes.category IS 'Dispute category: BANK_FEE_ERROR, UNRECOGNIZED_CHARGE, DUPLICATE_TRANSACTION, AMOUNT_MISMATCH, OTHER';
COMMENT ON COLUMN disputes.state IS 'Dispute state machine: DRAFT -> OPEN -> PENDING_EVIDENCE -> WON/LOST (LOST can reopen to OPEN)';
COMMENT ON COLUMN disputes.evidence IS 'JSON array of evidence objects with id, dispute_id, file_url, comment, submitted_by, submitted_at';
COMMENT ON COLUMN disputes.resolution IS 'Resolution description when dispute is won or lost';
COMMENT ON COLUMN disputes.reopen_reason IS 'Reason provided when reopening a lost dispute';
