-- Phase 1 of source-side cutover (additive only):
-- Adds the nullable side column so existing environments can backfill
-- explicit LEFT/RIGHT assignments before enforcement in 000018.
ALTER TABLE reconciliation_sources
    ADD COLUMN side TEXT;

-- Composite lookup path: find all sources for a context filtered by side (LEFT/RIGHT).
-- Used during match execution to partition transactions by reconciliation side.
CREATE INDEX idx_reconciliation_sources_context_side
    ON reconciliation_sources (context_id, side);
