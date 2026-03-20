SELECT CASE
    WHEN EXISTS (SELECT 1 FROM reconciliation_sources) THEN current_setting(
        'strict_source_side_cutover_blocked_reset_or_migrate_existing_sources_before_migration_000017'
    )
    ELSE 'ok'
END;

ALTER TABLE reconciliation_sources
    ADD COLUMN side TEXT NOT NULL,
    ADD CONSTRAINT chk_reconciliation_sources_side
        CHECK (side IN ('LEFT', 'RIGHT'));

CREATE INDEX idx_reconciliation_sources_context_side
    ON reconciliation_sources (context_id, side);
