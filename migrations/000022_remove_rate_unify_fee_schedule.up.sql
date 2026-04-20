-- Remove legacy Rate entity and unify fee system on FeeRule/FeeSchedule.
-- Fee verification now uses per-transaction FeeSchedule resolution via predicates
-- instead of a single context-wide Rate.
--
-- Coordinated cutover note:
-- This migration is intentionally fail-fast. It refuses to apply while any legacy
-- rate-backed contexts or legacy fee variance rows still exist, because dropping
-- the old schema before that cutover would destroy data and behavior that cannot
-- be reconstructed safely.

SELECT CASE
    WHEN EXISTS (
        SELECT 1
        FROM reconciliation_contexts
        WHERE rate_id IS NOT NULL
    ) THEN current_setting(
        'migration_000022_blocked_backfill_legacy_context_rates_before_cutover'
    )
    ELSE 'ok'
END;

SELECT CASE
    WHEN EXISTS (
        SELECT 1
        FROM match_fee_variances
        WHERE fee_schedule_id IS NULL
    ) THEN current_setting(
        'migration_000022_blocked_archive_or_backfill_legacy_fee_variances_before_cutover'
    )
    ELSE 'ok'
END;

-- 1. Drop FK index and column from reconciliation_contexts
DROP INDEX IF EXISTS idx_contexts_rate;
ALTER TABLE reconciliation_contexts DROP COLUMN IF EXISTS rate_id;

-- 2. Migrate match_fee_variances: drop rate_id, enforce fee_schedule_id
--    (fee_schedule_id was added as nullable in migration 000002)
ALTER TABLE match_fee_variances ADD COLUMN IF NOT EXISTS fee_schedule_name_snapshot TEXT;

UPDATE match_fee_variances AS fv
SET fee_schedule_name_snapshot = fs.name
FROM fee_schedules AS fs
WHERE fv.fee_schedule_id = fs.id
  AND fv.fee_schedule_name_snapshot IS NULL;

ALTER TABLE match_fee_variances DROP COLUMN IF EXISTS rate_id;

ALTER TABLE match_fee_variances ALTER COLUMN fee_schedule_id SET NOT NULL;
ALTER TABLE match_fee_variances ALTER COLUMN fee_schedule_name_snapshot SET NOT NULL;

-- 2b. Index fee_schedule_id for variance report JOIN performance.
CREATE INDEX IF NOT EXISTS idx_fee_variances_schedule ON match_fee_variances(fee_schedule_id);

-- 3. Drop the rates table and its index (no longer referenced)
DROP INDEX IF EXISTS idx_rates_tenant;
DROP TABLE IF EXISTS rates;
