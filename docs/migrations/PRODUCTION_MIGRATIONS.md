# Production Migration Runbook

This runbook defines the minimum safe procedure for applying Matcher database migrations in production.

## Preconditions

- Confirm the target release commit/tag and migration files to apply.
- Confirm a tested rollback path (`*.down.sql`) exists for every new migration.
- Confirm recent backups/snapshots exist for the target PostgreSQL cluster.
- Confirm application error budget and maintenance window approval.

## Dry-Run Validation (Staging)

1. Deploy the exact production candidate image to staging.
2. Run:

```bash
make migrate-up
```

3. Execute smoke tests for:
   - Context/source/rule CRUD
   - Match run trigger path
   - Exception creation flow
4. Validate no migration remains dirty.

## Production Apply

1. Check table sizes for affected tables:

```sql
SELECT 
    schemaname,
    relname AS table_name,
    n_live_tup AS row_count
FROM pg_stat_user_tables
WHERE relname IN ('exceptions', 'outbox_events', 'reconciliation_sources', 'fee_rules')
ORDER BY n_live_tup DESC;
```

2. Scale write traffic down according to your operational policy.
3. Run migrations from the release artifact/environment:

```bash
make migrate-up
```

3. Verify service health and key APIs:
   - `GET /health`
   - `GET /ready`
4. Re-enable normal traffic.

## Rollback Procedure

If post-migration validation fails and rollback is required:

1. Pause new write traffic.
2. Roll back one step (or the required number of steps) in a controlled sequence:

```bash
make migrate-down
```

3. Re-run smoke checks and confirm readiness.
4. If rollback cannot restore service integrity, restore from backup and execute incident response.

## Index Creation Safety

Matcher's initial migrations use `CREATE INDEX` (not `CREATE INDEX CONCURRENTLY`).
On production tables with significant row counts, plain `CREATE INDEX` acquires a
`SHARE` lock that blocks writes for the duration of the index build.

**For large tables** (> 1M rows, or any table under sustained write load):

1. Before applying the migration, review `.up.sql` files for `CREATE INDEX` statements.
2. For each such statement on a large table, consider:
   - Running `CREATE INDEX CONCURRENTLY` manually _before_ applying the migration
   - Then converting the migration `CREATE INDEX` to `CREATE INDEX IF NOT EXISTS`
   - This avoids holding a write lock during the full index scan
3. Monitor `pg_stat_activity` for lock waits during migration apply.

Note: `CREATE INDEX CONCURRENTLY` cannot run inside a transaction. Run it as a
standalone statement outside the migration tool if needed.

## Fee Rule / Source Side Pre-Launch Cutover

The fee-rule feature introduces an intentional hard cutover for internal environments before public launch:

- `000016_fee_rules.up.sql` refuses to run while `reconciliation_sources.fee_schedule_id` still contains legacy bindings.
- `000017_add_source_side_to_reconciliation_sources.up.sql` adds the `side` column as nullable so existing rows are preserved.
- `000018_enforce_source_side_not_null.up.sql` enforces `NOT NULL` + `CHECK (side IN ('LEFT', 'RIGHT'))` — refuses to run while any source has a `NULL` side.
- `000019_drop_legacy_source_fee_schedule.up.sql` removes the legacy `fee_schedule_id` column after the cutover is complete.

### Why the Cutover Is Strict

The new matching model depends on explicit source sides. Automatically assigning `LEFT` or `RIGHT` to existing rows would silently invent matching behavior, which is riskier than blocking the migration.

### Recommended Path for Internal Environments

If the environment can be recreated, reset the data and rerun migrations from scratch.

If the environment must be preserved, follow the phased cutover:

```sql
-- Step 1: Clear legacy fee schedule bindings (before 000016).
UPDATE reconciliation_sources SET fee_schedule_id = NULL WHERE fee_schedule_id IS NOT NULL;

-- Step 2: Run 000016 (creates fee_rules table) and 000017 (adds nullable side column).

-- Step 3: Backfill explicit side assignments (between 000017 and 000018).
-- Inspect current sources:
SELECT id, context_id, name FROM reconciliation_sources WHERE side IS NULL;

-- Assign sides according to your intended matching topology:
UPDATE reconciliation_sources SET side = 'LEFT' WHERE name LIKE '%bank%';
UPDATE reconciliation_sources SET side = 'RIGHT' WHERE name LIKE '%gateway%';

-- Verify no NULL sides remain:
SELECT COUNT(*) FROM reconciliation_sources WHERE side IS NULL;  -- must be 0

-- Step 4: Run 000018 (enforces NOT NULL) and 000019 (drops legacy column).
```

### Rollback Expectations

- Rolling back `000018` removes the NOT NULL constraint; the `side` column becomes nullable again.
- Rolling back `000017` drops the `side` column entirely and loses source-side assignments.
- Rolling back `000019` restores only the `fee_schedule_id` column shape; it does not reconstruct old values.
- Treat these migrations as a schema rollback path, not a data restoration path.

## Notes

- Never manually modify migration history tables in production unless incident command explicitly approves and records it.
- Keep migrations backward-compatible with running application versions whenever possible.
- Prefer additive schema changes first, then cleanup/removal in later releases.
