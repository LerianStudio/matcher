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

1. Scale write traffic down according to your operational policy.
2. Run migrations from the release artifact/environment:

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
- `000017_add_source_side_to_reconciliation_sources.up.sql` refuses to run while `reconciliation_sources` already contains rows without explicit `LEFT`/`RIGHT` side assignments.
- `000018_drop_legacy_source_fee_schedule.up.sql` removes the legacy `fee_schedule_id` column after the cutover is complete.

### Why the Cutover Is Strict

The new matching model depends on explicit source sides. Automatically assigning `LEFT` or `RIGHT` to existing rows would silently invent matching behavior, which is riskier than blocking the migration.

### Recommended Path for Internal Environments

If the environment can be recreated, reset the data and rerun migrations from scratch.

If the environment must be preserved, backfill it explicitly before running the migrations:

```sql
-- Inspect legacy source-level fee schedule bindings.
SELECT id, context_id, name, fee_schedule_id
FROM reconciliation_sources
WHERE fee_schedule_id IS NOT NULL;

-- Inspect sources that still need explicit LEFT/RIGHT assignment.
SELECT id, context_id, name
FROM reconciliation_sources;
```

Then either:

1. reset the environment, or
2. manually assign `LEFT`/`RIGHT` according to the intended matching topology and clear legacy `fee_schedule_id` bindings before applying the cutover migrations.

### Rollback Expectations

- Rolling back `000017` drops the `side` column and loses source-side assignments.
- Rolling back `000018` restores only the `fee_schedule_id` column shape; it does not reconstruct old values.
- Treat these migrations as a schema rollback path, not a data restoration path.

## Notes

- Never manually modify migration history tables in production unless incident command explicitly approves and records it.
- Keep migrations backward-compatible with running application versions whenever possible.
- Prefer additive schema changes first, then cleanup/removal in later releases.
