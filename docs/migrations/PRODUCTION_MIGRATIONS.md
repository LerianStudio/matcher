# Production Migration Safety Guide

This guide documents safe practices for running database migrations in production environments with large tables.

## The CONCURRENTLY Problem

### Why We Can't Use CONCURRENTLY in Migrations

PostgreSQL's `CREATE INDEX CONCURRENTLY` is the safe way to create indexes on large tables without blocking writes. However, **golang-migrate runs all migrations inside a transaction block**, and PostgreSQL prohibits concurrent operations within transactions:

```
ERROR: CREATE INDEX CONCURRENTLY cannot run inside a transaction block
```

This limitation also affects:
- `DROP INDEX CONCURRENTLY`
- `REINDEX CONCURRENTLY`
- `ALTER TABLE ... ADD CONSTRAINT ... NOT VALID` followed by `VALIDATE CONSTRAINT` (supported with split migrations; see `golang-migrate Workaround` in `NOT VALID Constraints`)

### Impact of Non-Concurrent Index Creation

| Table Size | Approximate Lock Duration | Risk Level |
|------------|---------------------------|------------|
| < 10k rows | Milliseconds | Low |
| 10k - 100k rows | Seconds | Medium |
| 100k - 1M rows | Seconds to minutes | High |
| > 1M rows | Minutes to hours | Critical |

During this time, **all writes to the table are blocked**.

## Production Deployment Strategy

### Step 1: Identify Affected Migrations

Before deploying, review migrations for index creation or constraint validation:

```bash
grep -l "CREATE INDEX" migrations/*.up.sql
grep -l "ADD CONSTRAINT" migrations/*.up.sql
```

Current affected migrations (baseline):
- `000001_release_0_1_0.up.sql` - `idx_exceptions_reason`
- `000001_release_0_1_0.up.sql` - `idx_exceptions_resolution_type`
- `000001_release_0_1_0.up.sql` - `idx_outbox_events_failed_retry`

### Step 2: Check Table Sizes

```sql
SELECT 
    schemaname,
    relname AS table_name,
    n_live_tup AS row_count
FROM pg_stat_user_tables
WHERE relname IN ('exceptions', 'outbox_events', 'reconciliation_sources', 'fee_rules')
ORDER BY n_live_tup DESC;
```

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

### Step 3: Manual Index Creation (for Large Tables)

If tables have >100k rows, create indexes manually **before** running migrations:

#### For exceptions table (baseline 000001_release_0_1_0):

```sql
-- Create index concurrently (outside any transaction)
CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_exceptions_reason ON exceptions(reason);

-- Verify index was created successfully
SELECT indexname, indexdef 
FROM pg_indexes 
WHERE tablename = 'exceptions' AND indexname = 'idx_exceptions_reason';
```

#### For outbox_events table (baseline 000001_release_0_1_0):

```sql
-- Create partial index concurrently
CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_outbox_events_failed_retry 
    ON outbox_events(status, updated_at, attempts) 
    WHERE status = 'FAILED';

-- Verify
SELECT indexname, indexdef 
FROM pg_indexes 
WHERE tablename = 'outbox_events' AND indexname = 'idx_outbox_events_failed_retry';
```

### Step 4: Run Migrations

After manual index creation, run migrations normally. The `IF NOT EXISTS` clause ensures migrations won't fail if indexes already exist:

```bash
make migrate-up
```

## NOT VALID Constraints

### When to Use NOT VALID

Use `NOT VALID` when adding constraints to large tables:

```sql
-- Add constraint without validating existing rows (fast, no lock)
ALTER TABLE large_table 
    ADD CONSTRAINT fk_example 
    FOREIGN KEY (column_id) REFERENCES other_table(id) 
    NOT VALID;

-- Validate in a separate transaction (can be done later, lower lock)
ALTER TABLE large_table VALIDATE CONSTRAINT fk_example;
```

### Benefits

1. **Initial ADD**: Takes ShareUpdateExclusiveLock (allows reads and writes)
2. **VALIDATE**: Takes ShareUpdateExclusiveLock but can run concurrently with most operations
   VALIDATE still performs a full-table scan to verify existing rows, so even with a ShareUpdateExclusiveLock it can be I/O and CPU intensive on large tables.
   
   **Scheduling Guidance**: Run `VALIDATE CONSTRAINT` during off-peak hours or scheduled maintenance windows. Always test the operation on staging or a read replica first to estimate duration and resource impact. For partitioned tables, validate per-partition when possible, or batch validation across smaller table subsets to reduce I/O and CPU pressure on production systems.
3. **New rows**: Validated immediately after ADD, even before VALIDATE

### golang-migrate Workaround

Since both operations must run in separate transactions, split into two migration files:

```sql
-- 000015_add_constraint.up.sql
ALTER TABLE orders 
    ADD CONSTRAINT fk_customer 
    FOREIGN KEY (customer_id) REFERENCES customers(id) 
    NOT VALID;

-- 000016_validate_constraint.up.sql  
ALTER TABLE orders VALIDATE CONSTRAINT fk_customer;
```

## Rollback Considerations

### Dropping Indexes Safely

Similar to creation, `DROP INDEX CONCURRENTLY` cannot run in a transaction:

```sql
-- Manual rollback for large tables
DROP INDEX CONCURRENTLY IF EXISTS idx_exceptions_reason;
DROP INDEX CONCURRENTLY IF EXISTS idx_outbox_events_failed_retry;
```

### Migration Rollback Commands

```bash
# Rollback last migration
make migrate-down

# Rollback specific version (if supported)
migrate -path migrations -database "$DATABASE_URL" goto VERSION
```

## Monitoring During Migrations

### Check for Blocking Queries

```sql
SELECT 
    blocked.pid AS blocked_pid,
    blocked.query AS blocked_query,
    blocking.pid AS blocking_pid,
    blocking.query AS blocking_query,
    now() - blocked.query_start AS waiting_time
FROM pg_stat_activity blocked
JOIN pg_locks blocked_locks ON blocked.pid = blocked_locks.pid
JOIN pg_locks blocking_locks ON blocked_locks.locktype = blocking_locks.locktype
    AND blocked_locks.relation = blocking_locks.relation
    AND blocked_locks.pid != blocking_locks.pid
JOIN pg_stat_activity blocking ON blocking_locks.pid = blocking.pid
WHERE NOT blocked_locks.granted;
```

Shorter alternative for PostgreSQL 9.6+: query `pg_stat_activity` and use `pg_blocking_pids(pid)` with `cardinality` to list blocked PIDs, their blockers, and the query.

```sql
SELECT
    pid,
    pg_blocking_pids(pid) AS blocking_pids,
    query
FROM pg_stat_activity
WHERE cardinality(pg_blocking_pids(pid)) > 0;
```

**When to use each approach:** The simplified query using `pg_blocking_pids`, `pg_stat_activity`, and `cardinality` (requires PostgreSQL 9.6+) is ideal for quickly listing blocked PIDs and their blockers during routine monitoring. Use the detailed query above when you need lock types, relation/object information, waiting duration, or deeper diagnostics to understand *why* a lock is held.

### Monitor Index Creation Progress (PostgreSQL 12+)

```sql
SELECT 
    a.pid,
    a.query,
    p.phase,
    p.blocks_total,
    p.blocks_done,
    round(100.0 * p.blocks_done / nullif(p.blocks_total, 0), 1) AS "% done"
FROM pg_stat_progress_create_index p
JOIN pg_stat_activity a ON p.pid = a.pid;
```

## Checklist for Production Deployments

- [ ] Review all new migrations for index creation
- [ ] Check target table sizes in production
- [ ] For tables >100k rows, create indexes manually with CONCURRENTLY
- [ ] Verify indexes exist before running migrations
- [ ] Schedule deployment during low-traffic period
- [ ] Have rollback commands ready
- [ ] Monitor for blocking queries during deployment

## References

- [PostgreSQL CREATE INDEX CONCURRENTLY](https://www.postgresql.org/docs/current/sql-createindex.html#SQL-CREATEINDEX-CONCURRENTLY)
- [golang-migrate Transactions](https://github.com/golang-migrate/migrate#transactions)
- [PostgreSQL Lock Monitoring](https://wiki.postgresql.org/wiki/Lock_Monitoring)
