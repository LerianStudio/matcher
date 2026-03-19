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

## Notes

- Never manually modify migration history tables in production unless incident command explicitly approves and records it.
- Keep migrations backward-compatible with running application versions whenever possible.
- Prefer additive schema changes first, then cleanup/removal in later releases.
