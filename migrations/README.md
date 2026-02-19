# migrations

PostgreSQL schema migrations managed with [golang-migrate](https://github.com/golang-migrate/migrate).

## Naming Convention

Migrations use sequential versioning with descriptive names:

```
000001_init_schema.up.sql
000001_init_schema.down.sql
000002_fee_schedules.up.sql
000002_fee_schedules.down.sql
```

Every migration must have both `.up.sql` and `.down.sql` files.

## Commands

```bash
make migrate-up                          # Apply all pending migrations
make migrate-down                        # Rollback the last migration
make migrate-create NAME=add_feature     # Create a new migration pair
```

## Guidelines

- Always test both up and down migrations before merging.
- Add indexes for new foreign keys and filter columns.
- Never modify existing migration files after they have been applied.
- Audit tables are append-only: never add UPDATE or DELETE operations.
