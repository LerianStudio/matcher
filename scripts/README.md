# scripts

Utility scripts for development, CI, and database operations.

## Scripts

| Script | Description |
|--------|-------------|
| `check-tests.sh` | Verifies every `.go` file has a corresponding `_test.go` file |
| `check-test-tags.sh` | Validates test files have proper build tags (`unit`, `integration`, `e2e`) |
| `manual-indexes.sql` | Additional database indexes for performance tuning |
| `post-deploy-audit-logs-append-only.sql` | Post-deployment script enforcing append-only audit log constraints |

### postgres/

PostgreSQL-specific utility scripts.

## Usage

Most scripts are invoked through Makefile targets:

```bash
make check-tests      # Runs check-tests.sh
make check-test-tags  # Runs check-test-tags.sh
```
