# tests

Integration and end-to-end test suites for the Matcher service.

## Structure

| Directory | Build Tag | Description |
|-----------|-----------|-------------|
| [integration/](integration/) | `integration` | Tests against real infrastructure (PostgreSQL, Redis, RabbitMQ) using testcontainers |
| [e2e/](e2e/) | `e2e` | Full-stack journey tests exercising the complete API |

Unit tests are co-located with source files throughout the codebase (`*_test.go` with `//go:build unit` tag).

## Commands

```bash
make test             # Unit tests only
make test-int         # Integration tests (requires Docker)
make test-e2e         # End-to-end tests (requires full stack)
make test-e2e-fast    # Fast E2E tests (short mode, 5m timeout)
make test-all         # All tests with merged coverage
```

## Writing Tests

- Every `.go` file must have a corresponding `_test.go` (enforced by `make check-tests`).
- Test files must include a build tag: `//go:build unit`, `//go:build integration`, or `//go:build e2e`.
- Use testify for assertions and sqlmock for database unit tests.
- Use testcontainers for integration tests requiring real infrastructure.
