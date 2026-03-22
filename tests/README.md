# tests

Integration, end-to-end, chaos, and static analysis test suites for the Matcher service.

## Structure

| Directory | Build Tag | Description |
|-----------|-----------|-------------|
| [integration/](integration/) | `integration` | Tests against real infrastructure (PostgreSQL, Redis, RabbitMQ) using testcontainers |
| [e2e/](e2e/) | `e2e` | Full-stack journey tests exercising the complete API (36+ journey tests) |
| [chaos/](chaos/) | `chaos` | Fault-injection tests using Toxiproxy for resilience validation |
| [static/](static/) | `unit` | Static analysis tests (goroutine leak detection) |

Unit tests are co-located with source files throughout the codebase (`*_test.go` with `//go:build unit` tag).

## Commands

```bash
make test               # Unit tests only
make test-int           # Integration tests (requires Docker)
make test-e2e           # End-to-end tests (requires full stack)
make test-e2e-fast      # Fast E2E tests (short mode, 5m timeout)
make test-e2e-journeys  # Journey-based E2E tests only
make test-chaos         # Chaos/fault-injection tests (requires Toxiproxy)
make test-all           # All tests with merged coverage
```

## Test Categories

### Integration Tests
Tests against real infrastructure using testcontainers. Covers cross-domain flows, RabbitMQ messaging, exception lifecycle (disputes, comments, bulk ops, SLA, callbacks), ingestion flows (repository, search, preview, streaming, dedup), and governance (archives, audit queries, exports).

### E2E Journey Tests
Full-stack journey tests that exercise the complete API. Includes reconciliation flows, manual/force matching, exception handling, dispute lifecycle, fee schedules/rules, pagination, audit, exports, large volume, concurrent operations, error handling, multi-tenant isolation, and more. Also includes an AI-powered fuzz testing oracle using Claude for property-based validation.

### Chaos Tests
Fault-injection tests using Toxiproxy to verify graceful degradation under: PostgreSQL failures, Redis failures, RabbitMQ failures, outbox resilience, lifecycle disruption, idempotency under chaos, business logic under failure, and resource exhaustion.

## Writing Tests

- Every `.go` file must have a corresponding `_test.go` (enforced by `make check-tests`).
- Test files must include a build tag: `//go:build unit`, `//go:build integration`, `//go:build e2e`, or `//go:build chaos`.
- Use testify for assertions and sqlmock for database unit tests.
- Use testcontainers for integration tests requiring real infrastructure.
