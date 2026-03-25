# Project Rules

This project follows Lerian Studio Ring standards for Go services.

## 1. Architecture

- Modular monolith with bounded contexts under `internal/{context}` (configuration, discovery, ingestion, matching, exception, governance, reporting, outbox).
- Hexagonal Architecture per context:
  - `adapters/`: infrastructure and transports (HTTP, DB, MQ).
  - `domain/`: pure business logic and entities.
  - `ports/`: interfaces for external dependencies.
  - `services/`: application use cases (command/query).
- CQRS separation: write in `services/command/`, read in `services/query/`.
- Command use-case files end with `_commands.go`; query files end with `_queries.go`. Helper files with private methods may use descriptive names without the suffix.
- Domain entities remain pure logic (no logging/tracing). Enforced by `domain-no-logging` depguard rule.
- Keep domain models rich: enforce invariants in entities/value objects, not in adapters.
- Entities must expose state transitions via methods; avoid direct status mutation in services/adapters.
- Constructors validate invariants and return `(*T, error)` when creation can fail.
- Use value objects/enums with `Valid`/`IsValid` + parse helpers for critical types.
- Defensive copy caller-owned maps/slices before storing in entities.
- Identity fields (`ID`, `ContextID`, `TenantID`, `SourceID`) are immutable after creation.
- Domain invariants use `pkg/assert`; validation tags are only for inbound DTOs.
- **Nil checks vs asserters**: Use simple `if x == nil` for nil receiver checks, dependency injection (with sentinel errors), and adapter layer. Use `pkg/assert` for domain entity invariant validation and business rule validation with structured context.
- Avoid cross-context adapter imports; depend on ports instead.
- Avoid panics in all production paths.
- Only change infra/config (Docker, compose, env templates) in explicit DevOps tasks.

### Shared Kernel

- `internal/shared/` is the designated bridge between bounded contexts. Types needed by multiple contexts live here. Cross-context imports are blocked by depguard.
- **Interface location convention**:
  - `domain/repositories/` for a context's own aggregate store interfaces.
  - `ports/` for external dependency abstractions (EventPublisher, ObjectStorage, CacheProvider).
  - `internal/shared/ports/` for cross-context abstractions (OutboxRepository, AuditRepository, InfrastructureProvider, MatchTrigger).
- **Domain subdirectory variations**: `domain/value_objects/` (configuration, exception, ingestion, matching), `domain/enums/` (matching), `domain/errors/` (governance only).
- **Type-alias pattern**: When a type migrates to `shared/domain/`, the original package re-exports via type alias for backward compatibility.
- **Worker directories**: `services/worker/` for background jobs (configuration scheduler, governance archival, reporting export/cleanup, discovery poller/worker).
- **Outbox dispatcher exception**: Lives at `outbox/services/dispatcher.go` (services root, not in command/query/worker) because it is pure infrastructure.
- **Cross-context communication**: Via shared ports, outbox events, and cross adapters in `internal/shared/adapters/cross/`.

## 2. Required Libraries

- **AuthN/AuthZ**: `github.com/LerianStudio/lib-auth/v2` only.
- **Commons/Telemetry**: `github.com/LerianStudio/lib-commons/v4`.
- **Assertions**: `github.com/LerianStudio/lib-commons/v4/commons/assert` (no panics; referred to as `pkg/assert` in shorthand).
- **lib-commons submodules**:
  - Tracking/logging: `commons/log` (`libLog`), `commons/commons` (`libCommons.NewTrackingFromContext`).
  - OpenTelemetry: `commons/opentelemetry` (`libOpentelemetry`).
  - Database: `commons/postgres` (`libPostgres`).
  - Redis: `commons/redis` (`libRedis`).
  - Messaging: `commons/rabbitmq` (`libRabbitmq`).
  - Panic recovery: `commons/runtime` (`runtime.RecoverAndLogWithContext`, `runtime.SafeGoWithContextAndComponent`).
  - Runtime config: `commons/systemplane` (sole runtime configuration authority after bootstrap).
- **Key third-party**: `gofiber/fiber/v2` (HTTP), `Masterminds/squirrel` (SQL builder), `shopspring/decimal` (amounts), `google/uuid` (IDs).
- Do not introduce custom DB/Redis/MQ clients outside lib-commons wrappers.

## 3. Context + Observability

- Always use `libCommons.NewTrackingFromContext(ctx)` for logger/tracer/header data.
- Start a span per service method: `ctx, span := tracer.Start(ctx, "{context}.{operation}")` and `defer span.End()`.
- Service span naming: `{context}.{operation}` (e.g., `matching.run_match`, `configuration.create_context`).
- Handler span pattern: `ctx, span, logger := startHandlerSpan(fiberCtx, "handler.{context}.{operation}")` + `defer span.End()`.
- Error reporting: `libOpentelemetry.HandleSpanError(span, "message", err)` -- takes **value** not pointer.
- Business error events: `libOpentelemetry.HandleSpanBusinessErrorEvent(span, "message")` for non-critical domain errors.
- Error sanitization: `libLog.SafeError(logger, ctx, "msg", err, productionMode.Load())`.
- Structured logging: `logger.With(libLog.String("key", "val")).Log(ctx, level, "msg")`.
- Ensure adapters handle nil tracers/loggers gracefully (provide fallbacks) for testing contexts.
- Do not log inside domain entities/value objects.

## 4. HTTP Handler Patterns

- Framework: Fiber v2 (`gofiber/fiber/v2`).
- Handler constructor validates dependencies (nil checks with sentinel errors).
- Every handler starts with `ctx, span, logger := startHandlerSpan(fiberCtx, "handler.{context}.{operation}")` + `defer span.End()`.
- Body parsing: `libHTTP.ParseBodyAndValidate(fiberCtx, &payload)`.
- Context verification: `libHTTP.ParseAndVerifyTenantScopedID()` for path params with tenant ownership validation.
- Error-to-HTTP mapping: use `errors.Is(err, ErrSentinel)` to map domain errors to HTTP status codes.
- Response formatting: `libHTTP.Respond(fiberCtx, status, body)`, `libHTTP.RespondError(fiberCtx, status, title, message)`, `libHTTP.RespondStatus(fiberCtx, status)`.
- Swagger annotations required on all handlers (`@Summary`, `@Tags`, `@Param`, `@Success`, `@Failure`, `@Router`).
- Route registration uses `protected(resource, actions...)` higher-order function wrapping auth + tenant + idempotency + rate limiting.
- Production mode: `atomic.Bool` (`productionMode`) controls error detail exposure via `SafeError`.

## 5. Service Use Case Patterns

- One UseCase struct per bounded context (command and query are separate structs).
- Required dependencies validated in constructor with sentinel errors; optional deps via functional options (`UseCaseOption`).
- Method naming: domain-specific (e.g., `RunMatch()`, `ManualMatch()`, `CreateContext()`), NOT generic `Execute()`.
- Input structures: single struct per method (e.g., `RunMatchInput`, `AdjustEntryInput`).
- Every method starts with: `track := libCommons.NewTrackingFromContext(ctx)` + `ctx, span := track.Tracer.Start(ctx, "{context}.{operation}")` + `defer span.End()`.
- Put logic in entities when it only needs entity fields; use services for multi-aggregate or external dependency coordination.
- Keep services small and single-responsibility.
- Prefer explicit state (enums) over implicit derivation for critical domain status.

## 6. Repository Patterns

- Use `pgcommon.WithTenantTxProvider(ctx, provider, fn)` for new transactions with automatic tenant schema isolation.
- Use `pgcommon.WithTenantTxOrExistingProvider(ctx, provider, existingTx, fn)` for composable transactions (accepts optional caller-owned tx).
- Every write method must have a `WithTx` variant (enforced by custom linter `repositorytx`).
- Three-layer pattern: public `Create()` -> public `CreateWithTx()` -> private helper.
- Model / domain conversion: separate PostgreSQL model structs from domain entities, with `NewPostgreSQLModel()` and `ToEntity()` methods.
- Use `squirrel` for dynamic query building with `squirrel.Dollar` placeholder format.
- Cursor-based pagination via `pgcommon.ApplyIDCursorPagination()` with limit+1 pattern.
- `InfrastructureProvider` interface provides tenant-aware transaction and database access (`BeginTx`, `GetPrimaryDB`, `GetReplicaDB`, `GetRedisConnection`). Callers MUST release DB and Redis leases when finished.

## 7. Data + Multi-tenancy

- **CRITICAL**: Tenant info (`tenantID`, `tenantSlug`) must ONLY come from JWT claims via context.
  - NEVER accept tenant identifiers in request payloads, path params, query params, or custom headers.
  - Repository methods extract tenant from context via `auth.GetTenantID(ctx)`, never as function parameters.
  - This prevents tenant spoofing attacks.
- Apply schema via `auth.ApplyTenantSchema(ctx, tx)` inside transaction helpers.
- If tenant claims missing and auth disabled, run in single-tenant mode.
- Default tenant uses `public` schema (no UUID schema created). Background workers MUST include default tenant explicitly when enumerating tenants via `pg_namespace`.
- Read operations use replica connections with connection-scoped `SET search_path`.
- Transaction timeout: 30s default when context has no deadline.

## 8. Error Handling Patterns

- Sentinel errors defined at 5 locations:
  - `services/command/commands.go` -- use case sentinels.
  - `domain/entities/*.go` -- state transition errors.
  - `adapters/postgres/{name}/errors.go` -- repository sentinels.
  - `adapters/http/errors.go` or `handlers.go` -- HTTP-level errors.
  - `domain/errors/errors.go` -- governance only.
- All sentinels follow `Err[Category][Specific]` naming (e.g., `ErrMatchGroupMustBeProposedToConfirm`).
- Error wrapping: `fmt.Errorf("context: %w", err)` -- always `%w`, never `%v`. Enforced by forbidigo.
- Cross-context error re-export via type alias: `ErrTenantIDRequired = sharedDomain.ErrAuditTenantIDRequired`.
- No structured error types -- all `errors.New()` sentinels.

## 9. Worker Patterns

- Workers live in `services/worker/` (scheduler, archival, export, cleanup, discovery poller/worker).
- Ticker-based polling with configurable interval.
- Redis distributed lock (`SetNX` with TTL = 2x interval) prevents concurrent runs across instances.
- Graceful shutdown: `atomic.Bool` for running state, `sync.Once` for stop, channels for signal-based shutdown.
- Panic recovery: `defer runtime.RecoverAndLogWithContext(ctx, logger, component, name)` MUST be first defer (LIFO order matters).
- Re-entrant: `prepareRunState()` allows Start/Stop/Start cycles.
- Runtime config updates: only when stopped (`UpdateRuntimeConfig`).

## 10. Idempotency

- Middleware applied after auth + tenant extraction (needs tenant ID for key scoping).
- Key sources: explicit header (`X-Idempotency-Key` / `Idempotency-Key`) OR SHA-256 of request body.
- Key validation: max 128 chars, alphanumeric + colons/underscores/hyphens.
- State machine: PENDING -> COMPLETE (cached response replayed with `X-Idempotency-Replayed: true`) or FAILED (reacquirable).
- 409 Conflict when request in progress.

## 11. Redis Usage

- Distributed locking: `matcher:matchrun:lock:{contextID}` with Lua-verified release.
- Transaction deduplication: hash of `sourceID:externalID`, Redis SETNX with TTL (via `ingestion/adapters/redis/dedupe_service.go`).
- Idempotency cache: response caching for duplicate request detection.
- All keys tenant-scoped via `valkey.GetKeyFromContext()` (lib-commons tenant-manager).

## 12. RabbitMQ + Outbox Pattern

- All async communication via outbox pattern (no direct context-to-context messaging).
- Outbox dispatcher: polling interval configurable (~2s default), batch processing, max retry attempts.
- Per-tenant event processing with schema isolation.
- `ConfirmablePublisher`: broker confirmation + automatic channel recovery with exponential backoff.
- Dead-letter queue for failed messages.

## 13. Database

- PostgreSQL 17 with schema-per-tenant isolation.
- Repositories mirror patterns in `internal/configuration/adapters/postgres`.
- SQL queries must respect tenant isolation.
- Add indexes for join/filter keys in migrations.
- Keep migrations additive; avoid destructive changes in production.
- Enforce referential integrity with foreign keys where applicable.
- Avoid long-running transactions; keep write paths short and deterministic.
- Prefer read replicas for query services via `GetReplicaDB`.
- Migration validation: `make check-migrations` verifies pairs (up/down) and sequential numbering via `scripts/check-migrations.sh`.
- Migration naming: `000001_descriptive_name.up.sql` / `000001_descriptive_name.down.sql`.

## 14. Testing

### Build Tags

Build tags are the **authoritative** test type discriminator (required at top of file):

| Tag | Scope | External deps |
|-----|-------|---------------|
| `//go:build unit` | Unit tests | None (mocks only) |
| `//go:build integration` | Integration tests | Testcontainers |
| `//go:build e2e` | End-to-end tests | Full stack |
| `//go:build chaos` | Fault injection | Toxiproxy + containers |

### Frameworks + Helpers

- **Assertions**: `testify` (assert/require).
- **SQL mocking**: `DATA-DOG/go-sqlmock`.
- **Containers**: `testcontainers-go` with `wait.ForAll(ForLog, ForListeningPort)` for health.
- **Interface mocking**: `go.uber.org/mock` (gomock) for complex contracts; manual mocks for simple interfaces (5 or fewer methods).
- **Chaos**: Toxiproxy for fault injection (latency, reset, timeout, packet loss).
- **Test helpers**: `testutil.NewMockProviderFromDB()`, `testutil.NewClientWithResolver()`, `testutil.NewRedisClientWithMock()`.

### Patterns

- TDD (RED -> GREEN -> REFACTOR) required. Every commit should include tests.
- Integration pattern: `sync.Once` singleton containers per package, not per test.
- E2E pattern: fluent factory builders (e.g., `f.Context.NewContext().WithName("test").OneToOne().MustCreate(ctx)`), HTTP client per domain.
- Chaos pattern: Toxiproxy proxies per service (PG, Redis, RabbitMQ), fault injection methods.
- Coverage threshold: **70%** enforced in CI via shared workflow.
- `make check-tests` ensures every `.go` file has a corresponding `_test.go`.
- `make check-test-tags` verifies test files have proper build tags.
- No tests should rely on external services unless marked integration.
- Makefile unsets all matcher config env vars before test runs (`CLEAN_ENV`).

### Test File Naming

| Pattern | Purpose |
|---------|---------|
| `{name}_sqlmock_test.go` | SQL mock-based unit tests |
| `{name}_mock_test.go` | Other mock-based unit tests |
| `{name}.postgresql_test.go` | Postgres adapter tests (build tag discriminates unit vs integration) |
| `{name}_coverage_test.go` | Explicit coverage-focused tests |
| `{name}_coverage_sqlmock_test.go` | Coverage + sqlmock combined |

Do NOT merge test files when consolidating source files.

## 15. File Naming Conventions

### Postgres Adapter Files

Every aggregate-based postgres adapter directory uses Pattern A:

| File | Purpose |
|------|---------|
| `{name}.go` | Model structs, domain-to-DB conversions |
| `{name}.postgresql.go` | Repository implementation |
| `errors.go` | Adapter-specific sentinel errors |

**Flat layout exceptions**: `reporting/adapters/postgres/` (read-only projections), `shared/adapters/postgres/common/` (utilities), `governance/adapters/postgres/` (audit_log, no model file), `outbox/adapters/postgres/` (repository only).

### Command/Query Service Files

- Use plural suffix: `*_commands.go`, `*_queries.go`.
- Entity-grouped: each `*_commands.go` contains ALL write operations for an aggregate/entity group.
- Entry point: `commands.go` or `queries.go` (UseCase struct, constructor, shared errors/sentinels).
- Helper files: private methods may use descriptive names without suffix (e.g., `match_group_persistence.go`, `rule_execution_support.go`). The suffix is required for files exposing public use-case methods.

### Handler Splitting

Split into `handlers_{feature}.go` when a context has 3+ distinct feature areas (e.g., `handlers_run.go`, `handlers_manual.go`, `handlers_adjustment.go`).

### DTO Directory

`adapters/http/dto/` with files like `{entity}.go`, `requests.go`, `responses.go`, `converters.go`, `doc.go`.

## 16. Tooling

### Required Make Targets

| Category | Targets |
|----------|---------|
| Core | `dev`, `build`, `tidy`, `clean` |
| Quality | `lint`, `lint-fix`, `lint-custom`, `lint-custom-strict`, `format`, `sec`, `vet`, `vulncheck` |
| Testing | `test`, `test-unit`, `test-int`, `test-e2e`, `test-e2e-fast`, `test-e2e-journeys`, `test-e2e-dashboard`, `test-chaos`, `test-all` |
| Coverage | `cover`, `check-coverage` |
| Checks | `check-tests`, `check-test-tags`, `check-migrations`, `check-generated-artifacts` |
| Generation | `generate`, `generate-docs` |
| Docker | `docker-build`, `up`, `down`, `start`, `stop`, `restart`, `rebuild-up`, `clean-docker`, `logs` |
| Migration | `migrate-up`, `migrate-down`, `migrate-to`, `migrate-create` |
| CI | `ci` |

- Test runner: `gotestsum` if available, else `go test`.
- Docker Compose command auto-detected (`docker compose` vs `docker-compose`).

## 17. Linting

### Standard Linters (golangci-lint)

Run with `make lint`. Configuration in `.golangci.yml`. 75+ linters enabled.

**Key enforced rules:**

- **Multi-tenancy security**: Never accept tenant identifiers in request payloads, path params, or query params. Use `auth.GetTenantID(ctx)`.
- **UTC timestamps**: Always `time.Now().UTC()`.
- **SQL injection prevention**: Parameterized queries (`$1, $2, ...`), never `fmt.Sprintf` with `%s` for SQL.
- **Error wrapping**: `%w` not `%v` with `fmt.Errorf`.
- **Import organization**: stdlib -> third-party -> Lerian libs -> project (enforced by `gci`).
- **Stricter formatting**: `gofumpt` (superset of `gofmt`).

### forbidigo Security Patterns

| Blocked pattern | Replacement |
|----------------|-------------|
| `panic`, `log.Panic*`, `log.Fatal*`, `os.Exit` | Return errors (exemptions: `main.go`, `pkg/runtime`, test helpers) |
| `fmt.Print*` | Structured logging (exemptions: `cmd/`, `pkg/assert/`, tests) |
| `.Params("contextId")`, `.Query("contextId")` | `sharedhttp.ParseAndVerifyContextParam()` |
| `json:"tenant_id"`, `.Params("tenantId")`, `.Query("tenant...")` | `auth.GetTenantID(ctx)` |
| `time.Now()[^.]` | `time.Now().UTC()` |
| `fmt.Sprintf(...%s...SQL...)` | Parameterized queries |
| `fmt.Errorf(...%v...err)` | `fmt.Errorf(...%w...err)` |
| `runtime.SafeGoWithContext` (without Component) | `runtime.SafeGoWithContextAndComponent(...)` |

### depguard Architectural Rules

| Rule | Enforces |
|------|----------|
| `http-handlers-boundary` | HTTP handlers cannot import postgres adapters (all 8 contexts) |
| `cross-context-{name}` (x8) | Full cross-context isolation for all bounded contexts |
| `cross-context-outbox` | Outbox cannot import any business context |
| `service-no-adapters` | Services cannot import adapter packages; depend on ports |
| `dto-no-services` | DTOs cannot import service packages |
| `worker-no-adapters` | Workers cannot import postgres/redis/rabbitmq adapters directly |
| `cqrs-command-isolation` | Command services cannot import query packages |
| `cqrs-query-isolation` | Query services cannot import command packages |
| `domain-purity` | Domain services cannot import application or adapter packages |
| `domain-no-logging` | Domain layer cannot depend on logging/tracing/infrastructure |
| `ports-no-adapters` | Ports cannot import adapters (dependency inversion) |
| `entity-purity` | Entities cannot import repositories or `database/sql` |
| `shared-adapters-boundary` | Shared adapters use shared kernel, not context-specific entities |
| `cross-adapter-output` | Cross adapters import domain types only, not HTTP adapters |
| `testutil-isolation` | Test utilities cannot depend on production adapters |

### Custom Linters

Run with `make lint-custom`. Source in `tools/linters/`.

| Linter | Enforces |
|--------|----------|
| `entityconstructor` | `New<EntityName>(ctx, ...) (*EntityName, error)` pattern |
| `observability` | `NewTrackingFromContext` + span creation + `defer span.End()` |
| `repositorytx` | Write methods (`Create`, `Update`, `Delete`) have `*WithTx` variants |

### IDE Integration

VSCode settings in `.vscode/settings.json` configure golangci-lint as the linter, gofumpt as the formatter, and format/organize imports on save.

## 18. CI/CD

All CI uses shared workflows from `LerianStudio/github-actions-shared-workflows`.

| Workflow | Trigger | Purpose |
|----------|---------|---------|
| `go-combined-analysis.yml` | PRs to develop/release-candidate/main | Lint, security scan, unit tests, coverage (70% threshold) |
| `pr-security-scan.yml` | PRs to develop/release-candidate/main | PR-specific security scanning |
| `pr-validation.yml` | PRs to develop/release-candidate/main | Conventional commit format in PR title, 50-char min description, changelog check, auto-labeling |
| `build.yml` | Tag push | Docker build (DockerHub + GHCR) + GitOps value updates |
| `release.yml` | Push to develop/release-candidate/main | Automated semantic releases |

- Go version: module minimum `go 1.26.0` (in `go.mod`); CI and Dockerfile use toolchain `1.26.1`. golangci-lint v2.10.1.
- Coverage threshold: 70%, enforced via `fail_on_coverage_threshold: true`.

## 19. Docker

- **Dockerfile**: Multi-stage build. `golang:1.26.1-alpine` (builder) -> `gcr.io/distroless/static-debian12:nonroot` (runtime).
- Separate `/health-probe` binary for distroless healthchecks (30s interval, 5s timeout, 3 retries).
- **docker-compose services**:

| Service | Image | Port |
|---------|-------|------|
| postgres | `postgres:17` | 5432 |
| postgres-replica | `postgres:17` | 5433 |
| redis | `valkey/valkey:8` | 6379 |
| rabbitmq | `rabbitmq:4.1.3-management-alpine` | 5672, 15672 |
| seaweedfs | `chrislusf/seaweedfs:3.80` | 8333, 9333 |
| app | `golang:1.26.1-alpine` (air dev) | 4018 |

- All infrastructure services have healthchecks. App container depends on all infra services being healthy.

## 20. Dependency Maintenance Notes

### PDF Generation: `codeberg.org/go-pdf/fpdf`

- **Decision**: Migrated from archived `github.com/go-pdf/fpdf` v0.9.0 to `codeberg.org/go-pdf/fpdf` v0.11.1 (Feb 2026).
- **Rationale**: The GitHub repository was archived on March 4, 2025. Active development continued on Codeberg by the same maintainers with an identical API. This is a direct import path swap with no code changes required.
- **Scope**: Used only in `internal/reporting/services/query/exports/pdf.go` for report PDF generation (matched, unmatched, summary, variance reports).
- **Risk**: Low. Same library, same maintainers, MIT license, no external dependencies beyond Go stdlib. If Codeberg hosting becomes unavailable, the library is pure Go with zero transitive dependencies and could be vendored trivially.

### Core Runtime Dependency Refresh Policy

- **Scope**: Infrastructure/runtime upgrades (framework, logging, telemetry, messaging, networking, and datastore clients) must be treated as operationally sensitive.
- **Soak policy**: Stage for at least 7 days with `make test-int`, `make test-e2e-fast`, and readiness/health probes validated under representative load.
- **Rollback plan**: Keep a tested rollback path (revert dependency bump or pin previous known-good versions in `go.mod`), run `go mod tidy`, then re-run `make test` + `make test-int` before redeploy.
- **Owner**: Platform/Runtime maintainers must sign off rollout and rollback readiness during review.

## 21. Misc

- Avoid one-letter variable names unless required.
- Do not add inline comments unless requested.
- `atomic.Bool` for thread-safe production mode flag in handlers.
- Lease pattern for all DB/Redis connections (automatic cleanup via `Release()`).
- Avoid duplicating domain concepts between shared kernel and contexts; pick a single source of truth and map explicitly.
- Put logic in entities when it only needs entity fields; use domain services for multi-aggregate or external dependency coordination.
