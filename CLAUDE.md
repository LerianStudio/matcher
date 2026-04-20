# CLAUDE.md

Comprehensive reference for AI coding agents working in the Matcher codebase. Read [AGENTS.md](AGENTS.md) first for a concise overview, then use this file for deep patterns and conventions.

## Quick Start (30 seconds)

```bash
make up          # Start infrastructure (Postgres, Redis, RabbitMQ, SeaweedFS)
make migrate-up  # Apply database migrations
make dev         # Run with live reload (air) on :4018
make test        # Run unit tests
make lint        # Run 75+ linters
```

Health check: `GET http://localhost:4018/health`

## Project Identity

| Attribute | Value |
|-----------|-------|
| **Project** | Transaction reconciliation engine for Lerian Studio |
| **Language** | Go (module: `go 1.26.0`) |
| **Architecture** | Modular monolith: DDD + Hexagonal + CQRS-light |
| **Database** | PostgreSQL 17, schema-per-tenant isolation |
| **Cache/Locking** | Valkey (Redis-compatible) 8 |
| **Messaging** | RabbitMQ 4.1 via transactional outbox |
| **Object Storage** | S3-compatible (SeaweedFS in dev) |
| **Testing** | TDD required; testify + sqlmock + testcontainers |
| **License** | Elastic License 2.0 |

## Essential Commands

### Development

```bash
make dev              # Live reload with air (watches *.go, *.sql in cmd/, internal/, pkg/)
make build            # Build binary to bin/matcher
make tidy             # Clean Go modules (root + tools/)
make clean            # Remove build artifacts and tmp/
```

### Testing

```bash
make test             # Unit tests (alias for test-unit)
make test-unit        # Explicit unit tests with coverage
make test-int         # Integration tests (requires Docker)
make test-e2e         # E2E tests (requires full stack via make up)
make test-e2e-fast    # E2E in quick mode (short flag, 5m timeout)
make test-e2e-journeys # Journey-based E2E only
make test-e2e-discovery # Discovery E2E with mock Fetcher
make test-e2e-dashboard # 5k transaction dashboard stresser
make test-chaos       # Fault injection tests (Toxiproxy + containers)
make test-all         # All tests (unit + integration + e2e) with merged coverage

# Single test
go test -v -tags=unit -run TestFunctionName ./path/to/package/...
```

### Quality

```bash
make lint             # golangci-lint (75+ linters, .golangci.yml)
make lint-fix         # golangci-lint with auto-fix
make lint-custom      # Custom Matcher linters (entity, observability, tx patterns)
make lint-custom-strict # Custom linters in strict mode (fails on violations)
make format           # go fmt
make sec              # gosec security scanner
make vet              # go vet static analysis
make vulncheck        # Go vulnerability scanner
make ci               # Full local CI pipeline (lint + test + sec + vet + checks)
```

### Verification

```bash
make check-tests              # Every .go file has a _test.go
make check-test-tags          # Test files have proper build tags
make check-migrations         # Migration pairs and sequential numbering
make check-coverage           # Coverage meets 70% threshold
make check-generated-artifacts # Swagger docs are up to date
```

### Code Generation

```bash
make generate         # go:generate (mocks, etc.)
make generate-docs    # Swagger/OpenAPI docs to docs/swagger/
```

### Database

```bash
make migrate-up                   # Apply all pending migrations
make migrate-down                 # Rollback last migration
make migrate-to VERSION=<n>       # Migrate to specific version
make migrate-create NAME=<name>   # Create new migration pair
```

### Docker

```bash
make up               # Start all services (postgres, redis, rabbitmq, seaweedfs, app)
make down             # Stop all services
make restart          # Stop + start
make rebuild-up       # Rebuild images and restart
make logs             # Tail all service logs
make clean-docker     # Remove all containers, volumes, prune
make docker-build     # Build Docker image locally
```

## Architecture

### Bounded Contexts

```
internal/
├── auth/             # JWT extraction, tenant resolution, RBAC middleware
├── bootstrap/        # Composition root: config, DI, server, systemplane
├── configuration/    # Reconciliation contexts, sources, match rules, fee schedules/rules, scheduling
├── discovery/        # External data source discovery, schema detection, extraction
├── ingestion/        # File parsing (CSV/JSON/XML/ISO 20022), normalization, dedup
├── matching/         # Match orchestration, rule execution, fee verification, scoring
├── exception/        # Exception lifecycle, disputes, evidence, resolutions, bulk ops
├── governance/       # Immutable audit logs, hash chains, archival
├── reporting/        # Dashboard analytics, export jobs (CSV/PDF), variance reports
├── outbox/           # Reliable event publication via transactional outbox
├── shared/           # Shared kernel: cross-context domain types + port abstractions
└── testutil/         # Shared test helpers (Ptr[T], deterministic time)
```

### Hexagonal Structure (per context)

```
internal/{context}/
├── adapters/
│   ├── http/             # Fiber handlers + DTOs
│   │   └── dto/          # Request/response data-transfer objects
│   ├── postgres/         # Repository implementations
│   │   └── {aggregate}/  # One dir per aggregate root
│   └── rabbitmq/         # Message publishers/consumers
├── ports/                # External dependency abstractions (context-specific)
├── domain/
│   ├── entities/         # Aggregate roots with business logic
│   ├── value_objects/    # Value types (configuration, exception, ingestion, matching)
│   ├── enums/            # Type-safe enumerations (matching)
│   ├── repositories/     # Repository interfaces for own aggregates
│   ├── services/         # Domain services (matching has this for rule evaluators)
│   └── errors/           # Domain sentinel errors (governance)
└── services/
    ├── command/          # Write operations (*_commands.go + helpers)
    ├── query/            # Read operations (*_queries.go)
    └── worker/           # Background workers (configuration, governance, reporting, discovery)
```

### Interface Location Convention

| Location | Contains |
|----------|----------|
| `{context}/domain/repositories/` | Repository interfaces for that context's own aggregates |
| `{context}/ports/` | External dependency abstractions (EventPublisher, ObjectStorage, CacheProvider) |
| `internal/shared/ports/` | Cross-context abstractions (OutboxRepository, AuditLogRepository, InfrastructureProvider, MatchTrigger, TenantLister, FetcherClient, M2MProvider, IdempotencyRepository) |

### Shared Kernel (`internal/shared/`)

The designated bridge between bounded contexts. Contains types multiple contexts legitimately share.

```
internal/shared/
├── adapters/
│   ├── cross/        # Bridge adapters connecting contexts
│   ├── http/         # Shared HTTP middleware (idempotency, rate limiting, error mapping)
│   ├── m2m/          # Machine-to-machine credential adapters
│   ├── postgres/     # Common SQL utilities (pgcommon)
│   └── rabbitmq/     # Shared RabbitMQ publisher with confirms + DLQ
├── constants/        # Shared constants
├── domain/
│   ├── audit_log.go      # AuditLog entity (governance + matching)
│   ├── events.go         # Domain event base types
│   ├── field_map.go      # FieldMap for normalization (ingestion + matching)
│   ├── idempotency.go    # Idempotency domain types
│   ├── ingestion_events.go # Ingestion event payloads
│   ├── match_rule.go     # MatchRule (matching + configuration)
│   ├── outbox_event.go   # OutboxEvent envelope (outbox + all publishers)
│   ├── transaction.go    # Transaction entity (ingestion + matching + exception)
│   ├── exception/        # Exception severity value objects (type-aliased)
│   └── fee/              # Fee schedule domain (used by config + matching)
├── infrastructure/   # Shared infrastructure setup
├── ports/            # Cross-context port interfaces (15+ files)
├── sanitize/         # CSV formula injection prevention
├── testutil/         # Shared test mocks and helpers
└── utils/            # Generic utilities
```

**Cross-context import rule**: Bounded contexts **must not** import each other directly. Use `internal/shared/` as the bridge. Enforced by depguard rules in `.golangci.yml`.

**Type-alias pattern**: When a type migrates to `shared/domain/`, the original package re-exports via type alias:
```go
// exception/domain/value_objects/severity.go
import sharedexception "github.com/LerianStudio/matcher/internal/shared/domain/exception"
type Severity = sharedexception.Severity  // type alias, not redefinition
```

## Code Patterns

### 1. Domain Entities

- Constructor `New*()` enforces invariants, returns `(*T, error)`
- Use `pkg/assert` for validation (returns errors, never panics)
- Immutable IDs (`uuid.UUID`), UTC timestamps (`time.Now().UTC()`)
- Pure business logic: no logging, no tracing, no infrastructure imports
- Rich behavior via methods (e.g., `CanAutoConfirm()`, `Reject()`, `MarkComplete()`)
- Identity fields (`ID`, `ContextID`, `TenantID`) immutable after creation

```go
func NewMatchItem(ctx context.Context, txID uuid.UUID, allocated, expected decimal.Decimal, currency string) (*MatchItem, error) {
    asserter := assert.New(ctx, nil, "matcher", "match_item.new")
    if err := asserter.That(ctx, txID != uuid.Nil, "transaction id required"); err != nil {
        return nil, fmt.Errorf("match item transaction id: %w", err)
    }
    if err := asserter.That(ctx, !allocated.IsNegative(), "allocated amount non-negative"); err != nil {
        return nil, fmt.Errorf("match item allocated amount: %w", err)
    }
    return &MatchItem{
        ID: uuid.New(), TransactionID: txID, AllocatedAmount: allocated,
        CreatedAt: time.Now().UTC(), UpdatedAt: time.Now().UTC(),
    }, nil
}
```

### 2. Services (Use Cases)

- One UseCase struct per bounded context (separate for command vs query)
- Constructor validates required deps with sentinel errors; optional deps via `UseCaseOption`
- Domain-specific method names (`RunMatch()`, `ManualMatch()`), NOT generic `Execute()`
- Dedicated input struct per method
- Every method: tracking + span + defer

```go
func (uc *UseCase) RunMatch(ctx context.Context, input RunMatchInput) (*MatchRun, error) {
    track := libCommons.NewTrackingFromContext(ctx)
    ctx, span := track.Tracer.Start(ctx, "matching.run_match")
    defer span.End()
    // orchestration logic...
}
```

### 3. Repositories

- Implement domain port interfaces
- Use `pgcommon.WithTenantTxProvider(ctx, provider, fn)` for tenant-isolated transactions
- Use `pgcommon.WithTenantTxOrExistingProvider(ctx, provider, existingTx, fn)` for composable transactions
- Every write method must have a `*WithTx` variant (enforced by custom linter)
- Separate PostgreSQL model structs from domain entities (`NewPostgreSQLModel()` / `ToEntity()`)
- Use `squirrel` for dynamic query building with `squirrel.Dollar` placeholder format
- Cursor-based pagination via `pgcommon.ApplyIDCursorPagination()`

### 4. HTTP Handlers

- Fiber v2 framework (`gofiber/fiber/v2`)
- Every handler starts with: `ctx, span, logger := startHandlerSpan(fiberCtx, "handler.{context}.{operation}")` + `defer span.End()`
- Body parsing: `libHTTP.ParseBodyAndValidate(fiberCtx, &payload)`
- Context verification: `libHTTP.ParseAndVerifyTenantScopedID()` for path params
- Swagger annotations required on all handlers
- Route registration uses `protected(resource, actions...)` higher-order function
- Error-to-HTTP mapping via `errors.Is(err, ErrSentinel)` → HTTP status codes
- Response helpers: `libHTTP.Respond()`, `libHTTP.RespondError()`, `libHTTP.RespondStatus()`

### 5. Error Handling

- Sentinel errors via `errors.New()` at package level, naming: `Err[Category][Specific]`
- Error wrapping: `fmt.Errorf("context: %w", err)` — ALWAYS `%w`, never `%v`
- Check with `errors.Is(err, ErrNotFound)`
- Error tracing: `libOpentelemetry.HandleSpanError(span, "msg", err)` (takes value, not pointer)
- Business error events: `libOpentelemetry.HandleSpanBusinessErrorEvent(span, "message")`
- Error sanitization in production: `libLog.SafeError(logger, ctx, "msg", err, productionMode.Load())`

**Sentinel locations** (5 places):
1. `services/command/commands.go` — use case sentinels
2. `domain/entities/*.go` — state transition errors
3. `adapters/postgres/{name}/errors.go` — repository sentinels
4. `adapters/http/errors.go` or `handlers.go` — HTTP-level errors
5. `domain/errors/errors.go` — governance only

### 6. Nil Checks vs Asserters

| Use simple `if x == nil` for | Use `pkg/assert` for |
|-------------------------------|---------------------|
| Nil receiver checks (fast path) | Domain entity invariant validation |
| Dependency injection in constructors (sentinel errors) | Business rule validation with structured context |
| Infrastructure/adapter layer | Multiple sequential validations |

### 7. Multi-Tenancy (CRITICAL SECURITY)

- Tenant info (`tenantID`, `tenantSlug`) ONLY from JWT claims via context
- **NEVER** accept tenant identifiers in request payloads, path params, query params, or headers
- Extract via `auth.GetTenantID(ctx)` or `auth.GetTenantSlug(ctx)`
- Apply schema in transactions via `auth.ApplyTenantSchema(ctx, tx)`
- Use `pgcommon.WithTenantTxProvider` for automatic isolation
- If auth disabled or claims missing, run in single-tenant mode with `public` schema
- Read operations use replica connections with connection-scoped `SET search_path`
- Default tenant uses `public` schema (no UUID schema). Background workers MUST include default tenant when enumerating via `pg_namespace`

### 8. Testing Patterns

**Build tags** (required at top of file):

| Tag | Scope | External deps |
|-----|-------|---------------|
| `//go:build unit` | Unit tests | None (mocks only) |
| `//go:build integration` | Integration tests | Testcontainers |
| `//go:build e2e` | End-to-end | Full stack |
| `//go:build chaos` | Fault injection | Toxiproxy + containers |

**Test structure**:
- Co-locate tests with source (`*_test.go`)
- Use testify `assert`/`require`
- `sqlmock` for database unit tests
- `testcontainers-go` for integration tests
- `go.uber.org/mock` (gomock) for complex contracts; manual mocks for simple interfaces (<=5 methods)
- Every `.go` file must have a corresponding `_test.go` (enforced by `make check-tests`)
- Test env is clean: Makefile unsets all matcher config env vars before runs (`CLEAN_ENV`)
- Coverage threshold: **70%** enforced in CI

**Example**:
```go
//go:build unit

func TestNewMatchItem_ValidInput_Success(t *testing.T) {
    ctx := context.Background()
    txID := uuid.New()
    amount := decimal.NewFromFloat(100.50)
    item, err := NewMatchItem(ctx, txID, amount, amount, "USD")
    assert.NoError(t, err)
    assert.NotNil(t, item)
    assert.Equal(t, txID, item.TransactionID)
}
```

## File Naming Standards

### Postgres Adapters

Each aggregate directory follows Pattern A:
- `{name}.go` — model structs and domain<->DB conversions
- `{name}.postgresql.go` — repository implementation
- `{name}.postgresql_test.go` — postgres adapter tests (build tag discriminates unit vs integration)
- `{name}_sqlmock_test.go` — sqlmock-based unit tests
- `errors.go` — adapter-specific sentinel errors

**Flat layout exceptions**: `reporting/adapters/postgres/`, `shared/adapters/postgres/common/`, `governance/adapters/postgres/`

### Command/Query Services

- Commands: `*_commands.go` (entity-grouped, plural) — e.g., `match_group_commands.go`
- Queries: `*_queries.go` (entity-grouped, plural) — e.g., `dashboard_queries.go`
- Entry point: `commands.go` (UseCase struct, constructor, shared errors)
- Helper files: private methods use descriptive names without the `_commands.go` suffix (e.g., `match_group_persistence.go`, `rule_execution_support.go`)

### HTTP Handlers

Split into `handlers_{feature}.go` when a context has 3+ distinct feature areas.

### Test Files

- Build tags are the authoritative discriminator
- `_sqlmock_test.go` for SQL mock-based unit tests
- `_mock_test.go` for other mock-based unit tests (e.g., RabbitMQ)
- Test files are NOT merged even when source files are consolidated

## Gotchas & Non-Obvious Patterns

1. **Never panic in production** — Use `pkg/assert` (returns errors). Use `pkg/runtime` for panic recovery in goroutines: `defer runtime.RecoverAndLogWithContext(ctx, logger, component, name)` as first defer (LIFO order).

2. **Tenant isolation is non-negotiable** — PostgreSQL `search_path` per request. Transactions MUST apply tenant schema. Repository methods extract tenant from context, never from parameters.

3. **Audit trail is append-only** — NEVER UPDATE or DELETE audit tables. Events immutable after creation.

4. **Observability is everywhere** — Start spans at service boundaries, always `defer span.End()`. Use `libCommons.NewTrackingFromContext(ctx)` for logger + tracer + headerID.

5. **Transaction management** — Always provide `WithTx` variants. Use `pgcommon.WithTenantTxProvider` for automatic isolation. Keep transactions short and deterministic. 30s default timeout.

6. **Idempotency keys** — `sharedhttp.NewIdempotencyMiddleware(...)` for POST/PUT. Keys stored in Redis. Applied after auth + tenant extraction (needs tenant ID for scoping). Max 128 chars.

7. **Outbox pattern** — All async communication via outbox (no direct context-to-context messaging). Dispatcher polls with configurable interval (~2s). `ConfirmablePublisher` with broker confirmation and automatic channel recovery.

8. **Systemplane is runtime config authority** — Viper + env vars are bootstrap-only. After startup, `systemplane` owns all runtime config. Use `configManager.Get()` for values. v5 admin surface (management-plane, intentionally excluded from public OpenAPI): `GET /system/:namespace` (list with inline schema metadata), `GET /system/:namespace/:key` (read a single key), `PUT /system/:namespace/:key` (write a single key). The matcher namespace is `matcher`. The v4 `/v1/system/configs[...]` paths and the `/schema`, `/history`, `/reload` sub-endpoints are REMOVED — schema metadata is returned inline in list responses, history is available only via audit logs, and reload is no longer exposed (v5 auto-subscribes to changes).

9. **Docker Compose auto-detection** — Makefile auto-detects `docker compose` vs `docker-compose` via `$(DOCKER_CMD)`.

10. **Air live reload** — Watches `*.go`, `*.sql` in `cmd/`, `internal/`, `pkg/`. Excludes tests, tools, docs, migrations. Builds to `tmp/main`. Config in `.air.toml`.

11. **Workers vs Dispatcher** — Background workers in `services/worker/`. The outbox dispatcher is provided by `lib-commons/v5/commons/outbox` and wired in `internal/bootstrap/outbox_wiring.go`; matcher registers one handler per event type rather than owning its own dispatcher package.

12. **Migration naming** — Sequential: `000001_descriptive_name.up.sql` / `.down.sql`. Currently 32 migrations. Always provide rollback. Validate with `make check-migrations`.

## Configuration

### Zero-Config Defaults

Matcher uses zero-config defaults — all configuration has sensible defaults baked into `defaultConfig()` in `internal/bootstrap/config_defaults.go`. No `.env` or YAML files required. Override via environment variables for production.

### Bootstrap vs Runtime

| Bootstrap (require restart) | Runtime (hot-reloadable) |
|----------------------------|--------------------------|
| `SERVER_ADDRESS`, TLS, auth settings | Body limit, rate limits, worker intervals |
| `POSTGRES_HOST`, `REDIS_HOST`, `RABBITMQ_HOST` | Feature flags, timeouts |
| `OTEL_EXPORTER_OTLP_ENDPOINT`, `LOG_LEVEL` | Export settings, archival intervals |

> Note: `LOG_LEVEL` is bootstrap-only. Runtime log-level swapping is **not** implemented — changing `LOG_LEVEL` requires a process restart. The previous `app.log_level` systemplane key was removed in the lib-commons v5 migration because editing it via the admin API had no effect.

See [`config/.config-map.example`](config/.config-map.example) for all bootstrap-only keys.

### Environment Variable Categories

- **Application**: `ENV_NAME`, `LOG_LEVEL`, `SERVER_ADDRESS`
- **Tenancy**: `DEFAULT_TENANT_ID`, `DEFAULT_TENANT_SLUG`
- **PostgreSQL**: `POSTGRES_HOST`, `POSTGRES_PORT`, `POSTGRES_USER`, `POSTGRES_PASSWORD`, `POSTGRES_DB`, `POSTGRES_SSLMODE`, `POSTGRES_MAX_OPEN_CONNS`, `POSTGRES_MAX_IDLE_CONNS`
- **PostgreSQL Replica**: `POSTGRES_REPLICA_HOST`, `POSTGRES_REPLICA_PORT`, etc.
- **Redis**: `REDIS_HOST`, `REDIS_MASTER_NAME`, `REDIS_PASSWORD`, `REDIS_DB`
- **RabbitMQ**: `RABBITMQ_HOST`, `RABBITMQ_PORT`, `RABBITMQ_USER`, `RABBITMQ_PASSWORD`, `RABBITMQ_VHOST`
- **Auth**: `PLUGIN_AUTH_ENABLED`, `PLUGIN_AUTH_ADDRESS`, `AUTH_JWT_SECRET`
- **OpenTelemetry**: `ENABLE_TELEMETRY`, `OTEL_LIBRARY_NAME`, `OTEL_RESOURCE_SERVICE_NAME`, `OTEL_EXPORTER_OTLP_ENDPOINT`
- **Object Storage**: `OBJECT_STORAGE_ENDPOINT`, `OBJECT_STORAGE_BUCKET`, `OBJECT_STORAGE_ACCESS_KEY_ID`, `OBJECT_STORAGE_SECRET_ACCESS_KEY`
- **Systemplane**: `SYSTEMPLANE_SECRET_MASTER_KEY`

## Required Libraries

### Lerian-Specific

- **lib-auth/v3** (`v3.0.0-20260415175119-1568b252d48a`): JWT extraction, RBAC authorization, tenant schema application. This is a pre-release pseudo-version pending upstream tag — see the `lib-auth/v3 Pseudo-version Tracking` appendix below for action items.
  - `auth.GetTenantID(ctx)`, `auth.GetTenantSlug(ctx)`, `auth.ApplyTenantSchema(ctx, tx)`

- **lib-commons/v5** (`v5.0.2`): Common utilities, telemetry, infrastructure
  - Tracking: `libCommons.NewTrackingFromContext(ctx)` → logger, tracer, headerID
  - OpenTelemetry: `libOpentelemetry.HandleSpanError(span, "msg", err)`
  - Database: `libPostgres.New()` / `libPostgres.NewPrimaryReplica()`
  - Redis: `libRedis.New()`
  - Messaging: `libRabbitmq.New()`
  - Assertions: `commons/assert` (imported as `pkg/assert` by convention, but lives in lib-commons)
  - Panic recovery: `commons/runtime` (imported as `pkg/runtime` by convention, but lives in lib-commons)
  - Runtime config: `commons/systemplane`

### Key Third-Party

- `gofiber/fiber/v2` — HTTP framework
- `Masterminds/squirrel` — SQL query builder
- `shopspring/decimal` — Precise decimal arithmetic
- `google/uuid` — UUID generation
- `DATA-DOG/go-sqlmock` — SQL mocking for tests
- `testcontainers/testcontainers-go` — Container-based integration tests
- `go.uber.org/mock` — Interface mocking
- `Shopify/toxiproxy/v2` — Chaos testing

### Internal Packages (`pkg/`)

- **pkg/chanutil** — Safe channel utilities for goroutine communication
- **pkg/storageopt** — Functional options for object storage operations

## Linting & Security

### golangci-lint (75+ linters)

Key categories: Security (gosec, bidichk), Bugs (errcheck, govet, staticcheck), Error handling (err113, errorlint, wrapcheck), Performance (bodyclose, prealloc), Complexity (cyclop, gocognit, nestif), Style (gofumpt, gci, gocritic), Testing (paralleltest, testifylint), Database (rowserrcheck, sqlclosecheck).

### forbidigo Security Rules (blocked patterns)

| Blocked | Use instead |
|---------|-------------|
| `json:"tenant_id"`, `.Params("tenantId")`, `.Query("tenant")` | `auth.GetTenantID(ctx)` |
| `time.Now()[^.]` | `time.Now().UTC()` |
| `fmt.Sprintf.*%s.*SQL` | Parameterized queries (`$1`, `$2`) |
| `fmt.Errorf.*%v.*err` | `fmt.Errorf("...: %w", err)` |
| `panic`, `log.Fatal*`, `os.Exit` | Return errors |
| `.Params("contextId")` | `sharedhttp.ParseAndVerifyContextParam()` |

### depguard Architectural Boundaries

| Rule | Enforces |
|------|----------|
| `cross-context-{name}` (x8) | Full cross-context isolation; direct imports blocked |
| `http-handlers-boundary` | HTTP handlers cannot import postgres adapters |
| `service-no-adapters` | Services depend on ports, not adapters |
| `dto-no-services` | DTOs are pure data structures |
| `worker-no-adapters` | Workers depend on ports, not adapters |
| `cqrs-command-isolation` | Commands cannot import queries |
| `cqrs-query-isolation` | Queries cannot import commands |
| `domain-purity` | Domain cannot import application or adapter packages |
| `domain-no-logging` | Domain layer: no logging/tracing/infrastructure |
| `entity-purity` | Entities cannot import repositories or `database/sql` |

### Custom Linters (`tools/linters/`)

Run with `make lint-custom`:

| Linter | Enforces |
|--------|----------|
| `entityconstructor` | `New<EntityName>(ctx, ...) (*EntityName, error)` pattern |
| `observability` | `NewTrackingFromContext` + span creation + `defer span.End()` |
| `repositorytx` | Write methods (`Create`, `Update`, `Delete`) have `*WithTx` variants |

## CI/CD

All CI uses shared workflows from `LerianStudio/github-actions-shared-workflows`.

| Workflow | Trigger | Purpose |
|----------|---------|---------|
| `go-combined-analysis.yml` | PRs to develop/RC/main | Lint, security, unit tests, coverage (70%), migration integrity |
| `pr-security-scan.yml` | PRs | Security scanning |
| `pr-validation.yml` | PRs | Conventional commits, 50-char min, changelog, auto-labels |
| `build.yml` | Tag push | Docker build (DockerHub + GHCR) + GitOps updates |
| `release.yml` | Push to develop/RC/main | Automated semantic releases |

### Pre-Commit Checklist

1. `make lint` — linters pass
2. `make test` — unit tests pass
3. `make check-tests` — every `.go` has a `_test.go`
4. `make check-test-tags` — test files have proper build tags
5. `make sec` — no security issues
6. `make check-migrations` — migration pairs valid (if changed)
7. `make generate-docs` — Swagger updated (if API changed)
8. Commit message follows conventional commits

## Common Tasks

### Adding a New API Endpoint

1. Define request/response DTOs in `{context}/adapters/http/dto/`
2. Add handler method to `{context}/adapters/http/handlers.go` (or `handlers_{feature}.go`)
3. Register route in `{context}/adapters/http/routes.go`
4. Add Swagger annotations (`@Summary`, `@Tags`, `@Param`, `@Success`, `@Failure`, `@Router`)
5. Run `make generate-docs`
6. Write unit tests for handler
7. Wire up in `internal/bootstrap/routes.go`

### Adding a New Domain Entity

1. Create entity in `{context}/domain/entities/{name}.go` with `New{Name}()` constructor
2. Add business methods for state transitions
3. Create repository interface in `{context}/domain/repositories/`
4. Implement in `{context}/adapters/postgres/{name}/`
5. Write unit tests (entity behavior + sqlmock for repo)
6. Wire up in `internal/bootstrap/`

### Adding a New Migration

1. `make migrate-create NAME=descriptive_name`
2. Edit `.up.sql` with schema changes
3. Edit `.down.sql` with rollback logic
4. Test the forward path with `make migrate-up`
5. If the migration is intentionally irreversible, verify the rollback/preflight guard instead of forcing `migrate-down`
6. Add indexes for join/filter columns
7. Validate: `make check-migrations`

### Adding a New Use Case

1. Create service file in `{context}/services/command/` or `query/`
2. Define input struct
3. Add method to UseCase struct with domain-specific name
4. Start span, extract tracking, orchestrate domain logic
5. Write unit tests with mocked dependencies
6. Wire up in `internal/bootstrap/`

## Debugging Tips

| Problem | Solution |
|---------|----------|
| Tenant isolation failing | Verify `auth.ApplyTenantSchema(ctx, tx)` called in transaction |
| Migration stuck | `SELECT * FROM schema_migrations;` to check state |
| Connection pool exhausted | Increase `POSTGRES_MAX_OPEN_CONNS` or check for leaked connections |
| Integration tests fail | Ensure `make up` ran, check Docker services health |
| E2E tests timeout | Increase timeout: `-timeout=10m` |
| Testcontainers hang | Set `TESTCONTAINERS_RYUK_DISABLED=true` (macOS/Windows) |
| Redis key not found | Check TTL, verify Redis mode (standalone/sentinel/cluster) |
| RabbitMQ messages stuck | Check consumer registration, verify queue bindings |

## Key Files

| File | Purpose |
|------|---------|
| [`AGENTS.md`](AGENTS.md) | Concise agent overview (read first) |
| [`docs/PROJECT_RULES.md`](docs/PROJECT_RULES.md) | Architectural rules and constraints |
| [`config/.config-map.example`](config/.config-map.example) | Bootstrap env vars reference |
| [`docs/multi-tenant-guide.md`](docs/multi-tenant-guide.md) | Multi-tenancy implementation guide |
| [`.golangci.yml`](.golangci.yml) | Linter configuration (75+ linters) |
| [`internal/bootstrap/`](internal/bootstrap/) | Composition root (config, DI, server) |
| [`internal/shared/`](internal/shared/) | Shared kernel (cross-context types) |
| [`docs/swagger/swagger.json`](docs/swagger/swagger.json) | OpenAPI specification |

## lib-auth/v3 Pseudo-version Tracking

go.mod currently pins `github.com/LerianStudio/lib-auth/v3` at pseudo-version
`v3.0.0-20260415175119-1568b252d48a`. A tagged v3.0.0 does not yet exist
upstream. This is intentional during the lib-commons v5 + lib-auth v3
migration window.

**Action items before production deploy:**
- [ ] Confirm LerianStudio/lib-auth has published v3.0.0 (or v3.0.0-rc.N)
- [ ] Bump `go.mod` to the tagged version
- [ ] Run `go mod tidy && make test`
- [ ] Remove this tracking entry

**Monitoring:** `git ls-remote --tags https://github.com/LerianStudio/lib-auth | grep v3`

---

**Last Updated**: April 2026
**Go Version**: module `go 1.26.0`
**Migrations**: 32 (000001 through 000032)
