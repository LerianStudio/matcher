# CLAUDE.md

This file helps AI agents work effectively in the Matcher codebase. It complements [AGENTS.md](AGENTS.md) and [docs/PROJECT_RULES.md](docs/PROJECT_RULES.md).

## Quick Reference

**Project**: Transaction reconciliation engine for Lerian Studio ecosystem  
**Language**: Go 1.25.6  
**Pattern**: Modular monolith with DDD + Hexagonal Architecture + CQRS-light  
**Database**: PostgreSQL 17 with schema-per-tenant isolation  
**Testing**: TDD required; testify + sqlmock + testcontainers

## Essential Commands

```bash
# Development
make dev              # Live reload with air (watches *.go, *.sql)
make tidy             # Clean Go modules
make build            # Build binary to bin/matcher

# Testing
make test             # Unit tests only (default)
make test-unit        # Explicit unit tests
make test-int         # Integration tests (requires Docker)
make test-e2e         # E2E tests (requires full stack)
make test-e2e-fast    # Fast E2E tests (short mode, 5m timeout)
make test-e2e-journeys # Journey-based E2E tests only
make test-all         # All tests with merged coverage
make check-tests      # Verify every .go file has _test.go
go test -v -run TestFunctionName ./path/to/package/...  # Single test

# Quality
make lint             # golangci-lint (75+ linters enabled)
make sec              # gosec security scanner
make vet              # Go vet static analysis
make vulncheck        # Go vulnerability scanner
make format           # go fmt
make cover            # Coverage report (opens coverage.html)
make check-coverage   # Verify coverage thresholds

# Database
make migrate-up       # Apply migrations
make migrate-down     # Rollback last migration
make migrate-create NAME=add_feature  # New migration

# Docker
make up               # Start infrastructure (postgres, redis, rabbitmq)
make down             # Stop all services
make start            # Start existing containers
make stop             # Stop running containers
make restart          # Restart all containers (down + up)
make logs             # Tail all service logs
make clean-docker     # Remove all containers/volumes

# Code generation
make generate         # Generate mocks (go:generate)
make generate-docs    # Generate Swagger docs

# Environment
make set-env          # Copy .env.example to config/.env
make clear-envs       # Remove config/.env
```

## Architecture Quick Start

### Bounded Contexts

```
internal/
├── configuration/    # Reconciliation contexts, sources, match rules, fee schedules
├── ingestion/        # File parsing, normalization, deduplication
├── matching/         # Match orchestration, rule execution, fee verification, confidence scoring
├── exception/        # Exception lifecycle, disputes, evidence, resolutions
├── governance/       # Immutable audit logs
├── reporting/        # Dashboard analytics, export jobs, variance reports, archival
├── outbox/           # Reliable event publication
└── shared/           # Cross-context utilities and adapters
```

### Hexagonal Structure (per context)

```
internal/{context}/
├── adapters/
│   ├── http/             # Fiber handlers + DTOs
│   ├── postgres/         # Repository implementations
│   │   └── {aggregate}/  # One dir per aggregate root
│   └── rabbitmq/         # Message publishers/consumers
├── ports/                # Repository/service interfaces (required)
├── services/
│   ├── command/          # Write operations (*_commands.go)
│   └── query/            # Read operations (*_queries.go)
└── domain/
    ├── entities/         # Aggregate roots with business logic
    ├── enums/            # Type-safe enumerations
    └── errors/           # Domain-specific sentinel errors
```

## Code Patterns

### 1. Domain Entities

**Key traits**:
- Constructor functions enforce invariants (`New*()`)
- Use `pkg/assert` for validation (returns errors, never panics)
- Immutable IDs (`uuid.UUID`)
- Pure business logic (no logging/tracing)
- UTC timestamps (`time.Now().UTC()`)
- Rich behavior via methods (e.g., `CanAutoConfirm()`, `Reject()`)

**Example**:
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
        ID:              uuid.New(),
        TransactionID:   txID,
        AllocatedAmount: allocated,
        CreatedAt:       time.Now().UTC(),
        UpdatedAt:       time.Now().UTC(),
    }, nil
}
```

### 2. Services (Use Cases)

**Key traits**:
- One service struct per use case
- Hold dependencies via constructor
- Context-first signatures
- Dedicated input structs
- Domain-specific method names (e.g., `RunMatch()`, `ManualMatch()`) preferred over generic `Execute()`
- Sentinel errors at package level
- OpenTelemetry spans for every method
- Use `libCommons.NewTrackingFromContext(ctx)` for observability

**Example**:
```go
type RunMatchUseCase struct {
    contextRepo ports.ContextRepository
    matchRepo   ports.MatchRepository
    eventBus    ports.EventPublisher
}

func NewRunMatchUseCase(contextRepo ports.ContextRepository, matchRepo ports.MatchRepository, eventBus ports.EventPublisher) *RunMatchUseCase {
    return &RunMatchUseCase{contextRepo: contextRepo, matchRepo: matchRepo, eventBus: eventBus}
}

func (uc *RunMatchUseCase) RunMatch(ctx context.Context, input RunMatchInput) (*MatchRun, error) {
    track := libCommons.NewTrackingFromContext(ctx)
    ctx, span := track.Tracer.Start(ctx, "matching.run_match")
    defer span.End()

    // Orchestration logic...
}
```

### 3. Repositories

**Key traits**:
- Implement domain port interfaces
- Use `ports.InfrastructureProvider` for database access
- Provide `WithTx` variants for transactional operations
- Use `pgcommon.WithTenantTxProvider` for multi-tenancy
- Convert between PostgreSQL models and domain entities
- Use `squirrel` for dynamic query building
- Cursor-based pagination via `libHTTP.CursorPagination`

**Example**:
```go
func (repo *MatchGroupRepository) CreateBatch(ctx context.Context, groups []*domain.MatchGroup) error {
    return pgcommon.WithTenantTxProvider(ctx, repo.provider, func(tx *sql.Tx) error {
        return repo.CreateBatchWithTx(ctx, tx, groups)
    })
}

func (repo *MatchGroupRepository) CreateBatchWithTx(ctx context.Context, tx *sql.Tx, groups []*domain.MatchGroup) error {
    // Insert logic with tenant isolation...
}
```

### 4. HTTP Handlers

**Key traits**:
- Fiber framework (`gofiber/fiber/v2`)
- Separate request/response DTOs
- Validation via `sharedhttp.ParseBodyAndValidate()`
- Context verification via `ParseAndVerifyContextParam()`
- Swagger annotations for OpenAPI
- Error mapping to HTTP status codes
- Span per handler via `startHandlerSpan()`

**Example**:
```go
// @Summary      Run match job
// @Description  Execute matching rules for a reconciliation context
// @Tags         matching
// @Accept       json
// @Produce      json
// @Param        contextId   path      string           true  "Context ID"
// @Param        request     body      RunMatchRequest  true  "Match parameters"
// @Success      200         {object}  RunMatchResponse
// @Failure      400         {object}  sharedhttp.ErrorResponse
// @Router       /api/v1/contexts/{contextId}/matches [post]
func (h *MatchHandler) RunMatch(c *fiber.Ctx) error {
    ctx, span := startHandlerSpan(c, "run_match")
    defer span.End()
    
    // Parse and validate...
}
```

### 5. Error Handling

**Key traits**:
- Sentinel errors via `errors.New()` at package level
- Error wrapping with `fmt.Errorf("context: %w", err)`
- Check with `errors.Is(err, ErrNotFound)`
- Custom error types when extra context needed
- Trace errors via `libOpentelemetry.HandleSpanError(&span, "msg", err)`

**Example**:
```go
// Package-level sentinels
var (
    ErrContextNotFound = errors.New("reconciliation context not found")
    ErrInvalidRule     = errors.New("invalid match rule")
)

// Wrapping
if err := repo.Save(ctx, entity); err != nil {
    return nil, fmt.Errorf("save match group: %w", err)
}

// Checking
if errors.Is(err, ErrContextNotFound) {
    return fiber.NewError(fiber.StatusNotFound, "Context not found")
}
```

**Nil Checks vs Asserters**:

Use **simple `if x == nil`** for:
- Nil receiver checks at method start (fast path, no context available)
- Dependency injection validation in constructors (return sentinel errors)
- Infrastructure/adapter layer checks

Use **`pkg/assert`** for:
- Domain entity invariant validation (benefits from observability)
- Business rule validation with structured context
- Multiple sequential validations in constructors

```go
// Simple nil check - service layer dependency injection
if jobRepo == nil {
    return nil, ErrNilJobRepository
}

// Asserter - domain entity validation with observability
asserter := assert.New(ctx, nil, "matcher", "match_item.new")
if err := asserter.NotNil(ctx, txID, "transaction id required"); err != nil {
    return nil, fmt.Errorf("match item transaction id: %w", err)
}
```

### 6. Multi-Tenancy

**CRITICAL SECURITY PATTERN**:
- Tenant info (`tenantID`, `tenantSlug`) ONLY from JWT claims via context
- NEVER accept tenant identifiers in request payloads, path params, query params, or headers
- Extract via `auth.GetTenantID(ctx)` or `auth.GetTenantSlug(ctx)`
- Apply schema in transactions via `auth.ApplyTenantSchema(ctx, tx)`
- Use `pgcommon.WithTenantTxProvider` wrapper for automatic isolation
- If auth disabled or claims missing, run in single-tenant mode

### 7. Testing Patterns

**Build tags** (required at top of file):
```go
//go:build unit

//go:build integration

//go:build e2e
```

**Test structure**:
- Co-locate tests with source (`*_test.go`)
- Use testify assertions
- Mock dependencies with interfaces
- Use `sqlmock` for database tests
- Use `testcontainers` for integration tests
- Every `.go` file must have a corresponding `_test.go` (enforced by `make check-tests`)

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
- `{name}.go` — model structs and domain↔DB conversions
- `{name}.postgresql.go` — repository implementation
- `{name}.postgresql_test.go` — postgres adapter tests (unit with sqlmock OR integration with testcontainers; build tag is the discriminator)
- `{name}_sqlmock_test.go` — sqlmock-based unit tests (build tag: `unit`)
- `errors.go` — adapter-specific sentinel errors

**Exceptions** (flat layout, files directly in `postgres/` without subdirectory):
- `reporting/adapters/postgres/` — flat multi-repository, read-only projections
- `shared/adapters/postgres/common/` — utility functions
- `governance/adapters/postgres/` — audit_log adapter (flat, no model file; repository + errors only)
- `outbox/adapters/postgres/` — outbox adapter (flat, repository only)

### Command/Query Services
- Commands: `*_commands.go` (entity-grouped, plural) — e.g., `match_group_commands.go`, `adjustment_commands.go`
- Queries: `*_queries.go` (entity-grouped, plural) — e.g., `dashboard_queries.go`
- Entry point: `commands.go` (UseCase struct, constructor, shared errors)

### HTTP Handlers
Split into `handlers_{feature}.go` when a context has 3+ distinct feature areas:
- e.g., `handlers_run.go`, `handlers_manual.go`, `handlers_adjustment.go`

### Test Files
- Build tags (`//go:build unit`, `//go:build integration`) are the authoritative discriminator
- `_sqlmock_test.go` suffix for SQL mock-based unit tests
- `_mock_test.go` suffix for other mock-based unit tests (e.g., RabbitMQ)
- Test files are NOT merged even when source files are consolidated

## Gotchas & Non-Obvious Patterns

### 1. Never Panic in Production
- Use `pkg/assert` which returns errors instead
- Domain validation returns `(*T, error)` from constructors
- Use `pkg/runtime` for panic recovery in critical paths

### 2. Tenant Isolation is Non-Negotiable
- PostgreSQL `search_path` sets schema per request
- Transactions MUST apply tenant schema via `auth.ApplyTenantSchema(ctx, tx)`
- Repository methods extract tenant from context, never from parameters
- Prevents tenant spoofing attacks

### 3. Audit Trail is Append-Only
- NEVER UPDATE or DELETE from audit tables
- Use outbox pattern for reliable event publication
- Events immutable after creation

### 4. CQRS File Naming
- Write operations: `*_commands.go` (e.g., `match_group_commands.go`, `adjustment_commands.go`)
- Read operations: `*_queries.go` (e.g., `dashboard_queries.go`)
- Helps identify command/query separation at a glance

### 5. Observability is Everywhere
- Start spans at service boundaries: `ctx, span := tracer.Start(ctx, "name")`
- Always `defer span.End()`
- Log errors with structured fields: `logger.WithFields(...).Errorf(...)`
- Use `libCommons.NewTrackingFromContext(ctx)` to extract logger, tracer, headerID

### 6. Transaction Management
- Always provide `WithTx` variants for repositories
- Use `pgcommon.WithTenantTxProvider` for automatic tenant isolation + transaction management
- Keep transactions short and deterministic
- Avoid long-running operations inside transactions

### 7. Idempotency Keys
- Use `sharedhttp.IdempotencyAdapter` for POST/PUT operations
- Keys stored in Redis with configurable TTL
- Prevents duplicate mutations from client retries

### 8. Migration Naming
- Sequential versioning: `000001_init_schema.up.sql`, `000001_init_schema.down.sql`
- Descriptive names: `000015_add_custom_source_type.up.sql`
- Always provide `.down.sql` for rollback
- Test both up and down before merging

### 9. Air Live Reload
- Watches `*.go`, `*.sql` files
- Excludes `*_test.go`, `vendor/`, `docs/`, `migrations/`
- Builds to `tmp/main`
- Configure via `.air.toml`

### 10. Docker Compose Command Detection
- Makefile auto-detects `docker compose` vs `docker-compose`
- Uses `$(DOCKER_CMD)` variable
- No need to specify which version you have

## Configuration

### Environment Variables

All env vars are documented in `config/.env.example` and loaded via `internal/bootstrap/config.go`.

**Categories**:
- **Application**: `ENV_NAME`, `LOG_LEVEL`, `SERVER_ADDRESS`, `HTTP_BODY_LIMIT_BYTES`
- **CORS**: `CORS_ALLOWED_ORIGINS`, `CORS_ALLOWED_METHODS`, `CORS_ALLOWED_HEADERS`
- **TLS**: `SERVER_TLS_CERT_FILE`, `SERVER_TLS_KEY_FILE`
- **Tenancy**: `DEFAULT_TENANT_ID`, `DEFAULT_TENANT_SLUG`
- **PostgreSQL**: `POSTGRES_HOST`, `POSTGRES_PORT`, `POSTGRES_USER`, `POSTGRES_PASSWORD`, `POSTGRES_DB`, `POSTGRES_SSLMODE`, `POSTGRES_MAX_OPEN_CONNS`, `POSTGRES_MAX_IDLE_CONNS`
- **PostgreSQL Replica**: `POSTGRES_REPLICA_HOST`, `POSTGRES_REPLICA_PORT`, `POSTGRES_REPLICA_USER`, `POSTGRES_REPLICA_PASSWORD`, `POSTGRES_REPLICA_DB`, `POSTGRES_REPLICA_SSLMODE`
- **Redis**: `REDIS_HOST`, `REDIS_MASTER_NAME`, `REDIS_PASSWORD`, `REDIS_DB`, `REDIS_PROTOCOL`, `REDIS_TLS`, `REDIS_CA_CERT`, `REDIS_POOL_SIZE`, `REDIS_MIN_IDLE_CONNS`, `REDIS_READ_TIMEOUT_MS`, `REDIS_WRITE_TIMEOUT_MS`, `REDIS_DIAL_TIMEOUT_MS`
- **RabbitMQ**: `RABBITMQ_URI`, `RABBITMQ_HOST`, `RABBITMQ_PORT`, `RABBITMQ_USER`, `RABBITMQ_PASSWORD`, `RABBITMQ_VHOST`, `RABBITMQ_HEALTH_URL`
- **Auth**: `AUTH_ENABLED`, `AUTH_SERVICE_ADDRESS`, `AUTH_JWT_SECRET`
- **OpenTelemetry**: `ENABLE_TELEMETRY`, `OTEL_LIBRARY_NAME`, `OTEL_RESOURCE_SERVICE_NAME`, `OTEL_RESOURCE_SERVICE_VERSION`, `OTEL_EXPORTER_OTLP_ENDPOINT`, `OTEL_RESOURCE_DEPLOYMENT_ENVIRONMENT`

### Database Connection Patterns

**Primary/Replica Setup**:
- Write operations use primary connection
- Read operations can use replica (via lib-uncommons)
- Connection pooling configured via `POSTGRES_MAX_OPEN_CONNS`, `POSTGRES_MAX_IDLE_CONNS`

**Redis Modes**:
- Standalone: Set `REDIS_HOST`
- Sentinel: Set `REDIS_MASTER_NAME` + `REDIS_HOST` (comma-separated sentinels)
- Cluster: Handled by lib-uncommons based on `REDIS_PROTOCOL`

## Required Libraries

### Lerian-Specific

- **lib-auth/v2**: Authentication/authorization middleware
  - JWT extraction: `auth.GetTenantID(ctx)`, `auth.GetTenantSlug(ctx)`
  - Authorization: `auth.Authorize(serviceName, "resource", "action")`
  - Apply tenant schema: `auth.ApplyTenantSchema(ctx, tx)`

- **lib-uncommons**: Common utilities, telemetry, infrastructure
  - Tracking: `libCommons.NewTrackingFromContext(ctx)` → logger, tracer, headerID
  - OpenTelemetry: `libOpentelemetry.HandleSpanError(&span, "msg", err)`
  - Database: `database.NewPostgreSQLConnection()`, `database.NewPrimaryReplicaConnection()`
  - Redis: `redis.NewRedisClient()`
  - Messaging: `messaging.NewRabbitMQ()`

### Internal Packages

- **pkg/assert**: Safe assertions without panics
  - `asserter.That(ctx, condition, "msg", kv...)` → error
  - `asserter.NotEmpty(ctx, value, "msg")` → error
  - `asserter.NotNil(ctx, value, "msg")` → error

- **pkg/runtime**: Panic recovery and runtime observability
  - `runtime.RecoverAndLog(logger, name)` for deferred panic handling with logging
  - `runtime.RecoverAndLogWithContext(ctx, logger, component, name)` for context-aware recovery with metrics/tracing
  - `runtime.RecoverAndCrash(logger, name)` for critical paths that must crash on panic
  - `runtime.RecoverWithPolicy(logger, name, policy)` for configurable recovery policies (`CrashProcess` or log-and-continue)
  - `runtime.HandlePanicValue(ctx, logger, panicValue, component, name)` for framework-recovered panics (e.g., Fiber middleware)
  - All `*WithContext` variants record metrics, span events, and error reports

## Linting & Security

### golangci-lint Configuration

The project uses 75+ linters organized into categories:
- **Security**: gosec, bidichk
- **Bugs**: errcheck, govet, staticcheck
- **Unused code**: ineffassign, unparam, unused, wastedassign
- **Error handling**: err113, errchkjson, errname, errorlint, nilerr, wrapcheck
- **Performance**: bodyclose, perfsprint, prealloc
- **Complexity**: cyclop, gocognit, gocyclo, nestif
- **Style**: goconst, gocritic, misspell, revive, whitespace, gofumpt, gci
- **Context**: contextcheck, noctx
- **Database**: rowserrcheck, sqlclosecheck
- **Testing**: paralleltest, thelper, tparallel, testifylint
- **Nil safety**: nilnil, funlen, godot

### Security Rules (forbidigo)

The following patterns are **blocked** by linter:

```yaml
# Multi-tenancy - NEVER accept tenant from request
- json:"tenant_id"      # Use auth.GetTenantID(ctx)
- .Params("tenantId")   # Use auth.GetTenantID(ctx)
- .Query("tenant")      # Use auth.GetTenantID(ctx)

# Timestamps - ALWAYS use UTC
- time.Now()[^.]        # Use time.Now().UTC()

# SQL injection - ALWAYS use parameterized queries
- fmt.Sprintf.*%s.*SQL  # Use $1, $2, ... placeholders

# Error wrapping - ALWAYS preserve error chain
- fmt.Errorf.*%v.*err   # Use %w not %v
```

### Custom Linters (tools/linters/)

Run with `make lint-custom`. These enforce Matcher-specific patterns:

| Linter | What it checks |
|--------|----------------|
| `entityconstructor` | `New<Type>(ctx, ...) (*Type, error)` pattern |
| `observability` | `NewTrackingFromContext` + span creation |
| `repositorytx` | Write methods have `*WithTx` variants |

### Common Lint Fixes

- **Unchecked errors**: Always check `err` return values
- **Context propagation**: Pass `ctx` through call chains
- **Error wrapping**: Use `fmt.Errorf("msg: %w", err)` not `fmt.Errorf("msg: %v", err)`
- **SQL cleanup**: Defer `rows.Close()` and check `rows.Err()`
- **HTTP body close**: Defer `resp.Body.Close()`
- **Test parallelism**: Call `t.Parallel()` in table tests
- **UTC timestamps**: Use `time.Now().UTC()` not `time.Now()`
- **Import order**: stdlib → third-party → Lerian → project

## Key Documentation

| Document | Purpose |
|----------|---------|
| [CLAUDE.md](CLAUDE.md) | Main agent instruction file (this file's companion) |
| [docs/PROJECT_RULES.md](docs/PROJECT_RULES.md) | Critical architectural rules and constraints |
| [README.md](README.md) | User-facing project overview |
| [config/.env.example](config/.env.example) | Environment variable reference |
| [docs/pre-dev/matcher/prd.md](docs/pre-dev/matcher/prd.md) | Product requirements, user stories |
| [docs/pre-dev/matcher/trd.md](docs/pre-dev/matcher/trd.md) | Technical requirements, security |
| [docs/pre-dev/matcher/api-design.md](docs/pre-dev/matcher/api-design.md) | API contracts, error codes |
| [docs/pre-dev/matcher/data-model.md](docs/pre-dev/matcher/data-model.md) | Entity relationships, indexes |
| [docs/pre-dev/matcher/tasks.md](docs/pre-dev/matcher/tasks.md) | Implementation tasks (32 tasks, 6 epics) |

## CI/CD

### GitHub Actions Workflows

All CI/CD uses shared workflows from `LerianStudio/github-actions-shared-workflows`.
Coverage threshold enforced via workflow parameters (not local config file).

- **go-combined-analysis.yml**: Lint, security scan, unit tests, coverage (via `go-pr-analysis.yml` shared workflow)
  - Runs on PRs to `develop`/`release-candidate`/`main`
  - Go version: 1.25, golangci-lint v2.6.2
  - Coverage threshold: 70%, enforced via `fail_on_coverage_threshold: true`

- **build.yml**: Docker image build and GitOps deployment updates (on tag push)
  - Publishes to DockerHub (`lerianstudio`) and GHCR
  - Updates GitOps values for dev/stg/prd/sandbox environments

- **pr-security-scan.yml**: PR-specific security scanning via shared workflow
  - Runs on PRs to `develop`/`release-candidate`/`main`

- **pr-validation.yml**: PR title/scope validation and auto-labeling
  - Enforces conventional commit format in PR titles
  - Minimum 50-character description, changelog check

- **release.yml**: Automated semantic releases on push to `develop`/`release-candidate`/`main`

### Pre-Commit Checklist

Before pushing code:
1. `make lint` → all checks pass
2. `make lint-custom` → review custom linter warnings
3. `make sec` → no security issues
4. `make test` → all unit tests pass
5. `make test-int` → integration tests pass (if you changed adapters)
6. `make check-tests` → every `.go` has a `_test.go`
7. `make check-test-tags` → test files have proper build tags
8. `make generate-docs` → Swagger docs updated (if you changed API)
9. Commit message follows conventional commits (optional but encouraged)

## Debugging Tips

### Database Issues

- **Tenant isolation failing**: Check `auth.ApplyTenantSchema(ctx, tx)` called in transaction
- **Migration stuck**: Run `SELECT * FROM schema_migrations;` to see current version
- **Connection pool exhausted**: Increase `POSTGRES_MAX_OPEN_CONNS` or check for leaked connections

### Redis Issues

- **Key not found**: Check TTL configuration, verify Redis mode (standalone/sentinel/cluster)
- **Connection errors**: Verify `REDIS_HOST`, `REDIS_PASSWORD`, `REDIS_DB` values

### RabbitMQ Issues

- **Queue not created**: Check RabbitMQ health endpoint, verify `RABBITMQ_VHOST`
- **Messages not consumed**: Check consumer registration, verify queue bindings

### Testing Issues

- **Integration tests fail**: Ensure `make up` ran successfully, check Docker services
- **E2E tests timeout**: Increase timeout with `-timeout=10m` flag
- **Testcontainers hang**: Set `TESTCONTAINERS_RYUK_DISABLED=true` (macOS/Windows)

### Performance Profiling

```bash
# CPU profiling
go test -cpuprofile=cpu.prof -bench=. ./path/to/package
go tool pprof cpu.prof

# Memory profiling
go test -memprofile=mem.prof -bench=. ./path/to/package
go tool pprof mem.prof

# Trace
go test -trace=trace.out ./path/to/package
go tool trace trace.out
```

## Reference Codebases

Located in `.references/` (if available):
- **lib-uncommons**: DB connections, Redis, RabbitMQ, telemetry, graceful shutdown
- **lib-auth**: JWT extraction, authorization middleware patterns
- **midaz**: Hexagonal structure reference, CQRS separation, migration naming

When implementing new features, check reference codebases for established patterns.

## TDD Workflow

Matcher requires TDD (test-driven development):

1. **RED**: Write a failing test that defines desired behavior
2. **GREEN**: Write minimal code to make test pass
3. **REFACTOR**: Improve code structure while keeping tests green

**Every commit should include tests**. Use `make check-tests` to ensure no `.go` file is missing its `_test.go`.

## Common Tasks

### Adding a New API Endpoint

1. Define request/response DTOs in `adapters/http/dto/`
2. Add handler method to `adapters/http/handlers.go`
3. Register route in `adapters/http/routes.go`
4. Add Swagger annotations to handler
5. Run `make generate-docs` to update OpenAPI spec
6. Write unit tests for handler
7. Add integration test in `tests/integration/`

**Note**: The reporting context also includes async export jobs (PDF/CSV generation via object storage) and archive management features, which follow the same endpoint pattern but with background processing.

### Adding a New Domain Entity

1. Create entity in `domain/entities/{name}.go`
2. Define constructor with `New{Name}()` that validates invariants
3. Add business methods for state transitions
4. Create repository interface in `ports/repositories.go`
5. Implement repository in `adapters/postgres/{name}/`
6. Write unit tests for entity behavior
7. Write repository tests with sqlmock

### Adding a New Migration

1. `make migrate-create NAME=descriptive_name`
2. Edit generated `.up.sql` file with schema changes
3. Edit generated `.down.sql` file with rollback logic
4. Test locally: `make migrate-up && make migrate-down && make migrate-up`
5. Verify migrations don't break existing data
6. Add indexes for new foreign keys and filter columns

### Adding a New Use Case

1. Create service file in `services/command/` or `services/query/`
2. Define input struct with required fields
3. Create use case struct with dependencies
4. Implement constructor with dependency validation
5. Add domain-specific method (e.g., `RunMatch(ctx, input)`) rather than generic `Execute()`
6. Start span, extract tracking, orchestrate domain logic
7. Write unit tests with mocked dependencies
8. Wire up in `internal/bootstrap/dependencies.go`

## Support & Community

- **Discord**: [Lerian Studio Community](https://discord.gg/DnhqKwkGv3)
- **GitHub Issues**: [Bug reports & feature requests](https://github.com/LerianStudio/matcher/issues)
- **GitHub Discussions**: [Community discussions](https://github.com/LerianStudio/matcher/discussions)
- **Twitter**: [@LerianStudio](https://twitter.com/LerianStudio)
- **Email**: [contact@lerian.studio](mailto:contact@lerian.studio)

---

**Last Updated**: February 2026  
**Maintained By**: Lerian Studio Engineering Team

For updates to this file, ensure all commands and patterns are validated against the current codebase. Never document hypothetical features or commands.
