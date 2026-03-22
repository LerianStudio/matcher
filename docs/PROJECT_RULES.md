# Project Rules

This project follows Lerian Studio Ring standards for Go services.

## Architecture

- Domain entities should remain pure logic (no logging/tracing).
- Only change infra/config (Docker, compose, env templates) in explicit DevOps tasks.
- Modular monolith with bounded contexts under `internal/{context}`.
- Hexagonal Architecture per context:
  - `adapters/`: infrastructure and transports (HTTP, DB, MQ).
  - `domain/`: pure business logic and entities.
  - `ports/`: interfaces for dependencies.
  - `services/`: application use cases (command/query).
- CQRS separation: write in `services/command`, read in `services/query`.
- Command files end with `_commands.go`; query files end with `_queries.go`.
- Keep domain models rich: enforce invariants in entities/value objects, not in adapters.
- Entities must expose state transitions via methods; avoid direct status mutation in services/adapters.
- Constructors validate invariants and return `(*T, error)` when creation can fail.
- Use value objects/enums with `Valid`/`IsValid` + parse helpers for critical types.
- Defensive copy caller-owned maps/slices before storing in entities.
- Identity fields (`ID`, `ContextID`, `TenantID`, `SourceID`) are immutable after creation.
- Domain invariants use `pkg/assert`; validation tags are only for inbound DTOs.
- **Nil checks vs asserters**:
  - Use simple `if x == nil` for: nil receiver checks, dependency injection validation (with sentinel errors), adapter layer.
  - Use `pkg/assert` for: domain entity invariant validation (observability), business rule validation with structured context.
- Avoid duplicating domain concepts between shared kernel and contexts; pick a single source of truth and map explicitly.
- Put logic in entities when it only needs entity fields; use domain services for multi-aggregate or external dependency coordination.
- Keep services small and single-responsibility; avoid god services.
- Avoid cross-context adapter imports; depend on ports instead.
- Prefer explicit state (enums) over implicit derivation for critical domain status.
- Avoid panics in all production paths.

## Required Libraries

- AuthN/AuthZ: `github.com/LerianStudio/lib-auth/v2` only.
- Commons/Telemetry: `github.com/LerianStudio/lib-commons`.
- Assertions: `github.com/LerianStudio/matcher/pkg/assert` (no panics).
- Use lib-commons submodules for:
  - Tracking/logging (`libCommons.NewTrackingFromContext`).
  - OpenTelemetry (`commons/opentelemetry`).
  - Database connections (`database`).
  - Redis clients (`redis`).
  - Messaging (RabbitMQ) (`messaging`).
- Do not introduce custom DB/Redis/MQ clients outside lib-commons wrappers.

## Context + Observability

- Always use `libCommons.NewTrackingFromContext(ctx)` for logger/tracer/header data.
- Start a span per service method: `ctx, span := tracer.Start(ctx, "<name>")` and `defer span.End()`.
- Call `libOpentelemetry.HandleSpanError(&span, "message", err)` before returning errors.
- Ensure adapters handle nil tracers/loggers gracefully (provide fallbacks) to support testing contexts.
- Use structured logging (`logger.WithFields(...)`) for error context.
- Do not log inside domain entities/value objects.

## Data + Multi-tenancy

- Use `common.WithTenantTx(ctx, conn, fn)` in repositories.
- Apply schema via `auth.ApplyTenantSchema(ctx, tx)` inside transaction helpers.
- If tenant claims missing and auth disabled, run in single-tenant mode.
- **CRITICAL**: Tenant info (`tenantID`, `tenantSlug`) must ONLY come from JWT claims via context.
  - NEVER accept tenant identifiers in request payloads, path params, query params, or custom headers.
  - Repository methods must extract tenant from context via `auth.GetTenantID(ctx)`, never as function parameters.
  - This prevents tenant spoofing attacks where malicious actors could access other tenants' data.

## Database

- PostgreSQL repositories should mirror patterns in `internal/configuration/adapters/postgres`.
- Ensure SQL queries respect tenant isolation.
- Add indexes for join/filter keys in migrations.
- Keep migrations additive; avoid destructive changes in production.
- Enforce referential integrity with foreign keys where applicable.
- Avoid long-running transactions; keep write paths short and deterministic.
- Prefer read replicas for query services when supported by lib-commons.

## Testing

- TDD (RED -> GREEN -> REFACTOR) required.
- Use `testify` and `sqlmock` as needed.
- Maintain strict build tag formatting (`//go:build unit`) at file headers.
- No tests should rely on external services unless marked integration.

## Tooling

- Required Make targets: `dev`, `test`, `test-int`, `lint`, `lint-custom`, `security`, `build`, `docker-build`, `generate`, `migrate-up`, `migrate-down`, `migrate-create`.

## Linting

### Standard Linters (golangci-lint)

Run with `make lint`. Configuration in `.golangci.yml`.

**Key enforced rules:**

- **Multi-tenancy security**: Never accept tenant identifiers in request payloads, path params, or query params. Use `auth.GetTenantID(ctx)`.
- **UTC timestamps**: Always use `time.Now().UTC()` for consistent timezone handling.
- **SQL injection prevention**: Use parameterized queries (`$1, $2, ...`), never `fmt.Sprintf` with `%s` for SQL.
- **Error wrapping**: Use `%w` not `%v` when wrapping errors with `fmt.Errorf`.
- **Import organization**: stdlib → third-party → Lerian libs → project (enforced by `gci`).
- **Stricter formatting**: `gofumpt` for consistent formatting beyond `gofmt`.

### Custom Linters (Matcher-specific)

Run with `make lint-custom`. Source in `tools/linters/`.

**Entity Constructor Pattern** (`entityconstructor`):

- Constructors must be named `New<EntityName>`
- First parameter must be `context.Context`
- Return type must be `(*EntityName, error)`
- Example: `func NewMatchGroup(ctx context.Context, ...) (*MatchGroup, error)`

**Observability Pattern** (`observability`):

- Service `Execute`/`Run`/`Handle` methods must call `NewTrackingFromContext(ctx)`
- Must create span with `tracer.Start(ctx, "operation.name")`
- Must `defer span.End()` for cleanup

**Repository Transaction Pattern** (`repositorytx`):

- Write methods (`Create`, `Update`, `Delete`, etc.) must have `*WithTx` variants
- Example: `Create()` requires `CreateWithTx()`
- WithTx variants accept transaction parameter for atomic operations

### IDE Integration

VSCode settings in `.vscode/settings.json` configure:

- golangci-lint as the linter
- gofumpt as the formatter
- Format and organize imports on save

## File Naming Conventions

### Postgres Adapter Files

Every aggregate-based postgres adapter directory uses Pattern A:
| File | Purpose |
|------|---------|
| `{name}.go` | Model structs, domain↔DB conversions |
| `{name}.postgresql.go` | Repository implementation |
| `errors.go` | Adapter-specific sentinel errors |

**Exceptions** (flat layout, files directly in `postgres/` without subdirectory):

- `reporting/adapters/postgres/` — flat multi-repository, read-only projections
- `shared/adapters/postgres/common/` — utility functions
- `governance/adapters/postgres/` — audit_log adapter (flat, no model file; repository + errors only)
- `outbox/adapters/postgres/` — outbox adapter (flat, repository only)

### Command/Query Service Files

- Use plural suffix: `*_commands.go`, `*_queries.go`
- Files are entity-grouped: each `*_commands.go` contains ALL write operations for an aggregate/entity group, not one file per operation. Example: `match_group_commands.go` contains RunMatch, ManualMatch, and Unmatch; `adjustment_commands.go` contains all adjustment operations.
- Entry point file: `commands.go` or `queries.go` (UseCase struct, constructor, shared errors/sentinels)

### Handler Splitting

Split handlers into `handlers_{feature}.go` when a context has 3+ distinct feature areas. A "feature area" is a logical grouping of endpoints that share a common concern. Examples from the matching context: match execution (`handlers_run.go`), manual matching (`handlers_manual.go`), adjustments (`handlers_adjustment.go`), match group queries (`handlers_group.go`).

## Test Organization

### Build Tags

Build tags are the authoritative test type discriminator:

- `//go:build unit` — unit tests (mocks, no external deps)
- `//go:build integration` — integration tests (testcontainers)
- `//go:build e2e` — end-to-end tests (full stack)

### Test File Naming

- `{name}_sqlmock_test.go` — SQL mock-based unit tests
- `{name}_mock_test.go` — other mock-based unit tests
- `{name}.postgresql_test.go` — postgres adapter tests (unit with sqlmock OR integration with testcontainers; build tag is the discriminator)
- Do NOT merge test files when consolidating source files

## Dependency Maintenance Notes

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

## Misc

- Avoid one-letter variable names unless required.
- Do not add inline comments unless requested.
