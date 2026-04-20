# AGENTS.md — AI Agent Quick Reference

Universal entry point for any AI coding agent working on the Matcher codebase.

## Project Identity

**Matcher** is a transaction reconciliation engine built by Lerian Studio. It automates matching between a ledger (Midaz) and external systems (banks, payment processors, ERPs), applying configurable rules, managing exceptions, and maintaining an immutable audit trail.

- **Language**: Go 1.26 (module `go 1.26.0`, toolchain `1.26.1`)
- **Architecture**: Modular monolith — DDD + Hexagonal Architecture + CQRS-light
- **Database**: PostgreSQL 17, schema-per-tenant isolation
- **Cache/Locking**: Valkey 8 (Redis-compatible)
- **Messaging**: RabbitMQ 4.1 via transactional outbox pattern
- **Object Storage**: S3-compatible (SeaweedFS in dev)
- **License**: Elastic License 2.0

## Quick Start

```bash
make up          # Start Postgres, Redis, RabbitMQ, SeaweedFS, app
make migrate-up  # Apply database migrations
make dev         # Live reload on :4018
make test        # Run unit tests
make lint        # 75+ linters
```

Health: `GET http://localhost:4018/health`

## Architecture Overview

Eight bounded contexts under `internal/`, each following hexagonal structure:

| Context | Role |
|---------|------|
| `configuration` | Reconciliation contexts, sources, match rules, fees, scheduling |
| `discovery` | External data source discovery, schema detection |
| `ingestion` | File parsing (CSV/JSON/XML/ISO 20022), normalization, dedup |
| `matching` | Rule execution, confidence scoring, fee verification |
| `exception` | Exception lifecycle, disputes, evidence, resolutions |
| `governance` | Immutable audit logs, hash chains, archival |
| `reporting` | Dashboard analytics, export jobs (CSV/PDF) |
| `outbox` | Reliable event publication via transactional outbox |

Plus: `auth/` (JWT/RBAC), `bootstrap/` (composition root), `shared/` (shared kernel), `testutil/` (test helpers).

### Per-Context Structure

```
internal/{context}/
├── adapters/          # HTTP (Fiber), Postgres, RabbitMQ
├── domain/            # Entities, value objects, repository interfaces
├── ports/             # External dependency abstractions
└── services/          # command/ (writes), query/ (reads), worker/ (background)
```

### Cross-Context Rules

- Contexts **must not** import each other directly (enforced by depguard)
- Shared types live in `internal/shared/domain/`
- Shared port interfaces in `internal/shared/ports/`
- Cross-context bridge adapters in `internal/shared/adapters/cross/`
- All wiring in `internal/bootstrap/`

## Essential Commands

| Command | Purpose |
|---------|---------|
| `make dev` | Live reload with air |
| `make test` | Unit tests |
| `make test-int` | Integration tests (Docker) |
| `make test-e2e` | E2E tests (full stack) |
| `make test-chaos` | Fault injection (Toxiproxy) |
| `make lint` | golangci-lint (75+ linters) |
| `make lint-custom` | Custom architectural linters |
| `make sec` | gosec security scan |
| `make check-tests` | Verify every .go has _test.go |
| `make check-migrations` | Validate migration pairs |
| `make generate-docs` | Regenerate Swagger docs |
| `make ci` | Full local CI pipeline |

## Code Conventions

### Entity Constructors
```go
func NewEntity(ctx context.Context, ...) (*Entity, error)
```
Validate with `pkg/assert`. Never panic. Return `(*T, error)`.

### Service Methods
```go
func (uc *UseCase) RunMatch(ctx context.Context, input RunMatchInput) (*MatchRun, error) {
    track := libCommons.NewTrackingFromContext(ctx)
    ctx, span := track.Tracer.Start(ctx, "matching.run_match")
    defer span.End()
    // ...
}
```
Domain-specific names. Always start with tracking + span.

### Repository Pattern
- Every write method has a `WithTx` variant
- Use `pgcommon.WithTenantTxProvider` for tenant isolation
- Separate PostgreSQL models from domain entities

### Error Handling
- Sentinels: `var ErrNotFound = errors.New("not found")`
- Wrapping: `fmt.Errorf("context: %w", err)` — always `%w`
- Tracing: `libOpentelemetry.HandleSpanError(span, "msg", err)`

### File Naming
- Commands: `*_commands.go` — Queries: `*_queries.go`
- Postgres adapters: `{name}.postgresql.go`
- Tests: `_sqlmock_test.go`, `_mock_test.go` — build tag is the authority

## Testing Requirements

| Tag | Scope | Run with |
|-----|-------|----------|
| `//go:build unit` | Mocks only | `make test` |
| `//go:build integration` | Testcontainers | `make test-int` |
| `//go:build e2e` | Full stack | `make test-e2e` |
| `//go:build chaos` | Toxiproxy | `make test-chaos` |

- Every `.go` must have a `_test.go` (enforced by `make check-tests`)
- Coverage threshold: **70%** (enforced in CI)
- TDD required: RED -> GREEN -> REFACTOR

## Multi-Tenancy (Security-Critical)

```
NEVER accept tenant from request parameters.
ALWAYS extract from JWT context.
```

- Extract: `auth.GetTenantID(ctx)`, `auth.GetTenantSlug(ctx)`
- Isolate: `auth.ApplyTenantSchema(ctx, tx)` inside every transaction
- Enforce: `pgcommon.WithTenantTxProvider(ctx, provider, fn)`
- Default: `public` schema when auth disabled

This is enforced by `forbidigo` linter rules that block `json:"tenant_id"`, `.Params("tenantId")`, and `.Query("tenant")`.

## PR Standards

- Conventional commit format in PR titles
- Minimum 50-character description
- Run `make lint && make test && make check-tests` before pushing
- Run `make generate-docs` if API changed
- Run `make check-migrations` if migrations changed

## Key Files to Read

| Priority | File | Why |
|----------|------|-----|
| 1 | `AGENTS.md` (this file) | Quick orientation |
| 2 | `CLAUDE.md` | Deep patterns and full conventions |
| 3 | `docs/PROJECT_RULES.md` | Architectural constraints |
| 4 | `.golangci.yml` | Linter rules and architectural boundaries |
| 5 | `config/.config-map.example` | All bootstrap configuration keys |
| 6 | `internal/bootstrap/` | Composition root — how everything is wired |
| 7 | `internal/shared/` | Shared kernel — cross-context types and ports |
| 8 | `docs/multi-tenant-guide.md` | Multi-tenancy implementation |
| 9 | `docs/swagger/swagger.json` | Full API specification |

## What NOT to Do

1. **Never import between bounded contexts** — use `internal/shared/` as the bridge
2. **Never accept tenant IDs from requests** — JWT context only
3. **Never use `%v` for error wrapping** — always `%w`
4. **Never use `time.Now()` without `.UTC()`**
5. **Never panic in production code** — return errors
6. **Never skip `WithTx` variants** on write repository methods
7. **Never use `fmt.Sprintf` with `%s` for SQL** — parameterized queries only
8. **Never put logging/tracing in domain entities**
9. **Never update or delete audit log records** — append-only
10. **Never read Viper directly at runtime** — use systemplane after bootstrap
