![Lerian Matcher Banner](image/README/matcher-banner.png)

<div align="center">

[![Latest Release](https://img.shields.io/github/v/release/LerianStudio/matcher?include_prereleases)](https://github.com/LerianStudio/matcher/releases)
[![License](https://img.shields.io/badge/license-Elastic%20License%202.0-4c1.svg)](LICENSE.md)
[![Go Report](https://goreportcard.com/badge/github.com/lerianstudio/matcher)](https://goreportcard.com/report/github.com/lerianstudio/matcher)
[![Discord](https://img.shields.io/badge/Discord-Lerian%20Studio-%237289da.svg?logo=discord)](https://discord.gg/DnhqKwkGv3)

</div>

# Lerian Matcher

**Matcher** is Lerian Studio's reconciliation engine. It automates transaction matching between [Midaz](https://github.com/LerianStudio/midaz) (Lerian's ledger) and external systems — banks, payment processors, ERPs — applying configurable matching rules, managing exceptions through workflow integrations, and maintaining a complete audit trail for compliance.

## Why Matcher?

| | |
|---|---|
| **Automated Matching** | Reduce manual reconciliation with deterministic rules, tolerance-based matching, and confidence scoring |
| **Configurable Rules** | Define exact and tolerance-based rules, date windows, fee verification, and multi-source matches |
| **Exception Workflows** | Route unresolved items to JIRA, ServiceNow, or webhooks where teams already work |
| **Audit-First** | Append-only audit logging with hash chains for SOX-ready traceability |
| **Multi-Tenant** | Schema-per-tenant isolation in PostgreSQL with JWT-based tenant resolution |
| **Runtime Configurable** | Hot-reloadable settings via systemplane API — no restarts for tuning |

## Reconciliation Pipeline

Matcher implements a complete reconciliation pipeline:

1. **Configure** — Define reconciliation contexts, sources, field maps, match rules, and fee schedules
2. **Ingest** — Import external data (CSV, JSON, XML, ISO 20022), normalize fields, deduplicate
3. **Match** — Execute deterministic and tolerance-based matching, produce match groups with confidence scores
4. **Handle Exceptions** — Classify unmatched items, route to teams, record manual resolutions
5. **Govern** — Maintain immutable audit log, verify hash chain integrity, archive for compliance
6. **Report** — Dashboard analytics, variance analysis, export to CSV/PDF

## Architecture

Matcher is a **modular monolith** built with Domain-Driven Design (DDD), hexagonal architecture, and CQRS-light separation.

### Bounded Contexts

| Context | Purpose |
|---------|---------|
| **Configuration** | Reconciliation contexts, sources, field maps, match rules, fee schedules/rules, scheduling |
| **Discovery** | External data source connection management, schema detection, extraction orchestration |
| **Ingestion** | File parsing (CSV/JSON/XML/ISO 20022), normalization, BOM handling, Redis-based dedup |
| **Matching** | Rule execution, confidence scoring (0-100), fee verification, manual match/unmatch, adjustments, cross-currency |
| **Exception** | Exception lifecycle, disputes with evidence tracking, bulk operations, external dispatch |
| **Governance** | Immutable audit logs, cryptographic hash chain verification, S3 archival |
| **Reporting** | Dashboard metrics, async export jobs (CSV/PDF), streaming reports, Redis caching |
| **Outbox** | Reliable event publication via transactional outbox pattern |

### Technical Highlights

- **Hexagonal Architecture** — Ports and adapters per bounded context with strict import boundaries
- **Schema-Per-Tenant** — PostgreSQL `search_path` isolation with JWT-derived tenant identity
- **Transactional Outbox** — Reliable event publication for ingestion and matching workflows
- **Fee Schedule Engine** — Net-to-gross normalization with parallel and cascading fee application
- **Distributed Locking** — Redis-based locks preventing concurrent match runs
- **Cross-Currency Matching** — FX rate lookups and base-currency normalization
- **Idempotency** — Redis-backed idempotency keys for safe client retries
- **Systemplane** — Runtime configuration authority with hot-reloadable settings, history, and schema API
- **Chaos Testing** — Toxiproxy-based fault injection for resilience validation
- **OpenTelemetry** — Distributed tracing and metrics via lib-commons

## Getting Started

### Prerequisites

- [Go 1.26.0+](https://go.dev/dl/)
- [Docker](https://docs.docker.com/get-docker/) and Docker Compose
- [golang-migrate](https://github.com/golang-migrate/migrate) (for database migrations)

### 1. Clone and Start Infrastructure

```bash
git clone https://github.com/LerianStudio/matcher.git
cd matcher
make up
```

This starts PostgreSQL (primary + replica), Valkey (Redis-compatible), RabbitMQ, SeaweedFS (S3), and the app with live reload.

### 2. Apply Migrations

```bash
make migrate-up
```

### 3. Verify

```bash
curl http://localhost:4018/health
# {"status":"ok"}
```

The API is available at `http://localhost:4018`. Swagger UI is accessible at `http://localhost:4018/swagger/index.html` when running in non-production mode.

### Configuration

No configuration files needed — all defaults are baked into the binary and match the docker-compose setup.

For production, override via environment variables. See [`config/.config-map.example`](config/.config-map.example) for bootstrap-only keys (require restart). Runtime hot-reload is limited to systemplane-managed settings (for example: body limit, rate limits, worker intervals, feature flags, timeouts, export settings, and archival intervals) via `/system/matcher/:key` (`GET`/`PUT`) and `/system/matcher` (list with inline schema metadata). The admin API is a management-plane surface and is intentionally excluded from the public OpenAPI specification.

### Deployment Notes

**GOMEMLIMIT (Go memory hint).** The image does not set `GOMEMLIMIT`. Operators must configure it per-deployment at roughly 90% of the container memory limit (for example, `GOMEMLIMIT=450MiB` for a 500 MiB pod). Go 1.26 auto-detects cgroup CPU via `GOMAXPROCS` but does **not** auto-detect cgroup memory; leaving `GOMEMLIMIT` unset risks OOM-kills from an uncapped Go heap. Kubernetes example:

```yaml
env:
  - name: GOMEMLIMIT
    valueFrom:
      resourceFieldRef:
        resource: limits.memory
        divisor: "1"
```

## Project Structure

```
matcher/
├── cmd/                  # Application entry points
│   ├── matcher/          # Main service binary
│   └── health-probe/     # Health check binary for distroless containers
├── config/               # Environment templates and storage config
├── docs/                 # Design documents and API specs
│   ├── swagger/          # Generated OpenAPI spec (JSON + YAML)
│   ├── multi-tenant-guide.md
│   └── PROJECT_RULES.md
├── internal/             # Core application code (bounded contexts)
│   ├── auth/             # JWT extraction, RBAC, tenant resolution
│   ├── bootstrap/        # Composition root (config, DI, server, systemplane)
│   ├── configuration/    # Reconciliation setup and scheduling
│   ├── discovery/        # External data source discovery
│   ├── ingestion/        # File parsing and normalization
│   ├── matching/         # Core matching engine
│   ├── exception/        # Exception and dispute management
│   ├── governance/       # Audit logs and archival
│   ├── reporting/        # Analytics and exports
│   ├── outbox/           # Transactional outbox
│   ├── shared/           # Shared kernel (cross-context types and ports)
│   └── testutil/         # Shared test helpers
├── migrations/           # PostgreSQL schema migrations (21 migrations)
├── pkg/                  # Reusable library packages
│   ├── chanutil/         # Safe channel utilities
│   └── storageopt/       # Object storage functional options
├── scripts/              # Dev and CI utility scripts
├── tests/                # Integration, E2E, chaos, and static analysis tests
└── tools/                # Custom linters and dev tooling
```

## Development

### Common Commands

| Command | Purpose |
|---------|---------|
| `make dev` | Live reload with [air](https://github.com/air-verse/air) |
| `make build` | Build binary to `bin/matcher` |
| `make test` | Run unit tests |
| `make test-int` | Integration tests (requires Docker) |
| `make test-e2e` | End-to-end tests (requires full stack) |
| `make test-chaos` | Fault injection tests (Toxiproxy) |
| `make lint` | Linting (75+ linters via golangci-lint) |
| `make lint-custom` | Custom architectural linters |
| `make sec` | Security scanning (gosec) |
| `make ci` | Full local CI pipeline |

### Infrastructure

| Service | Image | Port |
|---------|-------|------|
| PostgreSQL (primary) | `postgres:17` | 5432 |
| PostgreSQL (replica) | `postgres:17` | 5433 |
| Valkey (Redis) | `valkey/valkey:8` | 6379 |
| RabbitMQ | `rabbitmq:4.1.3-management-alpine` | 5672 (AMQP), 15672 (UI) |
| SeaweedFS (S3) | `chrislusf/seaweedfs:3.80` | 8333 (S3), 9333 (Master) |
| Matcher App | `golang:1.26.1-alpine` | 4018 |

### Testing

Matcher uses TDD (Test-Driven Development) with four test tiers:

- **Unit** (`make test`): Pure logic tests with mocks — no external dependencies
- **Integration** (`make test-int`): Real containers via testcontainers
- **E2E** (`make test-e2e`): Full-stack journey tests against running services
- **Chaos** (`make test-chaos`): Fault injection with Toxiproxy (latency, connection loss, partitions)

Coverage threshold: **70%**, enforced in CI.

## API Documentation

The full OpenAPI specification is available at [`docs/swagger/swagger.json`](docs/swagger/swagger.json).

When running in development mode, Swagger UI is accessible at `/swagger/index.html`.

Key API areas:
- **Reconciliation Contexts** — Create and manage reconciliation configurations
- **Sources & Field Maps** — Define data sources and normalization rules
- **Match Rules & Fee Schedules** — Configure matching logic and fee verification
- **Ingestion** — Upload and parse external transaction data
- **Matching** — Execute match runs, manage match groups, handle adjustments
- **Exceptions & Disputes** — Manage exceptions, create disputes, collect evidence
- **Governance** — Query audit logs, verify hash chains, manage archives
- **Reporting** — Dashboard metrics, export jobs, variance analysis
- **System** — Health checks, runtime configuration (systemplane)

## Contributing

We welcome contributions from the community! Here's how to get started:

1. **Fork** the repository
2. **Create a branch** from `develop` (e.g., `feat/my-feature`)
3. **Follow conventions**: Run `make lint && make test && make check-tests` before pushing
4. **Submit a PR** to `develop` with a conventional commit title and a description of at least 50 characters

### PR Requirements

- Conventional commit format in PR titles (e.g., `feat: add batch matching endpoint`)
- Every `.go` file must have a corresponding `_test.go`
- Test build tags required (`//go:build unit`, etc.)
- Run `make generate-docs` if you changed any API endpoint

### Code Standards

- Go 1.26 with 75+ linters enforced
- Hexagonal architecture: adapters, ports, domain, services
- CQRS separation: `*_commands.go` for writes, `*_queries.go` for reads
- Multi-tenancy via JWT context — never accept tenant IDs from request parameters
- Error wrapping with `%w`, UTC timestamps, parameterized SQL queries

For detailed conventions, see [`docs/PROJECT_RULES.md`](docs/PROJECT_RULES.md).

## Community & Support

- **Discord**: [Join our community](https://discord.gg/DnhqKwkGv3) for discussions, support, and updates
- **GitHub Issues**: [Bug reports & feature requests](https://github.com/LerianStudio/matcher/issues)
- **GitHub Discussions**: [Community Q&A](https://github.com/LerianStudio/matcher/discussions)
- **Twitter/X**: [@LerianStudio](https://twitter.com/LerianStudio)
- **Email**: [contact@lerian.studio](mailto:contact@lerian.studio)

## License

Matcher is licensed under the [Elastic License 2.0](LICENSE.md).

## About Lerian

Matcher is developed by [Lerian](https://lerian.studio), a technology company founded in 2024, led by a team with deep experience in ledger and core banking systems. Matcher is part of the Lerian Studio ecosystem alongside [Midaz](https://github.com/LerianStudio/midaz) (open-source ledger).
