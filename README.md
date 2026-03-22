![banner](image/README/matcher-banner.png)

<div align="center">

[![Latest Release](https://img.shields.io/github/v/release/LerianStudio/matcher?include_prereleases)](https://github.com/LerianStudio/matcher/releases)
[![License](https://img.shields.io/badge/license-Elastic%20License%202.0-4c1.svg)](LICENSE)
[![Go Report](https://goreportcard.com/badge/github.com/lerianstudio/matcher)](https://goreportcard.com/report/github.com/lerianstudio/matcher)
[![Discord](https://img.shields.io/badge/Discord-Lerian%20Studio-%237289da.svg?logo=discord)](https://discord.gg/DnhqKwkGv3)

</div>

# Lerian Matcher: Reconciliation Engine

Matcher is Lerian Studio's reconciliation engine that automates transaction matching between Midaz (Lerian's ledger) and external systems like banks, payment processors, and ERPs. It applies configurable matching rules, identifies exceptions, routes unresolved items to workflow tools, and maintains a complete audit trail for compliance.

## Why Matcher?

- **Operations-Ready**: Reduce manual reconciliation time with automated matching and confidence scoring
- **Configurable Rules**: Define exact and tolerance-based rules, date windows, and multi-source matches
- **Workflow Integration**: Route exceptions to JIRA, ServiceNow, or webhooks where teams already work
- **Audit-First**: Append-only audit logging for SOX-ready traceability

## Core Reconciliation

At Lerian, reconciliation is a full pipeline that spans configuration, ingestion, matching, and governance:

1. **Configuration**: Define reconciliation contexts, sources, field maps, and match rules.
2. **Ingestion**: Import external data (CSV, JSON, XML, or integrations), normalize fields, and deduplicate.
3. **Matching**: Run deterministic and tolerance-based matching to produce match groups and confidence scores.
4. **Exception Management**: Classify unmatched items, route them to teams, and record manual resolutions.
5. **Governance**: Maintain an immutable audit log and reporting views for compliance.
6. **Reporting**: Dashboard analytics, export jobs, and variance analysis.

Matcher integrates with Midaz and third-party systems through inbound APIs, outbound webhooks, and message-driven workflows.

## Core Architecture

Matcher is built as a modular monolith with DDD, hexagonal architecture, and CQRS-light separation of command and query use cases.

### Domains

- **Configuration**: Reconciliation contexts, sources, field maps, match rules, fee schedules/rules, scheduling, clone operations
- **Ingestion**: Import jobs, file parsing (CSV/JSON/XML/ISO 20022), normalization, deduplication, auto-match trigger
- **Matching**: Match orchestration, rule execution, fee verification, confidence scoring, manual match/unmatch, adjustments, currency conversion
- **Exception**: Exception lifecycle, disputes with evidence tracking, resolution workflows, force-match, bulk operations, external dispatch (JIRA/webhooks)
- **Governance**: Immutable audit logs, hash chain verification, actor mapping, archive management
- **Reporting**: Dashboard analytics, export jobs (CSV/PDF), streaming reports, Redis caching, variance analysis
- **Discovery**: External data source discovery, schema detection, extraction management
- **Outbox**: Reliable event publication via transactional outbox pattern

### Services

1. **Configuration and Ingestion**: REST APIs for setup, fee schedule/rule management, scheduling, clone operations, and data onboarding with PostgreSQL persistence.
2. **Matching Engine**: Rule execution, confidence scoring, fee verification, manual matching, adjustments, currency conversion, and dry-run previews.
3. **Exception and Dispute Management**: Exception classification, dispute lifecycle, evidence collection, force-match, adjust-entry, bulk operations, and external dispatch (JIRA/webhooks).
4. **Discovery**: External data source connection management, schema detection, and extraction orchestration.
5. **Governance, Reporting, and Exports**: Immutable audit trail with hash chains, dashboard analytics with Redis caching, async export jobs (CSV/PDF), streaming reports, and audit log archival.
6. **Infrastructure Layer**: PostgreSQL (with replica support), Redis (dedupe/locks/idempotency/caching), RabbitMQ (async workflows), S3-compatible object storage (exports/archives), OpenTelemetry, Systemplane (runtime configuration).

### Reconciliation Processing

- **Upload or fetch data** into ingestion jobs
- **Normalize and map fields** based on configured sources
- **Deduplicate** records using Redis-based hashing
- **Persist** jobs and transactions with outbox events
- **Execute matching jobs** and emit match results
- **Route exceptions** and capture audit events

### Technical Highlights

- **Hexagonal Architecture**: Ports and adapters per bounded context
- **CQRS-Light**: Separate command and query use cases for clarity and performance
- **Schema-Per-Tenant**: PostgreSQL search_path isolation per tenant
- **Outbox Pattern**: Reliable event publication for ingestion and matching workflows
- **OpenTelemetry**: Standard tracing and metrics via lib-commons
- **Systemplane**: Runtime configuration authority with hot-reloadable settings, history, and schema
- **Fee Schedule Engine**: Net-to-gross normalization with parallel and cascading fee application
- **Export Pipeline**: Async export jobs (CSV/PDF) with S3-compatible object storage and presigned URLs
- **Rate Limiting**: Configurable rate limits per operation type
- **Idempotency**: Redis-backed idempotency keys for safe client retries
- **Distributed Locking**: Redis-based locks for concurrent match run prevention
- **Cross-Currency Matching**: FX rate lookups and base-currency normalization
- **Chaos Testing**: Toxiproxy-based fault injection for resilience validation

## Project Structure

```
matcher/
├── cmd/                  # Application entry points (matcher, health-probe)
├── config/               # Environment templates and storage config
├── docs/                 # Design documents, PRD, TRD, API specs
├── internal/             # Core application code (bounded contexts)
│   ├── auth/             # Authentication, authorization, multi-tenancy
│   ├── bootstrap/        # Service initialization, dependency wiring, systemplane
│   ├── configuration/    # Reconciliation contexts, sources, rules, fees, scheduling
│   ├── discovery/        # External data source discovery and schema detection
│   ├── ingestion/        # File parsing, normalization, deduplication
│   ├── matching/         # Match engine, rule execution, scoring, adjustments
│   ├── exception/        # Disputes, evidence, resolution workflows, bulk ops
│   ├── governance/       # Immutable audit logs, hash chains, archival
│   ├── reporting/        # Analytics, exports (CSV/PDF), dashboard caching
│   ├── outbox/           # Transactional outbox pattern
│   ├── shared/           # Cross-context domain objects, fee engine, adapters
│   └── testutil/         # Shared test helpers
├── migrations/           # PostgreSQL schema migrations (19 migrations)
├── pkg/                  # Reusable library packages
│   ├── chanutil/         # Safe channel utilities
│   ├── storageopt/       # Object storage functional options
│   └── systemplane/      # Runtime configuration authority (control plane)
├── scripts/              # Dev and CI utility scripts
├── tests/                # Integration, E2E, chaos, and static analysis tests
└── tools/                # Custom linters and dev tooling
```

Each directory contains a README with detailed documentation.

## Getting Started

### 1) Configure (Optional)

No configuration files needed — all defaults are baked into the binary and match the docker-compose setup.

For production, override via env vars. See `config/.config-map.example` for bootstrap-only keys (require restart). All other settings are hot-reloadable via the systemplane API (`/v1/system/configs`).

### 2) Start Infrastructure

```bash
docker-compose up -d postgres redis rabbitmq
```

Optional object storage for exports and archival:

```bash
docker-compose --profile storage up -d seaweedfs
```

Optional observability stack (requires OpenTelemetry Collector configured via `OTEL_EXPORTER_OTLP_ENDPOINT`).

### 3) Run Migrations

```bash
make migrate-up
```

For production rollout and rollback procedures, see `docs/migrations/PRODUCTION_MIGRATIONS.md`.

### 4) Run the Service

```bash
make dev
```

Health endpoints:

- `GET /health`
- `GET /ready`

### Common Commands

- `make test` — Unit tests
- `make test-int` — Integration tests (requires Docker)
- `make test-e2e` — End-to-end tests (requires full stack)
- `make test-e2e-journeys` — Journey-based E2E tests
- `make test-chaos` — Chaos/fault-injection tests (requires Toxiproxy)
- `make test-all` — All tests with merged coverage
- `make lint` — Linting (75+ linters)
- `make lint-custom` — Custom architectural linters
- `make sec` — Security scanning
- `make build` — Build binary

## Community & Support

- Join our [Discord community](https://discord.gg/DnhqKwkGv3) for discussions, support, and updates.
- For bug reports and feature requests, please use our [GitHub Issues](https://github.com/LerianStudio/matcher/issues).
- If you want to raise anything to the attention of the community, open a Discussion in our [GitHub](https://github.com/LerianStudio/matcher/discussions).
- Follow us on [Twitter](https://twitter.com/LerianStudio) for the latest news and announcements.

## Repo Activity

Repobeats badge coming soon.

## Contributing & License

We welcome contributions from the community. Contributing guidelines and license information will be published in this repository. If `CONTRIBUTING.md` and `LICENSE` are added, link them here.

## About Lerian

Matcher is developed by Lerian, a tech company founded in 2024, led by a team with a track record in developing ledger and core banking solutions. For any inquiries or support, please reach out to us at [contact@lerian.studio](mailto:contact@lerian.studio) or open a Discussion in our GitHub repository.
