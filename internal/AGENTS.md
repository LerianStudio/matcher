# Internal Directory (`internal/`)

This directory contains the private application code, organized by Bounded Contexts following the Modular Monolith architecture with DDD + Hexagonal Architecture + CQRS-light.

## Bounded Contexts

### Auth (`internal/auth`)
- **Role:** Security adapter.
- **Features:** JWT extraction (HS256/384/512), tenant context resolution, authorization middleware via lib-auth RBAC, PostgreSQL search_path isolation.
- [Documentation](auth/README.md)

### Bootstrap (`internal/bootstrap`)
- **Role:** Composition Root.
- **Features:** App configuration (zero-config defaults + env overrides), dependency injection, server lifecycle, infrastructure connections (PostgreSQL primary/replica, Redis, RabbitMQ, S3), systemplane integration (runtime config authority), dynamic infrastructure switching, worker lifecycle management, health checks, rate limiting, observability (OpenTelemetry), and migration orchestration with preflight guards for irreversible cutovers such as migration 022.
- [Documentation](bootstrap/README.md)

### Configuration (`internal/configuration`)
- **Role:** Metadata management.
- **Features:** Reconciliation contexts, sources, field maps, match rules, fee schedules, fee rules, reconciliation scheduling (cron-based), context cloning with all configuration, audit event publishing.
- [Documentation](configuration/README.md)

### Discovery (`internal/discovery`)
- **Role:** External data source integration.
- **Features:** Fetcher connection management, schema detection, extraction request orchestration, background discovery workers, extraction polling, data synchronization.
- [Documentation](discovery/README.md)

### Exception (`internal/exception`)
- **Role:** Exception management.
- **Features:** Exception lifecycle, disputes with state machine (Draft/Open/Pending Evidence/Won/Lost), comments, force-match resolution, adjust-entry resolution, external dispatch (JIRA/webhooks with rate limiting), bulk operations (assign/resolve/dispatch), SLA tracking, evidence collection, callback idempotency, audit logging.
- [Documentation](exception/README.md)

### Governance (`internal/governance`)
- **Role:** Audit and compliance.
- **Features:** Immutable append-only audit logging, cryptographic hash chain verification (tamper detection), actor mapping (ID to human-readable names), archive management (S3 archival with integrity verification), partition management, background archival worker.
- [Documentation](governance/README.md)

### Ingestion (`internal/ingestion`)
- **Role:** Data gateway.
- **Features:** File parsing (CSV/JSON/XML including ISO 20022 camt.053), field normalization, BOM handling, currency normalization, Redis-based deduplication, event publication via outbox, auto-match trigger after ingestion, file preview, transaction search.
- [Documentation](ingestion/README.md)

### Matching (`internal/matching`)
- **Role:** Core reconciliation engine.
- **Features:** Match runs (DryRun/Commit modes), deterministic and tolerance-based rule execution, exact/tolerance/date-window evaluators, confidence scoring (0-100), fee verification with variance tracking, manual match/unmatch, adjustments (write-off/correction), cross-currency matching with FX rates, distributed locking (Redis), source classification, proposal processing, outbox event publishing.
- [Documentation](matching/README.md)

### Reporting (`internal/reporting`)
- **Role:** Read model and analytics.
- **Features:** Dashboard metrics (volume, match rate, SLA, source breakdown, cash impact), async export jobs (CSV/PDF with S3 storage and presigned URLs), streaming report generation, Redis-based dashboard caching, background export/cleanup workers, rate-limited export endpoints, cursor pagination.
- [Documentation](reporting/README.md)

### Shared (`internal/shared`)
- **Role:** Shared kernel (cross-cutting concerns), including transactional outbox integration.
- **Features:** Canonical domain entities (Transaction, MatchRule, FieldMap, AuditLog, OutboxEvent), fee calculation engine (schedule calculation, verifier, normalization, fee schedule/rule models), cross-context bridge adapters, common SQL utilities, RabbitMQ publisher with confirms and DLQ, idempotency middleware, tenant-aware infrastructure ports and SQL helpers, CSV formula injection prevention. Outbox persistence and dispatcher are delegated to `lib-commons/v5/commons/outbox`; matcher wires the dispatcher, registers handlers, and publishes envelopes from the bounded contexts.
- [Documentation](shared/README.md)

### Testutil (`internal/testutil`)
- **Role:** Shared test helpers.
- **Features:** Generic pointer helper (`Ptr[T]`), deterministic time utilities for tests.
- [Documentation](testutil/README.md)

## Architecture Standard

All bounded contexts must follow the Hexagonal Architecture:
- `adapters/`: Implementation details (HTTP handlers, PostgreSQL repositories, Redis, RabbitMQ).
- `domain/`: Pure business logic (entities, value objects, domain services, repository interfaces).
- `services/`: Use cases (command/ for writes, query/ for reads, worker/ for background jobs).
- `ports/`: Interfaces for external dependencies defined by the domain.

### Cross-Context Communication

Bounded contexts **must not** import each other directly. Cross-context dependencies flow through:
1. `internal/shared/` for shared domain types and port interfaces.
2. `internal/shared/adapters/cross/` for bridge adapters that connect contexts.
3. The bootstrap wiring layer (`internal/bootstrap/`) for dependency injection.

This is enforced by `depguard` rules in `.golangci.yml`.
