# internal

Core application code organized as bounded contexts following Domain-Driven Design (DDD) with Hexagonal Architecture.

## Bounded Contexts

| Context | Description |
|---------|-------------|
| [auth](auth/) | Authentication, authorization, and multi-tenancy middleware |
| [bootstrap](bootstrap/) | Service initialization, dependency wiring, systemplane, and lifecycle management |
| [configuration](configuration/) | Reconciliation contexts, sources, field maps, match rules, fee schedules/rules, scheduling |
| [discovery](discovery/) | External data source discovery, schema detection, and extraction management |
| [ingestion](ingestion/) | File parsing (CSV/JSON/XML), normalization, deduplication, and transaction import |
| [matching](matching/) | Match orchestration, rule execution, fee verification, confidence scoring, adjustments |
| [exception](exception/) | Exception lifecycle, disputes, evidence tracking, resolution workflows, bulk operations |
| [governance](governance/) | Immutable audit logs, hash chain verification, actor mapping, archival |
| [reporting](reporting/) | Dashboard analytics, export jobs (CSV/PDF), streaming reports, caching |
| [outbox](outbox/) | Reliable event publication via the transactional outbox pattern |
| [shared](shared/) | Cross-context domain objects, fee engine, infrastructure adapters |
| [testutil](testutil/) | Shared test utilities and helpers |

## Architecture

Each bounded context follows the hexagonal structure:

```
{context}/
├── adapters/        # Infrastructure implementations (HTTP, PostgreSQL, Redis, RabbitMQ)
├── domain/          # Entities, value objects, domain services, and business rules
├── ports/           # Interfaces for external dependencies
└── services/        # Use cases split into command/ (writes), query/ (reads), and worker/ (background)
```

Contexts communicate through well-defined ports and shared domain objects in `internal/shared/`. Cross-context bridge adapters live in `shared/adapters/cross/`. Direct imports between bounded contexts are blocked by `depguard` linter rules.
