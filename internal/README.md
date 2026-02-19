# internal

Core application code organized as bounded contexts following Domain-Driven Design (DDD) with Hexagonal Architecture.

## Bounded Contexts

| Context | Description |
|---------|-------------|
| [auth](auth/) | Authentication, authorization, and multi-tenancy middleware |
| [bootstrap](bootstrap/) | Service initialization, dependency wiring, and lifecycle management |
| [configuration](configuration/) | Reconciliation contexts, sources, field maps, and match rules |
| [ingestion](ingestion/) | File parsing, normalization, deduplication, and transaction import |
| [matching](matching/) | Match orchestration, rule execution, fee verification, and confidence scoring |
| [exception](exception/) | Exception lifecycle, disputes, evidence tracking, and resolution workflows |
| [governance](governance/) | Immutable audit logs for compliance |
| [reporting](reporting/) | Dashboard analytics, export jobs, and variance reports |
| [outbox](outbox/) | Reliable event publication via the transactional outbox pattern |
| [shared](shared/) | Cross-context domain objects, value objects, and infrastructure adapters |
| [testutil](testutil/) | Shared test utilities and helpers |

## Architecture

Each bounded context follows the hexagonal structure:

```
{context}/
├── adapters/        # Infrastructure implementations (HTTP, PostgreSQL, Redis, RabbitMQ)
├── domain/          # Entities, value objects, and business rules
├── ports/           # Interfaces for external dependencies
└── services/        # Use cases split into command/ (writes) and query/ (reads)
```

Contexts communicate through well-defined ports and shared domain objects. Cross-context dependencies flow through the bootstrap wiring layer.
