# Internal Directory (`internal/`)

This directory contains the private application code, organized by Bounded Contexts following the Modular Monolith architecture.

## Bounded Contexts

### Auth (`internal/auth`)
- **Role:** Security adapter.
- **Features:** JWT extraction, Tenant context resolution, Authorization middleware.
- [Documentation](auth/README.md)

### Bootstrap (`internal/bootstrap`)
- **Role:** Composition Root.
- **Features:** App configuration, dependency injection, server lifecycle, infrastructure connections.
- [Documentation](bootstrap/README.md)

### Configuration (`internal/configuration`)
- **Role:** Metadata management.
- **Features:** Managing Reconcilation Contexts, Sources, Field Maps, and Match Rules.
- [Documentation](configuration/README.md)

### Exception (`internal/exception`)
- **Role:** Exception Management.
- **Features:** Managing disputes, resolution workflows, SLAs, and evidence handling for unmatched transactions or other exceptions.

### Governance (`internal/governance`)
- **Role:** Audit & Compliance.
- **Features:** Immutable audit logging for all critical system actions to ensure transparency and SOX compliance.

### Ingestion (`internal/ingestion`)
- **Role:** Data Gateway.
- **Features:** File parsing (CSV/JSON), Normalization, Deduplication, Event publication.
- [Documentation](ingestion/README.md)

### Matching (`internal/matching`)
- **Role:** Core Reconciliation Engine.
- **Features:** Executing match runs (DryRun/Commit), rule evaluation, transaction locking, fee verification, and persistent match result storage.

### Outbox (`internal/outbox`)
- **Role:** Reliable Messaging.
- **Features:** Implementation of the Transactional Outbox pattern to ensure atomic updates and event publishing.

### Reporting (`internal/reporting`)
- **Role:** Read Model & Analytics.
- **Features:** Queries for matched/unmatched transactions, variance reports, and dashboard metrics. Optimized for read performance.

### Shared (`internal/shared`)
- **Role:** Cross-cutting concerns.
- **Features:** Shared domain value objects, constants, and infrastructure adapters used by multiple contexts.

## Architecture Standard
All contexts must follow the Hexagonal Architecture:
- `adapters/`: Implementation details (HTTP handlers, DB repos).
- `domain/`: Pure business logic (Entities).
- `services/`: Use cases (Command/Query separation).
- `ports/`: Interfaces defined by the domain.
