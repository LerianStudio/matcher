# Shared Context

The `internal/shared` bounded context contains domain objects, value objects, and infrastructure adapters that are **shared across multiple bounded contexts**. It serves as a kernel for common definitions to avoid circular dependencies and duplication.

## Overview

This context includes:
1. **Common Domain Entities**: `Transaction`, `MatchRule`, and `FieldMap` are canonical data structures used by Ingestion, Matching, and Reporting.
2. **Cross-Context Adapters**: Bridge adapters that connect bounded contexts without creating direct dependencies.
3. **Fee Calculation Engine**: Full fee calculation subsystem with schedule calculation, verifier, normalization, and fee schedule/rule/structure models.
4. **Infrastructure Adapters**: Tenant-aware infrastructure ports, common SQL utilities, outbox repository, RabbitMQ publisher, idempotency middleware, and M2M helpers.
5. **Constants and Utilities**: System-wide constants and text utilities.

## Architecture

```
internal/shared/
├── adapters/
│   ├── cross/           # Cross-context adapters (config, ingestion, matching, exception bridges)
│   ├── http/            # Idempotency middleware and cursor pagination helpers
│   ├── postgres/
│   │   ├── common/      # Shared SQL utilities (cursor, nullable, tx, read helpers)
│   │   └── outbox/      # Shared outbox repository implementation
│   └── rabbitmq/        # Confirmable publisher, DLQ, constants
├── constants/           # System-wide constants (application name, pagination defaults)
├── domain/
│   ├── events.go        # Shared event types
│   ├── exception/       # Shared exception severity definitions
│   ├── fee/             # Fee calculation engine (schedule calculator, verifier, normalization, fee schedule/rule/structure)
│   ├── field_map.go     # Canonical FieldMap definition
│   ├── match_rule.go    # Canonical MatchRule definition
│   └── transaction.go   # Canonical Transaction entity
├── ports/               # InfrastructureProvider, MatchTrigger, TransactionProvider interfaces
├── testutil/            # Shared test helpers (decimal, logger, uuid, helpers)
└── utils/               # Text utilities
```

## Core Components

### Transaction Entity

The `Transaction` entity is the central data structure in Matcher:

```go
type Transaction struct {
    ID               uuid.UUID
    SourceID         uuid.UUID
    ExternalID       string
    Amount           decimal.Decimal
    Currency         string
    AmountBase       *decimal.Decimal // Converted to base currency
    BaseCurrency     *string
    Status           TransactionStatus // UNMATCHED, MATCHED, etc.
    Metadata         map[string]any    // Flexible key-value store
    // ...
}
```

### Cross-Context Adapters

Bridge adapters that connect bounded contexts without creating direct dependencies:
- **Configuration Bridge**: Lookup reconciliation contexts, sources, and match rules from the configuration context.
- **Ingestion-to-Matching Trigger**: Auto-match trigger fired after ingestion completes.
- **Exception-Matching Gateway**: Bridge between matching results and exception creation.

### Fee Calculation Engine

Full fee calculation subsystem shared across contexts:
- **Calculator**: Computes expected fees from fee schedules.
- **Verifier**: Compares expected vs. actual fees and reports variances.
- **Normalization**: Net-to-gross conversion and money handling.
- **Schedule/Rule Models**: Fee schedule, fee rule, and fee structure definitions.

### RabbitMQ Adapters

- **Confirmable Publisher**: Publisher with RabbitMQ publisher confirms for reliable message delivery.
- **DLQ Support**: Dead letter queue handling for failed messages.
- **Constants**: Exchange, queue, and routing key definitions.

### Idempotency Middleware

Redis-backed idempotency middleware for HTTP endpoints. Prevents duplicate mutations from client retries by caching responses keyed by idempotency tokens.

### Event Definitions

Shared event type constants used across bounded contexts for outbox event publication and message routing.

### Infrastructure

- **InfrastructureProvider**: Tenant-aware access to transactions, primary DBs, replica DBs, and Redis.
- **Common SQL Utilities**: Cursor pagination, nullable type helpers, transaction wrappers, and read helpers.

### Sanitization

- **CSV Formula Injection Prevention**: `sanitize/formula.go` prevents CSV formula injection attacks by escaping dangerous characters in exported data.

### Ports

- **InfrastructureProvider**: Interface for tenant-aware transactions, primary/replica DB access, and Redis access.
- **MatchTrigger**: Interface for triggering auto-matching after ingestion completes.
- **ObjectStorage**: Interface for S3-compatible object storage operations.
- **Fetcher**: Interface for external fetcher service communication.
- **TenantLister**: Interface for listing available tenants.
- **TxRunner**: Interface for transaction execution.

## Usage Policy

Components in `shared` should be stable and generic. Business logic specific to a single context (e.g., complex matching rules, dispute workflows) should stay in their respective contexts (`matching`, `exception`) and not leak into `shared`.
