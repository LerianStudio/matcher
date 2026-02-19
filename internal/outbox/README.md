# Outbox Context

The `internal/outbox` bounded context implements the **Transactional Outbox Pattern** to ensure reliable, atomic event publication.

## Overview

In distributed systems, updating the database and publishing an event to a message broker (like RabbitMQ) must be atomic. The Outbox pattern solves this by:
1. **Local Transaction**: Writing the event to an `outbox_events` table in the *same database transaction* as the business entity update.
2. **Async Dispatcher**: A background process ("Relay" or "Dispatcher") reads pending events and publishes them to the broker.

## Architecture

### Hexagonal Layers

```
internal/outbox/
├── adapters/
│   ├── postgres/        # Repository for storing/updating outbox events
│   └── rabbitmq/        # Publisher implementation
├── domain/
│   ├── entities/        # OutboxEvent
│   └── repositories/    # OutboxRepository interface
│       └── tx.go        # Transaction provider interface
├── ports/
│   └── doc.go           # Port documentation
└── services/
    └── dispatcher.go    # Background worker for polling and publishing
```

## Domain Model

### OutboxEvent

```go
type OutboxEvent struct {
    ID          uuid.UUID
    EventType   string    // e.g., "ingestion.completed", "match.confirmed"
    AggregateID uuid.UUID // ID of the entity (Job ID, MatchGroup ID)
    Payload     []byte    // JSON payload
    Status      string    // PENDING -> PROCESSING -> PUBLISHED / FAILED
    Attempts    int       // Retry count
    PublishedAt *time.Time
    LastError   string
}
```

## Workflow

1. **Creation**:
   - A service (e.g., Ingestion) starts a DB transaction.
   - Updates business state (e.g., `ingestion_jobs`).
   - Calls `outboxRepo.CreateWithTx(ctx, tx, event)`.
   - Commits transaction.

2. **Dispatching**:
   - The `Dispatcher` polls the `outbox_events` table for `PENDING` events.
   - Marks them `PROCESSING`.
   - Publishes to RabbitMQ.
   - If successful: Marks `PUBLISHED`.
   - If failed: Updates `LastError`, increments `Attempts`. If attempts < max, resets to `PENDING` (with backoff), otherwise `FAILED`.

## Usage

Used by:
- **Ingestion**: To publish `ingestion.completed` / `ingestion.failed`.
- **Matching**: To publish `match.confirmed` events.
- **Exception**: To publish audit events for dispute actions.

## Shared Instance

The outbox repository is instantiated once during bootstrap and shared across modules. Both ingestion and matching receive the same repository instance to ensure all events go through a single outbox table. This is critical for ordering guarantees and preventing event duplication.
