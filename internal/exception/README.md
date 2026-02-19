# Exception Context

The `internal/exception` bounded context manages the lifecycle of exceptions, disputes, comments, and manual resolutions for transactions that fail automated reconciliation. It ensures that every unmatched item is accounted for and resolved according to business rules.

## Overview

This context handles:
1. **Exception Management**: Listing, viewing, and resolving exceptions with full history tracking.
2. **Exception Comments**: Full CRUD for comments on exceptions (list, add, delete).
3. **Dispute Management**: Opening, closing, and resolving disputes with evidence submission.
4. **Force Match**: Force-matching an exception to resolve it against a target transaction.
5. **Adjust Entry**: Creating adjustments for exceptions that cannot be matched.
6. **Dispatch to External**: Sending exceptions to external systems (JIRA, webhooks) with rate limiting.
7. **Bulk Operations**: Bulk assign, resolve, and dispatch exceptions.
8. **Workflow Automation**: State transitions (Open → Pending Evidence → Won/Lost) and validation.
9. **Evidence Collection**: Attaching comments and file URLs to disputes.
10. **Audit Logging**: All significant actions are audited via the `AuditPublisher`.
11. **Idempotency**: Ensures callback processing from external systems (e.g., JIRA webhooks) is idempotent.

## Architecture

### Hexagonal Layers

```
internal/exception/
├── adapters/
│   ├── audit/           # Outbox-based audit publisher
│   ├── http/            # Handlers for exception, dispute, comment, and bulk APIs
│   │   ├── connectors/  # External system connectors (JIRA, webhooks)
│   │   └── dto/         # Request/response DTOs
│   ├── postgres/        # Repositories (exception, dispute, comment)
│   ├── redis/           # Idempotency and rate limiting
│   └── resolution/      # Resolution executor for force-match and adjust-entry
├── domain/
│   ├── dispute/         # Dispute aggregate, state machine, evidence
│   ├── entities/        # Exception and ExceptionComment entities
│   ├── repositories/    # Repository interfaces (exception, dispute, comment, idempotency)
│   ├── services/        # SLA and routing domain services
│   └── value_objects/   # Enums (State, Category, Severity, IdempotencyKey, etc.)
├── ports/               # Interfaces: AuditPublisher, ActorExtractor, ExternalConnector,
│                        #   ExceptionFinder, MatchingGateway, ResolutionExecutor,
│                        #   CallbackRateLimiter
└── services/
    ├── command/         # Write: disputes, comments, bulk ops, callbacks,
    │                    #   force-match, adjust-entry, dispatch
    └── query/           # Read: exceptions, comments
```

## Domain Model

### Exception Entity

The `Exception` entity represents a transaction that failed automated reconciliation. It tracks the exception's lifecycle, including assignment, resolution, and external dispatch status.

### ExceptionComment Entity

The `ExceptionComment` entity supports threaded discussion on exceptions. Comments can be added by users or systems and deleted when no longer relevant.

### Dispute Aggregate

The `Dispute` aggregate tracks the resolution process:
- **State**: `DRAFT`, `OPEN`, `PENDING_EVIDENCE`, `WON`, `LOST`.
- **Category**: `MISSING_TRANSACTION`, `AMOUNT_MISMATCH`, `DUPLICATE`, etc.
- **Evidence**: List of comments/files attached to the dispute.
- **Resolution**: Final outcome description when Won or Lost.

### Key Workflows

1. **Opening a Dispute**:
   - Validates the exception exists.
   - Creates a new Dispute in `DRAFT` or `OPEN` state.
   - Publishes an audit event.

2. **Submitting Evidence**:
   - Adds comments/files.
   - Automatically transitions from `PENDING_EVIDENCE` back to `OPEN`.

3. **Closing a Dispute**:
   - **Win**: Dispute accepted, transaction is adjusted or matched.
   - **Lose**: Dispute rejected, transaction remains unmatched.

4. **Force Match**:
   - Resolves an exception by force-matching it to a target transaction.
   - Executed via the `ResolutionExecutor` adapter.

5. **Adjust Entry**:
   - Creates an adjustment entry for an exception that cannot be matched.
   - Executed via the `ResolutionExecutor` adapter.

6. **Dispatch to External System**:
   - Sends exception details to JIRA, webhooks, or other external systems.
   - Rate-limited via `CallbackRateLimiter` (Redis-based).
   - Captures a snapshot of the exception state at dispatch time.

7. **Bulk Operations**:
   - **Bulk Assign**: Assign multiple exceptions to a user/team.
   - **Bulk Resolve**: Resolve multiple exceptions at once.
   - **Bulk Dispatch**: Send multiple exceptions to external systems (rate-limited).

## API Endpoints

| Method | Path | Description |
|--------|------|-------------|
| GET | `/v1/exceptions` | List exceptions |
| GET | `/v1/exceptions/:exceptionId` | Get exception details |
| GET | `/v1/exceptions/:exceptionId/history` | Get exception history |
| POST | `/v1/exceptions/:exceptionId/force-match` | Force-match an exception |
| POST | `/v1/exceptions/:exceptionId/adjust-entry` | Adjust entry for an exception |
| POST | `/v1/exceptions/:exceptionId/dispatch` | Dispatch to external system (rate-limited) |
| POST | `/v1/exceptions/bulk/assign` | Bulk assign exceptions |
| POST | `/v1/exceptions/bulk/resolve` | Bulk resolve exceptions |
| POST | `/v1/exceptions/bulk/dispatch` | Bulk dispatch exceptions (rate-limited) |
| GET | `/v1/exceptions/:exceptionId/comments` | List comments |
| POST | `/v1/exceptions/:exceptionId/comments` | Add comment |
| DELETE | `/v1/exceptions/:exceptionId/comments/:commentId` | Delete comment |
| POST | `/v1/exceptions/:exceptionId/disputes` | Open dispute |
| GET | `/v1/disputes` | List disputes |
| GET | `/v1/disputes/:disputeId` | Get dispute |
| POST | `/v1/disputes/:disputeId/close` | Close dispute |
| POST | `/v1/disputes/:disputeId/evidence` | Submit evidence |

## Adapters

### Resolution Executor

The resolution adapter executes resolution strategies:
- **Force Match**: Calls the matching gateway to force-match an exception against a target transaction.
- **Adjust Entry**: Creates adjustment entries via the matching gateway.

### External Connectors

The HTTP connector supports multiple external system integrations:
- **JIRA**: Creates issues with exception details, maps severity to priority.
- **Webhooks**: Posts exception snapshots to configured webhook URLs.

### Callback Rate Limiter

Redis-based rate limiter prevents excessive calls to external systems. Configurable per-connector rate limits protect downstream services from overload.

## Integration

- **Audit**: Uses `ports.AuditPublisher` to send events to the Governance context.
- **Actor Extraction**: Uses `ports.ActorExtractor` to identify the user/system performing actions.
- **Matching Gateway**: Uses `ports.MatchingGateway` to execute force-match and adjust-entry operations.
- **External Connectors**: Uses `ports.ExternalConnector` to dispatch exceptions to JIRA, webhooks, etc.
- **Webhooks**: Processes callbacks from external systems using `IdempotencyHelpers` to prevent duplicate processing.
