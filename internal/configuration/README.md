# Configuration Context

The `internal/configuration` bounded context manages the metadata and rules that drive the reconciliation engine. It allows users to define *what* to reconcile (Sources), *how* to normalize data (Field Maps), and *how* to match transactions (Match Rules).

## Overview

This context implements a **Hexagonal Architecture** with **CQRS** (Command Query Responsibility Segregation) to separate write and read concerns.

- **Domain**: Defines core entities (`ReconciliationContext`, `Source`, `MatchRule`, `FeeSchedule`, `ReconciliationSchedule`) and business rules.
- **Adapters**:
  - **Audit**: Outbox-based audit event publisher.
  - **HTTP**: Fiber handlers exposing REST endpoints.
  - **PostgreSQL**: Database persistence with multi-tenant schema isolation.
- **Ports**: External dependency interfaces (audit publisher, fee schedule, schedule repository).
- **Services**: Application logic split into `Command` (mutations, including clone), `Query` (retrieval), and `Worker` (background scheduler) use cases.

## Domain Model

### Entities

1.  **ReconciliationContext**: The top-level aggregate representing a reconciliation process (e.g., "Credit Card vs. Ledger").
    -   Attributes: `Type` (1:1, 1:N), `Interval` (cron schedule), `Status` (Active/Paused).
2.  **Source**: Represents an input data stream (e.g., "Bank CSV", "Internal Ledger API").
    -   Types: `Ingestion` (External), `Ledger` (Internal).
3.  **FieldMap**: Defines how raw source fields map to the canonical internal format.
    -   Note: A canonical `FieldMap` type also exists in `internal/shared/domain/` (the shared kernel version is authoritative for cross-context use; this context's version is for configuration-specific behavior).
4.  **MatchRule**: Ordered list of logic to identify matching sets of transactions.
5.  **FeeSchedule**: Fee rates and tolerances for fee verification during matching.
6.  **ReconciliationSchedule**: Cron-based scheduling for automated reconciliation runs.
7.  **CloneResult**: Value object returned by the context clone operation, containing the new context ID and a summary of all cloned child entities (sources, field maps, rules, schedules).

## Architecture

### Hexagonal Layers

```
internal/configuration/
├── adapters/
│   ├── audit/           # Outbox-based audit event publisher
│   ├── http/            # REST API Handlers (Fiber)
│   │   └── dto/         # Request/response DTOs
│   └── postgres/        # Repository Implementations
│       ├── common/      # Shared transaction utilities
│       ├── context/     # ReconciliationContext repository
│       ├── fee_rule/    # FeeRule repository
│       ├── field_map/   # FieldMap repository
│       ├── match_rule/  # MatchRule repository
│       ├── schedule/    # ReconciliationSchedule repository
│       └── source/      # Source repository
├── domain/
│   ├── entities/        # Core Business Logic
│   ├── repositories/    # Repository Interfaces
│   └── value_objects/   # Enums and Value Types
├── ports/               # External dependency interfaces (audit, fees, schedules)
└── services/
    ├── command/         # Write Operations (Create, Update, Delete, Clone)
    ├── query/           # Read Operations (Get, List)
    └── worker/          # Background scheduler worker
```

### CQRS Pattern

The service layer is explicitly split:
-   **Command UseCase**: Handles side-effects. Validates inputs and updates state via repositories.
    -   `commands.go` — UseCase struct, constructor, shared errors.
    -   `context_commands.go` — Context CRUD operations.
    -   `source_commands.go` — Source CRUD operations.
    -   `field_map_commands.go` — FieldMap CRUD operations.
    -   `match_rule_commands.go` — MatchRule CRUD and reorder operations.
    -   `fee_schedule_commands.go` — FeeSchedule CRUD and simulation.
    -   `fee_rule_commands.go` — FeeRule CRUD operations.
    -   `schedule_commands.go` — ReconciliationSchedule CRUD operations.
    -   Clone operations are split across multiple files: `clone_commands.go` (orchestrator), `clone_sources.go` (source + field map cloning), `clone_rules.go` (match rule cloning), `clone_context_creation.go` (new context assembly).
    -   `tx_helpers.go` — Shared transactional helper utilities.
-   **Query UseCase**: Handles data retrieval.
    -   `queries.go` — UseCase struct and constructor.
    -   `context_queries.go` — Context get/list.
    -   `source_queries.go` — Source get/list.
    -   `field_map_queries.go` — FieldMap get/list.
    -   `match_rule_queries.go` — MatchRule get/list.
    -   `fee_schedule_queries.go` — FeeSchedule get/list.
    -   `fee_rule_queries.go` — FeeRule get/list.
    -   `schedule_queries.go` — ReconciliationSchedule get/list.

### Multi-Tenancy

Persistence is handled via `adapters/postgres`. All repository methods utilize `common.WithTenantTx` to ensure queries run within the correct PostgreSQL schema (`SET LOCAL search_path`), enforcing strict data isolation per tenant.

## API Endpoints

The context exposes a RESTful API protected by the Auth layer:

| Resource | Method | Path | Description |
|----------|--------|------|-------------|
| **Context** | POST | `/v1/contexts` | Create a new reconciliation context |
| | GET | `/v1/contexts` | List contexts |
| | GET | `/v1/contexts/:contextId` | Get a context by ID |
| | PATCH | `/v1/contexts/:contextId` | Update a context |
| | DELETE | `/v1/contexts/:contextId` | Delete a context |
| **Clone** | POST | `/v1/contexts/:contextId/clone` | Clone a context with all its configuration |
| **Source** | POST | `/v1/contexts/:contextId/sources` | Add a data source |
| | GET | `/v1/contexts/:contextId/sources` | List sources |
| | GET | `/v1/contexts/:contextId/sources/:sourceId` | Get a source by ID |
| | PATCH | `/v1/contexts/:contextId/sources/:sourceId` | Update a source |
| | DELETE | `/v1/contexts/:contextId/sources/:sourceId` | Delete a source |
| **FieldMap** | POST | `/v1/contexts/:contextId/sources/:sourceId/field-maps` | Define field mappings |
| | GET | `/v1/contexts/:contextId/sources/:sourceId/field-maps` | Get field map for a source |
| | PATCH | `/v1/field-maps/:fieldMapId` | Update a field map |
| | DELETE | `/v1/field-maps/:fieldMapId` | Delete a field map |
| **MatchRule** | POST | `/v1/contexts/:contextId/rules` | Add matching logic |
| | GET | `/v1/contexts/:contextId/rules` | List match rules |
| | GET | `/v1/contexts/:contextId/rules/:ruleId` | Get a match rule by ID |
| | PATCH | `/v1/contexts/:contextId/rules/:ruleId` | Update a match rule |
| | DELETE | `/v1/contexts/:contextId/rules/:ruleId` | Delete a match rule |
| | POST | `/v1/contexts/:contextId/rules/reorder` | Change rule execution order |
| **FeeSchedule** | POST | `/v1/fee-schedules` | Create a fee schedule |
| | GET | `/v1/fee-schedules` | List fee schedules |
| | GET | `/v1/fee-schedules/:scheduleId` | Get a fee schedule |
| | PATCH | `/v1/fee-schedules/:scheduleId` | Update a fee schedule |
| | DELETE | `/v1/fee-schedules/:scheduleId` | Delete a fee schedule |
| | POST | `/v1/fee-schedules/:scheduleId/simulate` | Simulate fee calculation |
| **FeeRule** | POST | `/v1/fee-schedules/:scheduleId/rules` | Create a fee rule |
| | GET | `/v1/fee-schedules/:scheduleId/rules` | List fee rules |
| | GET | `/v1/fee-schedules/:scheduleId/rules/:ruleId` | Get a fee rule |
| | PATCH | `/v1/fee-schedules/:scheduleId/rules/:ruleId` | Update a fee rule |
| | DELETE | `/v1/fee-schedules/:scheduleId/rules/:ruleId` | Delete a fee rule |
| **Schedule** | POST | `/v1/contexts/:contextId/schedules` | Create a reconciliation schedule |
| | GET | `/v1/contexts/:contextId/schedules` | List schedules |
| | GET | `/v1/contexts/:contextId/schedules/:scheduleId` | Get a schedule |
| | PATCH | `/v1/contexts/:contextId/schedules/:scheduleId` | Update a schedule |
| | DELETE | `/v1/contexts/:contextId/schedules/:scheduleId` | Delete a schedule |

## Usage

### Dependency Injection

The module is initialized in `internal/bootstrap/init.go` and registered with the application router:

```go
// 1. Repositories (Postgres)
configContextRepository := configContextRepo.NewRepository(postgresConnection)
configSourceRepository, err := configSourceRepo.NewRepository(postgresConnection)
if err != nil {
    return fmt.Errorf("create source repository: %w", err)
}
configFieldMapRepository := configFieldMapRepo.NewRepository(postgresConnection)
configMatchRuleRepository := configMatchRuleRepo.NewRepository(postgresConnection)

// 2. Services (CQRS)
configCommandUseCase, err := configCommand.NewUseCase(
    configContextRepository,
    configSourceRepository,
    configFieldMapRepository,
    configMatchRuleRepository,
)
if err != nil {
    return fmt.Errorf("create config command use case: %w", err)
}
configQueryUseCase, err := configQuery.NewUseCase(
    configContextRepository,
    configSourceRepository,
    configFieldMapRepository,
    configMatchRuleRepository,
)
if err != nil {
    return fmt.Errorf("create config query use case: %w", err)
}

// 3. Adapter (HTTP)
configHandler, err := configHTTP.NewHandler(configCommandUseCase, configQueryUseCase)
if err != nil {
    return fmt.Errorf("create config handler: %w", err)
}

// 4. Routes
if err := configHTTP.RegisterRoutes(routes.Protected, configHandler); err != nil {
    return fmt.Errorf("register configuration routes: %w", err)
}
```
