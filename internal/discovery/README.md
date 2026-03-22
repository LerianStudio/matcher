# Discovery Context

The `internal/discovery` bounded context manages external data source connections, schema detection, and data extraction orchestration. It integrates with external "Fetcher" services that pull data from banks, payment processors, and other financial systems.

## Overview

This context handles:
1. **Connection Management**: Discovering, syncing, and testing external database connections from Fetcher services.
2. **Schema Detection**: Automatically discovering table structures (columns, types, nullability) from connected data sources.
3. **Extraction Orchestration**: Submitting, tracking, and polling data extraction jobs via the Fetcher service.
4. **Background Discovery**: Periodic sync of connections and schemas via a background worker with distributed locking.
5. **Extraction Polling**: Asynchronous per-extraction polling until jobs complete, fail, or time out.
6. **Schema Caching**: Redis-backed caching of discovered schemas to reduce redundant Fetcher calls.
7. **Data Synchronization**: Upsert logic for connections and schemas to keep local state consistent with Fetcher.

## Architecture

### Hexagonal Layers

```
internal/discovery/
├── adapters/
│   ├── http/            # REST API handlers (Fiber)
│   │   └── dto/         # Request/response DTOs
│   ├── postgres/        # Repository implementations
│   │   ├── connection/  # FetcherConnection repository
│   │   ├── schema/      # DiscoveredSchema repository
│   │   └── extraction/  # ExtractionRequest repository
│   ├── redis/           # Schema cache implementation
│   └── fetcher/         # HTTP client for Fetcher service
├── domain/
│   ├── entities/        # Core business entities
│   ├── repositories/    # Repository interfaces
│   └── value_objects/   # Enums and value types
├── ports/               # External dependency interfaces (SchemaCache, ExtractionJobPoller)
└── services/
    ├── command/         # Write operations (refresh, test, extract, poll)
    ├── query/           # Read operations (status, connections, schemas, extractions)
    ├── worker/          # Background discovery worker, extraction poller
    └── syncer/          # Connection/schema synchronization logic
```

### CQRS Pattern

The service layer is explicitly split:
- **Command UseCase**: Handles side-effects — refresh discovery, test connections, start extractions, poll extraction status.
- **Query UseCase**: Handles data retrieval — discovery status, connection listing, schema lookups, extraction details.

### Multi-Tenancy

All repository methods utilize tenant-scoped PostgreSQL schemas (`SET LOCAL search_path`) for strict data isolation. The discovery worker iterates over all tenants, syncing connections per-tenant. Manual refresh operations extract the tenant from JWT context.

## Domain Model

### Entities

1. **FetcherConnection**: Represents an external database connection discovered from the Fetcher service.
   - Attributes: `FetcherConnID`, `ConfigName`, `DatabaseType`, `Host`, `Port`, `DatabaseName`, `ProductName`.
   - **Status**: `AVAILABLE`, `UNREACHABLE`, `UNKNOWN`.
   - Tracks `SchemaDiscovered` flag and `LastSeenAt` timestamp.

2. **DiscoveredSchema**: Represents the schema of a table discovered from a Fetcher connection.
   - Attributes: `ConnectionID`, `TableName`, `Columns` (name, type, nullable).
   - Serializes column metadata as JSON for database storage.

3. **ExtractionRequest**: Tracks a data extraction job submitted to the Fetcher service.
   - Attributes: `ConnectionID`, `FetcherJobID`, `Tables`, `StartDate`, `EndDate`, `Filters`, `ResultPath`.
   - **Status**: `PENDING` → `SUBMITTED` → `EXTRACTING` → `COMPLETE` / `FAILED` / `CANCELLED`.
   - Optional `IngestionJobID` for downstream ingestion linkage.
   - Enforces safe state transitions and validates result paths against traversal attacks.

### Value Objects

- **ConnectionStatus**: `AVAILABLE`, `UNREACHABLE`, `UNKNOWN`
- **ExtractionStatus**: `PENDING`, `SUBMITTED`, `EXTRACTING`, `COMPLETE`, `FAILED`, `CANCELLED`

## Core Components

### Fetcher Client

The HTTP client adapter (`adapters/fetcher/`) communicates with the Fetcher REST API:
- Health checks (`IsHealthy`)
- Connection listing and schema discovery
- Connection testing with latency measurement
- Extraction job submission and status polling
- Response body size limits (10 MB) to prevent memory exhaustion

### Connection Syncer

The `syncer` package (`services/syncer/`) centralizes connection/schema synchronization logic shared by both the manual refresh command and the background discovery worker:
- Upserts connections (create or update based on `FetcherConnID`)
- Fetches and replaces schemas per connection
- Optionally invalidates and repopulates the schema cache on sync

### Discovery Worker

The background worker (`services/worker/discovery_worker.go`) runs on a configurable interval:
- Acquires a Redis distributed lock (`matcher:discovery:sync`) to prevent concurrent syncs
- Iterates over all tenants via `TenantLister`
- Delegates to the `ConnectionSyncer` for each tenant
- Uses panic recovery for resilience

### Extraction Poller

The extraction poller (`services/worker/extraction_poller.go`) spawns a per-extraction goroutine:
- Polls Fetcher at configurable intervals (default: 5 seconds)
- Times out after a configurable duration (default: 10 minutes)
- Invokes `onComplete` or `onFailed` callbacks on terminal state
- Not a long-running background worker — scoped to individual extraction requests

### Schema Cache

Redis-backed cache (`adapters/redis/schema_cache.go`) for discovered schemas:
- Reduces redundant Fetcher API calls for schema lookups
- TTL-based expiration with explicit invalidation on refresh
- Graceful cache miss handling

## API Endpoints

| Method | Path | Description |
|--------|------|-------------|
| GET | `/v1/discovery/status` | Get Fetcher integration status (health, connection count, last sync) |
| GET | `/v1/discovery/connections` | List all discovered connections |
| GET | `/v1/discovery/connections/:connectionId` | Get a connection by ID |
| GET | `/v1/discovery/connections/:connectionId/schema` | Get discovered table schemas for a connection |
| POST | `/v1/discovery/connections/:connectionId/test` | Test connectivity for a connection |
| POST | `/v1/discovery/connections/:connectionId/extractions` | Start a data extraction job |
| GET | `/v1/discovery/extractions/:extractionId` | Get an extraction request by ID |
| POST | `/v1/discovery/extractions/:extractionId/poll` | Poll Fetcher for extraction status update |
| POST | `/v1/discovery/refresh` | Force an immediate discovery sync with Fetcher |

## Workflow

### Discovery Sync

1. **Trigger**: Manual via `POST /v1/discovery/refresh` or automatic via the background `DiscoveryWorker`.
2. **Lock**: Acquires a Redis distributed lock to prevent concurrent syncs.
3. **Health Check**: Verifies the Fetcher service is reachable.
4. **Fetch Connections**: Lists all connections from Fetcher for the current tenant.
5. **Sync**: For each connection, the `ConnectionSyncer` upserts the connection record and replaces its schemas.
6. **Cache**: Invalidates and repopulates the Redis schema cache if configured.

### Data Extraction

1. **Request**: Client submits an extraction via `POST /v1/discovery/connections/:connectionId/extractions` with table selection, date range, and filters.
2. **Validate**: Validates the request parameters and verifies the connection exists.
3. **Submit**: Creates an `ExtractionRequest` record (`PENDING`), then submits the job to Fetcher (`SUBMITTED`).
4. **Poll**: The extraction poller (async goroutine) or manual `POST /v1/discovery/extractions/:extractionId/poll` checks Fetcher for status transitions (`EXTRACTING` → `COMPLETE` / `FAILED`).
5. **Complete**: On success, the result path is persisted. On failure, a sanitized error message is recorded.
