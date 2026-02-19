# Ingestion Context

The `internal/ingestion` bounded context is responsible for importing, parsing, normalizing, and persisting external transaction data. It is the gateway for data entering the matching engine.

## Overview

This context ensures that disparate data formats (CSV, JSON, XML) from various sources are converted into standardized `shared.Transaction` records. It handles:
- **File Parsing**: Reading raw bytes and converting them to structured records.
- **Normalization**: Mapping source-specific fields to canonical fields using `FieldMapRepository` (ported from the configuration context).
- **Deduplication**: Preventing duplicate processing using Redis-backed idempotency keys and a repository existence check.
- **Persistence**: Storing ingestion jobs and transactions in PostgreSQL with an outbox entry created in the same transaction as job updates.
- **Event Publication**: Outbox events are published to RabbitMQ (exchange: `matcher.events`) via the shared outbox dispatcher.

## Architecture

### Hexagonal Layers

```
internal/ingestion/
├── adapters/
│   ├── http/            # Upload, preview, search, and query endpoints
│   ├── parsers/         # File format parsers (CSV, JSON, XML)
│   │   ├── bom.go       # BOM (Byte Order Mark) handling
│   │   ├── currency.go  # Currency normalization
│   │   ├── normalizer.go # Field normalization
│   │   └── xml_elements.go # XML element traversal
│   ├── postgres/        # Job, transaction, and outbox repositories
│   ├── redis/           # Deduplication service
│   └── rabbitmq/        # Event publisher
├── domain/
│   ├── entities/        # IngestionJob, ingestion events
│   ├── repositories/    # Job and transaction repository interfaces
│   └── value_objects/   # JobStatus
├── ports/               # Parser, DedupeService, FieldMapRepository, SourceRepository,
│                        #   EventPublisher, MatchTrigger, Dispatcher
│   ├── match_trigger.go # Triggers auto-matching after ingestion completes
│   └── dispatcher.go    # Dispatcher port
└── services/
    ├── command/         # Upload & processing logic
    └── query/           # Job, transaction, preview, and search queries
        ├── preview_queries.go           # File preview before import
        └── transaction_search_queries.go # Transaction search
```

## Domain Model

### Entities

1. **IngestionJob**: Represents a single file upload. Tracks context/source IDs, status, timestamps, and metadata (file name, file size, row counts, error).
2. **IngestionCompletedEvent / IngestionFailedEvent**: Event payloads emitted when jobs complete or fail.

### Shared Transaction

The ingested records are stored as `shared.Transaction` with fields such as `ExternalID`, `Amount`, `Currency`, `Date`, `Description`, `Metadata`, `ExtractionStatus`, and `Status`.

### Value Objects

- **JobStatus**: `QUEUED` → `PROCESSING` → `COMPLETED` / `FAILED`

## Core Components

### Parsers & Normalizers

The context uses a parser registry to select the correct parser based on the provided file format:
- Supported formats: `csv`, `json`, `xml`.
- **BOM handling**: Automatically strips Byte Order Marks from uploaded files (`bom.go`).
- **Currency normalization**: Normalizes currency codes and symbols to ISO 4217 (`currency.go`).
- **Field normalization**: Applies configurable field normalization rules (`normalizer.go`).
- **XML traversal**: Handles nested XML element structures for complex data extraction (`xml_elements.go`).
- `Parser.Parse` expects a `FieldMap` mapping like:
  ```
  {
    "external_id": "source_field_for_id",
    "amount": "source_field_for_amount",
    "currency": "source_field_for_currency",
    "date": "source_field_for_date",
    "description": "source_field_for_description"
  }
  ```

### Deduplication Service

To ensure idempotency, the dedupe service:
- Hashes `source_id + external_id` (SHA256).
- Uses a Redis key that includes `context_id` (`matcher:dedupe:<contextId>:<hash>`).
- Marks via `MarkSeenWithRetry` (SETNX) and returns `ErrDuplicateTransaction` on duplicates.
- Performs a final database existence check before insert.

## API Endpoints

| Method | Path | Description |
|--------|------|-------------|
| POST   | `/v1/imports/contexts/:contextId/sources/:sourceId/upload` | Upload a file for processing (multipart: `file`, `format`) |
| GET    | `/v1/imports/contexts/:contextId/jobs` | List ingestion jobs for a context |
| GET    | `/v1/imports/contexts/:contextId/jobs/:jobId` | Get job details |
| GET    | `/v1/imports/contexts/:contextId/jobs/:jobId/transactions` | View imported records |
| POST   | `/v1/imports/contexts/:contextId/sources/:sourceId/preview` | Preview file before import |
| POST   | `/v1/imports/contexts/:contextId/transactions/:transactionId/ignore` | Ignore a transaction |
| GET    | `/v1/imports/contexts/:contextId/transactions/search` | Search transactions |

## Event-Driven Workflow

1. **Upload**: Client posts a file and format (`csv`, `json`, `xml`) to the upload endpoint.
2. **Process**: `command.UseCase.StartIngestion` loads the source and field map, then parses and normalizes rows.
3. **Dedupe**: Each transaction uses Redis idempotency keys and a repository existence check to skip duplicates.
4. **Persist**: Unique transactions are batch-inserted into PostgreSQL; the job is completed with total/failed row counts.
5. **Outbox**: `ingestion.completed` or `ingestion.failed` events are created inside the same transaction as the job update.
6. **Publish**: The shared outbox dispatcher publishes events to RabbitMQ (`matcher.events` exchange).
   - **Completed payload** includes: `event_type`, `job_id`, `context_id`, `source_id`, `transaction_count`,
     `date_range_start`, `date_range_end`, `total_rows`, `failed_rows`, `completed_at`, `timestamp`.
   - **Failed payload** includes: `event_type`, `job_id`, `context_id`, `source_id`, `error`, `timestamp`.

## Auto-Match Trigger

The `MatchTrigger` port (`ports/match_trigger.go`) enables automatic matching after ingestion completes. When all sources for a reconciliation context have been ingested, the trigger initiates a match run without manual intervention.

## File Preview

The preview service (`services/query/preview_queries.go`) allows users to preview a file's parsed output before committing to a full import. This helps validate field mappings and data quality without persisting any records.

## Transaction Search

The transaction search service (`services/query/transaction_search_queries.go`) provides full-text and filtered search across ingested transactions within a reconciliation context.
