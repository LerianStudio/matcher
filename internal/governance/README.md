# Governance Context

The `internal/governance` bounded context acts as the compliance backbone of the Matcher service. It provides an immutable, append-only ledger of all critical system actions, ensuring full traceability and SOX compliance.

## Overview

This context is responsible for:
1. **Audit Logging**: Recording "who did what, when, and to which entity" for every write operation.
2. **Immutability**: Enforcing that audit logs can never be updated or deleted.
3. **Tenant Isolation**: Ensuring audit logs are segregated by tenant.
4. **Actor Mapping**: Mapping actor IDs to human-readable names for audit display.
5. **Archive Management**: Archiving old audit log partitions to S3 and managing retrieval.
6. **Tamper Detection**: Cryptographic hash chain verification on audit log entries.

## Architecture

### Hexagonal Layers

```
internal/governance/
├── adapters/
│   ├── audit/           # Audit event consumer
│   ├── http/            # Handlers for audit logs and archive downloads
│   │   └── dto/         # Response DTOs
│   └── postgres/        # Repositories (audit_log, actor_mapping, archive_metadata)
├── domain/
│   ├── entities/        # AuditLog, ActorMapping, ArchiveMetadata
│   ├── hashchain/       # Hash chain for tamper detection
│   └── repositories/    # Repository interfaces
├── ports/               # External dependency interfaces
└── services/
    ├── command/         # Partition management commands
    └── worker/          # Archival worker (background S3 archival with config)
```

## Domain Model

### AuditLog Entity

The `AuditLog` entity is the core immutable record:

```go
type AuditLog struct {
    ID         uuid.UUID
    TenantID   uuid.UUID
    EntityType string    // e.g., "transaction", "match_run", "dispute"
    EntityID   uuid.UUID
    Action     string    // e.g., "created", "matched", "dispute_opened"
    ActorID    *string   // User ID or Service Name
    Changes    []byte    // JSON payload of the change/snapshot
    CreatedAt  time.Time
}
```

### ActorMapping Entity

The `ActorMapping` entity maps actor IDs (from JWT claims or service identifiers) to human-readable display names. This allows audit logs to show meaningful actor names without embedding PII directly in log entries.

### ArchiveMetadata Entity

The `ArchiveMetadata` entity tracks archived audit log partitions:
- **S3 Key**: Location of the archived partition in object storage.
- **Size**: Size of the archived data in bytes.
- **Row Count**: Number of audit log entries in the archive.
- **Hash**: Integrity hash for verifying archive contents.

### Hash Chain

The `hashchain` package provides cryptographic hash chain verification for audit logs. Each audit log entry includes a hash computed from the previous entry's hash and the current entry's content, creating a tamper-evident chain. Any modification to a historical entry breaks the chain and is detectable.

### Append-Only Pattern

The `AuditRepository` enforces immutability by strictly implementing `Create` operations. There are **no Update or Delete methods** in the repository interface or implementation.

## Services

### Partition Management (Command)

Manages PostgreSQL table partitions for audit logs:
- Creates new partitions for upcoming time periods.
- Detaches old partitions for archival.

### Archival Worker

Background job that archives old audit log partitions to S3:
- Runs on a configurable schedule.
- Exports detached partitions to compressed files.
- Uploads to S3 with integrity verification.
- Records archive metadata for retrieval.

## API Endpoints

| Method | Path | Description |
|--------|------|-------------|
| GET | `/v1/governance/audit-logs` | List audit logs |
| GET | `/v1/governance/audit-logs/:id` | Get audit log by ID |
| GET | `/v1/governance/entities/:entityType/:entityId/audit-logs` | List logs by entity |
| GET | `/v1/governance/archives` | List archived audit log partitions |
| GET | `/v1/governance/archives/:id/download` | Download archived audit log |

## Usage

Other contexts (Ingestion, Matching, Exception) publish events or call the audit repository directly (via ports) to record their actions.

### Example: Recording an Action

```go
auditLog, err := entities.NewAuditLog(
    ctx,
    tenantID,
    "dispute",
    disputeID,
    "opened",
    actorID,
    changePayload,
)
if err != nil {
    return err
}

return repo.Create(ctx, auditLog)
```
