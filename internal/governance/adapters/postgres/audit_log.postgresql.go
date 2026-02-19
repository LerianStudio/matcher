// Package postgres provides PostgreSQL adapters for the governance bounded context.
package postgres

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/Masterminds/squirrel"
	"github.com/google/uuid"

	libCommons "github.com/LerianStudio/lib-uncommons/v2/uncommons"
	libLog "github.com/LerianStudio/lib-uncommons/v2/uncommons/log"
	sharedhttp "github.com/LerianStudio/lib-uncommons/v2/uncommons/net/http"
	libOpentelemetry "github.com/LerianStudio/lib-uncommons/v2/uncommons/opentelemetry"

	"github.com/LerianStudio/matcher/internal/auth"
	"github.com/LerianStudio/matcher/internal/governance/domain/entities"
	"github.com/LerianStudio/matcher/internal/governance/domain/hashchain"
	"github.com/LerianStudio/matcher/internal/governance/domain/repositories"
	pgcommon "github.com/LerianStudio/matcher/internal/shared/adapters/postgres/common"
	"github.com/LerianStudio/matcher/internal/shared/ports"
)

const auditLogColumns = "id, tenant_id, entity_type, entity_id, action, actor_id, changes, created_at, tenant_seq, prev_hash, record_hash, hash_version"

// safeLimit converts a non-negative int to uint64 for squirrel's Limit method.
// Returns 0 for negative values (which squirrel treats as no limit).
func safeLimit(n int) uint64 {
	if n <= 0 {
		return 0
	}

	return uint64(n)
}

// buildNextCursor computes the next page cursor when we fetched limit+1 rows.
// The "+1" pattern fetches one extra row to determine if more pages exist:
// if we get limit+1 rows, there's a next page; we return only limit rows plus a cursor.
// Returns trimmed logs and the cursor string (empty if no more pages).
func buildNextCursor(logs []*entities.AuditLog, limit int) ([]*entities.AuditLog, string) {
	if len(logs) <= limit || limit <= 0 {
		return logs, ""
	}

	lastItem := logs[limit-1]

	cursor := sharedhttp.EncodeTimestampCursor(lastItem.CreatedAt, lastItem.ID)

	return logs[:limit], cursor
}

// Repository persists audit logs in Postgres.
//
// TODO(compliance): Retention & Archival Policy for SOX/GDPR Compliance
//
// OVERVIEW:
// Audit logs must be retained for a minimum of 7 years (SOX requirement) with
// GDPR-compliant data handling. This design uses PostgreSQL native partitioning
// with a tiered storage strategy to balance query performance against cost.
//
// RETENTION TIERS:
//   - Hot tier (0-90 days): Current partitions in primary PostgreSQL. Fully indexed,
//     fastest query performance. Used for real-time dashboards and active investigations.
//   - Warm tier (90 days - 2 years): Older partitions remain in PostgreSQL but on
//     lower-cost storage (e.g., pg_tablespace on cheaper disks or a separate
//     PostgreSQL instance with relaxed IOPS). Indexes may be selectively dropped
//     (e.g., keep only entity_type+entity_id, drop actor_id index).
//   - Cold tier (2-7 years): Archived to object storage (S3/GCS) as compressed
//     Parquet files, organized by tenant_id/year/month. Query access via a
//     federation layer (see below).
//   - Purge (>7 years): Automatic deletion via lifecycle policies on object storage
//     and DROP PARTITION on any remaining database partitions.
//
// PARTITIONING STRATEGY:
//
//	Use PostgreSQL declarative range partitioning on created_at with monthly
//	granularity. This enables efficient partition pruning for date-range queries
//	and O(1) archival via DETACH PARTITION + pg_dump.
//
//	Migration steps:
//	1. Create partitioned audit_logs table:
//	   CREATE TABLE audit_logs (...) PARTITION BY RANGE (created_at);
//	2. Create monthly partitions via pg_partman or a scheduled job:
//	   CREATE TABLE audit_logs_2026_01 PARTITION OF audit_logs
//	     FOR VALUES FROM ('2026-01-01') TO ('2026-02-01');
//	3. Add pg_partman configuration for automatic partition creation/detachment:
//	   SELECT partman.create_parent('public.audit_logs', 'created_at', 'native', 'monthly');
//	4. Migrate existing data into the partitioned table (backfill).
//	5. Ensure hash chain integrity is preserved across partition boundaries
//	   (prev_hash references work across partitions since they share the same
//	   logical table).
//
// ARCHIVAL WORKFLOW (warm -> cold):
//
//	Implemented as a periodic background worker (e.g., monthly cron or Go worker):
//	1. DETACH the target monthly partition from the live table.
//	2. Export the detached partition to compressed Parquet/CSV via COPY or pg_dump.
//	3. Upload to object storage with path: s3://audit-archive/{tenant_id}/{year}/{month}/
//	4. Verify upload integrity (checksum comparison).
//	5. Record the archival metadata (partition name, row count, checksum, archive path)
//	   in an audit_archive_manifest table.
//	6. DROP the detached partition only after successful verification.
//
// QUERY FEDERATION (searching across hot + cold):
//
//	For queries spanning archived periods:
//	1. Primary path: Query the live partitioned table (hot + warm tiers). PostgreSQL
//	   partition pruning handles date-range filtering automatically.
//	2. Cold path: If the requested date range extends beyond warm tier, query the
//	   audit_archive_manifest to locate relevant Parquet files, then use an
//	   external query engine (e.g., DuckDB, Athena, or a Go service that reads
//	   Parquet from S3) to search archived data.
//	3. Merge results in the application layer, maintaining created_at DESC ordering.
//	4. The API contract (cursor pagination) remains unchanged; the handler detects
//	   when live results are exhausted and transparently switches to archive queries.
//
// GDPR CONSIDERATIONS:
//   - actor_id may contain PII (email addresses). Implement pseudonymization for
//     cold-tier archives: replace actor_id with a salted hash, store the mapping
//     in a separate, access-controlled table with its own retention policy.
//   - Support GDPR erasure requests by updating the pseudonymization mapping table
//     (delete the mapping, rendering archived actor_id irreversible).
//   - changes JSON may contain PII; apply field-level redaction during archival
//     based on a configurable redaction policy per entity_type.
//
// MIGRATION REQUIREMENTS:
//   - Migration 000XXX_partition_audit_logs.up.sql: Convert audit_logs to partitioned table.
//   - Migration 000XXX_audit_archive_manifest.up.sql: Create manifest table for tracking archives.
//   - Both migrations must be reversible (provide .down.sql).
//   - Data backfill must be performed in batches to avoid long locks.
//   - Hash chain verification must be run after migration to ensure integrity.
//
// ESTIMATED EFFORT: 1-2 sprints (partitioning + archival worker + federation layer).
type Repository struct {
	provider ports.InfrastructureProvider
}

// NewRepository creates a new audit log repository.
func NewRepository(provider ports.InfrastructureProvider) *Repository {
	return &Repository{provider: provider}
}

func (repo *Repository) validateListByEntityParams(
	entityType string,
	entityID uuid.UUID,
	limit int,
) (string, error) {
	if repo == nil || repo.provider == nil {
		return "", ErrRepositoryNotInitialized
	}

	trimmedEntityType := strings.TrimSpace(entityType)
	if trimmedEntityType == "" {
		return "", entities.ErrEntityTypeRequired
	}

	if entityID == uuid.Nil {
		return "", entities.ErrEntityIDRequired
	}

	if limit <= 0 {
		return "", ErrLimitMustBePositive
	}

	return trimmedEntityType, nil
}

func getTenantIDFromContext(ctx context.Context) (uuid.UUID, error) {
	tenantIDStr := auth.GetTenantID(ctx)

	if tenantIDStr == "" {
		return uuid.Nil, entities.ErrTenantIDRequired
	}

	tenantID, err := uuid.Parse(tenantIDStr)
	if err != nil {
		logger, _, _, _ := libCommons.NewTrackingFromContext(ctx)
		logger.Log(ctx, libLog.LevelWarn, fmt.Sprintf("invalid tenant ID format in context: %q", tenantIDStr))

		return uuid.Nil, fmt.Errorf("invalid tenant id format: %w", entities.ErrTenantIDRequired)
	}

	return tenantID, nil
}

// Create inserts a new audit log entry.
func (repo *Repository) Create(
	ctx context.Context,
	auditLog *entities.AuditLog,
) (*entities.AuditLog, error) {
	if repo == nil || repo.provider == nil {
		return nil, ErrRepositoryNotInitialized
	}

	if auditLog == nil {
		return nil, ErrAuditLogRequired
	}

	logger, tracer, _, _ := libCommons.NewTrackingFromContext(ctx)
	ctx, span := tracer.Start(ctx, "postgres.create_audit_log")

	defer span.End()

	result, err := pgcommon.WithTenantTxProvider(
		ctx,
		repo.provider,
		func(tx *sql.Tx) (*entities.AuditLog, error) {
			return repo.executeCreate(ctx, tx, auditLog)
		},
	)
	if err != nil {
		wrappedErr := fmt.Errorf("create audit log transaction: %w", err)
		libOpentelemetry.HandleSpanError(span, "failed to create audit log", wrappedErr)

		logger.Log(ctx, libLog.LevelError, fmt.Sprintf("failed to create audit log: %v", wrappedErr))

		return nil, wrappedErr
	}

	return result, nil
}

// CreateWithTx inserts a new audit log entry using the provided transaction.
// This enables atomic audit logging within the same transaction as the audited operation.
func (repo *Repository) CreateWithTx(
	ctx context.Context,
	tx repositories.Tx,
	auditLog *entities.AuditLog,
) (*entities.AuditLog, error) {
	if repo == nil || repo.provider == nil {
		return nil, ErrRepositoryNotInitialized
	}

	if auditLog == nil {
		return nil, ErrAuditLogRequired
	}

	if tx == nil {
		return nil, ErrTransactionRequired
	}

	logger, tracer, _, _ := libCommons.NewTrackingFromContext(ctx)
	ctx, span := tracer.Start(ctx, "postgres.create_audit_log_with_tx")

	defer span.End()

	result, err := pgcommon.WithTenantTxOrExistingProvider(
		ctx,
		repo.provider,
		tx,
		func(innerTx *sql.Tx) (*entities.AuditLog, error) {
			return repo.executeCreate(ctx, innerTx, auditLog)
		},
	)
	if err != nil {
		wrappedErr := fmt.Errorf("create audit log transaction: %w", err)
		libOpentelemetry.HandleSpanError(span, "failed to create audit log", wrappedErr)

		logger.Log(ctx, libLog.LevelError, fmt.Sprintf("failed to create audit log: %v", wrappedErr))

		return nil, wrappedErr
	}

	return result, nil
}

// executeCreate performs the actual audit log creation within a transaction.
// It computes the hash chain by:
//  1. Acquiring the next sequence number for the tenant
//  2. Fetching the previous record's hash (or genesis hash for first record)
//  3. Computing the record hash and inserting with all hash chain fields.
//
// Timestamp precision: CreatedAt is truncated to microseconds before hashing
// and insertion to match PostgreSQL TIMESTAMPTZ storage precision. Without this,
// nanosecond-precision timestamps produce hashes that cannot be verified after
// a round-trip through PostgreSQL.
func (repo *Repository) executeCreate(
	ctx context.Context,
	tx *sql.Tx,
	auditLog *entities.AuditLog,
) (*entities.AuditLog, error) {
	tenantID, err := getTenantIDFromContext(ctx)
	if err == nil {
		auditLog.TenantID = tenantID
	} else {
		logger, _, _, _ := libCommons.NewTrackingFromContext(ctx)
		logger.Log(ctx, libLog.LevelWarn, fmt.Sprintf("tenant ID not found in context, using pre-set value on audit log: %v", err))
	}

	// Normalize timestamp to microsecond precision before hashing.
	// PostgreSQL TIMESTAMPTZ stores at most microsecond resolution; without
	// truncation the hash is computed from nanosecond-precision JSON but the
	// stored timestamp loses the extra digits, making VerifyRecordHash fail
	// on any subsequent read-back.
	auditLog.CreatedAt = auditLog.CreatedAt.Truncate(time.Microsecond)

	tenantSeq, err := repo.acquireNextSequence(ctx, tx, auditLog.TenantID)
	if err != nil {
		return nil, fmt.Errorf("acquire sequence: %w", err)
	}

	prevHash, err := repo.getPreviousHash(ctx, tx, auditLog.TenantID, tenantSeq)
	if err != nil {
		return nil, fmt.Errorf("get previous hash: %w", err)
	}

	recordData := hashchain.RecordData{
		ID:          auditLog.ID,
		TenantID:    auditLog.TenantID,
		TenantSeq:   tenantSeq,
		EntityType:  auditLog.EntityType,
		EntityID:    auditLog.EntityID,
		Action:      auditLog.Action,
		ActorID:     auditLog.ActorID,
		Changes:     json.RawMessage(auditLog.Changes),
		CreatedAt:   auditLog.CreatedAt,
		HashVersion: hashchain.HashVersion,
	}

	recordHash, err := hashchain.ComputeRecordHash(prevHash, recordData)
	if err != nil {
		return nil, fmt.Errorf("compute record hash: %w", err)
	}

	row := tx.QueryRowContext(
		ctx,
		`INSERT INTO audit_logs (id, tenant_id, entity_type, entity_id, action, actor_id, changes, created_at, tenant_seq, prev_hash, record_hash, hash_version)
		 VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12)
		 RETURNING `+auditLogColumns,
		auditLog.ID,
		auditLog.TenantID,
		auditLog.EntityType,
		auditLog.EntityID,
		auditLog.Action,
		auditLog.ActorID,
		auditLog.Changes,
		auditLog.CreatedAt,
		tenantSeq,
		prevHash,
		recordHash,
		hashchain.HashVersion,
	)

	return scanAuditLog(row)
}

// acquireNextSequence atomically increments and returns the next sequence number for a tenant.
// Uses INSERT ... ON CONFLICT for upsert to handle first-time tenant initialization.
//
// CONCURRENCY SAFETY: Before the upsert, this method acquires an exclusive row lock
// (SELECT ... FOR UPDATE) on the tenant's chain state row. This serializes all hash
// chain writes per tenant, preventing race conditions where concurrent writers could
// observe stale previous hashes via getPreviousHash and fork the hash chain.
//
// The lock is held until the transaction commits, ensuring:
//  1. Only one writer progresses past this point per tenant at a time.
//  2. The subsequent getPreviousHash sees the prior writer's fully committed data.
//
// For the first record (no chain_state row yet), the SELECT returns sql.ErrNoRows
// and the upsert INSERT path atomically creates and locks the new row.
func (repo *Repository) acquireNextSequence(
	ctx context.Context,
	tx *sql.Tx,
	tenantID uuid.UUID,
) (int64, error) {
	// Acquire exclusive row lock on existing chain state to serialize writers.
	// Without this lock, concurrent writers could read stale previous hashes,
	// forking the hash chain (see getPreviousHash for the read side).
	var lockPlaceholder int

	err := tx.QueryRowContext(
		ctx,
		`SELECT 1 FROM audit_log_chain_state WHERE tenant_id = $1 FOR UPDATE`,
		tenantID,
	).Scan(&lockPlaceholder)
	if err != nil && !errors.Is(err, sql.ErrNoRows) {
		return 0, fmt.Errorf("lock chain state: %w", err)
	}

	// sql.ErrNoRows means first record for this tenant; the INSERT path below
	// will atomically create and lock the row.

	var nextSeq int64

	err = tx.QueryRowContext(
		ctx,
		`INSERT INTO audit_log_chain_state (tenant_id, next_seq)
		 VALUES ($1, 2)
		 ON CONFLICT (tenant_id) DO UPDATE SET next_seq = audit_log_chain_state.next_seq + 1
		 RETURNING next_seq - 1`,
		tenantID,
	).Scan(&nextSeq)
	if err != nil {
		return 0, fmt.Errorf("upsert chain state: %w", err)
	}

	return nextSeq, nil
}

// getPreviousHash retrieves the hash of the previous record in the chain.
// Returns genesis hash (32 zero bytes) for the first record (seq=1).
//
// SAFETY: This query does not use FOR SHARE/FOR UPDATE because the exclusive
// row lock acquired by acquireNextSequence on audit_log_chain_state serializes
// all writers for the same tenant. By the time this method executes, any prior
// writer has already committed its audit_logs INSERT (it had to release the
// chain_state row lock that we now hold, which requires its transaction to commit).
func (repo *Repository) getPreviousHash(
	ctx context.Context,
	tx *sql.Tx,
	tenantID uuid.UUID,
	currentSeq int64,
) ([]byte, error) {
	if currentSeq == 1 {
		return hashchain.GenesisHash(), nil
	}

	var prevHash []byte

	err := tx.QueryRowContext(
		ctx,
		`SELECT record_hash FROM audit_logs WHERE tenant_id = $1 AND tenant_seq = $2`,
		tenantID,
		currentSeq-1,
	).Scan(&prevHash)
	if err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return nil, fmt.Errorf("%w: seq %d", ErrPreviousRecordNotFound, currentSeq-1)
		}

		return nil, fmt.Errorf("query previous hash: %w", err)
	}

	return prevHash, nil
}

// GetByID retrieves a single audit log by its ID.
// Tenant is extracted from context via auth.GetTenantID(ctx).
func (repo *Repository) GetByID(ctx context.Context, id uuid.UUID) (*entities.AuditLog, error) {
	if repo == nil || repo.provider == nil {
		return nil, ErrRepositoryNotInitialized
	}

	if id == uuid.Nil {
		return nil, ErrIDRequired
	}

	tenantID, err := getTenantIDFromContext(ctx)
	if err != nil {
		return nil, entities.ErrTenantIDRequired
	}

	logger, tracer, _, _ := libCommons.NewTrackingFromContext(ctx)
	ctx, span := tracer.Start(ctx, "postgres.get_audit_log_by_id")

	defer span.End()

	result, err := pgcommon.WithTenantReadQuery(
		ctx,
		repo.provider,
		func(qe pgcommon.QueryExecutor) (*entities.AuditLog, error) {
			row := qe.QueryRowContext(
				ctx,
				"SELECT "+auditLogColumns+" FROM audit_logs WHERE id = $1 AND tenant_id = $2",
				id,
				tenantID,
			)

			return scanAuditLog(row)
		},
	)
	if err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return nil, ErrAuditLogNotFound
		}

		wrappedErr := fmt.Errorf("get audit log by id transaction: %w", err)
		libOpentelemetry.HandleSpanError(span, "failed to get audit log by id", wrappedErr)

		logger.Log(ctx, libLog.LevelError, fmt.Sprintf("failed to get audit log by id: %v", wrappedErr))

		return nil, wrappedErr
	}

	return result, nil
}

// ListByEntity retrieves audit logs for a specific entity using cursor-based pagination.
// Uses timestamp+ID keyset pagination for correct ordering with (created_at DESC, id DESC).
// Tenant is extracted from context via auth.GetTenantID(ctx).
// Returns the logs, next cursor (empty if no more pages), and any error.
func (repo *Repository) ListByEntity(
	ctx context.Context,
	entityType string,
	entityID uuid.UUID,
	cursor *sharedhttp.TimestampCursor,
	limit int,
) ([]*entities.AuditLog, string, error) {
	trimmedEntityType, err := repo.validateListByEntityParams(entityType, entityID, limit)
	if err != nil {
		return nil, "", err
	}

	tenantID, err := getTenantIDFromContext(ctx)
	if err != nil {
		return nil, "", entities.ErrTenantIDRequired
	}

	logger, tracer, _, _ := libCommons.NewTrackingFromContext(ctx)
	ctx, span := tracer.Start(ctx, "postgres.list_audit_logs")

	defer span.End()

	type listResult struct {
		logs       []*entities.AuditLog
		nextCursor string
	}

	result, err := pgcommon.WithTenantReadQuery(
		ctx,
		repo.provider,
		func(qe pgcommon.QueryExecutor) (*listResult, error) {
			qb := squirrel.Select(strings.Split(auditLogColumns, ", ")...).
				From("audit_logs").
				Where(squirrel.Eq{"tenant_id": tenantID}).
				Where(squirrel.Eq{"entity_type": trimmedEntityType}).
				Where(squirrel.Eq{"entity_id": entityID}).
				OrderBy("created_at DESC", "id DESC").
				Limit(safeLimit(limit + 1)).
				PlaceholderFormat(squirrel.Dollar)

			if cursor != nil {
				qb = qb.Where("(created_at, id) < (?, ?)", cursor.Timestamp, cursor.ID)
			}

			query, args, err := qb.ToSql()
			if err != nil {
				return nil, fmt.Errorf("building query: %w", err)
			}

			rows, err := qe.QueryContext(ctx, query, args...)
			if err != nil {
				return nil, fmt.Errorf("querying audit logs: %w", err)
			}

			defer rows.Close()

			logs := make([]*entities.AuditLog, 0, limit)

			for rows.Next() {
				log, err := scanAuditLog(rows)
				if err != nil {
					return nil, fmt.Errorf("scanning audit log: %w", err)
				}

				logs = append(logs, log)
			}

			if err := rows.Err(); err != nil {
				return nil, fmt.Errorf("iterating audit logs: %w", err)
			}

			logs, nextCursor := buildNextCursor(logs, limit)

			return &listResult{logs: logs, nextCursor: nextCursor}, nil
		},
	)
	if err != nil {
		wrappedErr := fmt.Errorf("list audit logs by entity transaction: %w", err)
		libOpentelemetry.HandleSpanError(span, "failed to list audit logs by entity", wrappedErr)

		logger.Log(ctx, libLog.LevelError, fmt.Sprintf("failed to list audit logs: %v", wrappedErr))

		return nil, "", wrappedErr
	}

	return result.logs, result.nextCursor, nil
}

func applyAuditLogFilter(
	qb squirrel.SelectBuilder,
	filter entities.AuditLogFilter,
	cursor *sharedhttp.TimestampCursor,
) squirrel.SelectBuilder {
	if filter.Actor != nil {
		qb = qb.Where(squirrel.Eq{"actor_id": *filter.Actor})
	}

	if filter.Action != nil {
		qb = qb.Where(squirrel.Eq{"action": *filter.Action})
	}

	if filter.EntityType != nil {
		qb = qb.Where(squirrel.Eq{"entity_type": *filter.EntityType})
	}

	if filter.DateFrom != nil {
		qb = qb.Where(squirrel.GtOrEq{"created_at": *filter.DateFrom})
	}

	if filter.DateTo != nil {
		qb = qb.Where(squirrel.LtOrEq{"created_at": *filter.DateTo})
	}

	if cursor != nil {
		qb = qb.Where("(created_at, id) < (?, ?)", cursor.Timestamp, cursor.ID)
	}

	return qb
}

// List retrieves audit logs with optional filters using cursor-based pagination.
// Uses timestamp+ID keyset pagination for correct ordering with (created_at DESC, id DESC).
// Tenant is extracted from context via auth.GetTenantID(ctx).
// Returns the logs, next cursor (empty if no more pages), and any error.
func (repo *Repository) List(
	ctx context.Context,
	filter entities.AuditLogFilter,
	cursor *sharedhttp.TimestampCursor,
	limit int,
) ([]*entities.AuditLog, string, error) {
	if repo == nil || repo.provider == nil {
		return nil, "", ErrRepositoryNotInitialized
	}

	if limit <= 0 {
		return nil, "", ErrLimitMustBePositive
	}

	tenantID, err := getTenantIDFromContext(ctx)
	if err != nil {
		return nil, "", entities.ErrTenantIDRequired
	}

	logger, tracer, _, _ := libCommons.NewTrackingFromContext(ctx)
	ctx, span := tracer.Start(ctx, "postgres.list_audit_logs")

	defer span.End()

	type listResult struct {
		logs       []*entities.AuditLog
		nextCursor string
	}

	result, err := pgcommon.WithTenantReadQuery(
		ctx,
		repo.provider,
		func(qe pgcommon.QueryExecutor) (*listResult, error) {
			qb := squirrel.Select(strings.Split(auditLogColumns, ", ")...).
				From("audit_logs").
				Where(squirrel.Eq{"tenant_id": tenantID}).
				OrderBy("created_at DESC", "id DESC").
				Limit(safeLimit(limit + 1)).
				PlaceholderFormat(squirrel.Dollar)

			qb = applyAuditLogFilter(qb, filter, cursor)

			query, args, err := qb.ToSql()
			if err != nil {
				return nil, fmt.Errorf("building query: %w", err)
			}

			rows, err := qe.QueryContext(ctx, query, args...)
			if err != nil {
				return nil, fmt.Errorf("querying audit logs: %w", err)
			}

			defer rows.Close()

			logs := make([]*entities.AuditLog, 0, limit)

			for rows.Next() {
				log, err := scanAuditLog(rows)
				if err != nil {
					return nil, fmt.Errorf("scanning audit log: %w", err)
				}

				logs = append(logs, log)
			}

			if err := rows.Err(); err != nil {
				return nil, fmt.Errorf("iterating audit logs: %w", err)
			}

			logs, nextCursor := buildNextCursor(logs, limit)

			return &listResult{logs: logs, nextCursor: nextCursor}, nil
		},
	)
	if err != nil {
		wrappedErr := fmt.Errorf("list audit logs transaction: %w", err)
		libOpentelemetry.HandleSpanError(span, "failed to list audit logs", wrappedErr)

		logger.Log(ctx, libLog.LevelError, fmt.Sprintf("failed to list audit logs: %v", wrappedErr))

		return nil, "", wrappedErr
	}

	return result.logs, result.nextCursor, nil
}

func scanAuditLog(scanner interface{ Scan(dest ...any) error }) (*entities.AuditLog, error) {
	if scanner == nil {
		return nil, fmt.Errorf("scanning audit log: %w", ErrNilScanner)
	}

	var log entities.AuditLog

	var (
		tenantSeq   sql.NullInt64
		prevHash    []byte
		recordHash  []byte
		hashVersion sql.NullInt16
	)

	if err := scanner.Scan(
		&log.ID,
		&log.TenantID,
		&log.EntityType,
		&log.EntityID,
		&log.Action,
		&log.ActorID,
		&log.Changes,
		&log.CreatedAt,
		&tenantSeq,
		&prevHash,
		&recordHash,
		&hashVersion,
	); err != nil {
		return nil, fmt.Errorf("scanning audit log: %w", err)
	}

	// Normalize to UTC. The pgx/v5 stdlib driver may return TIMESTAMPTZ values
	// in the machine's local timezone. All domain code expects UTC timestamps.
	log.CreatedAt = log.CreatedAt.UTC()

	if tenantSeq.Valid {
		log.TenantSeq = tenantSeq.Int64
	}

	log.PrevHash = prevHash
	log.RecordHash = recordHash

	if hashVersion.Valid {
		log.HashVersion = hashVersion.Int16
	}

	return &log, nil
}

var _ repositories.AuditLogRepository = (*Repository)(nil)
