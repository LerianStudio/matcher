// Copyright 2025 Lerian Studio. All rights reserved.
// Use of this source code is governed by an Elastic License 2.0
// that can be found in the LICENSE.md file.

// Package postgres provides PostgreSQL adapters for the governance bounded context.
package postgres

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"time"

	"github.com/google/uuid"

	libCommons "github.com/LerianStudio/lib-commons/v5/commons"
	libLog "github.com/LerianStudio/lib-commons/v5/commons/log"
	sharedhttp "github.com/LerianStudio/lib-commons/v5/commons/net/http"

	"github.com/LerianStudio/matcher/internal/auth"
	"github.com/LerianStudio/matcher/internal/governance/domain/entities"
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
func buildNextCursor(logs []*entities.AuditLog, limit int) ([]*entities.AuditLog, string, error) {
	return buildNextCursorWithEncoder(logs, limit, sharedhttp.EncodeTimestampCursor)
}

func buildNextCursorWithEncoder(
	logs []*entities.AuditLog,
	limit int,
	encodeCursor func(time.Time, uuid.UUID) (string, error),
) ([]*entities.AuditLog, string, error) {
	trimmedLogs, nextCursor, err := pgcommon.TrimRecordsAndEncodeTimestampNextCursor(
		logs,
		limit,
		func(log *entities.AuditLog) (time.Time, uuid.UUID) {
			if log == nil {
				return time.Time{}, uuid.Nil
			}

			return log.CreatedAt, log.ID
		},
		encodeCursor,
	)
	if err != nil {
		if errors.Is(err, pgcommon.ErrCursorEncoderRequired) {
			return trimmedLogs, "", fmt.Errorf("encode next cursor: %w", ErrCursorEncoderRequired)
		}

		return trimmedLogs, "", fmt.Errorf("trim records and encode cursor: %w", err)
	}

	return trimmedLogs, nextCursor, nil
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
