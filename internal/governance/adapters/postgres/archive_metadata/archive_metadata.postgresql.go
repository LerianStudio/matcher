package archivemetadata

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"time"

	"github.com/Masterminds/squirrel"
	"github.com/google/uuid"

	libCommons "github.com/LerianStudio/lib-uncommons/v2/uncommons"
	libLog "github.com/LerianStudio/lib-uncommons/v2/uncommons/log"
	libOpentelemetry "github.com/LerianStudio/lib-uncommons/v2/uncommons/opentelemetry"

	"github.com/LerianStudio/matcher/internal/auth"
	"github.com/LerianStudio/matcher/internal/governance/domain/entities"
	"github.com/LerianStudio/matcher/internal/governance/domain/repositories"
	pgcommon "github.com/LerianStudio/matcher/internal/shared/adapters/postgres/common"
	"github.com/LerianStudio/matcher/internal/shared/ports"
)

const (
	tableName = "archive_metadata"
	columns   = "id, tenant_id, partition_name, date_range_start, date_range_end, row_count, " +
		"archive_key, checksum, compressed_size_bytes, storage_class, status, error_message, " +
		"archived_at, created_at, updated_at"
)

// Repository persists archive metadata in PostgreSQL.
type Repository struct {
	provider ports.InfrastructureProvider
}

// NewRepository creates a new archive metadata repository.
func NewRepository(provider ports.InfrastructureProvider) *Repository {
	return &Repository{provider: provider}
}

// psq returns a new squirrel statement builder with Dollar placeholder format.
func psq() squirrel.StatementBuilderType {
	return squirrel.StatementBuilder.PlaceholderFormat(squirrel.Dollar)
}

// executeInsert builds and executes the INSERT statement for archive metadata within a transaction.
func executeInsert(ctx context.Context, tx *sql.Tx, metadata *entities.ArchiveMetadata) error {
	query, args, err := psq().
		Insert(tableName).
		Columns(
			"id", "tenant_id", "partition_name", "date_range_start", "date_range_end",
			"row_count", "archive_key", "checksum", "compressed_size_bytes", "storage_class",
			"status", "error_message", "archived_at", "created_at", "updated_at",
		).
		Values(
			metadata.ID, metadata.TenantID, metadata.PartitionName,
			metadata.DateRangeStart, metadata.DateRangeEnd,
			metadata.RowCount, metadata.ArchiveKey, metadata.Checksum,
			metadata.CompressedSizeBytes, metadata.StorageClass,
			metadata.Status, metadata.ErrorMessage, metadata.ArchivedAt,
			metadata.CreatedAt, metadata.UpdatedAt,
		).
		ToSql()
	if err != nil {
		return fmt.Errorf("building insert query: %w", err)
	}

	if _, err := tx.ExecContext(ctx, query, args...); err != nil {
		return fmt.Errorf("inserting archive metadata: %w", err)
	}

	return nil
}

// executeUpdate builds and executes the UPDATE statement for archive metadata within a transaction.
// The WHERE clause includes both id and tenant_id for defense-in-depth tenant isolation,
// consistent with GetByID and other tenant-scoped queries.
func executeUpdate(ctx context.Context, tx *sql.Tx, metadata *entities.ArchiveMetadata) error {
	query, args, err := psq().
		Update(tableName).
		Set("row_count", metadata.RowCount).
		Set("archive_key", metadata.ArchiveKey).
		Set("checksum", metadata.Checksum).
		Set("compressed_size_bytes", metadata.CompressedSizeBytes).
		Set("storage_class", metadata.StorageClass).
		Set("status", metadata.Status).
		Set("error_message", metadata.ErrorMessage).
		Set("archived_at", metadata.ArchivedAt).
		Set("updated_at", metadata.UpdatedAt).
		Where(squirrel.Eq{"id": metadata.ID, "tenant_id": metadata.TenantID}).
		ToSql()
	if err != nil {
		return fmt.Errorf("building update query: %w", err)
	}

	result, err := tx.ExecContext(ctx, query, args...)
	if err != nil {
		return fmt.Errorf("updating archive metadata: %w", err)
	}

	rowsAffected, err := result.RowsAffected()
	if err != nil {
		return fmt.Errorf("checking rows affected: %w", err)
	}

	if rowsAffected == 0 {
		return ErrMetadataNotFound
	}

	return nil
}

// Create inserts a new archive metadata record.
func (repo *Repository) Create(ctx context.Context, metadata *entities.ArchiveMetadata) error {
	if repo == nil || repo.provider == nil {
		return ErrRepositoryNotInitialized
	}

	if metadata == nil {
		return ErrMetadataRequired
	}

	logger, tracer, _, _ := libCommons.NewTrackingFromContext(ctx)
	ctx, span := tracer.Start(ctx, "postgres.create_archive_metadata")

	defer span.End()

	_, err := pgcommon.WithTenantTxProvider(
		ctx,
		repo.provider,
		func(tx *sql.Tx) (struct{}, error) {
			return struct{}{}, executeInsert(ctx, tx, metadata)
		},
	)
	if err != nil {
		wrappedErr := fmt.Errorf("create archive metadata: %w", err)
		libOpentelemetry.HandleSpanError(span, "failed to create archive metadata", wrappedErr)

		logger.Log(ctx, libLog.LevelError, fmt.Sprintf("failed to create archive metadata: %v", wrappedErr))

		return wrappedErr
	}

	return nil
}

// CreateWithTx inserts a new archive metadata record using the provided transaction.
func (repo *Repository) CreateWithTx(ctx context.Context, tx *sql.Tx, metadata *entities.ArchiveMetadata) error {
	if repo == nil || repo.provider == nil {
		return ErrRepositoryNotInitialized
	}

	if metadata == nil {
		return ErrMetadataRequired
	}

	if tx == nil {
		return ErrTransactionRequired
	}

	logger, tracer, _, _ := libCommons.NewTrackingFromContext(ctx)
	ctx, span := tracer.Start(ctx, "postgres.create_archive_metadata_with_tx")

	defer span.End()

	_, err := pgcommon.WithTenantTxOrExistingProvider(
		ctx,
		repo.provider,
		tx,
		func(innerTx *sql.Tx) (struct{}, error) {
			return struct{}{}, executeInsert(ctx, innerTx, metadata)
		},
	)
	if err != nil {
		wrappedErr := fmt.Errorf("create archive metadata with tx: %w", err)
		libOpentelemetry.HandleSpanError(span, "failed to create archive metadata with tx", wrappedErr)

		logger.Log(ctx, libLog.LevelError, fmt.Sprintf("failed to create archive metadata with tx: %v", wrappedErr))

		return wrappedErr
	}

	return nil
}

// Update persists changes to an existing archive metadata record.
func (repo *Repository) Update(ctx context.Context, metadata *entities.ArchiveMetadata) error {
	if repo == nil || repo.provider == nil {
		return ErrRepositoryNotInitialized
	}

	if metadata == nil {
		return ErrMetadataRequired
	}

	logger, tracer, _, _ := libCommons.NewTrackingFromContext(ctx)
	ctx, span := tracer.Start(ctx, "postgres.update_archive_metadata")

	defer span.End()

	_, err := pgcommon.WithTenantTxProvider(
		ctx,
		repo.provider,
		func(tx *sql.Tx) (struct{}, error) {
			return struct{}{}, executeUpdate(ctx, tx, metadata)
		},
	)
	if err != nil {
		wrappedErr := fmt.Errorf("update archive metadata: %w", err)
		libOpentelemetry.HandleSpanError(span, "failed to update archive metadata", wrappedErr)

		logger.Log(ctx, libLog.LevelError, fmt.Sprintf("failed to update archive metadata: %v", wrappedErr))

		return wrappedErr
	}

	return nil
}

// UpdateWithTx persists changes to an existing archive metadata record using the provided transaction.
func (repo *Repository) UpdateWithTx(ctx context.Context, tx *sql.Tx, metadata *entities.ArchiveMetadata) error {
	if repo == nil || repo.provider == nil {
		return ErrRepositoryNotInitialized
	}

	if metadata == nil {
		return ErrMetadataRequired
	}

	if tx == nil {
		return ErrTransactionRequired
	}

	logger, tracer, _, _ := libCommons.NewTrackingFromContext(ctx)
	ctx, span := tracer.Start(ctx, "postgres.update_archive_metadata_with_tx")

	defer span.End()

	_, err := pgcommon.WithTenantTxOrExistingProvider(
		ctx,
		repo.provider,
		tx,
		func(innerTx *sql.Tx) (struct{}, error) {
			return struct{}{}, executeUpdate(ctx, innerTx, metadata)
		},
	)
	if err != nil {
		wrappedErr := fmt.Errorf("update archive metadata with tx: %w", err)
		libOpentelemetry.HandleSpanError(span, "failed to update archive metadata with tx", wrappedErr)

		logger.Log(ctx, libLog.LevelError, fmt.Sprintf("failed to update archive metadata with tx: %v", wrappedErr))

		return wrappedErr
	}

	return nil
}

// GetByID retrieves a single archive metadata record by its ID.
// Tenant is extracted from context via auth.GetTenantID(ctx) and included in the
// WHERE clause to prevent cross-tenant information disclosure via timing side-channels.
func (repo *Repository) GetByID(ctx context.Context, id uuid.UUID) (*entities.ArchiveMetadata, error) {
	if repo == nil || repo.provider == nil {
		return nil, ErrRepositoryNotInitialized
	}

	if id == uuid.Nil {
		return nil, ErrIDRequired
	}

	tenantIDStr := auth.GetTenantID(ctx)

	tenantID, err := uuid.Parse(tenantIDStr)
	if err != nil {
		return nil, ErrTenantIDRequired
	}

	logger, tracer, _, _ := libCommons.NewTrackingFromContext(ctx)
	ctx, span := tracer.Start(ctx, "postgres.get_archive_metadata_by_id")

	defer span.End()

	result, err := pgcommon.WithTenantReadQuery(
		ctx,
		repo.provider,
		func(qe pgcommon.QueryExecutor) (*entities.ArchiveMetadata, error) {
			query, args, err := psq().
				Select(selectColumns...).
				From(tableName).
				Where(squirrel.Eq{"id": id, "tenant_id": tenantID}).
				ToSql()
			if err != nil {
				return nil, fmt.Errorf("building select query: %w", err)
			}

			row := qe.QueryRowContext(ctx, query, args...)

			return scanArchiveMetadata(row)
		},
	)
	if err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return nil, ErrMetadataNotFound
		}

		wrappedErr := fmt.Errorf("get archive metadata by id: %w", err)
		libOpentelemetry.HandleSpanError(span, "failed to get archive metadata by id", wrappedErr)

		logger.Log(ctx, libLog.LevelError, fmt.Sprintf("failed to get archive metadata by id: %v", wrappedErr))

		return nil, wrappedErr
	}

	return result, nil
}

// GetByPartition retrieves archive metadata for a specific partition within a tenant.
func (repo *Repository) GetByPartition(
	ctx context.Context,
	tenantID uuid.UUID,
	partitionName string,
) (*entities.ArchiveMetadata, error) {
	if repo == nil || repo.provider == nil {
		return nil, ErrRepositoryNotInitialized
	}

	if tenantID == uuid.Nil {
		return nil, ErrTenantIDRequired
	}

	if partitionName == "" {
		return nil, ErrPartitionNameRequired
	}

	logger, tracer, _, _ := libCommons.NewTrackingFromContext(ctx)
	ctx, span := tracer.Start(ctx, "postgres.get_archive_metadata_by_partition")

	defer span.End()

	result, err := pgcommon.WithTenantReadQuery(
		ctx,
		repo.provider,
		func(qe pgcommon.QueryExecutor) (*entities.ArchiveMetadata, error) {
			query, args, err := psq().
				Select(selectColumns...).
				From(tableName).
				Where(squirrel.Eq{
					"tenant_id":      tenantID,
					"partition_name": partitionName,
				}).
				ToSql()
			if err != nil {
				return nil, fmt.Errorf("building select query: %w", err)
			}

			row := qe.QueryRowContext(ctx, query, args...)

			return scanArchiveMetadata(row)
		},
	)
	if err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return nil, ErrMetadataNotFound
		}

		wrappedErr := fmt.Errorf("get archive metadata by partition: %w", err)
		libOpentelemetry.HandleSpanError(span, "failed to get archive metadata by partition", wrappedErr)

		logger.Log(ctx, libLog.LevelError, fmt.Sprintf("failed to get archive metadata by partition: %v", wrappedErr))

		return nil, wrappedErr
	}

	return result, nil
}

// buildListByTenantQuery constructs the query for listing archive metadata with optional filters.
func buildListByTenantQuery(
	tenantID uuid.UUID,
	status string,
	from, to *time.Time,
	limit, offset int,
) squirrel.SelectBuilder {
	qb := psq().
		Select(selectColumns...).
		From(tableName).
		Where(squirrel.Eq{"tenant_id": tenantID}).
		OrderBy("created_at DESC").
		Limit(uint64(limit)).  //nolint:gosec //#nosec G115 -- limit is validated positive by caller
		Offset(uint64(offset)) //nolint:gosec //#nosec G115 -- offset is validated non-negative by caller

	if status != "" {
		qb = qb.Where(squirrel.Eq{"status": status})
	}

	if from != nil {
		qb = qb.Where(squirrel.GtOrEq{"date_range_start": *from})
	}

	if to != nil {
		qb = qb.Where(squirrel.LtOrEq{"date_range_end": *to})
	}

	return qb
}

// ListByTenant retrieves archive metadata for a tenant, optionally filtered by status and date bounds.
func (repo *Repository) ListByTenant(
	ctx context.Context,
	tenantID uuid.UUID,
	status string,
	from, to *time.Time,
	limit int,
	offset int,
) ([]*entities.ArchiveMetadata, error) {
	if repo == nil || repo.provider == nil {
		return nil, ErrRepositoryNotInitialized
	}

	if tenantID == uuid.Nil {
		return nil, ErrTenantIDRequired
	}

	if limit <= 0 {
		return nil, ErrLimitMustBePositive
	}

	logger, tracer, _, _ := libCommons.NewTrackingFromContext(ctx)
	ctx, span := tracer.Start(ctx, "postgres.list_archive_metadata_by_tenant")

	defer span.End()

	result, err := pgcommon.WithTenantReadQuery(
		ctx,
		repo.provider,
		func(qe pgcommon.QueryExecutor) ([]*entities.ArchiveMetadata, error) {
			qb := buildListByTenantQuery(tenantID, status, from, to, limit, offset)

			query, args, err := qb.ToSql()
			if err != nil {
				return nil, fmt.Errorf("building list query: %w", err)
			}

			rows, err := qe.QueryContext(ctx, query, args...)
			if err != nil {
				return nil, fmt.Errorf("querying archive metadata: %w", err)
			}

			defer rows.Close()

			results := make([]*entities.ArchiveMetadata, 0, limit)

			for rows.Next() {
				am, err := scanArchiveMetadata(rows)
				if err != nil {
					return nil, fmt.Errorf("scanning archive metadata: %w", err)
				}

				results = append(results, am)
			}

			if err := rows.Err(); err != nil {
				return nil, fmt.Errorf("iterating archive metadata: %w", err)
			}

			return results, nil
		},
	)
	if err != nil {
		wrappedErr := fmt.Errorf("list archive metadata by tenant: %w", err)
		libOpentelemetry.HandleSpanError(span, "failed to list archive metadata by tenant", wrappedErr)

		logger.Log(ctx, libLog.LevelError, fmt.Sprintf("failed to list archive metadata by tenant: %v", wrappedErr))

		return nil, wrappedErr
	}

	return result, nil
}

// ListPending retrieves all archive metadata records with PENDING status.
// NOTE: This intentionally queries across all tenants because it is used by the
// background archival worker to discover work items. The archive_metadata table
// lives in the shared/public schema, not per-tenant schemas.
func (repo *Repository) ListPending(ctx context.Context) ([]*entities.ArchiveMetadata, error) {
	if repo == nil || repo.provider == nil {
		return nil, ErrRepositoryNotInitialized
	}

	logger, tracer, _, _ := libCommons.NewTrackingFromContext(ctx)
	ctx, span := tracer.Start(ctx, "postgres.list_pending_archive_metadata")

	defer span.End()

	result, err := pgcommon.WithTenantReadQuery(
		ctx,
		repo.provider,
		func(qe pgcommon.QueryExecutor) ([]*entities.ArchiveMetadata, error) {
			query, args, err := psq().
				Select(selectColumns...).
				From(tableName).
				Where(squirrel.Eq{"status": entities.StatusPending}).
				OrderBy("created_at ASC").
				ToSql()
			if err != nil {
				return nil, fmt.Errorf("building list pending query: %w", err)
			}

			return scanArchiveMetadataRows(ctx, qe, query, args)
		},
	)
	if err != nil {
		wrappedErr := fmt.Errorf("list pending archive metadata: %w", err)
		libOpentelemetry.HandleSpanError(span, "failed to list pending archive metadata", wrappedErr)

		logger.Log(ctx, libLog.LevelError, fmt.Sprintf("failed to list pending archive metadata: %v", wrappedErr))

		return nil, wrappedErr
	}

	return result, nil
}

// ListIncomplete retrieves all archive metadata records that are not yet COMPLETE.
// NOTE: This intentionally queries across all tenants because it is used by the
// background archival worker for crash recovery to resume interrupted processes.
// The archive_metadata table lives in the shared/public schema, not per-tenant schemas.
func (repo *Repository) ListIncomplete(ctx context.Context) ([]*entities.ArchiveMetadata, error) {
	if repo == nil || repo.provider == nil {
		return nil, ErrRepositoryNotInitialized
	}

	logger, tracer, _, _ := libCommons.NewTrackingFromContext(ctx)
	ctx, span := tracer.Start(ctx, "postgres.list_incomplete_archive_metadata")

	defer span.End()

	result, err := pgcommon.WithTenantReadQuery(
		ctx,
		repo.provider,
		func(qe pgcommon.QueryExecutor) ([]*entities.ArchiveMetadata, error) {
			query, args, err := psq().
				Select(selectColumns...).
				From(tableName).
				Where(squirrel.NotEq{"status": entities.StatusComplete}).
				OrderBy("created_at ASC").
				ToSql()
			if err != nil {
				return nil, fmt.Errorf("building list incomplete query: %w", err)
			}

			return scanArchiveMetadataRows(ctx, qe, query, args)
		},
	)
	if err != nil {
		wrappedErr := fmt.Errorf("list incomplete archive metadata: %w", err)
		libOpentelemetry.HandleSpanError(span, "failed to list incomplete archive metadata", wrappedErr)

		logger.Log(ctx, libLog.LevelError, fmt.Sprintf("failed to list incomplete archive metadata: %v", wrappedErr))

		return nil, wrappedErr
	}

	return result, nil
}

// Compile-time interface compliance check.
var _ repositories.ArchiveMetadataRepository = (*Repository)(nil)
