// Package archivemetadata provides the PostgreSQL adapter for archive metadata persistence.
package archivemetadata

import (
	"context"
	"database/sql"
	"errors"
	"fmt"

	"github.com/LerianStudio/matcher/internal/governance/domain/entities"
	pgcommon "github.com/LerianStudio/matcher/internal/shared/adapters/postgres/common"
)

// selectColumns lists the columns used in SELECT queries.
var selectColumns = []string{
	"id", "tenant_id", "partition_name", "date_range_start", "date_range_end",
	"row_count", "archive_key", "checksum", "compressed_size_bytes", "storage_class",
	"status", "error_message", "archived_at", "created_at", "updated_at",
}

// Sentinel errors for the archive metadata repository.
var (
	ErrRepositoryNotInitialized = errors.New("archive metadata repository not initialized")
	ErrMetadataRequired         = errors.New("archive metadata is required")
	ErrMetadataNotFound         = errors.New("archive metadata not found")
	ErrIDRequired               = errors.New("id is required")
	ErrTenantIDRequired         = errors.New("tenant id is required")
	ErrPartitionNameRequired    = errors.New("partition name is required")
	ErrLimitMustBePositive      = errors.New("limit must be positive")
	ErrNilScanner               = errors.New("nil scanner")
	ErrTransactionRequired      = errors.New("transaction is required")
)

func scanArchiveMetadata(scanner interface{ Scan(dest ...any) error }) (*entities.ArchiveMetadata, error) {
	if scanner == nil {
		return nil, fmt.Errorf("scanning archive metadata: %w", ErrNilScanner)
	}

	var am entities.ArchiveMetadata

	var (
		archiveKey          sql.NullString
		checksum            sql.NullString
		compressedSizeBytes sql.NullInt64
		storageClass        sql.NullString
		errorMessage        sql.NullString
		archivedAt          sql.NullTime
	)

	if err := scanner.Scan(
		&am.ID,
		&am.TenantID,
		&am.PartitionName,
		&am.DateRangeStart,
		&am.DateRangeEnd,
		&am.RowCount,
		&archiveKey,
		&checksum,
		&compressedSizeBytes,
		&storageClass,
		&am.Status,
		&errorMessage,
		&archivedAt,
		&am.CreatedAt,
		&am.UpdatedAt,
	); err != nil {
		return nil, fmt.Errorf("scanning archive metadata: %w", err)
	}

	if archiveKey.Valid {
		am.ArchiveKey = archiveKey.String
	}

	if checksum.Valid {
		am.Checksum = checksum.String
	}

	if compressedSizeBytes.Valid {
		am.CompressedSizeBytes = compressedSizeBytes.Int64
	}

	if storageClass.Valid {
		am.StorageClass = storageClass.String
	}

	if errorMessage.Valid {
		am.ErrorMessage = errorMessage.String
	}

	if archivedAt.Valid {
		t := archivedAt.Time
		am.ArchivedAt = &t
	}

	return &am, nil
}

// scanArchiveMetadataRows executes a query and scans all returned rows into archive metadata structs.
func scanArchiveMetadataRows(
	ctx context.Context,
	qe pgcommon.QueryExecutor,
	query string,
	args []any,
) ([]*entities.ArchiveMetadata, error) {
	rows, err := qe.QueryContext(ctx, query, args...)
	if err != nil {
		return nil, fmt.Errorf("querying archive metadata: %w", err)
	}

	defer rows.Close()

	var results []*entities.ArchiveMetadata

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
}
