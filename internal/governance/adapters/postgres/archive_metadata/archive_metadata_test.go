// Copyright 2025 Lerian Studio. All rights reserved.
// Use of this source code is governed by an Elastic License 2.0
// that can be found in the LICENSE.md file.

//go:build unit

package archivemetadata

import (
	"context"
	"database/sql"
	"errors"
	"testing"
	"time"

	"github.com/DATA-DOG/go-sqlmock"
	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/LerianStudio/matcher/internal/governance/domain/entities"
	"github.com/LerianStudio/matcher/internal/shared/infrastructure/testutil"
)

// --- selectColumns tests ---

func TestSelectColumns_Count(t *testing.T) {
	t.Parallel()

	require.Len(t, selectColumns, 15, "selectColumns must list exactly 15 columns matching the archive_metadata table")
}

func TestSelectColumns_NamesAndOrder(t *testing.T) {
	t.Parallel()

	expected := []string{
		"id", "tenant_id", "partition_name", "date_range_start", "date_range_end",
		"row_count", "archive_key", "checksum", "compressed_size_bytes", "storage_class",
		"status", "error_message", "archived_at", "created_at", "updated_at",
	}

	assert.Equal(t, expected, selectColumns)
}

func TestSelectColumns_NoDuplicates(t *testing.T) {
	t.Parallel()

	seen := make(map[string]bool, len(selectColumns))
	for _, col := range selectColumns {
		require.False(t, seen[col], "duplicate column found: %s", col)
		seen[col] = true
	}
}

// --- scanArchiveMetadata partial nullable field combination tests ---
// The sibling file tests all-null and all-populated; these test mixed combinations.

func TestScanArchiveMetadata_PartialNullable_ArchiveKeyAndChecksumOnly(t *testing.T) {
	t.Parallel()

	metadataID := testutil.DeterministicUUID("archive-metadata-partial-archive-key-id")
	tenantID := testutil.DeterministicUUID("archive-metadata-partial-archive-key-tenant-id")
	createdAt := testutil.FixedTime()

	scanner := &fakeScanner{scan: func(dest ...any) error {
		*dest[0].(*uuid.UUID) = metadataID
		*dest[1].(*uuid.UUID) = tenantID
		*dest[2].(*string) = "audit_logs_2026_01"
		*dest[3].(*time.Time) = time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
		*dest[4].(*time.Time) = time.Date(2026, 2, 1, 0, 0, 0, 0, time.UTC)
		*dest[5].(*int64) = 100
		*dest[6].(*sql.NullString) = sql.NullString{String: "archives/key.jsonl.gz", Valid: true}
		*dest[7].(*sql.NullString) = sql.NullString{String: "sha256:abc123", Valid: true}
		*dest[8].(*sql.NullInt64) = sql.NullInt64{Valid: false}
		*dest[9].(*sql.NullString) = sql.NullString{Valid: false}
		*dest[10].(*entities.ArchiveStatus) = entities.StatusExported
		*dest[11].(*sql.NullString) = sql.NullString{Valid: false}
		*dest[12].(*sql.NullTime) = sql.NullTime{Valid: false}
		*dest[13].(*time.Time) = createdAt
		*dest[14].(*time.Time) = createdAt

		return nil
	}}

	result, err := scanArchiveMetadata(scanner)
	require.NoError(t, err)
	require.NotNil(t, result)

	assert.Equal(t, metadataID, result.ID)
	assert.Equal(t, tenantID, result.TenantID)
	assert.Equal(t, "archives/key.jsonl.gz", result.ArchiveKey)
	assert.Equal(t, "sha256:abc123", result.Checksum)
	assert.Zero(t, result.CompressedSizeBytes)
	assert.Empty(t, result.StorageClass)
	assert.Empty(t, result.ErrorMessage)
	assert.Nil(t, result.ArchivedAt)
	assert.Equal(t, createdAt, result.CreatedAt)
}

func TestScanArchiveMetadata_PartialNullable_ErrorMessageAndArchivedAtOnly(t *testing.T) {
	t.Parallel()

	metadataID := uuid.New()
	tenantID := uuid.New()
	createdAt := time.Now().UTC()
	archivedAt := time.Date(2026, 1, 15, 12, 0, 0, 0, time.UTC)

	scanner := &fakeScanner{scan: func(dest ...any) error {
		*dest[0].(*uuid.UUID) = metadataID
		*dest[1].(*uuid.UUID) = tenantID
		*dest[2].(*string) = "audit_logs_2026_01"
		*dest[3].(*time.Time) = time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
		*dest[4].(*time.Time) = time.Date(2026, 2, 1, 0, 0, 0, 0, time.UTC)
		*dest[5].(*int64) = 0
		*dest[6].(*sql.NullString) = sql.NullString{Valid: false}
		*dest[7].(*sql.NullString) = sql.NullString{Valid: false}
		*dest[8].(*sql.NullInt64) = sql.NullInt64{Valid: false}
		*dest[9].(*sql.NullString) = sql.NullString{Valid: false}
		*dest[10].(*entities.ArchiveStatus) = entities.StatusComplete
		*dest[11].(*sql.NullString) = sql.NullString{String: "upload retry succeeded", Valid: true}
		*dest[12].(*sql.NullTime) = sql.NullTime{Time: archivedAt, Valid: true}
		*dest[13].(*time.Time) = createdAt
		*dest[14].(*time.Time) = createdAt

		return nil
	}}

	result, err := scanArchiveMetadata(scanner)
	require.NoError(t, err)
	require.NotNil(t, result)

	assert.Empty(t, result.ArchiveKey)
	assert.Empty(t, result.Checksum)
	assert.Zero(t, result.CompressedSizeBytes)
	assert.Empty(t, result.StorageClass)
	assert.Equal(t, "upload retry succeeded", result.ErrorMessage)
	require.NotNil(t, result.ArchivedAt)
	assert.Equal(t, archivedAt, *result.ArchivedAt)
}

func TestScanArchiveMetadata_PartialNullable_CompressedSizeAndStorageClassOnly(t *testing.T) {
	t.Parallel()

	metadataID := uuid.New()
	tenantID := uuid.New()
	createdAt := time.Now().UTC()

	scanner := &fakeScanner{scan: func(dest ...any) error {
		*dest[0].(*uuid.UUID) = metadataID
		*dest[1].(*uuid.UUID) = tenantID
		*dest[2].(*string) = "audit_logs_2026_01"
		*dest[3].(*time.Time) = time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
		*dest[4].(*time.Time) = time.Date(2026, 2, 1, 0, 0, 0, 0, time.UTC)
		*dest[5].(*int64) = 250
		*dest[6].(*sql.NullString) = sql.NullString{Valid: false}
		*dest[7].(*sql.NullString) = sql.NullString{Valid: false}
		*dest[8].(*sql.NullInt64) = sql.NullInt64{Int64: 8192, Valid: true}
		*dest[9].(*sql.NullString) = sql.NullString{String: "STANDARD", Valid: true}
		*dest[10].(*entities.ArchiveStatus) = entities.StatusUploaded
		*dest[11].(*sql.NullString) = sql.NullString{Valid: false}
		*dest[12].(*sql.NullTime) = sql.NullTime{Valid: false}
		*dest[13].(*time.Time) = createdAt
		*dest[14].(*time.Time) = createdAt

		return nil
	}}

	result, err := scanArchiveMetadata(scanner)
	require.NoError(t, err)
	require.NotNil(t, result)

	assert.Empty(t, result.ArchiveKey)
	assert.Empty(t, result.Checksum)
	assert.Equal(t, int64(8192), result.CompressedSizeBytes)
	assert.Equal(t, "STANDARD", result.StorageClass)
	assert.Empty(t, result.ErrorMessage)
	assert.Nil(t, result.ArchivedAt)
}

// --- scanArchiveMetadata invalid status validation ---

func TestScanArchiveMetadata_InvalidStatusFromDatabase(t *testing.T) {
	t.Parallel()

	metadataID := uuid.New()
	tenantID := uuid.New()
	createdAt := time.Now().UTC()

	scanner := &fakeScanner{scan: func(dest ...any) error {
		*dest[0].(*uuid.UUID) = metadataID
		*dest[1].(*uuid.UUID) = tenantID
		*dest[2].(*string) = "audit_logs_2026_01"
		*dest[3].(*time.Time) = time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
		*dest[4].(*time.Time) = time.Date(2026, 2, 1, 0, 0, 0, 0, time.UTC)
		*dest[5].(*int64) = 100
		*dest[6].(*sql.NullString) = sql.NullString{Valid: false}
		*dest[7].(*sql.NullString) = sql.NullString{Valid: false}
		*dest[8].(*sql.NullInt64) = sql.NullInt64{Valid: false}
		*dest[9].(*sql.NullString) = sql.NullString{Valid: false}
		*dest[10].(*entities.ArchiveStatus) = entities.ArchiveStatus("CORRUPTED")
		*dest[11].(*sql.NullString) = sql.NullString{Valid: false}
		*dest[12].(*sql.NullTime) = sql.NullTime{Valid: false}
		*dest[13].(*time.Time) = createdAt
		*dest[14].(*time.Time) = createdAt

		return nil
	}}

	result, err := scanArchiveMetadata(scanner)
	require.Error(t, err)
	require.Nil(t, result)
	require.ErrorIs(t, err, ErrInvalidArchiveStatus)
	require.Contains(t, err.Error(), "CORRUPTED")
}

func TestScanArchiveMetadata_EmptyStatusFromDatabase(t *testing.T) {
	t.Parallel()

	metadataID := uuid.New()
	tenantID := uuid.New()
	createdAt := time.Now().UTC()

	scanner := &fakeScanner{scan: func(dest ...any) error {
		*dest[0].(*uuid.UUID) = metadataID
		*dest[1].(*uuid.UUID) = tenantID
		*dest[2].(*string) = "audit_logs_2026_01"
		*dest[3].(*time.Time) = time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
		*dest[4].(*time.Time) = time.Date(2026, 2, 1, 0, 0, 0, 0, time.UTC)
		*dest[5].(*int64) = 0
		*dest[6].(*sql.NullString) = sql.NullString{Valid: false}
		*dest[7].(*sql.NullString) = sql.NullString{Valid: false}
		*dest[8].(*sql.NullInt64) = sql.NullInt64{Valid: false}
		*dest[9].(*sql.NullString) = sql.NullString{Valid: false}
		*dest[10].(*entities.ArchiveStatus) = entities.ArchiveStatus("")
		*dest[11].(*sql.NullString) = sql.NullString{Valid: false}
		*dest[12].(*sql.NullTime) = sql.NullTime{Valid: false}
		*dest[13].(*time.Time) = createdAt
		*dest[14].(*time.Time) = createdAt

		return nil
	}}

	result, err := scanArchiveMetadata(scanner)
	require.Error(t, err)
	require.Nil(t, result)
	require.ErrorIs(t, err, ErrInvalidArchiveStatus)
}

// --- scanArchiveMetadata edge case tests ---

func TestScanArchiveMetadata_ValidEmptyStrings(t *testing.T) {
	t.Parallel()

	metadataID := uuid.New()
	tenantID := uuid.New()
	createdAt := time.Now().UTC()

	scanner := &fakeScanner{scan: func(dest ...any) error {
		*dest[0].(*uuid.UUID) = metadataID
		*dest[1].(*uuid.UUID) = tenantID
		*dest[2].(*string) = "audit_logs_2026_01"
		*dest[3].(*time.Time) = time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
		*dest[4].(*time.Time) = time.Date(2026, 2, 1, 0, 0, 0, 0, time.UTC)
		*dest[5].(*int64) = 0
		// Valid=true with empty strings: exercises the Valid branches
		*dest[6].(*sql.NullString) = sql.NullString{String: "", Valid: true}
		*dest[7].(*sql.NullString) = sql.NullString{String: "", Valid: true}
		*dest[8].(*sql.NullInt64) = sql.NullInt64{Valid: false}
		*dest[9].(*sql.NullString) = sql.NullString{String: "", Valid: true}
		*dest[10].(*entities.ArchiveStatus) = entities.StatusPending
		*dest[11].(*sql.NullString) = sql.NullString{String: "", Valid: true}
		*dest[12].(*sql.NullTime) = sql.NullTime{Valid: false}
		*dest[13].(*time.Time) = createdAt
		*dest[14].(*time.Time) = createdAt

		return nil
	}}

	result, err := scanArchiveMetadata(scanner)
	require.NoError(t, err)
	require.NotNil(t, result)

	// All Valid=true with empty strings: the Valid branch is taken, setting the field to ""
	assert.Empty(t, result.ArchiveKey)
	assert.Empty(t, result.Checksum)
	assert.Empty(t, result.StorageClass)
	assert.Empty(t, result.ErrorMessage)
}

func TestScanArchiveMetadata_ValidZeroCompressedSize(t *testing.T) {
	t.Parallel()

	metadataID := uuid.New()
	tenantID := uuid.New()
	createdAt := time.Now().UTC()

	scanner := &fakeScanner{scan: func(dest ...any) error {
		*dest[0].(*uuid.UUID) = metadataID
		*dest[1].(*uuid.UUID) = tenantID
		*dest[2].(*string) = "audit_logs_2026_01"
		*dest[3].(*time.Time) = time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
		*dest[4].(*time.Time) = time.Date(2026, 2, 1, 0, 0, 0, 0, time.UTC)
		*dest[5].(*int64) = 0
		*dest[6].(*sql.NullString) = sql.NullString{Valid: false}
		*dest[7].(*sql.NullString) = sql.NullString{Valid: false}
		// Valid=true with Int64=0: exercises the compressedSizeBytes.Valid branch
		*dest[8].(*sql.NullInt64) = sql.NullInt64{Int64: 0, Valid: true}
		*dest[9].(*sql.NullString) = sql.NullString{Valid: false}
		*dest[10].(*entities.ArchiveStatus) = entities.StatusPending
		*dest[11].(*sql.NullString) = sql.NullString{Valid: false}
		*dest[12].(*sql.NullTime) = sql.NullTime{Valid: false}
		*dest[13].(*time.Time) = createdAt
		*dest[14].(*time.Time) = createdAt

		return nil
	}}

	result, err := scanArchiveMetadata(scanner)
	require.NoError(t, err)
	require.NotNil(t, result)

	// Valid=true with zero int64 sets the field to 0 via the Valid branch
	assert.Equal(t, int64(0), result.CompressedSizeBytes)
}

func TestScanArchiveMetadata_ArchivedAtPointerIsIndependent(t *testing.T) {
	t.Parallel()

	archivedAt := time.Date(2026, 1, 15, 12, 0, 0, 0, time.UTC)
	createdAt := time.Now().UTC()

	scanner := &fakeScanner{scan: func(dest ...any) error {
		*dest[0].(*uuid.UUID) = uuid.New()
		*dest[1].(*uuid.UUID) = uuid.New()
		*dest[2].(*string) = "audit_logs_2026_01"
		*dest[3].(*time.Time) = time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
		*dest[4].(*time.Time) = time.Date(2026, 2, 1, 0, 0, 0, 0, time.UTC)
		*dest[5].(*int64) = 500
		*dest[6].(*sql.NullString) = sql.NullString{Valid: false}
		*dest[7].(*sql.NullString) = sql.NullString{Valid: false}
		*dest[8].(*sql.NullInt64) = sql.NullInt64{Valid: false}
		*dest[9].(*sql.NullString) = sql.NullString{Valid: false}
		*dest[10].(*entities.ArchiveStatus) = entities.StatusComplete
		*dest[11].(*sql.NullString) = sql.NullString{Valid: false}
		*dest[12].(*sql.NullTime) = sql.NullTime{Time: archivedAt, Valid: true}
		*dest[13].(*time.Time) = createdAt
		*dest[14].(*time.Time) = createdAt

		return nil
	}}

	result, err := scanArchiveMetadata(scanner)
	require.NoError(t, err)
	require.NotNil(t, result)
	require.NotNil(t, result.ArchivedAt)

	// The pointer must hold the correct value and be independent of the NullTime local var
	assert.Equal(t, archivedAt, *result.ArchivedAt)
	assert.True(t, result.ArchivedAt.Equal(archivedAt))
}

func TestScanArchiveMetadata_ScannerErrorPropagation(t *testing.T) {
	t.Parallel()

	scanErr := errors.New("column type mismatch")
	scanner := &fakeScanner{scan: func(_ ...any) error {
		return scanErr
	}}

	result, err := scanArchiveMetadata(scanner)
	require.Error(t, err)
	require.Nil(t, result)
	require.ErrorIs(t, err, scanErr)
	require.Contains(t, err.Error(), "scanning archive metadata")
}

func TestScanArchiveMetadata_MandatoryFieldsPopulated(t *testing.T) {
	t.Parallel()

	metadataID := uuid.New()
	tenantID := uuid.New()
	partitionName := "audit_logs_2026_03"
	rangeStart := time.Date(2026, 3, 1, 0, 0, 0, 0, time.UTC)
	rangeEnd := time.Date(2026, 4, 1, 0, 0, 0, 0, time.UTC)
	rowCount := int64(42)
	status := entities.StatusExporting
	createdAt := time.Date(2026, 3, 1, 10, 0, 0, 0, time.UTC)
	updatedAt := time.Date(2026, 3, 1, 11, 0, 0, 0, time.UTC)

	scanner := &fakeScanner{scan: func(dest ...any) error {
		*dest[0].(*uuid.UUID) = metadataID
		*dest[1].(*uuid.UUID) = tenantID
		*dest[2].(*string) = partitionName
		*dest[3].(*time.Time) = rangeStart
		*dest[4].(*time.Time) = rangeEnd
		*dest[5].(*int64) = rowCount
		*dest[6].(*sql.NullString) = sql.NullString{Valid: false}
		*dest[7].(*sql.NullString) = sql.NullString{Valid: false}
		*dest[8].(*sql.NullInt64) = sql.NullInt64{Valid: false}
		*dest[9].(*sql.NullString) = sql.NullString{Valid: false}
		*dest[10].(*entities.ArchiveStatus) = status
		*dest[11].(*sql.NullString) = sql.NullString{Valid: false}
		*dest[12].(*sql.NullTime) = sql.NullTime{Valid: false}
		*dest[13].(*time.Time) = createdAt
		*dest[14].(*time.Time) = updatedAt

		return nil
	}}

	result, err := scanArchiveMetadata(scanner)
	require.NoError(t, err)
	require.NotNil(t, result)

	// Verify all mandatory (non-nullable) fields are correctly mapped
	assert.Equal(t, metadataID, result.ID)
	assert.Equal(t, tenantID, result.TenantID)
	assert.Equal(t, partitionName, result.PartitionName)
	assert.Equal(t, rangeStart, result.DateRangeStart)
	assert.Equal(t, rangeEnd, result.DateRangeEnd)
	assert.Equal(t, rowCount, result.RowCount)
	assert.Equal(t, status, result.Status)
	assert.Equal(t, createdAt, result.CreatedAt)
	assert.Equal(t, updatedAt, result.UpdatedAt)
}

// --- scanArchiveMetadataRows direct tests ---
// The sibling file tests this indirectly through repository methods.
// These test the function directly, including the rows.Err() path.

func TestScanArchiveMetadataRows_MultipleRows(t *testing.T) {
	t.Parallel()

	db, mock, err := sqlmock.New()
	require.NoError(t, err)

	defer func() {
		mock.ExpectClose()
		require.NoError(t, db.Close())
	}()

	ctx := context.Background()
	id1, id2 := uuid.New(), uuid.New()
	tenantID := uuid.New()
	createdAt := time.Now().UTC()
	archivedAt := time.Now().UTC()

	rows := sqlmock.NewRows(selectColumns).
		AddRow(
			id1, tenantID, "audit_logs_2026_01",
			time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC),
			time.Date(2026, 2, 1, 0, 0, 0, 0, time.UTC),
			int64(100), nil, nil, nil, nil, entities.StatusPending,
			nil, nil, createdAt, createdAt,
		).
		AddRow(
			id2, tenantID, "audit_logs_2026_02",
			time.Date(2026, 2, 1, 0, 0, 0, 0, time.UTC),
			time.Date(2026, 3, 1, 0, 0, 0, 0, time.UTC),
			int64(500), "archives/key2.jsonl.gz", "sha256:def456", int64(4096), "GLACIER",
			entities.StatusComplete,
			nil, archivedAt, createdAt, createdAt,
		)

	mock.ExpectQuery("SELECT .+").WillReturnRows(rows)

	results, err := scanArchiveMetadataRows(ctx, db, "SELECT * FROM archive_metadata", nil)
	require.NoError(t, err)
	require.Len(t, results, 2)

	// First row: all nullable fields null
	assert.Equal(t, id1, results[0].ID)
	assert.Equal(t, "audit_logs_2026_01", results[0].PartitionName)
	assert.Equal(t, int64(100), results[0].RowCount)
	assert.Empty(t, results[0].ArchiveKey)
	assert.Nil(t, results[0].ArchivedAt)

	// Second row: nullable fields populated
	assert.Equal(t, id2, results[1].ID)
	assert.Equal(t, "audit_logs_2026_02", results[1].PartitionName)
	assert.Equal(t, int64(500), results[1].RowCount)
	assert.Equal(t, "archives/key2.jsonl.gz", results[1].ArchiveKey)
	assert.Equal(t, "sha256:def456", results[1].Checksum)
	assert.Equal(t, int64(4096), results[1].CompressedSizeBytes)
	assert.Equal(t, "GLACIER", results[1].StorageClass)
	assert.NotNil(t, results[1].ArchivedAt)

	require.NoError(t, mock.ExpectationsWereMet())
}

func TestScanArchiveMetadataRows_EmptyResult(t *testing.T) {
	t.Parallel()

	db, mock, err := sqlmock.New()
	require.NoError(t, err)

	defer func() {
		mock.ExpectClose()
		require.NoError(t, db.Close())
	}()

	ctx := context.Background()
	rows := sqlmock.NewRows(selectColumns)

	mock.ExpectQuery("SELECT .+").WillReturnRows(rows)

	results, err := scanArchiveMetadataRows(ctx, db, "SELECT * FROM archive_metadata WHERE 1=0", nil)
	require.NoError(t, err)
	require.Empty(t, results)

	require.NoError(t, mock.ExpectationsWereMet())
}

func TestScanArchiveMetadataRows_QueryError(t *testing.T) {
	t.Parallel()

	db, mock, err := sqlmock.New()
	require.NoError(t, err)

	defer func() {
		mock.ExpectClose()
		require.NoError(t, db.Close())
	}()

	ctx := context.Background()
	queryErr := errors.New("connection refused")

	mock.ExpectQuery("SELECT .+").WillReturnError(queryErr)

	results, err := scanArchiveMetadataRows(ctx, db, "SELECT * FROM archive_metadata", nil)
	require.Error(t, err)
	require.Nil(t, results)
	require.Contains(t, err.Error(), "querying archive metadata")
	require.ErrorIs(t, err, queryErr)

	require.NoError(t, mock.ExpectationsWereMet())
}

func TestScanArchiveMetadataRows_RowIterationError(t *testing.T) {
	t.Parallel()

	db, mock, err := sqlmock.New()
	require.NoError(t, err)

	defer func() {
		mock.ExpectClose()
		require.NoError(t, db.Close())
	}()

	ctx := context.Background()
	tenantID := uuid.New()
	createdAt := time.Now().UTC()
	iterErr := errors.New("connection lost during iteration")

	// Two rows added, but RowError on index 1 causes rows.Next() to return false
	// after the first row is scanned. rows.Err() then returns the error.
	rows := sqlmock.NewRows(selectColumns).
		AddRow(
			uuid.New(), tenantID, "audit_logs_2026_01",
			time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC),
			time.Date(2026, 2, 1, 0, 0, 0, 0, time.UTC),
			int64(100), nil, nil, nil, nil, entities.StatusPending,
			nil, nil, createdAt, createdAt,
		).
		AddRow(
			uuid.New(), tenantID, "audit_logs_2026_02",
			time.Date(2026, 2, 1, 0, 0, 0, 0, time.UTC),
			time.Date(2026, 3, 1, 0, 0, 0, 0, time.UTC),
			int64(200), nil, nil, nil, nil, entities.StatusPending,
			nil, nil, createdAt, createdAt,
		).
		RowError(1, iterErr)

	mock.ExpectQuery("SELECT .+").WillReturnRows(rows)

	results, err := scanArchiveMetadataRows(ctx, db, "SELECT * FROM archive_metadata", nil)
	require.Error(t, err)
	require.Nil(t, results)
	require.Contains(t, err.Error(), "iterating archive metadata")
	require.ErrorIs(t, err, iterErr)

	require.NoError(t, mock.ExpectationsWereMet())
}

func TestScanArchiveMetadataRows_WithQueryArgs(t *testing.T) {
	t.Parallel()

	db, mock, err := sqlmock.New()
	require.NoError(t, err)

	defer func() {
		mock.ExpectClose()
		require.NoError(t, db.Close())
	}()

	ctx := context.Background()
	tenantID := uuid.New()
	createdAt := time.Now().UTC()

	rows := sqlmock.NewRows(selectColumns).
		AddRow(
			uuid.New(), tenantID, "audit_logs_2026_01",
			time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC),
			time.Date(2026, 2, 1, 0, 0, 0, 0, time.UTC),
			int64(100), nil, nil, nil, nil, entities.StatusPending,
			nil, nil, createdAt, createdAt,
		)

	mock.ExpectQuery("SELECT .+ WHERE status = \\$1").
		WithArgs(entities.StatusPending).
		WillReturnRows(rows)

	args := []any{entities.StatusPending}
	results, err := scanArchiveMetadataRows(
		ctx, db,
		"SELECT * FROM archive_metadata WHERE status = $1",
		args,
	)
	require.NoError(t, err)
	require.Len(t, results, 1)
	assert.Equal(t, entities.StatusPending, results[0].Status)

	require.NoError(t, mock.ExpectationsWereMet())
}

func TestScanArchiveMetadataRows_SingleRow(t *testing.T) {
	t.Parallel()

	db, mock, err := sqlmock.New()
	require.NoError(t, err)

	defer func() {
		mock.ExpectClose()
		require.NoError(t, db.Close())
	}()

	ctx := context.Background()
	metadataID := uuid.New()
	tenantID := uuid.New()
	createdAt := time.Now().UTC()

	rows := sqlmock.NewRows(selectColumns).
		AddRow(
			metadataID, tenantID, "audit_logs_2026_03",
			time.Date(2026, 3, 1, 0, 0, 0, 0, time.UTC),
			time.Date(2026, 4, 1, 0, 0, 0, 0, time.UTC),
			int64(42), "key", "checksum", int64(1024), "GLACIER",
			entities.StatusComplete,
			nil, createdAt, createdAt, createdAt,
		)

	mock.ExpectQuery("SELECT .+").WillReturnRows(rows)

	results, err := scanArchiveMetadataRows(ctx, db, "SELECT * FROM archive_metadata LIMIT 1", nil)
	require.NoError(t, err)
	require.Len(t, results, 1)
	assert.Equal(t, metadataID, results[0].ID)
	assert.Equal(t, tenantID, results[0].TenantID)
	assert.Equal(t, "audit_logs_2026_03", results[0].PartitionName)
	assert.Equal(t, int64(42), results[0].RowCount)
	assert.Equal(t, "key", results[0].ArchiveKey)
	assert.Equal(t, "checksum", results[0].Checksum)
	assert.Equal(t, int64(1024), results[0].CompressedSizeBytes)
	assert.Equal(t, "GLACIER", results[0].StorageClass)
	assert.Equal(t, entities.StatusComplete, results[0].Status)

	require.NoError(t, mock.ExpectationsWereMet())
}
