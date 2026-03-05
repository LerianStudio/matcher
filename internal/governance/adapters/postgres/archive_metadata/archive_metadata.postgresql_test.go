//go:build unit

package archivemetadata

import (
	"context"
	"database/sql"
	"errors"
	"regexp"
	"testing"
	"time"

	"github.com/DATA-DOG/go-sqlmock"
	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/LerianStudio/matcher/internal/auth"
	"github.com/LerianStudio/matcher/internal/governance/domain/entities"
	"github.com/LerianStudio/matcher/internal/shared/infrastructure/testutil"
)

var errTestDatabaseError = errors.New("database error")

var archiveMetadataTestColumns = []string{
	"id", "tenant_id", "partition_name", "date_range_start", "date_range_end",
	"row_count", "archive_key", "checksum", "compressed_size_bytes", "storage_class",
	"status", "error_message", "archived_at", "created_at", "updated_at",
}

// contextWithTenant creates a context with the default tenant ID for testing.
func contextWithTenant() context.Context {
	return context.WithValue(context.Background(), auth.TenantIDKey, auth.DefaultTenantID)
}

// defaultTenantUUID returns the default tenant ID as a UUID for test data.
func defaultTenantUUID() uuid.UUID {
	return uuid.MustParse(auth.DefaultTenantID)
}

// setupMockRepository creates a repository with sqlmock for testing database interactions.
func setupMockRepository(t *testing.T) (*Repository, sqlmock.Sqlmock, func()) {
	t.Helper()

	db, mock, err := sqlmock.New()
	require.NoError(t, err)

	provider := testutil.NewMockProviderFromDB(t, db)
	repo := NewRepository(provider)

	finish := func() {
		mock.ExpectClose()
		require.NoError(t, db.Close())
		require.NoError(t, mock.ExpectationsWereMet())
	}

	return repo, mock, finish
}

func TestRepository_NilProvider(t *testing.T) {
	t.Parallel()

	repo := NewRepository(nil)
	ctx := context.Background()

	err := repo.Create(ctx, &entities.ArchiveMetadata{})
	require.ErrorIs(t, err, ErrRepositoryNotInitialized)

	err = repo.Update(ctx, &entities.ArchiveMetadata{})
	require.ErrorIs(t, err, ErrRepositoryNotInitialized)

	_, err = repo.GetByID(ctx, uuid.New())
	require.ErrorIs(t, err, ErrRepositoryNotInitialized)

	_, err = repo.GetByPartition(ctx, uuid.New(), "partition")
	require.ErrorIs(t, err, ErrRepositoryNotInitialized)

	_, err = repo.ListByTenant(ctx, uuid.New(), "", nil, nil, 10, 0)
	require.ErrorIs(t, err, ErrRepositoryNotInitialized)

	_, err = repo.ListPending(ctx)
	require.ErrorIs(t, err, ErrRepositoryNotInitialized)

	_, err = repo.ListIncomplete(ctx)
	require.ErrorIs(t, err, ErrRepositoryNotInitialized)
}

func TestRepository_CreateValidation(t *testing.T) {
	t.Parallel()

	repo := &Repository{provider: nil}
	ctx := context.Background()

	err := repo.Create(ctx, nil)
	require.ErrorIs(t, err, ErrRepositoryNotInitialized)
}

func TestRepository_UpdateValidation(t *testing.T) {
	t.Parallel()

	repo := &Repository{provider: nil}
	ctx := context.Background()

	err := repo.Update(ctx, nil)
	require.ErrorIs(t, err, ErrRepositoryNotInitialized)
}

func TestRepository_GetByIDValidation(t *testing.T) {
	t.Parallel()

	provider := &testutil.MockInfrastructureProvider{}
	repo := NewRepository(provider)
	ctx := context.Background()

	_, err := repo.GetByID(ctx, uuid.Nil)
	require.ErrorIs(t, err, ErrIDRequired)
}

func TestRepository_GetByPartitionValidation(t *testing.T) {
	t.Parallel()

	provider := &testutil.MockInfrastructureProvider{}
	repo := NewRepository(provider)
	ctx := context.Background()

	_, err := repo.GetByPartition(ctx, uuid.Nil, "partition")
	require.ErrorIs(t, err, ErrTenantIDRequired)

	_, err = repo.GetByPartition(ctx, uuid.New(), "")
	require.ErrorIs(t, err, ErrPartitionNameRequired)
}

func TestRepository_ListByTenantValidation(t *testing.T) {
	t.Parallel()

	provider := &testutil.MockInfrastructureProvider{}
	repo := NewRepository(provider)
	ctx := context.Background()

	_, err := repo.ListByTenant(ctx, uuid.Nil, "", nil, nil, 10, 0)
	require.ErrorIs(t, err, ErrTenantIDRequired)

	_, err = repo.ListByTenant(ctx, uuid.New(), "", nil, nil, 0, 0)
	require.ErrorIs(t, err, ErrLimitMustBePositive)

	_, err = repo.ListByTenant(ctx, uuid.New(), "", nil, nil, -1, 0)
	require.ErrorIs(t, err, ErrLimitMustBePositive)
}

func TestCreate_Success(t *testing.T) {
	t.Parallel()

	repo, mock, finish := setupMockRepository(t)
	defer finish()

	ctx := contextWithTenant()

	metadata := &entities.ArchiveMetadata{
		ID:             uuid.New(),
		TenantID:       defaultTenantUUID(),
		PartitionName:  "audit_logs_2026_01",
		DateRangeStart: time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC),
		DateRangeEnd:   time.Date(2026, 2, 1, 0, 0, 0, 0, time.UTC),
		Status:         entities.StatusPending,
		CreatedAt:      time.Now().UTC(),
		UpdatedAt:      time.Now().UTC(),
	}

	mock.ExpectBegin()
	mock.ExpectExec(regexp.QuoteMeta(`INSERT INTO archive_metadata`)).
		WithArgs(
			metadata.ID, metadata.TenantID, metadata.PartitionName,
			metadata.DateRangeStart, metadata.DateRangeEnd,
			metadata.RowCount, metadata.ArchiveKey, metadata.Checksum,
			metadata.CompressedSizeBytes, metadata.StorageClass,
			metadata.Status, metadata.ErrorMessage, metadata.ArchivedAt,
			metadata.CreatedAt, metadata.UpdatedAt,
		).
		WillReturnResult(sqlmock.NewResult(1, 1))
	mock.ExpectCommit()

	err := repo.Create(ctx, metadata)
	require.NoError(t, err)
}

func TestCreate_Error(t *testing.T) {
	t.Parallel()

	repo, mock, finish := setupMockRepository(t)
	defer finish()

	ctx := contextWithTenant()

	metadata := &entities.ArchiveMetadata{
		ID:             uuid.New(),
		TenantID:       defaultTenantUUID(),
		PartitionName:  "audit_logs_2026_01",
		DateRangeStart: time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC),
		DateRangeEnd:   time.Date(2026, 2, 1, 0, 0, 0, 0, time.UTC),
		Status:         entities.StatusPending,
		CreatedAt:      time.Now().UTC(),
		UpdatedAt:      time.Now().UTC(),
	}

	mock.ExpectBegin()
	mock.ExpectExec(regexp.QuoteMeta(`INSERT INTO archive_metadata`)).
		WillReturnError(errTestDatabaseError)
	mock.ExpectRollback()

	err := repo.Create(ctx, metadata)
	require.Error(t, err)
	require.Contains(t, err.Error(), "create archive metadata")
}

func TestCreate_NilMetadata(t *testing.T) {
	t.Parallel()

	provider := &testutil.MockInfrastructureProvider{}
	repo := NewRepository(provider)
	ctx := context.Background()

	err := repo.Create(ctx, nil)
	require.ErrorIs(t, err, ErrMetadataRequired)
}

func TestUpdate_Success(t *testing.T) {
	t.Parallel()

	repo, mock, finish := setupMockRepository(t)
	defer finish()

	ctx := contextWithTenant()

	metadata := &entities.ArchiveMetadata{
		ID:                  uuid.New(),
		TenantID:            defaultTenantUUID(),
		PartitionName:       "audit_logs_2026_01",
		DateRangeStart:      time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC),
		DateRangeEnd:        time.Date(2026, 2, 1, 0, 0, 0, 0, time.UTC),
		RowCount:            500,
		ArchiveKey:          "archives/tenant/2026/01/audit_logs.jsonl.gz",
		Checksum:            "sha256:abc123",
		CompressedSizeBytes: 2048,
		StorageClass:        "GLACIER",
		Status:              entities.StatusUploaded,
		UpdatedAt:           time.Now().UTC(),
	}

	mock.ExpectBegin()
	// squirrel sorts map keys alphabetically: "id" before "tenant_id"
	mock.ExpectExec(regexp.QuoteMeta(`UPDATE archive_metadata SET`)).
		WithArgs(
			metadata.RowCount, metadata.ArchiveKey, metadata.Checksum,
			metadata.CompressedSizeBytes, metadata.StorageClass,
			metadata.Status, metadata.ErrorMessage, metadata.ArchivedAt,
			metadata.UpdatedAt, metadata.ID, metadata.TenantID,
		).
		WillReturnResult(sqlmock.NewResult(0, 1))
	mock.ExpectCommit()

	err := repo.Update(ctx, metadata)
	require.NoError(t, err)
}

func TestUpdate_NotFound(t *testing.T) {
	t.Parallel()

	repo, mock, finish := setupMockRepository(t)
	defer finish()

	ctx := contextWithTenant()

	metadata := &entities.ArchiveMetadata{
		ID:        uuid.New(),
		Status:    entities.StatusPending,
		UpdatedAt: time.Now().UTC(),
	}

	mock.ExpectBegin()
	mock.ExpectExec(regexp.QuoteMeta(`UPDATE archive_metadata SET`)).
		WillReturnResult(sqlmock.NewResult(0, 0))
	mock.ExpectRollback()

	err := repo.Update(ctx, metadata)
	require.Error(t, err)
	require.ErrorIs(t, err, ErrMetadataNotFound)
}

func TestUpdate_NilMetadata(t *testing.T) {
	t.Parallel()

	provider := &testutil.MockInfrastructureProvider{}
	repo := NewRepository(provider)
	ctx := context.Background()

	err := repo.Update(ctx, nil)
	require.ErrorIs(t, err, ErrMetadataRequired)
}

func TestGetByID_Success(t *testing.T) {
	t.Parallel()

	repo, mock, finish := setupMockRepository(t)
	defer finish()

	ctx := contextWithTenant()

	metadataID := uuid.New()
	tenantID := defaultTenantUUID()
	createdAt := time.Now().UTC()
	archivedAt := time.Now().UTC()

	rows := sqlmock.NewRows(archiveMetadataTestColumns).
		AddRow(
			metadataID, tenantID, "audit_logs_2026_01",
			time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC),
			time.Date(2026, 2, 1, 0, 0, 0, 0, time.UTC),
			int64(500), "archives/key.jsonl.gz", "sha256:abc",
			int64(2048), "GLACIER", entities.StatusComplete,
			nil, archivedAt, createdAt, createdAt,
		)

	// squirrel sorts map keys alphabetically: "id" before "tenant_id"
	mock.ExpectQuery(regexp.QuoteMeta(`SELECT`)).
		WithArgs(metadataID, tenantID).
		WillReturnRows(rows)

	result, err := repo.GetByID(ctx, metadataID)
	require.NoError(t, err)
	require.NotNil(t, result)
	assert.Equal(t, metadataID, result.ID)
	assert.Equal(t, tenantID, result.TenantID)
	assert.Equal(t, "audit_logs_2026_01", result.PartitionName)
	assert.Equal(t, int64(500), result.RowCount)
	assert.Equal(t, "archives/key.jsonl.gz", result.ArchiveKey)
	assert.Equal(t, "sha256:abc", result.Checksum)
	assert.Equal(t, int64(2048), result.CompressedSizeBytes)
	assert.Equal(t, "GLACIER", result.StorageClass)
	assert.Equal(t, entities.StatusComplete, result.Status)
	assert.NotNil(t, result.ArchivedAt)
}

func TestGetByID_NotFound(t *testing.T) {
	t.Parallel()

	repo, mock, finish := setupMockRepository(t)
	defer finish()

	ctx := contextWithTenant()
	tenantID := defaultTenantUUID()
	missingID := uuid.New()

	// squirrel sorts map keys alphabetically: "id" before "tenant_id"
	mock.ExpectQuery(regexp.QuoteMeta(`SELECT`)).
		WithArgs(missingID, tenantID).
		WillReturnError(sql.ErrNoRows)

	_, err := repo.GetByID(ctx, missingID)
	require.ErrorIs(t, err, ErrMetadataNotFound)
}

func TestGetByID_InvalidTenantID(t *testing.T) {
	t.Parallel()

	provider := &testutil.MockInfrastructureProvider{}
	repo := NewRepository(provider)
	// Context with invalid (non-UUID) tenant ID
	ctx := context.WithValue(context.Background(), auth.TenantIDKey, "not-a-valid-uuid")

	_, err := repo.GetByID(ctx, uuid.New())
	require.ErrorIs(t, err, ErrTenantIDRequired)
}

func TestGetByPartition_Success(t *testing.T) {
	t.Parallel()

	repo, mock, finish := setupMockRepository(t)
	defer finish()

	ctx := contextWithTenant()

	metadataID := uuid.New()
	tenantID := defaultTenantUUID()
	partitionName := "audit_logs_2026_01"
	createdAt := time.Now().UTC()

	rows := sqlmock.NewRows(archiveMetadataTestColumns).
		AddRow(
			metadataID, tenantID, partitionName,
			time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC),
			time.Date(2026, 2, 1, 0, 0, 0, 0, time.UTC),
			int64(0), nil, nil, nil, nil, entities.StatusPending,
			nil, nil, createdAt, createdAt,
		)

	// squirrel generates map keys in alphabetical order: partition_name before tenant_id
	mock.ExpectQuery(regexp.QuoteMeta(`SELECT`)).
		WithArgs(partitionName, tenantID).
		WillReturnRows(rows)

	result, err := repo.GetByPartition(ctx, tenantID, partitionName)
	require.NoError(t, err)
	require.NotNil(t, result)
	assert.Equal(t, metadataID, result.ID)
	assert.Equal(t, partitionName, result.PartitionName)
	assert.Equal(t, entities.StatusPending, result.Status)
}

func TestGetByPartition_NotFound(t *testing.T) {
	t.Parallel()

	repo, mock, finish := setupMockRepository(t)
	defer finish()

	ctx := contextWithTenant()
	tenantID := defaultTenantUUID()

	// squirrel generates map keys in alphabetical order: partition_name before tenant_id
	mock.ExpectQuery(regexp.QuoteMeta(`SELECT`)).
		WithArgs("nonexistent", tenantID).
		WillReturnError(sql.ErrNoRows)

	_, err := repo.GetByPartition(ctx, tenantID, "nonexistent")
	require.ErrorIs(t, err, ErrMetadataNotFound)
}

func TestListByTenant_Success(t *testing.T) {
	t.Parallel()

	repo, mock, finish := setupMockRepository(t)
	defer finish()

	ctx := contextWithTenant()
	tenantID := defaultTenantUUID()
	createdAt := time.Now().UTC()

	rows := sqlmock.NewRows(archiveMetadataTestColumns).
		AddRow(
			uuid.New(), tenantID, "audit_logs_2026_02",
			time.Date(2026, 2, 1, 0, 0, 0, 0, time.UTC),
			time.Date(2026, 3, 1, 0, 0, 0, 0, time.UTC),
			int64(200), nil, nil, nil, nil, entities.StatusPending,
			nil, nil, createdAt, createdAt,
		).
		AddRow(
			uuid.New(), tenantID, "audit_logs_2026_01",
			time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC),
			time.Date(2026, 2, 1, 0, 0, 0, 0, time.UTC),
			int64(500), "key", "checksum", int64(1024), "GLACIER", entities.StatusComplete,
			nil, createdAt, createdAt, createdAt,
		)

	mock.ExpectQuery(`SELECT .+ FROM archive_metadata WHERE tenant_id = \$1 ORDER BY`).
		WithArgs(tenantID).
		WillReturnRows(rows)

	results, err := repo.ListByTenant(ctx, tenantID, "", nil, nil, 10, 0)
	require.NoError(t, err)
	require.Len(t, results, 2)
	assert.Equal(t, "audit_logs_2026_02", results[0].PartitionName)
	assert.Equal(t, "audit_logs_2026_01", results[1].PartitionName)
}

func TestListByTenant_WithStatusFilter(t *testing.T) {
	t.Parallel()

	repo, mock, finish := setupMockRepository(t)
	defer finish()

	ctx := contextWithTenant()
	tenantID := defaultTenantUUID()
	createdAt := time.Now().UTC()

	rows := sqlmock.NewRows(archiveMetadataTestColumns).
		AddRow(
			uuid.New(), tenantID, "audit_logs_2026_01",
			time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC),
			time.Date(2026, 2, 1, 0, 0, 0, 0, time.UTC),
			int64(500), "key", "checksum", int64(1024), "GLACIER", entities.StatusComplete,
			nil, createdAt, createdAt, createdAt,
		)

	mock.ExpectQuery(`SELECT .+ FROM archive_metadata WHERE tenant_id = \$1 AND status = \$2 ORDER BY`).
		WithArgs(tenantID, entities.StatusComplete).
		WillReturnRows(rows)

	results, err := repo.ListByTenant(ctx, tenantID, entities.StatusComplete, nil, nil, 10, 0)
	require.NoError(t, err)
	require.Len(t, results, 1)
	assert.Equal(t, entities.StatusComplete, results[0].Status)
}

func TestListByTenant_QueryError(t *testing.T) {
	t.Parallel()

	repo, mock, finish := setupMockRepository(t)
	defer finish()

	ctx := contextWithTenant()
	tenantID := defaultTenantUUID()

	mock.ExpectQuery(`SELECT .+ FROM archive_metadata`).
		WithArgs(tenantID).
		WillReturnError(errTestDatabaseError)

	_, err := repo.ListByTenant(ctx, tenantID, "", nil, nil, 10, 0)
	require.Error(t, err)
	require.Contains(t, err.Error(), "list archive metadata by tenant")
}

func TestListPending_Success(t *testing.T) {
	t.Parallel()

	repo, mock, finish := setupMockRepository(t)
	defer finish()

	ctx := contextWithTenant()
	tenantID := defaultTenantUUID()
	createdAt := time.Now().UTC()

	rows := sqlmock.NewRows(archiveMetadataTestColumns).
		AddRow(
			uuid.New(), tenantID, "audit_logs_2025_11",
			time.Date(2025, 11, 1, 0, 0, 0, 0, time.UTC),
			time.Date(2025, 12, 1, 0, 0, 0, 0, time.UTC),
			int64(0), nil, nil, nil, nil, entities.StatusPending,
			nil, nil, createdAt, createdAt,
		)

	mock.ExpectQuery(`SELECT .+ FROM archive_metadata WHERE status = \$1 ORDER BY`).
		WithArgs(entities.StatusPending).
		WillReturnRows(rows)

	results, err := repo.ListPending(ctx)
	require.NoError(t, err)
	require.Len(t, results, 1)
	assert.Equal(t, entities.StatusPending, results[0].Status)
}

func TestListPending_Empty(t *testing.T) {
	t.Parallel()

	repo, mock, finish := setupMockRepository(t)
	defer finish()

	ctx := contextWithTenant()

	rows := sqlmock.NewRows(archiveMetadataTestColumns)

	mock.ExpectQuery(`SELECT .+ FROM archive_metadata WHERE status = \$1 ORDER BY`).
		WithArgs(entities.StatusPending).
		WillReturnRows(rows)

	results, err := repo.ListPending(ctx)
	require.NoError(t, err)
	require.Empty(t, results)
}

func TestListPending_QueryError(t *testing.T) {
	t.Parallel()

	repo, mock, finish := setupMockRepository(t)
	defer finish()

	ctx := contextWithTenant()

	mock.ExpectQuery(`SELECT .+ FROM archive_metadata WHERE status = \$1`).
		WithArgs(entities.StatusPending).
		WillReturnError(errTestDatabaseError)

	_, err := repo.ListPending(ctx)
	require.Error(t, err)
	require.Contains(t, err.Error(), "list pending archive metadata")
}

func TestListIncomplete_Success(t *testing.T) {
	t.Parallel()

	repo, mock, finish := setupMockRepository(t)
	defer finish()

	ctx := contextWithTenant()
	tenantID := defaultTenantUUID()
	createdAt := time.Now().UTC()

	rows := sqlmock.NewRows(archiveMetadataTestColumns).
		AddRow(
			uuid.New(), tenantID, "audit_logs_2025_10",
			time.Date(2025, 10, 1, 0, 0, 0, 0, time.UTC),
			time.Date(2025, 11, 1, 0, 0, 0, 0, time.UTC),
			int64(0), nil, nil, nil, nil, entities.StatusPending,
			nil, nil, createdAt, createdAt,
		).
		AddRow(
			uuid.New(), tenantID, "audit_logs_2025_11",
			time.Date(2025, 11, 1, 0, 0, 0, 0, time.UTC),
			time.Date(2025, 12, 1, 0, 0, 0, 0, time.UTC),
			int64(100), "key", "checksum", int64(512), "GLACIER", entities.StatusUploading,
			"previous error", nil, createdAt, createdAt,
		)

	mock.ExpectQuery(`SELECT .+ FROM archive_metadata WHERE status <> \$1 ORDER BY`).
		WithArgs(entities.StatusComplete).
		WillReturnRows(rows)

	results, err := repo.ListIncomplete(ctx)
	require.NoError(t, err)
	require.Len(t, results, 2)
	assert.Equal(t, entities.StatusPending, results[0].Status)
	assert.Equal(t, entities.StatusUploading, results[1].Status)
	assert.Equal(t, "previous error", results[1].ErrorMessage)
}

func TestListIncomplete_Empty(t *testing.T) {
	t.Parallel()

	repo, mock, finish := setupMockRepository(t)
	defer finish()

	ctx := contextWithTenant()

	rows := sqlmock.NewRows(archiveMetadataTestColumns)

	mock.ExpectQuery(`SELECT .+ FROM archive_metadata WHERE status <> \$1 ORDER BY`).
		WithArgs(entities.StatusComplete).
		WillReturnRows(rows)

	results, err := repo.ListIncomplete(ctx)
	require.NoError(t, err)
	require.Empty(t, results)
}

func TestListIncomplete_QueryError(t *testing.T) {
	t.Parallel()

	repo, mock, finish := setupMockRepository(t)
	defer finish()

	ctx := contextWithTenant()

	mock.ExpectQuery(`SELECT .+ FROM archive_metadata WHERE status <> \$1`).
		WithArgs(entities.StatusComplete).
		WillReturnError(errTestDatabaseError)

	_, err := repo.ListIncomplete(ctx)
	require.Error(t, err)
	require.Contains(t, err.Error(), "list incomplete archive metadata")
}

func TestScanArchiveMetadata_NilScanner(t *testing.T) {
	t.Parallel()

	_, err := scanArchiveMetadata(nil)
	require.Error(t, err)
	require.ErrorIs(t, err, ErrNilScanner)
}

func TestScanArchiveMetadata_NullableFields(t *testing.T) {
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
	assert.Equal(t, metadataID, result.ID)
	assert.Equal(t, tenantID, result.TenantID)
	assert.Empty(t, result.ArchiveKey)
	assert.Empty(t, result.Checksum)
	assert.Zero(t, result.CompressedSizeBytes)
	assert.Empty(t, result.StorageClass)
	assert.Empty(t, result.ErrorMessage)
	assert.Nil(t, result.ArchivedAt)
}

func TestScanArchiveMetadata_AllFieldsPopulated(t *testing.T) {
	t.Parallel()

	metadataID := uuid.New()
	tenantID := uuid.New()
	createdAt := time.Now().UTC()
	archivedAt := time.Now().UTC()

	scanner := &fakeScanner{scan: func(dest ...any) error {
		*dest[0].(*uuid.UUID) = metadataID
		*dest[1].(*uuid.UUID) = tenantID
		*dest[2].(*string) = "audit_logs_2026_01"
		*dest[3].(*time.Time) = time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
		*dest[4].(*time.Time) = time.Date(2026, 2, 1, 0, 0, 0, 0, time.UTC)
		*dest[5].(*int64) = 500
		*dest[6].(*sql.NullString) = sql.NullString{String: "archives/key.jsonl.gz", Valid: true}
		*dest[7].(*sql.NullString) = sql.NullString{String: "sha256:abc", Valid: true}
		*dest[8].(*sql.NullInt64) = sql.NullInt64{Int64: 2048, Valid: true}
		*dest[9].(*sql.NullString) = sql.NullString{String: "GLACIER", Valid: true}
		*dest[10].(*entities.ArchiveStatus) = entities.StatusComplete
		*dest[11].(*sql.NullString) = sql.NullString{String: "previous error", Valid: true}
		*dest[12].(*sql.NullTime) = sql.NullTime{Time: archivedAt, Valid: true}
		*dest[13].(*time.Time) = createdAt
		*dest[14].(*time.Time) = createdAt

		return nil
	}}

	result, err := scanArchiveMetadata(scanner)
	require.NoError(t, err)
	require.NotNil(t, result)
	assert.Equal(t, metadataID, result.ID)
	assert.Equal(t, int64(500), result.RowCount)
	assert.Equal(t, "archives/key.jsonl.gz", result.ArchiveKey)
	assert.Equal(t, "sha256:abc", result.Checksum)
	assert.Equal(t, int64(2048), result.CompressedSizeBytes)
	assert.Equal(t, "GLACIER", result.StorageClass)
	assert.Equal(t, entities.StatusComplete, result.Status)
	assert.Equal(t, "previous error", result.ErrorMessage)
	assert.NotNil(t, result.ArchivedAt)
}

func TestScanArchiveMetadata_ScanError(t *testing.T) {
	t.Parallel()

	scanErr := errors.New("scan failed")
	scanner := &fakeScanner{scan: func(_ ...any) error {
		return scanErr
	}}

	_, err := scanArchiveMetadata(scanner)
	require.Error(t, err)
	require.Contains(t, err.Error(), "scanning archive metadata")
}

func TestCreateWithTx_NilProvider(t *testing.T) {
	t.Parallel()

	repo := NewRepository(nil)
	ctx := context.Background()

	err := repo.CreateWithTx(ctx, nil, &entities.ArchiveMetadata{})
	require.ErrorIs(t, err, ErrRepositoryNotInitialized)
}

func TestCreateWithTx_NilMetadata(t *testing.T) {
	t.Parallel()

	provider := &testutil.MockInfrastructureProvider{}
	repo := NewRepository(provider)
	ctx := context.Background()

	err := repo.CreateWithTx(ctx, nil, nil)
	require.ErrorIs(t, err, ErrMetadataRequired)
}

func TestCreateWithTx_NilTx(t *testing.T) {
	t.Parallel()

	provider := &testutil.MockInfrastructureProvider{}
	repo := NewRepository(provider)
	ctx := context.Background()

	err := repo.CreateWithTx(ctx, nil, &entities.ArchiveMetadata{})
	require.ErrorIs(t, err, ErrTransactionRequired)
}

func TestUpdateWithTx_NilProvider(t *testing.T) {
	t.Parallel()

	repo := NewRepository(nil)
	ctx := context.Background()

	err := repo.UpdateWithTx(ctx, nil, &entities.ArchiveMetadata{})
	require.ErrorIs(t, err, ErrRepositoryNotInitialized)
}

func TestUpdateWithTx_NilMetadata(t *testing.T) {
	t.Parallel()

	provider := &testutil.MockInfrastructureProvider{}
	repo := NewRepository(provider)
	ctx := context.Background()

	err := repo.UpdateWithTx(ctx, nil, nil)
	require.ErrorIs(t, err, ErrMetadataRequired)
}

func TestUpdateWithTx_NilTx(t *testing.T) {
	t.Parallel()

	provider := &testutil.MockInfrastructureProvider{}
	repo := NewRepository(provider)
	ctx := context.Background()

	err := repo.UpdateWithTx(ctx, nil, &entities.ArchiveMetadata{})
	require.ErrorIs(t, err, ErrTransactionRequired)
}

func TestSentinelErrors(t *testing.T) {
	t.Parallel()

	errs := []struct {
		name string
		err  error
	}{
		{"ErrRepositoryNotInitialized", ErrRepositoryNotInitialized},
		{"ErrMetadataRequired", ErrMetadataRequired},
		{"ErrMetadataNotFound", ErrMetadataNotFound},
		{"ErrIDRequired", ErrIDRequired},
		{"ErrTenantIDRequired", ErrTenantIDRequired},
		{"ErrPartitionNameRequired", ErrPartitionNameRequired},
		{"ErrLimitMustBePositive", ErrLimitMustBePositive},
		{"ErrNilScanner", ErrNilScanner},
		{"ErrTransactionRequired", ErrTransactionRequired},
	}

	for _, tt := range errs {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			require.Error(t, tt.err)
			require.NotEmpty(t, tt.err.Error())
		})
	}
}

func TestSentinelErrors_AreDistinct(t *testing.T) {
	t.Parallel()

	errs := []error{
		ErrRepositoryNotInitialized,
		ErrMetadataRequired,
		ErrMetadataNotFound,
		ErrIDRequired,
		ErrTenantIDRequired,
		ErrPartitionNameRequired,
		ErrLimitMustBePositive,
		ErrNilScanner,
		ErrTransactionRequired,
	}

	for i, err1 := range errs {
		for j, err2 := range errs {
			if i != j {
				assert.NotEqual(t, err1, err2, "errors at index %d and %d should be distinct", i, j)
			}
		}
	}
}

// fakeScanner implements the scanner interface for testing.
type fakeScanner struct {
	scan func(dest ...any) error
}

func (f *fakeScanner) Scan(dest ...any) error {
	return f.scan(dest...)
}
