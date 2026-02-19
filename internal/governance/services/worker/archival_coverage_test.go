//go:build unit

package worker

import (
	"bytes"
	"compress/gzip"
	"context"
	"crypto/sha256"
	"errors"
	"testing"
	"time"

	"github.com/DATA-DOG/go-sqlmock"
	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.uber.org/mock/gomock"

	"github.com/LerianStudio/matcher/internal/auth"
	"github.com/LerianStudio/matcher/internal/governance/domain/entities"
	"github.com/LerianStudio/matcher/internal/governance/services/command"
)

// --- getOrCreateMetadata Tests ---

func TestGetOrCreateMetadata_ExistingMetadata(t *testing.T) {
	t.Parallel()

	deps := setupTestDeps(t)
	defer deps.ctrl.Finish()

	w, err := NewArchivalWorker(
		deps.archiveRepo, deps.partitionMgr, deps.storage,
		deps.db, deps.provider, deps.cfg, deps.logger,
	)
	require.NoError(t, err)

	tenantID := uuid.MustParse("550e8400-e29b-41d4-a716-446655440000")
	ctx := context.WithValue(context.Background(), auth.TenantIDKey, tenantID.String())

	existing := &entities.ArchiveMetadata{
		ID:            uuid.New(),
		TenantID:      tenantID,
		PartitionName: "audit_logs_2024_01",
		Status:        entities.StatusComplete,
	}

	partInfo := &command.PartitionInfo{
		Name:       "audit_logs_2024_01",
		RangeStart: time.Date(2024, 1, 1, 0, 0, 0, 0, time.UTC),
		RangeEnd:   time.Date(2024, 2, 1, 0, 0, 0, 0, time.UTC),
	}

	deps.archiveRepo.EXPECT().
		GetByPartition(gomock.Any(), tenantID, "audit_logs_2024_01").
		Return(existing, nil)

	result, err := w.getOrCreateMetadata(ctx, tenantID, partInfo)
	require.NoError(t, err)
	assert.Equal(t, existing, result)
}

func TestGetOrCreateMetadata_CreateNew(t *testing.T) {
	t.Parallel()

	deps := setupTestDeps(t)
	defer deps.ctrl.Finish()

	w, err := NewArchivalWorker(
		deps.archiveRepo, deps.partitionMgr, deps.storage,
		deps.db, deps.provider, deps.cfg, deps.logger,
	)
	require.NoError(t, err)

	tenantID := uuid.MustParse("550e8400-e29b-41d4-a716-446655440000")
	ctx := context.WithValue(context.Background(), auth.TenantIDKey, tenantID.String())

	partInfo := &command.PartitionInfo{
		Name:       "audit_logs_2024_01",
		RangeStart: time.Date(2024, 1, 1, 0, 0, 0, 0, time.UTC),
		RangeEnd:   time.Date(2024, 2, 1, 0, 0, 0, 0, time.UTC),
	}

	deps.archiveRepo.EXPECT().
		GetByPartition(gomock.Any(), tenantID, "audit_logs_2024_01").
		Return(nil, errors.New("not found"))

	deps.archiveRepo.EXPECT().
		Create(gomock.Any(), gomock.Any()).
		Return(nil)

	result, err := w.getOrCreateMetadata(ctx, tenantID, partInfo)
	require.NoError(t, err)
	assert.NotNil(t, result)
	assert.Equal(t, tenantID, result.TenantID)
	assert.Equal(t, "audit_logs_2024_01", result.PartitionName)
}

func TestGetOrCreateMetadata_CreateFails(t *testing.T) {
	t.Parallel()

	deps := setupTestDeps(t)
	defer deps.ctrl.Finish()

	w, err := NewArchivalWorker(
		deps.archiveRepo, deps.partitionMgr, deps.storage,
		deps.db, deps.provider, deps.cfg, deps.logger,
	)
	require.NoError(t, err)

	tenantID := uuid.MustParse("550e8400-e29b-41d4-a716-446655440000")
	ctx := context.WithValue(context.Background(), auth.TenantIDKey, tenantID.String())

	partInfo := &command.PartitionInfo{
		Name:       "audit_logs_2024_01",
		RangeStart: time.Date(2024, 1, 1, 0, 0, 0, 0, time.UTC),
		RangeEnd:   time.Date(2024, 2, 1, 0, 0, 0, 0, time.UTC),
	}

	deps.archiveRepo.EXPECT().
		GetByPartition(gomock.Any(), tenantID, "audit_logs_2024_01").
		Return(nil, errors.New("not found"))

	deps.archiveRepo.EXPECT().
		Create(gomock.Any(), gomock.Any()).
		Return(errors.New("db error"))

	result, err := w.getOrCreateMetadata(ctx, tenantID, partInfo)
	require.Error(t, err)
	assert.Nil(t, result)
	assert.Contains(t, err.Error(), "persist archive metadata")
}

// --- releaseLock Tests ---

func TestReleaseLock_Success(t *testing.T) {
	t.Parallel()

	deps := setupTestDeps(t)
	defer deps.ctrl.Finish()

	w, err := NewArchivalWorker(
		deps.archiveRepo, deps.partitionMgr, deps.storage,
		deps.db, deps.provider, deps.cfg, deps.logger,
	)
	require.NoError(t, err)

	ctx := context.Background()

	// First acquire the lock.
	acquired, token, err := w.acquireLock(ctx)
	require.NoError(t, err)
	require.True(t, acquired)

	// Release should succeed.
	w.releaseLock(ctx, token)

	// Lock should be released, so we can acquire again.
	acquired2, _, err := w.acquireLock(ctx)
	require.NoError(t, err)
	assert.True(t, acquired2)
}

func TestReleaseLock_WrongToken(t *testing.T) {
	t.Parallel()

	deps := setupTestDeps(t)
	defer deps.ctrl.Finish()

	w, err := NewArchivalWorker(
		deps.archiveRepo, deps.partitionMgr, deps.storage,
		deps.db, deps.provider, deps.cfg, deps.logger,
	)
	require.NoError(t, err)

	ctx := context.Background()

	// Acquire the lock.
	acquired, _, err := w.acquireLock(ctx)
	require.NoError(t, err)
	require.True(t, acquired)

	// Release with wrong token should not release it.
	w.releaseLock(ctx, "wrong-token")

	// Lock should still be held.
	acquired2, _, err := w.acquireLock(ctx)
	require.NoError(t, err)
	assert.False(t, acquired2)
}

func TestReleaseLock_NilRedisConn(t *testing.T) {
	t.Parallel()

	deps := setupTestDeps(t)
	defer deps.ctrl.Finish()

	// Override provider with nil Redis.
	deps.provider.RedisConn = nil

	w, err := NewArchivalWorker(
		deps.archiveRepo, deps.partitionMgr, deps.storage,
		deps.db, deps.provider, deps.cfg, deps.logger,
	)
	require.NoError(t, err)

	// Should not panic.
	w.releaseLock(context.Background(), "some-token")
}

// --- encodePartitionRows Tests ---

func TestEncodePartitionRows_EmptyRows(t *testing.T) {
	t.Parallel()

	db, mock, err := sqlmock.New()
	require.NoError(t, err)
	defer db.Close()

	rows := sqlmock.NewRows([]string{
		"id", "tenant_id", "entity_type", "entity_id", "action",
		"actor_id", "changes", "created_at", "tenant_seq",
		"prev_hash", "record_hash", "hash_version",
	})
	mock.ExpectQuery("SELECT").WillReturnRows(rows)

	sqlRows, err := db.Query("SELECT")
	require.NoError(t, err)
	defer sqlRows.Close()

	var buf bytes.Buffer
	gzWriter, err := gzip.NewWriterLevel(&buf, gzip.BestCompression)
	require.NoError(t, err)

	hasher := sha256.New()

	count, err := encodePartitionRows(sqlRows, gzWriter, hasher)
	require.NoError(t, err)
	assert.Equal(t, int64(0), count)
}

func TestEncodePartitionRows_SingleRow(t *testing.T) {
	t.Parallel()

	db, mock, err := sqlmock.New()
	require.NoError(t, err)
	defer db.Close()

	tenantID := uuid.New().String()
	entityID := uuid.New().String()

	rows := sqlmock.NewRows([]string{
		"id", "tenant_id", "entity_type", "entity_id", "action",
		"actor_id", "changes", "created_at", "tenant_seq",
		"prev_hash", "record_hash", "hash_version",
	}).AddRow(
		uuid.New().String(), tenantID, "transaction", entityID, "create",
		nil, nil, time.Now().UTC(), nil, nil, nil, nil,
	)
	mock.ExpectQuery("SELECT").WillReturnRows(rows)

	sqlRows, err := db.Query("SELECT")
	require.NoError(t, err)
	defer sqlRows.Close()

	var buf bytes.Buffer
	gzWriter, err := gzip.NewWriterLevel(&buf, gzip.BestCompression)
	require.NoError(t, err)

	hasher := sha256.New()

	count, err := encodePartitionRows(sqlRows, gzWriter, hasher)
	require.NoError(t, err)
	assert.Equal(t, int64(1), count)
}

func TestEncodePartitionRows_MultipleRows(t *testing.T) {
	t.Parallel()

	db, mock, err := sqlmock.New()
	require.NoError(t, err)
	defer db.Close()

	tenantID := uuid.New().String()

	rows := sqlmock.NewRows([]string{
		"id", "tenant_id", "entity_type", "entity_id", "action",
		"actor_id", "changes", "created_at", "tenant_seq",
		"prev_hash", "record_hash", "hash_version",
	})

	for i := 0; i < 5; i++ {
		rows.AddRow(
			uuid.New().String(), tenantID, "transaction", uuid.New().String(), "create",
			nil, nil, time.Now().UTC(), nil, nil, nil, nil,
		)
	}

	mock.ExpectQuery("SELECT").WillReturnRows(rows)

	sqlRows, err := db.Query("SELECT")
	require.NoError(t, err)
	defer sqlRows.Close()

	var buf bytes.Buffer
	gzWriter, err := gzip.NewWriterLevel(&buf, gzip.BestCompression)
	require.NoError(t, err)

	hasher := sha256.New()

	count, err := encodePartitionRows(sqlRows, gzWriter, hasher)
	require.NoError(t, err)
	assert.Equal(t, int64(5), count)
}

func TestEncodePartitionRows_ScanError(t *testing.T) {
	t.Parallel()

	db, mock, err := sqlmock.New()
	require.NoError(t, err)
	defer db.Close()

	// Wrong number of columns to cause a scan error.
	rows := sqlmock.NewRows([]string{"id"}).AddRow("bad-data")
	mock.ExpectQuery("SELECT").WillReturnRows(rows)

	sqlRows, err := db.Query("SELECT")
	require.NoError(t, err)
	defer sqlRows.Close()

	var buf bytes.Buffer
	gzWriter, err := gzip.NewWriterLevel(&buf, gzip.BestCompression)
	require.NoError(t, err)

	hasher := sha256.New()

	count, err := encodePartitionRows(sqlRows, gzWriter, hasher)
	require.Error(t, err)
	assert.Equal(t, int64(0), count)
}

// --- scanAuditLogRow Tests ---

func TestScanAuditLogRow_Success(t *testing.T) {
	t.Parallel()

	db, mock, err := sqlmock.New()
	require.NoError(t, err)
	defer db.Close()

	tenantID := uuid.New().String()
	entityID := uuid.New().String()
	actorID := "actor-123"
	changes := `{"key":"value"}`
	seq := int64(42)
	prevHash := "abc"
	recordHash := "def"
	hashVersion := 1
	now := time.Now().UTC()

	rows := sqlmock.NewRows([]string{
		"id", "tenant_id", "entity_type", "entity_id", "action",
		"actor_id", "changes", "created_at", "tenant_seq",
		"prev_hash", "record_hash", "hash_version",
	}).AddRow(
		uuid.New().String(), tenantID, "context", entityID, "update",
		&actorID, &changes, now, &seq, &prevHash, &recordHash, &hashVersion,
	)
	mock.ExpectQuery("SELECT").WillReturnRows(rows)

	sqlRows, err := db.Query("SELECT")
	require.NoError(t, err)
	defer sqlRows.Close()

	require.True(t, sqlRows.Next())

	row, err := scanAuditLogRow(sqlRows)
	require.NoError(t, err)
	assert.NotNil(t, row)
	assert.Equal(t, tenantID, row.TenantID)
	assert.Equal(t, "context", row.EntityType)
	assert.NotNil(t, row.ActorID)
	assert.Equal(t, actorID, *row.ActorID)
	assert.NotNil(t, row.TenantSeq)
	assert.Equal(t, seq, *row.TenantSeq)
}

// --- archiveTenant Tests ---

func TestArchiveTenantCov_ListPartitionsError(t *testing.T) {
	t.Parallel()

	deps := setupTestDeps(t)
	defer deps.ctrl.Finish()

	w, err := NewArchivalWorker(
		deps.archiveRepo, deps.partitionMgr, deps.storage,
		deps.db, deps.provider, deps.cfg, deps.logger,
	)
	require.NoError(t, err)

	tenantID := uuid.MustParse("550e8400-e29b-41d4-a716-446655440000")
	ctx := context.WithValue(context.Background(), auth.TenantIDKey, tenantID.String())

	// Make partition manager fail by not setting up DB expectations.
	deps.pmMock.ExpectBegin()
	deps.pmMock.ExpectExec("SET LOCAL search_path").WillReturnResult(sqlmock.NewResult(0, 0))
	deps.pmMock.ExpectQuery("SELECT").WillReturnError(errors.New("db error"))
	deps.pmMock.ExpectRollback()

	err = w.archiveTenant(ctx, tenantID)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "list partitions")
}

// --- processTenant Tests ---

func TestProcessTenantCov_InvalidTenantID(t *testing.T) {
	t.Parallel()

	deps := setupTestDeps(t)
	defer deps.ctrl.Finish()

	w, err := NewArchivalWorker(
		deps.archiveRepo, deps.partitionMgr, deps.storage,
		deps.db, deps.provider, deps.cfg, deps.logger,
	)
	require.NoError(t, err)

	// Should log a warning but not panic.
	w.processTenant(context.Background(), "not-a-uuid")
}

// --- provisionPartitions Tests ---

func TestProvisionPartitionsCov_Error(t *testing.T) {
	t.Parallel()

	deps := setupTestDeps(t)
	defer deps.ctrl.Finish()

	w, err := NewArchivalWorker(
		deps.archiveRepo, deps.partitionMgr, deps.storage,
		deps.db, deps.provider, deps.cfg, deps.logger,
	)
	require.NoError(t, err)

	ctx := context.WithValue(context.Background(), auth.TenantIDKey, uuid.New().String())

	// Make partition manager fail by setting up DB to return an error.
	deps.pmMock.ExpectBegin()
	deps.pmMock.ExpectExec("SET LOCAL search_path").WillReturnResult(sqlmock.NewResult(0, 0))
	deps.pmMock.ExpectExec("CREATE TABLE IF NOT EXISTS").WillReturnError(errors.New("db error"))
	deps.pmMock.ExpectRollback()

	err = w.provisionPartitions(ctx)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "ensure partitions exist")
}

// --- handlePartitionError Tests ---

func TestHandlePartitionErrorCov_UpdateFails(t *testing.T) {
	t.Parallel()

	deps := setupTestDeps(t)
	defer deps.ctrl.Finish()

	w, err := NewArchivalWorker(
		deps.archiveRepo, deps.partitionMgr, deps.storage,
		deps.db, deps.provider, deps.cfg, deps.logger,
	)
	require.NoError(t, err)

	metadataID := uuid.New()
	metadata := &entities.ArchiveMetadata{
		ID:            metadataID,
		PartitionName: "audit_logs_2024_01",
	}

	deps.archiveRepo.EXPECT().
		Update(gomock.Any(), gomock.Any()).
		Return(errors.New("update failed"))
	deps.archiveRepo.EXPECT().
		GetByID(gomock.Any(), metadataID).
		Return(nil, errors.New("reload failed"))

	wrappedErr := w.handlePartitionError(
		context.Background(),
		metadata,
		"test operation",
		errors.New("original error"),
	)
	require.Error(t, wrappedErr)
	assert.Contains(t, wrappedErr.Error(), "test operation")
	assert.Contains(t, wrappedErr.Error(), "original error")
}
