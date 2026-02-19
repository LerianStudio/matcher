//go:build unit

package worker

import (
	"bytes"
	"compress/gzip"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"io"
	"testing"
	"time"

	"github.com/DATA-DOG/go-sqlmock"
	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.uber.org/mock/gomock"

	"github.com/LerianStudio/matcher/internal/auth"
	"github.com/LerianStudio/matcher/internal/governance/domain/entities"
)

// buildVerificationArchive creates a gzip-compressed JSONL archive with the given
// number of rows, returning the compressed bytes, the SHA-256 checksum of the
// uncompressed content (matching the export encoding pattern), and the row count.
// If custom rows are provided they are used; otherwise random rows are generated.
func buildVerificationArchive(t *testing.T, rowCount int, customRows ...map[string]any) (io.ReadCloser, string, int64) {
	t.Helper()

	var buf bytes.Buffer

	hasher := sha256.New()

	gzWriter, err := gzip.NewWriterLevel(&buf, gzip.BestCompression)
	require.NoError(t, err)

	encoder := json.NewEncoder(gzWriter)

	for i := 0; i < rowCount; i++ {
		var row map[string]any
		if i < len(customRows) {
			row = customRows[i]
		} else {
			row = map[string]any{
				"id":          uuid.New().String(),
				"tenant_id":   uuid.New().String(),
				"entity_type": "transaction",
				"entity_id":   uuid.New().String(),
				"action":      "create",
			}
		}

		jsonBytes, marshalErr := json.Marshal(row)
		require.NoError(t, marshalErr)

		// sha256.Hash.Write never returns error per Go hash.Hash contract.
		_, writeErr := hasher.Write(jsonBytes)
		require.NoError(t, writeErr)

		_, writeErr = hasher.Write([]byte("\n"))
		require.NoError(t, writeErr)

		require.NoError(t, encoder.Encode(row))
	}

	require.NoError(t, gzWriter.Close())

	checksum := hex.EncodeToString(hasher.Sum(nil))

	return io.NopCloser(bytes.NewReader(buf.Bytes())), checksum, int64(rowCount)
}

// --- verifyArchive tests ---

func TestVerifyArchive(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name       string
		metadata   *entities.ArchiveMetadata
		setupMocks func(deps *testDeps)
		wantErr    bool
		wantErrIs  error
	}{
		{
			name: "row count zero",
			metadata: &entities.ArchiveMetadata{
				Checksum:   "abc123",
				RowCount:   0,
				ArchiveKey: "archives/test",
			},
			setupMocks: func(deps *testDeps) {
				deps.archiveRepo.EXPECT().Update(gomock.Any(), gomock.Any()).Return(nil).AnyTimes()
			},
			wantErr:   true,
			wantErrIs: ErrRowCountMismatch,
		},
		{
			name: "negative row count",
			metadata: &entities.ArchiveMetadata{
				Checksum:   "abc123",
				RowCount:   -1,
				ArchiveKey: "archives/test",
			},
			setupMocks: nil,
			wantErr:    true,
			wantErrIs:  ErrRowCountMismatch,
		},
		{
			name: "storage download error",
			metadata: &entities.ArchiveMetadata{
				Checksum:   "abc123",
				RowCount:   100,
				ArchiveKey: "archives/test",
			},
			setupMocks: func(deps *testDeps) {
				deps.storage.EXPECT().Download(gomock.Any(), "archives/test").
					Return(nil, errors.New("storage unavailable"))
			},
			wantErr:   true,
			wantErrIs: nil,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			deps := setupTestDeps(t)
			defer deps.ctrl.Finish()

			if tt.setupMocks != nil {
				tt.setupMocks(deps)
			}

			w, err := NewArchivalWorker(
				deps.archiveRepo, deps.partitionMgr, deps.storage,
				deps.db, deps.provider, deps.cfg, deps.logger,
			)
			require.NoError(t, err)

			err = w.verifyArchive(context.Background(), tt.metadata)

			if tt.wantErr {
				assert.Error(t, err)

				if tt.wantErrIs != nil {
					assert.ErrorIs(t, err, tt.wantErrIs)
				}
			} else {
				assert.NoError(t, err)
			}
		})
	}
}

func TestVerifyArchive_DownloadAndVerify_Success(t *testing.T) {
	t.Parallel()

	deps := setupTestDeps(t)
	defer deps.ctrl.Finish()

	w, err := NewArchivalWorker(
		deps.archiveRepo, deps.partitionMgr, deps.storage,
		deps.db, deps.provider, deps.cfg, deps.logger,
	)
	require.NoError(t, err)

	reader, checksum, rowCount := buildVerificationArchive(t, 5)

	metadata := &entities.ArchiveMetadata{
		Checksum:   checksum,
		RowCount:   rowCount,
		ArchiveKey: "archives/test",
	}

	deps.storage.EXPECT().Download(gomock.Any(), "archives/test").Return(reader, nil)

	err = w.verifyArchive(context.Background(), metadata)
	assert.NoError(t, err)
}

func TestVerifyArchive_ChecksumMismatch(t *testing.T) {
	t.Parallel()

	deps := setupTestDeps(t)
	defer deps.ctrl.Finish()

	w, err := NewArchivalWorker(
		deps.archiveRepo, deps.partitionMgr, deps.storage,
		deps.db, deps.provider, deps.cfg, deps.logger,
	)
	require.NoError(t, err)

	reader, _, rowCount := buildVerificationArchive(t, 3)

	metadata := &entities.ArchiveMetadata{
		Checksum:   "0000000000000000000000000000000000000000000000000000000000000000",
		RowCount:   rowCount,
		ArchiveKey: "archives/test",
	}

	deps.storage.EXPECT().Download(gomock.Any(), "archives/test").Return(reader, nil)

	err = w.verifyArchive(context.Background(), metadata)
	assert.Error(t, err)
	assert.ErrorIs(t, err, ErrChecksumMismatch)
}

func TestVerifyArchive_RowCountMismatch(t *testing.T) {
	t.Parallel()

	deps := setupTestDeps(t)
	defer deps.ctrl.Finish()

	w, err := NewArchivalWorker(
		deps.archiveRepo, deps.partitionMgr, deps.storage,
		deps.db, deps.provider, deps.cfg, deps.logger,
	)
	require.NoError(t, err)

	reader, checksum, _ := buildVerificationArchive(t, 3)

	metadata := &entities.ArchiveMetadata{
		Checksum:   checksum,
		RowCount:   999, // Wrong row count.
		ArchiveKey: "archives/test",
	}

	deps.storage.EXPECT().Download(gomock.Any(), "archives/test").Return(reader, nil)

	err = w.verifyArchive(context.Background(), metadata)
	assert.Error(t, err)
	assert.ErrorIs(t, err, ErrRowCountMismatch)
}

// --- transitionTo tests ---

func TestTransitionTo_TransitionFunctionError(t *testing.T) {
	t.Parallel()

	deps := setupTestDeps(t)
	defer deps.ctrl.Finish()

	w, err := NewArchivalWorker(
		deps.archiveRepo, deps.partitionMgr, deps.storage,
		deps.db, deps.provider, deps.cfg, deps.logger,
	)
	require.NoError(t, err)

	metadata := &entities.ArchiveMetadata{
		Status: entities.StatusPending,
	}

	transitionErr := errors.New("transition failed")
	err = w.transitionTo(context.Background(), metadata, func() error {
		return transitionErr
	})
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "state transition")
}

func TestTransitionTo_PersistError(t *testing.T) {
	t.Parallel()

	deps := setupTestDeps(t)
	defer deps.ctrl.Finish()

	w, err := NewArchivalWorker(
		deps.archiveRepo, deps.partitionMgr, deps.storage,
		deps.db, deps.provider, deps.cfg, deps.logger,
	)
	require.NoError(t, err)

	metadata := &entities.ArchiveMetadata{
		Status: entities.StatusPending,
	}

	deps.archiveRepo.EXPECT().Update(gomock.Any(), gomock.Any()).
		Return(errors.New("db error"))

	err = w.transitionTo(context.Background(), metadata, func() error {
		return nil
	})
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "persist state transition")
}

func TestTransitionTo_Success(t *testing.T) {
	t.Parallel()

	deps := setupTestDeps(t)
	defer deps.ctrl.Finish()

	w, err := NewArchivalWorker(
		deps.archiveRepo, deps.partitionMgr, deps.storage,
		deps.db, deps.provider, deps.cfg, deps.logger,
	)
	require.NoError(t, err)

	metadata := &entities.ArchiveMetadata{
		Status: entities.StatusPending,
	}

	deps.archiveRepo.EXPECT().Update(gomock.Any(), gomock.Any()).Return(nil)

	called := false
	err = w.transitionTo(context.Background(), metadata, func() error {
		called = true
		return nil
	})
	assert.NoError(t, err)
	assert.True(t, called)
}

// --- transitionToExported tests ---

func TestTransitionToExported_MarkExportedError(t *testing.T) {
	t.Parallel()

	deps := setupTestDeps(t)
	defer deps.ctrl.Finish()

	w, err := NewArchivalWorker(
		deps.archiveRepo, deps.partitionMgr, deps.storage,
		deps.db, deps.provider, deps.cfg, deps.logger,
	)
	require.NoError(t, err)

	// MarkExported with negative row count should fail
	metadata := &entities.ArchiveMetadata{
		Status: entities.StatusExporting,
	}

	err = w.transitionToExported(context.Background(), metadata, -1)
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "mark exported")
}

func TestTransitionToExported_PersistError(t *testing.T) {
	t.Parallel()

	deps := setupTestDeps(t)
	defer deps.ctrl.Finish()

	w, err := NewArchivalWorker(
		deps.archiveRepo, deps.partitionMgr, deps.storage,
		deps.db, deps.provider, deps.cfg, deps.logger,
	)
	require.NoError(t, err)

	metadata := &entities.ArchiveMetadata{
		Status: entities.StatusExporting,
	}

	deps.archiveRepo.EXPECT().Update(gomock.Any(), gomock.Any()).
		Return(errors.New("db error"))

	err = w.transitionToExported(context.Background(), metadata, 100)
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "persist exported state")
}

func TestTransitionToExported_Success(t *testing.T) {
	t.Parallel()

	deps := setupTestDeps(t)
	defer deps.ctrl.Finish()

	w, err := NewArchivalWorker(
		deps.archiveRepo, deps.partitionMgr, deps.storage,
		deps.db, deps.provider, deps.cfg, deps.logger,
	)
	require.NoError(t, err)

	metadata := &entities.ArchiveMetadata{
		Status: entities.StatusExporting,
	}

	deps.archiveRepo.EXPECT().Update(gomock.Any(), gomock.Any()).Return(nil)

	err = w.transitionToExported(context.Background(), metadata, 100)
	require.NoError(t, err)
	assert.Equal(t, int64(100), metadata.RowCount)
	assert.Equal(t, entities.StatusExported, metadata.Status)
}

// --- transitionToUploaded tests ---

func TestTransitionToUploaded_PersistError(t *testing.T) {
	t.Parallel()

	deps := setupTestDeps(t)
	defer deps.ctrl.Finish()

	w, err := NewArchivalWorker(
		deps.archiveRepo, deps.partitionMgr, deps.storage,
		deps.db, deps.provider, deps.cfg, deps.logger,
	)
	require.NoError(t, err)

	metadata := &entities.ArchiveMetadata{
		Status: entities.StatusUploading,
	}

	deps.archiveRepo.EXPECT().Update(gomock.Any(), gomock.Any()).
		Return(errors.New("db error"))

	err = w.transitionToUploaded(context.Background(), metadata, "key", "checksum", 1024)
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "persist uploaded state")
}

func TestTransitionToUploaded_Success(t *testing.T) {
	t.Parallel()

	deps := setupTestDeps(t)
	defer deps.ctrl.Finish()

	w, err := NewArchivalWorker(
		deps.archiveRepo, deps.partitionMgr, deps.storage,
		deps.db, deps.provider, deps.cfg, deps.logger,
	)
	require.NoError(t, err)

	metadata := &entities.ArchiveMetadata{
		Status: entities.StatusUploading,
	}

	deps.archiveRepo.EXPECT().Update(gomock.Any(), gomock.Any()).Return(nil)

	err = w.transitionToUploaded(context.Background(), metadata, "key", "checksum", 1024)
	require.NoError(t, err)
	assert.Equal(t, "key", metadata.ArchiveKey)
	assert.Equal(t, "checksum", metadata.Checksum)
	assert.Equal(t, int64(1024), metadata.CompressedSizeBytes)
	assert.Equal(t, "GLACIER", metadata.StorageClass)
	assert.Equal(t, entities.StatusUploaded, metadata.Status)
}

// --- handlePartitionError tests ---

func TestHandlePartitionError_UpdateSuccess(t *testing.T) {
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
		Status:        entities.StatusExporting,
	}

	reloaded := &entities.ArchiveMetadata{
		ID:            metadataID,
		PartitionName: "audit_logs_2024_01",
		Status:        entities.StatusExporting,
		ErrorMessage:  "test operation: test error",
	}

	deps.archiveRepo.EXPECT().Update(gomock.Any(), gomock.Any()).Return(nil)
	deps.archiveRepo.EXPECT().GetByID(gomock.Any(), metadataID).Return(reloaded, nil)

	err = w.handlePartitionError(context.Background(), metadata, "test operation", errors.New("test error"))
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "test operation")
	assert.Contains(t, err.Error(), "test error")
	assert.NotEmpty(t, metadata.ErrorMessage)
}

func TestHandlePartitionError_UpdateFails(t *testing.T) {
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

	deps.archiveRepo.EXPECT().Update(gomock.Any(), gomock.Any()).
		Return(errors.New("update failed"))
	// GetByID still called after update error; reload may also fail.
	deps.archiveRepo.EXPECT().GetByID(gomock.Any(), metadataID).
		Return(nil, errors.New("reload failed"))

	// Should still return the original wrapped error, not the update error
	err = w.handlePartitionError(context.Background(), metadata, "test operation", errors.New("test error"))
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "test operation")
}

// --- processTenant tests ---

func TestProcessTenant_InvalidTenantID(t *testing.T) {
	t.Parallel()

	deps := setupTestDeps(t)
	defer deps.ctrl.Finish()

	w, err := NewArchivalWorker(
		deps.archiveRepo, deps.partitionMgr, deps.storage,
		deps.db, deps.provider, deps.cfg, deps.logger,
	)
	require.NoError(t, err)

	// intentionally only verifies no panic; other behavior tested elsewhere
	w.processTenant(context.Background(), "not-a-valid-uuid")
}

func TestProcessTenant_ProvisionPartitionsError(t *testing.T) {
	t.Parallel()

	deps := setupTestDeps(t)
	defer deps.ctrl.Finish()

	w, err := NewArchivalWorker(
		deps.archiveRepo, deps.partitionMgr, deps.storage,
		deps.db, deps.provider, deps.cfg, deps.logger,
	)
	require.NoError(t, err)

	tenantID := uuid.New().String()
	ctx := context.WithValue(context.Background(), auth.TenantIDKey, tenantID)

	// The partition manager will call EnsurePartitionsExist which does DB ops
	// This should fail because pmMock has no expectations set up, but processTenant
	// should handle the error gracefully.
	deps.pmMock.ExpectBegin()
	deps.pmMock.ExpectExec("SET LOCAL search_path").
		WillReturnError(errors.New("schema not found"))
	deps.pmMock.ExpectRollback()

	// archiveTenant also needs ListPartitions — set up for that too
	deps.pmMock.ExpectBegin()
	deps.pmMock.ExpectExec("SET LOCAL search_path").
		WillReturnError(errors.New("schema not found"))
	deps.pmMock.ExpectRollback()

	// Should not panic
	w.processTenant(ctx, tenantID)
}

// --- archiveTenant tests ---

func TestArchiveTenant_ListPartitionsError(t *testing.T) {
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

	// ListPartitions fails at BeginTx
	deps.pmMock.ExpectBegin().WillReturnError(errors.New("db connection lost"))

	err = w.archiveTenant(ctx, tenantID)
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "list partitions")
}

// --- archivePartition already complete test ---

func TestArchivePartition_SkipsComplete(t *testing.T) {
	t.Parallel()

	deps := setupTestDeps(t)
	defer deps.ctrl.Finish()

	w, err := NewArchivalWorker(
		deps.archiveRepo, deps.partitionMgr, deps.storage,
		deps.db, deps.provider, deps.cfg, deps.logger,
	)
	require.NoError(t, err)

	metadata := &entities.ArchiveMetadata{
		ID:            uuid.New(),
		PartitionName: "audit_logs_2024_01",
		Status:        entities.StatusComplete,
	}

	// No repo calls should be made — gomock will fail if any are called.
	err = w.archivePartition(context.Background(), metadata)
	assert.NoError(t, err)
}

// --- handleUploadingState - re-export on crash recovery ---

func TestHandleUploadingState_ReExportError(t *testing.T) {
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

	metadataID := uuid.New()
	metadata := &entities.ArchiveMetadata{
		ID:             metadataID,
		TenantID:       tenantID,
		PartitionName:  "audit_logs_2024_01",
		DateRangeStart: time.Date(2024, 1, 1, 0, 0, 0, 0, time.UTC),
		DateRangeEnd:   time.Date(2024, 2, 1, 0, 0, 0, 0, time.UTC),
		Status:         entities.StatusUploading,
	}

	// Export fails during re-export
	deps.sqlMock.ExpectBegin()
	deps.sqlMock.ExpectExec("SET LOCAL search_path").WillReturnResult(sqlmock.NewResult(0, 0))
	deps.sqlMock.ExpectQuery("SELECT id, tenant_id").
		WillReturnError(errors.New("db connection lost"))
	deps.sqlMock.ExpectRollback()

	deps.archiveRepo.EXPECT().Update(gomock.Any(), gomock.Any()).Return(nil)
	deps.archiveRepo.EXPECT().GetByID(gomock.Any(), metadataID).Return(metadata, nil)

	// nil exportBuf triggers re-export
	err = w.handleUploadingState(ctx, metadata, nil, "")
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "re-export partition")
}

// --- handleDetachingState ---

func TestHandleDetachingState_DetachError(t *testing.T) {
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

	metadataID := uuid.New()
	metadata := &entities.ArchiveMetadata{
		ID:             metadataID,
		TenantID:       tenantID,
		PartitionName:  "audit_logs_2015_01",
		DateRangeStart: time.Date(2015, 1, 1, 0, 0, 0, 0, time.UTC),
		DateRangeEnd:   time.Date(2015, 2, 1, 0, 0, 0, 0, time.UTC),
		Status:         entities.StatusDetaching,
	}

	deps.pmMock.ExpectBegin()
	deps.pmMock.ExpectExec("SET LOCAL search_path").WillReturnResult(sqlmock.NewResult(0, 0))
	deps.pmMock.ExpectExec("ALTER TABLE audit_logs DETACH PARTITION").
		WillReturnError(errors.New("detach failed"))
	deps.pmMock.ExpectRollback()

	deps.archiveRepo.EXPECT().Update(gomock.Any(), gomock.Any()).Return(nil)
	deps.archiveRepo.EXPECT().GetByID(gomock.Any(), metadataID).Return(metadata, nil)

	err = w.handleDetachingState(ctx, metadata)
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "detach")
}

// --- releaseLock tests ---

func TestReleaseLock_NilRedisConnection(t *testing.T) {
	t.Parallel()

	deps := setupTestDeps(t)
	defer deps.ctrl.Finish()

	// Override provider to return nil
	deps.provider.RedisConn = nil

	w, err := NewArchivalWorker(
		deps.archiveRepo, deps.partitionMgr, deps.storage,
		deps.db, deps.provider, deps.cfg, deps.logger,
	)
	require.NoError(t, err)

	// Should not panic
	w.releaseLock(context.Background(), "some-token")
}

// getOrCreateMetadata is tested indirectly through archivePartition and archiveTenant
// because it takes a *command.PartitionInfo which requires the partition manager.

// --- archiveKey format tests ---

func TestArchiveKey_JanuaryPartition(t *testing.T) {
	t.Parallel()

	deps := setupTestDeps(t)
	defer deps.ctrl.Finish()

	w, err := NewArchivalWorker(
		deps.archiveRepo, deps.partitionMgr, deps.storage,
		deps.db, deps.provider, deps.cfg, deps.logger,
	)
	require.NoError(t, err)

	tenantID := uuid.MustParse("550e8400-e29b-41d4-a716-446655440000")
	archiveID := uuid.MustParse("aabbccdd-0011-2233-4455-667788990011")
	metadata := &entities.ArchiveMetadata{
		ID:             archiveID,
		TenantID:       tenantID,
		PartitionName:  "audit_logs_2024_01",
		DateRangeStart: time.Date(2024, 1, 1, 0, 0, 0, 0, time.UTC),
		DateRangeEnd:   time.Date(2024, 2, 1, 0, 0, 0, 0, time.UTC),
	}

	key := w.archiveKey(metadata)
	expected := "archives/audit-logs/550e8400-e29b-41d4-a716-446655440000/2024/01/aabbccdd-0011-2233-4455-667788990011/audit_logs_2024_01.jsonl.gz"
	assert.Equal(t, expected, key)
}

func TestArchiveKey_DecemberPartition(t *testing.T) {
	t.Parallel()

	deps := setupTestDeps(t)
	defer deps.ctrl.Finish()

	w, err := NewArchivalWorker(
		deps.archiveRepo, deps.partitionMgr, deps.storage,
		deps.db, deps.provider, deps.cfg, deps.logger,
	)
	require.NoError(t, err)

	tenantID := uuid.MustParse("660e8400-e29b-41d4-a716-446655440001")
	archiveID := uuid.MustParse("aabbccdd-0011-2233-4455-667788990022")
	metadata := &entities.ArchiveMetadata{
		ID:             archiveID,
		TenantID:       tenantID,
		PartitionName:  "audit_logs_2023_12",
		DateRangeStart: time.Date(2023, 12, 1, 0, 0, 0, 0, time.UTC),
		DateRangeEnd:   time.Date(2024, 1, 1, 0, 0, 0, 0, time.UTC),
	}

	key := w.archiveKey(metadata)
	expected := "archives/audit-logs/660e8400-e29b-41d4-a716-446655440001/2023/12/aabbccdd-0011-2233-4455-667788990022/audit_logs_2023_12.jsonl.gz"
	assert.Equal(t, expected, key)
}

// --- detachAndDrop tests ---

func TestDetachAndDrop_DetachError(t *testing.T) {
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

	metadata := &entities.ArchiveMetadata{
		PartitionName:  "audit_logs_2015_01",
		DateRangeStart: time.Date(2015, 1, 1, 0, 0, 0, 0, time.UTC),
		DateRangeEnd:   time.Date(2015, 2, 1, 0, 0, 0, 0, time.UTC),
	}

	deps.pmMock.ExpectBegin()
	deps.pmMock.ExpectExec("SET LOCAL search_path").WillReturnResult(sqlmock.NewResult(0, 0))
	deps.pmMock.ExpectExec("ALTER TABLE audit_logs DETACH PARTITION").
		WillReturnError(errors.New("detach error"))
	deps.pmMock.ExpectRollback()

	err = w.detachAndDrop(ctx, metadata)
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "detach partition")
}

// --- provisionPartitions test ---

func TestProvisionPartitions_Error(t *testing.T) {
	t.Parallel()

	deps := setupTestDeps(t)
	defer deps.ctrl.Finish()

	w, err := NewArchivalWorker(
		deps.archiveRepo, deps.partitionMgr, deps.storage,
		deps.db, deps.provider, deps.cfg, deps.logger,
	)
	require.NoError(t, err)

	ctx := context.WithValue(context.Background(), auth.TenantIDKey, uuid.New().String())

	deps.pmMock.ExpectBegin()
	deps.pmMock.ExpectExec("SET LOCAL search_path").
		WillReturnError(errors.New("schema not found"))
	deps.pmMock.ExpectRollback()

	err = w.provisionPartitions(ctx)
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "ensure partitions exist")
}

// --- acquireLock with nil redis client ---

func TestAcquireLock_NilRedisConnection(t *testing.T) {
	t.Parallel()

	deps := setupTestDeps(t)
	defer deps.ctrl.Finish()

	deps.provider.RedisConn = nil

	w, err := NewArchivalWorker(
		deps.archiveRepo, deps.partitionMgr, deps.storage,
		deps.db, deps.provider, deps.cfg, deps.logger,
	)
	require.NoError(t, err)

	acquired, token, err := w.acquireLock(context.Background())
	assert.Error(t, err)
	assert.False(t, acquired)
	assert.Empty(t, token)
	assert.ErrorIs(t, err, ErrNilRedisClient)
}

// --- archivePartition from various states ---

func TestArchivePartition_FromUploadedState(t *testing.T) {
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

	metadata := &entities.ArchiveMetadata{
		ID:             uuid.New(),
		TenantID:       tenantID,
		PartitionName:  "audit_logs_2015_01",
		DateRangeStart: time.Date(2015, 1, 1, 0, 0, 0, 0, time.UTC),
		DateRangeEnd:   time.Date(2015, 2, 1, 0, 0, 0, 0, time.UTC),
		Status:         entities.StatusUploaded,
		RowCount:       100,
		ArchiveKey:     "archives/test",
		Checksum:       "abc123",
	}

	deps.archiveRepo.EXPECT().Update(gomock.Any(), gomock.Any()).Return(nil).Times(4)

	// Build a valid verification archive matching the metadata.
	archiveReader, checksum, _ := buildVerificationArchive(t, 100)
	metadata.Checksum = checksum

	deps.storage.EXPECT().Download(gomock.Any(), gomock.Any()).Return(archiveReader, nil)

	// Detach
	deps.pmMock.ExpectBegin()
	deps.pmMock.ExpectExec("SET LOCAL search_path").WillReturnResult(sqlmock.NewResult(0, 0))
	deps.pmMock.ExpectExec("ALTER TABLE audit_logs DETACH PARTITION").
		WillReturnResult(sqlmock.NewResult(0, 0))
	deps.pmMock.ExpectCommit()

	// Drop
	deps.pmMock.ExpectBegin()
	deps.pmMock.ExpectExec("SET LOCAL search_path").WillReturnResult(sqlmock.NewResult(0, 0))
	deps.pmMock.ExpectExec("DROP TABLE IF EXISTS").
		WillReturnResult(sqlmock.NewResult(0, 0))
	deps.pmMock.ExpectCommit()

	err = w.archivePartition(ctx, metadata)
	assert.NoError(t, err)
}
