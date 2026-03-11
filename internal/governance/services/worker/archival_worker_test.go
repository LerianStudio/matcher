//go:build unit

package worker

import (
	"bytes"
	"context"
	"database/sql"
	"errors"
	"io"
	"regexp"
	"testing"
	"time"

	"github.com/DATA-DOG/go-sqlmock"
	"github.com/alicebob/miniredis/v2"
	"github.com/google/uuid"
	"github.com/redis/go-redis/v9"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/LerianStudio/matcher/internal/auth"
	"github.com/LerianStudio/matcher/internal/governance/domain/entities"
	"github.com/LerianStudio/matcher/internal/governance/domain/repositories/mocks"
	"github.com/LerianStudio/matcher/internal/governance/services/command"
	infraTestutil "github.com/LerianStudio/matcher/internal/shared/infrastructure/testutil"
	sharedPortMocks "github.com/LerianStudio/matcher/internal/shared/ports/mocks"
	sharedTestutil "github.com/LerianStudio/matcher/internal/shared/testutil"

	"go.uber.org/mock/gomock"
)

// buildTestArchive creates a gzip-compressed JSONL archive from the given rows,
// returning the compressed bytes as an io.ReadCloser and the SHA-256 checksum.
// Delegates to buildVerificationArchive for the core encoding logic.
func buildTestArchive(t *testing.T, rows ...map[string]any) (io.ReadCloser, string) {
	t.Helper()

	reader, checksum, _ := buildVerificationArchive(t, len(rows), rows...)

	return reader, checksum
}

// testDeps bundles test dependencies.
type testDeps struct {
	ctrl         *gomock.Controller
	archiveRepo  *mocks.MockArchiveMetadataRepository
	storage      *sharedPortMocks.MockObjectStorageClient
	db           *sql.DB
	sqlMock      sqlmock.Sqlmock
	miniRedis    *miniredis.Miniredis
	provider     *infraTestutil.MockInfrastructureProvider
	partitionMgr *command.PartitionManager
	pmMock       sqlmock.Sqlmock
	cfg          ArchivalWorkerConfig
	logger       *sharedTestutil.TestLogger
}

func setupTestDeps(t *testing.T) *testDeps {
	t.Helper()

	ctrl := gomock.NewController(t)

	db, mock, err := sqlmock.New()
	require.NoError(t, err)

	srv := miniredis.RunT(t)
	redisClient := redis.NewClient(&redis.Options{Addr: srv.Addr()})
	conn := infraTestutil.NewRedisClientWithMock(redisClient)
	provider := &infraTestutil.MockInfrastructureProvider{RedisConn: conn}

	// Create partition manager with sqlmock db.
	// No pre-seeded expectations; tests that use partition manager operations
	// set up their own expectations via deps.pmMock.
	pmDB, pmMock, err := sqlmock.New()
	require.NoError(t, err)

	testLogger := &sharedTestutil.TestLogger{}

	pm, err := command.NewPartitionManager(pmDB, testLogger, nil)
	require.NoError(t, err)

	cfg := ArchivalWorkerConfig{
		Interval:            1 * time.Minute,
		HotRetentionDays:    90,
		WarmRetentionMonths: 24,
		ColdRetentionMonths: 84,
		BatchSize:           5000,
		StorageBucket:       "test-bucket",
		StoragePrefix:       "archives/audit-logs",
		StorageClass:        "GLACIER",
		PartitionLookahead:  3,
		PresignExpiry:       1 * time.Hour,
	}

	return &testDeps{
		ctrl:         ctrl,
		archiveRepo:  mocks.NewMockArchiveMetadataRepository(ctrl),
		storage:      sharedPortMocks.NewMockObjectStorageClient(ctrl),
		db:           db,
		sqlMock:      mock,
		miniRedis:    srv,
		provider:     provider,
		partitionMgr: pm,
		pmMock:       pmMock,
		cfg:          cfg,
		logger:       testLogger,
	}
}

// --- Constructor Tests ---

func TestNewArchivalWorker_Success(t *testing.T) {
	t.Parallel()

	deps := setupTestDeps(t)
	defer deps.ctrl.Finish()

	w, err := NewArchivalWorker(
		deps.archiveRepo,
		deps.partitionMgr,
		deps.storage,
		deps.db,
		deps.provider,
		deps.cfg,
		deps.logger,
	)

	assert.NoError(t, err)
	assert.NotNil(t, w)
}

func TestNewArchivalWorker_NilArchiveRepo(t *testing.T) {
	t.Parallel()

	deps := setupTestDeps(t)
	defer deps.ctrl.Finish()

	w, err := NewArchivalWorker(nil, deps.partitionMgr, deps.storage, deps.db, deps.provider, deps.cfg, deps.logger)

	assert.ErrorIs(t, err, ErrNilArchiveRepo)
	assert.Nil(t, w)
}

func TestNewArchivalWorker_NilPartitionManager(t *testing.T) {
	t.Parallel()

	deps := setupTestDeps(t)
	defer deps.ctrl.Finish()

	w, err := NewArchivalWorker(deps.archiveRepo, nil, deps.storage, deps.db, deps.provider, deps.cfg, deps.logger)

	assert.ErrorIs(t, err, ErrNilPartitionManager)
	assert.Nil(t, w)
}

func TestNewArchivalWorker_NilStorageClient(t *testing.T) {
	t.Parallel()

	deps := setupTestDeps(t)
	defer deps.ctrl.Finish()

	w, err := NewArchivalWorker(deps.archiveRepo, deps.partitionMgr, nil, deps.db, deps.provider, deps.cfg, deps.logger)

	assert.ErrorIs(t, err, ErrNilStorageClient)
	assert.Nil(t, w)
}

func TestNewArchivalWorker_NilDB(t *testing.T) {
	t.Parallel()

	deps := setupTestDeps(t)
	defer deps.ctrl.Finish()

	w, err := NewArchivalWorker(deps.archiveRepo, deps.partitionMgr, deps.storage, nil, deps.provider, deps.cfg, deps.logger)

	assert.ErrorIs(t, err, command.ErrNilDB)
	assert.Nil(t, w)
}

func TestNewArchivalWorker_NilInfraProvider(t *testing.T) {
	t.Parallel()

	deps := setupTestDeps(t)
	defer deps.ctrl.Finish()

	w, err := NewArchivalWorker(deps.archiveRepo, deps.partitionMgr, deps.storage, deps.db, nil, deps.cfg, deps.logger)

	assert.ErrorIs(t, err, ErrNilRedisClient)
	assert.Nil(t, w)
}

func TestNewArchivalWorker_NilLoggerDefaultsToNop(t *testing.T) {
	t.Parallel()

	deps := setupTestDeps(t)
	defer deps.ctrl.Finish()

	w, err := NewArchivalWorker(
		deps.archiveRepo,
		deps.partitionMgr,
		deps.storage,
		deps.db,
		deps.provider,
		deps.cfg,
		nil,
	)

	require.NoError(t, err)
	require.NotNil(t, w)
	require.NotNil(t, w.logger)
}

// --- Lifecycle Tests ---

func TestArchivalWorker_StartStop(t *testing.T) {
	t.Parallel()

	deps := setupTestDeps(t)
	defer deps.ctrl.Finish()

	// Set up expectations for the archival cycle that runs on start.
	// The cycle will try to acquire lock, then list incomplete, then list tenants.
	deps.archiveRepo.EXPECT().ListIncomplete(gomock.Any()).Return(nil, nil).AnyTimes()

	w, err := NewArchivalWorker(
		deps.archiveRepo,
		deps.partitionMgr,
		deps.storage,
		deps.db,
		deps.provider,
		deps.cfg,
		deps.logger,
	)
	require.NoError(t, err)

	// Expect list tenants query on db.
	deps.sqlMock.ExpectQuery("SELECT nspname FROM pg_namespace").
		WillReturnRows(sqlmock.NewRows([]string{"nspname"}))

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	err = w.Start(ctx)
	assert.NoError(t, err)

	// Verify double-start returns error.
	err = w.Start(ctx)
	assert.ErrorIs(t, err, ErrWorkerAlreadyRunning)

	// Wait for the archival cycle to complete instead of using a fixed sleep.
	assert.Eventually(t, func() bool {
		return deps.sqlMock.ExpectationsWereMet() == nil
	}, 2*time.Second, 10*time.Millisecond, "archival cycle did not complete")

	// Stop the worker.
	cancel()
	err = w.Stop()
	assert.NoError(t, err)
}

func TestArchivalWorker_StartStopStartStop_Success(t *testing.T) {
	t.Parallel()

	deps := setupTestDeps(t)
	defer deps.ctrl.Finish()

	deps.archiveRepo.EXPECT().ListIncomplete(gomock.Any()).Return(nil, nil).AnyTimes()

	w, err := NewArchivalWorker(
		deps.archiveRepo,
		deps.partitionMgr,
		deps.storage,
		deps.db,
		deps.provider,
		deps.cfg,
		deps.logger,
	)
	require.NoError(t, err)

	deps.sqlMock.ExpectQuery("SELECT nspname FROM pg_namespace").WillReturnRows(sqlmock.NewRows([]string{"nspname"}))
	deps.sqlMock.ExpectQuery("SELECT nspname FROM pg_namespace").WillReturnRows(sqlmock.NewRows([]string{"nspname"}))

	ctx := context.Background()
	require.NoError(t, w.Start(ctx))
	require.Eventually(t, func() bool {
		return w.running.Load()
	}, 500*time.Millisecond, 10*time.Millisecond)
	require.NoError(t, w.Stop())
	require.NoError(t, w.Start(ctx))
	require.Eventually(t, func() bool {
		return deps.sqlMock.ExpectationsWereMet() == nil
	}, 2*time.Second, 10*time.Millisecond)
	require.NoError(t, w.Stop())
}

func TestArchivalWorker_StopWithoutStart(t *testing.T) {
	t.Parallel()

	deps := setupTestDeps(t)
	defer deps.ctrl.Finish()

	w, err := NewArchivalWorker(
		deps.archiveRepo,
		deps.partitionMgr,
		deps.storage,
		deps.db,
		deps.provider,
		deps.cfg,
		deps.logger,
	)
	require.NoError(t, err)

	err = w.Stop()
	assert.ErrorIs(t, err, ErrWorkerNotRunning)
}

func TestArchivalWorker_Done(t *testing.T) {
	t.Parallel()

	deps := setupTestDeps(t)
	defer deps.ctrl.Finish()

	w, err := NewArchivalWorker(
		deps.archiveRepo,
		deps.partitionMgr,
		deps.storage,
		deps.db,
		deps.provider,
		deps.cfg,
		deps.logger,
	)
	require.NoError(t, err)

	doneCh := w.Done()
	assert.NotNil(t, doneCh)
}

// --- Lock Tests ---

func TestArchivalWorker_AcquireLock_Success(t *testing.T) {
	t.Parallel()

	deps := setupTestDeps(t)
	defer deps.ctrl.Finish()

	w, err := NewArchivalWorker(
		deps.archiveRepo,
		deps.partitionMgr,
		deps.storage,
		deps.db,
		deps.provider,
		deps.cfg,
		deps.logger,
	)
	require.NoError(t, err)

	ctx := context.Background()

	acquired, token, err := w.acquireLock(ctx)
	assert.NoError(t, err)
	assert.True(t, acquired)
	assert.NotEmpty(t, token)
}

func TestArchivalWorker_AcquireLock_AlreadyHeld(t *testing.T) {
	t.Parallel()

	deps := setupTestDeps(t)
	defer deps.ctrl.Finish()

	w, err := NewArchivalWorker(
		deps.archiveRepo,
		deps.partitionMgr,
		deps.storage,
		deps.db,
		deps.provider,
		deps.cfg,
		deps.logger,
	)
	require.NoError(t, err)

	ctx := context.Background()

	// First acquisition succeeds.
	acquired1, _, err := w.acquireLock(ctx)
	assert.NoError(t, err)
	assert.True(t, acquired1)

	// Second acquisition fails (lock held).
	acquired2, _, err := w.acquireLock(ctx)
	assert.NoError(t, err)
	assert.False(t, acquired2)
}

// --- Archive Key Tests ---

func TestArchivalWorker_ArchiveKey(t *testing.T) {
	t.Parallel()

	deps := setupTestDeps(t)
	defer deps.ctrl.Finish()

	w, err := NewArchivalWorker(
		deps.archiveRepo,
		deps.partitionMgr,
		deps.storage,
		deps.db,
		deps.provider,
		deps.cfg,
		deps.logger,
	)
	require.NoError(t, err)

	tenantID := uuid.MustParse("550e8400-e29b-41d4-a716-446655440000")
	archiveID := uuid.MustParse("aabbccdd-0011-2233-4455-667788990000")
	rangeStart := time.Date(2025, 6, 1, 0, 0, 0, 0, time.UTC)
	rangeEnd := time.Date(2025, 7, 1, 0, 0, 0, 0, time.UTC)

	metadata := &entities.ArchiveMetadata{
		ID:             archiveID,
		TenantID:       tenantID,
		PartitionName:  "audit_logs_2025_06",
		DateRangeStart: rangeStart,
		DateRangeEnd:   rangeEnd,
	}

	key := w.archiveKey(metadata)

	expected := "archives/audit-logs/550e8400-e29b-41d4-a716-446655440000/2025/06/aabbccdd-0011-2233-4455-667788990000/audit_logs_2025_06.jsonl.gz"
	assert.Equal(t, expected, key)
}

// --- State Machine Tests ---

func TestArchivePartition_PendingToComplete_Success(t *testing.T) {
	t.Parallel()

	deps := setupTestDeps(t)
	defer deps.ctrl.Finish()

	w, err := NewArchivalWorker(
		deps.archiveRepo,
		deps.partitionMgr,
		deps.storage,
		deps.db,
		deps.provider,
		deps.cfg,
		deps.logger,
	)
	require.NoError(t, err)

	tenantID := uuid.MustParse("550e8400-e29b-41d4-a716-446655440000")
	ctx := context.WithValue(context.Background(), auth.TenantIDKey, tenantID.String())

	metadata := &entities.ArchiveMetadata{
		ID:             uuid.New(),
		TenantID:       tenantID,
		PartitionName:  "audit_logs_2015_01",
		DateRangeStart: time.Date(2024, 1, 1, 0, 0, 0, 0, time.UTC),
		DateRangeEnd:   time.Date(2024, 2, 1, 0, 0, 0, 0, time.UTC),
		Status:         entities.StatusPending,
	}

	// Expect Update calls for each state transition.
	deps.archiveRepo.EXPECT().Update(gomock.Any(), gomock.Any()).Return(nil).AnyTimes()

	// Setup DB expectations for export query.
	deps.sqlMock.ExpectBegin()
	deps.sqlMock.ExpectExec("SET LOCAL search_path").WillReturnResult(sqlmock.NewResult(0, 0))
	deps.sqlMock.ExpectQuery("SELECT id, tenant_id, entity_type, entity_id").
		WillReturnRows(
			sqlmock.NewRows([]string{
				"id", "tenant_id", "entity_type", "entity_id", "action",
				"actor_id", "changes", "created_at", "tenant_seq",
				"prev_hash", "record_hash", "hash_version",
			}).AddRow(
				uuid.New().String(), tenantID.String(), "transaction", uuid.New().String(), "create",
				nil, nil, time.Now().UTC(), int64(1),
				nil, nil, nil,
			),
		)
	deps.sqlMock.ExpectCommit()

	// Capture the uploaded archive content so we can return it from Download.
	var uploadedContent []byte

	deps.storage.EXPECT().
		UploadWithOptions(gomock.Any(), gomock.Any(), gomock.Any(), archiveContentType, gomock.Any()).
		DoAndReturn(func(_ context.Context, _ string, reader io.Reader, _ string, _ ...interface{}) (string, error) {
			var buf bytes.Buffer
			_, copyErr := io.Copy(&buf, reader)
			require.NoError(t, copyErr)
			uploadedContent = buf.Bytes()

			return "archives/key", nil
		})

	// Expect Download for verification: return the same content that was uploaded.
	deps.storage.EXPECT().Download(gomock.Any(), gomock.Any()).
		DoAndReturn(func(_ context.Context, _ string) (io.ReadCloser, error) {
			return io.NopCloser(bytes.NewReader(uploadedContent)), nil
		})

	// Setup partition manager DB expectations for DetachPartition.
	deps.pmMock.ExpectBegin()
	deps.pmMock.ExpectExec("SET LOCAL search_path").WillReturnResult(sqlmock.NewResult(0, 0))
	deps.pmMock.ExpectExec(regexp.QuoteMeta("ALTER TABLE audit_logs DETACH PARTITION audit_logs_2015_01")).
		WillReturnResult(sqlmock.NewResult(0, 0))
	deps.pmMock.ExpectCommit()

	// Setup partition manager DB expectations for DropPartition.
	deps.pmMock.ExpectBegin()
	deps.pmMock.ExpectExec("SET LOCAL search_path").WillReturnResult(sqlmock.NewResult(0, 0))
	deps.pmMock.ExpectExec(regexp.QuoteMeta("DROP TABLE IF EXISTS audit_logs_2015_01")).
		WillReturnResult(sqlmock.NewResult(0, 0))
	deps.pmMock.ExpectCommit()

	err = w.archivePartition(ctx, metadata)
	assert.NoError(t, err)
	assert.NoError(t, deps.pmMock.ExpectationsWereMet())
}

func TestArchivePartition_PendingToDetachError(t *testing.T) {
	t.Parallel()

	deps := setupTestDeps(t)
	defer deps.ctrl.Finish()

	w, err := NewArchivalWorker(
		deps.archiveRepo,
		deps.partitionMgr,
		deps.storage,
		deps.db,
		deps.provider,
		deps.cfg,
		deps.logger,
	)
	require.NoError(t, err)

	tenantID := uuid.MustParse("550e8400-e29b-41d4-a716-446655440000")
	ctx := context.WithValue(context.Background(), auth.TenantIDKey, tenantID.String())

	metadataID := uuid.New()
	metadata := &entities.ArchiveMetadata{
		ID:             metadataID,
		TenantID:       tenantID,
		PartitionName:  "audit_logs_2015_01",
		DateRangeStart: time.Date(2024, 1, 1, 0, 0, 0, 0, time.UTC),
		DateRangeEnd:   time.Date(2024, 2, 1, 0, 0, 0, 0, time.UTC),
		Status:         entities.StatusPending,
	}

	// Expect Update calls for each state transition.
	deps.archiveRepo.EXPECT().Update(gomock.Any(), gomock.Any()).Return(nil).AnyTimes()
	// Expect GetByID after handlePartitionError reloads metadata.
	deps.archiveRepo.EXPECT().GetByID(gomock.Any(), metadataID).Return(metadata, nil).AnyTimes()

	// Setup DB expectations for export query.
	deps.sqlMock.ExpectBegin()
	deps.sqlMock.ExpectExec("SET LOCAL search_path").WillReturnResult(sqlmock.NewResult(0, 0))
	deps.sqlMock.ExpectQuery("SELECT id, tenant_id, entity_type, entity_id").
		WillReturnRows(
			sqlmock.NewRows([]string{
				"id", "tenant_id", "entity_type", "entity_id", "action",
				"actor_id", "changes", "created_at", "tenant_seq",
				"prev_hash", "record_hash", "hash_version",
			}).AddRow(
				uuid.New().String(), tenantID.String(), "transaction", uuid.New().String(), "create",
				nil, nil, time.Now().UTC(), int64(1),
				nil, nil, nil,
			),
		)
	deps.sqlMock.ExpectCommit()

	// Capture the uploaded archive content so we can return it from Download.
	var uploadedContent []byte

	deps.storage.EXPECT().
		UploadWithOptions(gomock.Any(), gomock.Any(), gomock.Any(), archiveContentType, gomock.Any()).
		DoAndReturn(func(_ context.Context, _ string, reader io.Reader, _ string, _ ...interface{}) (string, error) {
			var buf bytes.Buffer
			_, copyErr := io.Copy(&buf, reader)
			require.NoError(t, copyErr)
			uploadedContent = buf.Bytes()

			return "archives/key", nil
		})

	// Expect Download for verification.
	deps.storage.EXPECT().Download(gomock.Any(), gomock.Any()).
		DoAndReturn(func(_ context.Context, _ string) (io.ReadCloser, error) {
			return io.NopCloser(bytes.NewReader(uploadedContent)), nil
		})

	// Setup partition manager to fail at detach.
	deps.pmMock.ExpectBegin()
	deps.pmMock.ExpectExec("SET LOCAL search_path").WillReturnResult(sqlmock.NewResult(0, 0))
	deps.pmMock.ExpectExec("ALTER TABLE audit_logs DETACH PARTITION").
		WillReturnError(errors.New("partition does not exist"))
	deps.pmMock.ExpectRollback()

	err = w.archivePartition(ctx, metadata)
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "detach")
}

func TestArchivePartition_ResumesFromExported_Success(t *testing.T) {
	t.Parallel()

	deps := setupTestDeps(t)
	defer deps.ctrl.Finish()

	w, err := NewArchivalWorker(
		deps.archiveRepo,
		deps.partitionMgr,
		deps.storage,
		deps.db,
		deps.provider,
		deps.cfg,
		deps.logger,
	)
	require.NoError(t, err)

	tenantID := uuid.MustParse("550e8400-e29b-41d4-a716-446655440000")
	ctx := context.WithValue(context.Background(), auth.TenantIDKey, tenantID.String())

	// Start from EXPORTED state -- simulates crash recovery.
	// RowCount=1 matches the single row returned by the mock DB query.
	metadata := &entities.ArchiveMetadata{
		ID:             uuid.New(),
		TenantID:       tenantID,
		PartitionName:  "audit_logs_2015_01",
		DateRangeStart: time.Date(2024, 1, 1, 0, 0, 0, 0, time.UTC),
		DateRangeEnd:   time.Date(2024, 2, 1, 0, 0, 0, 0, time.UTC),
		Status:         entities.StatusExported,
		RowCount:       1,
	}

	deps.archiveRepo.EXPECT().Update(gomock.Any(), gomock.Any()).Return(nil).AnyTimes()

	// Expect re-export for upload (since buffer is nil on resume).
	deps.sqlMock.ExpectBegin()
	deps.sqlMock.ExpectExec("SET LOCAL search_path").WillReturnResult(sqlmock.NewResult(0, 0))
	deps.sqlMock.ExpectQuery("SELECT id, tenant_id, entity_type, entity_id").
		WillReturnRows(
			sqlmock.NewRows([]string{
				"id", "tenant_id", "entity_type", "entity_id", "action",
				"actor_id", "changes", "created_at", "tenant_seq",
				"prev_hash", "record_hash", "hash_version",
			}).AddRow(
				uuid.New().String(), tenantID.String(), "transaction", uuid.New().String(), "update",
				nil, nil, time.Now().UTC(), int64(1),
				nil, nil, nil,
			),
		)
	deps.sqlMock.ExpectCommit()

	// Capture the uploaded archive content so we can return it from Download.
	var uploadedContent []byte

	deps.storage.EXPECT().
		UploadWithOptions(gomock.Any(), gomock.Any(), gomock.Any(), archiveContentType, gomock.Any()).
		DoAndReturn(func(_ context.Context, _ string, reader io.Reader, _ string, _ ...interface{}) (string, error) {
			var buf bytes.Buffer
			_, copyErr := io.Copy(&buf, reader)
			require.NoError(t, copyErr)
			uploadedContent = buf.Bytes()

			return "archives/key", nil
		})

	deps.storage.EXPECT().Download(gomock.Any(), gomock.Any()).
		DoAndReturn(func(_ context.Context, _ string) (io.ReadCloser, error) {
			return io.NopCloser(bytes.NewReader(uploadedContent)), nil
		})

	// Setup partition manager DB expectations for DetachPartition.
	deps.pmMock.ExpectBegin()
	deps.pmMock.ExpectExec("SET LOCAL search_path").WillReturnResult(sqlmock.NewResult(0, 0))
	deps.pmMock.ExpectExec(regexp.QuoteMeta("ALTER TABLE audit_logs DETACH PARTITION audit_logs_2015_01")).
		WillReturnResult(sqlmock.NewResult(0, 0))
	deps.pmMock.ExpectCommit()

	// Setup partition manager DB expectations for DropPartition.
	deps.pmMock.ExpectBegin()
	deps.pmMock.ExpectExec("SET LOCAL search_path").WillReturnResult(sqlmock.NewResult(0, 0))
	deps.pmMock.ExpectExec(regexp.QuoteMeta("DROP TABLE IF EXISTS audit_logs_2015_01")).
		WillReturnResult(sqlmock.NewResult(0, 0))
	deps.pmMock.ExpectCommit()

	err = w.archivePartition(ctx, metadata)
	assert.NoError(t, err)
	assert.NoError(t, deps.pmMock.ExpectationsWereMet())
}

func TestArchivePartition_ResumesFromExported_DetachError(t *testing.T) {
	t.Parallel()

	deps := setupTestDeps(t)
	defer deps.ctrl.Finish()

	w, err := NewArchivalWorker(
		deps.archiveRepo,
		deps.partitionMgr,
		deps.storage,
		deps.db,
		deps.provider,
		deps.cfg,
		deps.logger,
	)
	require.NoError(t, err)

	tenantID := uuid.MustParse("550e8400-e29b-41d4-a716-446655440000")
	ctx := context.WithValue(context.Background(), auth.TenantIDKey, tenantID.String())

	metadataID := uuid.New()
	// Start from EXPORTED state -- simulates crash recovery.
	// RowCount=1 matches the single row returned by the mock DB query.
	metadata := &entities.ArchiveMetadata{
		ID:             metadataID,
		TenantID:       tenantID,
		PartitionName:  "audit_logs_2015_01",
		DateRangeStart: time.Date(2024, 1, 1, 0, 0, 0, 0, time.UTC),
		DateRangeEnd:   time.Date(2024, 2, 1, 0, 0, 0, 0, time.UTC),
		Status:         entities.StatusExported,
		RowCount:       1,
	}

	deps.archiveRepo.EXPECT().Update(gomock.Any(), gomock.Any()).Return(nil).AnyTimes()
	deps.archiveRepo.EXPECT().GetByID(gomock.Any(), metadataID).Return(metadata, nil).AnyTimes()

	// Expect re-export for upload (since buffer is nil on resume).
	deps.sqlMock.ExpectBegin()
	deps.sqlMock.ExpectExec("SET LOCAL search_path").WillReturnResult(sqlmock.NewResult(0, 0))
	deps.sqlMock.ExpectQuery("SELECT id, tenant_id, entity_type, entity_id").
		WillReturnRows(
			sqlmock.NewRows([]string{
				"id", "tenant_id", "entity_type", "entity_id", "action",
				"actor_id", "changes", "created_at", "tenant_seq",
				"prev_hash", "record_hash", "hash_version",
			}).AddRow(
				uuid.New().String(), tenantID.String(), "transaction", uuid.New().String(), "update",
				nil, nil, time.Now().UTC(), int64(1),
				nil, nil, nil,
			),
		)
	deps.sqlMock.ExpectCommit()

	// Capture the uploaded archive content so we can return it from Download.
	var uploadedContent []byte

	deps.storage.EXPECT().
		UploadWithOptions(gomock.Any(), gomock.Any(), gomock.Any(), archiveContentType, gomock.Any()).
		DoAndReturn(func(_ context.Context, _ string, reader io.Reader, _ string, _ ...interface{}) (string, error) {
			var buf bytes.Buffer
			_, copyErr := io.Copy(&buf, reader)
			require.NoError(t, copyErr)
			uploadedContent = buf.Bytes()

			return "archives/key", nil
		})

	deps.storage.EXPECT().Download(gomock.Any(), gomock.Any()).
		DoAndReturn(func(_ context.Context, _ string) (io.ReadCloser, error) {
			return io.NopCloser(bytes.NewReader(uploadedContent)), nil
		})

	// Partition manager detach fails.
	deps.pmMock.ExpectBegin()
	deps.pmMock.ExpectExec("SET LOCAL search_path").WillReturnResult(sqlmock.NewResult(0, 0))
	deps.pmMock.ExpectExec("ALTER TABLE audit_logs DETACH PARTITION").
		WillReturnError(errors.New("partition does not exist"))
	deps.pmMock.ExpectRollback()

	err = w.archivePartition(ctx, metadata)
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "detach")
}

func TestArchivePartition_ErrorMarksMetadata(t *testing.T) {
	t.Parallel()

	deps := setupTestDeps(t)
	defer deps.ctrl.Finish()

	w, err := NewArchivalWorker(
		deps.archiveRepo,
		deps.partitionMgr,
		deps.storage,
		deps.db,
		deps.provider,
		deps.cfg,
		deps.logger,
	)
	require.NoError(t, err)

	tenantID := uuid.MustParse("550e8400-e29b-41d4-a716-446655440000")
	ctx := context.WithValue(context.Background(), auth.TenantIDKey, tenantID.String())

	metadataID := uuid.New()
	metadata := &entities.ArchiveMetadata{
		ID:             metadataID,
		TenantID:       tenantID,
		PartitionName:  "audit_logs_2015_01",
		DateRangeStart: time.Date(2024, 1, 1, 0, 0, 0, 0, time.UTC),
		DateRangeEnd:   time.Date(2024, 2, 1, 0, 0, 0, 0, time.UTC),
		Status:         entities.StatusExporting,
	}

	// The export will fail because the DB query fails.
	deps.sqlMock.ExpectBegin()
	deps.sqlMock.ExpectExec("SET LOCAL search_path").WillReturnResult(sqlmock.NewResult(0, 0))
	deps.sqlMock.ExpectQuery("SELECT id, tenant_id").
		WillReturnError(errors.New("db connection lost"))
	deps.sqlMock.ExpectRollback()

	// Expect the error to be persisted.
	deps.archiveRepo.EXPECT().Update(gomock.Any(), gomock.Any()).
		DoAndReturn(func(_ context.Context, m *entities.ArchiveMetadata) error {
			assert.NotEmpty(t, m.ErrorMessage, "error message should be set")
			return nil
		})

	// Expect metadata reload after error.
	deps.archiveRepo.EXPECT().GetByID(gomock.Any(), metadataID).Return(metadata, nil)

	err = w.archivePartition(ctx, metadata)
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "export partition")
}

func TestArchivePartition_ChecksumVerificationFailure(t *testing.T) {
	t.Parallel()

	deps := setupTestDeps(t)
	defer deps.ctrl.Finish()

	w, err := NewArchivalWorker(
		deps.archiveRepo,
		deps.partitionMgr,
		deps.storage,
		deps.db,
		deps.provider,
		deps.cfg,
		deps.logger,
	)
	require.NoError(t, err)

	tenantID := uuid.MustParse("550e8400-e29b-41d4-a716-446655440000")
	ctx := context.WithValue(context.Background(), auth.TenantIDKey, tenantID.String())

	metadataID := uuid.New()
	// Start from VERIFYING state with empty checksum.
	metadata := &entities.ArchiveMetadata{
		ID:             metadataID,
		TenantID:       tenantID,
		PartitionName:  "audit_logs_2015_01",
		DateRangeStart: time.Date(2024, 1, 1, 0, 0, 0, 0, time.UTC),
		DateRangeEnd:   time.Date(2024, 2, 1, 0, 0, 0, 0, time.UTC),
		Status:         entities.StatusVerifying,
		RowCount:       100,
		ArchiveKey:     "archives/test",
		Checksum:       "", // Empty checksum triggers verification failure.
	}

	// Expect error to be persisted.
	deps.archiveRepo.EXPECT().Update(gomock.Any(), gomock.Any()).Return(nil)
	deps.archiveRepo.EXPECT().GetByID(gomock.Any(), metadataID).Return(metadata, nil)

	err = w.archivePartition(ctx, metadata)
	assert.Error(t, err)
	assert.ErrorIs(t, err, ErrChecksumMismatch)
}

func TestArchivePartition_StorageDownloadFailure(t *testing.T) {
	t.Parallel()

	deps := setupTestDeps(t)
	defer deps.ctrl.Finish()

	w, err := NewArchivalWorker(
		deps.archiveRepo,
		deps.partitionMgr,
		deps.storage,
		deps.db,
		deps.provider,
		deps.cfg,
		deps.logger,
	)
	require.NoError(t, err)

	tenantID := uuid.MustParse("550e8400-e29b-41d4-a716-446655440000")
	ctx := context.WithValue(context.Background(), auth.TenantIDKey, tenantID.String())

	metadataID := uuid.New()
	metadata := &entities.ArchiveMetadata{
		ID:             metadataID,
		TenantID:       tenantID,
		PartitionName:  "audit_logs_2015_01",
		DateRangeStart: time.Date(2024, 1, 1, 0, 0, 0, 0, time.UTC),
		DateRangeEnd:   time.Date(2024, 2, 1, 0, 0, 0, 0, time.UTC),
		Status:         entities.StatusVerifying,
		RowCount:       100,
		ArchiveKey:     "archives/test",
		Checksum:       "abc123",
	}

	// Storage download fails.
	deps.storage.EXPECT().Download(gomock.Any(), "archives/test").
		Return(nil, errors.New("object not found"))

	deps.archiveRepo.EXPECT().Update(gomock.Any(), gomock.Any()).Return(nil)
	deps.archiveRepo.EXPECT().GetByID(gomock.Any(), metadataID).Return(metadata, nil)

	err = w.archivePartition(ctx, metadata)
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "download archive")
}

// --- Tenant Iteration Tests ---

func TestListTenants_Success(t *testing.T) {
	t.Parallel()

	deps := setupTestDeps(t)
	defer deps.ctrl.Finish()

	w, err := NewArchivalWorker(
		deps.archiveRepo,
		deps.partitionMgr,
		deps.storage,
		deps.db,
		deps.provider,
		deps.cfg,
		deps.logger,
	)
	require.NoError(t, err)

	deps.sqlMock.ExpectQuery("SELECT nspname FROM pg_namespace").
		WillReturnRows(
			sqlmock.NewRows([]string{"nspname"}).
				AddRow("550e8400-e29b-41d4-a716-446655440000").
				AddRow("660e8400-e29b-41d4-a716-446655440001"),
		)

	tenants, err := w.listTenants(context.Background())
	assert.NoError(t, err)
	assert.Len(t, tenants, 3)
	assert.Equal(t, "550e8400-e29b-41d4-a716-446655440000", tenants[0])
	assert.Equal(t, "660e8400-e29b-41d4-a716-446655440001", tenants[1])
	assert.Contains(t, tenants, auth.DefaultTenantID, "default tenant must be included")
}

func TestListTenants_DefaultTenantAlreadyPresent(t *testing.T) {
	t.Parallel()

	deps := setupTestDeps(t)
	defer deps.ctrl.Finish()

	w, err := NewArchivalWorker(
		deps.archiveRepo,
		deps.partitionMgr,
		deps.storage,
		deps.db,
		deps.provider,
		deps.cfg,
		deps.logger,
	)
	require.NoError(t, err)

	deps.sqlMock.ExpectQuery("SELECT nspname FROM pg_namespace").
		WillReturnRows(
			sqlmock.NewRows([]string{"nspname"}).
				AddRow(auth.DefaultTenantID).
				AddRow("550e8400-e29b-41d4-a716-446655440000"),
		)

	tenants, err := w.listTenants(context.Background())
	assert.NoError(t, err)
	assert.Len(t, tenants, 2, "default tenant already in DB must not be duplicated")
	assert.Contains(t, tenants, auth.DefaultTenantID)
}

func TestListTenants_DBError(t *testing.T) {
	t.Parallel()

	deps := setupTestDeps(t)
	defer deps.ctrl.Finish()

	w, err := NewArchivalWorker(
		deps.archiveRepo,
		deps.partitionMgr,
		deps.storage,
		deps.db,
		deps.provider,
		deps.cfg,
		deps.logger,
	)
	require.NoError(t, err)

	deps.sqlMock.ExpectQuery("SELECT nspname FROM pg_namespace").
		WillReturnError(errors.New("connection refused"))

	tenants, err := w.listTenants(context.Background())
	assert.Error(t, err)
	assert.Nil(t, tenants)
}

// --- Resume Incomplete Tests ---

func TestResumeIncomplete_SkipsOnError(t *testing.T) {
	t.Parallel()

	deps := setupTestDeps(t)
	defer deps.ctrl.Finish()

	w, err := NewArchivalWorker(
		deps.archiveRepo,
		deps.partitionMgr,
		deps.storage,
		deps.db,
		deps.provider,
		deps.cfg,
		deps.logger,
	)
	require.NoError(t, err)

	deps.archiveRepo.EXPECT().ListIncomplete(gomock.Any()).
		Return(nil, errors.New("db error"))

	// Should not panic.
	w.resumeIncomplete(context.Background())
}

func TestResumeIncomplete_ProcessesIncomplete(t *testing.T) {
	t.Parallel()

	deps := setupTestDeps(t)
	defer deps.ctrl.Finish()

	w, err := NewArchivalWorker(
		deps.archiveRepo,
		deps.partitionMgr,
		deps.storage,
		deps.db,
		deps.provider,
		deps.cfg,
		deps.logger,
	)
	require.NoError(t, err)

	tenantID := uuid.MustParse("550e8400-e29b-41d4-a716-446655440000")

	incompleteArchive := &entities.ArchiveMetadata{
		ID:             uuid.New(),
		TenantID:       tenantID,
		PartitionName:  "audit_logs_2015_01",
		DateRangeStart: time.Date(2024, 1, 1, 0, 0, 0, 0, time.UTC),
		DateRangeEnd:   time.Date(2024, 2, 1, 0, 0, 0, 0, time.UTC),
		Status:         entities.StatusVerifying,
		RowCount:       50,
		ArchiveKey:     "archives/test",
		Checksum:       "validchecksum",
	}

	deps.archiveRepo.EXPECT().ListIncomplete(gomock.Any()).
		Return([]*entities.ArchiveMetadata{incompleteArchive}, nil)

	// Build a valid verification archive matching the stored checksum and row count.
	archiveReader, checksum, _ := buildVerificationArchive(t, 50)
	incompleteArchive.Checksum = checksum
	incompleteArchive.RowCount = 50

	deps.storage.EXPECT().Download(gomock.Any(), "archives/test").Return(archiveReader, nil)

	// Expect state transitions.
	deps.archiveRepo.EXPECT().Update(gomock.Any(), gomock.Any()).Return(nil).AnyTimes()
	// After verification succeeds and detach fails, handlePartitionError reloads metadata.
	deps.archiveRepo.EXPECT().GetByID(gomock.Any(), incompleteArchive.ID).Return(incompleteArchive, nil).AnyTimes()

	w.resumeIncomplete(context.Background())
}

// --- ArchiveCycle Lock Skip Test ---

func TestArchiveCycle_SkipsWhenLockNotAcquired(t *testing.T) {
	t.Parallel()

	deps := setupTestDeps(t)
	defer deps.ctrl.Finish()

	w, err := NewArchivalWorker(
		deps.archiveRepo,
		deps.partitionMgr,
		deps.storage,
		deps.db,
		deps.provider,
		deps.cfg,
		deps.logger,
	)
	require.NoError(t, err)

	// Pre-acquire the lock so the worker can't get it.
	deps.miniRedis.Set(lockKey, "someone-else")

	// The worker should skip the cycle without calling any repo methods.
	// (No EXPECT calls for archiveRepo, which means gomock will fail if called.)

	w.archiveCycle(context.Background())
}

// --- Tracking Tests ---

func TestTracking_FallsBackToInstanceLogger(t *testing.T) {
	t.Parallel()

	deps := setupTestDeps(t)
	defer deps.ctrl.Finish()

	w, err := NewArchivalWorker(
		deps.archiveRepo,
		deps.partitionMgr,
		deps.storage,
		deps.db,
		deps.provider,
		deps.cfg,
		deps.logger,
	)
	require.NoError(t, err)

	l, tr := w.tracking(context.Background())
	assert.NotNil(t, l)
	assert.NotNil(t, tr)
}
