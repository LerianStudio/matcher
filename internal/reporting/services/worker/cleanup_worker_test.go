//go:build unit

package worker

import (
	"context"
	"errors"
	"sync/atomic"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.uber.org/mock/gomock"

	libLog "github.com/LerianStudio/lib-commons/v4/commons/log"

	"github.com/LerianStudio/matcher/internal/reporting/domain/entities"
	repomocks "github.com/LerianStudio/matcher/internal/reporting/domain/repositories/mocks"
	portsmocks "github.com/LerianStudio/matcher/internal/reporting/ports/mocks"
)

var (
	errTestDatabaseError = errors.New("database error")
	errTestStorageError  = errors.New("storage error")
	errTestUpdateError   = errors.New("update error")
)

func waitForCondition(t *testing.T, timeout, interval time.Duration, condition func() bool) {
	t.Helper()

	deadline := time.Now().Add(timeout)

	for {
		if condition() {
			return
		}

		if time.Now().After(deadline) {
			require.Fail(t, "condition not met before timeout")
			return
		}

		time.Sleep(interval)
	}
}

func setupCleanupWorkerMocks(
	t *testing.T,
) (*repomocks.MockExportJobRepository, *portsmocks.MockObjectStorageClient, CleanupWorkerConfig, *libLog.NopLogger) {
	t.Helper()

	ctrl := gomock.NewController(t)
	jobRepo := repomocks.NewMockExportJobRepository(ctrl)
	storage := portsmocks.NewMockObjectStorageClient(ctrl)
	cfg := CleanupWorkerConfig{
		Interval:              50 * time.Millisecond,
		BatchSize:             10,
		FileDeleteGracePeriod: 1 * time.Hour,
	}
	logger := &libLog.NopLogger{}

	return jobRepo, storage, cfg, logger
}

func startAndWaitWorker(t *testing.T, worker *CleanupWorker, condition func() bool) {
	t.Helper()

	ctx := context.Background()
	err := worker.Start(ctx)
	require.NoError(t, err)

	waitForCondition(t, 500*time.Millisecond, 10*time.Millisecond, condition)

	err = worker.Stop()
	require.NoError(t, err)
}

func TestNewCleanupWorker_ValidDependencies(t *testing.T) {
	t.Parallel()

	ctrl := gomock.NewController(t)
	jobRepo := repomocks.NewMockExportJobRepository(ctrl)
	storage := portsmocks.NewMockObjectStorageClient(ctrl)
	cfg := CleanupWorkerConfig{}
	logger := &libLog.NopLogger{}

	worker, err := NewCleanupWorker(jobRepo, storage, cfg, logger)

	require.NoError(t, err)
	assert.NotNil(t, worker)
	assert.NotNil(t, worker.stopCh)
	assert.NotNil(t, worker.doneCh)
}

func TestNewCleanupWorker_NilJobRepository(t *testing.T) {
	t.Parallel()

	ctrl := gomock.NewController(t)
	storage := portsmocks.NewMockObjectStorageClient(ctrl)
	cfg := CleanupWorkerConfig{}
	logger := &libLog.NopLogger{}

	worker, err := NewCleanupWorker(nil, storage, cfg, logger)

	require.Error(t, err)
	assert.Nil(t, worker)
	require.ErrorIs(t, err, ErrNilJobRepository)
}

func TestNewCleanupWorker_NilStorageClient(t *testing.T) {
	t.Parallel()

	ctrl := gomock.NewController(t)
	jobRepo := repomocks.NewMockExportJobRepository(ctrl)
	cfg := CleanupWorkerConfig{}
	logger := &libLog.NopLogger{}

	worker, err := NewCleanupWorker(jobRepo, nil, cfg, logger)

	require.Error(t, err)
	assert.Nil(t, worker)
	require.ErrorIs(t, err, ErrNilStorageClient)
}

func TestNewCleanupWorker_DefaultIntervalWhenZero(t *testing.T) {
	t.Parallel()

	ctrl := gomock.NewController(t)
	jobRepo := repomocks.NewMockExportJobRepository(ctrl)
	storage := portsmocks.NewMockObjectStorageClient(ctrl)
	cfg := CleanupWorkerConfig{Interval: 0}
	logger := &libLog.NopLogger{}
	worker, err := NewCleanupWorker(jobRepo, storage, cfg, logger)

	require.NoError(t, err)
	assert.Equal(t, defaultCleanupInterval, worker.cfg.Interval)
}

func TestNewCleanupWorker_DefaultIntervalWhenNegative(t *testing.T) {
	t.Parallel()

	ctrl := gomock.NewController(t)
	jobRepo := repomocks.NewMockExportJobRepository(ctrl)
	storage := portsmocks.NewMockObjectStorageClient(ctrl)
	cfg := CleanupWorkerConfig{Interval: -1 * time.Hour}
	logger := &libLog.NopLogger{}
	worker, err := NewCleanupWorker(jobRepo, storage, cfg, logger)

	require.NoError(t, err)
	assert.Equal(t, defaultCleanupInterval, worker.cfg.Interval)
}

func TestNewCleanupWorker_DefaultBatchSizeWhenZero(t *testing.T) {
	t.Parallel()

	ctrl := gomock.NewController(t)
	jobRepo := repomocks.NewMockExportJobRepository(ctrl)
	storage := portsmocks.NewMockObjectStorageClient(ctrl)
	cfg := CleanupWorkerConfig{BatchSize: 0}
	logger := &libLog.NopLogger{}
	worker, err := NewCleanupWorker(jobRepo, storage, cfg, logger)

	require.NoError(t, err)
	assert.Equal(t, defaultCleanupBatch, worker.cfg.BatchSize)
}

func TestNewCleanupWorker_DefaultBatchSizeWhenNegative(t *testing.T) {
	t.Parallel()

	ctrl := gomock.NewController(t)
	jobRepo := repomocks.NewMockExportJobRepository(ctrl)
	storage := portsmocks.NewMockObjectStorageClient(ctrl)
	cfg := CleanupWorkerConfig{BatchSize: -10}
	logger := &libLog.NopLogger{}
	worker, err := NewCleanupWorker(jobRepo, storage, cfg, logger)

	require.NoError(t, err)
	assert.Equal(t, defaultCleanupBatch, worker.cfg.BatchSize)
}

func TestNewCleanupWorker_CustomIntervalWhenPositive(t *testing.T) {
	t.Parallel()

	ctrl := gomock.NewController(t)
	jobRepo := repomocks.NewMockExportJobRepository(ctrl)
	storage := portsmocks.NewMockObjectStorageClient(ctrl)
	cfg := CleanupWorkerConfig{Interval: 30 * time.Minute}
	logger := &libLog.NopLogger{}
	worker, err := NewCleanupWorker(jobRepo, storage, cfg, logger)

	require.NoError(t, err)
	assert.Equal(t, 30*time.Minute, worker.cfg.Interval)
}

func TestNewCleanupWorker_CustomBatchSizeWhenPositive(t *testing.T) {
	t.Parallel()

	ctrl := gomock.NewController(t)
	jobRepo := repomocks.NewMockExportJobRepository(ctrl)
	storage := portsmocks.NewMockObjectStorageClient(ctrl)
	cfg := CleanupWorkerConfig{BatchSize: 50}
	logger := &libLog.NopLogger{}
	worker, err := NewCleanupWorker(jobRepo, storage, cfg, logger)

	require.NoError(t, err)
	assert.Equal(t, 50, worker.cfg.BatchSize)
}

func TestNewCleanupWorker_DefaultGracePeriodWhenZero(t *testing.T) {
	t.Parallel()

	ctrl := gomock.NewController(t)
	jobRepo := repomocks.NewMockExportJobRepository(ctrl)
	storage := portsmocks.NewMockObjectStorageClient(ctrl)
	cfg := CleanupWorkerConfig{FileDeleteGracePeriod: 0}
	logger := &libLog.NopLogger{}
	worker, err := NewCleanupWorker(jobRepo, storage, cfg, logger)

	require.NoError(t, err)
	assert.Equal(t, defaultFileDeleteGrace, worker.cfg.FileDeleteGracePeriod)
}

func TestNewCleanupWorker_DefaultGracePeriodWhenNegative(t *testing.T) {
	t.Parallel()

	ctrl := gomock.NewController(t)
	jobRepo := repomocks.NewMockExportJobRepository(ctrl)
	storage := portsmocks.NewMockObjectStorageClient(ctrl)
	cfg := CleanupWorkerConfig{FileDeleteGracePeriod: -30 * time.Minute}
	logger := &libLog.NopLogger{}
	worker, err := NewCleanupWorker(jobRepo, storage, cfg, logger)

	require.NoError(t, err)
	assert.Equal(t, defaultFileDeleteGrace, worker.cfg.FileDeleteGracePeriod)
}

func TestNewCleanupWorker_CustomGracePeriodWhenPositive(t *testing.T) {
	t.Parallel()

	ctrl := gomock.NewController(t)
	jobRepo := repomocks.NewMockExportJobRepository(ctrl)
	storage := portsmocks.NewMockObjectStorageClient(ctrl)
	cfg := CleanupWorkerConfig{FileDeleteGracePeriod: 2 * time.Hour}
	logger := &libLog.NopLogger{}
	worker, err := NewCleanupWorker(jobRepo, storage, cfg, logger)

	require.NoError(t, err)
	assert.Equal(t, 2*time.Hour, worker.cfg.FileDeleteGracePeriod)
}

func TestNewCleanupWorker_WithNilLogger(t *testing.T) {
	t.Parallel()

	ctrl := gomock.NewController(t)
	jobRepo := repomocks.NewMockExportJobRepository(ctrl)
	storage := portsmocks.NewMockObjectStorageClient(ctrl)
	cfg := CleanupWorkerConfig{}
	worker, err := NewCleanupWorker(jobRepo, storage, cfg, nil)

	require.NoError(t, err)
	assert.NotNil(t, worker)
	assert.Nil(t, worker.logger)
}

func TestCleanupWorker_Start(t *testing.T) {
	t.Parallel()

	t.Run("starts successfully", func(t *testing.T) {
		t.Parallel()

		ctrl := gomock.NewController(t)
		jobRepo := repomocks.NewMockExportJobRepository(ctrl)
		storage := portsmocks.NewMockObjectStorageClient(ctrl)
		cfg := CleanupWorkerConfig{Interval: 100 * time.Millisecond}
		logger := &libLog.NopLogger{}

		jobRepo.EXPECT().
			ListExpired(gomock.Any(), gomock.Any()).
			Return([]*entities.ExportJob{}, nil).
			AnyTimes()

		worker, err := NewCleanupWorker(jobRepo, storage, cfg, logger)
		require.NoError(t, err)

		ctx := context.Background()
		err = worker.Start(ctx)
		require.NoError(t, err)

		waitForCondition(t, 200*time.Millisecond, 5*time.Millisecond, func() bool {
			return worker.running.Load()
		})

		err = worker.Stop()
		require.NoError(t, err)

		waitForCondition(t, 200*time.Millisecond, 5*time.Millisecond, func() bool {
			return !worker.running.Load()
		})
	})

	t.Run("returns error when already running", func(t *testing.T) {
		t.Parallel()

		ctrl := gomock.NewController(t)
		jobRepo := repomocks.NewMockExportJobRepository(ctrl)
		storage := portsmocks.NewMockObjectStorageClient(ctrl)
		cfg := CleanupWorkerConfig{Interval: 100 * time.Millisecond}
		logger := &libLog.NopLogger{}

		jobRepo.EXPECT().
			ListExpired(gomock.Any(), gomock.Any()).
			Return([]*entities.ExportJob{}, nil).
			AnyTimes()

		worker, err := NewCleanupWorker(jobRepo, storage, cfg, logger)
		require.NoError(t, err)

		ctx := context.Background()
		err = worker.Start(ctx)
		require.NoError(t, err)

		waitForCondition(t, 200*time.Millisecond, 5*time.Millisecond, func() bool {
			return worker.running.Load()
		})

		err = worker.Start(ctx)
		require.ErrorIs(t, err, ErrWorkerAlreadyRunning)

		err = worker.Stop()
		require.NoError(t, err)

		waitForCondition(t, 200*time.Millisecond, 5*time.Millisecond, func() bool {
			return !worker.running.Load()
		})
	})
}

func TestCleanupWorker_Stop(t *testing.T) {
	t.Parallel()

	t.Run("stops successfully", func(t *testing.T) {
		t.Parallel()

		ctrl := gomock.NewController(t)
		jobRepo := repomocks.NewMockExportJobRepository(ctrl)
		storage := portsmocks.NewMockObjectStorageClient(ctrl)
		cfg := CleanupWorkerConfig{Interval: 100 * time.Millisecond}
		logger := &libLog.NopLogger{}

		jobRepo.EXPECT().
			ListExpired(gomock.Any(), gomock.Any()).
			Return([]*entities.ExportJob{}, nil).
			AnyTimes()

		worker, err := NewCleanupWorker(jobRepo, storage, cfg, logger)
		require.NoError(t, err)

		ctx := context.Background()
		err = worker.Start(ctx)
		require.NoError(t, err)

		waitForCondition(t, 200*time.Millisecond, 5*time.Millisecond, func() bool {
			return worker.running.Load()
		})

		err = worker.Stop()
		require.NoError(t, err)
		assert.False(t, worker.running.Load())
	})

	t.Run("returns error when not running", func(t *testing.T) {
		t.Parallel()

		ctrl := gomock.NewController(t)
		jobRepo := repomocks.NewMockExportJobRepository(ctrl)
		storage := portsmocks.NewMockObjectStorageClient(ctrl)
		cfg := CleanupWorkerConfig{Interval: 100 * time.Millisecond}
		logger := &libLog.NopLogger{}

		worker, err := NewCleanupWorker(jobRepo, storage, cfg, logger)
		require.NoError(t, err)

		err = worker.Stop()
		require.ErrorIs(t, err, ErrWorkerNotRunning)
	})
}

func TestCleanupWorker_CleanupExpired_WithFiles(t *testing.T) {
	t.Parallel()

	expiredJob := &entities.ExportJob{
		ID:        uuid.New(),
		Status:    entities.ExportJobStatusSucceeded,
		FileKey:   "exports/test-file.csv",
		ExpiresAt: time.Now().UTC().Add(-24 * time.Hour),
	}

	jobRepo, storage, cfg, logger := setupCleanupWorkerMocks(t)

	var updateCalled atomic.Bool

	var listCalls atomic.Int32

	jobRepo.EXPECT().
		ListExpired(gomock.Any(), cfg.BatchSize).
		DoAndReturn(func(context.Context, int) ([]*entities.ExportJob, error) {
			if listCalls.Add(1) == 1 {
				return []*entities.ExportJob{expiredJob}, nil
			}

			return []*entities.ExportJob{}, nil
		}).
		AnyTimes()
	storage.EXPECT().Delete(gomock.Any(), expiredJob.FileKey).Return(nil).AnyTimes()
	jobRepo.EXPECT().
		Update(gomock.Any(), gomock.Any()).
		DoAndReturn(func(context.Context, *entities.ExportJob) error {
			updateCalled.Store(true)
			return nil
		}).
		AnyTimes()

	worker, err := NewCleanupWorker(jobRepo, storage, cfg, logger)
	require.NoError(t, err)

	startAndWaitWorker(t, worker, updateCalled.Load)
	assert.True(t, updateCalled.Load())
}

func TestCleanupWorker_CleanupExpired_EmptyList(t *testing.T) {
	t.Parallel()

	jobRepo, storage, cfg, logger := setupCleanupWorkerMocks(t)

	var listCalled, updateCalled atomic.Bool

	jobRepo.EXPECT().
		ListExpired(gomock.Any(), cfg.BatchSize).
		DoAndReturn(func(context.Context, int) ([]*entities.ExportJob, error) {
			listCalled.Store(true)
			return []*entities.ExportJob{}, nil
		}).
		AnyTimes()
	jobRepo.EXPECT().
		Update(gomock.Any(), gomock.Any()).
		DoAndReturn(func(context.Context, *entities.ExportJob) error {
			updateCalled.Store(true)
			return nil
		}).
		AnyTimes()

	worker, err := NewCleanupWorker(jobRepo, storage, cfg, logger)
	require.NoError(t, err)

	startAndWaitWorker(t, worker, listCalled.Load)
	assert.False(t, updateCalled.Load())
}

func TestCleanupWorker_CleanupExpired_ListError(t *testing.T) {
	t.Parallel()

	jobRepo, storage, cfg, logger := setupCleanupWorkerMocks(t)

	var listCalled atomic.Bool

	jobRepo.EXPECT().
		ListExpired(gomock.Any(), cfg.BatchSize).
		DoAndReturn(func(context.Context, int) ([]*entities.ExportJob, error) {
			listCalled.Store(true)
			return nil, errTestDatabaseError
		}).
		AnyTimes()

	worker, err := NewCleanupWorker(jobRepo, storage, cfg, logger)
	require.NoError(t, err)

	startAndWaitWorker(t, worker, listCalled.Load)
}

func TestCleanupWorker_CleanupExpired_StorageDeleteError(t *testing.T) {
	t.Parallel()

	expiredJob := &entities.ExportJob{
		ID:        uuid.New(),
		Status:    entities.ExportJobStatusSucceeded,
		FileKey:   "exports/test-file.csv",
		ExpiresAt: time.Now().UTC().Add(-24 * time.Hour),
	}

	jobRepo, storage, cfg, logger := setupCleanupWorkerMocks(t)

	var updateCalled atomic.Bool

	jobRepo.EXPECT().
		ListExpired(gomock.Any(), cfg.BatchSize).
		Return([]*entities.ExportJob{expiredJob}, nil).
		AnyTimes()
	storage.EXPECT().Delete(gomock.Any(), expiredJob.FileKey).Return(errTestStorageError).AnyTimes()
	jobRepo.EXPECT().
		Update(gomock.Any(), gomock.Any()).
		DoAndReturn(func(context.Context, *entities.ExportJob) error {
			updateCalled.Store(true)
			return nil
		}).
		AnyTimes()

	worker, err := NewCleanupWorker(jobRepo, storage, cfg, logger)
	require.NoError(t, err)

	startAndWaitWorker(t, worker, updateCalled.Load)
	assert.True(t, updateCalled.Load())
}

func TestCleanupWorker_CleanupExpired_UpdateError(t *testing.T) {
	t.Parallel()

	jobID := uuid.New()
	expiredJob := &entities.ExportJob{
		ID:        jobID,
		Status:    entities.ExportJobStatusSucceeded,
		FileKey:   "exports/test-file.csv",
		ExpiresAt: time.Now().UTC().Add(-24 * time.Hour),
	}

	ctrl := gomock.NewController(t)
	jobRepo := repomocks.NewMockExportJobRepository(ctrl)
	storage := portsmocks.NewMockObjectStorageClient(ctrl)
	cfg := CleanupWorkerConfig{
		Interval:  50 * time.Millisecond,
		BatchSize: 10,
	}
	logger := &libLog.NopLogger{}

	var updateCalled atomic.Bool

	jobRepo.EXPECT().
		ListExpired(gomock.Any(), cfg.BatchSize).
		Return([]*entities.ExportJob{expiredJob}, nil).
		AnyTimes()
	storage.EXPECT().Delete(gomock.Any(), expiredJob.FileKey).Return(nil).AnyTimes()
	jobRepo.EXPECT().
		Update(gomock.Any(), gomock.Any()).
		DoAndReturn(func(context.Context, *entities.ExportJob) error {
			updateCalled.Store(true)
			return errTestUpdateError
		}).
		AnyTimes()

	worker, err := NewCleanupWorker(jobRepo, storage, cfg, logger)
	require.NoError(t, err)

	ctx := context.Background()
	err = worker.Start(ctx)
	require.NoError(t, err)

	waitForCondition(t, 500*time.Millisecond, 10*time.Millisecond, updateCalled.Load)

	err = worker.Stop()
	require.NoError(t, err)
}

func TestCleanupWorker_CleanupExpired_NoFileKey(t *testing.T) {
	t.Parallel()

	jobID := uuid.New()
	expiredJob := &entities.ExportJob{
		ID:        jobID,
		Status:    entities.ExportJobStatusQueued,
		FileKey:   "",
		ExpiresAt: time.Now().UTC().Add(-24 * time.Hour),
	}

	ctrl := gomock.NewController(t)
	jobRepo := repomocks.NewMockExportJobRepository(ctrl)
	storage := portsmocks.NewMockObjectStorageClient(ctrl)
	cfg := CleanupWorkerConfig{
		Interval:  50 * time.Millisecond,
		BatchSize: 10,
	}
	logger := &libLog.NopLogger{}

	var updateCalled atomic.Bool

	jobRepo.EXPECT().
		ListExpired(gomock.Any(), cfg.BatchSize).
		Return([]*entities.ExportJob{expiredJob}, nil).
		AnyTimes()
	storage.EXPECT().Delete(gomock.Any(), gomock.Any()).Times(0)
	jobRepo.EXPECT().
		Update(gomock.Any(), gomock.Any()).
		DoAndReturn(func(context.Context, *entities.ExportJob) error {
			updateCalled.Store(true)
			return nil
		}).
		AnyTimes()

	worker, err := NewCleanupWorker(jobRepo, storage, cfg, logger)
	require.NoError(t, err)

	ctx := context.Background()
	err = worker.Start(ctx)
	require.NoError(t, err)

	waitForCondition(t, 500*time.Millisecond, 10*time.Millisecond, updateCalled.Load)

	err = worker.Stop()
	require.NoError(t, err)

	assert.True(t, updateCalled.Load())
}

func TestCleanupWorker_ContextCancellation(t *testing.T) {
	t.Parallel()

	t.Run("stops on context cancellation", func(t *testing.T) {
		t.Parallel()

		ctrl := gomock.NewController(t)
		jobRepo := repomocks.NewMockExportJobRepository(ctrl)
		storage := portsmocks.NewMockObjectStorageClient(ctrl)
		cfg := CleanupWorkerConfig{
			Interval:  1 * time.Hour,
			BatchSize: 10,
		}
		logger := &libLog.NopLogger{}

		jobRepo.EXPECT().
			ListExpired(gomock.Any(), gomock.Any()).
			Return([]*entities.ExportJob{}, nil).
			AnyTimes()

		worker, err := NewCleanupWorker(jobRepo, storage, cfg, logger)
		require.NoError(t, err)

		ctx, cancel := context.WithCancel(context.Background())
		err = worker.Start(ctx)
		require.NoError(t, err)

		doneCh := worker.Done()

		cancel()

		select {
		case <-doneCh:
		case <-time.After(200 * time.Millisecond):
			require.Fail(t, "expected worker to stop after context cancellation")
		}
	})
}

func TestCleanupWorker_MultipleExpiredJobs(t *testing.T) {
	t.Parallel()

	t.Run("processes multiple expired jobs", func(t *testing.T) {
		t.Parallel()

		expiredJobs := make([]*entities.ExportJob, 5)
		for i := 0; i < 5; i++ {
			expiredJobs[i] = &entities.ExportJob{
				ID:        uuid.New(),
				Status:    entities.ExportJobStatusSucceeded,
				FileKey:   "exports/file-" + uuid.New().String() + ".csv",
				ExpiresAt: time.Now().Add(-time.Duration(i+1) * time.Hour),
			}
		}

		ctrl := gomock.NewController(t)
		jobRepo := repomocks.NewMockExportJobRepository(ctrl)
		storage := portsmocks.NewMockObjectStorageClient(ctrl)
		cfg := CleanupWorkerConfig{
			Interval:  50 * time.Millisecond,
			BatchSize: 10,
		}
		logger := &libLog.NopLogger{}

		var updateCount atomic.Int32

		jobRepo.EXPECT().
			ListExpired(gomock.Any(), cfg.BatchSize).
			Return(expiredJobs, nil).
			AnyTimes()
		storage.EXPECT().Delete(gomock.Any(), gomock.Any()).Return(nil).AnyTimes()
		jobRepo.EXPECT().
			Update(gomock.Any(), gomock.Any()).
			DoAndReturn(func(context.Context, *entities.ExportJob) error {
				updateCount.Add(1)
				return nil
			}).
			AnyTimes()

		worker, err := NewCleanupWorker(jobRepo, storage, cfg, logger)
		require.NoError(t, err)

		ctx := context.Background()
		err = worker.Start(ctx)
		require.NoError(t, err)

		waitForCondition(t, 500*time.Millisecond, 10*time.Millisecond, func() bool {
			return updateCount.Load() > 0
		})

		err = worker.Stop()
		require.NoError(t, err)

		assert.Positive(t, updateCount.Load())
	})
}

func TestCleanupWorker_TickerBehavior(t *testing.T) {
	t.Parallel()

	t.Run("runs cleanup on interval", func(t *testing.T) {
		t.Parallel()

		expiredJobs := []*entities.ExportJob{
			{
				ID:        uuid.New(),
				Status:    entities.ExportJobStatusSucceeded,
				FileKey:   "exports/test.csv",
				ExpiresAt: time.Now().Add(-1 * time.Hour),
			},
		}

		ctrl := gomock.NewController(t)
		jobRepo := repomocks.NewMockExportJobRepository(ctrl)
		storage := portsmocks.NewMockObjectStorageClient(ctrl)
		cfg := CleanupWorkerConfig{
			Interval:  30 * time.Millisecond,
			BatchSize: 10,
		}
		logger := &libLog.NopLogger{}

		var listCalls atomic.Int32

		jobRepo.EXPECT().
			ListExpired(gomock.Any(), cfg.BatchSize).
			DoAndReturn(func(context.Context, int) ([]*entities.ExportJob, error) {
				listCalls.Add(1)
				return expiredJobs, nil
			}).
			AnyTimes()
		storage.EXPECT().Delete(gomock.Any(), gomock.Any()).Return(nil).AnyTimes()
		jobRepo.EXPECT().Update(gomock.Any(), gomock.Any()).Return(nil).AnyTimes()

		worker, err := NewCleanupWorker(jobRepo, storage, cfg, logger)
		require.NoError(t, err)

		ctx := context.Background()
		err = worker.Start(ctx)
		require.NoError(t, err)

		waitForCondition(t, 500*time.Millisecond, 10*time.Millisecond, func() bool {
			return listCalls.Load() >= 2
		})

		err = worker.Stop()
		require.NoError(t, err)

		assert.GreaterOrEqual(t, listCalls.Load(), int32(2))
	})
}

func TestCleanupWorker_Done(t *testing.T) {
	t.Parallel()

	ctrl := gomock.NewController(t)
	jobRepo := repomocks.NewMockExportJobRepository(ctrl)
	storage := portsmocks.NewMockObjectStorageClient(ctrl)
	cfg := CleanupWorkerConfig{
		Interval:  100 * time.Millisecond,
		BatchSize: 10,
	}
	logger := &libLog.NopLogger{}

	jobRepo.EXPECT().
		ListExpired(gomock.Any(), gomock.Any()).
		Return([]*entities.ExportJob{}, nil).
		AnyTimes()

	worker, err := NewCleanupWorker(jobRepo, storage, cfg, logger)
	require.NoError(t, err)

	doneCh := worker.Done()
	assert.NotNil(t, doneCh)

	ctx := context.Background()
	err = worker.Start(ctx)
	require.NoError(t, err)

	err = worker.Stop()
	require.NoError(t, err)

	select {
	case <-doneCh:
	case <-time.After(200 * time.Millisecond):
		require.Fail(t, "done channel should be closed after stop")
	}
}

func TestCleanupWorker_ListExpiredError_ContinuesRunning(t *testing.T) {
	t.Parallel()

	ctrl := gomock.NewController(t)
	jobRepo := repomocks.NewMockExportJobRepository(ctrl)
	storage := portsmocks.NewMockObjectStorageClient(ctrl)
	cfg := CleanupWorkerConfig{
		Interval:  30 * time.Millisecond,
		BatchSize: 10,
	}
	logger := &libLog.NopLogger{}

	var callCount atomic.Int32

	jobRepo.EXPECT().
		ListExpired(gomock.Any(), cfg.BatchSize).
		DoAndReturn(func(context.Context, int) ([]*entities.ExportJob, error) {
			callCount.Add(1)
			return nil, errTestDatabaseError
		}).
		AnyTimes()

	worker, err := NewCleanupWorker(jobRepo, storage, cfg, logger)
	require.NoError(t, err)

	ctx := context.Background()
	err = worker.Start(ctx)
	require.NoError(t, err)

	waitForCondition(t, 300*time.Millisecond, 10*time.Millisecond, func() bool {
		return callCount.Load() >= 2
	})

	err = worker.Stop()
	require.NoError(t, err)

	assert.GreaterOrEqual(t, callCount.Load(), int32(2))
}

func TestCleanupWorker_StopTwice(t *testing.T) {
	t.Parallel()

	ctrl := gomock.NewController(t)
	jobRepo := repomocks.NewMockExportJobRepository(ctrl)
	storage := portsmocks.NewMockObjectStorageClient(ctrl)
	cfg := CleanupWorkerConfig{
		Interval:  100 * time.Millisecond,
		BatchSize: 10,
	}
	logger := &libLog.NopLogger{}

	jobRepo.EXPECT().
		ListExpired(gomock.Any(), gomock.Any()).
		Return([]*entities.ExportJob{}, nil).
		AnyTimes()

	worker, err := NewCleanupWorker(jobRepo, storage, cfg, logger)
	require.NoError(t, err)

	ctx := context.Background()
	err = worker.Start(ctx)
	require.NoError(t, err)

	waitForCondition(t, 200*time.Millisecond, 5*time.Millisecond, func() bool {
		return worker.running.Load()
	})

	err = worker.Stop()
	require.NoError(t, err)

	err = worker.Stop()
	require.ErrorIs(t, err, ErrWorkerNotRunning)
}

func TestCleanupWorker_StartTwice(t *testing.T) {
	t.Parallel()

	ctrl := gomock.NewController(t)
	jobRepo := repomocks.NewMockExportJobRepository(ctrl)
	storage := portsmocks.NewMockObjectStorageClient(ctrl)
	cfg := CleanupWorkerConfig{
		Interval:  100 * time.Millisecond,
		BatchSize: 10,
	}
	logger := &libLog.NopLogger{}

	jobRepo.EXPECT().
		ListExpired(gomock.Any(), gomock.Any()).
		Return([]*entities.ExportJob{}, nil).
		AnyTimes()

	worker, err := NewCleanupWorker(jobRepo, storage, cfg, logger)
	require.NoError(t, err)

	ctx := context.Background()
	err = worker.Start(ctx)
	require.NoError(t, err)

	err = worker.Start(ctx)
	require.ErrorIs(t, err, ErrWorkerAlreadyRunning)

	err = worker.Stop()
	require.NoError(t, err)
}

func TestCleanupWorker_StopBeforeStart(t *testing.T) {
	t.Parallel()

	ctrl := gomock.NewController(t)
	jobRepo := repomocks.NewMockExportJobRepository(ctrl)
	storage := portsmocks.NewMockObjectStorageClient(ctrl)
	cfg := CleanupWorkerConfig{
		Interval:  100 * time.Millisecond,
		BatchSize: 10,
	}
	logger := &libLog.NopLogger{}

	worker, err := NewCleanupWorker(jobRepo, storage, cfg, logger)
	require.NoError(t, err)

	err = worker.Stop()
	require.ErrorIs(t, err, ErrWorkerNotRunning)
}

func TestCleanupWorker_CleanupExpired_EmptyJobsList(t *testing.T) {
	t.Parallel()

	ctrl := gomock.NewController(t)
	jobRepo := repomocks.NewMockExportJobRepository(ctrl)
	storage := portsmocks.NewMockObjectStorageClient(ctrl)
	cfg := CleanupWorkerConfig{
		Interval:  50 * time.Millisecond,
		BatchSize: 10,
	}
	logger := &libLog.NopLogger{}

	var listCalled atomic.Bool

	jobRepo.EXPECT().
		ListExpired(gomock.Any(), cfg.BatchSize).
		DoAndReturn(func(context.Context, int) ([]*entities.ExportJob, error) {
			listCalled.Store(true)
			return []*entities.ExportJob{}, nil
		}).
		AnyTimes()

	storage.EXPECT().Delete(gomock.Any(), gomock.Any()).Times(0)
	jobRepo.EXPECT().Update(gomock.Any(), gomock.Any()).Times(0)

	worker, err := NewCleanupWorker(jobRepo, storage, cfg, logger)
	require.NoError(t, err)

	ctx := context.Background()
	err = worker.Start(ctx)
	require.NoError(t, err)

	waitForCondition(t, 200*time.Millisecond, 10*time.Millisecond, listCalled.Load)

	err = worker.Stop()
	require.NoError(t, err)

	assert.True(t, listCalled.Load())
}

func TestCleanupWorker_GracePeriod_FileNotDeletedWithinGrace(t *testing.T) {
	t.Parallel()

	// Job expired 10 minutes ago; grace period is 1 hour.
	// File should NOT be deleted, but job should be marked as expired.
	expiredJob := &entities.ExportJob{
		ID:        uuid.New(),
		Status:    entities.ExportJobStatusSucceeded,
		FileKey:   "exports/recent-file.csv",
		ExpiresAt: time.Now().UTC().Add(-10 * time.Minute),
	}

	ctrl := gomock.NewController(t)
	jobRepo := repomocks.NewMockExportJobRepository(ctrl)
	storage := portsmocks.NewMockObjectStorageClient(ctrl)
	cfg := CleanupWorkerConfig{
		Interval:              50 * time.Millisecond,
		BatchSize:             10,
		FileDeleteGracePeriod: 1 * time.Hour,
	}
	logger := &libLog.NopLogger{}

	var updateCalled atomic.Bool

	var listCalls atomic.Int32

	jobRepo.EXPECT().
		ListExpired(gomock.Any(), cfg.BatchSize).
		DoAndReturn(func(context.Context, int) ([]*entities.ExportJob, error) {
			if listCalls.Add(1) == 1 {
				return []*entities.ExportJob{expiredJob}, nil
			}

			return []*entities.ExportJob{}, nil
		}).
		AnyTimes()
	// File should NOT be deleted -- grace period has not elapsed.
	storage.EXPECT().Delete(gomock.Any(), gomock.Any()).Times(0)
	jobRepo.EXPECT().
		Update(gomock.Any(), gomock.Any()).
		DoAndReturn(func(_ context.Context, job *entities.ExportJob) error {
			assert.Equal(t, entities.ExportJobStatusExpired, job.Status)
			updateCalled.Store(true)
			return nil
		}).
		AnyTimes()

	worker, err := NewCleanupWorker(jobRepo, storage, cfg, logger)
	require.NoError(t, err)

	startAndWaitWorker(t, worker, updateCalled.Load)
	assert.True(t, updateCalled.Load())
}

func TestCleanupWorker_GracePeriod_FileDeletedAfterGrace(t *testing.T) {
	t.Parallel()

	// Job expired 3 hours ago; grace period is 1 hour.
	// File SHOULD be deleted after marking as expired.
	expiredJob := &entities.ExportJob{
		ID:        uuid.New(),
		Status:    entities.ExportJobStatusSucceeded,
		FileKey:   "exports/old-file.csv",
		ExpiresAt: time.Now().UTC().Add(-3 * time.Hour),
	}

	ctrl := gomock.NewController(t)
	jobRepo := repomocks.NewMockExportJobRepository(ctrl)
	storage := portsmocks.NewMockObjectStorageClient(ctrl)
	cfg := CleanupWorkerConfig{
		Interval:              50 * time.Millisecond,
		BatchSize:             10,
		FileDeleteGracePeriod: 1 * time.Hour,
	}
	logger := &libLog.NopLogger{}

	var deleteCalled atomic.Bool

	var listCalls atomic.Int32

	jobRepo.EXPECT().
		ListExpired(gomock.Any(), cfg.BatchSize).
		DoAndReturn(func(context.Context, int) ([]*entities.ExportJob, error) {
			if listCalls.Add(1) == 1 {
				return []*entities.ExportJob{expiredJob}, nil
			}

			return []*entities.ExportJob{}, nil
		}).
		AnyTimes()
	storage.EXPECT().
		Delete(gomock.Any(), expiredJob.FileKey).
		DoAndReturn(func(context.Context, string) error {
			deleteCalled.Store(true)
			return nil
		}).
		AnyTimes()
	jobRepo.EXPECT().
		Update(gomock.Any(), gomock.Any()).
		Return(nil).
		AnyTimes()

	worker, err := NewCleanupWorker(jobRepo, storage, cfg, logger)
	require.NoError(t, err)

	startAndWaitWorker(t, worker, deleteCalled.Load)
	assert.True(t, deleteCalled.Load())
}

func TestCleanupWorker_AlreadyExpiredJob_SkipsMarkExpired(t *testing.T) {
	t.Parallel()

	// Job is already in EXPIRED status and past grace period.
	// Should skip MarkExpired + Update but still delete file.
	expiredJob := &entities.ExportJob{
		ID:        uuid.New(),
		Status:    entities.ExportJobStatusExpired,
		FileKey:   "exports/already-expired.csv",
		ExpiresAt: time.Now().UTC().Add(-3 * time.Hour),
	}

	ctrl := gomock.NewController(t)
	jobRepo := repomocks.NewMockExportJobRepository(ctrl)
	storage := portsmocks.NewMockObjectStorageClient(ctrl)
	cfg := CleanupWorkerConfig{
		Interval:              50 * time.Millisecond,
		BatchSize:             10,
		FileDeleteGracePeriod: 1 * time.Hour,
	}
	logger := &libLog.NopLogger{}

	var deleteCalled atomic.Bool

	var listCalls atomic.Int32

	jobRepo.EXPECT().
		ListExpired(gomock.Any(), cfg.BatchSize).
		DoAndReturn(func(context.Context, int) ([]*entities.ExportJob, error) {
			if listCalls.Add(1) == 1 {
				return []*entities.ExportJob{expiredJob}, nil
			}

			return []*entities.ExportJob{}, nil
		}).
		AnyTimes()
	// No Update expected -- job is already EXPIRED.
	jobRepo.EXPECT().Update(gomock.Any(), gomock.Any()).Times(0)
	storage.EXPECT().
		Delete(gomock.Any(), expiredJob.FileKey).
		DoAndReturn(func(context.Context, string) error {
			deleteCalled.Store(true)
			return nil
		}).
		AnyTimes()

	worker, err := NewCleanupWorker(jobRepo, storage, cfg, logger)
	require.NoError(t, err)

	startAndWaitWorker(t, worker, deleteCalled.Load)
	assert.True(t, deleteCalled.Load())
}
