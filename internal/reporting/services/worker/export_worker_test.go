//go:build unit

package worker

import (
	"context"
	"errors"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.uber.org/mock/gomock"

	libLog "github.com/LerianStudio/lib-commons/v4/commons/log"
	libHTTP "github.com/LerianStudio/lib-commons/v4/commons/net/http"

	"github.com/LerianStudio/matcher/internal/reporting/domain/entities"
	"github.com/LerianStudio/matcher/internal/reporting/domain/repositories"
	repomocks "github.com/LerianStudio/matcher/internal/reporting/domain/repositories/mocks"
	portsmocks "github.com/LerianStudio/matcher/internal/shared/ports/mocks"
)

var (
	errTestClaimFailed    = errors.New("database connection error")
	errTestRequeueFailed  = errors.New("database error")
	errTestUpdateFailed   = errors.New("update failed")
	errTestGenericFailure = errors.New("test error")
	errTestOriginalError  = errors.New("original error")
	errTestRequeueError   = errors.New("requeue error")
	errTestFetchFailed    = errors.New("database error")
)

type mockReportRepoForWorker struct {
	matchedItems   []*entities.MatchedItem
	matchedNextKey string
	matchedErr     error

	unmatchedItems   []*entities.UnmatchedItem
	unmatchedNextKey string
	unmatchedErr     error

	varianceItems   []*entities.VarianceReportRow
	varianceNextKey string
	varianceErr     error
}

func (m *mockReportRepoForWorker) ListMatchedPage(
	_ context.Context,
	_ entities.ReportFilter,
	_ string,
	_ int,
) ([]*entities.MatchedItem, string, error) {
	return m.matchedItems, m.matchedNextKey, m.matchedErr
}

func (m *mockReportRepoForWorker) ListUnmatchedPage(
	_ context.Context,
	_ entities.ReportFilter,
	_ string,
	_ int,
) ([]*entities.UnmatchedItem, string, error) {
	return m.unmatchedItems, m.unmatchedNextKey, m.unmatchedErr
}

func (m *mockReportRepoForWorker) ListVariancePage(
	_ context.Context,
	_ entities.VarianceReportFilter,
	_ string,
	_ int,
) ([]*entities.VarianceReportRow, string, error) {
	return m.varianceItems, m.varianceNextKey, m.varianceErr
}

var _ repositories.ReportRepository = (*mockReportRepoForWorker)(nil)

func TestNewExportWorker(t *testing.T) {
	t.Parallel()

	t.Run("creates worker with valid dependencies", func(t *testing.T) {
		t.Parallel()

		ctrl := gomock.NewController(t)
		jobRepo := repomocks.NewMockExportJobRepository(ctrl)
		reportRepo := &mockReportRepoForWorker{}
		storage := portsmocks.NewMockObjectStorageClient(ctrl)
		cfg := ExportWorkerConfig{}
		logger := &libLog.NopLogger{}

		worker, err := NewExportWorker(jobRepo, reportRepo, storage, cfg, logger)

		require.NoError(t, err)
		assert.NotNil(t, worker)
	})

	t.Run("returns error with nil job repo", func(t *testing.T) {
		t.Parallel()

		ctrl := gomock.NewController(t)
		reportRepo := &mockReportRepoForWorker{}
		storage := portsmocks.NewMockObjectStorageClient(ctrl)
		cfg := ExportWorkerConfig{}
		logger := &libLog.NopLogger{}

		worker, err := NewExportWorker(nil, reportRepo, storage, cfg, logger)

		require.Error(t, err)
		assert.Nil(t, worker)
		require.ErrorIs(t, err, ErrNilJobRepository)
	})

	t.Run("returns error with nil report repo", func(t *testing.T) {
		t.Parallel()

		ctrl := gomock.NewController(t)
		jobRepo := repomocks.NewMockExportJobRepository(ctrl)
		storage := portsmocks.NewMockObjectStorageClient(ctrl)
		cfg := ExportWorkerConfig{}
		logger := &libLog.NopLogger{}

		worker, err := NewExportWorker(jobRepo, nil, storage, cfg, logger)

		require.Error(t, err)
		assert.Nil(t, worker)
		require.ErrorIs(t, err, ErrNilReportRepository)
	})

	t.Run("returns error with nil storage", func(t *testing.T) {
		t.Parallel()

		ctrl := gomock.NewController(t)
		jobRepo := repomocks.NewMockExportJobRepository(ctrl)
		reportRepo := &mockReportRepoForWorker{}
		cfg := ExportWorkerConfig{}
		logger := &libLog.NopLogger{}

		worker, err := NewExportWorker(jobRepo, reportRepo, nil, cfg, logger)

		require.Error(t, err)
		assert.Nil(t, worker)
		require.ErrorIs(t, err, ErrNilStorageClient)
	})

	t.Run("applies default poll interval", func(t *testing.T) {
		t.Parallel()

		ctrl := gomock.NewController(t)
		jobRepo := repomocks.NewMockExportJobRepository(ctrl)
		reportRepo := &mockReportRepoForWorker{}
		storage := portsmocks.NewMockObjectStorageClient(ctrl)
		cfg := ExportWorkerConfig{PollInterval: 0}
		logger := &libLog.NopLogger{}

		worker, err := NewExportWorker(jobRepo, reportRepo, storage, cfg, logger)

		require.NoError(t, err)
		assert.Equal(t, defaultPollInterval, worker.cfg.PollInterval)
	})

	t.Run("applies default page size", func(t *testing.T) {
		t.Parallel()

		ctrl := gomock.NewController(t)
		jobRepo := repomocks.NewMockExportJobRepository(ctrl)
		reportRepo := &mockReportRepoForWorker{}
		storage := portsmocks.NewMockObjectStorageClient(ctrl)
		cfg := ExportWorkerConfig{PageSize: 0}
		logger := &libLog.NopLogger{}

		worker, err := NewExportWorker(jobRepo, reportRepo, storage, cfg, logger)

		require.NoError(t, err)
		assert.Equal(t, defaultPageSize, worker.cfg.PageSize)
	})
}

func TestExportWorker_StartStop(t *testing.T) {
	t.Parallel()

	t.Run("starts and stops successfully", func(t *testing.T) {
		t.Parallel()

		ctrl := gomock.NewController(t)
		jobRepo := repomocks.NewMockExportJobRepository(ctrl)
		reportRepo := &mockReportRepoForWorker{}
		storage := portsmocks.NewMockObjectStorageClient(ctrl)
		cfg := ExportWorkerConfig{PollInterval: 100 * time.Millisecond}
		logger := &libLog.NopLogger{}

		var claimCount atomic.Int32
		jobRepo.EXPECT().ClaimNextQueued(gomock.Any()).DoAndReturn(func(context.Context) (*entities.ExportJob, error) {
			claimCount.Add(1)
			return nil, nil
		}).AnyTimes()

		worker, err := NewExportWorker(jobRepo, reportRepo, storage, cfg, logger)
		require.NoError(t, err)

		ctx := context.Background()
		err = worker.Start(ctx)
		require.NoError(t, err)

		require.Eventually(t, func() bool {
			return worker.running.Load()
		}, 200*time.Millisecond, 5*time.Millisecond)

		err = worker.Stop()
		require.NoError(t, err)
	})

	t.Run("returns error when already running", func(t *testing.T) {
		t.Parallel()

		ctrl := gomock.NewController(t)
		jobRepo := repomocks.NewMockExportJobRepository(ctrl)
		reportRepo := &mockReportRepoForWorker{}
		storage := portsmocks.NewMockObjectStorageClient(ctrl)
		cfg := ExportWorkerConfig{PollInterval: 100 * time.Millisecond}
		logger := &libLog.NopLogger{}

		var claimCount atomic.Int32
		jobRepo.EXPECT().ClaimNextQueued(gomock.Any()).DoAndReturn(func(context.Context) (*entities.ExportJob, error) {
			claimCount.Add(1)
			return nil, nil
		}).AnyTimes()

		worker, err := NewExportWorker(jobRepo, reportRepo, storage, cfg, logger)
		require.NoError(t, err)

		ctx := context.Background()
		err = worker.Start(ctx)
		require.NoError(t, err)

		err = worker.Start(ctx)
		require.ErrorIs(t, err, ErrWorkerAlreadyRunning)

		err = worker.Stop()
		require.NoError(t, err)
	})

	t.Run("returns error when stopping not running worker", func(t *testing.T) {
		t.Parallel()

		ctrl := gomock.NewController(t)
		jobRepo := repomocks.NewMockExportJobRepository(ctrl)
		reportRepo := &mockReportRepoForWorker{}
		storage := portsmocks.NewMockObjectStorageClient(ctrl)
		cfg := ExportWorkerConfig{PollInterval: 100 * time.Millisecond}
		logger := &libLog.NopLogger{}

		worker, err := NewExportWorker(jobRepo, reportRepo, storage, cfg, logger)
		require.NoError(t, err)

		err = worker.Stop()
		require.ErrorIs(t, err, ErrWorkerNotRunning)
	})

	t.Run("supports stop start stop restart cycle", func(t *testing.T) {
		t.Parallel()

		ctrl := gomock.NewController(t)
		jobRepo := repomocks.NewMockExportJobRepository(ctrl)
		reportRepo := &mockReportRepoForWorker{}
		storage := portsmocks.NewMockObjectStorageClient(ctrl)
		cfg := ExportWorkerConfig{PollInterval: 100 * time.Millisecond}
		logger := &libLog.NopLogger{}

		var claimCount atomic.Int32
		jobRepo.EXPECT().ClaimNextQueued(gomock.Any()).DoAndReturn(func(context.Context) (*entities.ExportJob, error) {
			claimCount.Add(1)
			return nil, nil
		}).AnyTimes()

		worker, err := NewExportWorker(jobRepo, reportRepo, storage, cfg, logger)
		require.NoError(t, err)

		require.NoError(t, worker.Start(context.Background()))
		require.Eventually(t, func() bool {
			return claimCount.Load() >= 1
		}, 300*time.Millisecond, 10*time.Millisecond)
		require.NoError(t, worker.Stop())

		before := claimCount.Load()
		require.NoError(t, worker.Start(context.Background()))
		require.Eventually(t, func() bool {
			return claimCount.Load() > before
		}, 300*time.Millisecond, 10*time.Millisecond)
		require.NoError(t, worker.Stop())
	})

	t.Run("rejects runtime config update while running", func(t *testing.T) {
		t.Parallel()

		ctrl := gomock.NewController(t)
		jobRepo := repomocks.NewMockExportJobRepository(ctrl)
		reportRepo := &mockReportRepoForWorker{}
		storage := portsmocks.NewMockObjectStorageClient(ctrl)
		cfg := ExportWorkerConfig{PollInterval: 100 * time.Millisecond}
		logger := &libLog.NopLogger{}

		jobRepo.EXPECT().ClaimNextQueued(gomock.Any()).Return(nil, nil).AnyTimes()

		worker, err := NewExportWorker(jobRepo, reportRepo, storage, cfg, logger)
		require.NoError(t, err)
		require.NoError(t, worker.Start(context.Background()))

		err = worker.UpdateRuntimeConfig(ExportWorkerConfig{PollInterval: time.Second, PageSize: 42})
		require.ErrorIs(t, err, ErrRuntimeConfigUpdateWhileRunning)
		require.NoError(t, worker.Stop())
	})
}

func TestNewCleanupWorker(t *testing.T) {
	t.Parallel()

	t.Run("creates cleanup worker with valid dependencies", func(t *testing.T) {
		t.Parallel()

		ctrl := gomock.NewController(t)
		jobRepo := repomocks.NewMockExportJobRepository(ctrl)
		storage := portsmocks.NewMockObjectStorageClient(ctrl)
		cfg := CleanupWorkerConfig{}
		logger := &libLog.NopLogger{}

		worker, err := NewCleanupWorker(jobRepo, storage, cfg, logger)

		require.NoError(t, err)
		assert.NotNil(t, worker)
	})

	t.Run("returns error with nil job repo", func(t *testing.T) {
		t.Parallel()

		ctrl := gomock.NewController(t)
		storage := portsmocks.NewMockObjectStorageClient(ctrl)
		cfg := CleanupWorkerConfig{}
		logger := &libLog.NopLogger{}

		worker, err := NewCleanupWorker(nil, storage, cfg, logger)

		require.Error(t, err)
		assert.Nil(t, worker)
		require.ErrorIs(t, err, ErrNilJobRepository)
	})

	t.Run("returns error with nil storage", func(t *testing.T) {
		t.Parallel()

		ctrl := gomock.NewController(t)
		jobRepo := repomocks.NewMockExportJobRepository(ctrl)
		cfg := CleanupWorkerConfig{}
		logger := &libLog.NopLogger{}

		worker, err := NewCleanupWorker(jobRepo, nil, cfg, logger)

		require.Error(t, err)
		assert.Nil(t, worker)
		require.ErrorIs(t, err, ErrNilStorageClient)
	})

	t.Run("applies default interval", func(t *testing.T) {
		t.Parallel()

		ctrl := gomock.NewController(t)
		jobRepo := repomocks.NewMockExportJobRepository(ctrl)
		storage := portsmocks.NewMockObjectStorageClient(ctrl)
		logger := &libLog.NopLogger{}
		cfg := CleanupWorkerConfig{Interval: 0}
		worker, err := NewCleanupWorker(jobRepo, storage, cfg, logger)

		require.NoError(t, err)
		assert.Equal(t, defaultCleanupInterval, worker.cfg.Interval)
	})

	t.Run("applies default batch size", func(t *testing.T) {
		t.Parallel()

		ctrl := gomock.NewController(t)
		jobRepo := repomocks.NewMockExportJobRepository(ctrl)
		storage := portsmocks.NewMockObjectStorageClient(ctrl)
		logger := &libLog.NopLogger{}
		cfg := CleanupWorkerConfig{BatchSize: 0}
		worker, err := NewCleanupWorker(jobRepo, storage, cfg, logger)

		require.NoError(t, err)
		assert.Equal(t, defaultCleanupBatch, worker.cfg.BatchSize)
	})
}

func TestCleanupWorker_StartStop(t *testing.T) {
	t.Parallel()

	t.Run("starts and stops successfully", func(t *testing.T) {
		t.Parallel()

		ctrl := gomock.NewController(t)
		jobRepo := repomocks.NewMockExportJobRepository(ctrl)
		storage := portsmocks.NewMockObjectStorageClient(ctrl)
		cfg := CleanupWorkerConfig{Interval: 100 * time.Millisecond}
		logger := &libLog.NopLogger{}

		var listExpiredCount atomic.Int32
		jobRepo.EXPECT().
			ListExpired(gomock.Any(), gomock.Any()).
			DoAndReturn(func(context.Context, int) ([]*entities.ExportJob, error) {
				listExpiredCount.Add(1)
				return []*entities.ExportJob{}, nil
			}).
			AnyTimes()

		worker, err := NewCleanupWorker(jobRepo, storage, cfg, logger)
		require.NoError(t, err)

		ctx := context.Background()
		err = worker.Start(ctx)
		require.NoError(t, err)

		require.Eventually(t, func() bool {
			return worker.running.Load()
		}, 200*time.Millisecond, 5*time.Millisecond)

		err = worker.Stop()
		require.NoError(t, err)
	})

	t.Run("returns error when already running", func(t *testing.T) {
		t.Parallel()

		ctrl := gomock.NewController(t)
		jobRepo := repomocks.NewMockExportJobRepository(ctrl)
		storage := portsmocks.NewMockObjectStorageClient(ctrl)
		cfg := CleanupWorkerConfig{Interval: 100 * time.Millisecond}
		logger := &libLog.NopLogger{}

		var listExpiredCount atomic.Int32
		jobRepo.EXPECT().
			ListExpired(gomock.Any(), gomock.Any()).
			DoAndReturn(func(context.Context, int) ([]*entities.ExportJob, error) {
				listExpiredCount.Add(1)
				return []*entities.ExportJob{}, nil
			}).
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
	})

	t.Run("supports stop start stop restart cycle", func(t *testing.T) {
		t.Parallel()

		ctrl := gomock.NewController(t)
		jobRepo := repomocks.NewMockExportJobRepository(ctrl)
		storage := portsmocks.NewMockObjectStorageClient(ctrl)
		cfg := CleanupWorkerConfig{Interval: 100 * time.Millisecond}
		logger := &libLog.NopLogger{}

		var listExpiredCount atomic.Int32
		jobRepo.EXPECT().
			ListExpired(gomock.Any(), gomock.Any()).
			DoAndReturn(func(context.Context, int) ([]*entities.ExportJob, error) {
				listExpiredCount.Add(1)
				return []*entities.ExportJob{}, nil
			}).
			AnyTimes()

		worker, err := NewCleanupWorker(jobRepo, storage, cfg, logger)
		require.NoError(t, err)

		require.NoError(t, worker.Start(context.Background()))
		require.Eventually(t, func() bool {
			return listExpiredCount.Load() >= 1
		}, 300*time.Millisecond, 10*time.Millisecond)
		require.NoError(t, worker.Stop())

		before := listExpiredCount.Load()
		require.NoError(t, worker.Start(context.Background()))
		require.Eventually(t, func() bool {
			return listExpiredCount.Load() > before
		}, 300*time.Millisecond, 10*time.Millisecond)
		require.NoError(t, worker.Stop())
	})

	t.Run("rejects runtime config update while running", func(t *testing.T) {
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
		require.NoError(t, worker.Start(context.Background()))

		err = worker.UpdateRuntimeConfig(CleanupWorkerConfig{Interval: time.Second, BatchSize: 10})
		require.ErrorIs(t, err, ErrRuntimeConfigUpdateWhileRunning)
		require.NoError(t, worker.Stop())
	})
}

// mockReportRepoForWorker needs to implement the full interface.
// Adding stub methods for the remaining interface requirements.

func (m *mockReportRepoForWorker) ListMatched(
	_ context.Context,
	_ entities.ReportFilter,
) ([]*entities.MatchedItem, libHTTP.CursorPagination, error) {
	return nil, libHTTP.CursorPagination{}, nil
}

func (m *mockReportRepoForWorker) ListUnmatched(
	_ context.Context,
	_ entities.ReportFilter,
) ([]*entities.UnmatchedItem, libHTTP.CursorPagination, error) {
	return nil, libHTTP.CursorPagination{}, nil
}

func (m *mockReportRepoForWorker) GetSummary(
	_ context.Context,
	_ entities.ReportFilter,
) (*entities.SummaryReport, error) {
	return nil, nil
}

func (m *mockReportRepoForWorker) GetVarianceReport(
	_ context.Context,
	_ entities.VarianceReportFilter,
) ([]*entities.VarianceReportRow, libHTTP.CursorPagination, error) {
	return nil, libHTTP.CursorPagination{}, nil
}

func (m *mockReportRepoForWorker) ListMatchedForExport(
	_ context.Context,
	_ entities.ReportFilter,
	_ int,
) ([]*entities.MatchedItem, error) {
	return nil, nil
}

func (m *mockReportRepoForWorker) ListUnmatchedForExport(
	_ context.Context,
	_ entities.ReportFilter,
	_ int,
) ([]*entities.UnmatchedItem, error) {
	return nil, nil
}

func (m *mockReportRepoForWorker) ListVarianceForExport(
	_ context.Context,
	_ entities.VarianceReportFilter,
	_ int,
) ([]*entities.VarianceReportRow, error) {
	return nil, nil
}

func (m *mockReportRepoForWorker) CountMatched(
	_ context.Context,
	_ entities.ReportFilter,
) (int64, error) {
	return 0, nil
}

func (m *mockReportRepoForWorker) CountUnmatched(
	_ context.Context,
	_ entities.ReportFilter,
) (int64, error) {
	return 0, nil
}

func (m *mockReportRepoForWorker) CountTransactions(
	_ context.Context,
	_ entities.ReportFilter,
) (int64, error) {
	return 0, nil
}

func (m *mockReportRepoForWorker) CountExceptions(
	_ context.Context,
	_ entities.ReportFilter,
) (int64, error) {
	return 0, nil
}

func TestExportWorker_RetryConfig(t *testing.T) {
	t.Parallel()

	t.Run("applies default retry config", func(t *testing.T) {
		t.Parallel()

		ctrl := gomock.NewController(t)
		jobRepo := repomocks.NewMockExportJobRepository(ctrl)
		reportRepo := &mockReportRepoForWorker{}
		storage := portsmocks.NewMockObjectStorageClient(ctrl)
		cfg := ExportWorkerConfig{}
		logger := &libLog.NopLogger{}

		worker, err := NewExportWorker(jobRepo, reportRepo, storage, cfg, logger)

		require.NoError(t, err)
		assert.Equal(t, defaultMaxRetries, worker.cfg.MaxRetries)
		assert.Equal(t, defaultInitialBackoff, worker.cfg.InitialBackoff)
		assert.Equal(t, defaultMaxBackoff, worker.cfg.MaxBackoff)
		assert.InDelta(t, defaultBackoffMultiplier, worker.cfg.BackoffMultiplier, 0.0001)
	})

	t.Run("uses custom retry config when provided", func(t *testing.T) {
		t.Parallel()

		ctrl := gomock.NewController(t)
		jobRepo := repomocks.NewMockExportJobRepository(ctrl)
		reportRepo := &mockReportRepoForWorker{}
		storage := portsmocks.NewMockObjectStorageClient(ctrl)
		cfg := ExportWorkerConfig{
			MaxRetries:        5,
			InitialBackoff:    2 * time.Second,
			MaxBackoff:        10 * time.Minute,
			BackoffMultiplier: 3.0,
		}
		logger := &libLog.NopLogger{}

		worker, err := NewExportWorker(jobRepo, reportRepo, storage, cfg, logger)

		require.NoError(t, err)
		assert.Equal(t, 5, worker.cfg.MaxRetries)
		assert.Equal(t, 2*time.Second, worker.cfg.InitialBackoff)
		assert.Equal(t, 10*time.Minute, worker.cfg.MaxBackoff)
		assert.InDelta(t, 3.0, worker.cfg.BackoffMultiplier, 0.0001)
	})
}

func TestExportWorker_CalculateBackoff(t *testing.T) {
	t.Parallel()

	ctrl := gomock.NewController(t)
	jobRepo := repomocks.NewMockExportJobRepository(ctrl)
	reportRepo := &mockReportRepoForWorker{}
	storage := portsmocks.NewMockObjectStorageClient(ctrl)
	cfg := ExportWorkerConfig{
		InitialBackoff:    1 * time.Second,
		MaxBackoff:        5 * time.Minute,
		BackoffMultiplier: 2.0,
	}
	logger := &libLog.NopLogger{}

	worker, err := NewExportWorker(jobRepo, reportRepo, storage, cfg, logger)
	require.NoError(t, err)

	tests := []struct {
		name     string
		attempt  int
		expected time.Duration
	}{
		{
			name:     "attempt 0 returns initial backoff",
			attempt:  0,
			expected: 1 * time.Second,
		},
		{
			name:     "attempt 1 returns initial backoff",
			attempt:  1,
			expected: 1 * time.Second,
		},
		{
			name:     "attempt 2 returns doubled backoff",
			attempt:  2,
			expected: 2 * time.Second,
		},
		{
			name:     "attempt 3 returns quadrupled backoff",
			attempt:  3,
			expected: 4 * time.Second,
		},
		{
			name:     "high attempt respects max backoff",
			attempt:  20,
			expected: 5 * time.Minute,
		},
		{
			name:     "negative attempt returns initial backoff",
			attempt:  -1,
			expected: 1 * time.Second,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			result := worker.calculateBackoff(tt.attempt)
			assert.Equal(t, tt.expected, result)
		})
	}
}

func TestExportWorker_GetExtension(t *testing.T) {
	t.Parallel()

	ctrl := gomock.NewController(t)
	jobRepo := repomocks.NewMockExportJobRepository(ctrl)
	reportRepo := &mockReportRepoForWorker{}
	storage := portsmocks.NewMockObjectStorageClient(ctrl)
	cfg := ExportWorkerConfig{}
	logger := &libLog.NopLogger{}

	worker, err := NewExportWorker(jobRepo, reportRepo, storage, cfg, logger)
	require.NoError(t, err)

	tests := []struct {
		name     string
		format   entities.ExportFormat
		expected string
	}{
		{
			name:     "CSV format",
			format:   entities.ExportFormatCSV,
			expected: "csv",
		},
		{
			name:     "JSON format",
			format:   entities.ExportFormatJSON,
			expected: "json",
		},
		{
			name:     "XML format",
			format:   entities.ExportFormatXML,
			expected: "xml",
		},
		{
			name:     "unknown format returns dat",
			format:   "UNKNOWN",
			expected: "dat",
		},
		{
			name:     "empty format returns dat",
			format:   "",
			expected: "dat",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			result := worker.getExtension(tt.format)
			assert.Equal(t, tt.expected, result)
		})
	}
}

func TestExportWorker_GetContentType(t *testing.T) {
	t.Parallel()

	ctrl := gomock.NewController(t)
	jobRepo := repomocks.NewMockExportJobRepository(ctrl)
	reportRepo := &mockReportRepoForWorker{}
	storage := portsmocks.NewMockObjectStorageClient(ctrl)
	cfg := ExportWorkerConfig{}
	logger := &libLog.NopLogger{}

	worker, err := NewExportWorker(jobRepo, reportRepo, storage, cfg, logger)
	require.NoError(t, err)

	tests := []struct {
		name     string
		format   entities.ExportFormat
		expected string
	}{
		{
			name:     "CSV format",
			format:   entities.ExportFormatCSV,
			expected: "text/csv",
		},
		{
			name:     "JSON format",
			format:   entities.ExportFormatJSON,
			expected: "application/json",
		},
		{
			name:     "XML format",
			format:   entities.ExportFormatXML,
			expected: "application/xml",
		},
		{
			name:     "unknown format returns octet-stream",
			format:   "UNKNOWN",
			expected: "application/octet-stream",
		},
		{
			name:     "empty format returns octet-stream",
			format:   "",
			expected: "application/octet-stream",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			result := worker.getContentType(tt.format)
			assert.Equal(t, tt.expected, result)
		})
	}
}

func TestExportWorker_GenerateFileKey(t *testing.T) {
	t.Parallel()

	ctrl := gomock.NewController(t)
	jobRepo := repomocks.NewMockExportJobRepository(ctrl)
	reportRepo := &mockReportRepoForWorker{}
	storage := portsmocks.NewMockObjectStorageClient(ctrl)
	cfg := ExportWorkerConfig{}
	logger := &libLog.NopLogger{}

	worker, err := NewExportWorker(jobRepo, reportRepo, storage, cfg, logger)
	require.NoError(t, err)

	tenantID := uuid.MustParse("11111111-1111-1111-1111-111111111111")
	contextID := uuid.MustParse("22222222-2222-2222-2222-222222222222")
	jobID := uuid.MustParse("33333333-3333-3333-3333-333333333333")

	tests := []struct {
		name     string
		job      *entities.ExportJob
		expected string
	}{
		{
			name: "CSV file key",
			job: &entities.ExportJob{
				ID:         jobID,
				TenantID:   tenantID,
				ContextID:  contextID,
				ReportType: entities.ExportReportTypeMatched,
				Format:     entities.ExportFormatCSV,
			},
			expected: "11111111-1111-1111-1111-111111111111/exports/22222222-2222-2222-2222-222222222222/33333333-3333-3333-3333-333333333333-MATCHED.csv",
		},
		{
			name: "JSON file key",
			job: &entities.ExportJob{
				ID:         jobID,
				TenantID:   tenantID,
				ContextID:  contextID,
				ReportType: entities.ExportReportTypeUnmatched,
				Format:     entities.ExportFormatJSON,
			},
			expected: "11111111-1111-1111-1111-111111111111/exports/22222222-2222-2222-2222-222222222222/33333333-3333-3333-3333-333333333333-UNMATCHED.json",
		},
		{
			name: "XML file key",
			job: &entities.ExportJob{
				ID:         jobID,
				TenantID:   tenantID,
				ContextID:  contextID,
				ReportType: entities.ExportReportTypeVariance,
				Format:     entities.ExportFormatXML,
			},
			expected: "11111111-1111-1111-1111-111111111111/exports/22222222-2222-2222-2222-222222222222/33333333-3333-3333-3333-333333333333-VARIANCE.xml",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			result, err := worker.generateFileKey(tt.job)
			require.NoError(t, err)
			assert.Equal(t, tt.expected, result)
		})
	}
}

func TestExportWorker_DefaultTempDir(t *testing.T) {
	t.Parallel()

	ctrl := gomock.NewController(t)
	jobRepo := repomocks.NewMockExportJobRepository(ctrl)
	reportRepo := &mockReportRepoForWorker{}
	storage := portsmocks.NewMockObjectStorageClient(ctrl)
	cfg := ExportWorkerConfig{TempDir: ""}
	logger := &libLog.NopLogger{}

	worker, err := NewExportWorker(jobRepo, reportRepo, storage, cfg, logger)

	require.NoError(t, err)
	assert.NotEmpty(t, worker.cfg.TempDir)
}

func TestExportWorker_GenerateFileKey_ExactFormat(t *testing.T) {
	t.Parallel()

	worker := &ExportWorker{}
	jobID := uuid.MustParse("aabbccdd-0011-2233-4455-667788990011")
	tenantID := uuid.MustParse("550e8400-e29b-41d4-a716-446655440000")
	contextID := uuid.MustParse("660e8400-e29b-41d4-a716-446655440000")

	job := &entities.ExportJob{
		ID:         jobID,
		TenantID:   tenantID,
		ContextID:  contextID,
		ReportType: entities.ExportReportTypeMatched,
		Format:     entities.ExportFormatCSV,
	}

	result, err := worker.generateFileKey(job)
	require.NoError(t, err)
	assert.Equal(t, "550e8400-e29b-41d4-a716-446655440000/exports/660e8400-e29b-41d4-a716-446655440000/aabbccdd-0011-2233-4455-667788990011-MATCHED.csv", result)
}

func TestExportWorker_NegativeConfigValues(t *testing.T) {
	t.Parallel()

	ctrl := gomock.NewController(t)
	jobRepo := repomocks.NewMockExportJobRepository(ctrl)
	reportRepo := &mockReportRepoForWorker{}
	storage := portsmocks.NewMockObjectStorageClient(ctrl)
	cfg := ExportWorkerConfig{
		PollInterval:      -1 * time.Second,
		PageSize:          -100,
		MaxRetries:        -5,
		InitialBackoff:    -1 * time.Second,
		MaxBackoff:        -1 * time.Minute,
		BackoffMultiplier: -2.0,
	}
	logger := &libLog.NopLogger{}

	worker, err := NewExportWorker(jobRepo, reportRepo, storage, cfg, logger)

	require.NoError(t, err)
	assert.Equal(t, defaultPollInterval, worker.cfg.PollInterval)
	assert.Equal(t, defaultPageSize, worker.cfg.PageSize)
	assert.Equal(t, defaultMaxRetries, worker.cfg.MaxRetries)
	assert.Equal(t, defaultInitialBackoff, worker.cfg.InitialBackoff)
	assert.Equal(t, defaultMaxBackoff, worker.cfg.MaxBackoff)
	assert.InDelta(t, defaultBackoffMultiplier, worker.cfg.BackoffMultiplier, 0.0001)
}

func TestExportWorker_WithNilLogger(t *testing.T) {
	t.Parallel()

	ctrl := gomock.NewController(t)
	jobRepo := repomocks.NewMockExportJobRepository(ctrl)
	reportRepo := &mockReportRepoForWorker{}
	storage := portsmocks.NewMockObjectStorageClient(ctrl)
	cfg := ExportWorkerConfig{}

	worker, err := NewExportWorker(jobRepo, reportRepo, storage, cfg, nil)

	require.NoError(t, err)
	assert.NotNil(t, worker)
	assert.Nil(t, worker.logger)
}

func TestExportWorker_ContextCancellation(t *testing.T) {
	t.Parallel()

	ctrl := gomock.NewController(t)
	jobRepo := repomocks.NewMockExportJobRepository(ctrl)
	reportRepo := &mockReportRepoForWorker{}
	storage := portsmocks.NewMockObjectStorageClient(ctrl)
	cfg := ExportWorkerConfig{
		PollInterval: 1 * time.Hour,
	}
	logger := &libLog.NopLogger{}

	jobRepo.EXPECT().ClaimNextQueued(gomock.Any()).Return(nil, nil).AnyTimes()

	worker, err := NewExportWorker(jobRepo, reportRepo, storage, cfg, logger)
	require.NoError(t, err)

	ctx, cancel := context.WithCancel(context.Background())
	err = worker.Start(ctx)
	require.NoError(t, err)

	cancel()

	select {
	case <-worker.doneCh:
	case <-time.After(200 * time.Millisecond):
		require.Fail(t, "expected worker to stop after context cancellation")
	}
}

func TestExportWorker_PollAndProcess_ClaimError(t *testing.T) {
	t.Parallel()

	ctrl := gomock.NewController(t)
	jobRepo := repomocks.NewMockExportJobRepository(ctrl)
	reportRepo := &mockReportRepoForWorker{}
	storage := portsmocks.NewMockObjectStorageClient(ctrl)
	cfg := ExportWorkerConfig{
		PollInterval: 50 * time.Millisecond,
	}
	logger := &libLog.NopLogger{}

	var claimCalled atomic.Bool

	jobRepo.EXPECT().
		ClaimNextQueued(gomock.Any()).
		DoAndReturn(func(context.Context) (*entities.ExportJob, error) {
			claimCalled.Store(true)

			return nil, errTestClaimFailed
		}).
		AnyTimes()

	worker, err := NewExportWorker(jobRepo, reportRepo, storage, cfg, logger)
	require.NoError(t, err)

	ctx := context.Background()
	err = worker.Start(ctx)
	require.NoError(t, err)

	require.Eventually(t, claimCalled.Load, 200*time.Millisecond, 10*time.Millisecond)

	err = worker.Stop()
	require.NoError(t, err)
}

func TestExportWorker_PollAndProcess_NilJob(t *testing.T) {
	t.Parallel()

	ctrl := gomock.NewController(t)
	jobRepo := repomocks.NewMockExportJobRepository(ctrl)
	reportRepo := &mockReportRepoForWorker{}
	storage := portsmocks.NewMockObjectStorageClient(ctrl)
	cfg := ExportWorkerConfig{
		PollInterval: 50 * time.Millisecond,
	}
	logger := &libLog.NopLogger{}

	var claimCount atomic.Int32

	jobRepo.EXPECT().
		ClaimNextQueued(gomock.Any()).
		DoAndReturn(func(context.Context) (*entities.ExportJob, error) {
			claimCount.Add(1)
			return nil, nil
		}).
		AnyTimes()

	worker, err := NewExportWorker(jobRepo, reportRepo, storage, cfg, logger)
	require.NoError(t, err)

	ctx := context.Background()
	err = worker.Start(ctx)
	require.NoError(t, err)

	require.Eventually(t, func() bool {
		return claimCount.Load() >= 2
	}, 300*time.Millisecond, 10*time.Millisecond)

	err = worker.Stop()
	require.NoError(t, err)
}

func TestExportWorker_StreamExport_UnsupportedReportType(t *testing.T) {
	t.Parallel()

	ctrl := gomock.NewController(t)
	jobRepo := repomocks.NewMockExportJobRepository(ctrl)
	reportRepo := &mockReportRepoForWorker{}
	storage := portsmocks.NewMockObjectStorageClient(ctrl)
	cfg := ExportWorkerConfig{}
	logger := &libLog.NopLogger{}

	worker, err := NewExportWorker(jobRepo, reportRepo, storage, cfg, logger)
	require.NoError(t, err)

	job := &entities.ExportJob{
		ID:         uuid.New(),
		TenantID:   uuid.New(),
		ContextID:  uuid.New(),
		ReportType: "INVALID_TYPE",
		Format:     entities.ExportFormatCSV,
	}

	var buf strings.Builder

	count, err := worker.streamExport(context.Background(), job, &buf)

	require.Error(t, err)
	require.ErrorIs(t, err, ErrUnsupportedReportType)
	assert.Equal(t, int64(0), count)
}

func TestExportWorker_StreamMatched_UnsupportedFormat(t *testing.T) {
	t.Parallel()

	ctrl := gomock.NewController(t)
	jobRepo := repomocks.NewMockExportJobRepository(ctrl)
	reportRepo := &mockReportRepoForWorker{}
	storage := portsmocks.NewMockObjectStorageClient(ctrl)
	cfg := ExportWorkerConfig{}
	logger := &libLog.NopLogger{}

	worker, err := NewExportWorker(jobRepo, reportRepo, storage, cfg, logger)
	require.NoError(t, err)

	job := &entities.ExportJob{
		ID:         uuid.New(),
		TenantID:   uuid.New(),
		ContextID:  uuid.New(),
		ReportType: entities.ExportReportTypeMatched,
		Format:     "INVALID_FORMAT",
	}

	filter := entities.ReportFilter{
		ContextID: job.ContextID,
	}

	var buf strings.Builder

	count, err := worker.streamMatched(context.Background(), job, filter, &buf)

	require.Error(t, err)
	require.ErrorIs(t, err, ErrUnsupportedFormat)
	assert.Equal(t, int64(0), count)
}

func TestExportWorker_StreamUnmatched_UnsupportedFormat(t *testing.T) {
	t.Parallel()

	ctrl := gomock.NewController(t)
	jobRepo := repomocks.NewMockExportJobRepository(ctrl)
	reportRepo := &mockReportRepoForWorker{}
	storage := portsmocks.NewMockObjectStorageClient(ctrl)
	cfg := ExportWorkerConfig{}
	logger := &libLog.NopLogger{}

	worker, err := NewExportWorker(jobRepo, reportRepo, storage, cfg, logger)
	require.NoError(t, err)

	job := &entities.ExportJob{
		ID:         uuid.New(),
		TenantID:   uuid.New(),
		ContextID:  uuid.New(),
		ReportType: entities.ExportReportTypeUnmatched,
		Format:     "INVALID_FORMAT",
	}

	filter := entities.ReportFilter{
		ContextID: job.ContextID,
	}

	var buf strings.Builder

	count, err := worker.streamUnmatched(context.Background(), job, filter, &buf)

	require.Error(t, err)
	require.ErrorIs(t, err, ErrUnsupportedFormat)
	assert.Equal(t, int64(0), count)
}

func TestExportWorker_StreamVariance_UnsupportedFormat(t *testing.T) {
	t.Parallel()

	ctrl := gomock.NewController(t)
	jobRepo := repomocks.NewMockExportJobRepository(ctrl)
	reportRepo := &mockReportRepoForWorker{}
	storage := portsmocks.NewMockObjectStorageClient(ctrl)
	cfg := ExportWorkerConfig{}
	logger := &libLog.NopLogger{}

	worker, err := NewExportWorker(jobRepo, reportRepo, storage, cfg, logger)
	require.NoError(t, err)

	job := &entities.ExportJob{
		ID:         uuid.New(),
		TenantID:   uuid.New(),
		ContextID:  uuid.New(),
		ReportType: entities.ExportReportTypeVariance,
		Format:     "INVALID_FORMAT",
	}

	var buf strings.Builder

	count, err := worker.streamVariance(context.Background(), job, &buf)

	require.Error(t, err)
	require.ErrorIs(t, err, ErrUnsupportedFormat)
	assert.Equal(t, int64(0), count)
}

func TestExportWorker_FailJob_WithRetry(t *testing.T) {
	t.Parallel()

	ctrl := gomock.NewController(t)
	jobRepo := repomocks.NewMockExportJobRepository(ctrl)
	reportRepo := &mockReportRepoForWorker{}
	storage := portsmocks.NewMockObjectStorageClient(ctrl)
	cfg := ExportWorkerConfig{
		MaxRetries:        3,
		InitialBackoff:    1 * time.Second,
		MaxBackoff:        5 * time.Minute,
		BackoffMultiplier: 2.0,
	}
	logger := &libLog.NopLogger{}

	worker, err := NewExportWorker(jobRepo, reportRepo, storage, cfg, logger)
	require.NoError(t, err)

	job := &entities.ExportJob{
		ID:       uuid.New(),
		Attempts: 1,
		Status:   entities.ExportJobStatusRunning,
	}

	var requeueCalled atomic.Bool

	jobRepo.EXPECT().
		RequeueForRetry(gomock.Any(), gomock.Any()).
		DoAndReturn(func(context.Context, *entities.ExportJob) error {
			requeueCalled.Store(true)

			return nil
		})

	worker.failJob(context.Background(), job, errTestGenericFailure)

	assert.True(t, requeueCalled.Load())
}

func TestExportWorker_FailJob_MaxRetriesExceeded(t *testing.T) {
	t.Parallel()

	ctrl := gomock.NewController(t)
	jobRepo := repomocks.NewMockExportJobRepository(ctrl)
	reportRepo := &mockReportRepoForWorker{}
	storage := portsmocks.NewMockObjectStorageClient(ctrl)
	cfg := ExportWorkerConfig{
		MaxRetries:        3,
		InitialBackoff:    1 * time.Second,
		MaxBackoff:        5 * time.Minute,
		BackoffMultiplier: 2.0,
	}
	logger := &libLog.NopLogger{}

	worker, err := NewExportWorker(jobRepo, reportRepo, storage, cfg, logger)
	require.NoError(t, err)

	job := &entities.ExportJob{
		ID:       uuid.New(),
		Attempts: 4, // 1 initial + 3 retries = 4 total attempts exceeds MaxRetries=3
		Status:   entities.ExportJobStatusRunning,
	}

	var updateStatusCalled atomic.Bool

	jobRepo.EXPECT().
		UpdateStatus(gomock.Any(), gomock.Any()).
		DoAndReturn(func(_ context.Context, j *entities.ExportJob) error {
			updateStatusCalled.Store(true)
			assert.Equal(t, entities.ExportJobStatusFailed, j.Status)

			return nil
		})

	worker.failJob(context.Background(), job, errTestGenericFailure)

	assert.True(t, updateStatusCalled.Load())
}

func TestExportWorker_RequeueForRetry_FailsToRequeue(t *testing.T) {
	t.Parallel()

	ctrl := gomock.NewController(t)
	jobRepo := repomocks.NewMockExportJobRepository(ctrl)
	reportRepo := &mockReportRepoForWorker{}
	storage := portsmocks.NewMockObjectStorageClient(ctrl)
	cfg := ExportWorkerConfig{
		MaxRetries:        3,
		InitialBackoff:    1 * time.Second,
		MaxBackoff:        5 * time.Minute,
		BackoffMultiplier: 2.0,
	}
	logger := &libLog.NopLogger{}

	worker, err := NewExportWorker(jobRepo, reportRepo, storage, cfg, logger)
	require.NoError(t, err)

	job := &entities.ExportJob{
		ID:       uuid.New(),
		Attempts: 1,
		Status:   entities.ExportJobStatusRunning,
	}

	jobRepo.EXPECT().RequeueForRetry(gomock.Any(), gomock.Any()).Return(errTestRequeueFailed)
	jobRepo.EXPECT().UpdateStatus(gomock.Any(), gomock.Any()).Return(nil)

	err = worker.requeueForRetry(context.Background(), job, errTestOriginalError)

	require.Error(t, err)
	require.ErrorIs(t, err, errTestRequeueFailed)
}

func TestExportWorker_HandleRequeueFailure(t *testing.T) {
	t.Parallel()

	ctrl := gomock.NewController(t)
	jobRepo := repomocks.NewMockExportJobRepository(ctrl)
	reportRepo := &mockReportRepoForWorker{}
	storage := portsmocks.NewMockObjectStorageClient(ctrl)
	cfg := ExportWorkerConfig{}
	logger := &libLog.NopLogger{}

	worker, err := NewExportWorker(jobRepo, reportRepo, storage, cfg, logger)
	require.NoError(t, err)

	job := &entities.ExportJob{
		ID:     uuid.New(),
		Status: entities.ExportJobStatusQueued,
	}

	var failCalled atomic.Bool

	jobRepo.EXPECT().
		UpdateStatus(gomock.Any(), gomock.Any()).
		DoAndReturn(func(_ context.Context, j *entities.ExportJob) error {
			failCalled.Store(true)
			assert.Equal(t, entities.ExportJobStatusFailed, j.Status)
			assert.Contains(t, j.Error, "original error")
			assert.Contains(t, j.Error, "requeue error")

			return nil
		})

	worker.handleRequeueFailure(
		context.Background(),
		job,
		errTestOriginalError,
		errTestRequeueError,
	)

	assert.True(t, failCalled.Load())
}

func TestExportWorker_HandleRequeueFailure_UpdateStatusFails(t *testing.T) {
	t.Parallel()

	ctrl := gomock.NewController(t)
	jobRepo := repomocks.NewMockExportJobRepository(ctrl)
	reportRepo := &mockReportRepoForWorker{}
	storage := portsmocks.NewMockObjectStorageClient(ctrl)
	cfg := ExportWorkerConfig{}
	logger := &libLog.NopLogger{}

	worker, err := NewExportWorker(jobRepo, reportRepo, storage, cfg, logger)
	require.NoError(t, err)

	job := &entities.ExportJob{
		ID:     uuid.New(),
		Status: entities.ExportJobStatusQueued,
	}

	jobRepo.EXPECT().UpdateStatus(gomock.Any(), gomock.Any()).Return(errTestUpdateFailed)

	worker.handleRequeueFailure(
		context.Background(),
		job,
		errTestOriginalError,
		errTestRequeueError,
	)

	assert.Equal(t, entities.ExportJobStatusFailed, job.Status)
}

func TestExportWorker_StreamMatchedCSV_Success(t *testing.T) {
	t.Parallel()

	ctrl := gomock.NewController(t)
	jobRepo := repomocks.NewMockExportJobRepository(ctrl)
	storage := portsmocks.NewMockObjectStorageClient(ctrl)
	cfg := ExportWorkerConfig{PageSize: 100}
	logger := &libLog.NopLogger{}

	matchedItems := []*entities.MatchedItem{
		{
			TransactionID: uuid.New(),
			MatchGroupID:  uuid.New(),
			SourceID:      uuid.New(),
		},
	}

	reportRepo := &mockReportRepoForWorker{
		matchedItems:   matchedItems,
		matchedNextKey: "",
	}

	worker, err := NewExportWorker(jobRepo, reportRepo, storage, cfg, logger)
	require.NoError(t, err)

	job := &entities.ExportJob{
		ID:         uuid.New(),
		TenantID:   uuid.New(),
		ContextID:  uuid.New(),
		ReportType: entities.ExportReportTypeMatched,
		Format:     entities.ExportFormatCSV,
	}

	filter := entities.ReportFilter{
		ContextID: job.ContextID,
	}

	jobRepo.EXPECT().
		UpdateProgress(gomock.Any(), job.ID, gomock.Any(), gomock.Any()).
		Return(nil).
		AnyTimes()

	var buf strings.Builder

	count, err := worker.streamMatchedCSV(context.Background(), job, filter, &buf)

	require.NoError(t, err)
	assert.Equal(t, int64(1), count)
	assert.Contains(t, buf.String(), "transaction_id")
}

func TestExportWorker_StreamMatchedCSV_FetchError(t *testing.T) {
	t.Parallel()

	ctrl := gomock.NewController(t)
	jobRepo := repomocks.NewMockExportJobRepository(ctrl)
	storage := portsmocks.NewMockObjectStorageClient(ctrl)
	cfg := ExportWorkerConfig{PageSize: 100}
	logger := &libLog.NopLogger{}

	reportRepo := &mockReportRepoForWorker{
		matchedErr: errTestFetchFailed,
	}

	worker, err := NewExportWorker(jobRepo, reportRepo, storage, cfg, logger)
	require.NoError(t, err)

	job := &entities.ExportJob{
		ID:         uuid.New(),
		TenantID:   uuid.New(),
		ContextID:  uuid.New(),
		ReportType: entities.ExportReportTypeMatched,
		Format:     entities.ExportFormatCSV,
	}

	filter := entities.ReportFilter{
		ContextID: job.ContextID,
	}

	var buf strings.Builder

	count, err := worker.streamMatchedCSV(context.Background(), job, filter, &buf)

	require.Error(t, err)
	assert.Contains(t, err.Error(), "fetching matched page")
	assert.Equal(t, int64(0), count)
}

func TestExportWorker_StreamMatchedJSON_Success(t *testing.T) {
	t.Parallel()

	ctrl := gomock.NewController(t)
	jobRepo := repomocks.NewMockExportJobRepository(ctrl)
	storage := portsmocks.NewMockObjectStorageClient(ctrl)
	cfg := ExportWorkerConfig{PageSize: 100}
	logger := &libLog.NopLogger{}

	matchedItems := []*entities.MatchedItem{
		{
			TransactionID: uuid.New(),
			MatchGroupID:  uuid.New(),
			SourceID:      uuid.New(),
		},
	}

	reportRepo := &mockReportRepoForWorker{
		matchedItems:   matchedItems,
		matchedNextKey: "",
	}

	worker, err := NewExportWorker(jobRepo, reportRepo, storage, cfg, logger)
	require.NoError(t, err)

	job := &entities.ExportJob{
		ID:         uuid.New(),
		TenantID:   uuid.New(),
		ContextID:  uuid.New(),
		ReportType: entities.ExportReportTypeMatched,
		Format:     entities.ExportFormatJSON,
	}

	filter := entities.ReportFilter{
		ContextID: job.ContextID,
	}

	jobRepo.EXPECT().
		UpdateProgress(gomock.Any(), job.ID, gomock.Any(), gomock.Any()).
		Return(nil).
		AnyTimes()

	var buf strings.Builder

	count, err := worker.streamMatchedJSON(context.Background(), job, filter, &buf)

	require.NoError(t, err)
	assert.Equal(t, int64(1), count)
	assert.Contains(t, buf.String(), "[")
	assert.Contains(t, buf.String(), "]")
}

func TestExportWorker_StreamMatchedXML_Success(t *testing.T) {
	t.Parallel()

	ctrl := gomock.NewController(t)
	jobRepo := repomocks.NewMockExportJobRepository(ctrl)
	storage := portsmocks.NewMockObjectStorageClient(ctrl)
	cfg := ExportWorkerConfig{PageSize: 100}
	logger := &libLog.NopLogger{}

	matchedItems := []*entities.MatchedItem{
		{
			TransactionID: uuid.New(),
			MatchGroupID:  uuid.New(),
			SourceID:      uuid.New(),
		},
	}

	reportRepo := &mockReportRepoForWorker{
		matchedItems:   matchedItems,
		matchedNextKey: "",
	}

	worker, err := NewExportWorker(jobRepo, reportRepo, storage, cfg, logger)
	require.NoError(t, err)

	job := &entities.ExportJob{
		ID:         uuid.New(),
		TenantID:   uuid.New(),
		ContextID:  uuid.New(),
		ReportType: entities.ExportReportTypeMatched,
		Format:     entities.ExportFormatXML,
	}

	filter := entities.ReportFilter{
		ContextID: job.ContextID,
	}

	jobRepo.EXPECT().
		UpdateProgress(gomock.Any(), job.ID, gomock.Any(), gomock.Any()).
		Return(nil).
		AnyTimes()

	var buf strings.Builder

	count, err := worker.streamMatchedXML(context.Background(), job, filter, &buf)

	require.NoError(t, err)
	assert.Equal(t, int64(1), count)
	assert.Contains(t, buf.String(), "<matchedItems>")
	assert.Contains(t, buf.String(), "</matchedItems>")
}

func TestExportWorker_StreamUnmatchedCSV_Success(t *testing.T) {
	t.Parallel()

	ctrl := gomock.NewController(t)
	jobRepo := repomocks.NewMockExportJobRepository(ctrl)
	storage := portsmocks.NewMockObjectStorageClient(ctrl)
	cfg := ExportWorkerConfig{PageSize: 100}
	logger := &libLog.NopLogger{}

	unmatchedItems := []*entities.UnmatchedItem{
		{
			TransactionID: uuid.New(),
			SourceID:      uuid.New(),
			Status:        "PENDING",
		},
	}

	reportRepo := &mockReportRepoForWorker{
		unmatchedItems:   unmatchedItems,
		unmatchedNextKey: "",
	}

	worker, err := NewExportWorker(jobRepo, reportRepo, storage, cfg, logger)
	require.NoError(t, err)

	job := &entities.ExportJob{
		ID:         uuid.New(),
		TenantID:   uuid.New(),
		ContextID:  uuid.New(),
		ReportType: entities.ExportReportTypeUnmatched,
		Format:     entities.ExportFormatCSV,
	}

	filter := entities.ReportFilter{
		ContextID: job.ContextID,
	}

	jobRepo.EXPECT().
		UpdateProgress(gomock.Any(), job.ID, gomock.Any(), gomock.Any()).
		Return(nil).
		AnyTimes()

	var buf strings.Builder

	count, err := worker.streamUnmatchedCSV(context.Background(), job, filter, &buf)

	require.NoError(t, err)
	assert.Equal(t, int64(1), count)
	assert.Contains(t, buf.String(), "transaction_id")
}

func TestExportWorker_StreamUnmatchedJSON_Success(t *testing.T) {
	t.Parallel()

	ctrl := gomock.NewController(t)
	jobRepo := repomocks.NewMockExportJobRepository(ctrl)
	storage := portsmocks.NewMockObjectStorageClient(ctrl)
	cfg := ExportWorkerConfig{PageSize: 100}
	logger := &libLog.NopLogger{}

	unmatchedItems := []*entities.UnmatchedItem{
		{
			TransactionID: uuid.New(),
			SourceID:      uuid.New(),
			Status:        "PENDING",
		},
	}

	reportRepo := &mockReportRepoForWorker{
		unmatchedItems:   unmatchedItems,
		unmatchedNextKey: "",
	}

	worker, err := NewExportWorker(jobRepo, reportRepo, storage, cfg, logger)
	require.NoError(t, err)

	job := &entities.ExportJob{
		ID:         uuid.New(),
		TenantID:   uuid.New(),
		ContextID:  uuid.New(),
		ReportType: entities.ExportReportTypeUnmatched,
		Format:     entities.ExportFormatJSON,
	}

	filter := entities.ReportFilter{
		ContextID: job.ContextID,
	}

	jobRepo.EXPECT().
		UpdateProgress(gomock.Any(), job.ID, gomock.Any(), gomock.Any()).
		Return(nil).
		AnyTimes()

	var buf strings.Builder

	count, err := worker.streamUnmatchedJSON(context.Background(), job, filter, &buf)

	require.NoError(t, err)
	assert.Equal(t, int64(1), count)
	assert.Contains(t, buf.String(), "[")
}

func TestExportWorker_StreamUnmatchedXML_Success(t *testing.T) {
	t.Parallel()

	ctrl := gomock.NewController(t)
	jobRepo := repomocks.NewMockExportJobRepository(ctrl)
	storage := portsmocks.NewMockObjectStorageClient(ctrl)
	cfg := ExportWorkerConfig{PageSize: 100}
	logger := &libLog.NopLogger{}

	unmatchedItems := []*entities.UnmatchedItem{
		{
			TransactionID: uuid.New(),
			SourceID:      uuid.New(),
			Status:        "PENDING",
		},
	}

	reportRepo := &mockReportRepoForWorker{
		unmatchedItems:   unmatchedItems,
		unmatchedNextKey: "",
	}

	worker, err := NewExportWorker(jobRepo, reportRepo, storage, cfg, logger)
	require.NoError(t, err)

	job := &entities.ExportJob{
		ID:         uuid.New(),
		TenantID:   uuid.New(),
		ContextID:  uuid.New(),
		ReportType: entities.ExportReportTypeUnmatched,
		Format:     entities.ExportFormatXML,
	}

	filter := entities.ReportFilter{
		ContextID: job.ContextID,
	}

	jobRepo.EXPECT().
		UpdateProgress(gomock.Any(), job.ID, gomock.Any(), gomock.Any()).
		Return(nil).
		AnyTimes()

	var buf strings.Builder

	count, err := worker.streamUnmatchedXML(context.Background(), job, filter, &buf)

	require.NoError(t, err)
	assert.Equal(t, int64(1), count)
	assert.Contains(t, buf.String(), "<unmatchedItems>")
}

func TestExportWorker_StreamVarianceCSV_Success(t *testing.T) {
	t.Parallel()

	ctrl := gomock.NewController(t)
	jobRepo := repomocks.NewMockExportJobRepository(ctrl)
	storage := portsmocks.NewMockObjectStorageClient(ctrl)
	cfg := ExportWorkerConfig{PageSize: 100}
	logger := &libLog.NopLogger{}

	varianceItems := []*entities.VarianceReportRow{
		{
			SourceID: uuid.New(),
			Currency: "USD",
			FeeType:  "PROCESSING",
		},
	}

	reportRepo := &mockReportRepoForWorker{
		varianceItems:   varianceItems,
		varianceNextKey: "",
	}

	worker, err := NewExportWorker(jobRepo, reportRepo, storage, cfg, logger)
	require.NoError(t, err)

	job := &entities.ExportJob{
		ID:         uuid.New(),
		TenantID:   uuid.New(),
		ContextID:  uuid.New(),
		ReportType: entities.ExportReportTypeVariance,
		Format:     entities.ExportFormatCSV,
	}

	jobRepo.EXPECT().
		UpdateProgress(gomock.Any(), job.ID, gomock.Any(), gomock.Any()).
		Return(nil).
		AnyTimes()

	var buf strings.Builder

	filter := entities.VarianceReportFilter{ContextID: job.ContextID}
	count, err := worker.streamVarianceCSV(context.Background(), job, filter, &buf)

	require.NoError(t, err)
	assert.Equal(t, int64(1), count)
}

func TestExportWorker_StreamVarianceJSON_Success(t *testing.T) {
	t.Parallel()

	ctrl := gomock.NewController(t)
	jobRepo := repomocks.NewMockExportJobRepository(ctrl)
	storage := portsmocks.NewMockObjectStorageClient(ctrl)
	cfg := ExportWorkerConfig{PageSize: 100}
	logger := &libLog.NopLogger{}

	varianceItems := []*entities.VarianceReportRow{
		{
			SourceID: uuid.New(),
			Currency: "USD",
			FeeType:  "PROCESSING",
		},
	}

	reportRepo := &mockReportRepoForWorker{
		varianceItems:   varianceItems,
		varianceNextKey: "",
	}

	worker, err := NewExportWorker(jobRepo, reportRepo, storage, cfg, logger)
	require.NoError(t, err)

	job := &entities.ExportJob{
		ID:         uuid.New(),
		TenantID:   uuid.New(),
		ContextID:  uuid.New(),
		ReportType: entities.ExportReportTypeVariance,
		Format:     entities.ExportFormatJSON,
	}

	jobRepo.EXPECT().
		UpdateProgress(gomock.Any(), job.ID, gomock.Any(), gomock.Any()).
		Return(nil).
		AnyTimes()

	var buf strings.Builder

	filter := entities.VarianceReportFilter{ContextID: job.ContextID}
	count, err := worker.streamVarianceJSON(context.Background(), job, filter, &buf)

	require.NoError(t, err)
	assert.Equal(t, int64(1), count)
	assert.Contains(t, buf.String(), "[")
}

func TestExportWorker_StreamVarianceXML_Success(t *testing.T) {
	t.Parallel()

	ctrl := gomock.NewController(t)
	jobRepo := repomocks.NewMockExportJobRepository(ctrl)
	storage := portsmocks.NewMockObjectStorageClient(ctrl)
	cfg := ExportWorkerConfig{PageSize: 100}
	logger := &libLog.NopLogger{}

	varianceItems := []*entities.VarianceReportRow{
		{
			SourceID: uuid.New(),
			Currency: "USD",
			FeeType:  "PROCESSING",
		},
	}

	reportRepo := &mockReportRepoForWorker{
		varianceItems:   varianceItems,
		varianceNextKey: "",
	}

	worker, err := NewExportWorker(jobRepo, reportRepo, storage, cfg, logger)
	require.NoError(t, err)

	job := &entities.ExportJob{
		ID:         uuid.New(),
		TenantID:   uuid.New(),
		ContextID:  uuid.New(),
		ReportType: entities.ExportReportTypeVariance,
		Format:     entities.ExportFormatXML,
	}

	jobRepo.EXPECT().
		UpdateProgress(gomock.Any(), job.ID, gomock.Any(), gomock.Any()).
		Return(nil).
		AnyTimes()

	var buf strings.Builder

	filter := entities.VarianceReportFilter{ContextID: job.ContextID}
	count, err := worker.streamVarianceXML(context.Background(), job, filter, &buf)

	require.NoError(t, err)
	assert.Equal(t, int64(1), count)
	assert.Contains(t, buf.String(), "<varianceRows>")
}

func TestExportWorker_StreamUnmatchedCSV_FetchError(t *testing.T) {
	t.Parallel()

	ctrl := gomock.NewController(t)
	jobRepo := repomocks.NewMockExportJobRepository(ctrl)
	storage := portsmocks.NewMockObjectStorageClient(ctrl)
	cfg := ExportWorkerConfig{PageSize: 100}
	logger := &libLog.NopLogger{}

	reportRepo := &mockReportRepoForWorker{
		unmatchedErr: errTestFetchFailed,
	}

	worker, err := NewExportWorker(jobRepo, reportRepo, storage, cfg, logger)
	require.NoError(t, err)

	job := &entities.ExportJob{
		ID:         uuid.New(),
		TenantID:   uuid.New(),
		ContextID:  uuid.New(),
		ReportType: entities.ExportReportTypeUnmatched,
		Format:     entities.ExportFormatCSV,
	}

	filter := entities.ReportFilter{
		ContextID: job.ContextID,
	}

	var buf strings.Builder

	count, err := worker.streamUnmatchedCSV(context.Background(), job, filter, &buf)

	require.Error(t, err)
	assert.Contains(t, err.Error(), "fetching unmatched page")
	assert.Equal(t, int64(0), count)
}

func TestExportWorker_StreamVarianceCSV_FetchError(t *testing.T) {
	t.Parallel()

	ctrl := gomock.NewController(t)
	jobRepo := repomocks.NewMockExportJobRepository(ctrl)
	storage := portsmocks.NewMockObjectStorageClient(ctrl)
	cfg := ExportWorkerConfig{PageSize: 100}
	logger := &libLog.NopLogger{}

	reportRepo := &mockReportRepoForWorker{
		varianceErr: errTestFetchFailed,
	}

	worker, err := NewExportWorker(jobRepo, reportRepo, storage, cfg, logger)
	require.NoError(t, err)

	job := &entities.ExportJob{
		ID:         uuid.New(),
		TenantID:   uuid.New(),
		ContextID:  uuid.New(),
		ReportType: entities.ExportReportTypeVariance,
		Format:     entities.ExportFormatCSV,
	}

	var buf strings.Builder

	filter := entities.VarianceReportFilter{ContextID: job.ContextID}
	count, err := worker.streamVarianceCSV(context.Background(), job, filter, &buf)

	require.Error(t, err)
	assert.Contains(t, err.Error(), "fetching variance page")
	assert.Equal(t, int64(0), count)
}

func TestExportWorker_StreamExport_MatchedCSV(t *testing.T) {
	t.Parallel()

	ctrl := gomock.NewController(t)
	jobRepo := repomocks.NewMockExportJobRepository(ctrl)
	storage := portsmocks.NewMockObjectStorageClient(ctrl)
	cfg := ExportWorkerConfig{PageSize: 100}
	logger := &libLog.NopLogger{}

	matchedItems := []*entities.MatchedItem{
		{
			TransactionID: uuid.New(),
			MatchGroupID:  uuid.New(),
			SourceID:      uuid.New(),
		},
	}

	reportRepo := &mockReportRepoForWorker{
		matchedItems:   matchedItems,
		matchedNextKey: "",
	}

	worker, err := NewExportWorker(jobRepo, reportRepo, storage, cfg, logger)
	require.NoError(t, err)

	job := &entities.ExportJob{
		ID:         uuid.New(),
		TenantID:   uuid.New(),
		ContextID:  uuid.New(),
		ReportType: entities.ExportReportTypeMatched,
		Format:     entities.ExportFormatCSV,
	}

	jobRepo.EXPECT().
		UpdateProgress(gomock.Any(), job.ID, gomock.Any(), gomock.Any()).
		Return(nil).
		AnyTimes()

	var buf strings.Builder

	count, err := worker.streamExport(context.Background(), job, &buf)

	require.NoError(t, err)
	assert.Equal(t, int64(1), count)
}

func TestExportWorker_StreamExport_UnmatchedJSON(t *testing.T) {
	t.Parallel()

	ctrl := gomock.NewController(t)
	jobRepo := repomocks.NewMockExportJobRepository(ctrl)
	storage := portsmocks.NewMockObjectStorageClient(ctrl)
	cfg := ExportWorkerConfig{PageSize: 100}
	logger := &libLog.NopLogger{}

	unmatchedItems := []*entities.UnmatchedItem{
		{
			TransactionID: uuid.New(),
			SourceID:      uuid.New(),
			Status:        "PENDING",
		},
	}

	reportRepo := &mockReportRepoForWorker{
		unmatchedItems:   unmatchedItems,
		unmatchedNextKey: "",
	}

	worker, err := NewExportWorker(jobRepo, reportRepo, storage, cfg, logger)
	require.NoError(t, err)

	job := &entities.ExportJob{
		ID:         uuid.New(),
		TenantID:   uuid.New(),
		ContextID:  uuid.New(),
		ReportType: entities.ExportReportTypeUnmatched,
		Format:     entities.ExportFormatJSON,
	}

	jobRepo.EXPECT().
		UpdateProgress(gomock.Any(), job.ID, gomock.Any(), gomock.Any()).
		Return(nil).
		AnyTimes()

	var buf strings.Builder

	count, err := worker.streamExport(context.Background(), job, &buf)

	require.NoError(t, err)
	assert.Equal(t, int64(1), count)
}

func TestExportWorker_StreamExport_VarianceXML(t *testing.T) {
	t.Parallel()

	ctrl := gomock.NewController(t)
	jobRepo := repomocks.NewMockExportJobRepository(ctrl)
	storage := portsmocks.NewMockObjectStorageClient(ctrl)
	cfg := ExportWorkerConfig{PageSize: 100}
	logger := &libLog.NopLogger{}

	varianceItems := []*entities.VarianceReportRow{
		{
			SourceID: uuid.New(),
			Currency: "USD",
			FeeType:  "PROCESSING",
		},
	}

	reportRepo := &mockReportRepoForWorker{
		varianceItems:   varianceItems,
		varianceNextKey: "",
	}

	worker, err := NewExportWorker(jobRepo, reportRepo, storage, cfg, logger)
	require.NoError(t, err)

	job := &entities.ExportJob{
		ID:         uuid.New(),
		TenantID:   uuid.New(),
		ContextID:  uuid.New(),
		ReportType: entities.ExportReportTypeVariance,
		Format:     entities.ExportFormatXML,
	}

	jobRepo.EXPECT().
		UpdateProgress(gomock.Any(), job.ID, gomock.Any(), gomock.Any()).
		Return(nil).
		AnyTimes()

	var buf strings.Builder

	count, err := worker.streamExport(context.Background(), job, &buf)

	require.NoError(t, err)
	assert.Equal(t, int64(1), count)
}

func TestExportWorker_FailJob_RequeueError(t *testing.T) {
	t.Parallel()

	ctrl := gomock.NewController(t)
	jobRepo := repomocks.NewMockExportJobRepository(ctrl)
	reportRepo := &mockReportRepoForWorker{}
	storage := portsmocks.NewMockObjectStorageClient(ctrl)
	cfg := ExportWorkerConfig{
		MaxRetries:        3,
		InitialBackoff:    1 * time.Second,
		MaxBackoff:        5 * time.Minute,
		BackoffMultiplier: 2.0,
	}
	logger := &libLog.NopLogger{}

	worker, err := NewExportWorker(jobRepo, reportRepo, storage, cfg, logger)
	require.NoError(t, err)

	job := &entities.ExportJob{
		ID:       uuid.New(),
		Attempts: 1,
		Status:   entities.ExportJobStatusRunning,
	}

	jobRepo.EXPECT().RequeueForRetry(gomock.Any(), gomock.Any()).Return(errTestRequeueFailed)
	jobRepo.EXPECT().UpdateStatus(gomock.Any(), gomock.Any()).Return(nil)

	worker.failJob(context.Background(), job, errTestGenericFailure)
}

func TestExportWorker_FailJob_UpdateStatusError(t *testing.T) {
	t.Parallel()

	ctrl := gomock.NewController(t)
	jobRepo := repomocks.NewMockExportJobRepository(ctrl)
	reportRepo := &mockReportRepoForWorker{}
	storage := portsmocks.NewMockObjectStorageClient(ctrl)
	cfg := ExportWorkerConfig{
		MaxRetries:        3,
		InitialBackoff:    1 * time.Second,
		MaxBackoff:        5 * time.Minute,
		BackoffMultiplier: 2.0,
	}
	logger := &libLog.NopLogger{}

	worker, err := NewExportWorker(jobRepo, reportRepo, storage, cfg, logger)
	require.NoError(t, err)

	job := &entities.ExportJob{
		ID:       uuid.New(),
		Attempts: 4, // 1 initial + 3 retries = 4 total attempts exceeds MaxRetries=3
		Status:   entities.ExportJobStatusRunning,
	}

	jobRepo.EXPECT().UpdateStatus(gomock.Any(), gomock.Any()).Return(errTestUpdateFailed)

	worker.failJob(context.Background(), job, errTestGenericFailure)
}
