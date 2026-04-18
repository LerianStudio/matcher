//go:build unit

package command

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/otel"
	"go.uber.org/mock/gomock"

	libCommons "github.com/LerianStudio/lib-commons/v5/commons"
	libLog "github.com/LerianStudio/lib-commons/v5/commons/log"
	libHTTP "github.com/LerianStudio/lib-commons/v5/commons/net/http"

	"github.com/LerianStudio/matcher/internal/reporting/domain/entities"
	"github.com/LerianStudio/matcher/internal/reporting/domain/repositories"
	repomocks "github.com/LerianStudio/matcher/internal/reporting/domain/repositories/mocks"
)

var (
	errTestDatabaseError = errors.New("database error")
	errTestJobNotFound   = errors.New("job not found")
)

type exportJobRepoMockConfig struct {
	createErr          error
	createCalled       *bool
	getByIDJob         *entities.ExportJob
	getByIDErr         error
	listJobs           []*entities.ExportJob
	listCursor         libHTTP.CursorPagination
	listErr            error
	updateErr          error
	updateStatusErr    error
	updateStatusCalled *bool
	updateProgressErr  error
	claimJob           *entities.ExportJob
	claimErr           error
	expiredJobs        []*entities.ExportJob
	expiredErr         error
	deleteErr          error
	listByContextJobs  []*entities.ExportJob
	listByContextErr   error
}

// newExportJobRepoMock builds a mock ExportJobRepository pre-wired with the
// given config.  All methods default to AnyTimes() so that tests focusing on
// one use case aren't forced to specify expectations for every unrelated
// method on the interface.
//
// Design decision: AnyTimes() on all methods is a deliberate trade-off for
// this mock helper.  The ExportJobRepository interface has 10+ methods, and
// each test typically exercises only 1-2 of them.  Requiring explicit call
// counts for every method would bloat each test with ~20 lines of irrelevant
// setup.  The config struct already provides per-method error injection and
// call tracking (createCalled, updateStatusCalled), which gives targeted
// assertion power where it matters.  Tests that need stricter call-count
// verification should build their own gomock expectations directly.
func newExportJobRepoMock(
	t *testing.T,
	cfg exportJobRepoMockConfig,
) *repomocks.MockExportJobRepository {
	t.Helper()

	ctrl := gomock.NewController(t)
	mock := repomocks.NewMockExportJobRepository(ctrl)

	mock.EXPECT().
		Create(gomock.Any(), gomock.Any()).
		DoAndReturn(func(context.Context, *entities.ExportJob) error {
			if cfg.createCalled != nil {
				*cfg.createCalled = true
			}

			return cfg.createErr
		}).
		AnyTimes()

	mock.EXPECT().
		GetByID(gomock.Any(), gomock.Any()).
		Return(cfg.getByIDJob, cfg.getByIDErr).
		AnyTimes()
	mock.EXPECT().Update(gomock.Any(), gomock.Any()).Return(cfg.updateErr).AnyTimes()
	mock.EXPECT().
		UpdateStatus(gomock.Any(), gomock.Any()).
		DoAndReturn(func(context.Context, *entities.ExportJob) error {
			if cfg.updateStatusCalled != nil {
				*cfg.updateStatusCalled = true
			}

			return cfg.updateStatusErr
		}).
		AnyTimes()
	mock.EXPECT().
		UpdateProgress(gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any()).
		Return(cfg.updateProgressErr).
		AnyTimes()
	mock.EXPECT().
		List(gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any()).
		Return(cfg.listJobs, cfg.listCursor, cfg.listErr).
		AnyTimes()
	mock.EXPECT().
		ListByContext(gomock.Any(), gomock.Any(), gomock.Any()).
		Return(cfg.listByContextJobs, cfg.listByContextErr).
		AnyTimes()
	mock.EXPECT().
		ListExpired(gomock.Any(), gomock.Any()).
		Return(cfg.expiredJobs, cfg.expiredErr).
		AnyTimes()
	mock.EXPECT().ClaimNextQueued(gomock.Any()).Return(cfg.claimJob, cfg.claimErr).AnyTimes()
	mock.EXPECT().Delete(gomock.Any(), gomock.Any()).Return(cfg.deleteErr).AnyTimes()

	return mock
}

func contextWithTracking() context.Context {
	ctx := context.Background()
	ctx = libCommons.ContextWithLogger(ctx, &libLog.NopLogger{})
	ctx = libCommons.ContextWithTracer(ctx, otel.Tracer("test"))

	return ctx
}

func TestNewExportJobUseCase(t *testing.T) {
	t.Parallel()

	t.Run("creates use case with valid repository", func(t *testing.T) {
		t.Parallel()

		repo := newExportJobRepoMock(t, exportJobRepoMockConfig{})
		uc, err := NewExportJobUseCase(repo)

		require.NoError(t, err)
		assert.NotNil(t, uc)
		assert.Equal(t, repo, uc.repo)
	})

	t.Run("returns error with nil repository", func(t *testing.T) {
		t.Parallel()

		uc, err := NewExportJobUseCase(nil)

		require.Error(t, err)
		assert.Nil(t, uc)
		require.ErrorIs(t, err, ErrNilExportJobRepository)
	})
}

func TestExportJobUseCase_CreateExportJob_Success(t *testing.T) {
	t.Parallel()

	createCalled := false
	repo := newExportJobRepoMock(t, exportJobRepoMockConfig{createCalled: &createCalled})
	uc, err := NewExportJobUseCase(repo)
	require.NoError(t, err)

	ctx := contextWithTracking()
	input := CreateExportJobInput{
		TenantID:   uuid.New(),
		ContextID:  uuid.New(),
		ReportType: entities.ExportReportTypeMatched,
		Format:     entities.ExportFormatCSV,
		Filter: entities.ExportJobFilter{
			DateFrom: time.Now().UTC().AddDate(0, -1, 0),
			DateTo:   time.Now().UTC(),
		},
	}

	output, err := uc.CreateExportJob(ctx, input)

	require.NoError(t, err)
	assert.NotNil(t, output)
	assert.NotEqual(t, uuid.Nil, output.JobID)
	assert.Equal(t, entities.ExportJobStatusQueued, output.Status)
	assert.Contains(t, output.StatusURL, "/v1/export-jobs/")
	assert.True(t, createCalled)
}

func TestExportJobUseCase_CreateExportJob_InvalidFormat(t *testing.T) {
	t.Parallel()

	repo := newExportJobRepoMock(t, exportJobRepoMockConfig{})
	uc, err := NewExportJobUseCase(repo)
	require.NoError(t, err)

	ctx := contextWithTracking()
	input := CreateExportJobInput{
		TenantID:   uuid.New(),
		ContextID:  uuid.New(),
		ReportType: entities.ExportReportTypeMatched,
		Format:     "INVALID",
		Filter:     entities.ExportJobFilter{},
	}

	output, err := uc.CreateExportJob(ctx, input)

	require.Error(t, err)
	assert.Nil(t, output)
	assert.Contains(t, err.Error(), "creating export job entity")
}

func TestExportJobUseCase_CreateExportJob_InvalidReportType(t *testing.T) {
	t.Parallel()

	repo := newExportJobRepoMock(t, exportJobRepoMockConfig{})
	uc, err := NewExportJobUseCase(repo)
	require.NoError(t, err)

	ctx := contextWithTracking()
	input := CreateExportJobInput{
		TenantID:   uuid.New(),
		ContextID:  uuid.New(),
		ReportType: "INVALID_TYPE",
		Format:     entities.ExportFormatCSV,
		Filter:     entities.ExportJobFilter{},
	}

	output, err := uc.CreateExportJob(ctx, input)

	require.Error(t, err)
	assert.Nil(t, output)
	assert.Contains(t, err.Error(), "creating export job entity")
}

func TestExportJobUseCase_CreateExportJob_RepositoryFailure(t *testing.T) {
	t.Parallel()

	repo := newExportJobRepoMock(t, exportJobRepoMockConfig{createErr: errTestDatabaseError})
	uc, err := NewExportJobUseCase(repo)
	require.NoError(t, err)

	ctx := contextWithTracking()
	input := CreateExportJobInput{
		TenantID:   uuid.New(),
		ContextID:  uuid.New(),
		ReportType: entities.ExportReportTypeUnmatched,
		Format:     entities.ExportFormatJSON,
		Filter:     entities.ExportJobFilter{},
	}

	output, err := uc.CreateExportJob(ctx, input)

	require.Error(t, err)
	assert.Nil(t, output)
	assert.Contains(t, err.Error(), "persisting export job")
}

func TestExportJobUseCase_CreateExportJob_AllValidFormats(t *testing.T) {
	t.Parallel()

	formats := []entities.ExportFormat{
		entities.ExportFormatCSV,
		entities.ExportFormatJSON,
		entities.ExportFormatXML,
		entities.ExportFormatPDF,
	}

	for _, format := range formats {
		t.Run(string(format), func(t *testing.T) {
			t.Parallel()

			repo := newExportJobRepoMock(t, exportJobRepoMockConfig{})
			uc, err := NewExportJobUseCase(repo)
			require.NoError(t, err)

			ctx := contextWithTracking()
			input := CreateExportJobInput{
				TenantID:   uuid.New(),
				ContextID:  uuid.New(),
				ReportType: entities.ExportReportTypeMatched,
				Format:     format,
				Filter:     entities.ExportJobFilter{},
			}

			output, err := uc.CreateExportJob(ctx, input)

			require.NoError(t, err)
			assert.NotNil(t, output)
		})
	}
}

func TestExportJobUseCase_CreateExportJob_AllValidReportTypes(t *testing.T) {
	t.Parallel()

	reportTypes := []entities.ExportReportType{
		entities.ExportReportTypeMatched,
		entities.ExportReportTypeUnmatched,
		entities.ExportReportTypeSummary,
		entities.ExportReportTypeVariance,
		entities.ExportReportTypeExceptions,
	}

	for _, reportType := range reportTypes {
		t.Run(string(reportType), func(t *testing.T) {
			t.Parallel()

			repo := newExportJobRepoMock(t, exportJobRepoMockConfig{})
			uc, err := NewExportJobUseCase(repo)
			require.NoError(t, err)

			ctx := contextWithTracking()
			input := CreateExportJobInput{
				TenantID:   uuid.New(),
				ContextID:  uuid.New(),
				ReportType: reportType,
				Format:     entities.ExportFormatCSV,
				Filter:     entities.ExportJobFilter{},
			}

			output, err := uc.CreateExportJob(ctx, input)

			require.NoError(t, err)
			assert.NotNil(t, output)
		})
	}
}

func TestExportJobUseCase_CancelExportJob(t *testing.T) {
	t.Parallel()

	t.Run("cancels queued job successfully", func(t *testing.T) {
		t.Parallel()

		jobID := uuid.New()
		queuedJob := &entities.ExportJob{
			ID:     jobID,
			Status: entities.ExportJobStatusQueued,
		}

		updateStatusCalled := false
		repo := newExportJobRepoMock(t, exportJobRepoMockConfig{
			getByIDJob:         queuedJob,
			updateStatusCalled: &updateStatusCalled,
		})
		uc, err := NewExportJobUseCase(repo)
		require.NoError(t, err)

		ctx := contextWithTracking()
		err = uc.CancelExportJob(ctx, jobID)

		require.NoError(t, err)
		assert.True(t, updateStatusCalled)
		assert.Equal(t, entities.ExportJobStatusCanceled, queuedJob.Status)
	})

	t.Run("cancels running job successfully", func(t *testing.T) {
		t.Parallel()

		jobID := uuid.New()
		runningJob := &entities.ExportJob{
			ID:     jobID,
			Status: entities.ExportJobStatusRunning,
		}

		updateStatusCalled := false
		repo := newExportJobRepoMock(t, exportJobRepoMockConfig{
			getByIDJob:         runningJob,
			updateStatusCalled: &updateStatusCalled,
		})
		uc, err := NewExportJobUseCase(repo)
		require.NoError(t, err)

		ctx := contextWithTracking()
		err = uc.CancelExportJob(ctx, jobID)

		require.NoError(t, err)
		assert.True(t, updateStatusCalled)
	})

	t.Run("returns error for terminal states", func(t *testing.T) {
		t.Parallel()

		terminalStates := []entities.ExportJobStatus{
			entities.ExportJobStatusSucceeded,
			entities.ExportJobStatusFailed,
			entities.ExportJobStatusExpired,
			entities.ExportJobStatusCanceled,
		}

		for _, status := range terminalStates {
			t.Run(string(status), func(t *testing.T) {
				t.Parallel()

				jobID := uuid.New()
				terminalJob := &entities.ExportJob{
					ID:     jobID,
					Status: status,
				}

				repo := newExportJobRepoMock(t, exportJobRepoMockConfig{getByIDJob: terminalJob})
				uc, err := NewExportJobUseCase(repo)
				require.NoError(t, err)

				ctx := contextWithTracking()
				err = uc.CancelExportJob(ctx, jobID)

				require.Error(t, err)
				require.ErrorIs(t, err, ErrJobInTerminalState)
			})
		}
	})

	t.Run("returns error when job not found", func(t *testing.T) {
		t.Parallel()

		repo := newExportJobRepoMock(t, exportJobRepoMockConfig{getByIDErr: errTestJobNotFound})
		uc, err := NewExportJobUseCase(repo)
		require.NoError(t, err)

		ctx := contextWithTracking()
		err = uc.CancelExportJob(ctx, uuid.New())

		require.Error(t, err)
		assert.Contains(t, err.Error(), "getting export job")
	})

	t.Run("returns sentinel ErrExportJobNotFound for not-found repository error", func(t *testing.T) {
		t.Parallel()

		repo := newExportJobRepoMock(t, exportJobRepoMockConfig{
			getByIDErr: repositories.ErrExportJobNotFound,
		})
		uc, err := NewExportJobUseCase(repo)
		require.NoError(t, err)

		ctx := contextWithTracking()
		err = uc.CancelExportJob(ctx, uuid.New())

		require.Error(t, err)
		require.ErrorIs(t, err, ErrExportJobNotFound)
	})

	t.Run("returns error on update failure", func(t *testing.T) {
		t.Parallel()

		jobID := uuid.New()
		queuedJob := &entities.ExportJob{
			ID:     jobID,
			Status: entities.ExportJobStatusQueued,
		}

		repo := newExportJobRepoMock(t, exportJobRepoMockConfig{
			getByIDJob:      queuedJob,
			updateStatusErr: errTestDatabaseError,
		})
		uc, err := NewExportJobUseCase(repo)
		require.NoError(t, err)

		ctx := contextWithTracking()
		err = uc.CancelExportJob(ctx, jobID)

		require.Error(t, err)
		assert.Contains(t, err.Error(), "canceling export job")
	})
}

func TestExportJobUseCase_WithFilter(t *testing.T) {
	t.Parallel()

	t.Run("creates job with source filter", func(t *testing.T) {
		t.Parallel()

		repo := newExportJobRepoMock(t, exportJobRepoMockConfig{})
		uc, err := NewExportJobUseCase(repo)
		require.NoError(t, err)

		sourceID := uuid.New()
		ctx := contextWithTracking()
		input := CreateExportJobInput{
			TenantID:   uuid.New(),
			ContextID:  uuid.New(),
			ReportType: entities.ExportReportTypeMatched,
			Format:     entities.ExportFormatCSV,
			Filter: entities.ExportJobFilter{
				DateFrom: time.Now().UTC().AddDate(0, -1, 0),
				DateTo:   time.Now().UTC(),
				SourceID: &sourceID,
			},
		}

		output, err := uc.CreateExportJob(ctx, input)

		require.NoError(t, err)
		assert.NotNil(t, output)
	})

	t.Run("creates job with status filter", func(t *testing.T) {
		t.Parallel()

		repo := newExportJobRepoMock(t, exportJobRepoMockConfig{})
		uc, err := NewExportJobUseCase(repo)
		require.NoError(t, err)

		status := entities.ExportJobStatus("PENDING")
		ctx := contextWithTracking()
		input := CreateExportJobInput{
			TenantID:   uuid.New(),
			ContextID:  uuid.New(),
			ReportType: entities.ExportReportTypeUnmatched,
			Format:     entities.ExportFormatJSON,
			Filter: entities.ExportJobFilter{
				DateFrom: time.Now().UTC().AddDate(0, -1, 0),
				DateTo:   time.Now().UTC(),
				Status:   &status,
			},
		}

		output, err := uc.CreateExportJob(ctx, input)

		require.NoError(t, err)
		assert.NotNil(t, output)
	})
}

func TestExportJobUseCase_Concurrency(t *testing.T) {
	t.Parallel()

	t.Run("handles concurrent create requests", func(t *testing.T) {
		t.Parallel()

		repo := newExportJobRepoMock(t, exportJobRepoMockConfig{})
		uc, err := NewExportJobUseCase(repo)
		require.NoError(t, err)

		ctx := contextWithTracking()
		errCh := make(chan error, 10)

		var wg sync.WaitGroup

		for i := 0; i < 10; i++ {
			wg.Add(1)

			go func() {
				defer wg.Done()

				input := CreateExportJobInput{
					TenantID:   uuid.New(),
					ContextID:  uuid.New(),
					ReportType: entities.ExportReportTypeMatched,
					Format:     entities.ExportFormatCSV,
					Filter:     entities.ExportJobFilter{},
				}

				_, err := uc.CreateExportJob(ctx, input)
				errCh <- err
			}()
		}

		wg.Wait()
		close(errCh)

		for err := range errCh {
			require.NoError(t, err)
		}
	})
}
