//go:build unit

package worker

import (
	"context"
	"errors"
	"strings"
	"testing"

	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.uber.org/mock/gomock"

	libLog "github.com/LerianStudio/lib-commons/v4/commons/log"

	"github.com/LerianStudio/matcher/internal/auth"
	"github.com/LerianStudio/matcher/internal/reporting/domain/entities"
	repomocks "github.com/LerianStudio/matcher/internal/reporting/domain/repositories/mocks"
	portsmocks "github.com/LerianStudio/matcher/internal/shared/ports/mocks"
)

// --- processJob Tests ---

func TestExportWorker_ProcessJob_Success(t *testing.T) {
	t.Parallel()

	ctrl := gomock.NewController(t)
	jobRepo := repomocks.NewMockExportJobRepository(ctrl)
	storage := portsmocks.NewMockObjectStorageClient(ctrl)
	cfg := ExportWorkerConfig{PageSize: 100, TempDir: t.TempDir()}
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
		Filter:     entities.ExportJobFilter{},
		Status:     entities.ExportJobStatusRunning,
	}

	jobRepo.EXPECT().
		UpdateProgress(gomock.Any(), job.ID, gomock.Any(), gomock.Any()).
		Return(nil).
		AnyTimes()

	storage.EXPECT().
		Upload(gomock.Any(), gomock.Any(), gomock.Any(), "text/csv").
		Return("key", nil)

	jobRepo.EXPECT().
		Update(gomock.Any(), gomock.Any()).
		Return(nil)

	worker.processJob(context.Background(), job)

	assert.Equal(t, entities.ExportJobStatusSucceeded, job.Status)
}

func TestExportWorker_ProcessJob_VarianceUsesJobTenantContext(t *testing.T) {
	t.Parallel()

	ctrl := gomock.NewController(t)
	jobRepo := repomocks.NewMockExportJobRepository(ctrl)
	storage := portsmocks.NewMockObjectStorageClient(ctrl)
	cfg := ExportWorkerConfig{PageSize: 100, TempDir: t.TempDir()}
	logger := &libLog.NopLogger{}
	tenantID := uuid.MustParse("33333333-3333-3333-3333-333333333333")

	reportRepo := &mockReportRepoForWorker{
		varianceItems: []*entities.VarianceReportRow{{
			SourceID:        uuid.New(),
			Currency:        "USD",
			FeeScheduleID:   uuid.MustParse("00000000-0000-0000-0000-000000000093"),
			FeeScheduleName: "TENANT-SCOPED",
		}},
		varianceNextKey: "",
	}

	worker, err := NewExportWorker(jobRepo, reportRepo, storage, cfg, logger)
	require.NoError(t, err)

	job := &entities.ExportJob{
		ID:         uuid.New(),
		TenantID:   tenantID,
		ContextID:  uuid.New(),
		ReportType: entities.ExportReportTypeVariance,
		Format:     entities.ExportFormatJSON,
		Filter:     entities.ExportJobFilter{},
		Status:     entities.ExportJobStatusRunning,
	}

	jobRepo.EXPECT().
		UpdateProgress(gomock.Any(), job.ID, gomock.Any(), gomock.Any()).
		DoAndReturn(func(ctx context.Context, _ uuid.UUID, _, _ int64) error {
			assert.Equal(t, tenantID.String(), auth.GetTenantID(ctx))
			return nil
		}).
		AnyTimes()

	storage.EXPECT().
		Upload(gomock.Any(), gomock.Any(), gomock.Any(), "application/json").
		Return("key", nil)

	jobRepo.EXPECT().
		Update(gomock.Any(), gomock.Any()).
		DoAndReturn(func(ctx context.Context, _ *entities.ExportJob) error {
			assert.Equal(t, tenantID.String(), auth.GetTenantID(ctx))
			return nil
		})

	worker.processJob(context.Background(), job)

	require.Equal(t, []string{tenantID.String()}, reportRepo.varianceTenantIDsSeen)
	assert.Equal(t, entities.ExportJobStatusSucceeded, job.Status)
}

func TestExportWorker_ProcessJob_StreamExportError(t *testing.T) {
	t.Parallel()

	ctrl := gomock.NewController(t)
	jobRepo := repomocks.NewMockExportJobRepository(ctrl)
	storage := portsmocks.NewMockObjectStorageClient(ctrl)
	cfg := ExportWorkerConfig{
		PageSize:          100,
		TempDir:           t.TempDir(),
		MaxRetries:        3,
		InitialBackoff:    1,
		MaxBackoff:        1,
		BackoffMultiplier: 1.0,
	}
	logger := &libLog.NopLogger{}

	reportRepo := &mockReportRepoForWorker{
		matchedErr: errors.New("stream error"),
	}

	worker, err := NewExportWorker(jobRepo, reportRepo, storage, cfg, logger)
	require.NoError(t, err)

	job := &entities.ExportJob{
		ID:         uuid.New(),
		TenantID:   uuid.New(),
		ContextID:  uuid.New(),
		ReportType: entities.ExportReportTypeMatched,
		Format:     entities.ExportFormatCSV,
		Filter:     entities.ExportJobFilter{},
		Attempts:   1,
		Status:     entities.ExportJobStatusRunning,
	}

	jobRepo.EXPECT().
		RequeueForRetry(gomock.Any(), gomock.Any()).
		Return(nil)

	worker.processJob(context.Background(), job)

	assert.Equal(t, entities.ExportJobStatusQueued, job.Status)
	assert.Contains(t, job.Error, "stream error")
	assert.NotNil(t, job.NextRetryAt)
}

func TestExportWorker_ProcessJob_UploadError(t *testing.T) {
	t.Parallel()

	ctrl := gomock.NewController(t)
	jobRepo := repomocks.NewMockExportJobRepository(ctrl)
	storage := portsmocks.NewMockObjectStorageClient(ctrl)
	cfg := ExportWorkerConfig{
		PageSize:          100,
		TempDir:           t.TempDir(),
		MaxRetries:        3,
		InitialBackoff:    1,
		MaxBackoff:        1,
		BackoffMultiplier: 1.0,
	}
	logger := &libLog.NopLogger{}

	reportRepo := &mockReportRepoForWorker{
		matchedItems:   []*entities.MatchedItem{{TransactionID: uuid.New(), MatchGroupID: uuid.New(), SourceID: uuid.New()}},
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
		Filter:     entities.ExportJobFilter{},
		Attempts:   1,
		Status:     entities.ExportJobStatusRunning,
	}

	jobRepo.EXPECT().
		UpdateProgress(gomock.Any(), job.ID, gomock.Any(), gomock.Any()).
		Return(nil).
		AnyTimes()

	storage.EXPECT().
		Upload(gomock.Any(), gomock.Any(), gomock.Any(), "text/csv").
		Return("", errors.New("storage error"))

	jobRepo.EXPECT().
		RequeueForRetry(gomock.Any(), gomock.Any()).
		Return(nil)

	worker.processJob(context.Background(), job)

	assert.Equal(t, entities.ExportJobStatusQueued, job.Status)
	assert.Contains(t, job.Error, "uploading to storage")
	assert.NotNil(t, job.NextRetryAt)
}

func TestExportWorker_ProcessJob_UpdateJobError(t *testing.T) {
	t.Parallel()

	ctrl := gomock.NewController(t)
	jobRepo := repomocks.NewMockExportJobRepository(ctrl)
	storage := portsmocks.NewMockObjectStorageClient(ctrl)
	cfg := ExportWorkerConfig{PageSize: 100, TempDir: t.TempDir()}
	logger := &libLog.NopLogger{}

	reportRepo := &mockReportRepoForWorker{
		matchedItems:   []*entities.MatchedItem{{TransactionID: uuid.New(), MatchGroupID: uuid.New(), SourceID: uuid.New()}},
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
		Filter:     entities.ExportJobFilter{},
		Status:     entities.ExportJobStatusRunning,
	}

	jobRepo.EXPECT().
		UpdateProgress(gomock.Any(), job.ID, gomock.Any(), gomock.Any()).
		Return(nil).
		AnyTimes()

	storage.EXPECT().
		Upload(gomock.Any(), gomock.Any(), gomock.Any(), "text/csv").
		Return("key", nil)

	jobRepo.EXPECT().
		Update(gomock.Any(), gomock.Any()).
		Return(errors.New("update failed"))

	worker.processJob(context.Background(), job)

	// MarkSucceeded is called before Update, so the job is already in succeeded state
	// even though the DB update failed.
	assert.Equal(t, entities.ExportJobStatusSucceeded, job.Status)
	assert.NotEmpty(t, job.FileKey)
	assert.NotEmpty(t, job.FileName)
	assert.NotEmpty(t, job.SHA256)
}

func TestExportWorker_ProcessJob_UnsupportedReportType(t *testing.T) {
	t.Parallel()

	ctrl := gomock.NewController(t)
	jobRepo := repomocks.NewMockExportJobRepository(ctrl)
	storage := portsmocks.NewMockObjectStorageClient(ctrl)
	cfg := ExportWorkerConfig{
		PageSize:          100,
		TempDir:           t.TempDir(),
		MaxRetries:        3,
		InitialBackoff:    1,
		MaxBackoff:        1,
		BackoffMultiplier: 1.0,
	}
	logger := &libLog.NopLogger{}

	reportRepo := &mockReportRepoForWorker{}

	worker, err := NewExportWorker(jobRepo, reportRepo, storage, cfg, logger)
	require.NoError(t, err)

	job := &entities.ExportJob{
		ID:         uuid.New(),
		TenantID:   uuid.New(),
		ContextID:  uuid.New(),
		ReportType: "UNKNOWN",
		Format:     entities.ExportFormatCSV,
		Filter:     entities.ExportJobFilter{},
		Attempts:   1,
		Status:     entities.ExportJobStatusRunning,
	}

	jobRepo.EXPECT().
		RequeueForRetry(gomock.Any(), gomock.Any()).
		Return(nil)

	worker.processJob(context.Background(), job)

	assert.Equal(t, entities.ExportJobStatusQueued, job.Status)
	assert.Contains(t, job.Error, "unsupported report type")
	assert.Contains(t, job.Error, "UNKNOWN")
	assert.NotNil(t, job.NextRetryAt)
}

// --- Fetch Error Tests for JSON/XML ---

func TestExportWorker_StreamMatchedJSON_FetchError(t *testing.T) {
	t.Parallel()

	ctrl := gomock.NewController(t)
	jobRepo := repomocks.NewMockExportJobRepository(ctrl)
	storage := portsmocks.NewMockObjectStorageClient(ctrl)
	cfg := ExportWorkerConfig{PageSize: 100}
	logger := &libLog.NopLogger{}

	reportRepo := &mockReportRepoForWorker{
		matchedErr: errors.New("db error"),
	}

	worker, err := NewExportWorker(jobRepo, reportRepo, storage, cfg, logger)
	require.NoError(t, err)

	job := &entities.ExportJob{
		ID:        uuid.New(),
		ContextID: uuid.New(),
		Format:    entities.ExportFormatJSON,
	}

	filter := entities.ReportFilter{ContextID: job.ContextID}

	var buf strings.Builder

	count, err := worker.streamMatchedJSON(context.Background(), job, filter, &buf)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "fetching matched page")
	assert.Equal(t, int64(0), count)
}

func TestExportWorker_StreamMatchedXML_FetchError(t *testing.T) {
	t.Parallel()

	ctrl := gomock.NewController(t)
	jobRepo := repomocks.NewMockExportJobRepository(ctrl)
	storage := portsmocks.NewMockObjectStorageClient(ctrl)
	cfg := ExportWorkerConfig{PageSize: 100}
	logger := &libLog.NopLogger{}

	reportRepo := &mockReportRepoForWorker{
		matchedErr: errors.New("db error"),
	}

	worker, err := NewExportWorker(jobRepo, reportRepo, storage, cfg, logger)
	require.NoError(t, err)

	job := &entities.ExportJob{
		ID:        uuid.New(),
		ContextID: uuid.New(),
		Format:    entities.ExportFormatXML,
	}

	filter := entities.ReportFilter{ContextID: job.ContextID}

	var buf strings.Builder

	count, err := worker.streamMatchedXML(context.Background(), job, filter, &buf)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "fetching matched page")
	assert.Equal(t, int64(0), count)
}

func TestExportWorker_StreamUnmatchedJSON_FetchError(t *testing.T) {
	t.Parallel()

	ctrl := gomock.NewController(t)
	jobRepo := repomocks.NewMockExportJobRepository(ctrl)
	storage := portsmocks.NewMockObjectStorageClient(ctrl)
	cfg := ExportWorkerConfig{PageSize: 100}
	logger := &libLog.NopLogger{}

	reportRepo := &mockReportRepoForWorker{
		unmatchedErr: errors.New("db error"),
	}

	worker, err := NewExportWorker(jobRepo, reportRepo, storage, cfg, logger)
	require.NoError(t, err)

	job := &entities.ExportJob{
		ID:        uuid.New(),
		ContextID: uuid.New(),
		Format:    entities.ExportFormatJSON,
	}

	filter := entities.ReportFilter{ContextID: job.ContextID}

	var buf strings.Builder

	count, err := worker.streamUnmatchedJSON(context.Background(), job, filter, &buf)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "fetching unmatched page")
	assert.Equal(t, int64(0), count)
}

func TestExportWorker_StreamUnmatchedXML_FetchError(t *testing.T) {
	t.Parallel()

	ctrl := gomock.NewController(t)
	jobRepo := repomocks.NewMockExportJobRepository(ctrl)
	storage := portsmocks.NewMockObjectStorageClient(ctrl)
	cfg := ExportWorkerConfig{PageSize: 100}
	logger := &libLog.NopLogger{}

	reportRepo := &mockReportRepoForWorker{
		unmatchedErr: errors.New("db error"),
	}

	worker, err := NewExportWorker(jobRepo, reportRepo, storage, cfg, logger)
	require.NoError(t, err)

	job := &entities.ExportJob{
		ID:        uuid.New(),
		ContextID: uuid.New(),
		Format:    entities.ExportFormatXML,
	}

	filter := entities.ReportFilter{ContextID: job.ContextID}

	var buf strings.Builder

	count, err := worker.streamUnmatchedXML(context.Background(), job, filter, &buf)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "fetching unmatched page")
	assert.Equal(t, int64(0), count)
}

func TestExportWorker_StreamVarianceJSON_FetchError(t *testing.T) {
	t.Parallel()

	ctrl := gomock.NewController(t)
	jobRepo := repomocks.NewMockExportJobRepository(ctrl)
	storage := portsmocks.NewMockObjectStorageClient(ctrl)
	cfg := ExportWorkerConfig{PageSize: 100}
	logger := &libLog.NopLogger{}

	reportRepo := &mockReportRepoForWorker{
		varianceErr: errors.New("db error"),
	}

	worker, err := NewExportWorker(jobRepo, reportRepo, storage, cfg, logger)
	require.NoError(t, err)

	job := &entities.ExportJob{
		ID:        uuid.New(),
		ContextID: uuid.New(),
		Format:    entities.ExportFormatJSON,
	}

	filter := entities.VarianceReportFilter{ContextID: job.ContextID}

	var buf strings.Builder

	count, err := worker.streamVarianceJSON(context.Background(), job, filter, &buf)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "fetching variance page")
	assert.Equal(t, int64(0), count)
}

func TestExportWorker_StreamVarianceXML_FetchError(t *testing.T) {
	t.Parallel()

	ctrl := gomock.NewController(t)
	jobRepo := repomocks.NewMockExportJobRepository(ctrl)
	storage := portsmocks.NewMockObjectStorageClient(ctrl)
	cfg := ExportWorkerConfig{PageSize: 100}
	logger := &libLog.NopLogger{}

	reportRepo := &mockReportRepoForWorker{
		varianceErr: errors.New("db error"),
	}

	worker, err := NewExportWorker(jobRepo, reportRepo, storage, cfg, logger)
	require.NoError(t, err)

	job := &entities.ExportJob{
		ID:        uuid.New(),
		ContextID: uuid.New(),
		Format:    entities.ExportFormatXML,
	}

	filter := entities.VarianceReportFilter{ContextID: job.ContextID}

	var buf strings.Builder

	count, err := worker.streamVarianceXML(context.Background(), job, filter, &buf)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "fetching variance page")
	assert.Equal(t, int64(0), count)
}

// --- ProcessJob with nil logger ---

func TestExportWorker_ProcessJob_NilLogger(t *testing.T) {
	t.Parallel()

	ctrl := gomock.NewController(t)
	jobRepo := repomocks.NewMockExportJobRepository(ctrl)
	storage := portsmocks.NewMockObjectStorageClient(ctrl)
	cfg := ExportWorkerConfig{PageSize: 100, TempDir: t.TempDir()}

	reportRepo := &mockReportRepoForWorker{
		matchedItems:   []*entities.MatchedItem{{TransactionID: uuid.New(), MatchGroupID: uuid.New(), SourceID: uuid.New()}},
		matchedNextKey: "",
	}

	worker, err := NewExportWorker(jobRepo, reportRepo, storage, cfg, &libLog.NopLogger{})
	require.NoError(t, err)

	job := &entities.ExportJob{
		ID:         uuid.New(),
		TenantID:   uuid.New(),
		ContextID:  uuid.New(),
		ReportType: entities.ExportReportTypeMatched,
		Format:     entities.ExportFormatCSV,
		Filter:     entities.ExportJobFilter{},
		Status:     entities.ExportJobStatusRunning,
	}

	jobRepo.EXPECT().
		UpdateProgress(gomock.Any(), job.ID, gomock.Any(), gomock.Any()).
		Return(nil).
		AnyTimes()

	storage.EXPECT().
		Upload(gomock.Any(), gomock.Any(), gomock.Any(), "text/csv").
		Return("key", nil)

	jobRepo.EXPECT().
		Update(gomock.Any(), gomock.Any()).
		Return(nil)

	worker.processJob(context.Background(), job)

	assert.Equal(t, entities.ExportJobStatusSucceeded, job.Status)
	assert.NotEmpty(t, job.FileKey)
	assert.NotEmpty(t, job.FileName)
	assert.NotEmpty(t, job.SHA256)
}
