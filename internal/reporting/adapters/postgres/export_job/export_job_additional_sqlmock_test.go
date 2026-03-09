//go:build unit

package export_job

import (
	"context"
	"database/sql"
	"encoding/json"
	"testing"
	"time"

	sqlmock "github.com/DATA-DOG/go-sqlmock"
	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	pkgHTTP "github.com/LerianStudio/lib-commons/v4/commons/net/http"
	"github.com/LerianStudio/matcher/internal/reporting/domain/entities"
	"github.com/LerianStudio/matcher/internal/reporting/domain/repositories"
)

// helper to create a valid ExportJob for tests.
func newTestJob(t *testing.T) *entities.ExportJob {
	t.Helper()

	ctx := context.Background()
	job, err := entities.NewExportJob(
		ctx,
		uuid.New(),
		uuid.New(),
		entities.ExportReportTypeMatched,
		entities.ExportFormatCSV,
		entities.ExportJobFilter{
			DateFrom: time.Now().UTC().Add(-24 * time.Hour),
			DateTo:   time.Now().UTC(),
		},
	)
	require.NoError(t, err)

	return job
}

// helper to build export job result rows.
func exportJobResultRows(t *testing.T, job *entities.ExportJob) *sqlmock.Rows {
	t.Helper()

	filterJSON, err := job.Filter.ToJSON()
	require.NoError(t, err)

	return sqlmock.NewRows(exportJobColumns()).AddRow(
		job.ID,
		job.TenantID,
		job.ContextID,
		job.ReportType,
		job.Format,
		filterJSON,
		job.Status,
		job.RecordsWritten,
		job.BytesWritten,
		sql.NullString{}, // file_key
		sql.NullString{}, // file_name
		sql.NullString{}, // sha256
		sql.NullString{}, // error
		job.Attempts,     // attempts
		sql.NullTime{},   // next_retry_at
		job.CreatedAt,    // created_at
		sql.NullTime{},   // started_at
		sql.NullTime{},   // finished_at
		job.ExpiresAt,    // expires_at
		job.UpdatedAt,    // updated_at
	)
}

// --- Create error paths ---

func TestRepository_Create_ExecError(t *testing.T) {
	t.Parallel()

	repo, mock, finish := setupRepository(t)
	defer finish()

	ctx := context.Background()
	job := newTestJob(t)

	mock.ExpectBegin()
	mock.ExpectExec("INSERT INTO export_jobs").
		WillReturnError(sql.ErrConnDone)
	mock.ExpectRollback()

	err := repo.Create(ctx, job)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "create export job")
}

// --- CreateWithTx tests ---

func TestRepository_CreateWithTx_Success(t *testing.T) {
	t.Parallel()

	repo, _, finish := setupRepository(t)
	defer finish()

	db, mock, err := sqlmock.New()
	require.NoError(t, err)
	defer db.Close()

	ctx := context.Background()
	job := newTestJob(t)

	mock.ExpectBegin()
	mock.ExpectExec("INSERT INTO export_jobs").
		WillReturnResult(sqlmock.NewResult(1, 1))

	tx, err := db.Begin()
	require.NoError(t, err)

	err = repo.CreateWithTx(ctx, tx, job)
	require.NoError(t, err)
}

func TestRepository_CreateWithTx_ExecError(t *testing.T) {
	t.Parallel()

	repo, _, finish := setupRepository(t)
	defer finish()

	db, mock, err := sqlmock.New()
	require.NoError(t, err)
	defer db.Close()

	ctx := context.Background()
	job := newTestJob(t)

	mock.ExpectBegin()
	mock.ExpectExec("INSERT INTO export_jobs").
		WillReturnError(sql.ErrConnDone)

	tx, err := db.Begin()
	require.NoError(t, err)

	err = repo.CreateWithTx(ctx, tx, job)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "exec insert")
}

// --- GetByID success path ---

func TestRepository_GetByID_Success(t *testing.T) {
	t.Parallel()

	repo, mock, finish := setupRepository(t)
	defer finish()

	ctx := context.Background()
	job := newTestJob(t)

	rows := exportJobResultRows(t, job)

	mock.ExpectBegin()
	mock.ExpectQuery("SELECT").WillReturnRows(rows)
	mock.ExpectCommit()

	result, err := repo.GetByID(ctx, job.ID)
	require.NoError(t, err)
	require.NotNil(t, result)
	assert.Equal(t, job.ID, result.ID)
	assert.Equal(t, job.TenantID, result.TenantID)
	assert.Equal(t, job.ContextID, result.ContextID)
	assert.Equal(t, job.ReportType, result.ReportType)
	assert.Equal(t, job.Format, result.Format)
	assert.Equal(t, job.Status, result.Status)
}

func TestRepository_GetByID_QueryError(t *testing.T) {
	t.Parallel()

	repo, mock, finish := setupRepository(t)
	defer finish()

	ctx := context.Background()
	jobID := uuid.New()

	mock.ExpectBegin()
	mock.ExpectQuery("SELECT").
		WillReturnError(sql.ErrConnDone)
	mock.ExpectRollback()

	job, err := repo.GetByID(ctx, jobID)
	assert.Nil(t, job)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "get export job by id")
}

// --- Update error paths ---

func TestRepository_Update_ExecError(t *testing.T) {
	t.Parallel()

	repo, mock, finish := setupRepository(t)
	defer finish()

	ctx := context.Background()
	job := newTestJob(t)

	mock.ExpectBegin()
	mock.ExpectExec("UPDATE export_jobs").
		WillReturnError(sql.ErrConnDone)
	mock.ExpectRollback()

	err := repo.Update(ctx, job)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "update export job")
}

// --- UpdateWithTx tests ---

func TestRepository_UpdateWithTx_Success(t *testing.T) {
	t.Parallel()

	repo, _, finish := setupRepository(t)
	defer finish()

	db, mock, err := sqlmock.New()
	require.NoError(t, err)
	defer db.Close()

	ctx := context.Background()
	job := newTestJob(t)

	mock.ExpectBegin()
	mock.ExpectExec("UPDATE export_jobs").
		WillReturnResult(sqlmock.NewResult(0, 1))

	tx, err := db.Begin()
	require.NoError(t, err)

	err = repo.UpdateWithTx(ctx, tx, job)
	require.NoError(t, err)
}

func TestRepository_UpdateWithTx_NotFound(t *testing.T) {
	t.Parallel()

	repo, _, finish := setupRepository(t)
	defer finish()

	db, mock, err := sqlmock.New()
	require.NoError(t, err)
	defer db.Close()

	ctx := context.Background()
	job := newTestJob(t)

	mock.ExpectBegin()
	mock.ExpectExec("UPDATE export_jobs").
		WillReturnResult(sqlmock.NewResult(0, 0))

	tx, err := db.Begin()
	require.NoError(t, err)

	err = repo.UpdateWithTx(ctx, tx, job)
	require.Error(t, err)
	require.ErrorIs(t, err, repositories.ErrExportJobNotFound)
}

// --- UpdateStatus tests ---

func TestRepository_UpdateStatus_Success(t *testing.T) {
	t.Parallel()

	repo, mock, finish := setupRepository(t)
	defer finish()

	ctx := context.Background()
	job := newTestJob(t)
	job.MarkRunning()

	mock.ExpectBegin()
	mock.ExpectExec("UPDATE export_jobs").
		WillReturnResult(sqlmock.NewResult(0, 1))
	mock.ExpectCommit()

	err := repo.UpdateStatus(ctx, job)
	require.NoError(t, err)
}

// --- UpdateStatusWithTx tests ---

func TestRepository_UpdateStatusWithTx_Success(t *testing.T) {
	t.Parallel()

	repo, _, finish := setupRepository(t)
	defer finish()

	db, mock, err := sqlmock.New()
	require.NoError(t, err)
	defer db.Close()

	ctx := context.Background()
	job := newTestJob(t)
	job.MarkRunning()

	mock.ExpectBegin()
	mock.ExpectExec("UPDATE export_jobs").
		WillReturnResult(sqlmock.NewResult(0, 1))

	tx, err := db.Begin()
	require.NoError(t, err)

	err = repo.UpdateStatusWithTx(ctx, tx, job)
	require.NoError(t, err)
}

// --- UpdateProgress error paths ---

func TestRepository_UpdateProgress_ExecError(t *testing.T) {
	t.Parallel()

	repo, mock, finish := setupRepository(t)
	defer finish()

	ctx := context.Background()
	jobID := uuid.New()

	mock.ExpectBegin()
	mock.ExpectExec("UPDATE export_jobs").
		WillReturnError(sql.ErrConnDone)
	mock.ExpectRollback()

	err := repo.UpdateProgress(ctx, jobID, 100, 5000)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "update export job progress")
}

// --- UpdateProgressWithTx tests ---

func TestRepository_UpdateProgressWithTx_Success(t *testing.T) {
	t.Parallel()

	repo, _, finish := setupRepository(t)
	defer finish()

	db, mock, err := sqlmock.New()
	require.NoError(t, err)
	defer db.Close()

	ctx := context.Background()
	jobID := uuid.New()

	mock.ExpectBegin()
	mock.ExpectExec("UPDATE export_jobs").
		WillReturnResult(sqlmock.NewResult(0, 1))

	tx, err := db.Begin()
	require.NoError(t, err)

	err = repo.UpdateProgressWithTx(ctx, tx, jobID, 50, 2500)
	require.NoError(t, err)
}

func TestRepository_UpdateProgressWithTx_ExecError(t *testing.T) {
	t.Parallel()

	repo, _, finish := setupRepository(t)
	defer finish()

	db, mock, err := sqlmock.New()
	require.NoError(t, err)
	defer db.Close()

	ctx := context.Background()
	jobID := uuid.New()

	mock.ExpectBegin()
	mock.ExpectExec("UPDATE export_jobs").
		WillReturnError(sql.ErrConnDone)

	tx, err := db.Begin()
	require.NoError(t, err)

	err = repo.UpdateProgressWithTx(ctx, tx, jobID, 50, 2500)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "exec update")
}

// --- Delete error paths ---

func TestRepository_Delete_ExecError(t *testing.T) {
	t.Parallel()

	repo, mock, finish := setupRepository(t)
	defer finish()

	ctx := context.Background()
	jobID := uuid.New()

	mock.ExpectBegin()
	mock.ExpectExec("DELETE FROM export_jobs").
		WillReturnError(sql.ErrConnDone)
	mock.ExpectRollback()

	err := repo.Delete(ctx, jobID)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "delete export job")
}

// --- DeleteWithTx tests ---

func TestRepository_DeleteWithTx_Success(t *testing.T) {
	t.Parallel()

	repo, _, finish := setupRepository(t)
	defer finish()

	db, mock, err := sqlmock.New()
	require.NoError(t, err)
	defer db.Close()

	ctx := context.Background()
	jobID := uuid.New()

	mock.ExpectBegin()
	mock.ExpectExec("DELETE FROM export_jobs").
		WillReturnResult(sqlmock.NewResult(0, 1))

	tx, err := db.Begin()
	require.NoError(t, err)

	err = repo.DeleteWithTx(ctx, tx, jobID)
	require.NoError(t, err)
}

func TestRepository_DeleteWithTx_ExecError(t *testing.T) {
	t.Parallel()

	repo, _, finish := setupRepository(t)
	defer finish()

	db, mock, err := sqlmock.New()
	require.NoError(t, err)
	defer db.Close()

	ctx := context.Background()
	jobID := uuid.New()

	mock.ExpectBegin()
	mock.ExpectExec("DELETE FROM export_jobs").
		WillReturnError(sql.ErrConnDone)

	tx, err := db.Begin()
	require.NoError(t, err)

	err = repo.DeleteWithTx(ctx, tx, jobID)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "exec delete")
}

// --- List with cursor pagination ---

func TestRepository_List_WithCursor(t *testing.T) {
	t.Parallel()

	repo, mock, finish := setupRepository(t)
	defer finish()

	ctx := context.Background()
	cursorTime := time.Now().UTC().Add(-1 * time.Hour)
	cursorID := uuid.New()
	cursor := &pkgHTTP.TimestampCursor{Timestamp: cursorTime, ID: cursorID}

	mock.ExpectBegin()
	mock.ExpectQuery("SELECT").
		WillReturnRows(sqlmock.NewRows(exportJobColumns()))
	mock.ExpectCommit()

	jobs, pagination, err := repo.List(ctx, nil, cursor, 10)
	require.NoError(t, err)
	assert.Empty(t, jobs)
	assert.Empty(t, pagination.Next)
}

func TestRepository_List_QueryError(t *testing.T) {
	t.Parallel()

	repo, mock, finish := setupRepository(t)
	defer finish()

	ctx := context.Background()

	mock.ExpectBegin()
	mock.ExpectQuery("SELECT").
		WillReturnError(sql.ErrConnDone)
	mock.ExpectRollback()

	_, _, err := repo.List(ctx, nil, nil, 10)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "list export jobs")
}

func TestRepository_List_WithResults(t *testing.T) {
	t.Parallel()

	repo, mock, finish := setupRepository(t)
	defer finish()

	ctx := context.Background()
	job := newTestJob(t)
	rows := exportJobResultRows(t, job)

	mock.ExpectBegin()
	mock.ExpectQuery("SELECT").WillReturnRows(rows)
	mock.ExpectCommit()

	jobs, _, err := repo.List(ctx, nil, nil, 10)
	require.NoError(t, err)
	assert.Len(t, jobs, 1)
	assert.Equal(t, job.ID, jobs[0].ID)
}

func TestRepository_List_HasMoreResults(t *testing.T) {
	t.Parallel()

	repo, mock, finish := setupRepository(t)
	defer finish()

	ctx := context.Background()

	// Create 2 jobs (limit=1 means 2 results triggers hasMore)
	job1 := newTestJob(t)
	job2 := newTestJob(t)
	filter1JSON, err := job1.Filter.ToJSON()
	require.NoError(t, err)

	filter2JSON, err := job2.Filter.ToJSON()
	require.NoError(t, err)

	rows := sqlmock.NewRows(exportJobColumns()).
		AddRow(
			job1.ID, job1.TenantID, job1.ContextID, job1.ReportType, job1.Format,
			filter1JSON, job1.Status, job1.RecordsWritten, job1.BytesWritten,
			sql.NullString{}, sql.NullString{}, sql.NullString{}, sql.NullString{},
			job1.Attempts, sql.NullTime{}, job1.CreatedAt, sql.NullTime{}, sql.NullTime{},
			job1.ExpiresAt, job1.UpdatedAt,
		).
		AddRow(
			job2.ID, job2.TenantID, job2.ContextID, job2.ReportType, job2.Format,
			filter2JSON, job2.Status, job2.RecordsWritten, job2.BytesWritten,
			sql.NullString{}, sql.NullString{}, sql.NullString{}, sql.NullString{},
			job2.Attempts, sql.NullTime{}, job2.CreatedAt, sql.NullTime{}, sql.NullTime{},
			job2.ExpiresAt, job2.UpdatedAt,
		)

	mock.ExpectBegin()
	mock.ExpectQuery("SELECT").WillReturnRows(rows)
	mock.ExpectCommit()

	jobs, pagination, err := repo.List(ctx, nil, nil, 1)
	require.NoError(t, err)
	assert.Len(t, jobs, 1)
	assert.NotEmpty(t, pagination.Next, "should have a next cursor when there are more results")
}

func TestTrimExportJobsAndEncodeNextCursor_PropagatesEncoderError(t *testing.T) {
	t.Parallel()

	now := time.Now().UTC()
	records := []*entities.ExportJob{
		{ID: uuid.New(), CreatedAt: now},
		{ID: uuid.New(), CreatedAt: now.Add(-time.Minute)},
	}

	trimmed, cursor, err := trimExportJobsAndEncodeNextCursor(
		records,
		1,
		func(_ time.Time, _ uuid.UUID) (string, error) {
			return "", sql.ErrTxDone
		},
	)

	require.Error(t, err)
	assert.ErrorIs(t, err, sql.ErrTxDone)
	assert.Len(t, trimmed, 1)
	assert.Empty(t, cursor)
}

func TestTrimExportJobsAndEncodeNextCursor_NilEncoder(t *testing.T) {
	t.Parallel()

	now := time.Now().UTC()
	records := []*entities.ExportJob{
		{ID: uuid.New(), CreatedAt: now},
		{ID: uuid.New(), CreatedAt: now.Add(-time.Minute)},
	}

	trimmed, cursor, err := trimExportJobsAndEncodeNextCursor(records, 1, nil)

	require.Error(t, err)
	assert.ErrorIs(t, err, ErrCursorEncoderRequired)
	assert.Len(t, trimmed, 1)
	assert.Empty(t, cursor)
}

// --- ListByContext error paths ---

func TestRepository_ListByContext_QueryError(t *testing.T) {
	t.Parallel()

	repo, mock, finish := setupRepository(t)
	defer finish()

	ctx := context.Background()
	contextID := uuid.New()

	mock.ExpectBegin()
	mock.ExpectQuery("SELECT").
		WillReturnError(sql.ErrConnDone)
	mock.ExpectRollback()

	_, err := repo.ListByContext(ctx, contextID, 10)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "list export jobs by context")
}

func TestRepository_ListByContext_WithResults(t *testing.T) {
	t.Parallel()

	repo, mock, finish := setupRepository(t)
	defer finish()

	ctx := context.Background()
	contextID := uuid.New()
	job := newTestJob(t)
	rows := exportJobResultRows(t, job)

	mock.ExpectBegin()
	mock.ExpectQuery("SELECT").WillReturnRows(rows)
	mock.ExpectCommit()

	jobs, err := repo.ListByContext(ctx, contextID, 10)
	require.NoError(t, err)
	assert.Len(t, jobs, 1)
}

// --- ListExpired error paths ---

func TestRepository_ListExpired_QueryError(t *testing.T) {
	t.Parallel()

	repo, mock, finish := setupRepository(t)
	defer finish()

	ctx := context.Background()

	mock.ExpectBegin()
	mock.ExpectQuery("SELECT").
		WillReturnError(sql.ErrConnDone)
	mock.ExpectRollback()

	_, err := repo.ListExpired(ctx, 10)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "list expired export jobs")
}

func TestRepository_ListExpired_WithResults(t *testing.T) {
	t.Parallel()

	repo, mock, finish := setupRepository(t)
	defer finish()

	ctx := context.Background()
	job := newTestJob(t)
	rows := exportJobResultRows(t, job)

	mock.ExpectBegin()
	mock.ExpectQuery("SELECT").WillReturnRows(rows)
	mock.ExpectCommit()

	jobs, err := repo.ListExpired(ctx, 10)
	require.NoError(t, err)
	assert.Len(t, jobs, 1)
}

// --- ClaimNextQueued tests ---

func TestRepository_ClaimNextQueued_Success(t *testing.T) {
	t.Parallel()

	repo, mock, finish := setupRepository(t)
	defer finish()

	ctx := context.Background()
	job := newTestJob(t)
	job.MarkRunning()

	filterJSON, err := job.Filter.ToJSON()
	require.NoError(t, err)

	startedAt := sql.NullTime{Time: *job.StartedAt, Valid: true}

	rows := sqlmock.NewRows(exportJobColumns()).AddRow(
		job.ID, job.TenantID, job.ContextID, job.ReportType, job.Format,
		filterJSON, job.Status, job.RecordsWritten, job.BytesWritten,
		sql.NullString{}, sql.NullString{}, sql.NullString{}, sql.NullString{},
		job.Attempts, sql.NullTime{}, job.CreatedAt, startedAt, sql.NullTime{},
		job.ExpiresAt, job.UpdatedAt,
	)

	mock.ExpectBegin()
	mock.ExpectQuery("UPDATE export_jobs").WillReturnRows(rows)
	mock.ExpectCommit()

	result, err := repo.ClaimNextQueued(ctx)
	require.NoError(t, err)
	require.NotNil(t, result)
	assert.Equal(t, job.ID, result.ID)
	assert.Equal(t, entities.ExportJobStatusRunning, result.Status)
}

func TestRepository_ClaimNextQueued_QueryError(t *testing.T) {
	t.Parallel()

	repo, mock, finish := setupRepository(t)
	defer finish()

	ctx := context.Background()

	mock.ExpectBegin()
	mock.ExpectQuery("UPDATE export_jobs").
		WillReturnError(sql.ErrConnDone)
	mock.ExpectRollback()

	_, err := repo.ClaimNextQueued(ctx)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "claim next queued export job")
}

// --- RequeueForRetry error paths ---

func TestRepository_RequeueForRetry_ExecError(t *testing.T) {
	t.Parallel()

	repo, mock, finish := setupRepository(t)
	defer finish()

	ctx := context.Background()
	job := newTestJob(t)

	mock.ExpectBegin()
	mock.ExpectExec("UPDATE export_jobs").
		WillReturnError(sql.ErrConnDone)
	mock.ExpectRollback()

	err := repo.RequeueForRetry(ctx, job)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "requeue export job for retry")
}

// --- scanExportJob with nullable fields populated ---

func TestScanExportJob_WithOptionalFields(t *testing.T) {
	t.Parallel()

	db, mock, err := sqlmock.New()
	require.NoError(t, err)
	defer db.Close()

	now := time.Now().UTC()
	jobID := uuid.New()
	tenantID := uuid.New()
	contextID := uuid.New()
	retryTime := now.Add(5 * time.Minute)
	filterJSON, _ := json.Marshal(entities.ExportJobFilter{
		DateFrom: now.Add(-24 * time.Hour),
		DateTo:   now,
	})

	rows := sqlmock.NewRows(exportJobColumns()).AddRow(
		jobID, tenantID, contextID, "MATCHED", "CSV",
		filterJSON, "SUCCEEDED", int64(100), int64(5000),
		sql.NullString{String: "file-key-123", Valid: true},
		sql.NullString{String: "export.csv", Valid: true},
		sql.NullString{String: "sha256hash", Valid: true},
		sql.NullString{},
		3,
		sql.NullTime{Time: retryTime, Valid: true},
		now, // created_at
		sql.NullTime{Time: now.Add(-5 * time.Minute), Valid: true}, // started_at
		sql.NullTime{Time: now, Valid: true},                       // finished_at
		now.Add(7*24*time.Hour),                                    // expires_at
		now,                                                        // updated_at
	)

	mock.ExpectQuery("SELECT").WillReturnRows(rows)

	row := db.QueryRow("SELECT 1")
	job, err := scanExportJob(row)

	require.NoError(t, err)
	require.NotNil(t, job)
	assert.Equal(t, jobID, job.ID)
	assert.Equal(t, tenantID, job.TenantID)
	assert.Equal(t, "file-key-123", job.FileKey)
	assert.Equal(t, "export.csv", job.FileName)
	assert.Equal(t, "sha256hash", job.SHA256)
	assert.Equal(t, 3, job.Attempts)
	assert.NotNil(t, job.NextRetryAt)
	assert.NotNil(t, job.StartedAt)
	assert.NotNil(t, job.FinishedAt)
	assert.Equal(t, int64(100), job.RecordsWritten)
	assert.Equal(t, int64(5000), job.BytesWritten)
}

// --- scanExportJobs tests ---

func TestScanExportJobs_Success(t *testing.T) {
	t.Parallel()

	db, mock, err := sqlmock.New()
	require.NoError(t, err)
	defer db.Close()

	now := time.Now().UTC()
	job1ID := uuid.New()
	job2ID := uuid.New()
	tenantID := uuid.New()
	contextID := uuid.New()
	filterJSON, _ := json.Marshal(entities.ExportJobFilter{
		DateFrom: now.Add(-24 * time.Hour),
		DateTo:   now,
	})

	rows := sqlmock.NewRows(exportJobColumns()).
		AddRow(
			job1ID, tenantID, contextID, "MATCHED", "CSV",
			filterJSON, "QUEUED", int64(0), int64(0),
			sql.NullString{}, sql.NullString{}, sql.NullString{}, sql.NullString{},
			0, sql.NullTime{}, now, sql.NullTime{}, sql.NullTime{},
			now.Add(7*24*time.Hour), now,
		).
		AddRow(
			job2ID, tenantID, contextID, "UNMATCHED", "JSON",
			filterJSON, "RUNNING", int64(50), int64(2500),
			sql.NullString{}, sql.NullString{}, sql.NullString{}, sql.NullString{},
			1, sql.NullTime{}, now, sql.NullTime{Time: now, Valid: true}, sql.NullTime{},
			now.Add(7*24*time.Hour), now,
		)

	mock.ExpectQuery("SELECT").WillReturnRows(rows)

	sqlRows, err := db.Query("SELECT 1")
	require.NoError(t, err)

	jobs, err := scanExportJobs(sqlRows, nil)
	require.NoError(t, err)
	assert.Len(t, jobs, 2)
	assert.Equal(t, job1ID, jobs[0].ID)
	assert.Equal(t, job2ID, jobs[1].ID)
}

func TestScanExportJobs_QueryError(t *testing.T) {
	t.Parallel()

	jobs, err := scanExportJobs(nil, sql.ErrConnDone)
	require.Error(t, err)
	assert.Nil(t, jobs)
	assert.Contains(t, err.Error(), "query export jobs")
}

func TestScanExportJobs_InvalidJSON(t *testing.T) {
	t.Parallel()

	db, mock, err := sqlmock.New()
	require.NoError(t, err)
	defer db.Close()

	now := time.Now().UTC()
	jobID := uuid.New()

	rows := sqlmock.NewRows(exportJobColumns()).AddRow(
		jobID, uuid.New(), uuid.New(), "MATCHED", "CSV",
		[]byte("invalid-json"), "QUEUED", int64(0), int64(0),
		sql.NullString{}, sql.NullString{}, sql.NullString{}, sql.NullString{},
		0, sql.NullTime{}, now, sql.NullTime{}, sql.NullTime{},
		now.Add(7*24*time.Hour), now,
	)

	mock.ExpectQuery("SELECT").WillReturnRows(rows)

	sqlRows, err := db.Query("SELECT 1")
	require.NoError(t, err)

	jobs, err := scanExportJobs(sqlRows, nil)
	require.Error(t, err)
	assert.Nil(t, jobs)
	assert.Contains(t, err.Error(), "unmarshal filter")
}

func TestScanExportJobs_Empty(t *testing.T) {
	t.Parallel()

	db, mock, err := sqlmock.New()
	require.NoError(t, err)
	defer db.Close()

	rows := sqlmock.NewRows(exportJobColumns())
	mock.ExpectQuery("SELECT").WillReturnRows(rows)

	sqlRows, err := db.Query("SELECT 1")
	require.NoError(t, err)

	jobs, err := scanExportJobs(sqlRows, nil)
	require.NoError(t, err)
	assert.Empty(t, jobs)
}

// --- scanExportJob invalid JSON filter ---

func TestScanExportJob_InvalidFilterJSON(t *testing.T) {
	t.Parallel()

	db, mock, err := sqlmock.New()
	require.NoError(t, err)
	defer db.Close()

	now := time.Now().UTC()
	rows := sqlmock.NewRows(exportJobColumns()).AddRow(
		uuid.New(), uuid.New(), uuid.New(), "MATCHED", "CSV",
		[]byte("not-json"), "QUEUED", int64(0), int64(0),
		sql.NullString{}, sql.NullString{}, sql.NullString{}, sql.NullString{},
		0, sql.NullTime{}, now, sql.NullTime{}, sql.NullTime{},
		now.Add(7*24*time.Hour), now,
	)

	mock.ExpectQuery("SELECT").WillReturnRows(rows)

	row := db.QueryRow("SELECT 1")
	job, err := scanExportJob(row)

	require.Error(t, err)
	assert.Nil(t, job)
	assert.Contains(t, err.Error(), "unmarshal filter")
}

// --- scanExportJob scan error (not ErrNoRows) ---

func TestScanExportJob_ScanError(t *testing.T) {
	t.Parallel()

	db, mock, err := sqlmock.New()
	require.NoError(t, err)
	defer db.Close()

	// Return wrong number of columns to trigger scan error
	rows := sqlmock.NewRows([]string{"id"}).AddRow(uuid.New())
	mock.ExpectQuery("SELECT").WillReturnRows(rows)

	row := db.QueryRow("SELECT 1")
	job, err := scanExportJob(row)

	require.Error(t, err)
	assert.Nil(t, job)
	assert.Contains(t, err.Error(), "scan export job")
}

// --- scanExportJobs with optional nullable fields populated ---

func TestScanExportJobs_WithOptionalFields(t *testing.T) {
	t.Parallel()

	db, mock, err := sqlmock.New()
	require.NoError(t, err)
	defer db.Close()

	now := time.Now().UTC()
	jobID := uuid.New()
	tenantID := uuid.New()
	contextID := uuid.New()
	retryTime := now.Add(5 * time.Minute)
	filterJSON, _ := json.Marshal(entities.ExportJobFilter{
		DateFrom: now.Add(-24 * time.Hour),
		DateTo:   now,
	})

	rows := sqlmock.NewRows(exportJobColumns()).AddRow(
		jobID, tenantID, contextID, "MATCHED", "CSV",
		filterJSON, "SUCCEEDED", int64(100), int64(5000),
		sql.NullString{String: "file-key-123", Valid: true},
		sql.NullString{String: "export.csv", Valid: true},
		sql.NullString{String: "sha256hash", Valid: true},
		sql.NullString{String: "some error", Valid: true},
		3,
		sql.NullTime{Time: retryTime, Valid: true},
		now,
		sql.NullTime{Time: now.Add(-5 * time.Minute), Valid: true},
		sql.NullTime{Time: now, Valid: true},
		now.Add(7*24*time.Hour),
		now,
	)

	mock.ExpectQuery("SELECT").WillReturnRows(rows)

	sqlRows, err := db.Query("SELECT 1")
	require.NoError(t, err)

	jobs, err := scanExportJobs(sqlRows, nil)
	require.NoError(t, err)
	require.Len(t, jobs, 1)
	assert.Equal(t, "file-key-123", jobs[0].FileKey)
	assert.Equal(t, "export.csv", jobs[0].FileName)
	assert.Equal(t, "sha256hash", jobs[0].SHA256)
	assert.Equal(t, "some error", jobs[0].Error)
	assert.NotNil(t, jobs[0].NextRetryAt)
	assert.NotNil(t, jobs[0].StartedAt)
	assert.NotNil(t, jobs[0].FinishedAt)
}
