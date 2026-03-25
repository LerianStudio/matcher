//go:build unit

package job

import (
	"context"
	"database/sql"
	"errors"
	"regexp"
	"testing"
	"time"

	sqlmock "github.com/DATA-DOG/go-sqlmock"
	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	pkgHTTP "github.com/LerianStudio/lib-commons/v4/commons/net/http"
	"github.com/LerianStudio/matcher/internal/ingestion/domain/entities"
	"github.com/LerianStudio/matcher/internal/ingestion/domain/repositories"
	"github.com/LerianStudio/matcher/internal/ingestion/domain/value_objects"
	sharedpg "github.com/LerianStudio/matcher/internal/shared/adapters/postgres/common"
	"github.com/LerianStudio/matcher/internal/shared/infrastructure/testutil"
	sharedtestutil "github.com/LerianStudio/matcher/internal/shared/testutil"
)

var (
	errTestShouldNotReachConnection = errors.New("should not reach connection")
	errTestConnectionRefused        = errors.New("connection refused")
	errTestDatabaseError            = errors.New("database error")
	errTestConnectionError          = errors.New("connection error")
	errTestUpdateFailed             = errors.New("update failed")
	errTestQueryFailed              = errors.New("query failed")
	errTestFunctionError            = errors.New("function error")
	errTestRowIterationError        = errors.New("row iteration error")
)

func setupMock(t *testing.T) (*Repository, sqlmock.Sqlmock, func()) {
	t.Helper()

	db, mock, err := sqlmock.New()
	require.NoError(t, err)

	provider := testutil.NewMockProviderFromDB(t, db)
	provider.ReplicaDB = db
	repo := NewRepository(provider)

	finish := func() {
		mock.ExpectClose()
		require.NoError(t, db.Close())
		require.NoError(t, mock.ExpectationsWereMet())
	}

	return repo, mock, finish
}

func TestNewRepository_WithProvider(t *testing.T) {
	t.Parallel()

	t.Run("creates repository with provider", func(t *testing.T) {
		t.Parallel()

		provider := &testutil.MockInfrastructureProvider{}
		repo := NewRepository(provider)

		require.NotNil(t, repo)
		assert.Equal(t, provider, repo.provider)
	})

	t.Run("creates repository with nil provider", func(t *testing.T) {
		t.Parallel()

		repo := NewRepository(nil)

		assert.NotNil(t, repo)
		assert.Nil(t, repo.provider)
	})
}

func TestRepository_NilRepository_AllMethods(t *testing.T) {
	t.Parallel()

	ctx := context.Background()

	var repo *Repository

	t.Run("Create with nil repository returns error", func(t *testing.T) {
		t.Parallel()

		result, err := repo.Create(ctx, &entities.IngestionJob{})

		require.Error(t, err)
		require.Nil(t, result)
		require.ErrorIs(t, err, errRepoNotInit)
	})

	t.Run("CreateWithTx with nil repository returns error", func(t *testing.T) {
		t.Parallel()

		result, err := repo.CreateWithTx(ctx, nil, &entities.IngestionJob{})

		require.Error(t, err)
		require.Nil(t, result)
		require.ErrorIs(t, err, errRepoNotInit)
	})

	t.Run("FindByID with nil repository returns error", func(t *testing.T) {
		t.Parallel()

		result, err := repo.FindByID(ctx, uuid.New())

		require.Error(t, err)
		require.Nil(t, result)
		require.ErrorIs(t, err, errRepoNotInit)
	})

	t.Run("FindByContextID with nil repository returns error", func(t *testing.T) {
		t.Parallel()

		result, pagination, err := repo.FindByContextID(
			ctx,
			uuid.New(),
			repositories.CursorFilter{},
		)

		require.Error(t, err)
		require.Nil(t, result)
		require.Empty(t, pagination.Next)
		require.ErrorIs(t, err, errRepoNotInit)
	})

	t.Run("Update with nil repository returns error", func(t *testing.T) {
		t.Parallel()

		result, err := repo.Update(ctx, &entities.IngestionJob{})

		require.Error(t, err)
		require.Nil(t, result)
		require.ErrorIs(t, err, errRepoNotInit)
	})

	t.Run("UpdateWithTx with nil repository returns error", func(t *testing.T) {
		t.Parallel()

		result, err := repo.UpdateWithTx(ctx, nil, &entities.IngestionJob{})

		require.Error(t, err)
		require.Nil(t, result)
		require.ErrorIs(t, err, errRepoNotInit)
	})

	t.Run("WithTx with nil repository returns error", func(t *testing.T) {
		t.Parallel()

		err := repo.WithTx(ctx, func(tx *sql.Tx) error {
			return nil
		})

		require.Error(t, err)
		require.ErrorIs(t, err, errRepoNotInit)
	})
}

func TestRepository_NilProvider_AllMethods(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	repo := &Repository{provider: nil}

	t.Run("Create with nil provider returns error", func(t *testing.T) {
		t.Parallel()

		result, err := repo.Create(ctx, &entities.IngestionJob{})

		require.Error(t, err)
		require.Nil(t, result)
		require.ErrorIs(t, err, errRepoNotInit)
	})

	t.Run("CreateWithTx with nil provider returns error", func(t *testing.T) {
		t.Parallel()

		result, err := repo.CreateWithTx(ctx, nil, &entities.IngestionJob{})

		require.Error(t, err)
		require.Nil(t, result)
		require.ErrorIs(t, err, errRepoNotInit)
	})

	t.Run("FindByID with nil provider returns error", func(t *testing.T) {
		t.Parallel()

		result, err := repo.FindByID(ctx, uuid.New())

		require.Error(t, err)
		require.Nil(t, result)
		require.ErrorIs(t, err, errRepoNotInit)
	})

	t.Run("FindByContextID with nil provider returns error", func(t *testing.T) {
		t.Parallel()

		result, pagination, err := repo.FindByContextID(
			ctx,
			uuid.New(),
			repositories.CursorFilter{},
		)

		require.Error(t, err)
		require.Nil(t, result)
		require.Empty(t, pagination.Next)
		require.ErrorIs(t, err, errRepoNotInit)
	})

	t.Run("Update with nil provider returns error", func(t *testing.T) {
		t.Parallel()

		result, err := repo.Update(ctx, &entities.IngestionJob{})

		require.Error(t, err)
		require.Nil(t, result)
		require.ErrorIs(t, err, errRepoNotInit)
	})

	t.Run("UpdateWithTx with nil provider returns error", func(t *testing.T) {
		t.Parallel()

		result, err := repo.UpdateWithTx(ctx, nil, &entities.IngestionJob{})

		require.Error(t, err)
		require.Nil(t, result)
		require.ErrorIs(t, err, errRepoNotInit)
	})

	t.Run("WithTx with nil provider returns error", func(t *testing.T) {
		t.Parallel()

		err := repo.WithTx(ctx, func(tx *sql.Tx) error {
			return nil
		})

		require.Error(t, err)
		require.ErrorIs(t, err, errRepoNotInit)
	})
}

func TestRepository_NilEntity_ReturnsError(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	provider := &testutil.MockInfrastructureProvider{
		PostgresErr: errTestShouldNotReachConnection,
	}
	repo := NewRepository(provider)

	t.Run("Create with nil job returns error", func(t *testing.T) {
		t.Parallel()

		result, err := repo.Create(ctx, nil)

		require.Error(t, err)
		require.Nil(t, result)
		require.ErrorIs(t, err, errJobEntityRequired)
	})

	t.Run("Update with nil job returns error", func(t *testing.T) {
		t.Parallel()

		result, err := repo.Update(ctx, nil)

		require.Error(t, err)
		require.Nil(t, result)
		require.ErrorIs(t, err, errJobEntityRequired)
	})
}

func TestRepository_ConnectionError_PropagatedCorrectly(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	provider := &testutil.MockInfrastructureProvider{
		PostgresErr: errTestConnectionRefused,
	}
	repo := NewRepository(provider)

	t.Run("Create returns connection error", func(t *testing.T) {
		t.Parallel()

		result, err := repo.Create(ctx, &entities.IngestionJob{
			ID: uuid.New(),
		})

		require.Error(t, err)
		require.Nil(t, result)
		require.ErrorIs(t, err, errTestConnectionRefused)
	})

	t.Run("FindByID returns connection error", func(t *testing.T) {
		t.Parallel()

		result, err := repo.FindByID(ctx, uuid.New())

		require.Error(t, err)
		require.Nil(t, result)
		require.ErrorIs(t, err, errTestConnectionRefused)
	})

	t.Run("FindByContextID returns connection error", func(t *testing.T) {
		t.Parallel()

		result, pagination, err := repo.FindByContextID(
			ctx,
			uuid.New(),
			repositories.CursorFilter{},
		)

		require.Error(t, err)
		require.Nil(t, result)
		require.Empty(t, pagination.Next)
		require.ErrorIs(t, err, errTestConnectionRefused)
	})

	t.Run("Update returns connection error", func(t *testing.T) {
		t.Parallel()

		result, err := repo.Update(ctx, &entities.IngestionJob{
			ID: uuid.New(),
		})

		require.Error(t, err)
		require.Nil(t, result)
		require.ErrorIs(t, err, errTestConnectionRefused)
	})

	t.Run("WithTx returns connection error", func(t *testing.T) {
		t.Parallel()

		err := repo.WithTx(ctx, func(tx *sql.Tx) error {
			return nil
		})

		require.Error(t, err)
		require.ErrorIs(t, err, errTestConnectionRefused)
	})
}

func TestNormalizeJobSortColumn(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name     string
		input    string
		expected string
	}{
		{
			name:     "empty string defaults to id",
			input:    "",
			expected: "id",
		},
		{
			name:     "id returns id",
			input:    "id",
			expected: "id",
		},
		{
			name:     "ID uppercase returns id",
			input:    "ID",
			expected: "id",
		},
		{
			name:     "created_at returns created_at",
			input:    "created_at",
			expected: "created_at",
		},
		{
			name:     "CREATED_AT uppercase returns created_at",
			input:    "CREATED_AT",
			expected: "created_at",
		},
		{
			name:     "started_at returns started_at",
			input:    "started_at",
			expected: "started_at",
		},
		{
			name:     "completed_at returns completed_at",
			input:    "completed_at",
			expected: "completed_at",
		},
		{
			name:     "status returns status",
			input:    "status",
			expected: "status",
		},
		{
			name:     "STATUS uppercase returns status",
			input:    "STATUS",
			expected: "status",
		},
		{
			name:     "unknown column defaults to id",
			input:    "unknown_column",
			expected: "id",
		},
		{
			name:     "whitespace is trimmed",
			input:    "  created_at  ",
			expected: "created_at",
		},
		{
			name:     "mixed case with whitespace",
			input:    "  Started_At  ",
			expected: "started_at",
		},
		{
			name:     "sql injection attempt defaults to id",
			input:    "id; DROP TABLE users;--",
			expected: "id",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			result := normalizeJobSortColumn(tt.input)
			assert.Equal(t, tt.expected, result)
		})
	}
}

func TestJobSortValue(t *testing.T) {
	t.Parallel()

	now := sharedtestutil.FixedTime()
	completedAt := now.Add(time.Hour)
	jobID := sharedtestutil.MustDeterministicUUID("TestJobSortValue")

	job := &entities.IngestionJob{
		ID:          jobID,
		Status:      value_objects.JobStatusCompleted,
		CreatedAt:   now,
		StartedAt:   now,
		CompletedAt: &completedAt,
	}

	t.Run("created_at returns formatted time", func(t *testing.T) {
		t.Parallel()

		result := jobSortValue(job, "created_at")
		assert.Equal(t, now.Format(time.RFC3339Nano), result)
	})

	t.Run("started_at returns formatted time", func(t *testing.T) {
		t.Parallel()

		result := jobSortValue(job, "started_at")
		assert.Equal(t, now.Format(time.RFC3339Nano), result)
	})

	t.Run("started_at zero value returns empty string", func(t *testing.T) {
		t.Parallel()

		zeroJob := &entities.IngestionJob{ID: jobID}
		result := jobSortValue(zeroJob, "started_at")
		assert.Equal(t, "", result)
	})

	t.Run("completed_at nil returns empty string", func(t *testing.T) {
		t.Parallel()

		noCompleteJob := &entities.IngestionJob{ID: jobID}
		result := jobSortValue(noCompleteJob, "completed_at")
		assert.Equal(t, "", result)
	})

	t.Run("status returns status string", func(t *testing.T) {
		t.Parallel()

		result := jobSortValue(job, "status")
		assert.Equal(t, job.Status.String(), result)
	})

	t.Run("default returns id string", func(t *testing.T) {
		t.Parallel()

		result := jobSortValue(job, "unknown")
		assert.Equal(t, jobID.String(), result)
	})

	t.Run("nil job returns empty string", func(t *testing.T) {
		t.Parallel()

		assert.Empty(t, jobSortValue(nil, columnCreatedAt))
	})
}

func TestCalculateJobSortPagination_PropagatesCalculatorError(t *testing.T) {
	t.Parallel()

	_, err := calculateJobSortPagination(
		true,
		true,
		true,
		columnCreatedAt,
		time.Now().UTC().Format(time.RFC3339Nano),
		uuid.New().String(),
		time.Now().UTC().Add(time.Minute).Format(time.RFC3339Nano),
		uuid.New().String(),
		func(
			_ bool,
			_ bool,
			_ bool,
			_ string,
			_ string,
			_ string,
			_ string,
			_ string,
		) (string, string, error) {
			return "", "", sql.ErrTxDone
		},
	)

	require.Error(t, err)
	assert.ErrorIs(t, err, sql.ErrTxDone)
	assert.Contains(t, err.Error(), "calculate sort cursor pagination")
}

func TestCalculateJobSortPagination_NilCalculator(t *testing.T) {
	t.Parallel()

	_, err := calculateJobSortPagination(
		true,
		true,
		true,
		columnCreatedAt,
		time.Now().UTC().Format(time.RFC3339Nano),
		uuid.New().String(),
		time.Now().UTC().Add(time.Minute).Format(time.RFC3339Nano),
		uuid.New().String(),
		nil,
	)

	require.Error(t, err)
	assert.ErrorIs(t, err, sharedpg.ErrSortCursorCalculatorRequired)
	assert.Contains(t, err.Error(), "calculate sort cursor pagination")
}

func TestCalculateJobPagination_NilBoundaryRecord(t *testing.T) {
	t.Parallel()

	_, err := calculateJobPagination(
		[]*entities.IngestionJob{nil},
		true,
		true,
		false,
		pkgHTTP.CursorDirectionNext,
		columnCreatedAt,
	)

	require.Error(t, err)
	assert.ErrorIs(t, err, sharedpg.ErrSortCursorBoundaryRecordNil)
	assert.Contains(t, err.Error(), "validate job pagination boundaries")
}

func TestSentinelErrors_HaveMessages(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name     string
		err      error
		contains string
	}{
		{
			name:     "errJobEntityRequired has message",
			err:      errJobEntityRequired,
			contains: "entity",
		},
		{
			name:     "errJobModelRequired has message",
			err:      errJobModelRequired,
			contains: "model",
		},
		{
			name:     "errInvalidJobStatus has message",
			err:      errInvalidJobStatus,
			contains: "status",
		},
		{
			name:     "errRepoNotInit has message",
			err:      errRepoNotInit,
			contains: "repository",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			require.Error(t, tt.err)
			assert.Contains(t, tt.err.Error(), tt.contains)
		})
	}
}

func TestSentinelErrors_AreDistinct(t *testing.T) {
	t.Parallel()

	t.Run("errJobEntityRequired is distinct", func(t *testing.T) {
		t.Parallel()

		require.NotErrorIs(t, errJobEntityRequired, errJobModelRequired)
		require.NotErrorIs(t, errJobEntityRequired, errInvalidJobStatus)
		require.NotErrorIs(t, errJobEntityRequired, errRepoNotInit)
	})

	t.Run("errJobModelRequired is distinct", func(t *testing.T) {
		t.Parallel()

		require.NotErrorIs(t, errJobModelRequired, errJobEntityRequired)
		require.NotErrorIs(t, errJobModelRequired, errInvalidJobStatus)
		require.NotErrorIs(t, errJobModelRequired, errRepoNotInit)
	})

	t.Run("errInvalidJobStatus is distinct", func(t *testing.T) {
		t.Parallel()

		require.NotErrorIs(t, errInvalidJobStatus, errJobEntityRequired)
		require.NotErrorIs(t, errInvalidJobStatus, errJobModelRequired)
		require.NotErrorIs(t, errInvalidJobStatus, errRepoNotInit)
	})

	t.Run("errRepoNotInit is distinct", func(t *testing.T) {
		t.Parallel()

		require.NotErrorIs(t, errRepoNotInit, errJobEntityRequired)
		require.NotErrorIs(t, errRepoNotInit, errJobModelRequired)
		require.NotErrorIs(t, errRepoNotInit, errInvalidJobStatus)
	})
}

func TestRepository_Create_Success(t *testing.T) {
	t.Parallel()

	repo, mock, finish := setupMock(t)
	defer finish()

	ctx := context.Background()
	now := time.Now().UTC()
	jobID := uuid.New()
	contextID := uuid.New()
	sourceID := uuid.New()

	job := &entities.IngestionJob{
		ID:        jobID,
		ContextID: contextID,
		SourceID:  sourceID,
		Status:    value_objects.JobStatusProcessing,
		StartedAt: now,
		Metadata: entities.JobMetadata{
			FileName: "test.csv",
			FileSize: 1024,
		},
		CreatedAt: now,
		UpdatedAt: now,
	}

	query := regexp.QuoteMeta(
		`INSERT INTO ingestion_jobs (id, context_id, source_id, status, started_at, completed_at, metadata, created_at, updated_at)
			VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9)
			RETURNING ` + jobColumns,
	)

	rows := sqlmock.NewRows([]string{
		"id", "context_id", "source_id", "status", "started_at", "completed_at", "metadata", "created_at", "updated_at",
	}).AddRow(
		jobID.String(),
		contextID.String(),
		sourceID.String(),
		"PROCESSING",
		now,
		sql.NullTime{},
		[]byte(`{"fileName":"test.csv","fileSize":1024}`),
		now,
		now,
	)

	mock.ExpectBegin()
	mock.ExpectQuery(query).WithArgs(
		jobID.String(),
		contextID.String(),
		sourceID.String(),
		"PROCESSING",
		sqlmock.AnyArg(),
		sqlmock.AnyArg(),
		sqlmock.AnyArg(),
		sqlmock.AnyArg(),
		sqlmock.AnyArg(),
	).WillReturnRows(rows)
	mock.ExpectCommit()

	result, err := repo.Create(ctx, job)

	require.NoError(t, err)
	require.NotNil(t, result)
	assert.Equal(t, jobID, result.ID)
	assert.Equal(t, contextID, result.ContextID)
	assert.Equal(t, sourceID, result.SourceID)
	assert.Equal(t, value_objects.JobStatusProcessing, result.Status)
}

func TestRepository_Create_QueryError(t *testing.T) {
	t.Parallel()

	repo, mock, finish := setupMock(t)
	defer finish()

	ctx := context.Background()
	now := time.Now().UTC()
	job := &entities.IngestionJob{
		ID:        uuid.New(),
		ContextID: uuid.New(),
		SourceID:  uuid.New(),
		Status:    value_objects.JobStatusProcessing,
		StartedAt: now,
		Metadata:  entities.JobMetadata{FileName: "test.csv"},
		CreatedAt: now,
		UpdatedAt: now,
	}

	query := regexp.QuoteMeta(
		`INSERT INTO ingestion_jobs (id, context_id, source_id, status, started_at, completed_at, metadata, created_at, updated_at)
			VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9)
			RETURNING ` + jobColumns,
	)

	mock.ExpectBegin()
	mock.ExpectQuery(query).WithArgs(
		sqlmock.AnyArg(),
		sqlmock.AnyArg(),
		sqlmock.AnyArg(),
		sqlmock.AnyArg(),
		sqlmock.AnyArg(),
		sqlmock.AnyArg(),
		sqlmock.AnyArg(),
		sqlmock.AnyArg(),
		sqlmock.AnyArg(),
	).WillReturnError(errTestDatabaseError)
	mock.ExpectRollback()

	result, err := repo.Create(ctx, job)

	require.Error(t, err)
	require.Nil(t, result)
	require.Contains(t, err.Error(), "failed to create job")
}

func TestRepository_FindByID_Success(t *testing.T) {
	t.Parallel()

	repo, mock, finish := setupMock(t)
	defer finish()

	ctx := context.Background()
	now := time.Now().UTC()
	completedAt := now.Add(time.Hour)
	jobID := uuid.New()
	contextID := uuid.New()
	sourceID := uuid.New()

	query := regexp.QuoteMeta("SELECT " + jobColumns + " FROM ingestion_jobs WHERE id = $1")

	rows := sqlmock.NewRows([]string{
		"id", "context_id", "source_id", "status", "started_at", "completed_at", "metadata", "created_at", "updated_at",
	}).AddRow(
		jobID.String(),
		contextID.String(),
		sourceID.String(),
		"COMPLETED",
		now,
		sql.NullTime{Time: completedAt, Valid: true},
		[]byte(`{"fileName":"result.csv","fileSize":2048,"totalRows":100,"failedRows":5}`),
		now,
		now,
	)

	mock.ExpectBegin()
	mock.ExpectQuery(query).WithArgs(jobID.String()).WillReturnRows(rows)
	mock.ExpectCommit()

	result, err := repo.FindByID(ctx, jobID)

	require.NoError(t, err)
	require.NotNil(t, result)
	assert.Equal(t, jobID, result.ID)
	assert.Equal(t, contextID, result.ContextID)
	assert.Equal(t, sourceID, result.SourceID)
	assert.Equal(t, value_objects.JobStatusCompleted, result.Status)
	require.NotNil(t, result.CompletedAt)
	assert.Equal(t, "result.csv", result.Metadata.FileName)
	assert.Equal(t, 100, result.Metadata.TotalRows)
	assert.Equal(t, 5, result.Metadata.FailedRows)
}

func TestRepository_FindByID_NotFound(t *testing.T) {
	t.Parallel()

	repo, mock, finish := setupMock(t)
	defer finish()

	ctx := context.Background()
	missingID := uuid.New()

	query := regexp.QuoteMeta("SELECT " + jobColumns + " FROM ingestion_jobs WHERE id = $1")

	mock.ExpectBegin()
	mock.ExpectQuery(query).WithArgs(missingID.String()).WillReturnError(sql.ErrNoRows)
	mock.ExpectRollback()

	result, err := repo.FindByID(ctx, missingID)

	require.Error(t, err)
	require.Nil(t, result)
	require.ErrorIs(t, err, sql.ErrNoRows)
}

func TestRepository_FindByID_QueryError(t *testing.T) {
	t.Parallel()

	repo, mock, finish := setupMock(t)
	defer finish()

	ctx := context.Background()
	jobID := uuid.New()

	query := regexp.QuoteMeta("SELECT " + jobColumns + " FROM ingestion_jobs WHERE id = $1")

	mock.ExpectBegin()
	mock.ExpectQuery(query).WithArgs(jobID.String()).WillReturnError(errTestConnectionError)
	mock.ExpectRollback()

	result, err := repo.FindByID(ctx, jobID)

	require.Error(t, err)
	require.Nil(t, result)
	require.Contains(t, err.Error(), "failed to find job")
}

func TestRepository_Update_Success(t *testing.T) {
	t.Parallel()

	repo, mock, finish := setupMock(t)
	defer finish()

	ctx := context.Background()
	now := time.Now().UTC()
	completedAt := now.Add(time.Hour)
	jobID := uuid.New()
	contextID := uuid.New()
	sourceID := uuid.New()

	job := &entities.IngestionJob{
		ID:          jobID,
		ContextID:   contextID,
		SourceID:    sourceID,
		Status:      value_objects.JobStatusCompleted,
		StartedAt:   now,
		CompletedAt: &completedAt,
		Metadata: entities.JobMetadata{
			FileName:   "test.csv",
			FileSize:   1024,
			TotalRows:  50,
			FailedRows: 2,
		},
		CreatedAt: now,
		UpdatedAt: now,
	}

	query := regexp.QuoteMeta(
		`UPDATE ingestion_jobs SET status = $1, started_at = $2, completed_at = $3, metadata = $4, updated_at = $5 WHERE id = $6
			RETURNING ` + jobColumns,
	)

	rows := sqlmock.NewRows([]string{
		"id", "context_id", "source_id", "status", "started_at", "completed_at", "metadata", "created_at", "updated_at",
	}).AddRow(
		jobID.String(),
		contextID.String(),
		sourceID.String(),
		"COMPLETED",
		now,
		sql.NullTime{Time: completedAt, Valid: true},
		[]byte(`{"fileName":"test.csv","fileSize":1024,"totalRows":50,"failedRows":2}`),
		now,
		now,
	)

	mock.ExpectBegin()
	mock.ExpectQuery(query).WithArgs(
		"COMPLETED",
		sqlmock.AnyArg(),
		sqlmock.AnyArg(),
		sqlmock.AnyArg(),
		sqlmock.AnyArg(),
		jobID.String(),
	).WillReturnRows(rows)
	mock.ExpectCommit()

	result, err := repo.Update(ctx, job)

	require.NoError(t, err)
	require.NotNil(t, result)
	assert.Equal(t, jobID, result.ID)
	assert.Equal(t, value_objects.JobStatusCompleted, result.Status)
	assert.Equal(t, 50, result.Metadata.TotalRows)
	assert.Equal(t, 2, result.Metadata.FailedRows)
}

func TestRepository_Update_NotFound(t *testing.T) {
	t.Parallel()

	repo, mock, finish := setupMock(t)
	defer finish()

	ctx := context.Background()
	now := time.Now().UTC()
	job := &entities.IngestionJob{
		ID:        uuid.New(),
		ContextID: uuid.New(),
		SourceID:  uuid.New(),
		Status:    value_objects.JobStatusProcessing,
		StartedAt: now,
		Metadata:  entities.JobMetadata{FileName: "test.csv"},
		CreatedAt: now,
		UpdatedAt: now,
	}

	query := regexp.QuoteMeta(
		`UPDATE ingestion_jobs SET status = $1, started_at = $2, completed_at = $3, metadata = $4, updated_at = $5 WHERE id = $6
			RETURNING ` + jobColumns,
	)

	mock.ExpectBegin()
	mock.ExpectQuery(query).WithArgs(
		sqlmock.AnyArg(),
		sqlmock.AnyArg(),
		sqlmock.AnyArg(),
		sqlmock.AnyArg(),
		sqlmock.AnyArg(),
		sqlmock.AnyArg(),
	).WillReturnError(sql.ErrNoRows)
	mock.ExpectRollback()

	result, err := repo.Update(ctx, job)

	require.Error(t, err)
	require.Nil(t, result)
	require.ErrorIs(t, err, sql.ErrNoRows)
}

func TestRepository_Update_QueryError(t *testing.T) {
	t.Parallel()

	repo, mock, finish := setupMock(t)
	defer finish()

	ctx := context.Background()
	now := time.Now().UTC()
	job := &entities.IngestionJob{
		ID:        uuid.New(),
		ContextID: uuid.New(),
		SourceID:  uuid.New(),
		Status:    value_objects.JobStatusProcessing,
		StartedAt: now,
		Metadata:  entities.JobMetadata{FileName: "test.csv"},
		CreatedAt: now,
		UpdatedAt: now,
	}

	query := regexp.QuoteMeta(
		`UPDATE ingestion_jobs SET status = $1, started_at = $2, completed_at = $3, metadata = $4, updated_at = $5 WHERE id = $6
			RETURNING ` + jobColumns,
	)

	mock.ExpectBegin()
	mock.ExpectQuery(query).WithArgs(
		sqlmock.AnyArg(),
		sqlmock.AnyArg(),
		sqlmock.AnyArg(),
		sqlmock.AnyArg(),
		sqlmock.AnyArg(),
		sqlmock.AnyArg(),
	).WillReturnError(errTestUpdateFailed)
	mock.ExpectRollback()

	result, err := repo.Update(ctx, job)

	require.Error(t, err)
	require.Nil(t, result)
	require.Contains(t, err.Error(), "failed to update job")
}

func TestRepository_FindByContextID_Success(t *testing.T) {
	t.Parallel()

	repo, mock, finish := setupMock(t)
	defer finish()

	ctx := context.Background()
	now := time.Now().UTC()
	contextID := uuid.New()
	jobID1 := uuid.New()
	jobID2 := uuid.New()
	sourceID := uuid.New()

	rows := sqlmock.NewRows([]string{
		"id", "context_id", "source_id", "status", "started_at", "completed_at", "metadata", "created_at", "updated_at",
	}).AddRow(
		jobID1.String(),
		contextID.String(),
		sourceID.String(),
		"PROCESSING",
		now,
		sql.NullTime{},
		[]byte(`{"fileName":"file1.csv"}`),
		now,
		now,
	).AddRow(
		jobID2.String(),
		contextID.String(),
		sourceID.String(),
		"COMPLETED",
		now,
		sql.NullTime{Time: now.Add(time.Hour), Valid: true},
		[]byte(`{"fileName":"file2.csv","totalRows":10}`),
		now,
		now,
	)

	mock.ExpectBegin()
	mock.ExpectQuery("SELECT").WillReturnRows(rows)
	mock.ExpectCommit()

	result, pagination, err := repo.FindByContextID(ctx, contextID, repositories.CursorFilter{
		Limit:  10,
		SortBy: "id",
	})

	require.NoError(t, err)
	require.Len(t, result, 2)
	assert.Equal(t, jobID1, result[0].ID)
	assert.Equal(t, jobID2, result[1].ID)
	assert.Empty(t, pagination.Next)
}

func TestRepository_FindByContextID_EmptyResult(t *testing.T) {
	t.Parallel()

	repo, mock, finish := setupMock(t)
	defer finish()

	ctx := context.Background()
	contextID := uuid.New()

	rows := sqlmock.NewRows([]string{
		"id", "context_id", "source_id", "status", "started_at", "completed_at", "metadata", "created_at", "updated_at",
	})

	mock.ExpectBegin()
	mock.ExpectQuery("SELECT").WillReturnRows(rows)
	mock.ExpectCommit()

	result, pagination, err := repo.FindByContextID(ctx, contextID, repositories.CursorFilter{
		Limit:  10,
		SortBy: "id",
	})

	require.NoError(t, err)
	require.Empty(t, result)
	assert.Empty(t, pagination.Next)
	assert.Empty(t, pagination.Prev)
}

func TestRepository_FindByContextID_QueryError(t *testing.T) {
	t.Parallel()

	repo, mock, finish := setupMock(t)
	defer finish()

	ctx := context.Background()
	contextID := uuid.New()

	mock.ExpectBegin()
	mock.ExpectQuery("SELECT").WillReturnError(errTestQueryFailed)
	mock.ExpectRollback()

	result, pagination, err := repo.FindByContextID(ctx, contextID, repositories.CursorFilter{
		Limit:  10,
		SortBy: "id",
	})

	require.Error(t, err)
	require.Nil(t, result)
	require.Empty(t, pagination.Next)
	require.Contains(t, err.Error(), "failed to list jobs by context")
}

func TestRepository_FindByContextID_InvalidSortCursor(t *testing.T) {
	t.Parallel()

	repo, mock, finish := setupMock(t)
	defer finish()

	ctx := context.Background()
	contextID := uuid.New()

	mock.ExpectBegin()
	mock.ExpectRollback()

	result, pagination, err := repo.FindByContextID(ctx, contextID, repositories.CursorFilter{
		Limit:  10,
		SortBy: "created_at",
		Cursor: "someCursor",
	})

	require.Error(t, err)
	require.Nil(t, result)
	require.Empty(t, pagination.Next)
	require.ErrorIs(t, err, pkgHTTP.ErrInvalidCursor)
}

func TestRepository_FindByContextID_SortByDifferentColumns(t *testing.T) {
	t.Parallel()

	sortColumns := []string{"created_at", "started_at", "completed_at", "status"}

	for _, sortBy := range sortColumns {
		t.Run("sort_by_"+sortBy, func(t *testing.T) {
			t.Parallel()

			repo, mock, finish := setupMock(t)
			defer finish()

			ctx := context.Background()
			contextID := uuid.New()

			rows := sqlmock.NewRows([]string{
				"id", "context_id", "source_id", "status", "started_at", "completed_at", "metadata", "created_at", "updated_at",
			})

			mock.ExpectBegin()
			mock.ExpectQuery("SELECT").WillReturnRows(rows)
			mock.ExpectCommit()

			result, _, err := repo.FindByContextID(ctx, contextID, repositories.CursorFilter{
				Limit:  10,
				SortBy: sortBy,
			})

			require.NoError(t, err)
			require.Empty(t, result)
		})
	}
}

func TestRepository_FindByContextID_DefaultLimit(t *testing.T) {
	t.Parallel()

	repo, mock, finish := setupMock(t)
	defer finish()

	ctx := context.Background()
	contextID := uuid.New()

	rows := sqlmock.NewRows([]string{
		"id", "context_id", "source_id", "status", "started_at", "completed_at", "metadata", "created_at", "updated_at",
	})

	mock.ExpectBegin()
	mock.ExpectQuery("SELECT").WillReturnRows(rows)
	mock.ExpectCommit()

	result, _, err := repo.FindByContextID(ctx, contextID, repositories.CursorFilter{
		Limit:  0,
		SortBy: "id",
	})

	require.NoError(t, err)
	require.Empty(t, result)
}

func TestRepository_FindByContextID_LimitCappedAtMaximum(t *testing.T) {
	t.Parallel()

	repo, mock, finish := setupMock(t)
	defer finish()

	ctx := context.Background()
	contextID := uuid.New()

	rows := sqlmock.NewRows([]string{
		"id", "context_id", "source_id", "status", "started_at", "completed_at", "metadata", "created_at", "updated_at",
	})

	mock.ExpectBegin()
	mock.ExpectQuery("(?i)limit 201").WillReturnRows(rows)
	mock.ExpectCommit()

	result, _, err := repo.FindByContextID(ctx, contextID, repositories.CursorFilter{
		Limit:  999,
		SortBy: "id",
	})

	require.NoError(t, err)
	require.Empty(t, result)
}

func TestRepository_WithTx_Success(t *testing.T) {
	t.Parallel()

	repo, mock, finish := setupMock(t)
	defer finish()

	ctx := context.Background()

	mock.ExpectBegin()
	mock.ExpectCommit()

	err := repo.WithTx(ctx, func(tx *sql.Tx) error {
		return nil
	})

	require.NoError(t, err)
}

func TestRepository_WithTx_FunctionError(t *testing.T) {
	t.Parallel()

	repo, mock, finish := setupMock(t)
	defer finish()

	ctx := context.Background()

	mock.ExpectBegin()
	mock.ExpectRollback()

	err := repo.WithTx(ctx, func(tx *sql.Tx) error {
		return errTestFunctionError
	})

	require.Error(t, err)
	require.Contains(t, err.Error(), "job transaction failed")
}

func TestRepository_FindByContextID_WithPagination(t *testing.T) {
	t.Parallel()

	repo, mock, finish := setupMock(t)
	defer finish()

	ctx := context.Background()
	now := time.Now().UTC()
	contextID := uuid.New()
	sourceID := uuid.New()

	jobIDs := make([]uuid.UUID, 3)
	for i := range jobIDs {
		jobIDs[i] = uuid.New()
	}

	rows := sqlmock.NewRows([]string{
		"id", "context_id", "source_id", "status", "started_at", "completed_at", "metadata", "created_at", "updated_at",
	})

	for _, id := range jobIDs {
		rows.AddRow(
			id.String(),
			contextID.String(),
			sourceID.String(),
			"PROCESSING",
			now,
			sql.NullTime{},
			[]byte(`{"fileName":"test.csv"}`),
			now,
			now,
		)
	}

	mock.ExpectBegin()
	mock.ExpectQuery("SELECT").WillReturnRows(rows)
	mock.ExpectCommit()

	result, pagination, err := repo.FindByContextID(ctx, contextID, repositories.CursorFilter{
		Limit:  2,
		SortBy: "id",
	})

	require.NoError(t, err)
	require.Len(t, result, 2)
	require.NotEmpty(t, pagination.Next)
}

func TestRepository_FindByContextID_RowsError(t *testing.T) {
	t.Parallel()

	repo, mock, finish := setupMock(t)
	defer finish()

	ctx := context.Background()
	contextID := uuid.New()
	now := time.Now().UTC()

	rows := sqlmock.NewRows([]string{
		"id", "context_id", "source_id", "status", "started_at", "completed_at", "metadata", "created_at", "updated_at",
	}).AddRow(
		uuid.New().String(),
		contextID.String(),
		uuid.New().String(),
		"PROCESSING",
		now,
		sql.NullTime{},
		[]byte(`{}`),
		now,
		now,
	).RowError(0, errTestRowIterationError)

	mock.ExpectBegin()
	mock.ExpectQuery("SELECT").WillReturnRows(rows)
	mock.ExpectRollback()

	result, pagination, err := repo.FindByContextID(ctx, contextID, repositories.CursorFilter{
		Limit:  10,
		SortBy: "id",
	})

	require.Error(t, err)
	require.Nil(t, result)
	require.Empty(t, pagination.Next)
}

func TestRepository_FindByContextID_ScanError(t *testing.T) {
	t.Parallel()

	repo, mock, finish := setupMock(t)
	defer finish()

	ctx := context.Background()
	contextID := uuid.New()

	rows := sqlmock.NewRows([]string{
		"id", "context_id", "source_id", "status", "started_at", "completed_at", "metadata", "created_at", "updated_at",
	}).AddRow(
		"invalid-uuid",
		contextID.String(),
		uuid.New().String(),
		"PROCESSING",
		time.Now().UTC(),
		sql.NullTime{},
		[]byte(`{}`),
		time.Now().UTC(),
		time.Now().UTC(),
	)

	mock.ExpectBegin()
	mock.ExpectQuery("SELECT").WillReturnRows(rows)
	mock.ExpectRollback()

	result, pagination, err := repo.FindByContextID(ctx, contextID, repositories.CursorFilter{
		Limit:  10,
		SortBy: "id",
	})

	require.Error(t, err)
	require.Nil(t, result)
	require.Empty(t, pagination.Next)
}

func TestScanJob_InvalidUUID(t *testing.T) {
	t.Parallel()

	repo, mock, finish := setupMock(t)
	defer finish()

	ctx := context.Background()
	jobID := uuid.New()

	query := regexp.QuoteMeta("SELECT " + jobColumns + " FROM ingestion_jobs WHERE id = $1")

	rows := sqlmock.NewRows([]string{
		"id", "context_id", "source_id", "status", "started_at", "completed_at", "metadata", "created_at", "updated_at",
	}).AddRow(
		"not-a-valid-uuid",
		uuid.New().String(),
		uuid.New().String(),
		"PROCESSING",
		time.Now().UTC(),
		sql.NullTime{},
		[]byte(`{}`),
		time.Now().UTC(),
		time.Now().UTC(),
	)

	mock.ExpectBegin()
	mock.ExpectQuery(query).WithArgs(jobID.String()).WillReturnRows(rows)
	mock.ExpectRollback()

	result, err := repo.FindByID(ctx, jobID)

	require.Error(t, err)
	require.Nil(t, result)
	require.Contains(t, err.Error(), "parsing ID")
}

func TestScanJob_InvalidStatus(t *testing.T) {
	t.Parallel()

	repo, mock, finish := setupMock(t)
	defer finish()

	ctx := context.Background()
	jobID := uuid.New()

	query := regexp.QuoteMeta("SELECT " + jobColumns + " FROM ingestion_jobs WHERE id = $1")

	rows := sqlmock.NewRows([]string{
		"id", "context_id", "source_id", "status", "started_at", "completed_at", "metadata", "created_at", "updated_at",
	}).AddRow(
		jobID.String(),
		uuid.New().String(),
		uuid.New().String(),
		"INVALID_STATUS",
		time.Now().UTC(),
		sql.NullTime{},
		[]byte(`{}`),
		time.Now().UTC(),
		time.Now().UTC(),
	)

	mock.ExpectBegin()
	mock.ExpectQuery(query).WithArgs(jobID.String()).WillReturnRows(rows)
	mock.ExpectRollback()

	result, err := repo.FindByID(ctx, jobID)

	require.Error(t, err)
	require.Nil(t, result)
	require.Contains(t, err.Error(), "parsing Status")
}
