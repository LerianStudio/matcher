//go:build unit

package extraction

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"testing"
	"time"

	sqlmock "github.com/DATA-DOG/go-sqlmock"
	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/LerianStudio/matcher/internal/discovery/domain/entities"
	"github.com/LerianStudio/matcher/internal/discovery/domain/repositories"
	vo "github.com/LerianStudio/matcher/internal/discovery/domain/value_objects"
	"github.com/LerianStudio/matcher/internal/shared/infrastructure/testutil"
)

var (
	errTestQuery = errors.New("query error")
	errTestExec  = errors.New("exec error")
)

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

func createTestExtraction() *entities.ExtractionRequest {
	now := time.Now().UTC()

	return &entities.ExtractionRequest{
		ID:             uuid.New(),
		ConnectionID:   uuid.New(),
		IngestionJobID: uuid.New(),
		FetcherJobID:   "fetcher-job-789",
		Tables:         map[string]interface{}{"transactions": map[string]interface{}{"columns": []interface{}{"id", "amount"}}},
		StartDate:      "2026-03-01",
		EndDate:        "2026-03-08",
		Filters:        map[string]interface{}{"equals": map[string]interface{}{"currency": "USD"}},
		Status:         vo.ExtractionStatusPending,
		ResultPath:     "/data/output/result.csv",
		ErrorMessage:   "",
		CreatedAt:      now,
		UpdatedAt:      now,
	}
}

func extractionColumns() []string {
	return []string{
		"id", "connection_id", "ingestion_job_id", "fetcher_job_id",
		"tables", "start_date", "end_date", "filters", "status", "result_path", "error_message",
		"created_at", "updated_at",
		// T-005 bridge failure columns (migration 000026).
		"bridge_attempts", "bridge_last_error", "bridge_last_error_message", "bridge_failed_at",
	}
}

func TestNewRepository(t *testing.T) {
	t.Parallel()

	t.Run("with valid provider", func(t *testing.T) {
		t.Parallel()

		provider := &testutil.MockInfrastructureProvider{}
		repo := NewRepository(provider)

		require.NotNil(t, repo)
		assert.Equal(t, provider, repo.provider)
	})

	t.Run("with nil provider", func(t *testing.T) {
		t.Parallel()

		repo := NewRepository(nil)

		require.NotNil(t, repo)
		assert.Nil(t, repo.provider)
	})
}

func TestRepository_Create(t *testing.T) {
	t.Parallel()

	t.Run("nil repository", func(t *testing.T) {
		t.Parallel()

		var repo *Repository
		err := repo.Create(context.Background(), createTestExtraction())

		assert.ErrorIs(t, err, ErrRepoNotInitialized)
	})

	t.Run("nil provider", func(t *testing.T) {
		t.Parallel()

		repo := NewRepository(nil)
		err := repo.Create(context.Background(), createTestExtraction())

		assert.ErrorIs(t, err, ErrRepoNotInitialized)
	})

	t.Run("nil entity", func(t *testing.T) {
		t.Parallel()

		repo, _, finish := setupMockRepository(t)
		defer finish()

		err := repo.Create(context.Background(), nil)

		assert.ErrorIs(t, err, ErrEntityRequired)
	})

	t.Run("successful create", func(t *testing.T) {
		t.Parallel()

		repo, mock, finish := setupMockRepository(t)
		defer finish()

		req := createTestExtraction()

		mock.ExpectBegin()
		mock.ExpectExec("INSERT INTO extraction_requests").
			WithArgs(
				req.ID,
				req.ConnectionID,
				req.IngestionJobID,
				req.FetcherJobID,
				sqlmock.AnyArg(),
				sqlmock.AnyArg(),
				sqlmock.AnyArg(),
				sqlmock.AnyArg(),
				req.Status.String(),
				req.ResultPath,
				sqlmock.AnyArg(),
				sqlmock.AnyArg(),
				sqlmock.AnyArg(),
				// T-005 bridge_* columns: bridge_attempts, bridge_last_error,
				// bridge_last_error_message, bridge_failed_at.
				0,
				sqlmock.AnyArg(),
				sqlmock.AnyArg(),
				sqlmock.AnyArg(),
			).
			WillReturnResult(sqlmock.NewResult(1, 1))
		mock.ExpectCommit()

		err := repo.Create(context.Background(), req)

		assert.NoError(t, err)
	})

	t.Run("nil filters persist as sql null", func(t *testing.T) {
		t.Parallel()

		repo, mock, finish := setupMockRepository(t)
		defer finish()

		req := createTestExtraction()
		req.Filters = nil

		mock.ExpectBegin()
		mock.ExpectExec("INSERT INTO extraction_requests").
			WithArgs(
				req.ID,
				req.ConnectionID,
				req.IngestionJobID,
				req.FetcherJobID,
				sqlmock.AnyArg(),
				sqlmock.AnyArg(),
				sqlmock.AnyArg(),
				nil,
				req.Status.String(),
				req.ResultPath,
				sqlmock.AnyArg(),
				sqlmock.AnyArg(),
				sqlmock.AnyArg(),
				0,
				sqlmock.AnyArg(),
				sqlmock.AnyArg(),
				sqlmock.AnyArg(),
			).
			WillReturnResult(sqlmock.NewResult(1, 1))
		mock.ExpectCommit()

		err := repo.Create(context.Background(), req)

		assert.NoError(t, err)
	})

	t.Run("exec error", func(t *testing.T) {
		t.Parallel()

		repo, mock, finish := setupMockRepository(t)
		defer finish()

		req := createTestExtraction()

		mock.ExpectBegin()
		mock.ExpectExec("INSERT INTO extraction_requests").
			WithArgs(
				req.ID,
				req.ConnectionID,
				req.IngestionJobID,
				req.FetcherJobID,
				sqlmock.AnyArg(),
				sqlmock.AnyArg(),
				sqlmock.AnyArg(),
				sqlmock.AnyArg(),
				req.Status.String(),
				req.ResultPath,
				sqlmock.AnyArg(),
				sqlmock.AnyArg(),
				sqlmock.AnyArg(),
				0,
				sqlmock.AnyArg(),
				sqlmock.AnyArg(),
				sqlmock.AnyArg(),
			).
			WillReturnError(errTestExec)
		mock.ExpectRollback()

		err := repo.Create(context.Background(), req)

		assert.Error(t, err)
	})
}

func TestRepository_Update(t *testing.T) {
	t.Parallel()

	t.Run("nil repository", func(t *testing.T) {
		t.Parallel()

		var repo *Repository
		err := repo.Update(context.Background(), createTestExtraction())

		assert.ErrorIs(t, err, ErrRepoNotInitialized)
	})

	t.Run("nil entity", func(t *testing.T) {
		t.Parallel()

		repo, _, finish := setupMockRepository(t)
		defer finish()

		err := repo.Update(context.Background(), nil)

		assert.ErrorIs(t, err, ErrEntityRequired)
	})

	t.Run("successful update", func(t *testing.T) {
		t.Parallel()

		repo, mock, finish := setupMockRepository(t)
		defer finish()

		req := createTestExtraction()
		err := req.MarkSubmitted("job-abc")
		require.NoError(t, err)

		mock.ExpectBegin()
		mock.ExpectExec("UPDATE extraction_requests SET").
			WithArgs(
				req.ConnectionID,
				req.IngestionJobID,
				req.FetcherJobID,
				sqlmock.AnyArg(),
				sqlmock.AnyArg(),
				sqlmock.AnyArg(),
				sqlmock.AnyArg(),
				req.Status.String(),
				sqlmock.AnyArg(),
				sqlmock.AnyArg(),
				sqlmock.AnyArg(),
				// T-005 bridge_* columns.
				0,
				sqlmock.AnyArg(),
				sqlmock.AnyArg(),
				sqlmock.AnyArg(),
				req.ID,
			).
			WillReturnResult(sqlmock.NewResult(0, 1))
		mock.ExpectCommit()

		err = repo.Update(context.Background(), req)

		assert.NoError(t, err)
	})

	t.Run("not found on update", func(t *testing.T) {
		t.Parallel()

		repo, mock, finish := setupMockRepository(t)
		defer finish()

		req := createTestExtraction()

		mock.ExpectBegin()
		mock.ExpectExec("UPDATE extraction_requests SET").
			WithArgs(
				req.ConnectionID,
				req.IngestionJobID,
				req.FetcherJobID,
				sqlmock.AnyArg(),
				sqlmock.AnyArg(),
				sqlmock.AnyArg(),
				sqlmock.AnyArg(),
				req.Status.String(),
				sqlmock.AnyArg(),
				sqlmock.AnyArg(),
				sqlmock.AnyArg(),
				0,
				sqlmock.AnyArg(),
				sqlmock.AnyArg(),
				sqlmock.AnyArg(),
				req.ID,
			).
			WillReturnResult(sqlmock.NewResult(0, 0))
		mock.ExpectRollback()

		err := repo.Update(context.Background(), req)

		assert.Error(t, err)
	})
}

func TestRepository_FindByID(t *testing.T) {
	t.Parallel()

	t.Run("nil repository", func(t *testing.T) {
		t.Parallel()

		var repo *Repository
		result, err := repo.FindByID(context.Background(), uuid.New())

		assert.ErrorIs(t, err, ErrRepoNotInitialized)
		assert.Nil(t, result)
	})

	t.Run("successful find by id", func(t *testing.T) {
		t.Parallel()

		repo, mock, finish := setupMockRepository(t)
		defer finish()

		req := createTestExtraction()
		tablesJSON, err := json.Marshal(req.Tables)
		require.NoError(t, err)

		filtersJSON, err := json.Marshal(req.Filters)
		require.NoError(t, err)

		rows := sqlmock.NewRows(extractionColumns()).
			AddRow(
				req.ID,
				req.ConnectionID,
				req.IngestionJobID,
				sql.NullString{String: req.FetcherJobID, Valid: req.FetcherJobID != ""},
				tablesJSON,
				sql.NullString{String: req.StartDate, Valid: req.StartDate != ""},
				sql.NullString{String: req.EndDate, Valid: req.EndDate != ""},
				filtersJSON,
				req.Status.String(),
				sql.NullString{String: req.ResultPath, Valid: req.ResultPath != ""},
				sql.NullString{String: req.ErrorMessage, Valid: req.ErrorMessage != ""},
				req.CreatedAt,
				req.UpdatedAt,
				// T-005 bridge_* columns.
				0,
				sql.NullString{},
				sql.NullString{},
				sql.NullTime{},
			)

		mock.ExpectBegin()
		mock.ExpectQuery("SELECT .+ FROM extraction_requests WHERE id").
			WithArgs(req.ID.String()).
			WillReturnRows(rows)
		mock.ExpectCommit()

		result, err := repo.FindByID(context.Background(), req.ID)

		require.NoError(t, err)
		require.NotNil(t, result)
		assert.Equal(t, req.ConnectionID, result.ConnectionID)
		assert.Equal(t, req.Status, result.Status)
	})

	t.Run("not found", func(t *testing.T) {
		t.Parallel()

		repo, mock, finish := setupMockRepository(t)
		defer finish()

		mock.ExpectBegin()
		mock.ExpectQuery("SELECT .+ FROM extraction_requests WHERE id").
			WillReturnError(sql.ErrNoRows)
		mock.ExpectRollback()

		result, err := repo.FindByID(context.Background(), uuid.New())

		assert.ErrorIs(t, err, repositories.ErrExtractionNotFound)
		assert.Nil(t, result)
	})
}

func TestRepository_CreateWithTx_NilTransaction(t *testing.T) {
	t.Parallel()

	repo, _, finish := setupMockRepository(t)
	defer finish()

	err := repo.CreateWithTx(context.Background(), nil, createTestExtraction())

	assert.ErrorIs(t, err, ErrTransactionRequired)
}

func TestRepository_CreateWithTx_Success(t *testing.T) {
	t.Parallel()

	db, mock, err := sqlmock.New()
	require.NoError(t, err)
	defer db.Close()

	provider := testutil.NewMockProviderFromDB(t, db)
	repo := NewRepository(provider)
	req := createTestExtraction()
	tablesJSON, err := req.TablesJSON()
	require.NoError(t, err)
	filtersJSON, err := req.FiltersJSON()
	require.NoError(t, err)

	mock.ExpectBegin()
	tx, err := db.Begin()
	require.NoError(t, err)

	mock.ExpectExec("INSERT INTO extraction_requests").
		WithArgs(
			req.ID, req.ConnectionID, req.IngestionJobID, req.FetcherJobID,
			tablesJSON, sqlmock.AnyArg(), sqlmock.AnyArg(), filtersJSON,
			req.Status.String(), req.ResultPath, sqlmock.AnyArg(),
			req.CreatedAt, req.UpdatedAt,
			// T-005 bridge_* columns.
			0, sqlmock.AnyArg(), sqlmock.AnyArg(), sqlmock.AnyArg(),
		).
		WillReturnResult(sqlmock.NewResult(1, 1))
	mock.ExpectRollback()

	err = repo.CreateWithTx(context.Background(), tx, req)
	require.NoError(t, err)
	require.NoError(t, tx.Rollback())
	require.NoError(t, mock.ExpectationsWereMet())
}

func TestRepository_UpdateWithTx_NilTransaction(t *testing.T) {
	t.Parallel()

	repo, _, finish := setupMockRepository(t)
	defer finish()

	err := repo.UpdateWithTx(context.Background(), nil, createTestExtraction())

	assert.ErrorIs(t, err, ErrTransactionRequired)
}

func TestRepository_UpdateWithTx_Success(t *testing.T) {
	t.Parallel()

	db, mock, err := sqlmock.New()
	require.NoError(t, err)
	defer db.Close()

	provider := testutil.NewMockProviderFromDB(t, db)
	repo := NewRepository(provider)
	req := createTestExtraction()
	tablesJSON, err := req.TablesJSON()
	require.NoError(t, err)
	filtersJSON, err := req.FiltersJSON()
	require.NoError(t, err)

	mock.ExpectBegin()
	tx, err := db.Begin()
	require.NoError(t, err)

	mock.ExpectExec("UPDATE extraction_requests SET").
		WithArgs(
			req.ConnectionID, req.IngestionJobID, req.FetcherJobID, tablesJSON,
			sqlmock.AnyArg(), sqlmock.AnyArg(), filtersJSON,
			req.Status.String(), req.ResultPath, sqlmock.AnyArg(), req.UpdatedAt,
			// T-005 bridge_* columns.
			0, sqlmock.AnyArg(), sqlmock.AnyArg(), sqlmock.AnyArg(),
			req.ID,
		).
		WillReturnResult(sqlmock.NewResult(0, 1))
	mock.ExpectRollback()

	err = repo.UpdateWithTx(context.Background(), tx, req)
	require.NoError(t, err)
	require.NoError(t, tx.Rollback())
	require.NoError(t, mock.ExpectationsWereMet())
}

func TestRepository_UpdateIfUnchanged(t *testing.T) {
	t.Parallel()

	t.Run("successful conditional update", func(t *testing.T) {
		t.Parallel()

		repo, mock, finish := setupMockRepository(t)
		defer finish()

		req := createTestExtraction()
		expectedUpdatedAt := req.UpdatedAt
		require.NoError(t, req.MarkSubmitted("job-abc"))

		mock.ExpectBegin()
		mock.ExpectExec("UPDATE extraction_requests SET").
			WithArgs(
				req.ConnectionID,
				req.IngestionJobID,
				req.FetcherJobID,
				sqlmock.AnyArg(),
				sqlmock.AnyArg(),
				sqlmock.AnyArg(),
				sqlmock.AnyArg(),
				req.Status.String(),
				nil,
				nil,
				req.UpdatedAt,
				// T-005 bridge_* columns.
				0,
				sqlmock.AnyArg(),
				sqlmock.AnyArg(),
				sqlmock.AnyArg(),
				req.ID,
				expectedUpdatedAt,
			).
			WillReturnResult(sqlmock.NewResult(0, 1))
		mock.ExpectCommit()

		err := repo.UpdateIfUnchanged(context.Background(), req, expectedUpdatedAt)

		assert.NoError(t, err)
	})

	t.Run("returns extraction conflict when row changed", func(t *testing.T) {
		t.Parallel()

		repo, mock, finish := setupMockRepository(t)
		defer finish()

		req := createTestExtraction()
		expectedUpdatedAt := req.UpdatedAt

		mock.ExpectBegin()
		mock.ExpectExec("UPDATE extraction_requests SET").
			WillReturnResult(sqlmock.NewResult(0, 0))
		mock.ExpectRollback()

		err := repo.UpdateIfUnchanged(context.Background(), req, expectedUpdatedAt)

		assert.ErrorIs(t, err, repositories.ErrExtractionConflict)
	})

	t.Run("exec error", func(t *testing.T) {
		t.Parallel()

		repo, mock, finish := setupMockRepository(t)
		defer finish()

		req := createTestExtraction()

		mock.ExpectBegin()
		mock.ExpectExec("UPDATE extraction_requests SET").
			WillReturnError(errTestExec)
		mock.ExpectRollback()

		err := repo.UpdateIfUnchanged(context.Background(), req, req.UpdatedAt)

		assert.Error(t, err)
		assert.Contains(t, err.Error(), "update extraction request if unchanged")
	})
}

func TestRepository_UpdateIfUnchangedWithTx_NilTransaction(t *testing.T) {
	t.Parallel()

	repo, _, finish := setupMockRepository(t)
	defer finish()

	err := repo.UpdateIfUnchangedWithTx(context.Background(), nil, createTestExtraction(), time.Now().UTC())

	assert.ErrorIs(t, err, ErrTransactionRequired)
}

func TestRepository_UpdateIfUnchangedWithTx_Success(t *testing.T) {
	t.Parallel()

	db, mock, err := sqlmock.New()
	require.NoError(t, err)
	defer db.Close()

	provider := testutil.NewMockProviderFromDB(t, db)
	repo := NewRepository(provider)
	req := createTestExtraction()
	expectedUpdatedAt := req.UpdatedAt
	require.NoError(t, req.MarkSubmitted("job-abc"))

	mock.ExpectBegin()
	tx, err := db.Begin()
	require.NoError(t, err)

	mock.ExpectExec("UPDATE extraction_requests SET").
		WithArgs(
			req.ConnectionID,
			req.IngestionJobID,
			req.FetcherJobID,
			sqlmock.AnyArg(),
			sqlmock.AnyArg(),
			sqlmock.AnyArg(),
			sqlmock.AnyArg(),
			req.Status.String(),
			nil,
			nil,
			req.UpdatedAt,
			// T-005 bridge_* columns.
			0,
			sqlmock.AnyArg(),
			sqlmock.AnyArg(),
			sqlmock.AnyArg(),
			req.ID,
			expectedUpdatedAt,
		).
		WillReturnResult(sqlmock.NewResult(0, 1))
	mock.ExpectRollback()

	err = repo.UpdateIfUnchangedWithTx(context.Background(), tx, req, expectedUpdatedAt)
	require.NoError(t, err)
	require.NoError(t, tx.Rollback())
	require.NoError(t, mock.ExpectationsWereMet())
}

func TestExtractionModel_ToDomain_Nil(t *testing.T) {
	t.Parallel()

	var model *ExtractionModel
	result, err := model.ToDomain()

	assert.ErrorIs(t, err, ErrModelRequired)
	assert.Nil(t, result)
}

func TestFromDomain_Nil(t *testing.T) {
	t.Parallel()

	result, err := FromDomain(nil)

	assert.ErrorIs(t, err, ErrEntityRequired)
	assert.Nil(t, result)
}

func TestExtractionModel_RoundTrip(t *testing.T) {
	t.Parallel()

	original := createTestExtraction()
	model, err := FromDomain(original)

	require.NoError(t, err)
	require.NotNil(t, model)

	entity, err := model.ToDomain()

	require.NoError(t, err)
	require.NotNil(t, entity)
	assert.Equal(t, original.ID, entity.ID)
	assert.Equal(t, original.ConnectionID, entity.ConnectionID)
	assert.Equal(t, original.IngestionJobID, entity.IngestionJobID)
	assert.Equal(t, original.FetcherJobID, entity.FetcherJobID)
	assert.Equal(t, original.Status, entity.Status)
	assert.Equal(t, original.ResultPath, entity.ResultPath)
}

func TestExtractionModel_ToDomain_InvalidStatus(t *testing.T) {
	t.Parallel()

	model := &ExtractionModel{
		ID:             uuid.New(),
		ConnectionID:   uuid.New(),
		IngestionJobID: uuid.NullUUID{UUID: uuid.New(), Valid: true},
		Tables:         []byte("{}"),
		Status:         "INVALID_STATUS",
		CreatedAt:      time.Now().UTC(),
		UpdatedAt:      time.Now().UTC(),
	}

	result, err := model.ToDomain()

	assert.Error(t, err)
	assert.Nil(t, result)
	assert.Contains(t, err.Error(), "parse extraction status")
}

func TestNullStringToString(t *testing.T) {
	t.Parallel()

	t.Run("valid string", func(t *testing.T) {
		t.Parallel()

		ns := sql.NullString{String: "hello", Valid: true}
		assert.Equal(t, "hello", nullStringToString(ns))
	})

	t.Run("null string", func(t *testing.T) {
		t.Parallel()

		ns := sql.NullString{}
		assert.Equal(t, "", nullStringToString(ns))
	})
}
