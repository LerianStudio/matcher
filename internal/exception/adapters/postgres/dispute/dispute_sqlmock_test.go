//go:build unit

package dispute

import (
	"context"
	"database/sql"
	"errors"
	"testing"
	"time"

	sqlmock "github.com/DATA-DOG/go-sqlmock"
	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	libHTTP "github.com/LerianStudio/lib-uncommons/v2/uncommons/net/http"

	"github.com/LerianStudio/matcher/internal/exception/domain/dispute"
	"github.com/LerianStudio/matcher/internal/exception/domain/repositories"
	"github.com/LerianStudio/matcher/internal/shared/infrastructure/testutil"
)

var errSqlmockDB = errors.New("sqlmock database error")

func setupRepoV2(t *testing.T) (*Repository, sqlmock.Sqlmock, func()) {
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

func disputeColumnList() []string {
	return []string{
		"id", "exception_id", "category", "state", "description",
		"opened_by", "resolution", "reopen_reason", "evidence", "created_at", "updated_at",
	}
}

// --- List ---

func TestRepository_List_NilRepo(t *testing.T) {
	t.Parallel()

	var repo *Repository
	ctx := context.Background()

	result, pagination, err := repo.List(ctx, repositories.DisputeFilter{}, repositories.CursorFilter{})

	require.ErrorIs(t, err, ErrRepoNotInitialized)
	require.Nil(t, result)
	assert.Empty(t, pagination.Next)
	assert.Empty(t, pagination.Prev)
}

func TestRepository_List_NilProvider(t *testing.T) {
	t.Parallel()

	repo := &Repository{provider: nil}
	ctx := context.Background()

	result, pagination, err := repo.List(ctx, repositories.DisputeFilter{}, repositories.CursorFilter{})

	require.ErrorIs(t, err, ErrRepoNotInitialized)
	require.Nil(t, result)
	assert.Empty(t, pagination.Next)
	assert.Empty(t, pagination.Prev)
}

func TestRepository_List_Success_NoCursor(t *testing.T) {
	t.Parallel()

	repo, mock, finish := setupRepoV2(t)
	defer finish()

	ctx := context.Background()
	now := time.Now().UTC()
	disputeID := uuid.New()
	exceptionID := uuid.New()

	filter := repositories.DisputeFilter{}
	cursor := repositories.CursorFilter{
		Limit: 10,
	}

	mock.ExpectBegin()
	mock.ExpectQuery("SELECT").
		WillReturnRows(sqlmock.NewRows(disputeColumnList()).
			AddRow(
				disputeID.String(), exceptionID.String(),
				"BANK_FEE_ERROR", "OPEN", "Test",
				"user@test.com", sql.NullString{}, sql.NullString{},
				[]byte("[]"), now, now,
			))
	mock.ExpectCommit()

	result, _, err := repo.List(ctx, filter, cursor)

	require.NoError(t, err)
	require.Len(t, result, 1)
	assert.Equal(t, disputeID, result[0].ID)
}

func TestRepository_List_Empty(t *testing.T) {
	t.Parallel()

	repo, mock, finish := setupRepoV2(t)
	defer finish()

	ctx := context.Background()

	mock.ExpectBegin()
	mock.ExpectQuery("SELECT").
		WillReturnRows(sqlmock.NewRows(disputeColumnList()))
	mock.ExpectCommit()

	result, pagination, err := repo.List(ctx, repositories.DisputeFilter{}, repositories.CursorFilter{Limit: 10})

	require.NoError(t, err)
	require.Empty(t, result)
	assert.Empty(t, pagination.Next)
	assert.Empty(t, pagination.Prev)
}

func TestRepository_List_QueryError(t *testing.T) {
	t.Parallel()

	repo, mock, finish := setupRepoV2(t)
	defer finish()

	ctx := context.Background()

	mock.ExpectBegin()
	mock.ExpectQuery("SELECT").
		WillReturnError(errSqlmockDB)
	mock.ExpectRollback()

	result, _, err := repo.List(ctx, repositories.DisputeFilter{}, repositories.CursorFilter{Limit: 10})

	require.Error(t, err)
	require.Nil(t, result)
	assert.Contains(t, err.Error(), "failed to list disputes")
	assert.Contains(t, err.Error(), "execute dispute list query")
}

func TestRepository_List_InvalidCursor(t *testing.T) {
	t.Parallel()

	repo, mock, finish := setupRepoV2(t)
	defer finish()

	ctx := context.Background()

	mock.ExpectBegin()
	mock.ExpectRollback()

	result, _, err := repo.List(
		ctx,
		repositories.DisputeFilter{},
		repositories.CursorFilter{Limit: 10, Cursor: "not-valid-base64"},
	)

	require.Error(t, err)
	require.Nil(t, result)
	require.ErrorIs(t, err, libHTTP.ErrInvalidCursor)
	assert.Contains(t, err.Error(), "failed to list disputes")
}

func TestRepository_List_WithStateFilter(t *testing.T) {
	t.Parallel()

	repo, mock, finish := setupRepoV2(t)
	defer finish()

	ctx := context.Background()
	now := time.Now().UTC()
	disputeID := uuid.New()
	exceptionID := uuid.New()
	state := dispute.DisputeStateOpen

	filter := repositories.DisputeFilter{
		State: &state,
	}
	cursor := repositories.CursorFilter{Limit: 10}

	mock.ExpectBegin()
	mock.ExpectQuery("SELECT").
		WillReturnRows(sqlmock.NewRows(disputeColumnList()).
			AddRow(
				disputeID.String(), exceptionID.String(),
				"OTHER", "OPEN", "Filtered",
				"user@test.com", sql.NullString{}, sql.NullString{},
				[]byte("[]"), now, now,
			))
	mock.ExpectCommit()

	result, _, err := repo.List(ctx, filter, cursor)

	require.NoError(t, err)
	require.Len(t, result, 1)
	assert.Equal(t, dispute.DisputeStateOpen, result[0].State)
}

func TestRepository_List_WithCategoryFilter(t *testing.T) {
	t.Parallel()

	repo, mock, finish := setupRepoV2(t)
	defer finish()

	ctx := context.Background()
	now := time.Now().UTC()
	disputeID := uuid.New()
	exceptionID := uuid.New()
	category := dispute.DisputeCategoryBankFeeError

	filter := repositories.DisputeFilter{
		Category: &category,
	}
	cursor := repositories.CursorFilter{Limit: 10}

	mock.ExpectBegin()
	mock.ExpectQuery("SELECT").
		WillReturnRows(sqlmock.NewRows(disputeColumnList()).
			AddRow(
				disputeID.String(), exceptionID.String(),
				"BANK_FEE_ERROR", "DRAFT", "Fee error",
				"analyst@test.com", sql.NullString{}, sql.NullString{},
				[]byte("[]"), now, now,
			))
	mock.ExpectCommit()

	result, _, err := repo.List(ctx, filter, cursor)

	require.NoError(t, err)
	require.Len(t, result, 1)
	assert.Equal(t, dispute.DisputeCategoryBankFeeError, result[0].Category)
}

func TestRepository_List_WithDateFilters(t *testing.T) {
	t.Parallel()

	repo, mock, finish := setupRepoV2(t)
	defer finish()

	ctx := context.Background()
	now := time.Now().UTC()
	disputeID := uuid.New()
	exceptionID := uuid.New()
	dateFrom := now.Add(-24 * time.Hour)
	dateTo := now

	filter := repositories.DisputeFilter{
		DateFrom: &dateFrom,
		DateTo:   &dateTo,
	}
	cursor := repositories.CursorFilter{Limit: 10}

	mock.ExpectBegin()
	mock.ExpectQuery("SELECT").
		WillReturnRows(sqlmock.NewRows(disputeColumnList()).
			AddRow(
				disputeID.String(), exceptionID.String(),
				"OTHER", "OPEN", "Date-filtered",
				"user@test.com", sql.NullString{}, sql.NullString{},
				[]byte("[]"), now, now,
			))
	mock.ExpectCommit()

	result, _, err := repo.List(ctx, filter, cursor)

	require.NoError(t, err)
	require.Len(t, result, 1)
}

func TestRepository_List_SortByCreatedAt(t *testing.T) {
	t.Parallel()

	repo, mock, finish := setupRepoV2(t)
	defer finish()

	ctx := context.Background()
	now := time.Now().UTC()
	id1 := uuid.New()
	id2 := uuid.New()
	exceptionID := uuid.New()

	cursor := repositories.CursorFilter{
		Limit:     10,
		SortBy:    "created_at",
		SortOrder: "ASC",
	}

	mock.ExpectBegin()
	mock.ExpectQuery("SELECT").
		WillReturnRows(sqlmock.NewRows(disputeColumnList()).
			AddRow(id1.String(), exceptionID.String(), "OTHER", "OPEN", "First", "u@t.com", sql.NullString{}, sql.NullString{}, []byte("[]"), now, now).
			AddRow(id2.String(), exceptionID.String(), "OTHER", "DRAFT", "Second", "u@t.com", sql.NullString{}, sql.NullString{}, []byte("[]"), now.Add(time.Minute), now.Add(time.Minute)))
	mock.ExpectCommit()

	result, _, err := repo.List(ctx, repositories.DisputeFilter{}, cursor)

	require.NoError(t, err)
	require.Len(t, result, 2)
}

func TestRepository_List_SortCursorPaginationMetadata(t *testing.T) {
	t.Parallel()

	repo, mock, finish := setupRepoV2(t)
	defer finish()

	ctx := context.Background()
	now := time.Now().UTC()
	id1 := uuid.New()
	id2 := uuid.New()
	exceptionID := uuid.New()

	cursor := repositories.CursorFilter{
		Limit:     1,
		SortBy:    "created_at",
		SortOrder: "ASC",
	}

	mock.ExpectBegin()
	mock.ExpectQuery("SELECT").
		WillReturnRows(sqlmock.NewRows(disputeColumnList()).
			AddRow(id1.String(), exceptionID.String(), "OTHER", "OPEN", "First", "u@t.com", sql.NullString{}, sql.NullString{}, []byte("[]"), now, now).
			AddRow(id2.String(), exceptionID.String(), "OTHER", "OPEN", "Second", "u@t.com", sql.NullString{}, sql.NullString{}, []byte("[]"), now.Add(time.Minute), now.Add(time.Minute)))
	mock.ExpectCommit()

	result, pagination, err := repo.List(ctx, repositories.DisputeFilter{}, cursor)

	require.NoError(t, err)
	require.Len(t, result, 1)
	assert.NotEmpty(t, pagination.Next)
}

// --- Helper functions ---

func TestNormalizeDisputeSortColumn(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name     string
		input    string
		expected string
	}{
		{"empty defaults to id", "", "id"},
		{"id stays id", "id", "id"},
		{"created_at allowed", "created_at", "created_at"},
		{"updated_at allowed", "updated_at", "updated_at"},
		{"state allowed", "state", "state"},
		{"category allowed", "category", "category"},
		{"unknown defaults to id", "unknown_column", "id"},
		{"sql injection defaults to id", "id; DROP TABLE disputes", "id"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			result := normalizeDisputeSortColumn(tt.input)
			assert.Equal(t, tt.expected, result)
		})
	}
}

func TestDisputeSortValue(t *testing.T) {
	t.Parallel()

	now := time.Now().UTC()
	d := &dispute.Dispute{
		ID:        uuid.New(),
		State:     dispute.DisputeStateOpen,
		Category:  dispute.DisputeCategoryBankFeeError,
		CreatedAt: now,
		UpdatedAt: now,
	}

	tests := []struct {
		name     string
		column   string
		expected string
	}{
		{"created_at", "created_at", now.Format(time.RFC3339Nano)},
		{"updated_at", "updated_at", now.Format(time.RFC3339Nano)},
		{"state", "state", "OPEN"},
		{"category", "category", "BANK_FEE_ERROR"},
		{"default falls back to ID", "unknown", d.ID.String()},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			result := disputeSortValue(d, tt.column)
			assert.Equal(t, tt.expected, result)
		})
	}
}

// --- Scan model tests ---

func TestScanDisputeRows_InvalidUUID_Sqlmock(t *testing.T) {
	t.Parallel()

	db, mock, err := sqlmock.New()
	require.NoError(t, err)
	defer db.Close()

	now := time.Now().UTC()

	mock.ExpectQuery("SELECT").
		WillReturnRows(sqlmock.NewRows(disputeColumnList()).
			AddRow("bad-uuid", uuid.New().String(), "OTHER", "OPEN", "desc", "u@t.com",
				sql.NullString{}, sql.NullString{}, []byte("[]"), now, now))

	rows, err := db.Query("SELECT 1")
	require.NoError(t, err)
	defer rows.Close()

	require.True(t, rows.Next())

	result, err := scanDisputeRows(rows)

	require.Error(t, err)
	require.Nil(t, result)
	assert.Contains(t, err.Error(), "parse dispute id")
}

func TestScanDisputeRows_InvalidExceptionID_Sqlmock(t *testing.T) {
	t.Parallel()

	db, mock, err := sqlmock.New()
	require.NoError(t, err)
	defer db.Close()

	now := time.Now().UTC()

	mock.ExpectQuery("SELECT").
		WillReturnRows(sqlmock.NewRows(disputeColumnList()).
			AddRow(uuid.New().String(), "bad-uuid", "OTHER", "OPEN", "desc", "u@t.com",
				sql.NullString{}, sql.NullString{}, []byte("[]"), now, now))

	rows, err := db.Query("SELECT 1")
	require.NoError(t, err)
	defer rows.Close()

	require.True(t, rows.Next())

	result, err := scanDisputeRows(rows)

	require.Error(t, err)
	require.Nil(t, result)
	assert.Contains(t, err.Error(), "parse exception id")
}

func TestScanDisputeRows_InvalidCategory_Sqlmock(t *testing.T) {
	t.Parallel()

	db, mock, err := sqlmock.New()
	require.NoError(t, err)
	defer db.Close()

	now := time.Now().UTC()

	mock.ExpectQuery("SELECT").
		WillReturnRows(sqlmock.NewRows(disputeColumnList()).
			AddRow(uuid.New().String(), uuid.New().String(), "INVALID_CATEGORY", "OPEN", "desc", "u@t.com",
				sql.NullString{}, sql.NullString{}, []byte("[]"), now, now))

	rows, err := db.Query("SELECT 1")
	require.NoError(t, err)
	defer rows.Close()

	require.True(t, rows.Next())

	result, err := scanDisputeRows(rows)

	require.Error(t, err)
	require.Nil(t, result)
	assert.Contains(t, err.Error(), "parse category")
}

func TestScanDisputeRows_InvalidState_Sqlmock(t *testing.T) {
	t.Parallel()

	db, mock, err := sqlmock.New()
	require.NoError(t, err)
	defer db.Close()

	now := time.Now().UTC()

	mock.ExpectQuery("SELECT").
		WillReturnRows(sqlmock.NewRows(disputeColumnList()).
			AddRow(uuid.New().String(), uuid.New().String(), "OTHER", "INVALID_STATE", "desc", "u@t.com",
				sql.NullString{}, sql.NullString{}, []byte("[]"), now, now))

	rows, err := db.Query("SELECT 1")
	require.NoError(t, err)
	defer rows.Close()

	require.True(t, rows.Next())

	result, err := scanDisputeRows(rows)

	require.Error(t, err)
	require.Nil(t, result)
	assert.Contains(t, err.Error(), "parse state")
}

func TestScanDisputeRows_InvalidEvidenceJSON_Sqlmock(t *testing.T) {
	t.Parallel()

	db, mock, err := sqlmock.New()
	require.NoError(t, err)
	defer db.Close()

	now := time.Now().UTC()

	mock.ExpectQuery("SELECT").
		WillReturnRows(sqlmock.NewRows(disputeColumnList()).
			AddRow(uuid.New().String(), uuid.New().String(), "OTHER", "OPEN", "desc", "u@t.com",
				sql.NullString{}, sql.NullString{}, []byte(`{not valid json`), now, now))

	rows, err := db.Query("SELECT 1")
	require.NoError(t, err)
	defer rows.Close()

	require.True(t, rows.Next())

	result, err := scanDisputeRows(rows)

	require.Error(t, err)
	require.Nil(t, result)
	assert.Contains(t, err.Error(), "unmarshal evidence")
}
