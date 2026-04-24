// Copyright 2025 Lerian Studio. All rights reserved.
// Use of this source code is governed by an Elastic License 2.0
// that can be found in the LICENSE.md file.

//go:build unit

package transaction

import (
	"context"
	"database/sql"
	"database/sql/driver"
	"errors"
	"fmt"
	"testing"
	"time"

	sqlmock "github.com/DATA-DOG/go-sqlmock"
	"github.com/Masterminds/squirrel"
	"github.com/google/uuid"
	"github.com/shopspring/decimal"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	libHTTP "github.com/LerianStudio/lib-commons/v5/commons/net/http"
	pgcommon "github.com/LerianStudio/matcher/internal/ingestion/adapters/postgres/common"
	"github.com/LerianStudio/matcher/internal/ingestion/domain/repositories"
	sharedpg "github.com/LerianStudio/matcher/internal/shared/adapters/postgres/common"
	"github.com/LerianStudio/matcher/internal/shared/constants"
	shared "github.com/LerianStudio/matcher/internal/shared/domain"
	"github.com/LerianStudio/matcher/internal/shared/infrastructure/testutil"
)

// =============================================================================
// transactionSortValue
// =============================================================================

func TestTransactionSortValue(t *testing.T) {
	t.Parallel()

	now := time.Now().UTC()
	txID := uuid.New()

	tx := &shared.Transaction{
		ID:               txID,
		Date:             now,
		Status:           shared.TransactionStatusMatched,
		ExtractionStatus: shared.ExtractionStatusComplete,
		CreatedAt:        now,
	}

	tests := []struct {
		name     string
		column   string
		expected string
	}{
		{"created_at", columnCreatedAt, now.Format(time.RFC3339Nano)},
		{"date", columnDate, now.Format(time.RFC3339Nano)},
		{"status", columnStatus, "MATCHED"},
		{"extraction_status", columnExtractionStatus, "COMPLETE"},
		{"default", "unknown_column", txID.String()},
		{"id", "id", txID.String()},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			result := transactionSortValue(tx, tt.column)
			assert.Equal(t, tt.expected, result)
		})
	}

	t.Run("nil transaction returns empty string", func(t *testing.T) {
		t.Parallel()

		assert.Empty(t, transactionSortValue(nil, columnCreatedAt))
	})
}

// =============================================================================
// escapeLikePattern
// =============================================================================

func TestEscapeLikePattern(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name     string
		input    string
		expected string
	}{
		{"no special chars", "hello", "hello"},
		{"backslash", `hello\world`, `hello\\world`},
		{"percent", "50%", `50\%`},
		{"underscore", "hello_world", `hello\_world`},
		{"all special chars", `50%_\`, `50\%\_\\`},
		{"empty string", "", ""},
		{"multiple percents", "%%", `\%\%`},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			result := escapeLikePattern(tt.input)
			assert.Equal(t, tt.expected, result)
		})
	}
}

// =============================================================================
// buildSearchBaseWhere
// =============================================================================

func TestBuildSearchBaseWhere(t *testing.T) {
	t.Parallel()

	contextID := uuid.New()
	where := buildSearchBaseWhere(contextID)

	require.Len(t, where, 2)
}

// =============================================================================
// applySearchFilters
// =============================================================================

func TestApplySearchFilters_AllFilters(t *testing.T) {
	t.Parallel()

	amountMin := decimal.NewFromFloat(10.00)
	amountMax := decimal.NewFromFloat(500.00)
	dateFrom := time.Now().UTC().Add(-7 * 24 * time.Hour)
	dateTo := time.Now().UTC()
	sourceID := uuid.New()

	params := repositories.TransactionSearchParams{
		Query:     "test query",
		AmountMin: &amountMin,
		AmountMax: &amountMax,
		DateFrom:  &dateFrom,
		DateTo:    &dateTo,
		Reference: "REF-123",
		Currency:  "usd",
		SourceID:  &sourceID,
		Status:    "matched",
	}

	// Build the query just to check it doesn't panic
	contextID := uuid.New()
	baseWhere := buildSearchBaseWhere(contextID)

	qb := squirrelSelect(t)
	for _, w := range baseWhere {
		qb = qb.Where(w)
	}

	qb, filterErr := applySearchFilters(qb, params)
	require.NoError(t, filterErr)
	_, _, err := qb.ToSql()
	require.NoError(t, err)
}

func TestApplySearchFilters_EmptyParams(t *testing.T) {
	t.Parallel()

	params := repositories.TransactionSearchParams{}

	qb := squirrelSelect(t)
	qb, filterErr := applySearchFilters(qb, params)
	require.NoError(t, filterErr)
	_, _, err := qb.ToSql()
	require.NoError(t, err)
}

func TestApplySearchFilters_NilSourceID(t *testing.T) {
	t.Parallel()

	params := repositories.TransactionSearchParams{
		SourceID: nil,
	}

	qb := squirrelSelect(t)
	qb, filterErr := applySearchFilters(qb, params)
	require.NoError(t, filterErr)
	_, _, err := qb.ToSql()
	require.NoError(t, err)
}

func TestApplySearchFilters_NilUUIDSourceID(t *testing.T) {
	t.Parallel()

	nilUUID := uuid.Nil
	params := repositories.TransactionSearchParams{
		SourceID: &nilUUID,
	}

	qb := squirrelSelect(t)
	qb, filterErr := applySearchFilters(qb, params)
	require.NoError(t, filterErr)
	query, _, err := qb.ToSql()
	require.NoError(t, err)
	// Nil UUID should be skipped
	assert.NotContains(t, query, "source_id")
}

func TestApplySearchFilters_InvalidStatus(t *testing.T) {
	t.Parallel()

	params := repositories.TransactionSearchParams{
		Status: "INVALID_STATUS",
	}

	qb := squirrelSelect(t)
	_, filterErr := applySearchFilters(qb, params)
	require.Error(t, filterErr)
	assert.ErrorContains(t, filterErr, "invalid status filter")
}

func TestApplySearchFilters_ValidStatuses(t *testing.T) {
	t.Parallel()

	validStatuses := []string{"MATCHED", "UNMATCHED", "PENDING_REVIEW", "IGNORED", "matched", "unmatched"}

	for _, status := range validStatuses {
		t.Run(status, func(t *testing.T) {
			t.Parallel()

			params := repositories.TransactionSearchParams{
				Status: status,
			}

			qb := squirrelSelect(t)
			qb, filterErr := applySearchFilters(qb, params)
			require.NoError(t, filterErr)
			query, _, err := qb.ToSql()
			require.NoError(t, err)
			assert.Contains(t, query, "status")
		})
	}
}

// =============================================================================
// calculateTransactionPagination
// =============================================================================

func TestCalculateTransactionPagination_EmptyTransactions(t *testing.T) {
	t.Parallel()

	pagination, err := calculateTransactionPagination(nil, true, true, false, "prev", "id")
	require.NoError(t, err)
	assert.Empty(t, pagination.Next)
	assert.Empty(t, pagination.Prev)
}

func TestCalculateTransactionPagination_IDCursor(t *testing.T) {
	t.Parallel()

	id1 := uuid.New()
	id2 := uuid.New()
	transactions := []*shared.Transaction{
		{ID: id1, CreatedAt: time.Now().UTC()},
		{ID: id2, CreatedAt: time.Now().UTC()},
	}

	pagination, err := calculateTransactionPagination(
		transactions, true, true, true, "next", "id",
	)
	require.NoError(t, err)
	assert.NotEmpty(t, pagination.Next)
}

func TestCalculateTransactionPagination_SortCursor(t *testing.T) {
	t.Parallel()

	now := time.Now().UTC()
	id1 := uuid.New()
	id2 := uuid.New()
	transactions := []*shared.Transaction{
		{ID: id1, CreatedAt: now.Add(-time.Hour), Date: now.Add(-time.Hour)},
		{ID: id2, CreatedAt: now, Date: now},
	}

	pagination, err := calculateTransactionPagination(
		transactions, false, true, true, "next", columnCreatedAt,
	)
	require.NoError(t, err)
	// Sort cursor returns strings built from pkgHTTP.CalculateSortCursorPagination
	assert.NotNil(t, pagination)
}

func TestCalculateTransactionPagination_SingleTransaction(t *testing.T) {
	t.Parallel()

	id1 := uuid.New()
	transactions := []*shared.Transaction{
		{ID: id1, CreatedAt: time.Now().UTC()},
	}

	pagination, err := calculateTransactionPagination(
		transactions, true, true, false, "prev", "id",
	)
	require.NoError(t, err)
	assert.NotNil(t, pagination)
}

func TestCalculateTransactionSortPagination_PropagatesCalculatorError(t *testing.T) {
	t.Parallel()

	_, err := calculateTransactionSortPagination(
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

func TestCalculateTransactionSortPagination_NilCalculator(t *testing.T) {
	t.Parallel()

	_, err := calculateTransactionSortPagination(
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

func TestCalculateTransactionPagination_NilBoundaryRecord(t *testing.T) {
	t.Parallel()

	_, err := calculateTransactionPagination(
		[]*shared.Transaction{nil},
		true,
		true,
		false,
		libHTTP.CursorDirectionNext,
		columnCreatedAt,
	)

	require.Error(t, err)
	assert.ErrorIs(t, err, sharedpg.ErrSortCursorBoundaryRecordNil)
	assert.Contains(t, err.Error(), "validate transaction pagination boundaries")
}

// =============================================================================
// SearchTransactions
// =============================================================================

func TestSearchTransactions_NilRepoProvider(t *testing.T) {
	t.Parallel()

	repo := NewRepository(nil)
	ctx := context.Background()

	_, _, err := repo.SearchTransactions(ctx, uuid.New(), repositories.TransactionSearchParams{})
	require.ErrorIs(t, err, errTxRepoNotInit)
}

func TestSearchTransactions_NilContextID(t *testing.T) {
	t.Parallel()

	provider := &testutil.MockInfrastructureProvider{}
	repo := NewRepository(provider)
	ctx := context.Background()

	_, _, err := repo.SearchTransactions(ctx, uuid.Nil, repositories.TransactionSearchParams{})
	require.ErrorIs(t, err, errContextIDRequired)
}

func TestSearchTransactions_DefaultLimits(t *testing.T) {
	t.Parallel()

	repo, mock, finish := setupRepositoryWithMock(t)
	defer finish()

	ctx := context.Background()
	contextID := uuid.New()

	// Count query returns 0
	mock.ExpectQuery("SELECT COUNT").WillReturnRows(
		sqlmock.NewRows([]string{"count"}).AddRow(int64(0)),
	)

	result, total, err := repo.SearchTransactions(ctx, contextID, repositories.TransactionSearchParams{
		Limit: 0,
	})
	require.NoError(t, err)
	assert.Equal(t, int64(0), total)
	assert.Empty(t, result)
}

func TestSearchTransactions_MaxLimitCapped(t *testing.T) {
	t.Parallel()

	repo, mock, finish := setupRepositoryWithMock(t)
	defer finish()

	ctx := context.Background()
	contextID := uuid.New()

	// Count query returns 0
	mock.ExpectQuery("SELECT COUNT").WillReturnRows(
		sqlmock.NewRows([]string{"count"}).AddRow(int64(0)),
	)

	result, total, err := repo.SearchTransactions(ctx, contextID, repositories.TransactionSearchParams{
		Limit: 100, // Should be capped to 50
	})
	require.NoError(t, err)
	assert.Equal(t, int64(0), total)
	assert.Empty(t, result)
}

func TestSearchTransactions_NegativeOffset(t *testing.T) {
	t.Parallel()

	repo, mock, finish := setupRepositoryWithMock(t)
	defer finish()

	ctx := context.Background()
	contextID := uuid.New()

	// Count query returns 0
	mock.ExpectQuery("SELECT COUNT").WillReturnRows(
		sqlmock.NewRows([]string{"count"}).AddRow(int64(0)),
	)

	result, total, err := repo.SearchTransactions(ctx, contextID, repositories.TransactionSearchParams{
		Limit:  10,
		Offset: -5, // Should be set to 0
	})
	require.NoError(t, err)
	assert.Equal(t, int64(0), total)
	assert.Empty(t, result)
}

func TestSearchTransactions_Success(t *testing.T) {
	t.Parallel()

	repo, mock, finish := setupRepositoryWithMock(t)
	defer finish()

	ctx := context.Background()
	contextID := uuid.New()
	entity := createValidTransactionEntity()

	// Count query returns 1
	mock.ExpectQuery("SELECT COUNT").WillReturnRows(
		sqlmock.NewRows([]string{"count"}).AddRow(int64(1)),
	)

	// Data query returns the transaction
	mock.ExpectQuery("SELECT .* FROM transactions").WillReturnRows(
		sqlmock.NewRows(testTransactionColumns()).AddRow(createTransactionRow(entity)...),
	)

	result, total, err := repo.SearchTransactions(ctx, contextID, repositories.TransactionSearchParams{
		Limit: 10,
	})
	require.NoError(t, err)
	assert.Equal(t, int64(1), total)
	require.Len(t, result, 1)
}

func TestSearchTransactions_CountError(t *testing.T) {
	t.Parallel()

	repo, mock, finish := setupRepositoryWithMock(t)
	defer finish()

	ctx := context.Background()
	contextID := uuid.New()

	mock.ExpectQuery("SELECT COUNT").WillReturnError(errors.New("count error"))

	_, _, err := repo.SearchTransactions(ctx, contextID, repositories.TransactionSearchParams{
		Limit: 10,
	})
	require.Error(t, err)
	assert.Contains(t, err.Error(), "failed to search transactions")
}

func TestSearchTransactions_DataQueryError(t *testing.T) {
	t.Parallel()

	repo, mock, finish := setupRepositoryWithMock(t)
	defer finish()

	ctx := context.Background()
	contextID := uuid.New()

	// Count returns 1 so we proceed to data query
	mock.ExpectQuery("SELECT COUNT").WillReturnRows(
		sqlmock.NewRows([]string{"count"}).AddRow(int64(1)),
	)

	mock.ExpectQuery("SELECT .* FROM transactions").WillReturnError(errors.New("data query error"))

	_, _, err := repo.SearchTransactions(ctx, contextID, repositories.TransactionSearchParams{
		Limit: 10,
	})
	require.Error(t, err)
	assert.Contains(t, err.Error(), "failed to search transactions")
}

func TestSearchTransactions_WithAllFilters(t *testing.T) {
	t.Parallel()

	repo, mock, finish := setupRepositoryWithMock(t)
	defer finish()

	ctx := context.Background()
	contextID := uuid.New()
	sourceID := uuid.New()
	amountMin := decimal.NewFromFloat(10.00)
	amountMax := decimal.NewFromFloat(100.00)
	dateFrom := time.Now().UTC().Add(-7 * 24 * time.Hour)
	dateTo := time.Now().UTC()

	// Count returns 0
	mock.ExpectQuery("SELECT COUNT").WillReturnRows(
		sqlmock.NewRows([]string{"count"}).AddRow(int64(0)),
	)

	result, total, err := repo.SearchTransactions(ctx, contextID, repositories.TransactionSearchParams{
		Query:     "test",
		AmountMin: &amountMin,
		AmountMax: &amountMax,
		DateFrom:  &dateFrom,
		DateTo:    &dateTo,
		Reference: "REF-001",
		Currency:  "USD",
		SourceID:  &sourceID,
		Status:    "MATCHED",
		Limit:     20,
		Offset:    5,
	})
	require.NoError(t, err)
	assert.Equal(t, int64(0), total)
	assert.Empty(t, result)
}

// =============================================================================
// Repository — FindByContextAndIDs success
// =============================================================================

func TestRepository_FindByContextAndIDs_Success(t *testing.T) {
	t.Parallel()

	repo, mock, finish := setupRepositoryWithMock(t)
	defer finish()

	ctx := context.Background()
	contextID := uuid.New()
	entity := createValidTransactionEntity()

	mock.ExpectBegin()
	mock.ExpectQuery("SELECT .* FROM transactions").
		WillReturnRows(sqlmock.NewRows(testTransactionColumns()).AddRow(createTransactionRow(entity)...))
	mock.ExpectCommit()

	result, err := repo.FindByContextAndIDs(ctx, contextID, []uuid.UUID{entity.ID})
	require.NoError(t, err)
	require.Len(t, result, 1)
	assert.Equal(t, entity.ExternalID, result[0].ExternalID)
}

func TestRepository_FindByContextAndIDs_DBError(t *testing.T) {
	t.Parallel()

	repo, mock, finish := setupRepositoryWithMock(t)
	defer finish()

	ctx := context.Background()
	contextID := uuid.New()

	mock.ExpectBegin()
	mock.ExpectQuery("SELECT .* FROM transactions").
		WillReturnError(errors.New("db error"))
	mock.ExpectRollback()

	result, err := repo.FindByContextAndIDs(ctx, contextID, []uuid.UUID{uuid.New()})
	require.Error(t, err)
	require.Nil(t, result)
	assert.Contains(t, err.Error(), "failed to find transactions by context and ids")
}

// =============================================================================
// Repository — FindBySourceAndExternalID DB error
// =============================================================================

func TestRepository_FindBySourceAndExternalID_DBError(t *testing.T) {
	t.Parallel()

	repo, mock, finish := setupRepositoryWithMock(t)
	defer finish()

	ctx := context.Background()
	sourceID := uuid.New()

	mock.ExpectBegin()
	mock.ExpectQuery("SELECT .* FROM transactions WHERE source_id").
		WillReturnError(errors.New("db error"))
	mock.ExpectRollback()

	result, err := repo.FindBySourceAndExternalID(ctx, sourceID, "ext")
	require.Error(t, err)
	require.Nil(t, result)
	assert.Contains(t, err.Error(), "failed to find transaction by external ID")
}

// =============================================================================
// Repository — CreateBatch exec error for second item
// =============================================================================

func TestRepository_CreateBatch_ExecError(t *testing.T) {
	t.Parallel()

	repo, mock, finish := setupRepositoryWithMock(t)
	defer finish()

	ctx := context.Background()
	entity1 := createValidTransactionEntity()
	entity2 := createValidTransactionEntity()

	mock.ExpectBegin()
	mock.ExpectPrepare("INSERT INTO transactions")
	mock.ExpectExec("INSERT INTO transactions").WillReturnResult(sqlmock.NewResult(1, 1))
	mock.ExpectExec("INSERT INTO transactions").WillReturnError(errors.New("exec error"))
	mock.ExpectRollback()

	result, err := repo.CreateBatch(ctx, []*shared.Transaction{entity1, entity2})
	require.Error(t, err)
	require.Nil(t, result)
	assert.Contains(t, err.Error(), "failed to create batch")
}

// =============================================================================
// Repository — CreateBatchWithTx on nil repo
// =============================================================================

func TestRepository_CreateBatchWithTx_NilRepoProvider(t *testing.T) {
	t.Parallel()

	repo := NewRepository(nil)
	ctx := context.Background()

	_, err := repo.CreateBatchWithTx(ctx, nil, []*shared.Transaction{createValidTransactionEntity()})
	require.ErrorIs(t, err, errTxRepoNotInit)
}

// =============================================================================
// Repository — MarkMatchedWithTx success
// =============================================================================

func TestRepository_MarkMatchedWithTx_Success(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	db, mock, err := sqlmock.New()
	require.NoError(t, err)

	t.Cleanup(func() { db.Close() })

	provider := &testutil.MockInfrastructureProvider{}
	repo := NewRepository(provider)

	mock.ExpectBegin()

	tx, err := db.BeginTx(ctx, nil)
	require.NoError(t, err)

	contextID := uuid.New()
	txIDs := []uuid.UUID{uuid.New()}

	mock.ExpectExec("UPDATE transactions SET").WillReturnResult(sqlmock.NewResult(0, 1))

	err = repo.MarkMatchedWithTx(ctx, tx, contextID, txIDs)
	require.NoError(t, err)
}

// =============================================================================
// Repository — MarkPendingReviewWithTx success
// =============================================================================

func TestRepository_MarkPendingReviewWithTx_Success(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	db, mock, err := sqlmock.New()
	require.NoError(t, err)

	t.Cleanup(func() { db.Close() })

	provider := &testutil.MockInfrastructureProvider{}
	repo := NewRepository(provider)

	mock.ExpectBegin()

	tx, err := db.BeginTx(ctx, nil)
	require.NoError(t, err)

	contextID := uuid.New()
	txIDs := []uuid.UUID{uuid.New()}

	mock.ExpectExec("UPDATE transactions SET").WillReturnResult(sqlmock.NewResult(0, 1))

	err = repo.MarkPendingReviewWithTx(ctx, tx, contextID, txIDs)
	require.NoError(t, err)
}

// =============================================================================
// Repository — MarkUnmatchedWithTx success
// =============================================================================

func TestRepository_MarkUnmatchedWithTx_Success(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	db, mock, err := sqlmock.New()
	require.NoError(t, err)

	t.Cleanup(func() { db.Close() })

	provider := &testutil.MockInfrastructureProvider{}
	repo := NewRepository(provider)

	mock.ExpectBegin()

	tx, err := db.BeginTx(ctx, nil)
	require.NoError(t, err)

	contextID := uuid.New()
	txIDs := []uuid.UUID{uuid.New()}

	mock.ExpectExec("UPDATE transactions SET").WillReturnResult(sqlmock.NewResult(0, 1))

	err = repo.MarkUnmatchedWithTx(ctx, tx, contextID, txIDs)
	require.NoError(t, err)
}

// =============================================================================
// Repository — UpdateStatusWithTx success
// =============================================================================

func TestRepository_UpdateStatusWithTx_Success(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	db, mock, err := sqlmock.New()
	require.NoError(t, err)

	t.Cleanup(func() { db.Close() })

	provider := &testutil.MockInfrastructureProvider{}
	repo := NewRepository(provider)

	mock.ExpectBegin()

	tx, err := db.BeginTx(ctx, nil)
	require.NoError(t, err)

	id := uuid.New()
	contextID := uuid.New()
	entity := createValidTransactionEntity()
	entity.ID = id
	entity.Status = shared.TransactionStatusMatched

	mock.ExpectQuery("UPDATE transactions SET status").
		WithArgs(shared.TransactionStatusMatched.String(), id.String(), contextID.String()).
		WillReturnRows(sqlmock.NewRows(testTransactionColumns()).AddRow(createTransactionRow(entity)...))

	result, err := repo.UpdateStatusWithTx(ctx, tx, id, contextID, shared.TransactionStatusMatched)
	require.NoError(t, err)
	require.NotNil(t, result)
}

// =============================================================================
// Repository — ListUnmatchedByContext with date filters success
// =============================================================================

func TestRepository_ListUnmatchedByContext_WithDatesSuccess(t *testing.T) {
	t.Parallel()

	repo, mock, finish := setupRepositoryWithMock(t)
	defer finish()

	ctx := context.Background()
	contextID := uuid.New()
	entity := createValidTransactionEntity()
	entity.Status = shared.TransactionStatusUnmatched

	start := time.Now().UTC().Add(-7 * 24 * time.Hour)
	end := time.Now().UTC()

	mock.ExpectQuery("SELECT .* FROM transactions WHERE").
		WillReturnRows(sqlmock.NewRows(testTransactionColumns()).AddRow(createTransactionRow(entity)...))

	result, err := repo.ListUnmatchedByContext(ctx, contextID, &start, &end, 10, 0)
	require.NoError(t, err)
	require.Len(t, result, 1)
}

// =============================================================================
// Repository — FindByJobID with date sort
// =============================================================================

func TestRepository_FindByJobID_DateSort(t *testing.T) {
	t.Parallel()

	repo, mock, finish := setupRepositoryWithMock(t)
	defer finish()

	ctx := context.Background()
	jobID := uuid.New()
	entity := createValidTransactionEntity()
	entity.IngestionJobID = jobID

	mock.ExpectQuery("SELECT .* FROM transactions").
		WillReturnRows(sqlmock.NewRows(testTransactionColumns()).AddRow(createTransactionRow(entity)...))

	filter := repositories.CursorFilter{
		Limit:     10,
		SortBy:    "date",
		SortOrder: "DESC",
	}

	result, _, err := repo.FindByJobID(ctx, jobID, filter)
	require.NoError(t, err)
	require.Len(t, result, 1)
}

func TestRepository_FindByJobID_StatusSort(t *testing.T) {
	t.Parallel()

	repo, mock, finish := setupRepositoryWithMock(t)
	defer finish()

	ctx := context.Background()
	jobID := uuid.New()
	entity := createValidTransactionEntity()
	entity.IngestionJobID = jobID

	mock.ExpectQuery("SELECT .* FROM transactions").
		WillReturnRows(sqlmock.NewRows(testTransactionColumns()).AddRow(createTransactionRow(entity)...))

	filter := repositories.CursorFilter{
		Limit:     10,
		SortBy:    "status",
		SortOrder: "ASC",
	}

	result, _, err := repo.FindByJobID(ctx, jobID, filter)
	require.NoError(t, err)
	require.Len(t, result, 1)
}

// =============================================================================
// Repository — scanRowsToTransactions with row error
// =============================================================================

func TestScanRowsToTransactions_RowError(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	db, mock, err := sqlmock.New()
	require.NoError(t, err)

	t.Cleanup(func() { db.Close() })

	rowErr := errors.New("row iteration error")
	rows := sqlmock.NewRows([]string{"id"}).
		AddRow("test").
		RowError(0, rowErr)
	mock.ExpectQuery("SELECT").WillReturnRows(rows)

	sqlRows, err := db.QueryContext(ctx, "SELECT 1")
	require.NoError(t, err)

	defer sqlRows.Close()

	mockScan := func(scanner interface{ Scan(dest ...any) error }) (*shared.Transaction, error) {
		return &shared.Transaction{}, nil
	}

	result, err := scanRowsToTransactions(sqlRows, mockScan)
	require.Error(t, err)
	require.Nil(t, result)
	assert.Contains(t, err.Error(), "failed to iterate rows")
}

// =============================================================================
// Repository — applyCursorPagination
// =============================================================================

func TestApplyCursorPagination_IDCursorNoCursor(t *testing.T) {
	t.Parallel()

	qb := squirrelSelect(t)
	result, dir, pn, err := pgcommon.ApplyCursorPagination(qb, "", "id", "ASC", 10, true, "transactions")
	require.NoError(t, err)
	assert.NotNil(t, result)
	assert.Equal(t, "ASC", dir)
	assert.Equal(t, "next", pn, "cursorDirection defaults to next when no cursor is provided")
}

func TestApplyCursorPagination_SortCursorNoCursor(t *testing.T) {
	t.Parallel()

	qb := squirrelSelect(t)
	result, dir, pn, err := pgcommon.ApplyCursorPagination(qb, "", "created_at", "DESC", 10, false, "transactions")
	require.NoError(t, err)
	assert.NotNil(t, result)
	assert.Equal(t, "DESC", dir)
	assert.Equal(t, "next", pn, "cursorDirection defaults to next when no cursor is provided")
}

// =============================================================================
// Repository — ExistsBySourceAndExternalID scan error
// =============================================================================

func TestRepository_ExistsBySourceAndExternalID_ScanError(t *testing.T) {
	t.Parallel()

	repo, mock, finish := setupRepositoryWithMock(t)
	defer finish()

	ctx := context.Background()
	sourceID := uuid.New()

	mock.ExpectBegin()
	mock.ExpectQuery("SELECT EXISTS").
		WillReturnRows(sqlmock.NewRows([]string{"exists"}).AddRow("not-a-bool"))
	mock.ExpectRollback()

	_, err := repo.ExistsBySourceAndExternalID(ctx, sourceID, "ext")
	require.Error(t, err)
	assert.Contains(t, err.Error(), "failed to check transaction existence")
}

// =============================================================================
// Repository — ExistsBulkBySourceAndExternalID scan error
// =============================================================================

func TestRepository_ExistsBulkBySourceAndExternalID_ScanError(t *testing.T) {
	t.Parallel()

	repo, mock, finish := setupRepositoryWithMock(t)
	defer finish()

	ctx := context.Background()
	sourceID := uuid.New()
	keys := []repositories.ExternalIDKey{
		{SourceID: sourceID, ExternalID: "ext-1"},
	}

	mock.ExpectBegin()
	mock.ExpectQuery("SELECT t.source_id, t.external_id FROM transactions").
		WillReturnRows(sqlmock.NewRows([]string{"source_id", "external_id"}).
			AddRow("not-a-uuid", "ext-1"))
	mock.ExpectRollback()

	_, err := repo.ExistsBulkBySourceAndExternalID(ctx, keys)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "failed to check bulk transaction existence")
}

// =============================================================================
// Repository — CreateWithTx success
// =============================================================================

func TestRepository_CreateWithTx_Success(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	db, mock, err := sqlmock.New()
	require.NoError(t, err)

	t.Cleanup(func() { db.Close() })

	provider := &testutil.MockInfrastructureProvider{}
	repo := NewRepository(provider)

	entity := createValidTransactionEntity()

	mock.ExpectBegin()

	tx, err := db.BeginTx(ctx, nil)
	require.NoError(t, err)

	mock.ExpectExec("INSERT INTO transactions").
		WillReturnResult(sqlmock.NewResult(1, 1))

	mock.ExpectQuery("SELECT .* FROM transactions WHERE id").
		WillReturnRows(sqlmock.NewRows(testTransactionColumns()).AddRow(createTransactionRow(entity)...))

	result, err := repo.CreateWithTx(ctx, tx, entity)
	require.NoError(t, err)
	require.NotNil(t, result)
	assert.Equal(t, entity.ExternalID, result.ExternalID)
}

// =============================================================================
// Repository — Create with invalid entity status
// =============================================================================

func TestRepository_Create_InvalidEntityStatus(t *testing.T) {
	t.Parallel()

	provider := &testutil.MockInfrastructureProvider{}
	repo := NewRepository(provider)
	ctx := context.Background()

	entity := createValidTransactionEntity()
	entity.Status = shared.TransactionStatus("INVALID")

	result, err := repo.Create(ctx, entity)
	require.Error(t, err)
	require.Nil(t, result)
}

// =============================================================================
// Repository — FindByJobAndContextID sort by extraction_status
// =============================================================================

func TestRepository_FindByJobAndContextID_ExtractionStatusSort(t *testing.T) {
	t.Parallel()

	repo, mock, finish := setupRepositoryWithMock(t)
	defer finish()

	ctx := context.Background()
	jobID := uuid.New()
	contextID := uuid.New()
	entity := createValidTransactionEntity()

	mock.ExpectQuery("SELECT .* FROM transactions").
		WillReturnRows(sqlmock.NewRows(testTransactionColumns()).AddRow(createTransactionRow(entity)...))

	filter := repositories.CursorFilter{
		Limit:     10,
		SortBy:    "extraction_status",
		SortOrder: "ASC",
	}

	result, _, err := repo.FindByJobAndContextID(ctx, jobID, contextID, filter)
	require.NoError(t, err)
	require.Len(t, result, 1)
}

func TestRepository_FindByJobAndContextID_LimitCappedAtMaximum(t *testing.T) {
	t.Parallel()

	repo, mock, finish := setupRepositoryWithMock(t)
	defer finish()

	ctx := context.Background()
	jobID := uuid.New()
	contextID := uuid.New()

	mock.ExpectQuery(fmt.Sprintf("LIMIT %d", constants.MaximumPaginationLimit+1)).
		WillReturnRows(sqlmock.NewRows(testTransactionColumns()))

	filter := repositories.CursorFilter{
		Limit:     constants.MaximumPaginationLimit + 999,
		SortBy:    "id",
		SortOrder: "ASC",
	}

	result, pagination, err := repo.FindByJobAndContextID(ctx, jobID, contextID, filter)
	require.NoError(t, err)
	assert.Empty(t, result)
	assert.Empty(t, pagination.Next)
	assert.Empty(t, pagination.Prev)
}

// =============================================================================
// Repository — FindByJobID with pagination (more results than limit)
// =============================================================================

func TestRepository_FindByJobID_Pagination(t *testing.T) {
	t.Parallel()

	repo, mock, finish := setupRepositoryWithMock(t)
	defer finish()

	ctx := context.Background()
	jobID := uuid.New()

	// Create 3 entities (limit is 2, so we have pagination)
	entity1 := createValidTransactionEntity()
	entity1.IngestionJobID = jobID
	entity2 := createValidTransactionEntity()
	entity2.IngestionJobID = jobID
	entity3 := createValidTransactionEntity()
	entity3.IngestionJobID = jobID

	mock.ExpectQuery("SELECT .* FROM transactions").
		WillReturnRows(sqlmock.NewRows(testTransactionColumns()).
			AddRow(createTransactionRow(entity1)...).
			AddRow(createTransactionRow(entity2)...).
			AddRow(createTransactionRow(entity3)...))

	filter := repositories.CursorFilter{
		Limit:     2,
		SortBy:    "id",
		SortOrder: "ASC",
	}

	result, pagination, err := repo.FindByJobID(ctx, jobID, filter)
	require.NoError(t, err)
	require.Len(t, result, 2)
	assert.NotEmpty(t, pagination.Next)
}

func TestRepository_FindByJobID_LimitCappedAtMaximum(t *testing.T) {
	t.Parallel()

	repo, mock, finish := setupRepositoryWithMock(t)
	defer finish()

	ctx := context.Background()
	jobID := uuid.New()

	mock.ExpectQuery(fmt.Sprintf("LIMIT %d", constants.MaximumPaginationLimit+1)).
		WillReturnRows(sqlmock.NewRows(testTransactionColumns()))

	filter := repositories.CursorFilter{
		Limit:     constants.MaximumPaginationLimit + 500,
		SortBy:    "id",
		SortOrder: "ASC",
	}

	result, pagination, err := repo.FindByJobID(ctx, jobID, filter)
	require.NoError(t, err)
	assert.Empty(t, result)
	assert.Empty(t, pagination.Next)
	assert.Empty(t, pagination.Prev)
}

// =============================================================================
// helper to build squirrel select for filter tests
// =============================================================================

func squirrelSelect(t *testing.T) squirrel.SelectBuilder {
	t.Helper()

	return squirrel.Select("*").From("transactions").PlaceholderFormat(squirrel.Dollar)
}

// =============================================================================
// Transaction model with metadata containing special values
// =============================================================================

func TestNewTransactionPostgreSQLModel_SpecialMetadata(t *testing.T) {
	t.Parallel()

	now := time.Now().UTC()
	entity := &shared.Transaction{
		ID:               uuid.New(),
		IngestionJobID:   uuid.New(),
		SourceID:         uuid.New(),
		ExternalID:       "ext-meta",
		Amount:           decimal.NewFromFloat(100.00),
		Currency:         "USD",
		ExtractionStatus: shared.ExtractionStatusComplete,
		Status:           shared.TransactionStatusUnmatched,
		Date:             now,
		Metadata:         map[string]any{"nested": map[string]any{"key": "value"}, "number": 42.5},
		CreatedAt:        now,
		UpdatedAt:        now,
	}

	model, err := NewTransactionPostgreSQLModel(entity)
	require.NoError(t, err)
	require.NotNil(t, model)
	assert.Contains(t, string(model.Metadata), `"nested"`)
	assert.Contains(t, string(model.Metadata), `"number"`)
}

// =============================================================================
// Transaction row helper with descriptions having nullable values
// =============================================================================

func createTransactionRowWithDesc(entity *shared.Transaction) []driver.Value {
	desc := sql.NullString{Valid: false}
	if entity.Description != "" {
		desc = sql.NullString{String: entity.Description, Valid: true}
	}

	return []driver.Value{
		entity.ID.String(),
		entity.IngestionJobID.String(),
		entity.SourceID.String(),
		entity.ExternalID,
		entity.Amount,
		entity.Currency,
		nil,
		nil,
		nil,
		nil,
		nil,
		entity.ExtractionStatus.String(),
		entity.Date,
		desc,
		entity.Status.String(),
		[]byte(`{}`),
		entity.CreatedAt,
		entity.UpdatedAt,
	}
}

func TestRepository_FindByID_WithDescription(t *testing.T) {
	t.Parallel()

	repo, mock, finish := setupRepositoryWithMock(t)
	defer finish()

	ctx := context.Background()
	entity := createValidTransactionEntity()
	entity.Description = "Test Description"

	mock.ExpectQuery("SELECT .* FROM transactions WHERE id").
		WithArgs(entity.ID.String()).
		WillReturnRows(sqlmock.NewRows(testTransactionColumns()).AddRow(createTransactionRowWithDesc(entity)...))

	result, err := repo.FindByID(ctx, entity.ID)
	require.NoError(t, err)
	require.NotNil(t, result)
	assert.Equal(t, "Test Description", result.Description)
}
