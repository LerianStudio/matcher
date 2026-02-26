//go:build unit

package adjustment

import (
	"context"
	"database/sql"
	"fmt"
	"regexp"
	"testing"
	"time"

	sqlmock "github.com/DATA-DOG/go-sqlmock"
	"github.com/google/uuid"
	"github.com/shopspring/decimal"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	pkgHTTP "github.com/LerianStudio/lib-uncommons/v2/uncommons/net/http"
	matchingEntities "github.com/LerianStudio/matcher/internal/matching/domain/entities"
	matchingRepositories "github.com/LerianStudio/matcher/internal/matching/domain/repositories"
	pgcommon "github.com/LerianStudio/matcher/internal/shared/adapters/postgres/common"
	"github.com/LerianStudio/matcher/internal/shared/constants"
	"github.com/LerianStudio/matcher/internal/shared/infrastructure/testutil"
)

func setupRepository(t *testing.T) (*Repository, sqlmock.Sqlmock, func()) {
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

func TestNewRepository(t *testing.T) {
	t.Parallel()

	provider := &testutil.MockInfrastructureProvider{}
	repo := NewRepository(provider)

	require.NotNil(t, repo)
	assert.NotNil(t, repo.provider)
}

func TestRepository_ImplementsInterface(t *testing.T) {
	t.Parallel()

	var _ matchingRepositories.AdjustmentRepository = (*Repository)(nil)
}

func TestRepository_Create_NilRepository(t *testing.T) {
	t.Parallel()

	var repo *Repository

	_, err := repo.Create(context.Background(), &matchingEntities.Adjustment{})
	require.ErrorIs(t, err, ErrRepoNotInitialized)
}

func TestRepository_Create_NilProvider(t *testing.T) {
	t.Parallel()

	repo := &Repository{provider: nil}
	_, err := repo.Create(context.Background(), &matchingEntities.Adjustment{})
	require.ErrorIs(t, err, ErrRepoNotInitialized)
}

func TestRepository_Create_NilAdjustment(t *testing.T) {
	t.Parallel()

	repo, _, finish := setupRepository(t)
	defer finish()

	_, err := repo.Create(context.Background(), nil)
	require.ErrorIs(t, err, ErrAdjustmentEntityNeeded)
}

func TestRepository_Create_Success(t *testing.T) {
	t.Parallel()

	repo, mock, finish := setupRepository(t)
	defer finish()

	ctx := context.Background()
	contextID := uuid.New()
	matchGroupID := uuid.New()
	now := time.Now().UTC()

	adjustment, err := matchingEntities.NewAdjustment(
		ctx,
		contextID,
		&matchGroupID,
		nil,
		matchingEntities.AdjustmentTypeBankFee,
		matchingEntities.AdjustmentDirectionDebit,
		decimal.NewFromFloat(10.50),
		"USD",
		"Bank fee adjustment",
		"Processing fee",
		"user@example.com",
	)
	require.NoError(t, err)

	insertQuery := regexp.QuoteMeta(
		`INSERT INTO adjustments (id, context_id, match_group_id, transaction_id, type, direction, amount, currency, description, reason, created_by, created_at, updated_at)
			 VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13)`,
	)

	selectQuery := regexp.QuoteMeta("SELECT " + columns + " FROM adjustments WHERE id=$1")

	rows := sqlmock.NewRows([]string{
		"id", "context_id", "match_group_id", "transaction_id", "type", "direction", "amount",
		"currency", "description", "reason", "created_by", "created_at", "updated_at",
	}).AddRow(
		adjustment.ID.String(),
		contextID.String(),
		sql.NullString{String: matchGroupID.String(), Valid: true},
		sql.NullString{},
		"BANK_FEE",
		"DEBIT",
		decimal.NewFromFloat(10.50),
		"USD",
		"Bank fee adjustment",
		"Processing fee",
		"user@example.com",
		now,
		now,
	)

	mock.ExpectBegin()
	mock.ExpectExec(insertQuery).
		WithArgs(
			sqlmock.AnyArg(),
			sqlmock.AnyArg(),
			sqlmock.AnyArg(),
			sqlmock.AnyArg(),
			sqlmock.AnyArg(),
			sqlmock.AnyArg(),
			sqlmock.AnyArg(),
			sqlmock.AnyArg(),
			sqlmock.AnyArg(),
			sqlmock.AnyArg(),
			sqlmock.AnyArg(),
			sqlmock.AnyArg(),
			sqlmock.AnyArg(),
		).
		WillReturnResult(sqlmock.NewResult(1, 1))
	mock.ExpectQuery(selectQuery).WithArgs(adjustment.ID.String()).WillReturnRows(rows)
	mock.ExpectCommit()

	result, err := repo.Create(ctx, adjustment)
	require.NoError(t, err)
	require.NotNil(t, result)
	assert.Equal(t, adjustment.ID, result.ID)
	assert.Equal(t, contextID, result.ContextID)
}

func TestRepository_Create_InsertError(t *testing.T) {
	t.Parallel()

	repo, mock, finish := setupRepository(t)
	defer finish()

	ctx := context.Background()
	contextID := uuid.New()
	matchGroupID := uuid.New()

	adjustment, err := matchingEntities.NewAdjustment(
		ctx,
		contextID,
		&matchGroupID,
		nil,
		matchingEntities.AdjustmentTypeBankFee,
		matchingEntities.AdjustmentDirectionDebit,
		decimal.NewFromFloat(10.50),
		"USD",
		"Bank fee adjustment",
		"Processing fee",
		"user@example.com",
	)
	require.NoError(t, err)

	insertQuery := regexp.QuoteMeta(
		`INSERT INTO adjustments (id, context_id, match_group_id, transaction_id, type, direction, amount, currency, description, reason, created_by, created_at, updated_at)
			 VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13)`,
	)

	mock.ExpectBegin()
	mock.ExpectExec(insertQuery).
		WithArgs(
			sqlmock.AnyArg(),
			sqlmock.AnyArg(),
			sqlmock.AnyArg(),
			sqlmock.AnyArg(),
			sqlmock.AnyArg(),
			sqlmock.AnyArg(),
			sqlmock.AnyArg(),
			sqlmock.AnyArg(),
			sqlmock.AnyArg(),
			sqlmock.AnyArg(),
			sqlmock.AnyArg(),
			sqlmock.AnyArg(),
			sqlmock.AnyArg(),
		).
		WillReturnError(sql.ErrConnDone)
	mock.ExpectRollback()

	_, err = repo.Create(ctx, adjustment)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "insert adjustment")
}

func TestRepository_FindByID_NilRepository(t *testing.T) {
	t.Parallel()

	var repo *Repository

	_, err := repo.FindByID(context.Background(), uuid.New(), uuid.New())
	require.ErrorIs(t, err, ErrRepoNotInitialized)
}

func TestRepository_FindByID_NilProvider(t *testing.T) {
	t.Parallel()

	repo := &Repository{provider: nil}
	_, err := repo.FindByID(context.Background(), uuid.New(), uuid.New())
	require.ErrorIs(t, err, ErrRepoNotInitialized)
}

func TestRepository_FindByID_Success(t *testing.T) {
	t.Parallel()

	repo, mock, finish := setupRepository(t)
	defer finish()

	ctx := context.Background()
	contextID := uuid.New()
	adjustmentID := uuid.New()
	matchGroupID := uuid.New()
	now := time.Now().UTC()

	selectQuery := regexp.QuoteMeta(
		"SELECT " + columns + " FROM adjustments WHERE context_id=$1 AND id=$2",
	)

	rows := sqlmock.NewRows([]string{
		"id", "context_id", "match_group_id", "transaction_id", "type", "direction", "amount",
		"currency", "description", "reason", "created_by", "created_at", "updated_at",
	}).AddRow(
		adjustmentID.String(),
		contextID.String(),
		sql.NullString{String: matchGroupID.String(), Valid: true},
		sql.NullString{},
		"BANK_FEE",
		"DEBIT",
		decimal.NewFromFloat(10.50),
		"USD",
		"Bank fee adjustment",
		"Processing fee",
		"user@example.com",
		now,
		now,
	)

	mock.ExpectQuery(selectQuery).
		WithArgs(contextID.String(), adjustmentID.String()).
		WillReturnRows(rows)

	result, err := repo.FindByID(ctx, contextID, adjustmentID)
	require.NoError(t, err)
	require.NotNil(t, result)
	assert.Equal(t, adjustmentID, result.ID)
	assert.Equal(t, contextID, result.ContextID)
	require.NotNil(t, result.MatchGroupID)
	assert.Equal(t, matchGroupID, *result.MatchGroupID)
}

func TestRepository_FindByID_NotFound(t *testing.T) {
	t.Parallel()

	repo, mock, finish := setupRepository(t)
	defer finish()

	ctx := context.Background()
	contextID := uuid.New()
	adjustmentID := uuid.New()

	selectQuery := regexp.QuoteMeta(
		"SELECT " + columns + " FROM adjustments WHERE context_id=$1 AND id=$2",
	)

	mock.ExpectQuery(selectQuery).
		WithArgs(contextID.String(), adjustmentID.String()).
		WillReturnError(sql.ErrNoRows)

	_, err := repo.FindByID(ctx, contextID, adjustmentID)
	require.Error(t, err)
}

func TestRepository_ListByContextID_NilRepository(t *testing.T) {
	t.Parallel()

	var repo *Repository

	_, _, err := repo.ListByContextID(
		context.Background(),
		uuid.New(),
		matchingRepositories.CursorFilter{},
	)
	require.ErrorIs(t, err, ErrRepoNotInitialized)
}

func TestRepository_ListByContextID_NilProvider(t *testing.T) {
	t.Parallel()

	repo := &Repository{provider: nil}
	_, _, err := repo.ListByContextID(
		context.Background(),
		uuid.New(),
		matchingRepositories.CursorFilter{},
	)
	require.ErrorIs(t, err, ErrRepoNotInitialized)
}

func TestRepository_ListByContextID_Success(t *testing.T) {
	t.Parallel()

	repo, mock, finish := setupRepository(t)
	defer finish()

	ctx := context.Background()
	contextID := uuid.New()
	adjustmentID1 := uuid.New()
	adjustmentID2 := uuid.New()
	matchGroupID := uuid.New()
	now := time.Now().UTC()

	rows := sqlmock.NewRows([]string{
		"id", "context_id", "match_group_id", "transaction_id", "type", "direction", "amount",
		"currency", "description", "reason", "created_by", "created_at", "updated_at",
	}).AddRow(
		adjustmentID1.String(),
		contextID.String(),
		sql.NullString{String: matchGroupID.String(), Valid: true},
		sql.NullString{},
		"BANK_FEE",
		"DEBIT",
		decimal.NewFromFloat(10.50),
		"USD",
		"Bank fee adjustment",
		"Processing fee",
		"user@example.com",
		now,
		now,
	).AddRow(
		adjustmentID2.String(),
		contextID.String(),
		sql.NullString{String: matchGroupID.String(), Valid: true},
		sql.NullString{},
		"ROUNDING",
		"DEBIT",
		decimal.NewFromFloat(0.01),
		"USD",
		"Rounding adjustment",
		"Sub-cent rounding",
		"system",
		now,
		now,
	)

	mock.ExpectQuery("SELECT").WithArgs(contextID.String()).WillReturnRows(rows)

	results, pagination, err := repo.ListByContextID(
		ctx,
		contextID,
		matchingRepositories.CursorFilter{Limit: 10},
	)
	require.NoError(t, err)
	assert.Len(t, results, 2)
	assert.NotNil(t, pagination)
}

func TestRepository_ListByContextID_Empty(t *testing.T) {
	t.Parallel()

	repo, mock, finish := setupRepository(t)
	defer finish()

	ctx := context.Background()
	contextID := uuid.New()

	rows := sqlmock.NewRows([]string{
		"id", "context_id", "match_group_id", "transaction_id", "type", "direction", "amount",
		"currency", "description", "reason", "created_by", "created_at", "updated_at",
	})

	mock.ExpectQuery("SELECT").WithArgs(contextID.String()).WillReturnRows(rows)

	results, _, err := repo.ListByContextID(
		ctx,
		contextID,
		matchingRepositories.CursorFilter{Limit: 10},
	)
	require.NoError(t, err)
	assert.Empty(t, results)
}

func TestRepository_ListByContextID_LimitCappedAtMaximum(t *testing.T) {
	t.Parallel()

	repo, mock, finish := setupRepository(t)
	defer finish()

	ctx := context.Background()
	contextID := uuid.New()

	rows := sqlmock.NewRows([]string{
		"id", "context_id", "match_group_id", "transaction_id", "type", "direction", "amount",
		"currency", "description", "reason", "created_by", "created_at", "updated_at",
	})

	mock.ExpectQuery(fmt.Sprintf("LIMIT %d", constants.MaximumPaginationLimit+1)).
		WithArgs(contextID.String()).
		WillReturnRows(rows)

	results, pagination, err := repo.ListByContextID(
		ctx,
		contextID,
		matchingRepositories.CursorFilter{Limit: constants.MaximumPaginationLimit + 1},
	)
	require.NoError(t, err)
	assert.Empty(t, results)
	assert.Empty(t, pagination.Next)
	assert.Empty(t, pagination.Prev)
}

func TestRepository_ListByContextID_ReturnsRowsThroughBoundaryValidation(t *testing.T) {
	t.Parallel()

	repo, mock, finish := setupRepository(t)
	defer finish()

	ctx := context.Background()
	contextID := uuid.New()
	adjID := uuid.New()
	now := time.Now().UTC()

	rows := sqlmock.NewRows([]string{
		"id", "context_id", "match_group_id", "transaction_id", "type", "direction", "amount",
		"currency", "description", "reason", "created_by", "created_at", "updated_at",
	}).AddRow(
		adjID.String(), contextID.String(), nil, nil, "BANK_FEE", "DEBIT",
		decimal.NewFromFloat(10.50), "USD", "Test adjustment", "Test reason", "system",
		now, now,
	)

	mock.ExpectQuery("SELECT").
		WithArgs(contextID.String()).
		WillReturnRows(rows)

	results, pagination, err := repo.ListByContextID(
		ctx,
		contextID,
		matchingRepositories.CursorFilter{Limit: 10},
	)

	require.NoError(t, err)
	require.Len(t, results, 1)
	assert.Equal(t, adjID, results[0].ID)
	assert.Equal(t, contextID, results[0].ContextID)
	assert.Equal(t, matchingEntities.AdjustmentTypeBankFee, results[0].Type)
	assert.Equal(t, matchingEntities.AdjustmentDirectionDebit, results[0].Direction)
	assert.Empty(t, pagination.Prev, "first page should have no prev cursor")
}

func TestRepository_ListByContextID_InvalidSortCursor(t *testing.T) {
	t.Parallel()

	repo, _, finish := setupRepository(t)
	defer finish()

	ctx := context.Background()
	contextID := uuid.New()

	_, _, err := repo.ListByContextID(ctx, contextID, matchingRepositories.CursorFilter{
		Limit:  10,
		Cursor: "some-cursor",
		SortBy: "created_at",
	})
	require.Error(t, err)
	require.ErrorIs(t, err, pkgHTTP.ErrInvalidCursor)
}

func TestCalculateAdjustmentSortPagination_PropagatesCalculatorError(t *testing.T) {
	t.Parallel()

	_, err := calculateAdjustmentSortPagination(
		true,
		true,
		true,
		sortColumnCreatedAt,
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
	assert.Contains(t, err.Error(), "calculate adjustment cursor pagination")
}

func TestCalculateAdjustmentSortPagination_NilCalculator(t *testing.T) {
	t.Parallel()

	_, err := calculateAdjustmentSortPagination(
		true,
		true,
		true,
		sortColumnCreatedAt,
		time.Now().UTC().Format(time.RFC3339Nano),
		uuid.New().String(),
		time.Now().UTC().Add(time.Minute).Format(time.RFC3339Nano),
		uuid.New().String(),
		nil,
	)

	require.Error(t, err)
	assert.ErrorIs(t, err, pgcommon.ErrSortCursorCalculatorRequired)
}

func TestRepository_ListByMatchGroupID_NilRepository(t *testing.T) {
	t.Parallel()

	var repo *Repository

	_, err := repo.ListByMatchGroupID(context.Background(), uuid.New(), uuid.New())
	require.ErrorIs(t, err, ErrRepoNotInitialized)
}

func TestRepository_ListByMatchGroupID_NilProvider(t *testing.T) {
	t.Parallel()

	repo := &Repository{provider: nil}
	_, err := repo.ListByMatchGroupID(context.Background(), uuid.New(), uuid.New())
	require.ErrorIs(t, err, ErrRepoNotInitialized)
}

func TestRepository_ListByMatchGroupID_Success(t *testing.T) {
	t.Parallel()

	repo, mock, finish := setupRepository(t)
	defer finish()

	ctx := context.Background()
	contextID := uuid.New()
	matchGroupID := uuid.New()
	adjustmentID1 := uuid.New()
	adjustmentID2 := uuid.New()
	now := time.Now().UTC()

	selectQuery := regexp.QuoteMeta(
		"SELECT " + columns + " FROM adjustments WHERE context_id=$1 AND match_group_id=$2 ORDER BY created_at ASC",
	)

	rows := sqlmock.NewRows([]string{
		"id", "context_id", "match_group_id", "transaction_id", "type", "direction", "amount",
		"currency", "description", "reason", "created_by", "created_at", "updated_at",
	}).AddRow(
		adjustmentID1.String(),
		contextID.String(),
		sql.NullString{String: matchGroupID.String(), Valid: true},
		sql.NullString{},
		"BANK_FEE",
		"DEBIT",
		decimal.NewFromFloat(10.50),
		"USD",
		"Bank fee adjustment",
		"Processing fee",
		"user@example.com",
		now,
		now,
	).AddRow(
		adjustmentID2.String(),
		contextID.String(),
		sql.NullString{String: matchGroupID.String(), Valid: true},
		sql.NullString{},
		"ROUNDING",
		"DEBIT",
		decimal.NewFromFloat(0.01),
		"USD",
		"Rounding adjustment",
		"Sub-cent rounding",
		"system",
		now.Add(time.Second),
		now.Add(time.Second),
	)

	mock.ExpectQuery(selectQuery).
		WithArgs(contextID.String(), matchGroupID.String()).
		WillReturnRows(rows)

	results, err := repo.ListByMatchGroupID(ctx, contextID, matchGroupID)
	require.NoError(t, err)
	assert.Len(t, results, 2)
	assert.Equal(t, adjustmentID1, results[0].ID)
	assert.Equal(t, adjustmentID2, results[1].ID)
}

func TestRepository_ListByMatchGroupID_Empty(t *testing.T) {
	t.Parallel()

	repo, mock, finish := setupRepository(t)
	defer finish()

	ctx := context.Background()
	contextID := uuid.New()
	matchGroupID := uuid.New()

	selectQuery := regexp.QuoteMeta(
		"SELECT " + columns + " FROM adjustments WHERE context_id=$1 AND match_group_id=$2 ORDER BY created_at ASC",
	)

	rows := sqlmock.NewRows([]string{
		"id", "context_id", "match_group_id", "transaction_id", "type", "direction", "amount",
		"currency", "description", "reason", "created_by", "created_at", "updated_at",
	})

	mock.ExpectQuery(selectQuery).
		WithArgs(contextID.String(), matchGroupID.String()).
		WillReturnRows(rows)

	results, err := repo.ListByMatchGroupID(ctx, contextID, matchGroupID)
	require.NoError(t, err)
	assert.Empty(t, results)
}

func TestRepository_ListByMatchGroupID_QueryError(t *testing.T) {
	t.Parallel()

	repo, mock, finish := setupRepository(t)
	defer finish()

	ctx := context.Background()
	contextID := uuid.New()
	matchGroupID := uuid.New()

	selectQuery := regexp.QuoteMeta(
		"SELECT " + columns + " FROM adjustments WHERE context_id=$1 AND match_group_id=$2 ORDER BY created_at ASC",
	)

	mock.ExpectQuery(selectQuery).
		WithArgs(contextID.String(), matchGroupID.String()).
		WillReturnError(sql.ErrConnDone)

	_, err := repo.ListByMatchGroupID(ctx, contextID, matchGroupID)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "query adjustments")
}

func TestNormalizeSortColumn(t *testing.T) {
	t.Parallel()

	tests := []struct {
		input    string
		expected string
	}{
		{"id", "id"},
		{"ID", "id"},
		{"created_at", "created_at"},
		{"CREATED_AT", "created_at"},
		{"type", "type"},
		{"TYPE", "type"},
		{"unknown", "id"},
		{"", "id"},
		{"invalid_column", "id"},
	}

	for _, tt := range tests {
		t.Run(tt.input, func(t *testing.T) {
			t.Parallel()
			assert.Equal(t, tt.expected, normalizeSortColumn(tt.input))
		})
	}
}
