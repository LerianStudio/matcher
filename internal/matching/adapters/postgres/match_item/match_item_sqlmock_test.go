//go:build unit

package match_item

import (
	"context"
	"errors"
	"regexp"
	"testing"
	"time"

	sqlmock "github.com/DATA-DOG/go-sqlmock"
	"github.com/google/uuid"
	"github.com/shopspring/decimal"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	matchingEntities "github.com/LerianStudio/matcher/internal/matching/domain/entities"
	matchingRepos "github.com/LerianStudio/matcher/internal/matching/domain/repositories"
	"github.com/LerianStudio/matcher/internal/shared/infrastructure/testutil"
)

// Compile-time interface compliance assertion.
var _ matchingRepos.MatchItemRepository = (*Repository)(nil)

var (
	errTestQuery      = errors.New("query error")
	errTestPrepare    = errors.New("prepare error")
	errTestExec       = errors.New("exec error")
	errTestConnection = errors.New("connection refused")
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

func createTestMatchItem(t *testing.T) *matchingEntities.MatchItem {
	t.Helper()

	item, err := matchingEntities.NewMatchItem(
		context.Background(),
		uuid.New(),
		decimal.NewFromInt(100),
		"USD",
		decimal.NewFromInt(100),
	)
	require.NoError(t, err)

	item.MatchGroupID = uuid.New()

	return item
}

// TestNewRepository tests the NewRepository constructor.
func TestNewRepository(t *testing.T) {
	t.Parallel()

	t.Run("creates repository with valid provider", func(t *testing.T) {
		t.Parallel()

		provider := &testutil.MockInfrastructureProvider{}
		repo := NewRepository(provider)

		require.NotNil(t, repo)
		assert.Equal(t, provider, repo.provider)
	})

	t.Run("creates repository with nil provider", func(t *testing.T) {
		t.Parallel()

		repo := NewRepository(nil)

		require.NotNil(t, repo)
		assert.Nil(t, repo.provider)
	})
}

// TestCreateBatch_NilRepository tests CreateBatch with nil repository.
func TestCreateBatch_NilRepository(t *testing.T) {
	t.Parallel()

	var repo *Repository

	items, err := repo.CreateBatch(context.Background(), []*matchingEntities.MatchItem{})

	assert.Nil(t, items)
	require.ErrorIs(t, err, ErrRepoNotInitialized)
}

// TestCreateBatch_NilProvider tests CreateBatch with nil provider.
func TestCreateBatch_NilProvider(t *testing.T) {
	t.Parallel()

	repo := &Repository{provider: nil}

	items, err := repo.CreateBatch(context.Background(), []*matchingEntities.MatchItem{})

	assert.Nil(t, items)
	require.ErrorIs(t, err, ErrRepoNotInitialized)
}

// TestCreateBatch_EmptyItems tests CreateBatch with empty slice.
func TestCreateBatch_EmptyItems(t *testing.T) {
	t.Parallel()

	repo, _, finish := setupMockRepository(t)
	defer finish()

	items, err := repo.CreateBatch(context.Background(), []*matchingEntities.MatchItem{})

	assert.Nil(t, items)
	require.NoError(t, err)
}

// TestCreateBatch_NilItems tests CreateBatch with nil slice.
func TestCreateBatch_NilItems(t *testing.T) {
	t.Parallel()

	repo, _, finish := setupMockRepository(t)
	defer finish()

	items, err := repo.CreateBatch(context.Background(), nil)

	assert.Nil(t, items)
	require.NoError(t, err)
}

// TestCreateBatch_PrepareError tests CreateBatch when prepare fails.
func TestCreateBatch_PrepareError(t *testing.T) {
	t.Parallel()

	repo, mock, finish := setupMockRepository(t)
	defer finish()

	item := createTestMatchItem(t)

	mock.ExpectBegin()
	mock.ExpectPrepare(regexp.QuoteMeta("INSERT INTO match_items")).
		WillReturnError(errTestPrepare)
	mock.ExpectRollback()

	items, err := repo.CreateBatch(context.Background(), []*matchingEntities.MatchItem{item})

	assert.Nil(t, items)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "prepare insert match item")
}

// TestCreateBatch_ExecError tests CreateBatch when exec fails.
func TestCreateBatch_ExecError(t *testing.T) {
	t.Parallel()

	repo, mock, finish := setupMockRepository(t)
	defer finish()

	item := createTestMatchItem(t)

	mock.ExpectBegin()
	mock.ExpectPrepare(regexp.QuoteMeta("INSERT INTO match_items"))
	mock.ExpectExec(regexp.QuoteMeta("INSERT INTO match_items")).
		WithArgs(
			item.ID.String(),
			item.MatchGroupID.String(),
			item.TransactionID.String(),
			item.AllocatedAmount,
			item.AllocatedCurrency,
			item.ExpectedAmount,
			item.AllowPartial,
			sqlmock.AnyArg(),
			sqlmock.AnyArg(),
		).
		WillReturnError(errTestExec)
	mock.ExpectRollback()

	items, err := repo.CreateBatch(context.Background(), []*matchingEntities.MatchItem{item})

	assert.Nil(t, items)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "insert match item")
}

// TestCreateBatch_Success tests CreateBatch with valid items.
func TestCreateBatch_Success(t *testing.T) {
	t.Parallel()

	repo, mock, finish := setupMockRepository(t)
	defer finish()

	item1 := createTestMatchItem(t)
	item2 := createTestMatchItem(t)

	mock.ExpectBegin()
	mock.ExpectPrepare(regexp.QuoteMeta("INSERT INTO match_items"))

	mock.ExpectExec(regexp.QuoteMeta("INSERT INTO match_items")).
		WithArgs(
			item1.ID.String(),
			item1.MatchGroupID.String(),
			item1.TransactionID.String(),
			item1.AllocatedAmount,
			item1.AllocatedCurrency,
			item1.ExpectedAmount,
			item1.AllowPartial,
			sqlmock.AnyArg(),
			sqlmock.AnyArg(),
		).
		WillReturnResult(sqlmock.NewResult(1, 1))

	mock.ExpectExec(regexp.QuoteMeta("INSERT INTO match_items")).
		WithArgs(
			item2.ID.String(),
			item2.MatchGroupID.String(),
			item2.TransactionID.String(),
			item2.AllocatedAmount,
			item2.AllocatedCurrency,
			item2.ExpectedAmount,
			item2.AllowPartial,
			sqlmock.AnyArg(),
			sqlmock.AnyArg(),
		).
		WillReturnResult(sqlmock.NewResult(2, 1))

	mock.ExpectCommit()

	items, err := repo.CreateBatch(
		context.Background(),
		[]*matchingEntities.MatchItem{item1, item2},
	)

	require.NoError(t, err)
	require.Len(t, items, 2)
	assert.Equal(t, item1.ID, items[0].ID)
	assert.Equal(t, item2.ID, items[1].ID)
}

// TestCreateBatchWithTx_NilTx tests CreateBatchWithTx with nil tx.
func TestCreateBatchWithTx_NilTx(t *testing.T) {
	t.Parallel()

	repo, _, finish := setupMockRepository(t)
	defer finish()

	item := createTestMatchItem(t)

	items, err := repo.CreateBatchWithTx(
		context.Background(),
		nil,
		[]*matchingEntities.MatchItem{item},
	)

	assert.Nil(t, items)
	require.ErrorIs(t, err, ErrInvalidTx)
}

// TestCreateBatchWithTx_NilTx_WithMockProvider tests CreateBatchWithTx with nil tx using a mock provider.
func TestCreateBatchWithTx_NilTx_WithMockProvider(t *testing.T) {
	t.Parallel()

	provider := &testutil.MockInfrastructureProvider{}
	repo := NewRepository(provider)
	item := createTestMatchItem(t)

	items, err := repo.CreateBatchWithTx(
		context.Background(),
		nil,
		[]*matchingEntities.MatchItem{item},
	)

	assert.Nil(t, items)
	require.ErrorIs(t, err, ErrInvalidTx)
}

// TestCreateBatchWithTx_NilRepository tests CreateBatchWithTx with nil repository.
func TestCreateBatchWithTx_NilRepository(t *testing.T) {
	t.Parallel()

	var repo *Repository

	items, err := repo.CreateBatchWithTx(context.Background(), nil, []*matchingEntities.MatchItem{})

	assert.Nil(t, items)
	require.ErrorIs(t, err, ErrRepoNotInitialized)
}

// TestCreateBatchWithTx_NilProvider tests CreateBatchWithTx with nil provider.
func TestCreateBatchWithTx_NilProvider(t *testing.T) {
	t.Parallel()

	repo := &Repository{provider: nil}

	items, err := repo.CreateBatchWithTx(context.Background(), nil, []*matchingEntities.MatchItem{})

	assert.Nil(t, items)
	require.ErrorIs(t, err, ErrRepoNotInitialized)
}

// TestCreateBatch_NilItemInSlice tests CreateBatch with a nil item in the slice.
func TestCreateBatch_NilItemInSlice(t *testing.T) {
	t.Parallel()

	repo, mock, finish := setupMockRepository(t)
	defer finish()

	mock.ExpectBegin()
	mock.ExpectPrepare(regexp.QuoteMeta("INSERT INTO match_items"))
	mock.ExpectRollback()

	items, err := repo.CreateBatch(context.Background(), []*matchingEntities.MatchItem{nil})

	assert.Nil(t, items)
	require.Error(t, err)
	assert.ErrorIs(t, err, ErrMatchItemEntityNeeded)
}

// TestCreateBatch_TransactionBeginError tests CreateBatch when begin tx fails.
func TestCreateBatch_TransactionBeginError(t *testing.T) {
	t.Parallel()

	provider := &testutil.MockInfrastructureProvider{
		TxErr: errTestConnection,
	}
	repo := NewRepository(provider)

	item := createTestMatchItem(t)

	items, err := repo.CreateBatch(context.Background(), []*matchingEntities.MatchItem{item})

	assert.Nil(t, items)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "create match item batch transaction")
}

// TestListByMatchGroupID_NilRepository tests ListByMatchGroupID with nil repository.
func TestListByMatchGroupID_NilRepository(t *testing.T) {
	t.Parallel()

	var repo *Repository

	items, err := repo.ListByMatchGroupID(context.Background(), uuid.New())

	assert.Nil(t, items)
	require.ErrorIs(t, err, ErrRepoNotInitialized)
}

// TestListByMatchGroupID_NilProvider tests ListByMatchGroupID with nil provider.
func TestListByMatchGroupID_NilProvider(t *testing.T) {
	t.Parallel()

	repo := &Repository{provider: nil}

	items, err := repo.ListByMatchGroupID(context.Background(), uuid.New())

	assert.Nil(t, items)
	require.ErrorIs(t, err, ErrRepoNotInitialized)
}

// TestListByMatchGroupID_QueryError tests ListByMatchGroupID when query fails.
func TestListByMatchGroupID_QueryError(t *testing.T) {
	t.Parallel()

	db, mock, err := sqlmock.New()
	require.NoError(t, err)

	defer db.Close()

	provider := testutil.NewMockProviderFromDB(t, db)
	repo := NewRepository(provider)

	groupID := uuid.New()

	mock.ExpectQuery(regexp.QuoteMeta("SELECT")).
		WithArgs(groupID.String()).
		WillReturnError(errTestQuery)

	items, err := repo.ListByMatchGroupID(context.Background(), groupID)

	assert.Nil(t, items)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "query match items")
	require.NoError(t, mock.ExpectationsWereMet())
}

// TestListByMatchGroupID_ScanError tests ListByMatchGroupID when scan fails.
func TestListByMatchGroupID_ScanError(t *testing.T) {
	t.Parallel()

	db, mock, err := sqlmock.New()
	require.NoError(t, err)

	defer db.Close()

	provider := testutil.NewMockProviderFromDB(t, db)
	repo := NewRepository(provider)

	groupID := uuid.New()

	rows := sqlmock.NewRows([]string{
		"id", "match_group_id", "transaction_id", "allocated_amount",
		"allocated_currency", "expected_amount", "allow_partial", "created_at", "updated_at",
	}).AddRow(
		"invalid-uuid", // This will cause scan/parse error
		groupID.String(),
		uuid.New().String(),
		decimal.NewFromInt(100),
		"USD",
		decimal.NewFromInt(100),
		false,
		time.Now().UTC(),
		time.Now().UTC(),
	)

	mock.ExpectQuery(regexp.QuoteMeta("SELECT")).
		WithArgs(groupID.String()).
		WillReturnRows(rows)

	items, err := repo.ListByMatchGroupID(context.Background(), groupID)

	assert.Nil(t, items)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "invalid UUID")
	require.NoError(t, mock.ExpectationsWereMet())
}

// TestListByMatchGroupID_EmptyResult tests ListByMatchGroupID with no results.
func TestListByMatchGroupID_EmptyResult(t *testing.T) {
	t.Parallel()

	db, mock, err := sqlmock.New()
	require.NoError(t, err)

	defer db.Close()

	provider := testutil.NewMockProviderFromDB(t, db)
	repo := NewRepository(provider)

	groupID := uuid.New()

	rows := sqlmock.NewRows([]string{
		"id", "match_group_id", "transaction_id", "allocated_amount",
		"allocated_currency", "expected_amount", "allow_partial", "created_at", "updated_at",
	})

	mock.ExpectQuery(regexp.QuoteMeta("SELECT")).
		WithArgs(groupID.String()).
		WillReturnRows(rows)

	items, err := repo.ListByMatchGroupID(context.Background(), groupID)

	require.NoError(t, err)
	require.NotNil(t, items)
	assert.Empty(t, items)
	require.NoError(t, mock.ExpectationsWereMet())
}

// TestListByMatchGroupID_Success tests ListByMatchGroupID with valid results.
func TestListByMatchGroupID_Success(t *testing.T) {
	t.Parallel()

	db, mock, err := sqlmock.New()
	require.NoError(t, err)

	defer db.Close()

	provider := testutil.NewMockProviderFromDB(t, db)
	repo := NewRepository(provider)

	groupID := uuid.New()
	itemID1 := uuid.New()
	itemID2 := uuid.New()
	txID1 := uuid.New()
	txID2 := uuid.New()
	now := time.Now().UTC()

	rows := sqlmock.NewRows([]string{
		"id", "match_group_id", "transaction_id", "allocated_amount",
		"allocated_currency", "expected_amount", "allow_partial", "created_at", "updated_at",
	}).
		AddRow(itemID1.String(), groupID.String(), txID1.String(), decimal.NewFromInt(100), "USD", decimal.NewFromInt(100), false, now, now).
		AddRow(itemID2.String(), groupID.String(), txID2.String(), decimal.NewFromInt(200), "EUR", decimal.NewFromInt(200), true, now, now)

	mock.ExpectQuery(regexp.QuoteMeta("SELECT")).
		WithArgs(groupID.String()).
		WillReturnRows(rows)

	items, err := repo.ListByMatchGroupID(context.Background(), groupID)

	require.NoError(t, err)
	require.Len(t, items, 2)
	assert.Equal(t, itemID1, items[0].ID)
	assert.Equal(t, groupID, items[0].MatchGroupID)
	assert.Equal(t, txID1, items[0].TransactionID)
	assert.Equal(t, "USD", items[0].AllocatedCurrency)
	assert.False(t, items[0].AllowPartial)

	assert.Equal(t, itemID2, items[1].ID)
	assert.Equal(t, "EUR", items[1].AllocatedCurrency)
	assert.True(t, items[1].AllowPartial)
	require.NoError(t, mock.ExpectationsWereMet())
}

// TestListByMatchGroupID_RowsError tests ListByMatchGroupID when rows.Err() returns error.
func TestListByMatchGroupID_RowsError(t *testing.T) {
	t.Parallel()

	db, mock, err := sqlmock.New()
	require.NoError(t, err)

	defer db.Close()

	provider := testutil.NewMockProviderFromDB(t, db)
	repo := NewRepository(provider)

	groupID := uuid.New()
	now := time.Now().UTC()

	errRowIteration := errors.New("row iteration error")

	rows := sqlmock.NewRows([]string{
		"id", "match_group_id", "transaction_id", "allocated_amount",
		"allocated_currency", "expected_amount", "allow_partial", "created_at", "updated_at",
	}).
		AddRow(uuid.New().String(), groupID.String(), uuid.New().String(), decimal.NewFromInt(100), "USD", decimal.NewFromInt(100), false, now, now).
		RowError(0, errRowIteration)

	mock.ExpectQuery(regexp.QuoteMeta("SELECT")).
		WithArgs(groupID.String()).
		WillReturnRows(rows)

	items, err := repo.ListByMatchGroupID(context.Background(), groupID)

	assert.Nil(t, items)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "iterate match item rows")
	require.NoError(t, mock.ExpectationsWereMet())
}

// TestRepository_ImplementsInterface tests that Repository implements MatchItemRepository.
func TestRepository_ImplementsInterface(t *testing.T) {
	t.Parallel()

	provider := &testutil.MockInfrastructureProvider{}
	repo := NewRepository(provider)

	require.NotNil(t, repo)
}

// TestCreateBatch_CommitError tests CreateBatch when commit fails.
func TestCreateBatch_CommitError(t *testing.T) {
	t.Parallel()

	repo, mock, finish := setupMockRepository(t)
	defer finish()

	item := createTestMatchItem(t)

	errCommit := errors.New("commit failed")

	mock.ExpectBegin()
	mock.ExpectPrepare(regexp.QuoteMeta("INSERT INTO match_items"))
	mock.ExpectExec(regexp.QuoteMeta("INSERT INTO match_items")).
		WithArgs(
			item.ID.String(),
			item.MatchGroupID.String(),
			item.TransactionID.String(),
			item.AllocatedAmount,
			item.AllocatedCurrency,
			item.ExpectedAmount,
			item.AllowPartial,
			sqlmock.AnyArg(),
			sqlmock.AnyArg(),
		).
		WillReturnResult(sqlmock.NewResult(1, 1))
	mock.ExpectCommit().WillReturnError(errCommit)

	items, err := repo.CreateBatch(context.Background(), []*matchingEntities.MatchItem{item})

	assert.Nil(t, items)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "create match item batch transaction")
}

// TestCreateBatchWithTx_WithValidTx tests CreateBatchWithTx with a valid *sql.Tx.
func TestCreateBatchWithTx_WithValidTx(t *testing.T) {
	t.Parallel()

	db, mock, err := sqlmock.New()
	require.NoError(t, err)

	defer db.Close()

	provider := testutil.NewMockProviderFromDB(t, db)
	repo := NewRepository(provider)

	item := createTestMatchItem(t)

	mock.ExpectBegin()
	mock.ExpectPrepare(regexp.QuoteMeta("INSERT INTO match_items"))
	mock.ExpectExec(regexp.QuoteMeta("INSERT INTO match_items")).
		WithArgs(
			item.ID.String(),
			item.MatchGroupID.String(),
			item.TransactionID.String(),
			item.AllocatedAmount,
			item.AllocatedCurrency,
			item.ExpectedAmount,
			item.AllowPartial,
			sqlmock.AnyArg(),
			sqlmock.AnyArg(),
		).
		WillReturnResult(sqlmock.NewResult(1, 1))

	tx, err := db.BeginTx(context.Background(), nil)
	require.NoError(t, err)

	items, err := repo.CreateBatchWithTx(
		context.Background(),
		tx,
		[]*matchingEntities.MatchItem{item},
	)

	require.NoError(t, err)
	require.Len(t, items, 1)
	require.NoError(t, mock.ExpectationsWereMet())
}

// TestCreateBatchWithTx_EmptyItems tests CreateBatchWithTx with empty slice.
func TestCreateBatchWithTx_EmptyItems(t *testing.T) {
	t.Parallel()

	provider := &testutil.MockInfrastructureProvider{}
	repo := NewRepository(provider)

	items, err := repo.CreateBatchWithTx(context.Background(), nil, []*matchingEntities.MatchItem{})

	assert.Nil(t, items)
	require.ErrorIs(t, err, ErrInvalidTx)
}

// TestListByMatchGroupID_InvalidMatchGroupIDParse tests ListByMatchGroupID when match_group_id parse fails.
func TestListByMatchGroupID_InvalidMatchGroupIDParse(t *testing.T) {
	t.Parallel()

	db, mock, err := sqlmock.New()
	require.NoError(t, err)

	defer db.Close()

	provider := testutil.NewMockProviderFromDB(t, db)
	repo := NewRepository(provider)

	groupID := uuid.New()
	now := time.Now().UTC()

	rows := sqlmock.NewRows([]string{
		"id", "match_group_id", "transaction_id", "allocated_amount",
		"allocated_currency", "expected_amount", "allow_partial", "created_at", "updated_at",
	}).AddRow(
		uuid.New().String(),
		"invalid-uuid",
		uuid.New().String(),
		decimal.NewFromInt(100),
		"USD",
		decimal.NewFromInt(100),
		false,
		now,
		now,
	)

	mock.ExpectQuery(regexp.QuoteMeta("SELECT")).
		WithArgs(groupID.String()).
		WillReturnRows(rows)

	items, err := repo.ListByMatchGroupID(context.Background(), groupID)

	assert.Nil(t, items)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "invalid UUID")
	require.NoError(t, mock.ExpectationsWereMet())
}

// TestListByMatchGroupIDs_Success tests ListByMatchGroupIDs with multiple group IDs returning correctly grouped items.
func TestListByMatchGroupIDs_Success(t *testing.T) {
	t.Parallel()

	db, mock, err := sqlmock.New()
	require.NoError(t, err)

	defer db.Close()

	provider := testutil.NewMockProviderFromDB(t, db)
	repo := NewRepository(provider)

	groupID1 := uuid.New()
	groupID2 := uuid.New()
	itemID1 := uuid.New()
	itemID2 := uuid.New()
	itemID3 := uuid.New()
	txID1 := uuid.New()
	txID2 := uuid.New()
	txID3 := uuid.New()
	now := time.Now().UTC()

	rows := sqlmock.NewRows([]string{
		"id", "match_group_id", "transaction_id", "allocated_amount",
		"allocated_currency", "expected_amount", "allow_partial", "created_at", "updated_at",
	}).
		AddRow(itemID1.String(), groupID1.String(), txID1.String(), decimal.NewFromInt(100), "USD", decimal.NewFromInt(100), false, now, now).
		AddRow(itemID2.String(), groupID1.String(), txID2.String(), decimal.NewFromInt(200), "USD", decimal.NewFromInt(200), false, now, now).
		AddRow(itemID3.String(), groupID2.String(), txID3.String(), decimal.NewFromInt(300), "EUR", decimal.NewFromInt(300), true, now, now)

	mock.ExpectQuery(regexp.QuoteMeta("SELECT")).
		WithArgs(groupID1.String(), groupID2.String()).
		WillReturnRows(rows)

	result, err := repo.ListByMatchGroupIDs(context.Background(), []uuid.UUID{groupID1, groupID2})

	require.NoError(t, err)
	require.NotNil(t, result)
	require.Len(t, result, 2)

	// Group 1 should have 2 items.
	require.Len(t, result[groupID1], 2)
	assert.Equal(t, itemID1, result[groupID1][0].ID)
	assert.Equal(t, groupID1, result[groupID1][0].MatchGroupID)
	assert.Equal(t, txID1, result[groupID1][0].TransactionID)
	assert.Equal(t, "USD", result[groupID1][0].AllocatedCurrency)
	assert.False(t, result[groupID1][0].AllowPartial)

	assert.Equal(t, itemID2, result[groupID1][1].ID)
	assert.Equal(t, txID2, result[groupID1][1].TransactionID)

	// Group 2 should have 1 item.
	require.Len(t, result[groupID2], 1)
	assert.Equal(t, itemID3, result[groupID2][0].ID)
	assert.Equal(t, groupID2, result[groupID2][0].MatchGroupID)
	assert.Equal(t, "EUR", result[groupID2][0].AllocatedCurrency)
	assert.True(t, result[groupID2][0].AllowPartial)

	require.NoError(t, mock.ExpectationsWereMet())
}

// TestListByMatchGroupIDs_QueryError tests ListByMatchGroupIDs when the SQL query fails.
func TestListByMatchGroupIDs_QueryError(t *testing.T) {
	t.Parallel()

	db, mock, err := sqlmock.New()
	require.NoError(t, err)

	defer db.Close()

	provider := testutil.NewMockProviderFromDB(t, db)
	repo := NewRepository(provider)

	groupID := uuid.New()

	mock.ExpectQuery(regexp.QuoteMeta("SELECT")).
		WithArgs(groupID.String()).
		WillReturnError(errTestQuery)

	result, err := repo.ListByMatchGroupIDs(context.Background(), []uuid.UUID{groupID})

	assert.Nil(t, result)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "query match items by group ids")
	require.NoError(t, mock.ExpectationsWereMet())
}

// TestListByMatchGroupIDs_ScanError tests ListByMatchGroupIDs when row scanning fails due to invalid data.
func TestListByMatchGroupIDs_ScanError(t *testing.T) {
	t.Parallel()

	db, mock, err := sqlmock.New()
	require.NoError(t, err)

	defer db.Close()

	provider := testutil.NewMockProviderFromDB(t, db)
	repo := NewRepository(provider)

	groupID := uuid.New()

	rows := sqlmock.NewRows([]string{
		"id", "match_group_id", "transaction_id", "allocated_amount",
		"allocated_currency", "expected_amount", "allow_partial", "created_at", "updated_at",
	}).AddRow(
		"invalid-uuid", // Will cause parse error in ToEntity.
		groupID.String(),
		uuid.New().String(),
		decimal.NewFromInt(100),
		"USD",
		decimal.NewFromInt(100),
		false,
		time.Now().UTC(),
		time.Now().UTC(),
	)

	mock.ExpectQuery(regexp.QuoteMeta("SELECT")).
		WithArgs(groupID.String()).
		WillReturnRows(rows)

	result, err := repo.ListByMatchGroupIDs(context.Background(), []uuid.UUID{groupID})

	assert.Nil(t, result)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "invalid UUID")
	require.NoError(t, mock.ExpectationsWereMet())
}

// TestListByMatchGroupIDs_EmptyResult tests ListByMatchGroupIDs when no rows are returned.
func TestListByMatchGroupIDs_EmptyResult(t *testing.T) {
	t.Parallel()

	db, mock, err := sqlmock.New()
	require.NoError(t, err)

	defer db.Close()

	provider := testutil.NewMockProviderFromDB(t, db)
	repo := NewRepository(provider)

	groupID := uuid.New()

	rows := sqlmock.NewRows([]string{
		"id", "match_group_id", "transaction_id", "allocated_amount",
		"allocated_currency", "expected_amount", "allow_partial", "created_at", "updated_at",
	})

	mock.ExpectQuery(regexp.QuoteMeta("SELECT")).
		WithArgs(groupID.String()).
		WillReturnRows(rows)

	result, err := repo.ListByMatchGroupIDs(context.Background(), []uuid.UUID{groupID})

	require.NoError(t, err)
	require.NotNil(t, result)
	assert.Empty(t, result)
	require.NoError(t, mock.ExpectationsWereMet())
}

// TestListByMatchGroupIDs_EmptyInput tests ListByMatchGroupIDs with empty slice returns empty map without query.
func TestListByMatchGroupIDs_EmptyInput(t *testing.T) {
	t.Parallel()

	db, _, err := sqlmock.New()
	require.NoError(t, err)

	defer db.Close()

	provider := testutil.NewMockProviderFromDB(t, db)
	repo := NewRepository(provider)

	// No query expectations — empty input should short-circuit.
	result, err := repo.ListByMatchGroupIDs(context.Background(), []uuid.UUID{})

	require.NoError(t, err)
	require.NotNil(t, result)
	assert.Empty(t, result)
}

// TestListByMatchGroupIDs_RowsError tests ListByMatchGroupIDs when rows.Err() returns an error after iteration.
func TestListByMatchGroupIDs_RowsError(t *testing.T) {
	t.Parallel()

	db, mock, err := sqlmock.New()
	require.NoError(t, err)

	defer db.Close()

	provider := testutil.NewMockProviderFromDB(t, db)
	repo := NewRepository(provider)

	groupID := uuid.New()
	now := time.Now().UTC()

	errRowIteration := errors.New("row iteration error")

	rows := sqlmock.NewRows([]string{
		"id", "match_group_id", "transaction_id", "allocated_amount",
		"allocated_currency", "expected_amount", "allow_partial", "created_at", "updated_at",
	}).
		AddRow(uuid.New().String(), groupID.String(), uuid.New().String(), decimal.NewFromInt(100), "USD", decimal.NewFromInt(100), false, now, now).
		RowError(0, errRowIteration)

	mock.ExpectQuery(regexp.QuoteMeta("SELECT")).
		WithArgs(groupID.String()).
		WillReturnRows(rows)

	result, err := repo.ListByMatchGroupIDs(context.Background(), []uuid.UUID{groupID})

	assert.Nil(t, result)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "iterate match item rows")
	require.NoError(t, mock.ExpectationsWereMet())
}

// TestListByMatchGroupIDs_NilRepository tests ListByMatchGroupIDs with nil repository.
func TestListByMatchGroupIDs_NilRepository(t *testing.T) {
	t.Parallel()

	var repo *Repository

	result, err := repo.ListByMatchGroupIDs(context.Background(), []uuid.UUID{uuid.New()})

	assert.Nil(t, result)
	require.ErrorIs(t, err, ErrRepoNotInitialized)
}

// TestListByMatchGroupIDs_NilProvider tests ListByMatchGroupIDs with nil provider.
func TestListByMatchGroupIDs_NilProvider(t *testing.T) {
	t.Parallel()

	repo := &Repository{provider: nil}

	result, err := repo.ListByMatchGroupIDs(context.Background(), []uuid.UUID{uuid.New()})

	assert.Nil(t, result)
	require.ErrorIs(t, err, ErrRepoNotInitialized)
}

// TestListByMatchGroupID_InvalidTransactionIDParse tests ListByMatchGroupID when transaction_id parse fails.
func TestListByMatchGroupID_InvalidTransactionIDParse(t *testing.T) {
	t.Parallel()

	db, mock, err := sqlmock.New()
	require.NoError(t, err)

	defer db.Close()

	provider := testutil.NewMockProviderFromDB(t, db)
	repo := NewRepository(provider)

	groupID := uuid.New()
	now := time.Now().UTC()

	rows := sqlmock.NewRows([]string{
		"id", "match_group_id", "transaction_id", "allocated_amount",
		"allocated_currency", "expected_amount", "allow_partial", "created_at", "updated_at",
	}).AddRow(
		uuid.New().String(),
		groupID.String(),
		"invalid-uuid",
		decimal.NewFromInt(100),
		"USD",
		decimal.NewFromInt(100),
		false,
		now,
		now,
	)

	mock.ExpectQuery(regexp.QuoteMeta("SELECT")).
		WithArgs(groupID.String()).
		WillReturnRows(rows)

	items, err := repo.ListByMatchGroupID(context.Background(), groupID)

	assert.Nil(t, items)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "invalid UUID")
	require.NoError(t, mock.ExpectationsWereMet())
}
