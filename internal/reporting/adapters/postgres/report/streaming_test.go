//go:build unit

package report

import (
	"context"
	"database/sql"
	"errors"
	"regexp"
	"testing"
	"time"

	sqlmock "github.com/DATA-DOG/go-sqlmock"
	"github.com/google/uuid"
	"github.com/shopspring/decimal"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/LerianStudio/matcher/internal/reporting/domain/entities"
	"github.com/LerianStudio/matcher/internal/shared/infrastructure/testutil"
	sharedPorts "github.com/LerianStudio/matcher/internal/shared/ports"
)

var (
	errTestRowError          = errors.New("row error")
	errTestCommitFailed      = errors.New("commit failed")
	errTestConnectionRefused = errors.New("connection refused")
)

func setupStreamingRepository(t *testing.T) (*Repository, func()) {
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

	return repo, finish
}

func TestStreamMatchedForExport_NilRepository(t *testing.T) {
	t.Parallel()

	var repo *Repository

	ctx := context.Background()
	filter := entities.ReportFilter{ContextID: uuid.New()}

	iter, err := repo.StreamMatchedForExport(ctx, filter, 100)

	assert.Nil(t, iter)
	require.ErrorIs(t, err, ErrRepositoryNotInitialized)
}

func TestStreamMatchedForExport_NilProvider(t *testing.T) {
	t.Parallel()

	repo := &Repository{provider: nil}
	ctx := context.Background()
	filter := entities.ReportFilter{ContextID: uuid.New()}

	iter, err := repo.StreamMatchedForExport(ctx, filter, 100)

	assert.Nil(t, iter)
	require.ErrorIs(t, err, ErrRepositoryNotInitialized)
}

func TestStreamMatchedForExport_ContextIDRequired(t *testing.T) {
	t.Parallel()

	repo, finish := setupStreamingRepository(t)
	defer finish()

	ctx := context.Background()
	filter := entities.ReportFilter{ContextID: uuid.Nil}

	iter, err := repo.StreamMatchedForExport(ctx, filter, 100)

	assert.Nil(t, iter)
	require.ErrorIs(t, err, ErrContextIDRequired)
}

func TestStreamUnmatchedForExport_NilRepository(t *testing.T) {
	t.Parallel()

	var repo *Repository

	ctx := context.Background()
	filter := entities.ReportFilter{ContextID: uuid.New()}

	iter, err := repo.StreamUnmatchedForExport(ctx, filter, 100)

	assert.Nil(t, iter)
	require.ErrorIs(t, err, ErrRepositoryNotInitialized)
}

func TestStreamUnmatchedForExport_NilProvider(t *testing.T) {
	t.Parallel()

	repo := &Repository{provider: nil}
	ctx := context.Background()
	filter := entities.ReportFilter{ContextID: uuid.New()}

	iter, err := repo.StreamUnmatchedForExport(ctx, filter, 100)

	assert.Nil(t, iter)
	require.ErrorIs(t, err, ErrRepositoryNotInitialized)
}

func TestStreamUnmatchedForExport_ContextIDRequired(t *testing.T) {
	t.Parallel()

	repo, finish := setupStreamingRepository(t)
	defer finish()

	ctx := context.Background()
	filter := entities.ReportFilter{ContextID: uuid.Nil}

	iter, err := repo.StreamUnmatchedForExport(ctx, filter, 100)

	assert.Nil(t, iter)
	require.ErrorIs(t, err, ErrContextIDRequired)
}

func TestStreamVarianceForExport_NilRepository(t *testing.T) {
	t.Parallel()

	var repo *Repository

	ctx := context.Background()
	filter := entities.VarianceReportFilter{ContextID: uuid.New()}

	iter, err := repo.StreamVarianceForExport(ctx, filter, 100)

	assert.Nil(t, iter)
	require.ErrorIs(t, err, ErrRepositoryNotInitialized)
}

func TestStreamVarianceForExport_NilProvider(t *testing.T) {
	t.Parallel()

	repo := &Repository{provider: nil}
	ctx := context.Background()
	filter := entities.VarianceReportFilter{ContextID: uuid.New()}

	iter, err := repo.StreamVarianceForExport(ctx, filter, 100)

	assert.Nil(t, iter)
	require.ErrorIs(t, err, ErrRepositoryNotInitialized)
}

func TestStreamVarianceForExport_ContextIDRequired(t *testing.T) {
	t.Parallel()

	repo, finish := setupStreamingRepository(t)
	defer finish()

	ctx := context.Background()
	filter := entities.VarianceReportFilter{ContextID: uuid.Nil}

	iter, err := repo.StreamVarianceForExport(ctx, filter, 100)

	assert.Nil(t, iter)
	require.ErrorIs(t, err, ErrContextIDRequired)
}

func TestMatchedRowIterator_Methods(t *testing.T) {
	t.Parallel()

	t.Run("Next returns false when no rows", func(t *testing.T) {
		t.Parallel()

		db, mock, err := sqlmock.New()
		require.NoError(t, err)

		defer db.Close()

		rows := sqlmock.NewRows(
			[]string{"id", "match_group_id", "source_id", "amount", "currency", "date"},
		)
		mock.ExpectQuery("SELECT").WillReturnRows(rows)

		sqlRows, err := db.QueryContext(context.Background(), "SELECT 1")
		require.NoError(t, err)

		iter := &matchedRowIterator{rows: sqlRows, tx: nil}

		assert.False(t, iter.Next())
		assert.NoError(t, iter.Err())
		require.NoError(t, sqlRows.Err())
		assert.NoError(t, iter.Close())
	})

	t.Run("Err returns row error", func(t *testing.T) {
		t.Parallel()

		db, mock, err := sqlmock.New()
		require.NoError(t, err)

		defer db.Close()

		// RowError triggers on the specified row index during iteration
		// We need to add a row so that Next() advances to it and triggers the error
		rows := sqlmock.NewRows([]string{"id", "match_group_id", "source_id", "amount", "currency", "date"}).
			AddRow(uuid.New(), uuid.New(), uuid.New(), "100.00", "USD", time.Now().UTC()).
			RowError(0, errTestRowError)
		mock.ExpectQuery("SELECT").WillReturnRows(rows)

		sqlRows, err := db.QueryContext(context.Background(), "SELECT 1")
		require.NoError(t, err)

		iter := &matchedRowIterator{rows: sqlRows, tx: nil}

		// First Next() will return false because of the row error at index 0
		assert.False(t, iter.Next())
		require.Error(t, iter.Err())
		assert.Contains(t, iter.Err().Error(), "row error")
		require.Error(t, sqlRows.Err())
	})
}

func TestUnmatchedRowIterator_Methods(t *testing.T) {
	t.Parallel()

	t.Run("Next returns false when no rows", func(t *testing.T) {
		t.Parallel()

		db, mock, err := sqlmock.New()
		require.NoError(t, err)

		defer db.Close()

		rows := sqlmock.NewRows(
			[]string{
				"id",
				"source_id",
				"amount",
				"currency",
				"status",
				"date",
				"exception_id",
				"due_at",
			},
		)
		mock.ExpectQuery("SELECT").WillReturnRows(rows)

		sqlRows, err := db.QueryContext(context.Background(), "SELECT 1")
		require.NoError(t, err)

		iter := &unmatchedRowIterator{rows: sqlRows, tx: nil}

		assert.False(t, iter.Next())
		assert.NoError(t, iter.Err())
		require.NoError(t, sqlRows.Err())
		assert.NoError(t, iter.Close())
	})
}

func TestVarianceRowIterator_Methods(t *testing.T) {
	t.Parallel()

	t.Run("Next returns false when no rows", func(t *testing.T) {
		t.Parallel()

		db, mock, err := sqlmock.New()
		require.NoError(t, err)

		defer db.Close()

		rows := sqlmock.NewRows(
			[]string{
				"source_id",
				"currency",
				"fee_schedule_id",
				"fee_schedule_name",
				"total_expected",
				"total_actual",
				"net_variance",
			},
		)
		mock.ExpectQuery("SELECT").WillReturnRows(rows)

		sqlRows, err := db.QueryContext(context.Background(), "SELECT 1")
		require.NoError(t, err)

		iter := &varianceRowIterator{rows: sqlRows, tx: nil}

		assert.False(t, iter.Next())
		assert.NoError(t, iter.Err())
		require.NoError(t, sqlRows.Err())
		assert.NoError(t, iter.Close())
	})
}

func TestMatchedRowIterator_Scan(t *testing.T) {
	t.Parallel()

	db, mock, err := sqlmock.New()
	require.NoError(t, err)

	defer db.Close()

	transactionID := uuid.New()
	matchGroupID := uuid.New()
	sourceID := uuid.New()
	amount := decimal.NewFromFloat(100.50)
	currency := "USD"
	date := time.Now().UTC().Truncate(time.Second)

	rows := sqlmock.NewRows([]string{
		"id", "match_group_id", "source_id", "amount", "currency", "date",
	}).AddRow(
		transactionID, matchGroupID, sourceID, amount, currency, date,
	)

	mock.ExpectQuery("SELECT").WillReturnRows(rows)

	sqlRows, err := db.QueryContext(context.Background(), "SELECT 1")
	require.NoError(t, err)

	iter := &matchedRowIterator{rows: sqlRows, tx: nil}

	require.True(t, iter.Next())

	item, err := iter.Scan()
	require.NoError(t, err)
	assert.Equal(t, transactionID, item.TransactionID)
	assert.Equal(t, matchGroupID, item.MatchGroupID)
	assert.Equal(t, sourceID, item.SourceID)
	assert.True(t, item.Amount.Equal(amount))
	assert.Equal(t, currency, item.Currency)
	assert.True(t, item.Date.Equal(date))

	require.NoError(t, sqlRows.Err())
	assert.NoError(t, iter.Close())
}

func TestUnmatchedRowIterator_Scan(t *testing.T) {
	t.Parallel()

	db, mock, err := sqlmock.New()
	require.NoError(t, err)

	defer db.Close()

	transactionID := uuid.New()
	sourceID := uuid.New()
	exceptionID := uuid.New()
	amount := decimal.NewFromFloat(200.75)
	currency := "EUR"
	status := "PENDING"
	date := time.Now().UTC().Truncate(time.Second)
	dueAt := time.Now().UTC().Add(24 * time.Hour).Truncate(time.Second)

	rows := sqlmock.NewRows([]string{
		"id", "source_id", "amount", "currency", "status", "date", "exception_id", "due_at",
	}).AddRow(
		transactionID, sourceID, amount, currency, status, date, exceptionID, dueAt,
	)

	mock.ExpectQuery("SELECT").WillReturnRows(rows)

	sqlRows, err := db.QueryContext(context.Background(), "SELECT 1")
	require.NoError(t, err)

	iter := &unmatchedRowIterator{rows: sqlRows, tx: nil}

	require.True(t, iter.Next())

	item, err := iter.Scan()
	require.NoError(t, err)
	assert.Equal(t, transactionID, item.TransactionID)
	assert.Equal(t, sourceID, item.SourceID)
	assert.True(t, item.Amount.Equal(amount))
	assert.Equal(t, currency, item.Currency)
	assert.Equal(t, status, item.Status)
	assert.True(t, item.Date.Equal(date))
	require.NotNil(t, item.ExceptionID)
	assert.Equal(t, exceptionID, *item.ExceptionID)
	require.NotNil(t, item.DueAt)
	assert.True(t, item.DueAt.Equal(dueAt))

	require.NoError(t, sqlRows.Err())
	assert.NoError(t, iter.Close())
}

func TestVarianceRowIterator_Scan(t *testing.T) {
	t.Parallel()

	db, mock, err := sqlmock.New()
	require.NoError(t, err)

	defer db.Close()

	sourceID := uuid.New()
	currency := "USD"
	feeType := "PERCENTAGE"
	totalExpected := decimal.NewFromFloat(100.00)
	totalActual := decimal.NewFromFloat(110.00)
	netVariance := decimal.NewFromFloat(10.00)

	rows := sqlmock.NewRows([]string{
		"source_id", "currency", "fee_schedule_id", "fee_schedule_name", "total_expected", "total_actual", "net_variance",
	}).AddRow(
		sourceID, currency, uuid.New(), feeType, totalExpected, totalActual, netVariance,
	)

	mock.ExpectQuery("SELECT").WillReturnRows(rows)

	sqlRows, err := db.QueryContext(context.Background(), "SELECT 1")
	require.NoError(t, err)

	iter := &varianceRowIterator{rows: sqlRows, tx: nil}

	require.True(t, iter.Next())

	item, err := iter.Scan()
	require.NoError(t, err)
	assert.Equal(t, sourceID, item.SourceID)
	assert.Equal(t, currency, item.Currency)
	assert.Equal(t, feeType, item.FeeScheduleName)
	assert.True(t, item.TotalExpected.Equal(totalExpected))
	assert.True(t, item.TotalActual.Equal(totalActual))
	assert.True(t, item.NetVariance.Equal(netVariance))

	require.NoError(t, sqlRows.Err())
	assert.NoError(t, iter.Close())
}

func TestMatchedRowIterator_ScanError(t *testing.T) {
	t.Parallel()

	db, mock, err := sqlmock.New()
	require.NoError(t, err)

	defer db.Close()

	rows := sqlmock.NewRows([]string{
		"id", "match_group_id", "source_id", "amount", "currency", "date",
	}).AddRow(
		"not-a-uuid", uuid.New(), uuid.New(), decimal.NewFromInt(100), "USD", time.Now().UTC(),
	)

	mock.ExpectQuery("SELECT").WillReturnRows(rows)

	sqlRows, err := db.QueryContext(context.Background(), "SELECT 1")
	require.NoError(t, err)

	iter := &matchedRowIterator{rows: sqlRows, tx: nil}
	require.True(t, iter.Next())

	_, err = iter.Scan()
	require.Error(t, err)
	assert.Contains(t, err.Error(), "scanning matched item")

	require.NoError(t, sqlRows.Err())
	assert.NoError(t, iter.Close())
}

func TestUnmatchedRowIterator_ScanError(t *testing.T) {
	t.Parallel()

	db, mock, err := sqlmock.New()
	require.NoError(t, err)

	defer db.Close()

	rows := sqlmock.NewRows([]string{
		"id", "source_id", "amount", "currency", "status", "date", "exception_id", "due_at",
	}).AddRow(
		"not-a-uuid", uuid.New(), decimal.NewFromInt(100), "USD", "PENDING", time.Now().UTC(), nil, nil,
	)

	mock.ExpectQuery("SELECT").WillReturnRows(rows)

	sqlRows, err := db.QueryContext(context.Background(), "SELECT 1")
	require.NoError(t, err)

	iter := &unmatchedRowIterator{rows: sqlRows, tx: nil}
	require.True(t, iter.Next())

	_, err = iter.Scan()
	require.Error(t, err)
	assert.Contains(t, err.Error(), "scanning unmatched item")

	require.NoError(t, sqlRows.Err())
	assert.NoError(t, iter.Close())
}

func TestVarianceRowIterator_ScanError(t *testing.T) {
	t.Parallel()

	db, mock, err := sqlmock.New()
	require.NoError(t, err)

	defer db.Close()

	rows := sqlmock.NewRows([]string{
		"source_id", "currency", "fee_schedule_id", "fee_schedule_name", "total_expected", "total_actual", "net_variance",
	}).AddRow(
		"not-a-uuid", "USD", uuid.New(), "FLAT", decimal.NewFromInt(100), decimal.NewFromInt(100), decimal.Zero,
	)

	mock.ExpectQuery("SELECT").WillReturnRows(rows)

	sqlRows, err := db.QueryContext(context.Background(), "SELECT 1")
	require.NoError(t, err)

	iter := &varianceRowIterator{rows: sqlRows, tx: nil}
	require.True(t, iter.Next())

	_, err = iter.Scan()
	require.Error(t, err)
	assert.Contains(t, err.Error(), "scanning variance row")

	require.NoError(t, sqlRows.Err())
	assert.NoError(t, iter.Close())
}

func TestMatchedRowIterator_CloseWithTransaction(t *testing.T) {
	t.Parallel()

	db, mock, err := sqlmock.New()
	require.NoError(t, err)

	defer db.Close()

	mock.ExpectBegin()

	tx, err := db.BeginTx(context.Background(), nil)
	require.NoError(t, err)

	rows := sqlmock.NewRows(
		[]string{"id", "match_group_id", "source_id", "amount", "currency", "date"},
	)
	mock.ExpectQuery("SELECT").WillReturnRows(rows)
	mock.ExpectCommit()

	sqlRows, err := tx.QueryContext(context.Background(), "SELECT 1")
	require.NoError(t, err)

	iter := &matchedRowIterator{rows: sqlRows, tx: sharedPorts.NewTxLease(tx, nil)}

	require.NoError(t, sqlRows.Err())

	err = iter.Close()
	assert.NoError(t, err)
}

func TestUnmatchedRowIterator_CloseWithTransaction(t *testing.T) {
	t.Parallel()

	db, mock, err := sqlmock.New()
	require.NoError(t, err)

	defer db.Close()

	mock.ExpectBegin()

	tx, err := db.BeginTx(context.Background(), nil)
	require.NoError(t, err)

	rows := sqlmock.NewRows(
		[]string{
			"id",
			"source_id",
			"amount",
			"currency",
			"status",
			"date",
			"exception_id",
			"due_at",
		},
	)
	mock.ExpectQuery("SELECT").WillReturnRows(rows)
	mock.ExpectCommit()

	sqlRows, err := tx.QueryContext(context.Background(), "SELECT 1")
	require.NoError(t, err)

	iter := &unmatchedRowIterator{rows: sqlRows, tx: sharedPorts.NewTxLease(tx, nil)}

	require.NoError(t, sqlRows.Err())

	err = iter.Close()
	assert.NoError(t, err)
}

func TestVarianceRowIterator_CloseWithTransaction(t *testing.T) {
	t.Parallel()

	db, mock, err := sqlmock.New()
	require.NoError(t, err)

	defer db.Close()

	mock.ExpectBegin()

	tx, err := db.BeginTx(context.Background(), nil)
	require.NoError(t, err)

	rows := sqlmock.NewRows(
		[]string{
			"source_id",
			"currency",
			"fee_schedule_id",
			"fee_schedule_name",
			"total_expected",
			"total_actual",
			"net_variance",
		},
	)
	mock.ExpectQuery("SELECT").WillReturnRows(rows)
	mock.ExpectCommit()

	sqlRows, err := tx.QueryContext(context.Background(), "SELECT 1")
	require.NoError(t, err)

	iter := &varianceRowIterator{rows: sqlRows, tx: sharedPorts.NewTxLease(tx, nil)}

	require.NoError(t, sqlRows.Err())

	err = iter.Close()
	assert.NoError(t, err)
}

func TestMatchedRowIterator_CloseWithTransactionError(t *testing.T) {
	t.Parallel()

	db, mock, err := sqlmock.New()
	require.NoError(t, err)

	defer db.Close()

	mock.ExpectBegin()

	tx, err := db.BeginTx(context.Background(), nil)
	require.NoError(t, err)

	rows := sqlmock.NewRows([]string{"id"})
	mock.ExpectQuery("SELECT").WillReturnRows(rows)
	mock.ExpectCommit().WillReturnError(errTestCommitFailed)

	sqlRows, err := tx.QueryContext(context.Background(), "SELECT 1")
	require.NoError(t, err)

	iter := &matchedRowIterator{rows: sqlRows, tx: sharedPorts.NewTxLease(tx, nil)}

	require.NoError(t, sqlRows.Err())

	err = iter.Close()
	require.Error(t, err)
	assert.Contains(t, err.Error(), "commit failed")
	require.NoError(t, mock.ExpectationsWereMet())
}

func TestUnmatchedRowIterator_CloseWithTransactionError(t *testing.T) {
	t.Parallel()

	db, mock, err := sqlmock.New()
	require.NoError(t, err)

	defer db.Close()

	mock.ExpectBegin()

	tx, err := db.BeginTx(context.Background(), nil)
	require.NoError(t, err)

	rows := sqlmock.NewRows(
		[]string{
			"id",
			"source_id",
			"amount",
			"currency",
			"status",
			"date",
			"exception_id",
			"due_at",
		},
	)
	mock.ExpectQuery("SELECT").WillReturnRows(rows)
	mock.ExpectCommit().WillReturnError(errTestCommitFailed)

	sqlRows, err := tx.QueryContext(context.Background(), "SELECT 1")
	require.NoError(t, err)

	iter := &unmatchedRowIterator{rows: sqlRows, tx: sharedPorts.NewTxLease(tx, nil)}

	require.NoError(t, sqlRows.Err())

	err = iter.Close()
	require.Error(t, err)
	assert.Contains(t, err.Error(), "commit failed")
	require.NoError(t, mock.ExpectationsWereMet())
}

func TestVarianceRowIterator_CloseWithTransactionError(t *testing.T) {
	t.Parallel()

	db, mock, err := sqlmock.New()
	require.NoError(t, err)

	defer db.Close()

	mock.ExpectBegin()

	tx, err := db.BeginTx(context.Background(), nil)
	require.NoError(t, err)

	rows := sqlmock.NewRows(
		[]string{
			"source_id",
			"currency",
			"fee_schedule_id",
			"fee_schedule_name",
			"total_expected",
			"total_actual",
			"net_variance",
		},
	)
	mock.ExpectQuery("SELECT").WillReturnRows(rows)
	mock.ExpectCommit().WillReturnError(errTestCommitFailed)

	sqlRows, err := tx.QueryContext(context.Background(), "SELECT 1")
	require.NoError(t, err)

	iter := &varianceRowIterator{rows: sqlRows, tx: sharedPorts.NewTxLease(tx, nil)}

	require.NoError(t, sqlRows.Err())

	err = iter.Close()
	require.Error(t, err)
	assert.Contains(t, err.Error(), "commit failed")
}

func TestIterator_MultipleRows(t *testing.T) {
	t.Parallel()

	db, mock, err := sqlmock.New()
	require.NoError(t, err)

	defer db.Close()

	rows := sqlmock.NewRows([]string{
		"id", "match_group_id", "source_id", "amount", "currency", "date",
	}).
		AddRow(uuid.New(), uuid.New(), uuid.New(), decimal.NewFromInt(100), "USD", time.Now().UTC()).
		AddRow(uuid.New(), uuid.New(), uuid.New(), decimal.NewFromInt(200), "EUR", time.Now().UTC()).
		AddRow(uuid.New(), uuid.New(), uuid.New(), decimal.NewFromInt(300), "GBP", time.Now().UTC())

	mock.ExpectQuery("SELECT").WillReturnRows(rows)

	sqlRows, err := db.QueryContext(context.Background(), "SELECT 1")
	require.NoError(t, err)

	iter := &matchedRowIterator{rows: sqlRows, tx: nil}

	count := 0

	for iter.Next() {
		item, err := iter.Scan()
		require.NoError(t, err)
		require.NotNil(t, item)

		count++
	}

	assert.Equal(t, 3, count)
	assert.NoError(t, iter.Err())
	require.NoError(t, sqlRows.Err())
	assert.NoError(t, iter.Close())
}

func TestStreamMatchedForExport_TransactionBeginError(t *testing.T) {
	t.Parallel()

	provider := &testutil.MockInfrastructureProvider{
		TxErr: errTestConnectionRefused,
	}
	repo := NewRepository(provider)

	ctx := context.Background()
	filter := entities.ReportFilter{
		ContextID: uuid.New(),
		DateFrom:  time.Now().UTC().Add(-24 * time.Hour),
		DateTo:    time.Now().UTC(),
	}

	iter, err := repo.StreamMatchedForExport(ctx, filter, 100)

	assert.Nil(t, iter)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "begin streaming transaction")
}

func TestStreamUnmatchedForExport_TransactionBeginError(t *testing.T) {
	t.Parallel()

	provider := &testutil.MockInfrastructureProvider{
		TxErr: errTestConnectionRefused,
	}
	repo := NewRepository(provider)

	ctx := context.Background()
	filter := entities.ReportFilter{
		ContextID: uuid.New(),
		DateFrom:  time.Now().UTC().Add(-24 * time.Hour),
		DateTo:    time.Now().UTC(),
	}

	iter, err := repo.StreamUnmatchedForExport(ctx, filter, 100)

	assert.Nil(t, iter)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "begin streaming transaction")
}

func TestStreamVarianceForExport_TransactionBeginError(t *testing.T) {
	t.Parallel()

	provider := &testutil.MockInfrastructureProvider{
		TxErr: errTestConnectionRefused,
	}
	repo := NewRepository(provider)

	ctx := context.Background()
	filter := entities.VarianceReportFilter{
		ContextID: uuid.New(),
		DateFrom:  time.Now().UTC().Add(-24 * time.Hour),
		DateTo:    time.Now().UTC(),
	}

	iter, err := repo.StreamVarianceForExport(ctx, filter, 100)

	assert.Nil(t, iter)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "begin streaming transaction")
}

func TestStreamMatchedForExport_QueryError(t *testing.T) {
	t.Parallel()

	db, mock, err := sqlmock.New()
	require.NoError(t, err)

	defer db.Close()

	provider := testutil.NewMockProviderFromDB(t, db)
	repo := NewRepository(provider)

	ctx := context.Background()
	filter := entities.ReportFilter{
		ContextID: uuid.New(),
		DateFrom:  time.Now().UTC().Add(-24 * time.Hour),
		DateTo:    time.Now().UTC(),
	}

	mock.ExpectBegin()
	mock.ExpectQuery(regexp.QuoteMeta("SELECT")).WillReturnError(sql.ErrConnDone)
	mock.ExpectRollback()

	iter, err := repo.StreamMatchedForExport(ctx, filter, 100)

	assert.Nil(t, iter)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "querying matched items")
	require.NoError(t, mock.ExpectationsWereMet())
}

func TestStreamUnmatchedForExport_QueryError(t *testing.T) {
	t.Parallel()

	db, mock, err := sqlmock.New()
	require.NoError(t, err)

	defer db.Close()

	provider := testutil.NewMockProviderFromDB(t, db)
	repo := NewRepository(provider)

	ctx := context.Background()
	filter := entities.ReportFilter{
		ContextID: uuid.New(),
		DateFrom:  time.Now().UTC().Add(-24 * time.Hour),
		DateTo:    time.Now().UTC(),
	}

	mock.ExpectBegin()
	mock.ExpectQuery(regexp.QuoteMeta("SELECT")).WillReturnError(sql.ErrConnDone)
	mock.ExpectRollback()

	iter, err := repo.StreamUnmatchedForExport(ctx, filter, 100)

	assert.Nil(t, iter)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "querying unmatched items")
	require.NoError(t, mock.ExpectationsWereMet())
}

func TestStreamVarianceForExport_QueryError(t *testing.T) {
	t.Parallel()

	db, mock, err := sqlmock.New()
	require.NoError(t, err)

	defer db.Close()

	provider := testutil.NewMockProviderFromDB(t, db)
	repo := NewRepository(provider)

	ctx := context.Background()
	filter := entities.VarianceReportFilter{
		ContextID: uuid.New(),
		DateFrom:  time.Now().UTC().Add(-24 * time.Hour),
		DateTo:    time.Now().UTC(),
	}

	mock.ExpectBegin()
	mock.ExpectQuery(regexp.QuoteMeta("SELECT")).WillReturnError(sql.ErrConnDone)
	mock.ExpectRollback()

	iter, err := repo.StreamVarianceForExport(ctx, filter, 100)

	assert.Nil(t, iter)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "querying variance rows")
	require.NoError(t, mock.ExpectationsWereMet())
}

func TestRepository_ImplementsStreamingInterface(t *testing.T) {
	t.Parallel()

	db, _, err := sqlmock.New()
	require.NoError(t, err)

	defer db.Close()

	provider := testutil.NewMockProviderFromDB(t, db)
	repo := NewRepository(provider)

	assert.NotNil(t, repo)
}

func TestStreamMatchedForExport_WithSourceIDFilter(t *testing.T) {
	t.Parallel()

	db, mock, err := sqlmock.New()
	require.NoError(t, err)

	defer db.Close()

	provider := testutil.NewMockProviderFromDB(t, db)
	repo := NewRepository(provider)

	ctx := context.Background()
	sourceID := uuid.New()
	filter := entities.ReportFilter{
		ContextID: uuid.New(),
		DateFrom:  time.Now().UTC().Add(-24 * time.Hour),
		DateTo:    time.Now().UTC(),
		SourceID:  &sourceID,
	}

	mock.ExpectBegin()
	mock.ExpectQuery(regexp.QuoteMeta("SELECT")).
		WillReturnRows(sqlmock.NewRows([]string{"id", "match_group_id", "source_id", "amount", "currency", "date"}))
	mock.ExpectCommit()

	iter, err := repo.StreamMatchedForExport(ctx, filter, 100)

	require.NoError(t, err)
	require.NotNil(t, iter)
	assert.False(t, iter.Next())
	assert.NoError(t, iter.Close())
	require.NoError(t, mock.ExpectationsWereMet())
}

func TestStreamUnmatchedForExport_WithFilters(t *testing.T) {
	t.Parallel()

	db, mock, err := sqlmock.New()
	require.NoError(t, err)

	defer db.Close()

	provider := testutil.NewMockProviderFromDB(t, db)
	repo := NewRepository(provider)

	ctx := context.Background()
	sourceID := uuid.New()
	status := "PENDING"
	filter := entities.ReportFilter{
		ContextID: uuid.New(),
		DateFrom:  time.Now().UTC().Add(-24 * time.Hour),
		DateTo:    time.Now().UTC(),
		SourceID:  &sourceID,
		Status:    &status,
	}

	mock.ExpectBegin()
	mock.ExpectQuery(regexp.QuoteMeta("SELECT")).
		WillReturnRows(sqlmock.NewRows([]string{"id", "source_id", "amount", "currency", "status", "date", "exception_id", "due_at"}))
	mock.ExpectCommit()

	iter, err := repo.StreamUnmatchedForExport(ctx, filter, 100)

	require.NoError(t, err)
	require.NotNil(t, iter)
	assert.False(t, iter.Next())
	assert.NoError(t, iter.Close())
	require.NoError(t, mock.ExpectationsWereMet())
}

func TestStreamVarianceForExport_WithSourceIDFilter(t *testing.T) {
	t.Parallel()

	db, mock, err := sqlmock.New()
	require.NoError(t, err)

	defer db.Close()

	provider := testutil.NewMockProviderFromDB(t, db)
	repo := NewRepository(provider)

	ctx := context.Background()
	sourceID := uuid.New()
	filter := entities.VarianceReportFilter{
		ContextID: uuid.New(),
		DateFrom:  time.Now().UTC().Add(-24 * time.Hour),
		DateTo:    time.Now().UTC(),
		SourceID:  &sourceID,
	}

	mock.ExpectBegin()
	mock.ExpectQuery(regexp.QuoteMeta("SELECT")).
		WillReturnRows(sqlmock.NewRows([]string{"source_id", "currency", "fee_schedule_id", "fee_schedule_name", "total_expected", "total_actual", "net_variance"}))
	mock.ExpectCommit()

	iter, err := repo.StreamVarianceForExport(ctx, filter, 100)

	require.NoError(t, err)
	require.NotNil(t, iter)
	assert.False(t, iter.Next())
	assert.NoError(t, iter.Close())
	require.NoError(t, mock.ExpectationsWereMet())
}

func TestStreamMatchedForExport_SuccessfulIteration(t *testing.T) {
	t.Parallel()

	db, mock, err := sqlmock.New()
	require.NoError(t, err)

	defer db.Close()

	provider := testutil.NewMockProviderFromDB(t, db)
	repo := NewRepository(provider)

	ctx := context.Background()
	filter := entities.ReportFilter{
		ContextID: uuid.New(),
		DateFrom:  time.Now().UTC().Add(-24 * time.Hour),
		DateTo:    time.Now().UTC(),
	}

	txID1 := uuid.New()
	txID2 := uuid.New()
	groupID := uuid.New()
	sourceID := uuid.New()
	amount := decimal.NewFromFloat(100.50)
	date := time.Now().UTC()

	mock.ExpectBegin()
	mock.ExpectQuery(regexp.QuoteMeta("SELECT")).
		WillReturnRows(sqlmock.NewRows([]string{"id", "match_group_id", "source_id", "amount", "currency", "date"}).
			AddRow(txID1, groupID, sourceID, amount, "USD", date).
			AddRow(txID2, groupID, sourceID, amount, "EUR", date))
	mock.ExpectCommit()

	iter, err := repo.StreamMatchedForExport(ctx, filter, 100)

	require.NoError(t, err)
	require.NotNil(t, iter)

	count := 0
	for iter.Next() {
		item, err := iter.Scan()
		require.NoError(t, err)
		require.NotNil(t, item)
		count++
	}

	assert.Equal(t, 2, count)
	assert.NoError(t, iter.Err())
	assert.NoError(t, iter.Close())
	require.NoError(t, mock.ExpectationsWereMet())
}

func TestStreamUnmatchedForExport_SuccessfulIteration(t *testing.T) {
	t.Parallel()

	db, mock, err := sqlmock.New()
	require.NoError(t, err)

	defer db.Close()

	provider := testutil.NewMockProviderFromDB(t, db)
	repo := NewRepository(provider)

	ctx := context.Background()
	filter := entities.ReportFilter{
		ContextID: uuid.New(),
		DateFrom:  time.Now().UTC().Add(-24 * time.Hour),
		DateTo:    time.Now().UTC(),
	}

	txID := uuid.New()
	sourceID := uuid.New()
	amount := decimal.NewFromFloat(200.00)
	date := time.Now().UTC()
	exceptionID := uuid.New()
	dueAt := time.Now().UTC().Add(48 * time.Hour).UTC()

	mock.ExpectBegin()
	mock.ExpectQuery(regexp.QuoteMeta("SELECT")).
		WillReturnRows(sqlmock.NewRows([]string{"id", "source_id", "amount", "currency", "status", "date", "exception_id", "due_at"}).
			AddRow(txID, sourceID, amount, "USD", "PENDING", date, exceptionID, dueAt))
	mock.ExpectCommit()

	iter, err := repo.StreamUnmatchedForExport(ctx, filter, 100)

	require.NoError(t, err)
	require.NotNil(t, iter)

	require.True(t, iter.Next())
	item, err := iter.Scan()
	require.NoError(t, err)
	assert.Equal(t, txID, item.TransactionID)
	assert.Equal(t, sourceID, item.SourceID)
	assert.True(t, amount.Equal(item.Amount))

	assert.False(t, iter.Next())
	assert.NoError(t, iter.Err())
	assert.NoError(t, iter.Close())
	require.NoError(t, mock.ExpectationsWereMet())
}

func TestStreamVarianceForExport_SuccessfulIteration(t *testing.T) {
	t.Parallel()

	db, mock, err := sqlmock.New()
	require.NoError(t, err)

	defer db.Close()

	provider := testutil.NewMockProviderFromDB(t, db)
	repo := NewRepository(provider)

	ctx := context.Background()
	filter := entities.VarianceReportFilter{
		ContextID: uuid.New(),
		DateFrom:  time.Now().UTC().Add(-24 * time.Hour),
		DateTo:    time.Now().UTC(),
	}

	sourceID := uuid.New()
	feeScheduleID := uuid.New()
	totalExpected := decimal.NewFromFloat(1000.00)
	totalActual := decimal.NewFromFloat(950.00)
	netVariance := decimal.NewFromFloat(-50.00)

	mock.ExpectBegin()
	mock.ExpectQuery(regexp.QuoteMeta("SELECT")).
		WillReturnRows(sqlmock.NewRows([]string{"source_id", "currency", "fee_schedule_id", "fee_schedule_name", "total_expected", "total_actual", "net_variance"}).
			AddRow(sourceID, "USD", feeScheduleID, "FLAT", totalExpected, totalActual, netVariance))
	mock.ExpectCommit()

	iter, err := repo.StreamVarianceForExport(ctx, filter, 100)

	require.NoError(t, err)
	require.NotNil(t, iter)

	require.True(t, iter.Next())
	item, err := iter.Scan()
	require.NoError(t, err)
	assert.Equal(t, sourceID, item.SourceID)
	assert.Equal(t, "USD", item.Currency)
	assert.Equal(t, feeScheduleID, item.FeeScheduleID)
	assert.Equal(t, "FLAT", item.FeeScheduleName)
	require.NotNil(t, item.VariancePct)

	expectedVariancePct := netVariance.Div(totalExpected).Mul(decimal.NewFromInt(100))
	assert.True(
		t,
		item.VariancePct.Equal(expectedVariancePct),
		"expected VariancePct %s, got %s",
		expectedVariancePct,
		item.VariancePct,
	)

	assert.False(t, iter.Next())
	assert.NoError(t, iter.Err())
	assert.NoError(t, iter.Close())
	require.NoError(t, mock.ExpectationsWereMet())
}

func TestMatchedRowIterator_ScanAfterExhausted(t *testing.T) {
	t.Parallel()

	db, mock, err := sqlmock.New()
	require.NoError(t, err)

	defer db.Close()

	rows := sqlmock.NewRows(
		[]string{"id", "match_group_id", "source_id", "amount", "currency", "date"},
	)
	mock.ExpectQuery("SELECT").WillReturnRows(rows)

	sqlRows, err := db.QueryContext(context.Background(), "SELECT 1")
	require.NoError(t, err)

	iter := &matchedRowIterator{rows: sqlRows, tx: nil}

	assert.False(t, iter.Next())

	_, err = iter.Scan()
	require.Error(t, err)
	assert.Contains(t, err.Error(), "scanning matched item")
	assert.NoError(t, iter.Close())
}

func TestUnmatchedRowIterator_ScanAfterExhausted(t *testing.T) {
	t.Parallel()

	db, mock, err := sqlmock.New()
	require.NoError(t, err)

	defer db.Close()

	rows := sqlmock.NewRows(
		[]string{
			"id",
			"source_id",
			"amount",
			"currency",
			"status",
			"date",
			"exception_id",
			"due_at",
		},
	)
	mock.ExpectQuery("SELECT").WillReturnRows(rows)

	sqlRows, err := db.QueryContext(context.Background(), "SELECT 1")
	require.NoError(t, err)

	iter := &unmatchedRowIterator{rows: sqlRows, tx: nil}

	assert.False(t, iter.Next())

	_, err = iter.Scan()
	require.Error(t, err)
	assert.Contains(t, err.Error(), "scanning unmatched item")
	assert.NoError(t, iter.Close())
}

func TestVarianceRowIterator_ScanAfterExhausted(t *testing.T) {
	t.Parallel()

	db, mock, err := sqlmock.New()
	require.NoError(t, err)

	defer db.Close()

	rows := sqlmock.NewRows(
		[]string{
			"source_id",
			"currency",
			"fee_schedule_id",
			"fee_schedule_name",
			"total_expected",
			"total_actual",
			"net_variance",
		},
	)
	mock.ExpectQuery("SELECT").WillReturnRows(rows)

	sqlRows, err := db.QueryContext(context.Background(), "SELECT 1")
	require.NoError(t, err)

	iter := &varianceRowIterator{rows: sqlRows, tx: nil}

	assert.False(t, iter.Next())

	_, err = iter.Scan()
	require.Error(t, err)
	assert.Contains(t, err.Error(), "scanning variance row")
	assert.NoError(t, iter.Close())
}

func TestIterator_CloseWithoutTransaction(t *testing.T) {
	t.Parallel()

	t.Run("matched iterator close without tx", func(t *testing.T) {
		t.Parallel()

		db, mock, err := sqlmock.New()
		require.NoError(t, err)
		defer db.Close()

		rows := sqlmock.NewRows(
			[]string{"id", "match_group_id", "source_id", "amount", "currency", "date"},
		)
		mock.ExpectQuery("SELECT").WillReturnRows(rows)

		sqlRows, err := db.QueryContext(context.Background(), "SELECT 1")
		require.NoError(t, err)

		iter := &matchedRowIterator{rows: sqlRows, tx: nil}
		err = iter.Close()
		assert.NoError(t, err)
	})

	t.Run("unmatched iterator close without tx", func(t *testing.T) {
		t.Parallel()

		db, mock, err := sqlmock.New()
		require.NoError(t, err)
		defer db.Close()

		rows := sqlmock.NewRows(
			[]string{
				"id",
				"source_id",
				"amount",
				"currency",
				"status",
				"date",
				"exception_id",
				"due_at",
			},
		)
		mock.ExpectQuery("SELECT").WillReturnRows(rows)

		sqlRows, err := db.QueryContext(context.Background(), "SELECT 1")
		require.NoError(t, err)

		iter := &unmatchedRowIterator{rows: sqlRows, tx: nil}
		err = iter.Close()
		assert.NoError(t, err)
	})

	t.Run("variance iterator close without tx", func(t *testing.T) {
		t.Parallel()

		db, mock, err := sqlmock.New()
		require.NoError(t, err)
		defer db.Close()

		rows := sqlmock.NewRows(
			[]string{
				"source_id",
				"currency",
				"fee_schedule_id",
				"fee_schedule_name",
				"total_expected",
				"total_actual",
				"net_variance",
			},
		)
		mock.ExpectQuery("SELECT").WillReturnRows(rows)

		sqlRows, err := db.QueryContext(context.Background(), "SELECT 1")
		require.NoError(t, err)

		iter := &varianceRowIterator{rows: sqlRows, tx: nil}
		err = iter.Close()
		assert.NoError(t, err)
	})
}

func TestStreamMatchedForExport_ZeroMaxRecords(t *testing.T) {
	t.Parallel()

	db, mock, err := sqlmock.New()
	require.NoError(t, err)

	defer db.Close()

	provider := testutil.NewMockProviderFromDB(t, db)
	repo := NewRepository(provider)

	ctx := context.Background()
	filter := entities.ReportFilter{
		ContextID: uuid.New(),
		DateFrom:  time.Now().UTC().Add(-24 * time.Hour),
		DateTo:    time.Now().UTC(),
	}

	mock.ExpectBegin()
	mock.ExpectQuery(regexp.QuoteMeta("SELECT")).
		WillReturnRows(sqlmock.NewRows([]string{"id", "match_group_id", "source_id", "amount", "currency", "date"}))
	mock.ExpectCommit()

	iter, err := repo.StreamMatchedForExport(ctx, filter, 0)

	require.NoError(t, err)
	require.NotNil(t, iter)
	assert.NoError(t, iter.Close())
	require.NoError(t, mock.ExpectationsWereMet())
}

func TestStreamUnmatchedForExport_NegativeMaxRecords(t *testing.T) {
	t.Parallel()

	db, mock, err := sqlmock.New()
	require.NoError(t, err)

	defer db.Close()

	provider := testutil.NewMockProviderFromDB(t, db)
	repo := NewRepository(provider)

	ctx := context.Background()
	filter := entities.ReportFilter{
		ContextID: uuid.New(),
		DateFrom:  time.Now().UTC().Add(-24 * time.Hour),
		DateTo:    time.Now().UTC(),
	}

	mock.ExpectBegin()
	mock.ExpectQuery(regexp.QuoteMeta("SELECT")).
		WillReturnRows(sqlmock.NewRows([]string{"id", "source_id", "amount", "currency", "status", "date", "exception_id", "due_at"}))
	mock.ExpectCommit()

	iter, err := repo.StreamUnmatchedForExport(ctx, filter, -10)

	require.NoError(t, err)
	require.NotNil(t, iter)
	assert.NoError(t, iter.Close())
	require.NoError(t, mock.ExpectationsWereMet())
}

// TestStreamingIterators_NilRowsDefensivePath asserts that Next/Err/Scan do
// not deref a nil *sql.Rows. A nil rows value should never arise in the
// happy path (the stream constructors only return a populated iterator or an
// error), but if a caller uses `&matchedRowIterator{}` directly or closes
// the rows out-of-band the iterator methods must still be callable without
// panicking.
func TestStreamingIterators_NilRowsDefensivePath(t *testing.T) {
	t.Parallel()

	t.Run("matchedRowIterator with nil rows", func(t *testing.T) {
		t.Parallel()

		iter := &matchedRowIterator{}

		assert.False(t, iter.Next())
		assert.NoError(t, iter.Err())

		_, err := iter.Scan()
		require.ErrorIs(t, err, errIteratorNotInitialized)
		assert.NoError(t, iter.Close())
	})

	t.Run("unmatchedRowIterator with nil rows", func(t *testing.T) {
		t.Parallel()

		iter := &unmatchedRowIterator{}

		assert.False(t, iter.Next())
		assert.NoError(t, iter.Err())

		_, err := iter.Scan()
		require.ErrorIs(t, err, errIteratorNotInitialized)
		assert.NoError(t, iter.Close())
	})

	t.Run("varianceRowIterator with nil rows", func(t *testing.T) {
		t.Parallel()

		iter := &varianceRowIterator{}

		assert.False(t, iter.Next())
		assert.NoError(t, iter.Err())

		_, err := iter.Scan()
		require.ErrorIs(t, err, errIteratorNotInitialized)
		assert.NoError(t, iter.Close())
	})
}
