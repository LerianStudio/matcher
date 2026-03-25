//go:build unit

package cross

import (
	"context"
	"database/sql"
	"errors"
	"testing"
	"time"

	"github.com/DATA-DOG/go-sqlmock"
	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	ingestionTxRepo "github.com/LerianStudio/matcher/internal/ingestion/adapters/postgres/transaction"
	matchingRepos "github.com/LerianStudio/matcher/internal/matching/domain/repositories"
	shared "github.com/LerianStudio/matcher/internal/shared/domain"
	sharedPorts "github.com/LerianStudio/matcher/internal/shared/ports"
)

var errTxDBError = errors.New("transaction db error")

type mockInfraProvider struct {
	beginTxErr error
	beginTxFn  func(ctx context.Context) (*sql.Tx, error)
}

func (m *mockInfraProvider) GetRedisConnection(
	_ context.Context,
) (*sharedPorts.RedisConnectionLease, error) {
	return nil, nil
}

func (m *mockInfraProvider) BeginTx(ctx context.Context) (*sharedPorts.TxLease, error) {
	if m.beginTxFn != nil {
		tx, err := m.beginTxFn(ctx)
		if err != nil {
			return nil, err
		}

		return sharedPorts.NewTxLease(tx, nil), nil
	}

	return nil, m.beginTxErr
}

func (m *mockInfraProvider) GetReplicaDB(_ context.Context) (*sharedPorts.DBLease, error) {
	return nil, nil
}

func (m *mockInfraProvider) GetPrimaryDB(_ context.Context) (*sharedPorts.DBLease, error) {
	return nil, nil
}

type mockBaseRepo struct {
	listUnmatchedResult        []*shared.Transaction
	listUnmatchedErr           error
	findByContextIDsResult     []*shared.Transaction
	findByContextIDsErr        error
	markMatchedErr             error
	markMatchedWithTxErr       error
	markMatchedWithTxCalled    bool
	markMatchedCapturedTx      *sql.Tx
	markPendingReviewErr       error
	markPendingReviewWithTxErr error
	markPendingWithTxCalled    bool
	markPendingCapturedTx      *sql.Tx
	markUnmatchedErr           error
	markUnmatchedWithTxErr     error
	markUnmatchedWithTxCalled  bool
	markUnmatchedCapturedTx    *sql.Tx
}

func (m *mockBaseRepo) ListUnmatchedByContext(
	_ context.Context,
	_ uuid.UUID,
	_, _ *time.Time,
	_, _ int,
) ([]*shared.Transaction, error) {
	return m.listUnmatchedResult, m.listUnmatchedErr
}

func (m *mockBaseRepo) FindByContextAndIDs(
	_ context.Context,
	_ uuid.UUID,
	_ []uuid.UUID,
) ([]*shared.Transaction, error) {
	return m.findByContextIDsResult, m.findByContextIDsErr
}

func (m *mockBaseRepo) MarkMatched(_ context.Context, _ uuid.UUID, _ []uuid.UUID) error {
	return m.markMatchedErr
}

func (m *mockBaseRepo) MarkMatchedWithTx(_ context.Context, tx *sql.Tx, _ uuid.UUID, _ []uuid.UUID) error {
	m.markMatchedWithTxCalled = true
	m.markMatchedCapturedTx = tx
	return m.markMatchedWithTxErr
}

func (m *mockBaseRepo) MarkPendingReview(_ context.Context, _ uuid.UUID, _ []uuid.UUID) error {
	return m.markPendingReviewErr
}

func (m *mockBaseRepo) MarkPendingReviewWithTx(_ context.Context, tx *sql.Tx, _ uuid.UUID, _ []uuid.UUID) error {
	m.markPendingWithTxCalled = true
	m.markPendingCapturedTx = tx
	return m.markPendingReviewWithTxErr
}

func (m *mockBaseRepo) MarkUnmatched(_ context.Context, _ uuid.UUID, _ []uuid.UUID) error {
	return m.markUnmatchedErr
}

func (m *mockBaseRepo) MarkUnmatchedWithTx(_ context.Context, tx *sql.Tx, _ uuid.UUID, _ []uuid.UUID) error {
	m.markUnmatchedWithTxCalled = true
	m.markUnmatchedCapturedTx = tx
	return m.markUnmatchedWithTxErr
}

var _ BaseTransactionRepository = (*mockBaseRepo)(nil)

// mustNewAdapter creates an adapter with valid dependencies for tests.
func mustNewAdapter(t *testing.T, provider *mockInfraProvider, baseRepo *mockBaseRepo) *TransactionRepositoryAdapter {
	t.Helper()

	return &TransactionRepositoryAdapter{provider: provider, baseRepo: baseRepo}
}

// --- Constructor tests ---

func TestNewTransactionRepositoryAdapterFromRepo_Success(t *testing.T) {
	t.Parallel()

	provider := &mockInfraProvider{}
	baseRepo := ingestionTxRepo.NewRepository(provider)

	adapter, err := NewTransactionRepositoryAdapterFromRepo(provider, baseRepo)

	require.NoError(t, err)
	require.NotNil(t, adapter)
	assert.Equal(t, provider, adapter.provider)
}

func TestNewTransactionRepositoryAdapterFromRepo_NilProvider(t *testing.T) {
	t.Parallel()

	baseRepo := ingestionTxRepo.NewRepository(&mockInfraProvider{})

	adapter, err := NewTransactionRepositoryAdapterFromRepo(nil, baseRepo)

	require.Error(t, err)
	assert.Nil(t, adapter)
	require.ErrorIs(t, err, ErrNilProvider)
}

func TestNewTransactionRepositoryAdapterFromRepo_NilBaseRepo(t *testing.T) {
	t.Parallel()

	adapter, err := NewTransactionRepositoryAdapterFromRepo(&mockInfraProvider{}, nil)

	require.Error(t, err)
	assert.Nil(t, adapter)
	require.ErrorIs(t, err, ErrNilBaseRepo)
}

// --- Sentinel error tests ---

func TestTransactionRepositoryAdapter_SentinelErrors(t *testing.T) {
	t.Parallel()

	assert.Equal(
		t,
		"transaction repository adapter not initialized",
		ErrAdapterNotInitialized.Error(),
	)
	assert.Equal(t, "context id is required", ErrContextIDRequired.Error())
	assert.Equal(t, "infrastructure provider is required", ErrNilProvider.Error())
	assert.Equal(t, "base transaction repository is required", ErrNilBaseRepo.Error())
}

// --- Nil adapter (var declaration) tests ---

func TestTransactionRepositoryAdapter_ListUnmatchedByContext_NilAdapter(t *testing.T) {
	t.Parallel()

	var adapter *TransactionRepositoryAdapter

	ctx := context.Background()
	contextID := uuid.New()

	result, err := adapter.ListUnmatchedByContext(ctx, contextID, nil, nil, 100, 0)

	require.Error(t, err)
	assert.Nil(t, result)
	require.ErrorIs(t, err, ErrAdapterNotInitialized)
}

func TestTransactionRepositoryAdapter_ListUnmatchedByContext_NilBaseRepo(t *testing.T) {
	t.Parallel()

	adapter := &TransactionRepositoryAdapter{
		provider: nil,
		baseRepo: nil,
	}
	ctx := context.Background()
	contextID := uuid.New()

	result, err := adapter.ListUnmatchedByContext(ctx, contextID, nil, nil, 100, 0)

	require.Error(t, err)
	assert.Nil(t, result)
	require.ErrorIs(t, err, ErrAdapterNotInitialized)
}

func TestTransactionRepositoryAdapter_MarkMatched_NilAdapter(t *testing.T) {
	t.Parallel()

	var adapter *TransactionRepositoryAdapter

	ctx := context.Background()
	contextID := uuid.New()
	transactionIDs := []uuid.UUID{uuid.New()}

	err := adapter.MarkMatched(ctx, contextID, transactionIDs)

	require.Error(t, err)
	require.ErrorIs(t, err, ErrAdapterNotInitialized)
}

func TestTransactionRepositoryAdapter_MarkMatched_NilBaseRepo(t *testing.T) {
	t.Parallel()

	adapter := &TransactionRepositoryAdapter{
		provider: nil,
		baseRepo: nil,
	}
	ctx := context.Background()
	contextID := uuid.New()
	transactionIDs := []uuid.UUID{uuid.New()}

	err := adapter.MarkMatched(ctx, contextID, transactionIDs)

	require.Error(t, err)
	require.ErrorIs(t, err, ErrAdapterNotInitialized)
}

func TestTransactionRepositoryAdapter_MarkPendingReview_NilAdapter(t *testing.T) {
	t.Parallel()

	var adapter *TransactionRepositoryAdapter

	ctx := context.Background()
	contextID := uuid.New()
	transactionIDs := []uuid.UUID{uuid.New()}

	err := adapter.MarkPendingReview(ctx, contextID, transactionIDs)

	require.Error(t, err)
	require.ErrorIs(t, err, ErrAdapterNotInitialized)
}

func TestTransactionRepositoryAdapter_MarkPendingReview_NilBaseRepo(t *testing.T) {
	t.Parallel()

	adapter := &TransactionRepositoryAdapter{
		provider: nil,
		baseRepo: nil,
	}
	ctx := context.Background()
	contextID := uuid.New()
	transactionIDs := []uuid.UUID{uuid.New()}

	err := adapter.MarkPendingReview(ctx, contextID, transactionIDs)

	require.Error(t, err)
	require.ErrorIs(t, err, ErrAdapterNotInitialized)
}

func TestTransactionRepositoryAdapter_WithTx_NilAdapter(t *testing.T) {
	t.Parallel()

	var adapter *TransactionRepositoryAdapter

	ctx := context.Background()

	err := adapter.WithTx(ctx, func(tx matchingRepos.Tx) error {
		return nil
	})

	require.Error(t, err)
	require.ErrorIs(t, err, ErrAdapterNotInitialized)
}

func TestTransactionRepositoryAdapter_WithTx_NilProvider(t *testing.T) {
	t.Parallel()

	adapter := &TransactionRepositoryAdapter{
		provider: nil,
		baseRepo: nil,
	}
	ctx := context.Background()

	err := adapter.WithTx(ctx, func(tx matchingRepos.Tx) error {
		return nil
	})

	require.Error(t, err)
	require.ErrorIs(t, err, ErrAdapterNotInitialized)
}

func TestTransactionRepositoryAdapter_FindByContextAndIDs_NilAdapter(t *testing.T) {
	t.Parallel()

	var adapter *TransactionRepositoryAdapter

	ctx := context.Background()
	contextID := uuid.New()
	transactionIDs := []uuid.UUID{uuid.New()}

	result, err := adapter.FindByContextAndIDs(ctx, contextID, transactionIDs)

	require.Error(t, err)
	assert.Nil(t, result)
	require.ErrorIs(t, err, ErrAdapterNotInitialized)
}

func TestTransactionRepositoryAdapter_FindByContextAndIDs_NilBaseRepo(t *testing.T) {
	t.Parallel()

	adapter := &TransactionRepositoryAdapter{
		provider: nil,
		baseRepo: nil,
	}
	ctx := context.Background()
	contextID := uuid.New()
	transactionIDs := []uuid.UUID{uuid.New()}

	result, err := adapter.FindByContextAndIDs(ctx, contextID, transactionIDs)

	require.Error(t, err)
	assert.Nil(t, result)
	require.ErrorIs(t, err, ErrAdapterNotInitialized)
}

func TestTransactionRepositoryAdapter_MarkUnmatched_NilAdapter(t *testing.T) {
	t.Parallel()

	var adapter *TransactionRepositoryAdapter

	ctx := context.Background()
	contextID := uuid.New()
	transactionIDs := []uuid.UUID{uuid.New()}

	err := adapter.MarkUnmatched(ctx, contextID, transactionIDs)

	require.Error(t, err)
	require.ErrorIs(t, err, ErrAdapterNotInitialized)
}

func TestTransactionRepositoryAdapter_MarkUnmatched_NilBaseRepo(t *testing.T) {
	t.Parallel()

	adapter := &TransactionRepositoryAdapter{
		provider: nil,
		baseRepo: nil,
	}
	ctx := context.Background()
	contextID := uuid.New()
	transactionIDs := []uuid.UUID{uuid.New()}

	err := adapter.MarkUnmatched(ctx, contextID, transactionIDs)

	require.Error(t, err)
	require.ErrorIs(t, err, ErrAdapterNotInitialized)
}

func TestTransactionRepositoryAdapter_MarkMatchedWithTx_NilAdapter(t *testing.T) {
	t.Parallel()

	var adapter *TransactionRepositoryAdapter

	ctx := context.Background()
	contextID := uuid.New()
	transactionIDs := []uuid.UUID{uuid.New()}

	err := adapter.MarkMatchedWithTx(ctx, nil, contextID, transactionIDs)

	require.Error(t, err)
	require.ErrorIs(t, err, ErrAdapterNotInitialized)
}

func TestTransactionRepositoryAdapter_MarkMatchedWithTx_NilBaseRepo(t *testing.T) {
	t.Parallel()

	adapter := &TransactionRepositoryAdapter{
		provider: nil,
		baseRepo: nil,
	}
	ctx := context.Background()
	contextID := uuid.New()
	transactionIDs := []uuid.UUID{uuid.New()}

	err := adapter.MarkMatchedWithTx(ctx, nil, contextID, transactionIDs)

	require.Error(t, err)
	require.ErrorIs(t, err, ErrAdapterNotInitialized)
}

func TestTransactionRepositoryAdapter_MarkMatchedWithTx_DelegatesToBaseRepo(t *testing.T) {
	t.Parallel()

	db, mock, err := sqlmock.New()
	require.NoError(t, err)
	defer db.Close()

	mock.ExpectBegin()
	tx, err := db.Begin()
	require.NoError(t, err)
	defer func() { _ = tx.Rollback() }()

	baseRepo := &mockBaseRepo{}
	adapter := mustNewAdapter(t, &mockInfraProvider{}, baseRepo)

	err = adapter.MarkMatchedWithTx(context.Background(), tx, uuid.New(), []uuid.UUID{uuid.New()})
	require.NoError(t, err)
	assert.True(t, baseRepo.markMatchedWithTxCalled)
	assert.Same(t, tx, baseRepo.markMatchedCapturedTx)
	require.NoError(t, mock.ExpectationsWereMet())
}

func TestTransactionRepositoryAdapter_MarkPendingReviewWithTx_NilAdapter(t *testing.T) {
	t.Parallel()

	var adapter *TransactionRepositoryAdapter

	ctx := context.Background()
	contextID := uuid.New()
	transactionIDs := []uuid.UUID{uuid.New()}

	err := adapter.MarkPendingReviewWithTx(ctx, nil, contextID, transactionIDs)

	require.Error(t, err)
	require.ErrorIs(t, err, ErrAdapterNotInitialized)
}

func TestTransactionRepositoryAdapter_MarkPendingReviewWithTx_NilBaseRepo(t *testing.T) {
	t.Parallel()

	adapter := &TransactionRepositoryAdapter{
		provider: nil,
		baseRepo: nil,
	}
	ctx := context.Background()
	contextID := uuid.New()
	transactionIDs := []uuid.UUID{uuid.New()}

	err := adapter.MarkPendingReviewWithTx(ctx, nil, contextID, transactionIDs)

	require.Error(t, err)
	require.ErrorIs(t, err, ErrAdapterNotInitialized)
}

func TestTransactionRepositoryAdapter_MarkPendingReviewWithTx_DelegatesToBaseRepo(t *testing.T) {
	t.Parallel()

	db, mock, err := sqlmock.New()
	require.NoError(t, err)
	defer db.Close()

	mock.ExpectBegin()
	tx, err := db.Begin()
	require.NoError(t, err)
	defer func() { _ = tx.Rollback() }()

	baseRepo := &mockBaseRepo{}
	adapter := mustNewAdapter(t, &mockInfraProvider{}, baseRepo)

	err = adapter.MarkPendingReviewWithTx(context.Background(), tx, uuid.New(), []uuid.UUID{uuid.New()})
	require.NoError(t, err)
	assert.True(t, baseRepo.markPendingWithTxCalled)
	assert.Same(t, tx, baseRepo.markPendingCapturedTx)
	require.NoError(t, mock.ExpectationsWereMet())
}

// --- WithTx nil fn with valid provider ---

func TestTransactionRepositoryAdapter_WithTx_NilFnWithProvider(t *testing.T) {
	t.Parallel()

	provider := &mockInfraProvider{}
	baseRepo := &mockBaseRepo{}
	adapter := mustNewAdapter(t, provider, baseRepo)

	err := adapter.WithTx(context.Background(), nil)

	require.NoError(t, err)
}

func TestTransactionRepositoryAdapter_WithTx_ExecutesCallback(t *testing.T) {
	t.Parallel()

	db, mock, err := sqlmock.New()
	require.NoError(t, err)
	defer db.Close()

	provider := &mockInfraProvider{
		beginTxFn: func(context.Context) (*sql.Tx, error) {
			mock.ExpectBegin()
			mock.ExpectCommit()
			return db.Begin()
		},
	}
	adapter := mustNewAdapter(t, provider, &mockBaseRepo{})

	called := false
	err = adapter.WithTx(context.Background(), func(tx matchingRepos.Tx) error {
		called = true
		require.NotNil(t, tx)
		return nil
	})
	require.NoError(t, err)
	assert.True(t, called)
	require.NoError(t, mock.ExpectationsWereMet())
}

// --- Success / Error path tests ---

func TestTransactionRepositoryAdapter_ListUnmatchedByContext_Success(t *testing.T) {
	t.Parallel()

	provider := &mockInfraProvider{}
	expectedTxs := []*shared.Transaction{
		{ID: uuid.New()},
		{ID: uuid.New()},
	}
	baseRepo := &mockBaseRepo{listUnmatchedResult: expectedTxs}
	adapter := mustNewAdapter(t, provider, baseRepo)
	ctx := context.Background()
	contextID := uuid.New()

	result, err := adapter.ListUnmatchedByContext(ctx, contextID, nil, nil, 100, 0)

	require.NoError(t, err)
	assert.Equal(t, expectedTxs, result)
}

func TestTransactionRepositoryAdapter_ListUnmatchedByContext_Error(t *testing.T) {
	t.Parallel()

	provider := &mockInfraProvider{}
	baseRepo := &mockBaseRepo{listUnmatchedErr: errTxDBError}
	adapter := mustNewAdapter(t, provider, baseRepo)
	ctx := context.Background()
	contextID := uuid.New()

	result, err := adapter.ListUnmatchedByContext(ctx, contextID, nil, nil, 100, 0)

	require.Error(t, err)
	assert.Nil(t, result)
	assert.Contains(t, err.Error(), "list unmatched by context")
	require.ErrorIs(t, err, errTxDBError)
}

func TestTransactionRepositoryAdapter_FindByContextAndIDs_Success(t *testing.T) {
	t.Parallel()

	provider := &mockInfraProvider{}
	expectedTxs := []*shared.Transaction{
		{ID: uuid.New()},
	}
	baseRepo := &mockBaseRepo{findByContextIDsResult: expectedTxs}
	adapter := mustNewAdapter(t, provider, baseRepo)
	ctx := context.Background()
	contextID := uuid.New()
	transactionIDs := []uuid.UUID{uuid.New()}

	result, err := adapter.FindByContextAndIDs(ctx, contextID, transactionIDs)

	require.NoError(t, err)
	assert.Equal(t, expectedTxs, result)
}

func TestTransactionRepositoryAdapter_FindByContextAndIDs_Error(t *testing.T) {
	t.Parallel()

	provider := &mockInfraProvider{}
	baseRepo := &mockBaseRepo{findByContextIDsErr: errTxDBError}
	adapter := mustNewAdapter(t, provider, baseRepo)
	ctx := context.Background()
	contextID := uuid.New()
	transactionIDs := []uuid.UUID{uuid.New()}

	result, err := adapter.FindByContextAndIDs(ctx, contextID, transactionIDs)

	require.Error(t, err)
	assert.Nil(t, result)
	assert.Contains(t, err.Error(), "find by context and ids")
	require.ErrorIs(t, err, errTxDBError)
}

func TestTransactionRepositoryAdapter_MarkMatched_Success(t *testing.T) {
	t.Parallel()

	provider := &mockInfraProvider{}
	baseRepo := &mockBaseRepo{}
	adapter := mustNewAdapter(t, provider, baseRepo)
	ctx := context.Background()
	contextID := uuid.New()
	transactionIDs := []uuid.UUID{uuid.New()}

	err := adapter.MarkMatched(ctx, contextID, transactionIDs)

	require.NoError(t, err)
}

func TestTransactionRepositoryAdapter_MarkMatched_Error(t *testing.T) {
	t.Parallel()

	provider := &mockInfraProvider{}
	baseRepo := &mockBaseRepo{markMatchedErr: errTxDBError}
	adapter := mustNewAdapter(t, provider, baseRepo)
	ctx := context.Background()
	contextID := uuid.New()
	transactionIDs := []uuid.UUID{uuid.New()}

	err := adapter.MarkMatched(ctx, contextID, transactionIDs)

	require.Error(t, err)
	assert.Contains(t, err.Error(), "mark matched")
	require.ErrorIs(t, err, errTxDBError)
}

func TestTransactionRepositoryAdapter_MarkPendingReview_Success(t *testing.T) {
	t.Parallel()

	provider := &mockInfraProvider{}
	baseRepo := &mockBaseRepo{}
	adapter := mustNewAdapter(t, provider, baseRepo)
	ctx := context.Background()
	contextID := uuid.New()
	transactionIDs := []uuid.UUID{uuid.New()}

	err := adapter.MarkPendingReview(ctx, contextID, transactionIDs)

	require.NoError(t, err)
}

func TestTransactionRepositoryAdapter_MarkPendingReview_Error(t *testing.T) {
	t.Parallel()

	provider := &mockInfraProvider{}
	baseRepo := &mockBaseRepo{markPendingReviewErr: errTxDBError}
	adapter := mustNewAdapter(t, provider, baseRepo)
	ctx := context.Background()
	contextID := uuid.New()
	transactionIDs := []uuid.UUID{uuid.New()}

	err := adapter.MarkPendingReview(ctx, contextID, transactionIDs)

	require.Error(t, err)
	assert.Contains(t, err.Error(), "mark pending review")
	require.ErrorIs(t, err, errTxDBError)
}

func TestTransactionRepositoryAdapter_MarkUnmatched_Success(t *testing.T) {
	t.Parallel()

	provider := &mockInfraProvider{}
	baseRepo := &mockBaseRepo{}
	adapter := mustNewAdapter(t, provider, baseRepo)
	ctx := context.Background()
	contextID := uuid.New()
	transactionIDs := []uuid.UUID{uuid.New()}

	err := adapter.MarkUnmatched(ctx, contextID, transactionIDs)

	require.NoError(t, err)
}

func TestTransactionRepositoryAdapter_MarkUnmatched_Error(t *testing.T) {
	t.Parallel()

	provider := &mockInfraProvider{}
	baseRepo := &mockBaseRepo{markUnmatchedErr: errTxDBError}
	adapter := mustNewAdapter(t, provider, baseRepo)
	ctx := context.Background()
	contextID := uuid.New()
	transactionIDs := []uuid.UUID{uuid.New()}

	err := adapter.MarkUnmatched(ctx, contextID, transactionIDs)

	require.Error(t, err)
	assert.Contains(t, err.Error(), "mark unmatched")
	require.ErrorIs(t, err, errTxDBError)
}

// --- WithTx empty IDs with valid provider ---

func TestTransactionRepositoryAdapter_MarkMatchedWithTx_EmptyWithProvider(t *testing.T) {
	t.Parallel()

	provider := &mockInfraProvider{}
	baseRepo := &mockBaseRepo{}
	adapter := mustNewAdapter(t, provider, baseRepo)
	ctx := context.Background()
	contextID := uuid.New()

	err := adapter.MarkMatchedWithTx(ctx, nil, contextID, []uuid.UUID{})

	require.NoError(t, err)
}

func TestTransactionRepositoryAdapter_MarkPendingReviewWithTx_EmptyWithProvider(t *testing.T) {
	t.Parallel()

	provider := &mockInfraProvider{}
	baseRepo := &mockBaseRepo{}
	adapter := mustNewAdapter(t, provider, baseRepo)
	ctx := context.Background()
	contextID := uuid.New()

	err := adapter.MarkPendingReviewWithTx(ctx, nil, contextID, []uuid.UUID{})

	require.NoError(t, err)
}

// --- Nil contextID tests ---

func TestTransactionRepositoryAdapter_MarkMatchedWithTx_NilContextIDWithProvider(t *testing.T) {
	t.Parallel()

	provider := &mockInfraProvider{}
	baseRepo := &mockBaseRepo{}
	adapter := mustNewAdapter(t, provider, baseRepo)
	ctx := context.Background()
	transactionIDs := []uuid.UUID{uuid.New()}

	err := adapter.MarkMatchedWithTx(ctx, nil, uuid.Nil, transactionIDs)

	require.Error(t, err)
	require.ErrorIs(t, err, ErrContextIDRequired)
}

func TestTransactionRepositoryAdapter_MarkPendingReviewWithTx_NilContextIDWithProvider(
	t *testing.T,
) {
	t.Parallel()

	provider := &mockInfraProvider{}
	baseRepo := &mockBaseRepo{}
	adapter := mustNewAdapter(t, provider, baseRepo)
	ctx := context.Background()
	transactionIDs := []uuid.UUID{uuid.New()}

	err := adapter.MarkPendingReviewWithTx(ctx, nil, uuid.Nil, transactionIDs)

	require.Error(t, err)
	require.ErrorIs(t, err, ErrContextIDRequired)
}

func TestTransactionRepositoryAdapter_MarkMatched_NilContextIDWithProviderAndRepo(t *testing.T) {
	t.Parallel()

	provider := &mockInfraProvider{}
	baseRepo := &mockBaseRepo{}
	adapter := mustNewAdapter(t, provider, baseRepo)
	ctx := context.Background()
	transactionIDs := []uuid.UUID{uuid.New()}

	err := adapter.MarkMatched(ctx, uuid.Nil, transactionIDs)

	require.Error(t, err)
	require.ErrorIs(t, err, ErrContextIDRequired)
}

func TestTransactionRepositoryAdapter_MarkPendingReview_NilContextIDWithProviderAndRepo(
	t *testing.T,
) {
	t.Parallel()

	provider := &mockInfraProvider{}
	baseRepo := &mockBaseRepo{}
	adapter := mustNewAdapter(t, provider, baseRepo)
	ctx := context.Background()
	transactionIDs := []uuid.UUID{uuid.New()}

	err := adapter.MarkPendingReview(ctx, uuid.Nil, transactionIDs)

	require.Error(t, err)
	require.ErrorIs(t, err, ErrContextIDRequired)
}

func TestTransactionRepositoryAdapter_MarkUnmatched_NilContextIDWithProviderAndRepo(t *testing.T) {
	t.Parallel()

	provider := &mockInfraProvider{}
	baseRepo := &mockBaseRepo{}
	adapter := mustNewAdapter(t, provider, baseRepo)
	ctx := context.Background()
	transactionIDs := []uuid.UUID{uuid.New()}

	err := adapter.MarkUnmatched(ctx, uuid.Nil, transactionIDs)

	require.Error(t, err)
	require.ErrorIs(t, err, ErrContextIDRequired)
}

func TestTransactionRepositoryAdapter_ListUnmatchedByContext_NilContextIDWithProviderAndRepo(
	t *testing.T,
) {
	t.Parallel()

	provider := &mockInfraProvider{}
	baseRepo := &mockBaseRepo{}
	adapter := mustNewAdapter(t, provider, baseRepo)
	ctx := context.Background()

	result, err := adapter.ListUnmatchedByContext(ctx, uuid.Nil, nil, nil, 100, 0)

	require.Error(t, err)
	assert.Nil(t, result)
	require.ErrorIs(t, err, ErrContextIDRequired)
}

func TestTransactionRepositoryAdapter_FindByContextAndIDs_NilContextIDWithProviderAndRepo(
	t *testing.T,
) {
	t.Parallel()

	provider := &mockInfraProvider{}
	baseRepo := &mockBaseRepo{}
	adapter := mustNewAdapter(t, provider, baseRepo)
	ctx := context.Background()
	transactionIDs := []uuid.UUID{uuid.New()}

	result, err := adapter.FindByContextAndIDs(ctx, uuid.Nil, transactionIDs)

	require.Error(t, err)
	assert.Nil(t, result)
	require.ErrorIs(t, err, ErrContextIDRequired)
}

// --- MarkUnmatchedWithTx tests ---

func TestTransactionRepositoryAdapter_MarkUnmatchedWithTx_NilAdapter(t *testing.T) {
	t.Parallel()

	var adapter *TransactionRepositoryAdapter

	ctx := context.Background()
	contextID := uuid.New()
	transactionIDs := []uuid.UUID{uuid.New()}

	err := adapter.MarkUnmatchedWithTx(ctx, nil, contextID, transactionIDs)

	require.Error(t, err)
	require.ErrorIs(t, err, ErrAdapterNotInitialized)
}

func TestTransactionRepositoryAdapter_MarkUnmatchedWithTx_NilBaseRepo(t *testing.T) {
	t.Parallel()

	adapter := &TransactionRepositoryAdapter{
		provider: nil,
		baseRepo: nil,
	}
	ctx := context.Background()
	contextID := uuid.New()
	transactionIDs := []uuid.UUID{uuid.New()}

	err := adapter.MarkUnmatchedWithTx(ctx, nil, contextID, transactionIDs)

	require.Error(t, err)
	require.ErrorIs(t, err, ErrAdapterNotInitialized)
}

func TestTransactionRepositoryAdapter_MarkUnmatchedWithTx_DelegatesToBaseRepo(t *testing.T) {
	t.Parallel()

	db, mock, err := sqlmock.New()
	require.NoError(t, err)
	defer db.Close()

	mock.ExpectBegin()
	tx, err := db.Begin()
	require.NoError(t, err)
	defer func() { _ = tx.Rollback() }()

	baseRepo := &mockBaseRepo{}
	adapter := mustNewAdapter(t, &mockInfraProvider{}, baseRepo)

	err = adapter.MarkUnmatchedWithTx(context.Background(), tx, uuid.New(), []uuid.UUID{uuid.New()})
	require.NoError(t, err)
	assert.True(t, baseRepo.markUnmatchedWithTxCalled)
	assert.Same(t, tx, baseRepo.markUnmatchedCapturedTx)
	require.NoError(t, mock.ExpectationsWereMet())
}

func TestTransactionRepositoryAdapter_MarkUnmatchedWithTx_EmptyWithProvider(t *testing.T) {
	t.Parallel()

	provider := &mockInfraProvider{}
	baseRepo := &mockBaseRepo{}
	adapter := mustNewAdapter(t, provider, baseRepo)
	ctx := context.Background()
	contextID := uuid.New()

	err := adapter.MarkUnmatchedWithTx(ctx, nil, contextID, []uuid.UUID{})

	require.NoError(t, err)
}

func TestTransactionRepositoryAdapter_MarkUnmatchedWithTx_NilContextIDWithProvider(t *testing.T) {
	t.Parallel()

	provider := &mockInfraProvider{}
	baseRepo := &mockBaseRepo{}
	adapter := mustNewAdapter(t, provider, baseRepo)
	ctx := context.Background()
	transactionIDs := []uuid.UUID{uuid.New()}

	err := adapter.MarkUnmatchedWithTx(ctx, nil, uuid.Nil, transactionIDs)

	require.Error(t, err)
	require.ErrorIs(t, err, ErrContextIDRequired)
}
