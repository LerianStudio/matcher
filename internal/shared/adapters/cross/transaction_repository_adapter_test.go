//go:build unit

package cross

import (
	"context"
	"testing"

	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	ingestionTxRepo "github.com/LerianStudio/matcher/internal/ingestion/adapters/postgres/transaction"
	matchingRepos "github.com/LerianStudio/matcher/internal/matching/domain/repositories"
)

// Behavioral coverage for TransactionRepositoryAdapter lives in
// tests/integration/matching/cross_context_transactions_test.go, which
// exercises real Postgres + tenant-scoped transactions. This unit-level file
// only covers constructor contracts and nil-receiver guards that need no
// backend to verify.

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

// --- Nil-receiver guards: verify that calling methods on a nil or
// partially-initialized adapter returns ErrAdapterNotInitialized rather than
// panicking. These are cheap safety nets; real behaviour lives in integration
// tests. ---

func TestTransactionRepositoryAdapter_ListUnmatchedByContext_NilAdapter(t *testing.T) {
	t.Parallel()

	var adapter *TransactionRepositoryAdapter

	_, err := adapter.ListUnmatchedByContext(context.Background(), uuid.New(), nil, nil, 100, 0)

	require.ErrorIs(t, err, ErrAdapterNotInitialized)
}

func TestTransactionRepositoryAdapter_FindByContextAndIDs_NilAdapter(t *testing.T) {
	t.Parallel()

	var adapter *TransactionRepositoryAdapter

	_, err := adapter.FindByContextAndIDs(context.Background(), uuid.New(), []uuid.UUID{uuid.New()})

	require.ErrorIs(t, err, ErrAdapterNotInitialized)
}

func TestTransactionRepositoryAdapter_MarkMatchedWithTx_NilAdapter(t *testing.T) {
	t.Parallel()

	var adapter *TransactionRepositoryAdapter

	err := adapter.MarkMatchedWithTx(context.Background(), nil, uuid.New(), []uuid.UUID{uuid.New()})

	require.ErrorIs(t, err, ErrAdapterNotInitialized)
}

func TestTransactionRepositoryAdapter_MarkPendingReviewWithTx_NilAdapter(t *testing.T) {
	t.Parallel()

	var adapter *TransactionRepositoryAdapter

	err := adapter.MarkPendingReviewWithTx(context.Background(), nil, uuid.New(), []uuid.UUID{uuid.New()})

	require.ErrorIs(t, err, ErrAdapterNotInitialized)
}

func TestTransactionRepositoryAdapter_MarkUnmatchedWithTx_NilAdapter(t *testing.T) {
	t.Parallel()

	var adapter *TransactionRepositoryAdapter

	err := adapter.MarkUnmatchedWithTx(context.Background(), nil, uuid.New(), []uuid.UUID{uuid.New()})

	require.ErrorIs(t, err, ErrAdapterNotInitialized)
}

func TestTransactionRepositoryAdapter_WithTx_NilAdapter(t *testing.T) {
	t.Parallel()

	var adapter *TransactionRepositoryAdapter

	err := adapter.WithTx(context.Background(), func(_ matchingRepos.Tx) error { return nil })

	require.ErrorIs(t, err, ErrAdapterNotInitialized)
}

// --- Nil argument guards: these do not require a backend because the guard
// runs before any call is delegated. ---

func TestTransactionRepositoryAdapter_MarkMatchedWithTx_NilContextID(t *testing.T) {
	t.Parallel()

	provider := &mockInfraProvider{}
	baseRepo := ingestionTxRepo.NewRepository(provider)
	adapter, err := NewTransactionRepositoryAdapterFromRepo(provider, baseRepo)
	require.NoError(t, err)

	err = adapter.MarkMatchedWithTx(context.Background(), nil, uuid.Nil, []uuid.UUID{uuid.New()})

	require.ErrorIs(t, err, ErrContextIDRequired)
}

func TestTransactionRepositoryAdapter_MarkPendingReviewWithTx_NilContextID(t *testing.T) {
	t.Parallel()

	provider := &mockInfraProvider{}
	baseRepo := ingestionTxRepo.NewRepository(provider)
	adapter, err := NewTransactionRepositoryAdapterFromRepo(provider, baseRepo)
	require.NoError(t, err)

	err = adapter.MarkPendingReviewWithTx(context.Background(), nil, uuid.Nil, []uuid.UUID{uuid.New()})

	require.ErrorIs(t, err, ErrContextIDRequired)
}

func TestTransactionRepositoryAdapter_MarkUnmatchedWithTx_NilContextID(t *testing.T) {
	t.Parallel()

	provider := &mockInfraProvider{}
	baseRepo := ingestionTxRepo.NewRepository(provider)
	adapter, err := NewTransactionRepositoryAdapterFromRepo(provider, baseRepo)
	require.NoError(t, err)

	err = adapter.MarkUnmatchedWithTx(context.Background(), nil, uuid.Nil, []uuid.UUID{uuid.New()})

	require.ErrorIs(t, err, ErrContextIDRequired)
}

func TestTransactionRepositoryAdapter_MarkMatchedWithTx_EmptyIDs(t *testing.T) {
	t.Parallel()

	provider := &mockInfraProvider{}
	baseRepo := ingestionTxRepo.NewRepository(provider)
	adapter, err := NewTransactionRepositoryAdapterFromRepo(provider, baseRepo)
	require.NoError(t, err)

	err = adapter.MarkMatchedWithTx(context.Background(), nil, uuid.New(), []uuid.UUID{})

	require.NoError(t, err)
}

func TestTransactionRepositoryAdapter_MarkPendingReviewWithTx_EmptyIDs(t *testing.T) {
	t.Parallel()

	provider := &mockInfraProvider{}
	baseRepo := ingestionTxRepo.NewRepository(provider)
	adapter, err := NewTransactionRepositoryAdapterFromRepo(provider, baseRepo)
	require.NoError(t, err)

	err = adapter.MarkPendingReviewWithTx(context.Background(), nil, uuid.New(), []uuid.UUID{})

	require.NoError(t, err)
}

func TestTransactionRepositoryAdapter_MarkUnmatchedWithTx_EmptyIDs(t *testing.T) {
	t.Parallel()

	provider := &mockInfraProvider{}
	baseRepo := ingestionTxRepo.NewRepository(provider)
	adapter, err := NewTransactionRepositoryAdapterFromRepo(provider, baseRepo)
	require.NoError(t, err)

	err = adapter.MarkUnmatchedWithTx(context.Background(), nil, uuid.New(), []uuid.UUID{})

	require.NoError(t, err)
}

// --- Nil-callback guard for WithTx ---

func TestTransactionRepositoryAdapter_WithTx_NilFn(t *testing.T) {
	t.Parallel()

	provider := &mockInfraProvider{}
	baseRepo := ingestionTxRepo.NewRepository(provider)
	adapter, err := NewTransactionRepositoryAdapterFromRepo(provider, baseRepo)
	require.NoError(t, err)

	err = adapter.WithTx(context.Background(), nil)

	require.NoError(t, err)
}
