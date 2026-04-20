//go:build unit

package query

import (
	"context"
	"errors"
	"testing"

	"github.com/google/uuid"
	"github.com/shopspring/decimal"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	libHTTP "github.com/LerianStudio/lib-commons/v5/commons/net/http"

	"github.com/LerianStudio/matcher/internal/ingestion/domain/entities"
	"github.com/LerianStudio/matcher/internal/ingestion/domain/repositories"
	shared "github.com/LerianStudio/matcher/internal/shared/domain"
	"github.com/LerianStudio/matcher/internal/shared/infrastructure/testutil"
)

// Compile-time interface satisfaction checks.
var (
	_ repositories.TransactionRepository = (*stubTransactionRepo)(nil)
	_ repositories.JobRepository         = (*stubJobRepo)(nil)
)

// stubTransactionRepo is a manual stub mock for TransactionRepository.
type stubTransactionRepo struct {
	searchTxs   []*shared.Transaction
	searchTotal int64
	searchErr   error
}

func (s *stubTransactionRepo) Create(
	_ context.Context,
	_ *shared.Transaction,
) (*shared.Transaction, error) {
	return nil, nil
}

func (s *stubTransactionRepo) CreateBatch(
	_ context.Context,
	_ []*shared.Transaction,
) ([]*shared.Transaction, error) {
	return nil, nil
}

func (s *stubTransactionRepo) FindByID(
	_ context.Context,
	_ uuid.UUID,
) (*shared.Transaction, error) {
	return nil, nil
}

func (s *stubTransactionRepo) FindByJobID(
	_ context.Context,
	_ uuid.UUID,
	_ repositories.CursorFilter,
) ([]*shared.Transaction, libHTTP.CursorPagination, error) {
	return nil, libHTTP.CursorPagination{}, nil
}

func (s *stubTransactionRepo) FindByJobAndContextID(
	_ context.Context,
	_, _ uuid.UUID,
	_ repositories.CursorFilter,
) ([]*shared.Transaction, libHTTP.CursorPagination, error) {
	return nil, libHTTP.CursorPagination{}, nil
}

func (s *stubTransactionRepo) FindBySourceAndExternalID(
	_ context.Context,
	_ uuid.UUID,
	_ string,
) (*shared.Transaction, error) {
	return nil, nil
}

func (s *stubTransactionRepo) ExistsBySourceAndExternalID(
	_ context.Context,
	_ uuid.UUID,
	_ string,
) (bool, error) {
	return false, nil
}

func (s *stubTransactionRepo) ExistsBulkBySourceAndExternalID(
	_ context.Context,
	_ []repositories.ExternalIDKey,
) (map[repositories.ExternalIDKey]bool, error) {
	return make(map[repositories.ExternalIDKey]bool), nil
}

func (s *stubTransactionRepo) UpdateStatus(
	_ context.Context,
	_, _ uuid.UUID,
	_ shared.TransactionStatus,
) (*shared.Transaction, error) {
	return nil, nil
}

func (s *stubTransactionRepo) SearchTransactions(
	_ context.Context,
	_ uuid.UUID,
	_ repositories.TransactionSearchParams,
) ([]*shared.Transaction, int64, error) {
	return s.searchTxs, s.searchTotal, s.searchErr
}

// stubJobRepo is a minimal stub mock for JobRepository.
type stubJobRepo struct{}

func (s *stubJobRepo) Create(
	_ context.Context,
	_ *entities.IngestionJob,
) (*entities.IngestionJob, error) {
	return nil, nil
}

func (s *stubJobRepo) FindByID(
	_ context.Context,
	_ uuid.UUID,
) (*entities.IngestionJob, error) {
	return nil, nil
}

func (s *stubJobRepo) FindByContextID(
	_ context.Context,
	_ uuid.UUID,
	_ repositories.CursorFilter,
) ([]*entities.IngestionJob, libHTTP.CursorPagination, error) {
	return nil, libHTTP.CursorPagination{}, nil
}

func (s *stubJobRepo) Update(
	_ context.Context,
	_ *entities.IngestionJob,
) (*entities.IngestionJob, error) {
	return nil, nil
}

func (s *stubJobRepo) FindLatestByExtractionID(
	_ context.Context,
	_ uuid.UUID,
) (*entities.IngestionJob, error) {
	return nil, nil
}

func TestSearchTransactions_NilUseCase(t *testing.T) {
	t.Parallel()

	var uc *UseCase

	txs, total, err := uc.SearchTransactions(
		context.Background(),
		uuid.New(),
		repositories.TransactionSearchParams{},
	)

	assert.ErrorIs(t, err, ErrNilUseCase)
	assert.Nil(t, txs)
	assert.Equal(t, int64(0), total)
}

func TestSearchTransactions_Success(t *testing.T) {
	t.Parallel()

	contextID := testutil.DeterministicUUID("search-context")
	expectedTxs := []*shared.Transaction{
		{
			ID:         testutil.DeterministicUUID("tx-1"),
			SourceID:   testutil.DeterministicUUID("source-1"),
			ExternalID: "ext-001",
			Amount:     decimal.NewFromFloat(100.50),
			Currency:   "USD",
		},
		{
			ID:         testutil.DeterministicUUID("tx-2"),
			SourceID:   testutil.DeterministicUUID("source-2"),
			ExternalID: "ext-002",
			Amount:     decimal.NewFromFloat(200.00),
			Currency:   "EUR",
		},
	}

	txRepo := &stubTransactionRepo{
		searchTxs:   expectedTxs,
		searchTotal: 2,
	}

	uc, err := NewUseCase(&stubJobRepo{}, txRepo)
	require.NoError(t, err)

	params := repositories.TransactionSearchParams{
		Query:    "ext",
		Currency: "USD",
		Limit:    20,
	}

	txs, total, err := uc.SearchTransactions(context.Background(), contextID, params)

	require.NoError(t, err)
	assert.Len(t, txs, 2)
	assert.Equal(t, int64(2), total)
	assert.Equal(t, expectedTxs[0].ID, txs[0].ID)
	assert.Equal(t, expectedTxs[1].ID, txs[1].ID)
}

func TestSearchTransactions_RepositoryError(t *testing.T) {
	t.Parallel()

	repoErr := errors.New("connection refused")

	txRepo := &stubTransactionRepo{
		searchErr: repoErr,
	}

	uc, err := NewUseCase(&stubJobRepo{}, txRepo)
	require.NoError(t, err)

	txs, total, err := uc.SearchTransactions(
		context.Background(),
		uuid.New(),
		repositories.TransactionSearchParams{Query: "test"},
	)

	require.Error(t, err)
	assert.ErrorIs(t, err, repoErr)
	assert.Contains(t, err.Error(), "searching transactions")
	assert.Nil(t, txs)
	assert.Equal(t, int64(0), total)
}

func TestSearchTransactions_EmptyResult(t *testing.T) {
	t.Parallel()

	txRepo := &stubTransactionRepo{
		searchTxs:   []*shared.Transaction{},
		searchTotal: 0,
	}

	uc, err := NewUseCase(&stubJobRepo{}, txRepo)
	require.NoError(t, err)

	txs, total, err := uc.SearchTransactions(
		context.Background(),
		uuid.New(),
		repositories.TransactionSearchParams{Query: "nonexistent"},
	)

	require.NoError(t, err)
	assert.Empty(t, txs)
	assert.Equal(t, int64(0), total)
}
