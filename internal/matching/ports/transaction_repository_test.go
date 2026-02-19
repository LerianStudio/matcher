//go:build unit

package ports_test

import (
	"context"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/shopspring/decimal"
	"github.com/stretchr/testify/require"
	"go.uber.org/mock/gomock"

	"github.com/LerianStudio/matcher/internal/matching/domain/repositories"
	"github.com/LerianStudio/matcher/internal/matching/ports"
	"github.com/LerianStudio/matcher/internal/matching/ports/mocks"
	shared "github.com/LerianStudio/matcher/internal/shared/domain"
)

func TestTransactionRepository_MockCreation(t *testing.T) {
	t.Parallel()

	ctrl := gomock.NewController(t)
	mock := mocks.NewMockTransactionRepository(ctrl)

	require.NotNil(t, mock)
	require.NotNil(t, mock.EXPECT())
}

func TestTransactionRepository_InterfaceCompliance(t *testing.T) {
	t.Parallel()

	ctrl := gomock.NewController(t)
	mock := mocks.NewMockTransactionRepository(ctrl)

	var _ ports.TransactionRepository = mock
}

func TestTransactionRepository_ListUnmatchedByContext(t *testing.T) {
	t.Parallel()

	ctrl := gomock.NewController(t)
	mock := mocks.NewMockTransactionRepository(ctrl)

	ctx := context.Background()
	contextID := uuid.New()
	start := time.Now().Add(-24 * time.Hour)
	end := time.Now()
	limit := 100
	offset := 0

	tx1, err := shared.NewTransaction(
		uuid.New(),
		uuid.New(),
		"ext-1",
		decimal.NewFromInt(100),
		"USD",
		time.Now(),
		"payment",
		nil,
	)
	require.NoError(t, err)
	tx2, err := shared.NewTransaction(
		uuid.New(),
		uuid.New(),
		"ext-2",
		decimal.NewFromInt(200),
		"EUR",
		time.Now(),
		"refund",
		nil,
	)
	require.NoError(t, err)
	expectedTxs := []*shared.Transaction{tx1, tx2}

	mock.EXPECT().
		ListUnmatchedByContext(ctx, contextID, &start, &end, limit, offset).
		Return(expectedTxs, nil)

	txs, err := mock.ListUnmatchedByContext(ctx, contextID, &start, &end, limit, offset)
	require.NoError(t, err)
	require.Len(t, txs, 2)
	require.Equal(t, "ext-1", txs[0].ExternalID)
	require.Equal(t, "ext-2", txs[1].ExternalID)
}

func TestTransactionRepository_ListUnmatchedByContext_NilDates(t *testing.T) {
	t.Parallel()

	ctrl := gomock.NewController(t)
	mock := mocks.NewMockTransactionRepository(ctrl)

	ctx := context.Background()
	contextID := uuid.New()
	limit := 50
	offset := 10

	mock.EXPECT().
		ListUnmatchedByContext(ctx, contextID, nil, nil, limit, offset).
		Return([]*shared.Transaction{}, nil)

	txs, err := mock.ListUnmatchedByContext(ctx, contextID, nil, nil, limit, offset)
	require.NoError(t, err)
	require.Empty(t, txs)
}

func TestTransactionRepository_ListUnmatchedByContext_Error(t *testing.T) {
	t.Parallel()

	ctrl := gomock.NewController(t)
	mock := mocks.NewMockTransactionRepository(ctrl)

	ctx := context.Background()
	contextID := uuid.New()

	mock.EXPECT().
		ListUnmatchedByContext(ctx, contextID, nil, nil, 100, 0).
		Return(nil, context.DeadlineExceeded)

	txs, err := mock.ListUnmatchedByContext(ctx, contextID, nil, nil, 100, 0)
	require.ErrorIs(t, err, context.DeadlineExceeded)
	require.Nil(t, txs)
}

func TestTransactionRepository_MarkMatched(t *testing.T) {
	t.Parallel()

	ctrl := gomock.NewController(t)
	mock := mocks.NewMockTransactionRepository(ctrl)

	ctx := context.Background()
	contextID := uuid.New()
	transactionIDs := []uuid.UUID{uuid.New(), uuid.New()}

	mock.EXPECT().
		MarkMatched(ctx, contextID, transactionIDs).
		Return(nil)

	err := mock.MarkMatched(ctx, contextID, transactionIDs)
	require.NoError(t, err)
}

func TestTransactionRepository_MarkMatched_Error(t *testing.T) {
	t.Parallel()

	ctrl := gomock.NewController(t)
	mock := mocks.NewMockTransactionRepository(ctrl)

	ctx := context.Background()
	contextID := uuid.New()
	transactionIDs := []uuid.UUID{uuid.New()}

	mock.EXPECT().
		MarkMatched(ctx, contextID, transactionIDs).
		Return(context.DeadlineExceeded)

	err := mock.MarkMatched(ctx, contextID, transactionIDs)
	require.ErrorIs(t, err, context.DeadlineExceeded)
}

func TestTransactionRepository_MarkMatchedWithTx(t *testing.T) {
	t.Parallel()

	ctrl := gomock.NewController(t)
	mock := mocks.NewMockTransactionRepository(ctrl)

	ctx := context.Background()
	contextID := uuid.New()
	transactionIDs := []uuid.UUID{uuid.New()}

	mock.EXPECT().
		MarkMatchedWithTx(ctx, nil, contextID, transactionIDs).
		Return(nil)

	err := mock.MarkMatchedWithTx(ctx, nil, contextID, transactionIDs)
	require.NoError(t, err)
}

func TestTransactionRepository_MarkPendingReview(t *testing.T) {
	t.Parallel()

	ctrl := gomock.NewController(t)
	mock := mocks.NewMockTransactionRepository(ctrl)

	ctx := context.Background()
	contextID := uuid.New()
	transactionIDs := []uuid.UUID{uuid.New(), uuid.New(), uuid.New()}

	mock.EXPECT().
		MarkPendingReview(ctx, contextID, transactionIDs).
		Return(nil)

	err := mock.MarkPendingReview(ctx, contextID, transactionIDs)
	require.NoError(t, err)
}

func TestTransactionRepository_MarkPendingReview_Error(t *testing.T) {
	t.Parallel()

	ctrl := gomock.NewController(t)
	mock := mocks.NewMockTransactionRepository(ctrl)

	ctx := context.Background()
	contextID := uuid.New()
	transactionIDs := []uuid.UUID{uuid.New()}

	mock.EXPECT().
		MarkPendingReview(ctx, contextID, transactionIDs).
		Return(context.Canceled)

	err := mock.MarkPendingReview(ctx, contextID, transactionIDs)
	require.ErrorIs(t, err, context.Canceled)
}

func TestTransactionRepository_MarkPendingReviewWithTx(t *testing.T) {
	t.Parallel()

	ctrl := gomock.NewController(t)
	mock := mocks.NewMockTransactionRepository(ctrl)

	ctx := context.Background()
	contextID := uuid.New()
	transactionIDs := []uuid.UUID{uuid.New()}

	mock.EXPECT().
		MarkPendingReviewWithTx(ctx, nil, contextID, transactionIDs).
		Return(nil)

	err := mock.MarkPendingReviewWithTx(ctx, nil, contextID, transactionIDs)
	require.NoError(t, err)
}

func TestTransactionRepository_WithTx(t *testing.T) {
	t.Parallel()

	ctrl := gomock.NewController(t)
	mock := mocks.NewMockTransactionRepository(ctrl)

	ctx := context.Background()
	called := false

	mock.EXPECT().
		WithTx(ctx, gomock.Any()).
		DoAndReturn(func(_ context.Context, fn func(repositories.Tx) error) error {
			called = true
			return fn(nil)
		})

	err := mock.WithTx(ctx, func(_ repositories.Tx) error {
		return nil
	})
	require.NoError(t, err)
	require.True(t, called)
}

func TestTransactionRepository_WithTx_Error(t *testing.T) {
	t.Parallel()

	ctrl := gomock.NewController(t)
	mock := mocks.NewMockTransactionRepository(ctrl)

	ctx := context.Background()

	mock.EXPECT().
		WithTx(ctx, gomock.Any()).
		Return(context.DeadlineExceeded)

	err := mock.WithTx(ctx, func(_ repositories.Tx) error {
		return nil
	})
	require.ErrorIs(t, err, context.DeadlineExceeded)
}
