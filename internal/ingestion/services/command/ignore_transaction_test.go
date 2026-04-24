// Copyright 2025 Lerian Studio. All rights reserved.
// Use of this source code is governed by an Elastic License 2.0
// that can be found in the LICENSE.md file.

//go:build unit

package command

import (
	"context"
	"database/sql"
	"testing"

	"github.com/google/uuid"
	"github.com/shopspring/decimal"
	"github.com/stretchr/testify/require"

	shared "github.com/LerianStudio/matcher/internal/shared/domain"
)

type fakeIgnoreTxRepo struct {
	fakeTxRepo
	findByIDResult *shared.Transaction
	findByIDErr    error
	updateResult   *shared.Transaction
	updateErr      error
}

func (f *fakeIgnoreTxRepo) FindByID(_ context.Context, _ uuid.UUID) (*shared.Transaction, error) {
	return f.findByIDResult, f.findByIDErr
}

func (f *fakeIgnoreTxRepo) UpdateStatus(
	_ context.Context,
	_, _ uuid.UUID,
	status shared.TransactionStatus,
) (*shared.Transaction, error) {
	if f.updateResult != nil {
		f.updateResult.Status = status
	}

	return f.updateResult, f.updateErr
}

func TestIgnoreTransaction_NilUseCase(t *testing.T) {
	t.Parallel()

	var uc *UseCase

	_, err := uc.IgnoreTransaction(context.Background(), IgnoreTransactionInput{
		TransactionID: uuid.New(),
		ContextID:     uuid.New(),
		Reason:        "test reason",
	})
	require.ErrorIs(t, err, ErrNilUseCase)
}

func TestIgnoreTransaction_EmptyReason(t *testing.T) {
	t.Parallel()

	deps := newTestDeps()
	uc, err := NewUseCase(deps)
	require.NoError(t, err)

	_, err = uc.IgnoreTransaction(context.Background(), IgnoreTransactionInput{
		TransactionID: uuid.New(),
		ContextID:     uuid.New(),
		Reason:        "",
	})
	require.ErrorIs(t, err, ErrReasonRequired)
}

func TestIgnoreTransaction_WhitespaceOnlyReason(t *testing.T) {
	t.Parallel()

	deps := newTestDeps()
	uc, err := NewUseCase(deps)
	require.NoError(t, err)

	_, err = uc.IgnoreTransaction(context.Background(), IgnoreTransactionInput{
		TransactionID: uuid.New(),
		ContextID:     uuid.New(),
		Reason:        "   \t\n  ",
	})
	require.ErrorIs(t, err, ErrReasonRequired)
}

func TestIgnoreTransaction_TransactionNotFound(t *testing.T) {
	t.Parallel()

	deps := newTestDeps()
	deps.TransactionRepo = &fakeIgnoreTxRepo{
		findByIDResult: nil,
		findByIDErr:    sql.ErrNoRows,
	}

	uc, err := NewUseCase(deps)
	require.NoError(t, err)

	_, err = uc.IgnoreTransaction(context.Background(), IgnoreTransactionInput{
		TransactionID: uuid.New(),
		ContextID:     uuid.New(),
		Reason:        "duplicate entry",
	})
	require.ErrorIs(t, err, ErrTransactionNotFound)
}

func TestIgnoreTransaction_TransactionNilResult(t *testing.T) {
	t.Parallel()

	deps := newTestDeps()
	deps.TransactionRepo = &fakeIgnoreTxRepo{
		findByIDResult: nil,
		findByIDErr:    nil,
	}

	uc, err := NewUseCase(deps)
	require.NoError(t, err)

	_, err = uc.IgnoreTransaction(context.Background(), IgnoreTransactionInput{
		TransactionID: uuid.New(),
		ContextID:     uuid.New(),
		Reason:        "duplicate entry",
	})
	require.ErrorIs(t, err, ErrTransactionNotFound)
}

func TestIgnoreTransaction_TransactionFindError(t *testing.T) {
	t.Parallel()

	deps := newTestDeps()
	deps.TransactionRepo = &fakeIgnoreTxRepo{
		findByIDResult: nil,
		findByIDErr:    errTransactionFind,
	}

	uc, err := NewUseCase(deps)
	require.NoError(t, err)

	_, err = uc.IgnoreTransaction(context.Background(), IgnoreTransactionInput{
		TransactionID: uuid.New(),
		ContextID:     uuid.New(),
		Reason:        "duplicate entry",
	})
	require.Error(t, err)
	require.ErrorIs(t, err, errTransactionFind)
	require.Contains(t, err.Error(), "failed to find transaction")
}

func TestIgnoreTransaction_OnlyUnmatchedCanBeIgnored(t *testing.T) {
	t.Parallel()

	testCases := []struct {
		name   string
		status shared.TransactionStatus
	}{
		{"matched", shared.TransactionStatusMatched},
		{"ignored", shared.TransactionStatusIgnored},
		{"pending_review", shared.TransactionStatusPendingReview},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			txID := uuid.New()
			contextID := uuid.New()

			deps := newTestDeps()
			deps.TransactionRepo = &fakeIgnoreTxRepo{
				findByIDResult: &shared.Transaction{
					ID:     txID,
					Status: tc.status,
				},
				findByIDErr: nil,
			}

			uc, err := NewUseCase(deps)
			require.NoError(t, err)

			_, err = uc.IgnoreTransaction(context.Background(), IgnoreTransactionInput{
				TransactionID: txID,
				ContextID:     contextID,
				Reason:        "test reason",
			})
			require.ErrorIs(t, err, ErrTransactionNotIgnorable)
		})
	}
}

func TestIgnoreTransaction_UpdateError(t *testing.T) {
	t.Parallel()

	txID := uuid.New()
	contextID := uuid.New()

	deps := newTestDeps()
	deps.TransactionRepo = &fakeIgnoreTxRepo{
		findByIDResult: &shared.Transaction{
			ID:     txID,
			Status: shared.TransactionStatusUnmatched,
		},
		findByIDErr: nil,
		updateErr:   errTransactionUpdate,
	}

	uc, err := NewUseCase(deps)
	require.NoError(t, err)

	_, err = uc.IgnoreTransaction(context.Background(), IgnoreTransactionInput{
		TransactionID: txID,
		ContextID:     contextID,
		Reason:        "duplicate entry",
	})
	require.Error(t, err)
	require.ErrorIs(t, err, errTransactionUpdate)
	require.Contains(t, err.Error(), "failed to update transaction status")
}

func TestIgnoreTransaction_UpdateReturnsNoRows(t *testing.T) {
	t.Parallel()

	txID := uuid.New()
	contextID := uuid.New()

	deps := newTestDeps()
	deps.TransactionRepo = &fakeIgnoreTxRepo{
		findByIDResult: &shared.Transaction{
			ID:     txID,
			Status: shared.TransactionStatusUnmatched,
		},
		findByIDErr: nil,
		updateErr:   sql.ErrNoRows,
	}

	uc, err := NewUseCase(deps)
	require.NoError(t, err)

	_, err = uc.IgnoreTransaction(context.Background(), IgnoreTransactionInput{
		TransactionID: txID,
		ContextID:     contextID,
		Reason:        "duplicate entry",
	})
	require.ErrorIs(t, err, ErrTransactionNotFound)
}

func TestIgnoreTransaction_Success(t *testing.T) {
	t.Parallel()

	txID := uuid.New()
	contextID := uuid.New()

	originalTx := &shared.Transaction{
		ID:       txID,
		Status:   shared.TransactionStatusUnmatched,
		Amount:   decimal.NewFromFloat(100.50),
		Currency: "USD",
	}

	deps := newTestDeps()
	deps.TransactionRepo = &fakeIgnoreTxRepo{
		findByIDResult: originalTx,
		findByIDErr:    nil,
		updateResult: &shared.Transaction{
			ID:       txID,
			Status:   shared.TransactionStatusUnmatched,
			Amount:   decimal.NewFromFloat(100.50),
			Currency: "USD",
		},
		updateErr: nil,
	}

	uc, err := NewUseCase(deps)
	require.NoError(t, err)

	result, err := uc.IgnoreTransaction(context.Background(), IgnoreTransactionInput{
		TransactionID: txID,
		ContextID:     contextID,
		Reason:        "duplicate entry from legacy system",
	})
	require.NoError(t, err)
	require.NotNil(t, result)
	require.Equal(t, txID, result.ID)
	require.Equal(t, shared.TransactionStatusIgnored, result.Status)
}
