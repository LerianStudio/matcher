// Copyright 2025 Lerian Studio. All rights reserved.
// Use of this source code is governed by an Elastic License 2.0
// that can be found in the LICENSE.md file.

//go:build unit

package repositories

import (
	"context"
	"database/sql"
	"errors"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/shopspring/decimal"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/LerianStudio/matcher/internal/matching/domain/entities"
)

var (
	errTestDBConnectionFailed = errors.New("database connection failed")
	errTestRowsCannotBeNil    = errors.New("rows cannot be nil")
	errTestTransactionReq     = errors.New("transaction is required")
)

var _ FeeVarianceRepository = (*MockFeeVarianceRepository)(nil)

type MockFeeVarianceRepository struct {
	CreateBatchWithTxFunc func(ctx context.Context, tx Tx, rows []*entities.FeeVariance) ([]*entities.FeeVariance, error)
}

func (m *MockFeeVarianceRepository) CreateBatchWithTx(
	ctx context.Context,
	tx Tx,
	rows []*entities.FeeVariance,
) ([]*entities.FeeVariance, error) {
	if m.CreateBatchWithTxFunc != nil {
		return m.CreateBatchWithTxFunc(ctx, tx, rows)
	}

	return nil, nil
}

func createTestFeeVariance(id uuid.UUID) *entities.FeeVariance {
	return &entities.FeeVariance{
		ID:            id,
		ContextID:     uuid.New(),
		RunID:         uuid.New(),
		MatchGroupID:  uuid.New(),
		TransactionID: uuid.New(),
		FeeScheduleID: uuid.New(),
		Currency:      "USD",
		ExpectedFee:   decimal.NewFromFloat(10.00),
		ActualFee:     decimal.NewFromFloat(9.50),
		Delta:         decimal.NewFromFloat(0.50),
		ToleranceAbs:  decimal.NewFromFloat(1.00),
		TolerancePct:  decimal.NewFromFloat(0.05),
		VarianceType:  "within_tolerance",
		CreatedAt:     time.Now().UTC(),
		UpdatedAt:     time.Now().UTC(),
	}
}

func newSuccessfulMockFeeVarianceRepo() *MockFeeVarianceRepository {
	return &MockFeeVarianceRepository{
		CreateBatchWithTxFunc: func(_ context.Context, _ Tx, rows []*entities.FeeVariance) ([]*entities.FeeVariance, error) {
			return rows, nil
		},
	}
}

func newFailingMockFeeVarianceRepo(err error) *MockFeeVarianceRepository {
	return &MockFeeVarianceRepository{
		CreateBatchWithTxFunc: func(_ context.Context, _ Tx, _ []*entities.FeeVariance) ([]*entities.FeeVariance, error) {
			return nil, err
		},
	}
}

func newNilCheckingMockFeeVarianceRepo() *MockFeeVarianceRepository {
	return &MockFeeVarianceRepository{
		CreateBatchWithTxFunc: func(_ context.Context, _ Tx, rows []*entities.FeeVariance) ([]*entities.FeeVariance, error) {
			if rows == nil {
				return nil, errTestRowsCannotBeNil
			}

			return rows, nil
		},
	}
}

func createLargeFeeVarianceBatch(size int) []*entities.FeeVariance {
	batch := make([]*entities.FeeVariance, size)
	for i := 0; i < size; i++ {
		batch[i] = createTestFeeVariance(uuid.New())
	}

	return batch
}

func TestMockFeeVarianceRepository_CreateBatchWithTx(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name           string
		setupMock      func() *MockFeeVarianceRepository
		inputRows      []*entities.FeeVariance
		expectedCount  int
		expectedError  error
		validateResult func(t *testing.T, result []*entities.FeeVariance)
	}{
		{
			name:      "creates batch successfully",
			setupMock: newSuccessfulMockFeeVarianceRepo,
			inputRows: []*entities.FeeVariance{
				createTestFeeVariance(uuid.MustParse("11111111-1111-1111-1111-111111111111")),
				createTestFeeVariance(uuid.MustParse("22222222-2222-2222-2222-222222222222")),
			},
			expectedCount: 2,
			expectedError: nil,
			validateResult: func(t *testing.T, result []*entities.FeeVariance) {
				t.Helper()
				assert.Equal(
					t,
					uuid.MustParse("11111111-1111-1111-1111-111111111111"),
					result[0].ID,
				)
				assert.Equal(
					t,
					uuid.MustParse("22222222-2222-2222-2222-222222222222"),
					result[1].ID,
				)
			},
		},
		{
			name:           "returns error on database failure",
			setupMock:      func() *MockFeeVarianceRepository { return newFailingMockFeeVarianceRepo(errTestDBConnectionFailed) },
			inputRows:      []*entities.FeeVariance{createTestFeeVariance(uuid.New())},
			expectedCount:  0,
			expectedError:  errTestDBConnectionFailed,
			validateResult: nil,
		},
		{
			name:          "handles empty batch",
			setupMock:     newSuccessfulMockFeeVarianceRepo,
			inputRows:     []*entities.FeeVariance{},
			expectedCount: 0,
			expectedError: nil,
			validateResult: func(t *testing.T, result []*entities.FeeVariance) {
				t.Helper()
				assert.Empty(t, result)
			},
		},
		{
			name:           "handles nil batch",
			setupMock:      newNilCheckingMockFeeVarianceRepo,
			inputRows:      nil,
			expectedCount:  0,
			expectedError:  errTestRowsCannotBeNil,
			validateResult: nil,
		},
		{
			name:          "default behavior returns nil",
			setupMock:     func() *MockFeeVarianceRepository { return &MockFeeVarianceRepository{} },
			inputRows:     []*entities.FeeVariance{createTestFeeVariance(uuid.New())},
			expectedCount: 0,
			expectedError: nil,
			validateResult: func(t *testing.T, result []*entities.FeeVariance) {
				t.Helper()
				assert.Nil(t, result)
			},
		},
		{
			name:          "creates large batch",
			setupMock:     newSuccessfulMockFeeVarianceRepo,
			inputRows:     createLargeFeeVarianceBatch(100),
			expectedCount: 100,
			expectedError: nil,
			validateResult: func(t *testing.T, result []*entities.FeeVariance) {
				t.Helper()
				assert.Len(t, result, 100)
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			mock := tt.setupMock()
			ctx := context.Background()

			result, err := mock.CreateBatchWithTx(ctx, nil, tt.inputRows)

			if tt.expectedError != nil {
				require.Error(t, err)
				assert.Equal(t, tt.expectedError.Error(), err.Error())
			} else {
				require.NoError(t, err)
			}

			if tt.validateResult != nil {
				tt.validateResult(t, result)
			} else if tt.expectedCount > 0 {
				assert.Len(t, result, tt.expectedCount)
			}
		})
	}
}

func TestMockFeeVarianceRepository_ContextCancellation(t *testing.T) {
	t.Parallel()

	mock := &MockFeeVarianceRepository{
		CreateBatchWithTxFunc: func(ctx context.Context, _ Tx, rows []*entities.FeeVariance) ([]*entities.FeeVariance, error) {
			select {
			case <-ctx.Done():
				return nil, ctx.Err()
			default:
				return rows, nil
			}
		},
	}

	t.Run("respects context cancellation", func(t *testing.T) {
		t.Parallel()

		ctx, cancel := context.WithCancel(context.Background())
		cancel()

		result, err := mock.CreateBatchWithTx(
			ctx,
			nil,
			[]*entities.FeeVariance{createTestFeeVariance(uuid.New())},
		)

		require.Error(t, err)
		require.ErrorIs(t, err, context.Canceled)
		assert.Nil(t, result)
	})

	t.Run("works with active context", func(t *testing.T) {
		t.Parallel()

		ctx := context.Background()
		input := []*entities.FeeVariance{createTestFeeVariance(uuid.New())}

		result, err := mock.CreateBatchWithTx(ctx, nil, input)

		require.NoError(t, err)
		require.NotNil(t, result)
		assert.Len(t, result, 1)
	})
}

func TestMockFeeVarianceRepository_TransactionHandling(t *testing.T) {
	t.Parallel()

	t.Run("receives transaction parameter", func(t *testing.T) {
		t.Parallel()

		var receivedTx Tx

		mock := &MockFeeVarianceRepository{
			CreateBatchWithTxFunc: func(_ context.Context, tx Tx, rows []*entities.FeeVariance) ([]*entities.FeeVariance, error) {
				receivedTx = tx

				return rows, nil
			},
		}

		mockTx := new(sql.Tx)
		_, err := mock.CreateBatchWithTx(context.Background(), mockTx, []*entities.FeeVariance{})

		require.NoError(t, err)
		assert.Same(t, mockTx, receivedTx)
	})

	t.Run("handles nil transaction", func(t *testing.T) {
		t.Parallel()

		mock := &MockFeeVarianceRepository{
			CreateBatchWithTxFunc: func(_ context.Context, tx Tx, rows []*entities.FeeVariance) ([]*entities.FeeVariance, error) {
				if tx == nil {
					return nil, errTestTransactionReq
				}

				return rows, nil
			},
		}

		_, err := mock.CreateBatchWithTx(context.Background(), nil, []*entities.FeeVariance{})

		require.Error(t, err)
		assert.Equal(t, errTestTransactionReq.Error(), err.Error())
	})
}

func TestFeeVarianceRepository_InterfaceCompliance(t *testing.T) {
	t.Parallel()

	t.Run("mock implements interface", func(t *testing.T) {
		t.Parallel()

		var repo FeeVarianceRepository = &MockFeeVarianceRepository{}
		assert.NotNil(t, repo)
	})

	t.Run("interface has CreateBatchWithTx method", func(t *testing.T) {
		t.Parallel()

		mock := &MockFeeVarianceRepository{
			CreateBatchWithTxFunc: func(_ context.Context, _ Tx, rows []*entities.FeeVariance) ([]*entities.FeeVariance, error) {
				return rows, nil
			},
		}

		var repo FeeVarianceRepository = mock

		result, err := repo.CreateBatchWithTx(context.Background(), nil, []*entities.FeeVariance{})

		require.NoError(t, err)
		assert.Empty(t, result)
	})
}
