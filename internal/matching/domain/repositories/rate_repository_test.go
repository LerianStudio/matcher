//go:build unit

package repositories

import (
	"context"
	"errors"
	"testing"

	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/LerianStudio/matcher/internal/shared/domain/fee"
)

var (
	errTestRateNotFound   = errors.New("rate not found")
	errTestInvalidNilUUID = errors.New("invalid id: nil UUID")
)

var _ RateRepository = (*MockRateRepository)(nil)

type MockRateRepository struct {
	GetByIDFunc func(ctx context.Context, id uuid.UUID) (*fee.Rate, error)
}

func (m *MockRateRepository) GetByID(ctx context.Context, id uuid.UUID) (*fee.Rate, error) {
	if m.GetByIDFunc != nil {
		return m.GetByIDFunc(ctx, id)
	}

	return nil, nil
}

func TestMockRateRepository_GetByID(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name          string
		setupMock     func() *MockRateRepository
		inputID       uuid.UUID
		expectedRate  *fee.Rate
		expectedError error
	}{
		{
			name: "returns rate successfully",
			setupMock: func() *MockRateRepository {
				rateID := uuid.MustParse("11111111-1111-1111-1111-111111111111")

				return &MockRateRepository{
					GetByIDFunc: func(_ context.Context, id uuid.UUID) (*fee.Rate, error) {
						if id == rateID {
							return &fee.Rate{
								ID:       rateID,
								Currency: "USD",
							}, nil
						}

						return nil, errTestRateNotFound
					},
				}
			},
			inputID: uuid.MustParse("11111111-1111-1111-1111-111111111111"),
			expectedRate: &fee.Rate{
				ID:       uuid.MustParse("11111111-1111-1111-1111-111111111111"),
				Currency: "USD",
			},
			expectedError: nil,
		},
		{
			name: "returns error when rate not found",
			setupMock: func() *MockRateRepository {
				return &MockRateRepository{
					GetByIDFunc: func(_ context.Context, _ uuid.UUID) (*fee.Rate, error) {
						return nil, errTestRateNotFound
					},
				}
			},
			inputID:       uuid.MustParse("22222222-2222-2222-2222-222222222222"),
			expectedRate:  nil,
			expectedError: errTestRateNotFound,
		},
		{
			name: "handles nil UUID",
			setupMock: func() *MockRateRepository {
				return &MockRateRepository{
					GetByIDFunc: func(_ context.Context, id uuid.UUID) (*fee.Rate, error) {
						if id == uuid.Nil {
							return nil, errTestInvalidNilUUID
						}

						return &fee.Rate{ID: id}, nil
					},
				}
			},
			inputID:       uuid.Nil,
			expectedRate:  nil,
			expectedError: errTestInvalidNilUUID,
		},
		{
			name: "default behavior returns nil",
			setupMock: func() *MockRateRepository {
				return &MockRateRepository{}
			},
			inputID:       uuid.New(),
			expectedRate:  nil,
			expectedError: nil,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			mock := tt.setupMock()
			ctx := context.Background()

			rate, err := mock.GetByID(ctx, tt.inputID)

			if tt.expectedError != nil {
				require.Error(t, err)
				assert.Equal(t, tt.expectedError.Error(), err.Error())
			} else {
				require.NoError(t, err)
			}

			if tt.expectedRate != nil {
				require.NotNil(t, rate)
				assert.Equal(t, tt.expectedRate.ID, rate.ID)
				assert.Equal(t, tt.expectedRate.Currency, rate.Currency)
			} else {
				assert.Nil(t, rate)
			}
		})
	}
}

func TestMockRateRepository_ContextCancellation(t *testing.T) {
	t.Parallel()

	mock := &MockRateRepository{
		GetByIDFunc: func(ctx context.Context, _ uuid.UUID) (*fee.Rate, error) {
			select {
			case <-ctx.Done():
				return nil, ctx.Err()
			default:
				return &fee.Rate{ID: uuid.New()}, nil
			}
		},
	}

	t.Run("respects context cancellation", func(t *testing.T) {
		t.Parallel()

		ctx, cancel := context.WithCancel(context.Background())
		cancel()

		rate, err := mock.GetByID(ctx, uuid.New())

		require.Error(t, err)
		require.ErrorIs(t, err, context.Canceled)
		assert.Nil(t, rate)
	})

	t.Run("works with active context", func(t *testing.T) {
		t.Parallel()

		ctx := context.Background()

		rate, err := mock.GetByID(ctx, uuid.New())

		require.NoError(t, err)
		require.NotNil(t, rate)
	})
}

func TestRateRepository_InterfaceCompliance(t *testing.T) {
	t.Parallel()

	t.Run("mock implements interface", func(t *testing.T) {
		t.Parallel()

		var repo RateRepository = &MockRateRepository{}
		assert.NotNil(t, repo)
	})

	t.Run("interface has GetByID method", func(t *testing.T) {
		t.Parallel()

		mock := &MockRateRepository{
			GetByIDFunc: func(_ context.Context, id uuid.UUID) (*fee.Rate, error) {
				return &fee.Rate{ID: id}, nil
			},
		}

		var repo RateRepository = mock

		rate, err := repo.GetByID(context.Background(), uuid.New())

		require.NoError(t, err)
		require.NotNil(t, rate)
	})
}
