// Copyright 2025 Lerian Studio. All rights reserved.
// Use of this source code is governed by an Elastic License 2.0
// that can be found in the LICENSE.md file.

//go:build unit

package command

import (
	"context"
	"errors"
	"testing"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/shopspring/decimal"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/LerianStudio/matcher/internal/shared/domain/fee"
)

type feeScheduleRepoStub struct {
	createFn   func(context.Context, *fee.FeeSchedule) (*fee.FeeSchedule, error)
	getByIDFn  func(context.Context, uuid.UUID) (*fee.FeeSchedule, error)
	updateFn   func(context.Context, *fee.FeeSchedule) (*fee.FeeSchedule, error)
	deleteFn   func(context.Context, uuid.UUID) error
	listFn     func(context.Context, int) ([]*fee.FeeSchedule, error)
	getByIDsFn func(context.Context, []uuid.UUID) (map[uuid.UUID]*fee.FeeSchedule, error)
}

func (stub *feeScheduleRepoStub) Create(ctx context.Context, s *fee.FeeSchedule) (*fee.FeeSchedule, error) {
	if stub.createFn != nil {
		return stub.createFn(ctx, s)
	}

	return nil, errCreateNotImplemented
}

func (stub *feeScheduleRepoStub) GetByID(ctx context.Context, id uuid.UUID) (*fee.FeeSchedule, error) {
	if stub.getByIDFn != nil {
		return stub.getByIDFn(ctx, id)
	}

	return nil, errFindByIDNotImplemented
}

func (stub *feeScheduleRepoStub) Update(ctx context.Context, s *fee.FeeSchedule) (*fee.FeeSchedule, error) {
	if stub.updateFn != nil {
		return stub.updateFn(ctx, s)
	}

	return nil, errUpdateNotImplemented
}

func (stub *feeScheduleRepoStub) Delete(ctx context.Context, id uuid.UUID) error {
	if stub.deleteFn != nil {
		return stub.deleteFn(ctx, id)
	}

	return errDeleteNotImplemented
}

func (stub *feeScheduleRepoStub) List(ctx context.Context, limit int) ([]*fee.FeeSchedule, error) {
	if stub.listFn != nil {
		return stub.listFn(ctx, limit)
	}

	return nil, errFindAllNotImplemented
}

func (stub *feeScheduleRepoStub) GetByIDs(ctx context.Context, ids []uuid.UUID) (map[uuid.UUID]*fee.FeeSchedule, error) {
	if stub.getByIDsFn != nil {
		return stub.getByIDsFn(ctx, ids)
	}

	return nil, errFindAllNotImplemented
}

func newUseCaseWithFeeSchedule(t *testing.T, feeRepo *feeScheduleRepoStub) *UseCase {
	t.Helper()

	uc, err := NewUseCase(
		&contextRepoStub{},
		&sourceRepoStub{},
		&fieldMapRepoStub{},
		&matchRuleRepoStub{},
		WithFeeScheduleRepository(feeRepo),
	)
	require.NoError(t, err)

	return uc
}

func TestCreateFeeSchedule_Success(t *testing.T) {
	t.Parallel()

	repo := &feeScheduleRepoStub{
		createFn: func(_ context.Context, s *fee.FeeSchedule) (*fee.FeeSchedule, error) {
			return s, nil
		},
	}

	uc := newUseCaseWithFeeSchedule(t, repo)

	result, err := uc.CreateFeeSchedule(
		context.Background(),
		uuid.New(),
		"Test Schedule",
		"USD",
		"PARALLEL",
		2,
		"HALF_UP",
		[]fee.FeeScheduleItemInput{
			{
				Name:      "interchange",
				Priority:  1,
				Structure: fee.FlatFee{Amount: decimal.NewFromFloat(1.50)},
			},
		},
	)

	require.NoError(t, err)
	assert.Equal(t, "Test Schedule", result.Name)
	assert.Equal(t, "USD", result.Currency)
}

func TestCreateFeeSchedule_NilRepo(t *testing.T) {
	t.Parallel()

	uc, err := NewUseCase(
		&contextRepoStub{},
		&sourceRepoStub{},
		&fieldMapRepoStub{},
		&matchRuleRepoStub{},
	)
	require.NoError(t, err)

	_, err = uc.CreateFeeSchedule(
		context.Background(),
		uuid.New(),
		"Test",
		"USD",
		"PARALLEL",
		2,
		"HALF_UP",
		[]fee.FeeScheduleItemInput{
			{Name: "fee", Priority: 1, Structure: fee.FlatFee{Amount: decimal.NewFromInt(1)}},
		},
	)

	require.ErrorIs(t, err, ErrNilFeeScheduleRepository)
}

func TestCreateFeeSchedule_InvalidInput(t *testing.T) {
	t.Parallel()

	repo := &feeScheduleRepoStub{}
	uc := newUseCaseWithFeeSchedule(t, repo)

	_, err := uc.CreateFeeSchedule(
		context.Background(),
		uuid.New(),
		"",
		"USD",
		"PARALLEL",
		2,
		"HALF_UP",
		[]fee.FeeScheduleItemInput{
			{Name: "fee", Priority: 1, Structure: fee.FlatFee{Amount: decimal.NewFromInt(1)}},
		},
	)

	require.Error(t, err)
	require.ErrorIs(t, err, fee.ErrScheduleNameRequired)
}

func TestCreateFeeSchedule_RepoError(t *testing.T) {
	t.Parallel()

	repoErr := errors.New("db error")
	repo := &feeScheduleRepoStub{
		createFn: func(_ context.Context, _ *fee.FeeSchedule) (*fee.FeeSchedule, error) {
			return nil, repoErr
		},
	}

	uc := newUseCaseWithFeeSchedule(t, repo)

	_, err := uc.CreateFeeSchedule(
		context.Background(),
		uuid.New(),
		"Test",
		"USD",
		"PARALLEL",
		2,
		"HALF_UP",
		[]fee.FeeScheduleItemInput{
			{Name: "fee", Priority: 1, Structure: fee.FlatFee{Amount: decimal.NewFromInt(1)}},
		},
	)

	require.Error(t, err)
	require.ErrorIs(t, err, repoErr)
}

func TestUpdateFeeSchedule_Success(t *testing.T) {
	t.Parallel()

	scheduleID := uuid.New()
	existing := &fee.FeeSchedule{
		ID:               scheduleID,
		Name:             "Original",
		ApplicationOrder: fee.ApplicationOrderParallel,
		RoundingScale:    2,
		RoundingMode:     fee.RoundingModeHalfUp,
	}

	repo := &feeScheduleRepoStub{
		getByIDFn: func(_ context.Context, id uuid.UUID) (*fee.FeeSchedule, error) {
			if id == scheduleID {
				return existing, nil
			}

			return nil, errors.New("not found")
		},
		updateFn: func(_ context.Context, s *fee.FeeSchedule) (*fee.FeeSchedule, error) {
			return s, nil
		},
	}

	uc := newUseCaseWithFeeSchedule(t, repo)

	newName := "Updated"
	result, err := uc.UpdateFeeSchedule(context.Background(), scheduleID, &newName, nil, nil, nil)

	require.NoError(t, err)
	assert.Equal(t, "Updated", result.Name)
}

func TestUpdateFeeSchedule_NilRepo(t *testing.T) {
	t.Parallel()

	uc, err := NewUseCase(
		&contextRepoStub{},
		&sourceRepoStub{},
		&fieldMapRepoStub{},
		&matchRuleRepoStub{},
	)
	require.NoError(t, err)

	name := "test"
	_, err = uc.UpdateFeeSchedule(context.Background(), uuid.New(), &name, nil, nil, nil)
	require.ErrorIs(t, err, ErrNilFeeScheduleRepository)
}

func TestUpdateFeeSchedule_NotFound(t *testing.T) {
	t.Parallel()

	findErr := errors.New("not found")
	repo := &feeScheduleRepoStub{
		getByIDFn: func(_ context.Context, _ uuid.UUID) (*fee.FeeSchedule, error) {
			return nil, findErr
		},
	}

	uc := newUseCaseWithFeeSchedule(t, repo)

	name := "test"
	_, err := uc.UpdateFeeSchedule(context.Background(), uuid.New(), &name, nil, nil, nil)

	require.Error(t, err)
	require.ErrorIs(t, err, findErr)
}

func TestUpdateFeeSchedule_EmptyName(t *testing.T) {
	t.Parallel()

	repo := &feeScheduleRepoStub{
		getByIDFn: func(_ context.Context, _ uuid.UUID) (*fee.FeeSchedule, error) {
			return &fee.FeeSchedule{Name: "Original"}, nil
		},
	}

	uc := newUseCaseWithFeeSchedule(t, repo)

	emptyName := "   "
	_, err := uc.UpdateFeeSchedule(context.Background(), uuid.New(), &emptyName, nil, nil, nil)

	require.ErrorIs(t, err, fee.ErrScheduleNameRequired)
}

func TestDeleteFeeSchedule_Success(t *testing.T) {
	t.Parallel()

	scheduleID := uuid.New()
	repo := &feeScheduleRepoStub{
		getByIDFn: func(_ context.Context, id uuid.UUID) (*fee.FeeSchedule, error) {
			return &fee.FeeSchedule{ID: id}, nil
		},
		deleteFn: func(_ context.Context, _ uuid.UUID) error {
			return nil
		},
	}

	uc := newUseCaseWithFeeSchedule(t, repo)

	err := uc.DeleteFeeSchedule(context.Background(), scheduleID)
	require.NoError(t, err)
}

func TestDeleteFeeSchedule_NilRepo(t *testing.T) {
	t.Parallel()

	uc, err := NewUseCase(
		&contextRepoStub{},
		&sourceRepoStub{},
		&fieldMapRepoStub{},
		&matchRuleRepoStub{},
	)
	require.NoError(t, err)

	err = uc.DeleteFeeSchedule(context.Background(), uuid.New())
	require.ErrorIs(t, err, ErrNilFeeScheduleRepository)
}

func TestDeleteFeeSchedule_NotFound(t *testing.T) {
	t.Parallel()

	findErr := errors.New("not found")
	repo := &feeScheduleRepoStub{
		getByIDFn: func(_ context.Context, _ uuid.UUID) (*fee.FeeSchedule, error) {
			return nil, findErr
		},
	}

	uc := newUseCaseWithFeeSchedule(t, repo)

	err := uc.DeleteFeeSchedule(context.Background(), uuid.New())
	require.Error(t, err)
	require.ErrorIs(t, err, findErr)
}

func TestDeleteFeeSchedule_ReferencedByFeeRule(t *testing.T) {
	t.Parallel()

	repo := &feeScheduleRepoStub{
		getByIDFn: func(_ context.Context, id uuid.UUID) (*fee.FeeSchedule, error) {
			return &fee.FeeSchedule{ID: id}, nil
		},
		deleteFn: func(_ context.Context, _ uuid.UUID) error {
			return &pgconn.PgError{Code: "23503", ConstraintName: constraintFeeRuleSchedule}
		},
	}

	uc := newUseCaseWithFeeSchedule(t, repo)

	err := uc.DeleteFeeSchedule(context.Background(), uuid.New())
	require.ErrorIs(t, err, ErrFeeScheduleReferencedByFeeRule)
}

func TestDeleteFeeSchedule_ReferencedByVarianceHistory(t *testing.T) {
	t.Parallel()

	repo := &feeScheduleRepoStub{
		getByIDFn: func(_ context.Context, id uuid.UUID) (*fee.FeeSchedule, error) {
			return &fee.FeeSchedule{ID: id}, nil
		},
		deleteFn: func(_ context.Context, _ uuid.UUID) error {
			return &pgconn.PgError{Code: "23503", ConstraintName: constraintFeeVarianceSchedule}
		},
	}

	uc := newUseCaseWithFeeSchedule(t, repo)

	err := uc.DeleteFeeSchedule(context.Background(), uuid.New())
	require.ErrorIs(t, err, ErrFeeScheduleReferencedByVarianceHistory)
}

func TestParseFeeStructureFromRequest_Flat(t *testing.T) {
	t.Parallel()

	structure, err := ParseFeeStructureFromRequest("FLAT", map[string]any{
		"amount": "5.00",
	})

	require.NoError(t, err)
	assert.Equal(t, fee.FeeStructureFlat, structure.Type())
}

func TestParseFeeStructureFromRequest_Percentage(t *testing.T) {
	t.Parallel()

	structure, err := ParseFeeStructureFromRequest("PERCENTAGE", map[string]any{
		"rate": "0.015",
	})

	require.NoError(t, err)
	assert.Equal(t, fee.FeeStructurePercentage, structure.Type())
}

func TestParseFeeStructureFromRequest_Tiered(t *testing.T) {
	t.Parallel()

	structure, err := ParseFeeStructureFromRequest("TIERED", map[string]any{
		"tiers": []any{
			map[string]any{"upTo": "100", "rate": "0.01"},
			map[string]any{"rate": "0.005"},
		},
	})

	require.NoError(t, err)
	assert.Equal(t, fee.FeeStructureTiered, structure.Type())
}

func TestParseFeeStructureFromRequest_UnknownType(t *testing.T) {
	t.Parallel()

	_, err := ParseFeeStructureFromRequest("UNKNOWN", map[string]any{})
	require.Error(t, err)
}

func TestUpdateFeeSchedule_InvalidApplicationOrder(t *testing.T) {
	t.Parallel()

	repo := &feeScheduleRepoStub{
		getByIDFn: func(_ context.Context, _ uuid.UUID) (*fee.FeeSchedule, error) {
			return &fee.FeeSchedule{
				Name:             "Original",
				ApplicationOrder: fee.ApplicationOrderParallel,
				RoundingScale:    2,
				RoundingMode:     fee.RoundingModeHalfUp,
			}, nil
		},
	}

	uc := newUseCaseWithFeeSchedule(t, repo)

	invalidOrder := "INVALID_ORDER"
	_, err := uc.UpdateFeeSchedule(context.Background(), uuid.New(), nil, &invalidOrder, nil, nil)
	require.ErrorIs(t, err, fee.ErrInvalidApplicationOrder)
}

func TestUpdateFeeSchedule_InvalidRoundingScale(t *testing.T) {
	t.Parallel()

	repo := &feeScheduleRepoStub{
		getByIDFn: func(_ context.Context, _ uuid.UUID) (*fee.FeeSchedule, error) {
			return &fee.FeeSchedule{
				Name:             "Original",
				ApplicationOrder: fee.ApplicationOrderParallel,
				RoundingScale:    2,
				RoundingMode:     fee.RoundingModeHalfUp,
			}, nil
		},
	}

	uc := newUseCaseWithFeeSchedule(t, repo)

	t.Run("negative scale", func(t *testing.T) {
		t.Parallel()

		negScale := -1
		_, err := uc.UpdateFeeSchedule(context.Background(), uuid.New(), nil, nil, &negScale, nil)
		require.ErrorIs(t, err, fee.ErrInvalidRoundingScale)
	})

	t.Run("scale too large", func(t *testing.T) {
		t.Parallel()

		largeScale := 11
		_, err := uc.UpdateFeeSchedule(context.Background(), uuid.New(), nil, nil, &largeScale, nil)
		require.ErrorIs(t, err, fee.ErrInvalidRoundingScale)
	})
}

func TestUpdateFeeSchedule_InvalidRoundingMode(t *testing.T) {
	t.Parallel()

	repo := &feeScheduleRepoStub{
		getByIDFn: func(_ context.Context, _ uuid.UUID) (*fee.FeeSchedule, error) {
			return &fee.FeeSchedule{
				Name:             "Original",
				ApplicationOrder: fee.ApplicationOrderParallel,
				RoundingScale:    2,
				RoundingMode:     fee.RoundingModeHalfUp,
			}, nil
		},
	}

	uc := newUseCaseWithFeeSchedule(t, repo)

	invalidMode := "INVALID_MODE"
	_, err := uc.UpdateFeeSchedule(context.Background(), uuid.New(), nil, nil, nil, &invalidMode)
	require.ErrorIs(t, err, fee.ErrInvalidRoundingMode)
}

func TestUpdateFeeSchedule_RepoUpdateError(t *testing.T) {
	t.Parallel()

	repoErr := errors.New("db update error")
	repo := &feeScheduleRepoStub{
		getByIDFn: func(_ context.Context, _ uuid.UUID) (*fee.FeeSchedule, error) {
			return &fee.FeeSchedule{
				Name:             "Original",
				ApplicationOrder: fee.ApplicationOrderParallel,
				RoundingScale:    2,
				RoundingMode:     fee.RoundingModeHalfUp,
			}, nil
		},
		updateFn: func(_ context.Context, _ *fee.FeeSchedule) (*fee.FeeSchedule, error) {
			return nil, repoErr
		},
	}

	uc := newUseCaseWithFeeSchedule(t, repo)

	newName := "Updated"
	_, err := uc.UpdateFeeSchedule(context.Background(), uuid.New(), &newName, nil, nil, nil)
	require.Error(t, err)
	require.ErrorIs(t, err, repoErr)
}

func TestParseFeeStructureFromRequest_FlatNonStringAmount(t *testing.T) {
	t.Parallel()

	_, err := ParseFeeStructureFromRequest("FLAT", map[string]any{
		"amount": 5.0,
	})
	require.Error(t, err)
	require.ErrorIs(t, err, fee.ErrNilFeeStructure)
}

func TestParseFeeStructureFromRequest_FlatInvalidDecimal(t *testing.T) {
	t.Parallel()

	_, err := ParseFeeStructureFromRequest("FLAT", map[string]any{
		"amount": "abc",
	})
	require.Error(t, err)
}

func TestParseFeeStructureFromRequest_PercentageNonStringRate(t *testing.T) {
	t.Parallel()

	_, err := ParseFeeStructureFromRequest("PERCENTAGE", map[string]any{
		"rate": 0.015,
	})
	require.Error(t, err)
	require.ErrorIs(t, err, fee.ErrNilFeeStructure)
}

func TestParseFeeStructureFromRequest_PercentageInvalidDecimal(t *testing.T) {
	t.Parallel()

	_, err := ParseFeeStructureFromRequest("PERCENTAGE", map[string]any{
		"rate": "abc",
	})
	require.Error(t, err)
}

func TestParseFeeStructureFromRequest_TieredMissingTiers(t *testing.T) {
	t.Parallel()

	_, err := ParseFeeStructureFromRequest("TIERED", map[string]any{})
	require.Error(t, err)
	require.ErrorIs(t, err, fee.ErrInvalidTieredDefinition)
}

func TestParseFeeStructureFromRequest_TieredNonArrayTiers(t *testing.T) {
	t.Parallel()

	_, err := ParseFeeStructureFromRequest("TIERED", map[string]any{
		"tiers": "not-an-array",
	})
	require.Error(t, err)
	require.ErrorIs(t, err, fee.ErrInvalidTieredDefinition)
}
