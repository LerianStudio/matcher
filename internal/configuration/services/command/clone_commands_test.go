//go:build unit

package command

import (
	"context"
	"database/sql"
	"testing"

	"github.com/google/uuid"
	"github.com/shopspring/decimal"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.uber.org/mock/gomock"

	libHTTP "github.com/LerianStudio/lib-commons/v4/commons/net/http"

	"github.com/LerianStudio/matcher/internal/configuration/domain/entities"
	"github.com/LerianStudio/matcher/internal/configuration/domain/repositories/mocks"
	"github.com/LerianStudio/matcher/internal/configuration/domain/value_objects"
	"github.com/LerianStudio/matcher/internal/shared/domain/fee"
)

func TestCloneContext_NilRepository(t *testing.T) {
	t.Parallel()

	uc := &UseCase{}
	_, err := uc.CloneContext(context.Background(), CloneContextInput{})
	assert.ErrorIs(t, err, ErrNilContextRepository)
}

func TestCloneContext_EmptyName(t *testing.T) {
	t.Parallel()

	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	mockCtxRepo := mocks.NewMockContextRepository(ctrl)

	uc, err := NewUseCase(
		mockCtxRepo,
		mocks.NewMockSourceRepository(ctrl),
		mocks.NewMockFieldMapRepository(ctrl),
		mocks.NewMockMatchRuleRepository(ctrl),
		WithFeeRuleRepository(newFeeRuleMockRepo()),
	)
	require.NoError(t, err)

	_, err = uc.CloneContext(context.Background(), CloneContextInput{
		SourceContextID: uuid.New(),
		NewName:         "",
	})

	assert.ErrorIs(t, err, ErrCloneNameRequired)
}

func TestCloneContext_SourceNotFound(t *testing.T) {
	t.Parallel()

	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	mockCtxRepo := mocks.NewMockContextRepository(ctrl)
	sourceCtxID := uuid.New()

	mockCtxRepo.EXPECT().
		FindByID(gomock.Any(), sourceCtxID).
		Return(nil, sql.ErrNoRows)

	uc, err := NewUseCase(
		mockCtxRepo,
		mocks.NewMockSourceRepository(ctrl),
		mocks.NewMockFieldMapRepository(ctrl),
		mocks.NewMockMatchRuleRepository(ctrl),
		WithFeeRuleRepository(newFeeRuleMockRepo()),
	)
	require.NoError(t, err)

	_, err = uc.CloneContext(context.Background(), CloneContextInput{
		SourceContextID: sourceCtxID,
		NewName:         "Cloned Context",
		IncludeSources:  true,
		IncludeRules:    true,
	})

	assert.ErrorIs(t, err, sql.ErrNoRows)
}

func TestCloneContext_ContextOnly(t *testing.T) {
	t.Parallel()

	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	mockCtxRepo := mocks.NewMockContextRepository(ctrl)
	sourceCtxID := uuid.New()

	sourceContext := &entities.ReconciliationContext{
		ID:              sourceCtxID,
		TenantID:        uuid.New(),
		Name:            "Original Context",
		Type:            value_objects.ContextType("1:1"),
		Interval:        "daily",
		Status:          value_objects.ContextStatusActive,
		FeeToleranceAbs: decimal.NewFromFloat(0.01),
		FeeTolerancePct: decimal.NewFromFloat(0.5),
	}

	mockCtxRepo.EXPECT().
		FindByID(gomock.Any(), sourceCtxID).
		Return(sourceContext, nil)

	mockCtxRepo.EXPECT().
		Create(gomock.Any(), gomock.Any()).
		DoAndReturn(func(_ context.Context, entity *entities.ReconciliationContext) (*entities.ReconciliationContext, error) {
			assert.Equal(t, "Cloned Context", entity.Name)
			assert.Equal(t, sourceContext.TenantID, entity.TenantID)
			assert.Equal(t, sourceContext.Type, entity.Type)
			assert.Equal(t, sourceContext.Interval, entity.Interval)
			assert.Equal(t, value_objects.ContextStatusActive, entity.Status)
			return entity, nil
		})

	uc, err := NewUseCase(
		mockCtxRepo,
		mocks.NewMockSourceRepository(ctrl),
		mocks.NewMockFieldMapRepository(ctrl),
		mocks.NewMockMatchRuleRepository(ctrl),
	)
	require.NoError(t, err)

	result, err := uc.CloneContext(context.Background(), CloneContextInput{
		SourceContextID: sourceCtxID,
		NewName:         "Cloned Context",
		IncludeSources:  false,
		IncludeRules:    false,
	})

	require.NoError(t, err)
	assert.NotNil(t, result)
	assert.Equal(t, "Cloned Context", result.Context.Name)
	assert.Equal(t, 0, result.SourcesCloned)
	assert.Equal(t, 0, result.RulesCloned)
	assert.Equal(t, 0, result.FieldMapsCloned)
}

func TestCloneContext_WithSourcesAndRules(t *testing.T) {
	t.Parallel()

	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	mockCtxRepo := mocks.NewMockContextRepository(ctrl)
	mockSrcRepo := mocks.NewMockSourceRepository(ctrl)
	mockFMRepo := mocks.NewMockFieldMapRepository(ctrl)
	mockRuleRepo := mocks.NewMockMatchRuleRepository(ctrl)
	feeRuleRepo := newFeeRuleMockRepo()

	sourceCtxID := uuid.New()
	tenantID := uuid.New()
	sourceID1 := uuid.New()
	sourceID2 := uuid.New()

	sourceContext := &entities.ReconciliationContext{
		ID:       sourceCtxID,
		TenantID: tenantID,
		Name:     "Original",
		Type:     value_objects.ContextType("1:1"),
		Interval: "daily",
		Status:   value_objects.ContextStatusActive,
	}

	sources := []*entities.ReconciliationSource{
		{ID: sourceID1, ContextID: sourceCtxID, Name: "Source A", Type: value_objects.SourceType("BANK"), Config: map[string]any{"key": "val"}},
		{ID: sourceID2, ContextID: sourceCtxID, Name: "Source B", Type: value_objects.SourceType("LEDGER")},
	}

	rules := entities.MatchRules{
		{ID: uuid.New(), ContextID: sourceCtxID, Priority: 1, Type: "EXACT", Config: map[string]any{"field": "amount"}},
	}
	feeRule, err := fee.NewFeeRule(context.Background(), sourceCtxID, uuid.New(), fee.MatchingSideAny, "fee-rule", 1, validPredicates())
	require.NoError(t, err)
	feeRuleRepo.rules[feeRule.ID] = feeRule

	mockCtxRepo.EXPECT().FindByID(gomock.Any(), sourceCtxID).Return(sourceContext, nil)
	mockCtxRepo.EXPECT().Create(gomock.Any(), gomock.Any()).DoAndReturn(
		func(_ context.Context, e *entities.ReconciliationContext) (*entities.ReconciliationContext, error) {
			return e, nil
		})

	// Sources pagination: one page, no more.
	mockSrcRepo.EXPECT().FindByContextID(gomock.Any(), sourceCtxID, "", maxClonePaginationLimit).
		Return(sources, libHTTP.CursorPagination{}, nil)

	// Field map existence check.
	mockFMRepo.EXPECT().ExistsBySourceIDs(gomock.Any(), gomock.Len(2)).
		Return(map[uuid.UUID]bool{sourceID1: true, sourceID2: false}, nil)

	// Create sources.
	mockSrcRepo.EXPECT().Create(gomock.Any(), gomock.Any()).Return(nil, nil).Times(2)

	// Field map for source 1.
	mockFMRepo.EXPECT().FindBySourceID(gomock.Any(), sourceID1).
		Return(&entities.FieldMap{
			ID:        uuid.New(),
			ContextID: sourceCtxID,
			SourceID:  sourceID1,
			Mapping:   map[string]any{"amount": "$.amount"},
			Version:   3,
		}, nil)
	mockFMRepo.EXPECT().Create(gomock.Any(), gomock.Any()).Return(nil, nil)

	// Rules pagination: one page, no more.
	mockRuleRepo.EXPECT().FindByContextID(gomock.Any(), sourceCtxID, "", maxClonePaginationLimit).
		Return(rules, libHTTP.CursorPagination{}, nil)

	// Create rule.
	mockRuleRepo.EXPECT().Create(gomock.Any(), gomock.Any()).Return(nil, nil)

	uc, err := NewUseCase(mockCtxRepo, mockSrcRepo, mockFMRepo, mockRuleRepo, WithFeeRuleRepository(feeRuleRepo))
	require.NoError(t, err)

	result, err := uc.CloneContext(context.Background(), CloneContextInput{
		SourceContextID: sourceCtxID,
		NewName:         "Clone",
		IncludeSources:  true,
		IncludeRules:    true,
	})

	require.NoError(t, err)
	assert.Equal(t, 2, result.SourcesCloned)
	assert.Equal(t, 1, result.FieldMapsCloned)
	assert.Equal(t, 1, result.RulesCloned)
	assert.Equal(t, 1, result.FeeRulesCloned)
}

func TestCloneContext_IncludeRulesRequiresFeeRuleRepository(t *testing.T) {
	t.Parallel()

	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	mockCtxRepo := mocks.NewMockContextRepository(ctrl)

	uc, err := NewUseCase(
		mockCtxRepo,
		mocks.NewMockSourceRepository(ctrl),
		mocks.NewMockFieldMapRepository(ctrl),
		mocks.NewMockMatchRuleRepository(ctrl),
	)
	require.NoError(t, err)

	_, err = uc.CloneContext(context.Background(), CloneContextInput{
		SourceContextID: uuid.New(),
		NewName:         "Clone",
		IncludeRules:    true,
	})

	assert.ErrorIs(t, err, ErrNilFeeRuleRepository)
}

func TestCloneMap(t *testing.T) {
	t.Parallel()

	t.Run("nil map returns nil", func(t *testing.T) {
		t.Parallel()
		assert.Nil(t, cloneMap(context.Background(), nil))
	})

	t.Run("empty map returns empty", func(t *testing.T) {
		t.Parallel()
		result := cloneMap(context.Background(), map[string]any{})
		assert.NotNil(t, result)
		assert.Empty(t, result)
	})

	t.Run("copies top-level keys", func(t *testing.T) {
		t.Parallel()
		original := map[string]any{"a": float64(1), "b": "hello"}
		copied := cloneMap(context.Background(), original)
		assert.Equal(t, original, copied)

		// Mutating copied should not affect original.
		copied["c"] = "new"
		assert.NotContains(t, original, "c")
	})
}
