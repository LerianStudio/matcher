//go:build unit

package command

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
	"go.uber.org/mock/gomock"

	libHTTP "github.com/LerianStudio/lib-uncommons/v2/uncommons/net/http"

	"github.com/LerianStudio/matcher/internal/configuration/domain/entities"
	"github.com/LerianStudio/matcher/internal/configuration/domain/repositories/mocks"
	"github.com/LerianStudio/matcher/internal/configuration/domain/value_objects"
	portMocks "github.com/LerianStudio/matcher/internal/configuration/ports/mocks"
	"github.com/LerianStudio/matcher/internal/shared/domain/fee"
)

// =============================================================================
// WithFeeScheduleRepository option tests
// =============================================================================

func TestWithFeeScheduleRepository_SetsRepo(t *testing.T) {
	t.Parallel()

	repo := &feeScheduleRepoStub{}

	uc, err := NewUseCase(
		&contextRepoStub{},
		&sourceRepoStub{},
		&fieldMapRepoStub{},
		&matchRuleRepoStub{},
		WithFeeScheduleRepository(repo),
	)

	require.NoError(t, err)
	assert.Equal(t, repo, uc.feeScheduleRepo)
}

func TestWithFeeScheduleRepository_NilIgnored(t *testing.T) {
	t.Parallel()

	uc, err := NewUseCase(
		&contextRepoStub{},
		&sourceRepoStub{},
		&fieldMapRepoStub{},
		&matchRuleRepoStub{},
		WithFeeScheduleRepository(nil),
	)

	require.NoError(t, err)
	assert.Nil(t, uc.feeScheduleRepo)
}

func TestWithAuditPublisher_NilIgnored(t *testing.T) {
	t.Parallel()

	uc, err := NewUseCase(
		&contextRepoStub{},
		&sourceRepoStub{},
		&fieldMapRepoStub{},
		&matchRuleRepoStub{},
		WithAuditPublisher(nil),
	)

	require.NoError(t, err)
	assert.Nil(t, uc.auditPublisher)
}

// =============================================================================
// CloneContext — IncludeSources with nil source repo
// =============================================================================

func TestCloneContext_IncludeSourcesNilSourceRepo(t *testing.T) {
	t.Parallel()

	uc := &UseCase{
		contextRepo: &mockCtxRepo{
			findByIDFn: func(_ context.Context, _ uuid.UUID) (*entities.ReconciliationContext, error) {
				return &entities.ReconciliationContext{ID: uuid.New()}, nil
			},
		},
	}

	_, err := uc.CloneContext(context.Background(), CloneContextInput{
		SourceContextID: uuid.New(),
		NewName:         "Clone",
		IncludeSources:  true,
	})

	require.ErrorIs(t, err, ErrNilSourceRepository)
}

func TestCloneContext_IncludeSourcesNilFieldMapRepo(t *testing.T) {
	t.Parallel()

	uc := &UseCase{
		contextRepo: &mockCtxRepo{},
		sourceRepo:  &stubSourceRepo{},
	}

	_, err := uc.CloneContext(context.Background(), CloneContextInput{
		SourceContextID: uuid.New(),
		NewName:         "Clone",
		IncludeSources:  true,
	})

	require.ErrorIs(t, err, ErrNilFieldMapRepository)
}

func TestCloneContext_IncludeRulesNilMatchRuleRepo(t *testing.T) {
	t.Parallel()

	uc := &UseCase{
		contextRepo: &mockCtxRepo{},
	}

	_, err := uc.CloneContext(context.Background(), CloneContextInput{
		SourceContextID: uuid.New(),
		NewName:         "Clone",
		IncludeRules:    true,
	})

	require.ErrorIs(t, err, ErrNilMatchRuleRepository)
}

func TestCloneContext_IncludeFeeSchedulesNilRepo(t *testing.T) {
	t.Parallel()

	uc := &UseCase{
		contextRepo:  &mockCtxRepo{},
		sourceRepo:   &stubSourceRepo{},
		fieldMapRepo: &stubFieldMapRepo{},
	}

	_, err := uc.CloneContext(context.Background(), CloneContextInput{
		SourceContextID:     uuid.New(),
		NewName:             "Clone",
		IncludeSources:      true,
		IncludeFeeSchedules: true,
	})

	require.ErrorIs(t, err, ErrNilFeeScheduleRepository)
}

// =============================================================================
// CloneContext — context create error
// =============================================================================

func TestCloneContext_ContextCreateError(t *testing.T) {
	t.Parallel()

	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	mockCtxRepo := mocks.NewMockContextRepository(ctrl)
	sourceCtxID := uuid.New()

	mockCtxRepo.EXPECT().FindByID(gomock.Any(), sourceCtxID).
		Return(&entities.ReconciliationContext{
			ID:       sourceCtxID,
			TenantID: uuid.New(),
			Name:     "Original",
			Type:     value_objects.ContextType("1:1"),
			Interval: "daily",
			Status:   value_objects.ContextStatusActive,
		}, nil)

	dbErr := errors.New("context create db error")
	mockCtxRepo.EXPECT().Create(gomock.Any(), gomock.Any()).Return(nil, dbErr)

	uc, err := NewUseCase(
		mockCtxRepo,
		mocks.NewMockSourceRepository(ctrl),
		mocks.NewMockFieldMapRepository(ctrl),
		mocks.NewMockMatchRuleRepository(ctrl),
	)
	require.NoError(t, err)

	_, err = uc.CloneContext(context.Background(), CloneContextInput{
		SourceContextID: sourceCtxID,
		NewName:         "Clone",
	})

	require.Error(t, err)
	require.ErrorIs(t, err, dbErr)
}

// =============================================================================
// CloneContext — sources clone error
// =============================================================================

func TestCloneContext_SourcesCloneError(t *testing.T) {
	t.Parallel()

	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	mockCtxRepo := mocks.NewMockContextRepository(ctrl)
	mockSrcRepo := mocks.NewMockSourceRepository(ctrl)
	mockFMRepo := mocks.NewMockFieldMapRepository(ctrl)
	mockRuleRepo := mocks.NewMockMatchRuleRepository(ctrl)

	sourceCtxID := uuid.New()

	mockCtxRepo.EXPECT().FindByID(gomock.Any(), sourceCtxID).
		Return(&entities.ReconciliationContext{
			ID:       sourceCtxID,
			TenantID: uuid.New(),
			Name:     "Original",
			Type:     value_objects.ContextType("1:1"),
			Interval: "daily",
			Status:   value_objects.ContextStatusActive,
		}, nil)

	mockCtxRepo.EXPECT().Create(gomock.Any(), gomock.Any()).
		DoAndReturn(func(_ context.Context, e *entities.ReconciliationContext) (*entities.ReconciliationContext, error) {
			return e, nil
		})

	fetchErr := errors.New("source fetch error")
	mockSrcRepo.EXPECT().FindByContextID(gomock.Any(), sourceCtxID, "", maxClonePaginationLimit).
		Return(nil, libHTTP.CursorPagination{}, fetchErr)

	uc, err := NewUseCase(mockCtxRepo, mockSrcRepo, mockFMRepo, mockRuleRepo)
	require.NoError(t, err)

	_, err = uc.CloneContext(context.Background(), CloneContextInput{
		SourceContextID: sourceCtxID,
		NewName:         "Clone",
		IncludeSources:  true,
	})

	require.Error(t, err)
	require.ErrorIs(t, err, fetchErr)
}

// =============================================================================
// CloneContext — rules clone error
// =============================================================================

func TestCloneContext_RulesCloneError(t *testing.T) {
	t.Parallel()

	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	mockCtxRepo := mocks.NewMockContextRepository(ctrl)
	mockSrcRepo := mocks.NewMockSourceRepository(ctrl)
	mockFMRepo := mocks.NewMockFieldMapRepository(ctrl)
	mockRuleRepo := mocks.NewMockMatchRuleRepository(ctrl)

	sourceCtxID := uuid.New()

	mockCtxRepo.EXPECT().FindByID(gomock.Any(), sourceCtxID).
		Return(&entities.ReconciliationContext{
			ID:       sourceCtxID,
			TenantID: uuid.New(),
			Name:     "Original",
			Type:     value_objects.ContextType("1:1"),
			Interval: "daily",
			Status:   value_objects.ContextStatusActive,
		}, nil)

	mockCtxRepo.EXPECT().Create(gomock.Any(), gomock.Any()).
		DoAndReturn(func(_ context.Context, e *entities.ReconciliationContext) (*entities.ReconciliationContext, error) {
			return e, nil
		})

	ruleErr := errors.New("rule fetch error")
	mockRuleRepo.EXPECT().FindByContextID(gomock.Any(), sourceCtxID, "", maxClonePaginationLimit).
		Return(nil, libHTTP.CursorPagination{}, ruleErr)

	uc, err := NewUseCase(mockCtxRepo, mockSrcRepo, mockFMRepo, mockRuleRepo)
	require.NoError(t, err)

	_, err = uc.CloneContext(context.Background(), CloneContextInput{
		SourceContextID: sourceCtxID,
		NewName:         "Clone",
		IncludeRules:    true,
	})

	require.Error(t, err)
	require.ErrorIs(t, err, ruleErr)
}

// =============================================================================
// CloneContext — with fee schedule cloning
// =============================================================================

func TestCloneContext_WithFeeSchedules(t *testing.T) {
	t.Parallel()

	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	mockCtxRepo := mocks.NewMockContextRepository(ctrl)
	mockSrcRepo := mocks.NewMockSourceRepository(ctrl)
	mockFMRepo := mocks.NewMockFieldMapRepository(ctrl)
	mockRuleRepo := mocks.NewMockMatchRuleRepository(ctrl)

	sourceCtxID := uuid.New()
	tenantID := uuid.New()
	feeScheduleID := uuid.New()
	sourceID := uuid.New()

	mockCtxRepo.EXPECT().FindByID(gomock.Any(), sourceCtxID).
		Return(&entities.ReconciliationContext{
			ID:       sourceCtxID,
			TenantID: tenantID,
			Name:     "Original",
			Type:     value_objects.ContextType("1:1"),
			Interval: "daily",
			Status:   value_objects.ContextStatusActive,
		}, nil)

	mockCtxRepo.EXPECT().Create(gomock.Any(), gomock.Any()).
		DoAndReturn(func(_ context.Context, e *entities.ReconciliationContext) (*entities.ReconciliationContext, error) {
			return e, nil
		})

	sources := []*entities.ReconciliationSource{
		{ID: sourceID, ContextID: sourceCtxID, Name: "Source A", FeeScheduleID: &feeScheduleID},
	}

	mockSrcRepo.EXPECT().FindByContextID(gomock.Any(), sourceCtxID, "", maxClonePaginationLimit).
		Return(sources, libHTTP.CursorPagination{}, nil)

	feeRepo := &feeScheduleRepoStub{
		getByIDsFn: func(_ context.Context, ids []uuid.UUID) (map[uuid.UUID]*fee.FeeSchedule, error) {
			result := make(map[uuid.UUID]*fee.FeeSchedule)
			for _, id := range ids {
				result[id] = &fee.FeeSchedule{
					ID:               id,
					TenantID:         tenantID,
					Name:             "Original Fee",
					Currency:         "USD",
					ApplicationOrder: fee.ApplicationOrderParallel,
					RoundingScale:    2,
					RoundingMode:     fee.RoundingModeHalfUp,
					Items: []fee.FeeScheduleItem{
						{
							ID:        uuid.New(),
							Name:      "Interchange",
							Priority:  1,
							Structure: fee.FlatFee{Amount: decimal.NewFromFloat(1.50)},
						},
					},
				}
			}
			return result, nil
		},
		createFn: func(_ context.Context, s *fee.FeeSchedule) (*fee.FeeSchedule, error) {
			return s, nil
		},
	}

	// Field map existence check
	mockFMRepo.EXPECT().ExistsBySourceIDs(gomock.Any(), gomock.Len(1)).
		Return(map[uuid.UUID]bool{sourceID: false}, nil)

	// Create source
	mockSrcRepo.EXPECT().Create(gomock.Any(), gomock.Any()).
		DoAndReturn(func(_ context.Context, src *entities.ReconciliationSource) (*entities.ReconciliationSource, error) {
			return src, nil
		})

	uc, err := NewUseCase(
		mockCtxRepo, mockSrcRepo, mockFMRepo, mockRuleRepo,
		WithFeeScheduleRepository(feeRepo),
	)
	require.NoError(t, err)

	result, err := uc.CloneContext(context.Background(), CloneContextInput{
		SourceContextID:     sourceCtxID,
		NewName:             "Clone With Fees",
		IncludeSources:      true,
		IncludeFeeSchedules: true,
	})

	require.NoError(t, err)
	assert.Equal(t, 1, result.SourcesCloned)
	assert.Equal(t, 1, result.FeeSchedulesCloned)
}

// =============================================================================
// cloneReferencedFeeSchedules edge cases
// =============================================================================

func TestCloneReferencedFeeSchedules_NoFeeScheduleIDs(t *testing.T) {
	t.Parallel()

	uc := &UseCase{
		feeScheduleRepo: &feeScheduleRepoStub{},
	}

	sources := []*entities.ReconciliationSource{
		{ID: uuid.New(), Name: "No Fee"},
	}

	mapping, count, err := uc.cloneReferencedFeeSchedules(context.Background(), sources, uuid.New())
	require.NoError(t, err)
	assert.Empty(t, mapping)
	assert.Equal(t, 0, count)
}

func TestCloneReferencedFeeSchedules_NilFeeScheduleID(t *testing.T) {
	t.Parallel()

	uc := &UseCase{
		feeScheduleRepo: &feeScheduleRepoStub{},
	}

	nilID := uuid.Nil
	sources := []*entities.ReconciliationSource{
		{ID: uuid.New(), Name: "Nil Fee", FeeScheduleID: &nilID},
	}

	mapping, count, err := uc.cloneReferencedFeeSchedules(context.Background(), sources, uuid.New())
	require.NoError(t, err)
	assert.Empty(t, mapping)
	assert.Equal(t, 0, count)
}

func TestCloneReferencedFeeSchedules_FetchError(t *testing.T) {
	t.Parallel()

	fetchErr := errors.New("batch fetch error")

	uc := &UseCase{
		feeScheduleRepo: &feeScheduleRepoStub{
			getByIDsFn: func(_ context.Context, _ []uuid.UUID) (map[uuid.UUID]*fee.FeeSchedule, error) {
				return nil, fetchErr
			},
		},
	}

	feeID := uuid.New()
	sources := []*entities.ReconciliationSource{
		{ID: uuid.New(), Name: "Source", FeeScheduleID: &feeID},
	}

	_, _, err := uc.cloneReferencedFeeSchedules(context.Background(), sources, uuid.New())
	require.Error(t, err)
	require.ErrorIs(t, err, fetchErr)
}

func TestCloneReferencedFeeSchedules_NilScheduleInMap(t *testing.T) {
	t.Parallel()

	feeID := uuid.New()

	uc := &UseCase{
		feeScheduleRepo: &feeScheduleRepoStub{
			getByIDsFn: func(_ context.Context, _ []uuid.UUID) (map[uuid.UUID]*fee.FeeSchedule, error) {
				return map[uuid.UUID]*fee.FeeSchedule{feeID: nil}, nil
			},
		},
	}

	sources := []*entities.ReconciliationSource{
		{ID: uuid.New(), Name: "Source", FeeScheduleID: &feeID},
	}

	mapping, count, err := uc.cloneReferencedFeeSchedules(context.Background(), sources, uuid.New())
	require.NoError(t, err)
	assert.Empty(t, mapping)
	assert.Equal(t, 0, count)
}

func TestCloneReferencedFeeSchedules_CreateError(t *testing.T) {
	t.Parallel()

	feeID := uuid.New()
	createErr := errors.New("fee create error")

	uc := &UseCase{
		feeScheduleRepo: &feeScheduleRepoStub{
			getByIDsFn: func(_ context.Context, _ []uuid.UUID) (map[uuid.UUID]*fee.FeeSchedule, error) {
				return map[uuid.UUID]*fee.FeeSchedule{
					feeID: {
						ID:   feeID,
						Name: "Fee Schedule",
					},
				}, nil
			},
			createFn: func(_ context.Context, _ *fee.FeeSchedule) (*fee.FeeSchedule, error) {
				return nil, createErr
			},
		},
	}

	sources := []*entities.ReconciliationSource{
		{ID: uuid.New(), Name: "Source", FeeScheduleID: &feeID},
	}

	_, _, err := uc.cloneReferencedFeeSchedules(context.Background(), sources, uuid.New())
	require.Error(t, err)
	require.ErrorIs(t, err, createErr)
}

// =============================================================================
// cloneFieldMap edge cases
// =============================================================================

func TestCloneFieldMap_CreateError(t *testing.T) {
	t.Parallel()

	createErr := errors.New("field map create error")

	uc := &UseCase{
		fieldMapRepo: &fieldMapRepoStub{
			findBySourceIDFn: func(_ context.Context, _ uuid.UUID) (*entities.FieldMap, error) {
				return &entities.FieldMap{
					ID:      uuid.New(),
					Mapping: map[string]any{"field": "value"},
				}, nil
			},
			createFn: func(_ context.Context, _ *entities.FieldMap) (*entities.FieldMap, error) {
				return nil, createErr
			},
		},
	}

	cloned, err := uc.cloneFieldMap(context.Background(), nil, uuid.New(), uuid.New(), uuid.New(), time.Now().UTC())
	require.Error(t, err)
	assert.False(t, cloned)
	require.ErrorIs(t, err, createErr)
}

func TestCloneFieldMap_FindError(t *testing.T) {
	t.Parallel()

	findErr := errors.New("field map find error")

	uc := &UseCase{
		fieldMapRepo: &fieldMapRepoStub{
			findBySourceIDFn: func(_ context.Context, _ uuid.UUID) (*entities.FieldMap, error) {
				return nil, findErr
			},
		},
	}

	cloned, err := uc.cloneFieldMap(context.Background(), nil, uuid.New(), uuid.New(), uuid.New(), time.Now().UTC())
	require.Error(t, err)
	assert.False(t, cloned)
	require.ErrorIs(t, err, findErr)
}

func TestCloneFieldMap_NotFound(t *testing.T) {
	t.Parallel()

	uc := &UseCase{
		fieldMapRepo: &fieldMapRepoStub{
			findBySourceIDFn: func(_ context.Context, _ uuid.UUID) (*entities.FieldMap, error) {
				return nil, sql.ErrNoRows
			},
		},
	}

	cloned, err := uc.cloneFieldMap(context.Background(), nil, uuid.New(), uuid.New(), uuid.New(), time.Now().UTC())
	require.NoError(t, err)
	assert.False(t, cloned)
}

// =============================================================================
// cloneSourcesAndFieldMaps edge cases
// =============================================================================

func TestCloneSourcesAndFieldMaps_EmptySources(t *testing.T) {
	t.Parallel()

	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	mockSrcRepo := mocks.NewMockSourceRepository(ctrl)
	mockFMRepo := mocks.NewMockFieldMapRepository(ctrl)

	mockSrcRepo.EXPECT().FindByContextID(gomock.Any(), gomock.Any(), "", maxClonePaginationLimit).
		Return([]*entities.ReconciliationSource{}, libHTTP.CursorPagination{}, nil)

	uc := &UseCase{
		sourceRepo:   mockSrcRepo,
		fieldMapRepo: mockFMRepo,
	}

	sourcesCloned, fieldMapsCloned, feeSchedulesCloned, err := uc.cloneSourcesAndFieldMaps(
		context.Background(), nil, uuid.New(), uuid.New(), uuid.New(), false,
	)
	require.NoError(t, err)
	assert.Equal(t, 0, sourcesCloned)
	assert.Equal(t, 0, fieldMapsCloned)
	assert.Equal(t, 0, feeSchedulesCloned)
}

func TestCloneSourcesAndFieldMaps_ExistsBySourceIDsError(t *testing.T) {
	t.Parallel()

	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	mockSrcRepo := mocks.NewMockSourceRepository(ctrl)
	mockFMRepo := mocks.NewMockFieldMapRepository(ctrl)

	sourceID := uuid.New()
	sources := []*entities.ReconciliationSource{
		{ID: sourceID, Name: "Source A"},
	}

	mockSrcRepo.EXPECT().FindByContextID(gomock.Any(), gomock.Any(), "", maxClonePaginationLimit).
		Return(sources, libHTTP.CursorPagination{}, nil)

	existsErr := errors.New("exists check error")
	mockFMRepo.EXPECT().ExistsBySourceIDs(gomock.Any(), gomock.Any()).Return(nil, existsErr)

	uc := &UseCase{
		sourceRepo:   mockSrcRepo,
		fieldMapRepo: mockFMRepo,
	}

	_, _, _, err := uc.cloneSourcesAndFieldMaps(
		context.Background(), nil, uuid.New(), uuid.New(), uuid.New(), false,
	)
	require.Error(t, err)
	require.ErrorIs(t, err, existsErr)
}

func TestCloneSourcesAndFieldMaps_SourceCreateError(t *testing.T) {
	t.Parallel()

	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	mockSrcRepo := mocks.NewMockSourceRepository(ctrl)
	mockFMRepo := mocks.NewMockFieldMapRepository(ctrl)

	sourceID := uuid.New()
	sources := []*entities.ReconciliationSource{
		{ID: sourceID, Name: "Source A"},
	}

	mockSrcRepo.EXPECT().FindByContextID(gomock.Any(), gomock.Any(), "", maxClonePaginationLimit).
		Return(sources, libHTTP.CursorPagination{}, nil)

	mockFMRepo.EXPECT().ExistsBySourceIDs(gomock.Any(), gomock.Any()).
		Return(map[uuid.UUID]bool{sourceID: false}, nil)

	createErr := errors.New("source create error")
	mockSrcRepo.EXPECT().Create(gomock.Any(), gomock.Any()).Return(nil, createErr)

	uc := &UseCase{
		sourceRepo:   mockSrcRepo,
		fieldMapRepo: mockFMRepo,
	}

	_, _, _, err := uc.cloneSourcesAndFieldMaps(
		context.Background(), nil, uuid.New(), uuid.New(), uuid.New(), false,
	)
	require.Error(t, err)
	require.ErrorIs(t, err, createErr)
}

func TestCloneSourcesAndFieldMaps_FeeScheduleMappingApplied(t *testing.T) {
	t.Parallel()

	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	mockSrcRepo := mocks.NewMockSourceRepository(ctrl)
	mockFMRepo := mocks.NewMockFieldMapRepository(ctrl)

	sourceID := uuid.New()
	feeID := uuid.New()
	sources := []*entities.ReconciliationSource{
		{ID: sourceID, Name: "Source A", FeeScheduleID: &feeID},
	}

	mockSrcRepo.EXPECT().FindByContextID(gomock.Any(), gomock.Any(), "", maxClonePaginationLimit).
		Return(sources, libHTTP.CursorPagination{}, nil)

	mockFMRepo.EXPECT().ExistsBySourceIDs(gomock.Any(), gomock.Any()).
		Return(map[uuid.UUID]bool{sourceID: false}, nil)

	feeRepo := &feeScheduleRepoStub{
		getByIDsFn: func(_ context.Context, _ []uuid.UUID) (map[uuid.UUID]*fee.FeeSchedule, error) {
			return map[uuid.UUID]*fee.FeeSchedule{
				feeID: {ID: feeID, Name: "Fee"},
			}, nil
		},
		createFn: func(_ context.Context, s *fee.FeeSchedule) (*fee.FeeSchedule, error) {
			return s, nil
		},
	}

	var capturedSource *entities.ReconciliationSource
	mockSrcRepo.EXPECT().Create(gomock.Any(), gomock.Any()).
		DoAndReturn(func(_ context.Context, src *entities.ReconciliationSource) (*entities.ReconciliationSource, error) {
			capturedSource = src
			return src, nil
		})

	uc := &UseCase{
		sourceRepo:      mockSrcRepo,
		fieldMapRepo:    mockFMRepo,
		feeScheduleRepo: feeRepo,
	}

	sourcesCloned, _, feeSchedulesCloned, err := uc.cloneSourcesAndFieldMaps(
		context.Background(), nil, uuid.New(), uuid.New(), uuid.New(), true,
	)
	require.NoError(t, err)
	assert.Equal(t, 1, sourcesCloned)
	assert.Equal(t, 1, feeSchedulesCloned)
	require.NotNil(t, capturedSource)
	require.NotNil(t, capturedSource.FeeScheduleID)
	// The fee schedule ID should be the NEW cloned ID, not the original
	assert.NotEqual(t, feeID, *capturedSource.FeeScheduleID)
}

func TestCloneSourcesAndFieldMaps_SourceKeepsOriginalFeeScheduleID(t *testing.T) {
	t.Parallel()

	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	mockSrcRepo := mocks.NewMockSourceRepository(ctrl)
	mockFMRepo := mocks.NewMockFieldMapRepository(ctrl)

	sourceID := uuid.New()
	feeID := uuid.New()
	sources := []*entities.ReconciliationSource{
		{ID: sourceID, Name: "Source A", FeeScheduleID: &feeID},
	}

	mockSrcRepo.EXPECT().FindByContextID(gomock.Any(), gomock.Any(), "", maxClonePaginationLimit).
		Return(sources, libHTTP.CursorPagination{}, nil)

	mockFMRepo.EXPECT().ExistsBySourceIDs(gomock.Any(), gomock.Any()).
		Return(map[uuid.UUID]bool{sourceID: false}, nil)

	var capturedSource *entities.ReconciliationSource
	mockSrcRepo.EXPECT().Create(gomock.Any(), gomock.Any()).
		DoAndReturn(func(_ context.Context, src *entities.ReconciliationSource) (*entities.ReconciliationSource, error) {
			capturedSource = src
			return src, nil
		})

	uc := &UseCase{
		sourceRepo:   mockSrcRepo,
		fieldMapRepo: mockFMRepo,
	}

	// Without fee schedule cloning, the mapping is empty, so original ID is kept
	_, _, _, err := uc.cloneSourcesAndFieldMaps(
		context.Background(), nil, uuid.New(), uuid.New(), uuid.New(), false,
	)
	require.NoError(t, err)
	require.NotNil(t, capturedSource)
	require.NotNil(t, capturedSource.FeeScheduleID)
	assert.Equal(t, feeID, *capturedSource.FeeScheduleID)
}

// =============================================================================
// cloneMatchRules edge cases
// =============================================================================

func TestCloneMatchRules_CreateError(t *testing.T) {
	t.Parallel()

	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	mockRuleRepo := mocks.NewMockMatchRuleRepository(ctrl)

	rules := entities.MatchRules{
		{ID: uuid.New(), Priority: 1, Type: "EXACT", Config: map[string]any{"field": "amount"}},
	}

	mockRuleRepo.EXPECT().FindByContextID(gomock.Any(), gomock.Any(), "", maxClonePaginationLimit).
		Return(rules, libHTTP.CursorPagination{}, nil)

	createErr := errors.New("rule create error")
	mockRuleRepo.EXPECT().Create(gomock.Any(), gomock.Any()).Return(nil, createErr)

	uc := &UseCase{
		matchRuleRepo: mockRuleRepo,
	}

	_, err := uc.cloneMatchRules(context.Background(), uuid.New(), uuid.New())
	require.Error(t, err)
	require.ErrorIs(t, err, createErr)
}

// =============================================================================
// fetchAllSources/fetchAllRules multi-page
// =============================================================================

func TestFetchAllSources_MultiPage(t *testing.T) {
	t.Parallel()

	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	mockSrcRepo := mocks.NewMockSourceRepository(ctrl)
	ctxID := uuid.New()

	page1 := []*entities.ReconciliationSource{
		{ID: uuid.New(), Name: "Source 1"},
	}
	page2 := []*entities.ReconciliationSource{
		{ID: uuid.New(), Name: "Source 2"},
	}

	gomock.InOrder(
		mockSrcRepo.EXPECT().FindByContextID(gomock.Any(), ctxID, "", maxClonePaginationLimit).
			Return(page1, libHTTP.CursorPagination{Next: "cursor1"}, nil),
		mockSrcRepo.EXPECT().FindByContextID(gomock.Any(), ctxID, "cursor1", maxClonePaginationLimit).
			Return(page2, libHTTP.CursorPagination{}, nil),
	)

	uc := &UseCase{sourceRepo: mockSrcRepo}

	sources, err := uc.fetchAllSources(context.Background(), ctxID)
	require.NoError(t, err)
	require.Len(t, sources, 2)
	assert.Equal(t, "Source 1", sources[0].Name)
	assert.Equal(t, "Source 2", sources[1].Name)
}

func TestFetchAllRules_MultiPage(t *testing.T) {
	t.Parallel()

	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	mockRuleRepo := mocks.NewMockMatchRuleRepository(ctrl)
	ctxID := uuid.New()

	page1 := entities.MatchRules{
		{ID: uuid.New(), Priority: 1},
	}
	page2 := entities.MatchRules{
		{ID: uuid.New(), Priority: 2},
	}

	gomock.InOrder(
		mockRuleRepo.EXPECT().FindByContextID(gomock.Any(), ctxID, "", maxClonePaginationLimit).
			Return(page1, libHTTP.CursorPagination{Next: "cursor1"}, nil),
		mockRuleRepo.EXPECT().FindByContextID(gomock.Any(), ctxID, "cursor1", maxClonePaginationLimit).
			Return(page2, libHTTP.CursorPagination{}, nil),
	)

	uc := &UseCase{matchRuleRepo: mockRuleRepo}

	rules, err := uc.fetchAllRules(context.Background(), ctxID)
	require.NoError(t, err)
	require.Len(t, rules, 2)
}

// =============================================================================
// Schedule commands — additional error paths
// =============================================================================

func TestCreateSchedule_ContextGenericError(t *testing.T) {
	t.Parallel()

	ctxErr := errors.New("generic context error")
	ctxRepo := &mockCtxRepo{
		findByIDFn: func(_ context.Context, _ uuid.UUID) (*entities.ReconciliationContext, error) {
			return nil, ctxErr
		},
	}

	scheduleRepo := &mockScheduleRepo{}
	uc := newUseCaseWithCtxAndSchedule(ctxRepo, scheduleRepo)

	_, err := uc.CreateSchedule(context.Background(), uuid.New(), entities.CreateScheduleInput{
		CronExpression: validCronExpr,
	})

	require.Error(t, err)
	require.ErrorIs(t, err, ctxErr)
	assert.Contains(t, err.Error(), "verify context")
}

func TestCreateSchedule_PersistError(t *testing.T) {
	t.Parallel()

	persistErr := errors.New("persist error")
	scheduleRepo := &mockScheduleRepo{
		createFn: func(_ context.Context, _ *entities.ReconciliationSchedule) (*entities.ReconciliationSchedule, error) {
			return nil, persistErr
		},
	}

	uc := newUseCaseWithScheduleRepo(scheduleRepo)

	_, err := uc.CreateSchedule(context.Background(), uuid.New(), entities.CreateScheduleInput{
		CronExpression: validCronExpr,
	})

	require.Error(t, err)
	require.ErrorIs(t, err, persistErr)
	assert.Contains(t, err.Error(), "persist schedule")
}

func TestUpdateSchedule_UpdateEntityError(t *testing.T) {
	t.Parallel()

	scheduleID := uuid.New()
	existing := &entities.ReconciliationSchedule{
		ID:             scheduleID,
		ContextID:      uuid.New(),
		CronExpression: "0 0 * * *",
		Enabled:        true,
		CreatedAt:      time.Now().UTC().Add(-time.Hour),
		UpdatedAt:      time.Now().UTC().Add(-time.Hour),
	}

	scheduleRepo := &mockScheduleRepo{
		findByIDFn: func(_ context.Context, _ uuid.UUID) (*entities.ReconciliationSchedule, error) {
			return existing, nil
		},
	}

	uc := newUseCaseWithScheduleRepo(scheduleRepo)

	invalidCron := "invalid-cron"
	_, err := uc.UpdateSchedule(context.Background(), existing.ContextID, scheduleID, entities.UpdateScheduleInput{
		CronExpression: &invalidCron,
	})

	require.Error(t, err)
	assert.Contains(t, err.Error(), "update schedule entity")
}

func TestUpdateSchedule_PersistError(t *testing.T) {
	t.Parallel()

	scheduleID := uuid.New()
	contextID := uuid.New()
	existing := &entities.ReconciliationSchedule{
		ID:             scheduleID,
		ContextID:      contextID,
		CronExpression: "0 0 * * *",
		Enabled:        true,
		CreatedAt:      time.Now().UTC().Add(-time.Hour),
		UpdatedAt:      time.Now().UTC().Add(-time.Hour),
	}

	persistErr := errors.New("update persist error")
	scheduleRepo := &mockScheduleRepo{
		findByIDFn: func(_ context.Context, _ uuid.UUID) (*entities.ReconciliationSchedule, error) {
			return existing, nil
		},
		updateFn: func(_ context.Context, _ *entities.ReconciliationSchedule) (*entities.ReconciliationSchedule, error) {
			return nil, persistErr
		},
	}

	uc := newUseCaseWithScheduleRepo(scheduleRepo)

	disabled := false
	_, err := uc.UpdateSchedule(context.Background(), contextID, scheduleID, entities.UpdateScheduleInput{
		Enabled: &disabled,
	})

	require.Error(t, err)
	require.ErrorIs(t, err, persistErr)
	assert.Contains(t, err.Error(), "persist schedule update")
}

func TestUpdateSchedule_GenericFindError(t *testing.T) {
	t.Parallel()

	findErr := errors.New("generic find error")
	scheduleRepo := &mockScheduleRepo{
		findByIDFn: func(_ context.Context, _ uuid.UUID) (*entities.ReconciliationSchedule, error) {
			return nil, findErr
		},
	}

	uc := newUseCaseWithScheduleRepo(scheduleRepo)

	_, err := uc.UpdateSchedule(context.Background(), uuid.New(), uuid.New(), entities.UpdateScheduleInput{})

	require.Error(t, err)
	require.ErrorIs(t, err, findErr)
	assert.Contains(t, err.Error(), "find schedule")
}

func TestDeleteSchedule_GenericDeleteError(t *testing.T) {
	t.Parallel()

	contextID := uuid.New()
	existing := &entities.ReconciliationSchedule{
		ID:        uuid.New(),
		ContextID: contextID,
	}
	deleteErr := errors.New("generic delete error")
	scheduleRepo := &mockScheduleRepo{
		findByIDFn: func(_ context.Context, _ uuid.UUID) (*entities.ReconciliationSchedule, error) {
			return existing, nil
		},
		deleteFn: func(_ context.Context, _ uuid.UUID) error {
			return deleteErr
		},
	}

	uc := newUseCaseWithScheduleRepo(scheduleRepo)

	err := uc.DeleteSchedule(context.Background(), contextID, existing.ID)

	require.Error(t, err)
	require.ErrorIs(t, err, deleteErr)
	assert.Contains(t, err.Error(), "delete schedule")
}

// =============================================================================
// Fee schedule commands — CreatedFeeScheduleNil
// =============================================================================

func TestCreateFeeSchedule_CreatedNil(t *testing.T) {
	t.Parallel()

	repo := &feeScheduleRepoStub{
		createFn: func(_ context.Context, _ *fee.FeeSchedule) (*fee.FeeSchedule, error) {
			return nil, nil
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

	require.ErrorIs(t, err, ErrCreatedFeeScheduleNil)
}

func TestUpdateFeeSchedule_EntityNilFromRepo(t *testing.T) {
	t.Parallel()

	repo := &feeScheduleRepoStub{
		getByIDFn: func(_ context.Context, _ uuid.UUID) (*fee.FeeSchedule, error) {
			return nil, nil
		},
	}

	uc := newUseCaseWithFeeSchedule(t, repo)

	name := "Updated"
	_, err := uc.UpdateFeeSchedule(context.Background(), uuid.New(), &name, nil, nil, nil)

	require.ErrorIs(t, err, fee.ErrFeeScheduleNotFound)
}

func TestDeleteFeeSchedule_DeleteError(t *testing.T) {
	t.Parallel()

	deleteErr := errors.New("delete error")
	repo := &feeScheduleRepoStub{
		getByIDFn: func(_ context.Context, id uuid.UUID) (*fee.FeeSchedule, error) {
			return &fee.FeeSchedule{ID: id}, nil
		},
		deleteFn: func(_ context.Context, _ uuid.UUID) error {
			return deleteErr
		},
	}

	uc := newUseCaseWithFeeSchedule(t, repo)

	err := uc.DeleteFeeSchedule(context.Background(), uuid.New())
	require.Error(t, err)
	require.ErrorIs(t, err, deleteErr)
}

// =============================================================================
// Fee schedule sentinel errors
// =============================================================================

func TestFeeScheduleSentinelErrors(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name string
		err  error
	}{
		{"ErrNilFeeScheduleRepository", ErrNilFeeScheduleRepository},
		{"ErrCreatedFeeScheduleNil", ErrCreatedFeeScheduleNil},
		{"ErrUnknownFeeStructureType", ErrUnknownFeeStructureType},
		{"ErrNilScheduleRepository", ErrNilScheduleRepository},
		{"ErrScheduleNotFound", ErrScheduleNotFound},
		{"ErrCloneNameRequired", ErrCloneNameRequired},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			require.Error(t, tt.err)
			assert.NotEmpty(t, tt.err.Error())
		})
	}
}

// =============================================================================
// ParseFeeStructureFromRequest — tiered edge cases
// =============================================================================

func TestParseFeeStructureFromRequest_TieredNonMapEntry(t *testing.T) {
	t.Parallel()

	_, err := ParseFeeStructureFromRequest("TIERED", map[string]any{
		"tiers": []any{"not-a-map"},
	})

	require.Error(t, err)
	require.ErrorIs(t, err, fee.ErrInvalidTieredDefinition)
}

func TestParseFeeStructureFromRequest_TieredNoRate(t *testing.T) {
	t.Parallel()

	_, err := ParseFeeStructureFromRequest("TIERED", map[string]any{
		"tiers": []any{
			map[string]any{"upTo": "100"},
		},
	})

	require.Error(t, err)
	require.ErrorIs(t, err, fee.ErrInvalidTieredDefinition)
}

func TestParseFeeStructureFromRequest_TieredInvalidRateDecimal(t *testing.T) {
	t.Parallel()

	_, err := ParseFeeStructureFromRequest("TIERED", map[string]any{
		"tiers": []any{
			map[string]any{"rate": "not-a-number"},
		},
	})

	require.Error(t, err)
	assert.Contains(t, err.Error(), "invalid rate")
}

func TestParseFeeStructureFromRequest_TieredUpToNotString(t *testing.T) {
	t.Parallel()

	_, err := ParseFeeStructureFromRequest("TIERED", map[string]any{
		"tiers": []any{
			map[string]any{"rate": "0.01", "upTo": 100},
		},
	})

	require.Error(t, err)
	require.ErrorIs(t, err, fee.ErrInvalidTieredDefinition)
}

func TestParseFeeStructureFromRequest_TieredInvalidUpToDecimal(t *testing.T) {
	t.Parallel()

	_, err := ParseFeeStructureFromRequest("TIERED", map[string]any{
		"tiers": []any{
			map[string]any{"rate": "0.01", "upTo": "not-a-number"},
		},
	})

	require.Error(t, err)
	assert.Contains(t, err.Error(), "invalid upTo")
}

func TestParseFeeStructureFromRequest_TieredRateNonString(t *testing.T) {
	t.Parallel()

	_, err := ParseFeeStructureFromRequest("TIERED", map[string]any{
		"tiers": []any{
			map[string]any{"rate": 0.01},
		},
	})

	require.Error(t, err)
	require.ErrorIs(t, err, fee.ErrInvalidTieredDefinition)
}

// =============================================================================
// CloneContext with audit publisher
// =============================================================================

func TestCloneContext_WithAuditPublisher(t *testing.T) {
	t.Parallel()

	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	mockCtxRepo := mocks.NewMockContextRepository(ctrl)
	mockSrcRepo := mocks.NewMockSourceRepository(ctrl)
	mockFMRepo := mocks.NewMockFieldMapRepository(ctrl)
	mockRuleRepo := mocks.NewMockMatchRuleRepository(ctrl)
	mockAuditPub := portMocks.NewMockAuditPublisher(ctrl)

	sourceCtxID := uuid.New()

	mockCtxRepo.EXPECT().FindByID(gomock.Any(), sourceCtxID).
		Return(&entities.ReconciliationContext{
			ID:       sourceCtxID,
			TenantID: uuid.New(),
			Name:     "Original",
			Type:     value_objects.ContextType("1:1"),
			Interval: "daily",
			Status:   value_objects.ContextStatusActive,
		}, nil)

	mockCtxRepo.EXPECT().Create(gomock.Any(), gomock.Any()).
		DoAndReturn(func(_ context.Context, e *entities.ReconciliationContext) (*entities.ReconciliationContext, error) {
			return e, nil
		})

	mockAuditPub.EXPECT().Publish(gomock.Any(), gomock.Any()).Return(nil)

	uc, err := NewUseCase(mockCtxRepo, mockSrcRepo, mockFMRepo, mockRuleRepo, WithAuditPublisher(mockAuditPub))
	require.NoError(t, err)

	result, err := uc.CloneContext(context.Background(), CloneContextInput{
		SourceContextID: sourceCtxID,
		NewName:         "Clone",
	})

	require.NoError(t, err)
	assert.NotNil(t, result)
}

// =============================================================================
// updateSchedule* helpers — direct unit tests
// =============================================================================

func TestUpdateScheduleName_NilIsNoop(t *testing.T) {
	t.Parallel()

	entity := &fee.FeeSchedule{Name: "Original"}
	err := updateScheduleName(entity, nil)
	require.NoError(t, err)
	assert.Equal(t, "Original", entity.Name)
}

func TestUpdateScheduleApplicationOrder_NilIsNoop(t *testing.T) {
	t.Parallel()

	entity := &fee.FeeSchedule{ApplicationOrder: fee.ApplicationOrderParallel}
	err := updateScheduleApplicationOrder(entity, nil)
	require.NoError(t, err)
	assert.Equal(t, fee.ApplicationOrderParallel, entity.ApplicationOrder)
}

func TestUpdateScheduleApplicationOrder_ValidOrder(t *testing.T) {
	t.Parallel()

	entity := &fee.FeeSchedule{ApplicationOrder: fee.ApplicationOrderParallel}
	order := "CASCADING"
	err := updateScheduleApplicationOrder(entity, &order)
	require.NoError(t, err)
	assert.Equal(t, fee.ApplicationOrderCascading, entity.ApplicationOrder)
}

func TestUpdateScheduleRoundingScale_NilIsNoop(t *testing.T) {
	t.Parallel()

	entity := &fee.FeeSchedule{RoundingScale: 2}
	err := updateScheduleRoundingScale(entity, nil)
	require.NoError(t, err)
	assert.Equal(t, 2, entity.RoundingScale)
}

func TestUpdateScheduleRoundingScale_ValidScale(t *testing.T) {
	t.Parallel()

	entity := &fee.FeeSchedule{RoundingScale: 2}
	scale := 4
	err := updateScheduleRoundingScale(entity, &scale)
	require.NoError(t, err)
	assert.Equal(t, 4, entity.RoundingScale)
}

func TestUpdateScheduleRoundingMode_NilIsNoop(t *testing.T) {
	t.Parallel()

	entity := &fee.FeeSchedule{RoundingMode: fee.RoundingModeHalfUp}
	err := updateScheduleRoundingMode(entity, nil)
	require.NoError(t, err)
	assert.Equal(t, fee.RoundingModeHalfUp, entity.RoundingMode)
}

func TestUpdateScheduleRoundingMode_ValidMode(t *testing.T) {
	t.Parallel()

	entity := &fee.FeeSchedule{RoundingMode: fee.RoundingModeHalfUp}
	mode := "FLOOR"
	err := updateScheduleRoundingMode(entity, &mode)
	require.NoError(t, err)
	assert.Equal(t, fee.RoundingMode("FLOOR"), entity.RoundingMode)
}

// =============================================================================
// resolveRuleTypeAndConfig
// =============================================================================

func TestResolveRuleTypeAndConfig_BothProvided(t *testing.T) {
	t.Parallel()

	entity := &entities.MatchRule{
		Type:   "EXACT",
		Config: map[string]any{"field": "old"},
	}

	newType := entities.RuleType("FUZZY")
	newConfig := map[string]any{"field": "new"}
	input := entities.UpdateMatchRuleInput{
		Type:   &newType,
		Config: newConfig,
	}

	ruleType, config := resolveRuleTypeAndConfig(entity, input)
	assert.Equal(t, entities.RuleType("FUZZY"), ruleType)
	assert.Equal(t, "new", config["field"])
}

func TestResolveRuleTypeAndConfig_NeitherProvided(t *testing.T) {
	t.Parallel()

	entity := &entities.MatchRule{
		Type:   "EXACT",
		Config: map[string]any{"field": "original"},
	}

	input := entities.UpdateMatchRuleInput{}

	ruleType, config := resolveRuleTypeAndConfig(entity, input)
	assert.Equal(t, entities.RuleType("EXACT"), ruleType)
	assert.Equal(t, "original", config["field"])
}
