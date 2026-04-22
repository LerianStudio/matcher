//go:build unit

package query

import (
	"context"
	"errors"
	"testing"

	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.uber.org/mock/gomock"

	libHTTP "github.com/LerianStudio/lib-commons/v5/commons/net/http"

	"github.com/LerianStudio/matcher/internal/configuration/domain/entities"
	"github.com/LerianStudio/matcher/internal/configuration/domain/repositories/mocks"
	"github.com/LerianStudio/matcher/internal/configuration/domain/value_objects"
	shared "github.com/LerianStudio/matcher/internal/shared/domain"
)

// errDBError is a sentinel error for database errors in tests.
var errDBError = errors.New("db error")

func TestNewUseCase_Success(t *testing.T) {
	t.Parallel()

	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	contextRepo := mocks.NewMockContextRepository(ctrl)
	sourceRepo := mocks.NewMockSourceRepository(ctrl)
	fieldMapRepo := mocks.NewMockFieldMapRepository(ctrl)
	matchRuleRepo := mocks.NewMockMatchRuleRepository(ctrl)

	uc, err := NewUseCase(contextRepo, sourceRepo, fieldMapRepo, matchRuleRepo)

	require.NoError(t, err)
	require.NotNil(t, uc)
	assert.NotNil(t, uc.contextRepo)
	assert.NotNil(t, uc.sourceRepo)
	assert.NotNil(t, uc.fieldMapRepo)
	assert.NotNil(t, uc.matchRuleRepo)
}

func TestNewUseCase_NilContextRepository(t *testing.T) {
	t.Parallel()

	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	sourceRepo := mocks.NewMockSourceRepository(ctrl)
	fieldMapRepo := mocks.NewMockFieldMapRepository(ctrl)
	matchRuleRepo := mocks.NewMockMatchRuleRepository(ctrl)

	uc, err := NewUseCase(nil, sourceRepo, fieldMapRepo, matchRuleRepo)

	require.Nil(t, uc)
	require.ErrorIs(t, err, ErrNilContextRepository)
}

func TestNewUseCase_NilSourceRepository(t *testing.T) {
	t.Parallel()

	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	contextRepo := mocks.NewMockContextRepository(ctrl)
	fieldMapRepo := mocks.NewMockFieldMapRepository(ctrl)
	matchRuleRepo := mocks.NewMockMatchRuleRepository(ctrl)

	uc, err := NewUseCase(contextRepo, nil, fieldMapRepo, matchRuleRepo)

	require.Nil(t, uc)
	require.ErrorIs(t, err, ErrNilSourceRepository)
}

func TestNewUseCase_NilFieldMapRepository(t *testing.T) {
	t.Parallel()

	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	contextRepo := mocks.NewMockContextRepository(ctrl)
	sourceRepo := mocks.NewMockSourceRepository(ctrl)
	matchRuleRepo := mocks.NewMockMatchRuleRepository(ctrl)

	uc, err := NewUseCase(contextRepo, sourceRepo, nil, matchRuleRepo)

	require.Nil(t, uc)
	require.ErrorIs(t, err, ErrNilFieldMapRepository)
}

func TestNewUseCase_NilMatchRuleRepository(t *testing.T) {
	t.Parallel()

	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	contextRepo := mocks.NewMockContextRepository(ctrl)
	sourceRepo := mocks.NewMockSourceRepository(ctrl)
	fieldMapRepo := mocks.NewMockFieldMapRepository(ctrl)

	uc, err := NewUseCase(contextRepo, sourceRepo, fieldMapRepo, nil)

	require.Nil(t, uc)
	require.ErrorIs(t, err, ErrNilMatchRuleRepository)
}

func TestSourceQueries(t *testing.T) {
	t.Parallel()

	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	contextRepo := mocks.NewMockContextRepository(ctrl)
	sourceRepo := mocks.NewMockSourceRepository(ctrl)
	fieldMapRepo := mocks.NewMockFieldMapRepository(ctrl)
	matchRuleRepo := mocks.NewMockMatchRuleRepository(ctrl)

	uc, err := NewUseCase(contextRepo, sourceRepo, fieldMapRepo, matchRuleRepo)
	require.NoError(t, err)

	ctx := context.Background()
	contextID := uuid.New()
	sourceID := uuid.New()

	expected := &entities.ReconciliationSource{ID: sourceID}

	sourceType := value_objects.SourceTypeBank
	expectedList := []*entities.ReconciliationSource{expected}
	sourceRepo.EXPECT().
		FindByContextIDAndType(gomock.Any(), contextID, sourceType, "", 10).
		Return(expectedList, libHTTP.CursorPagination{}, nil)

	list, _, err := uc.ListSources(ctx, contextID, "", 10, &sourceType)
	require.NoError(t, err)
	require.Equal(t, expectedList, list)

	fallbackList := []*entities.ReconciliationSource{expected}
	sourceRepo.EXPECT().FindByContextID(gomock.Any(), contextID, "", 10).Return(fallbackList, libHTTP.CursorPagination{}, nil)
	list, _, err = uc.ListSources(ctx, contextID, "", 10, nil)
	require.NoError(t, err)
	require.Equal(t, fallbackList, list)
}

func TestFieldMapQueries(t *testing.T) {
	t.Parallel()

	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	contextRepo := mocks.NewMockContextRepository(ctrl)
	sourceRepo := mocks.NewMockSourceRepository(ctrl)
	fieldMapRepo := mocks.NewMockFieldMapRepository(ctrl)
	matchRuleRepo := mocks.NewMockMatchRuleRepository(ctrl)

	uc, err := NewUseCase(contextRepo, sourceRepo, fieldMapRepo, matchRuleRepo)
	require.NoError(t, err)

	ctx := context.Background()
	fieldMapID := uuid.New()
	sourceID := uuid.New()

	expected := &shared.FieldMap{ID: fieldMapID}
	fieldMapRepo.EXPECT().FindByID(gomock.Any(), fieldMapID).Return(expected, nil)

	result, err := uc.GetFieldMap(ctx, fieldMapID)
	require.NoError(t, err)
	require.Equal(t, expected, result)

	fieldMapRepo.EXPECT().FindBySourceID(gomock.Any(), sourceID).Return(expected, nil)

	result, err = uc.GetFieldMapBySource(ctx, sourceID)
	require.NoError(t, err)
	require.Equal(t, expected, result)
}

func TestMatchRuleQueries(t *testing.T) {
	t.Parallel()

	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	contextRepo := mocks.NewMockContextRepository(ctrl)
	sourceRepo := mocks.NewMockSourceRepository(ctrl)
	fieldMapRepo := mocks.NewMockFieldMapRepository(ctrl)
	matchRuleRepo := mocks.NewMockMatchRuleRepository(ctrl)

	uc, err := NewUseCase(contextRepo, sourceRepo, fieldMapRepo, matchRuleRepo)
	require.NoError(t, err)

	ctx := context.Background()
	contextID := uuid.New()

	expected := &entities.MatchRule{ID: uuid.New()}

	ruleType := shared.RuleTypeExact
	expectedList := entities.MatchRules{expected}
	matchRuleRepo.EXPECT().
		FindByContextIDAndType(gomock.Any(), contextID, ruleType, "", 10).
		Return(expectedList, libHTTP.CursorPagination{}, nil)

	list, _, err := uc.ListMatchRules(ctx, contextID, "", 10, &ruleType)
	require.NoError(t, err)
	require.Equal(t, expectedList, list)

	fallbackList := entities.MatchRules{expected}
	matchRuleRepo.EXPECT().
		FindByContextID(gomock.Any(), contextID, "", 10).
		Return(fallbackList, libHTTP.CursorPagination{}, nil)
	list, _, err = uc.ListMatchRules(ctx, contextID, "", 10, nil)
	require.NoError(t, err)
	require.Equal(t, fallbackList, list)
}
