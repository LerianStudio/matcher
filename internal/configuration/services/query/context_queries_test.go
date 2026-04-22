//go:build unit

package query

import (
	"context"
	"errors"
	"testing"

	"github.com/google/uuid"
	"github.com/stretchr/testify/require"
	"go.uber.org/mock/gomock"

	libHTTP "github.com/LerianStudio/lib-commons/v5/commons/net/http"

	"github.com/LerianStudio/matcher/internal/configuration/domain/entities"
	"github.com/LerianStudio/matcher/internal/configuration/domain/repositories/mocks"
	"github.com/LerianStudio/matcher/internal/configuration/domain/value_objects"
	shared "github.com/LerianStudio/matcher/internal/shared/domain"
)

var errContextRepositoryFailure = errors.New("repository failure")

func TestListContexts_NilContextRepo(t *testing.T) {
	t.Parallel()

	uc := &UseCase{}

	_, _, err := uc.ListContexts(context.Background(), "", 10, nil, nil)
	require.ErrorIs(t, err, ErrNilContextRepository)
}

func TestListContexts_RepositoryError(t *testing.T) {
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

	contextRepo.EXPECT().
		FindAll(gomock.Any(), "", 10, nil, nil).
		Return(nil, libHTTP.CursorPagination{}, errContextRepositoryFailure)

	_, _, err = uc.ListContexts(ctx, "", 10, nil, nil)
	require.Error(t, err)
	require.ErrorIs(t, err, errContextRepositoryFailure)
	require.Contains(t, err.Error(), "listing reconciliation contexts")
}

func TestListContexts_Success(t *testing.T) {
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
	tenantID := uuid.New()
	contextType := shared.ContextTypeOneToOne
	status := value_objects.ContextStatusActive
	expected := []*entities.ReconciliationContext{{ID: uuid.New(), TenantID: tenantID}}

	contextRepo.EXPECT().FindAll(gomock.Any(), "", 10, &contextType, &status).Return(expected, libHTTP.CursorPagination{}, nil)

	result, _, err := uc.ListContexts(ctx, "", 10, &contextType, &status)
	require.NoError(t, err)
	require.Equal(t, expected, result)
}

func TestCountContexts_NilUseCase(t *testing.T) {
	t.Parallel()

	var uc *UseCase

	_, err := uc.CountContexts(context.Background())
	require.ErrorIs(t, err, ErrNilContextRepository)
}

func TestCountContexts_NilContextRepo(t *testing.T) {
	t.Parallel()

	uc := &UseCase{}

	_, err := uc.CountContexts(context.Background())
	require.ErrorIs(t, err, ErrNilContextRepository)
}

func TestCountContexts_RepositoryError(t *testing.T) {
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

	contextRepo.EXPECT().Count(gomock.Any()).Return(int64(0), errContextRepositoryFailure)

	_, err = uc.CountContexts(ctx)
	require.Error(t, err)
	require.ErrorIs(t, err, errContextRepositoryFailure)
	require.Contains(t, err.Error(), "counting reconciliation contexts")
}

func TestCountContexts_Success(t *testing.T) {
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

	contextRepo.EXPECT().Count(gomock.Any()).Return(int64(42), nil)

	result, err := uc.CountContexts(ctx)
	require.NoError(t, err)
	require.Equal(t, int64(42), result)
}

func TestListContexts_WithCursor(t *testing.T) {
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
	cursor := "cursor-value"
	expected := []*entities.ReconciliationContext{{ID: uuid.New(), TenantID: uuid.New()}}

	contextRepo.EXPECT().FindAll(gomock.Any(), cursor, 10, nil, nil).Return(expected, libHTTP.CursorPagination{}, nil)

	result, _, err := uc.ListContexts(ctx, cursor, 10, nil, nil)
	require.NoError(t, err)
	require.Equal(t, expected, result)
}

func TestListContexts_WithCursorEmptyResult(t *testing.T) {
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
	cursor := "cursor-value"

	contextRepo.EXPECT().
		FindAll(gomock.Any(), cursor, 10, nil, nil).
		Return([]*entities.ReconciliationContext{}, libHTTP.CursorPagination{}, nil)

	result, _, err := uc.ListContexts(ctx, cursor, 10, nil, nil)
	require.NoError(t, err)
	require.Empty(t, result)
}

func TestListContexts_WithAllFilters(t *testing.T) {
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
	cursor := "cursor-value"
	contextType := shared.ContextTypeManyToMany
	status := value_objects.ContextStatusPaused
	expected := []*entities.ReconciliationContext{{ID: uuid.New(), TenantID: uuid.New()}}

	contextRepo.EXPECT().
		FindAll(gomock.Any(), cursor, 25, &contextType, &status).
		Return(expected, libHTTP.CursorPagination{}, nil)

	result, _, err := uc.ListContexts(ctx, cursor, 25, &contextType, &status)
	require.NoError(t, err)
	require.Equal(t, expected, result)
}

func TestCountContexts_ZeroCount(t *testing.T) {
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

	contextRepo.EXPECT().Count(gomock.Any()).Return(int64(0), nil)

	result, err := uc.CountContexts(ctx)
	require.NoError(t, err)
	require.Equal(t, int64(0), result)
}
