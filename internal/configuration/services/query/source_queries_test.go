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
)

var errSourceRepositoryFailure = errors.New("repository failure")

func TestGetSource_NilSourceRepo(t *testing.T) {
	t.Parallel()

	uc := &UseCase{}

	_, err := uc.GetSource(context.Background(), uuid.New(), uuid.New())
	require.ErrorIs(t, err, ErrNilSourceRepository)
}

func TestGetSource_RepositoryError(t *testing.T) {
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

	sourceRepo.EXPECT().
		FindByID(gomock.Any(), contextID, sourceID).
		Return(nil, errSourceRepositoryFailure)

	_, err = uc.GetSource(ctx, contextID, sourceID)
	require.Error(t, err)
	require.ErrorIs(t, err, errSourceRepositoryFailure)
	require.Contains(t, err.Error(), "finding reconciliation source")
}

func TestGetSource_Success(t *testing.T) {
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

	sourceRepo.EXPECT().FindByID(gomock.Any(), contextID, sourceID).Return(expected, nil)

	result, err := uc.GetSource(ctx, contextID, sourceID)
	require.NoError(t, err)
	require.Equal(t, expected, result)
}

func TestListSources_NilSourceRepo(t *testing.T) {
	t.Parallel()

	uc := &UseCase{}

	_, _, err := uc.ListSources(context.Background(), uuid.New(), "", 10, nil)
	require.ErrorIs(t, err, ErrNilSourceRepository)
}

func TestListSources_NilSourceTypeCallsFindByContextID(t *testing.T) {
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
	expected := []*entities.ReconciliationSource{{ID: uuid.New()}}

	sourceRepo.EXPECT().FindByContextID(gomock.Any(), contextID, "", 10).Return(expected, libHTTP.CursorPagination{}, nil)

	result, _, err := uc.ListSources(ctx, contextID, "", 10, nil)
	require.NoError(t, err)
	require.Equal(t, expected, result)
}

func TestListSources_WithSourceTypeCallsFindByContextIDAndType(t *testing.T) {
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
	sourceType := value_objects.SourceTypeBank
	expected := []*entities.ReconciliationSource{{ID: uuid.New()}}

	sourceRepo.EXPECT().
		FindByContextIDAndType(gomock.Any(), contextID, sourceType, "", 10).
		Return(expected, libHTTP.CursorPagination{}, nil)

	result, _, err := uc.ListSources(ctx, contextID, "", 10, &sourceType)
	require.NoError(t, err)
	require.Equal(t, expected, result)
}

func TestListSources_RepositoryError(t *testing.T) {
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

	sourceRepo.EXPECT().
		FindByContextID(gomock.Any(), contextID, "", 10).
		Return(nil, libHTTP.CursorPagination{}, errSourceRepositoryFailure)

	_, _, err = uc.ListSources(ctx, contextID, "", 10, nil)
	require.Error(t, err)
	require.ErrorIs(t, err, errSourceRepositoryFailure)
	require.Contains(t, err.Error(), "listing reconciliation sources")
}

func TestListSources_WithValidCursor(t *testing.T) {
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
	cursor := "cursor-value"
	expected := []*entities.ReconciliationSource{{ID: uuid.New()}}

	sourceRepo.EXPECT().
		FindByContextID(gomock.Any(), contextID, cursor, 10).
		Return(expected, libHTTP.CursorPagination{}, nil)

	result, _, err := uc.ListSources(ctx, contextID, cursor, 10, nil)
	require.NoError(t, err)
	require.Equal(t, expected, result)
}

func TestListSources_WithBoundaryCursor_EmptyResult(t *testing.T) {
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
	cursor := "cursor-value" // Cursor at end of results

	sourceRepo.EXPECT().
		FindByContextID(gomock.Any(), contextID, cursor, 10).
		Return([]*entities.ReconciliationSource{}, libHTTP.CursorPagination{}, nil)

	result, _, err := uc.ListSources(ctx, contextID, cursor, 10, nil)
	require.NoError(t, err)
	require.Empty(t, result)
}

func TestListSources_WithTypeFilter_RepositoryError(t *testing.T) {
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
	sourceType := value_objects.SourceTypeLedger

	sourceRepo.EXPECT().
		FindByContextIDAndType(gomock.Any(), contextID, sourceType, "", 10).
		Return(nil, libHTTP.CursorPagination{}, errSourceRepositoryFailure)

	_, _, err = uc.ListSources(ctx, contextID, "", 10, &sourceType)
	require.Error(t, err)
	require.ErrorIs(t, err, errSourceRepositoryFailure)
	require.Contains(t, err.Error(), "listing reconciliation sources")
}

func TestListSources_WithCursorAndType(t *testing.T) {
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
	cursor := "cursor-value"
	sourceType := value_objects.SourceTypeGateway
	expected := []*entities.ReconciliationSource{{ID: uuid.New()}, {ID: uuid.New()}}

	sourceRepo.EXPECT().
		FindByContextIDAndType(gomock.Any(), contextID, sourceType, cursor, 20).
		Return(expected, libHTTP.CursorPagination{}, nil)

	result, _, err := uc.ListSources(ctx, contextID, cursor, 20, &sourceType)
	require.NoError(t, err)
	require.Equal(t, expected, result)
	require.Len(t, result, 2)
}

func TestListSources_MultipleResults(t *testing.T) {
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
	expected := []*entities.ReconciliationSource{
		{ID: uuid.New()},
		{ID: uuid.New()},
		{ID: uuid.New()},
	}

	sourceRepo.EXPECT().FindByContextID(gomock.Any(), contextID, "", 50).Return(expected, libHTTP.CursorPagination{}, nil)

	result, _, err := uc.ListSources(ctx, contextID, "", 50, nil)
	require.NoError(t, err)
	require.Len(t, result, 3)
}
