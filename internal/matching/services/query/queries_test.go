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

	libHTTP "github.com/LerianStudio/lib-commons/v4/commons/net/http"

	matchingEntities "github.com/LerianStudio/matcher/internal/matching/domain/entities"
	matchingRepos "github.com/LerianStudio/matcher/internal/matching/domain/repositories"
	"github.com/LerianStudio/matcher/internal/matching/domain/repositories/mocks"
)

// errDBError is a sentinel error for database errors in tests.
var errDBError = errors.New("db error")

func TestNewUseCase_Success(t *testing.T) {
	t.Parallel()

	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	matchRunRepo := mocks.NewMockMatchRunRepository(ctrl)
	matchGroupRepo := mocks.NewMockMatchGroupRepository(ctrl)
	matchItemRepo := mocks.NewMockMatchItemRepository(ctrl)

	uc, err := NewUseCase(matchRunRepo, matchGroupRepo, matchItemRepo)

	require.NoError(t, err)
	require.NotNil(t, uc)
	assert.NotNil(t, uc.matchRunRepo)
	assert.NotNil(t, uc.matchGroupRepo)
	assert.NotNil(t, uc.matchItemRepo)
}

func TestNewUseCase_NilMatchRunRepository(t *testing.T) {
	t.Parallel()

	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	matchGroupRepo := mocks.NewMockMatchGroupRepository(ctrl)
	matchItemRepo := mocks.NewMockMatchItemRepository(ctrl)

	uc, err := NewUseCase(nil, matchGroupRepo, matchItemRepo)

	require.Nil(t, uc)
	require.ErrorIs(t, err, ErrNilMatchRunRepository)
}

func TestNewUseCase_NilMatchGroupRepository(t *testing.T) {
	t.Parallel()

	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	matchRunRepo := mocks.NewMockMatchRunRepository(ctrl)
	matchItemRepo := mocks.NewMockMatchItemRepository(ctrl)

	uc, err := NewUseCase(matchRunRepo, nil, matchItemRepo)

	require.Nil(t, uc)
	require.ErrorIs(t, err, ErrNilMatchGroupRepository)
}

func TestNewUseCase_NilMatchItemRepository(t *testing.T) {
	t.Parallel()

	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	matchRunRepo := mocks.NewMockMatchRunRepository(ctrl)
	matchGroupRepo := mocks.NewMockMatchGroupRepository(ctrl)

	uc, err := NewUseCase(matchRunRepo, matchGroupRepo, nil)

	require.Nil(t, uc)
	require.ErrorIs(t, err, ErrNilMatchItemRepository)
}

func TestGetMatchRun_Success(t *testing.T) {
	t.Parallel()

	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	matchRunRepo := mocks.NewMockMatchRunRepository(ctrl)
	matchGroupRepo := mocks.NewMockMatchGroupRepository(ctrl)
	matchItemRepo := mocks.NewMockMatchItemRepository(ctrl)

	uc, err := NewUseCase(matchRunRepo, matchGroupRepo, matchItemRepo)
	require.NoError(t, err)

	ctx := context.Background()
	contextID := uuid.New()
	runID := uuid.New()
	expected := &matchingEntities.MatchRun{ID: runID, ContextID: contextID}

	matchRunRepo.EXPECT().FindByID(gomock.Any(), contextID, runID).Return(expected, nil)

	result, err := uc.GetMatchRun(ctx, contextID, runID)
	require.NoError(t, err)
	require.Equal(t, expected, result)
}

func TestGetMatchRun_RepoError(t *testing.T) {
	t.Parallel()

	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	matchRunRepo := mocks.NewMockMatchRunRepository(ctrl)
	matchGroupRepo := mocks.NewMockMatchGroupRepository(ctrl)
	matchItemRepo := mocks.NewMockMatchItemRepository(ctrl)

	uc, err := NewUseCase(matchRunRepo, matchGroupRepo, matchItemRepo)
	require.NoError(t, err)

	ctx := context.Background()
	contextID := uuid.New()
	runID := uuid.New()

	matchRunRepo.EXPECT().FindByID(gomock.Any(), contextID, runID).Return(nil, errDBError)

	result, err := uc.GetMatchRun(ctx, contextID, runID)
	require.Nil(t, result)
	require.ErrorIs(t, err, errDBError)
}

func TestListMatchRuns_Success(t *testing.T) {
	t.Parallel()

	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	matchRunRepo := mocks.NewMockMatchRunRepository(ctrl)
	matchGroupRepo := mocks.NewMockMatchGroupRepository(ctrl)
	matchItemRepo := mocks.NewMockMatchItemRepository(ctrl)

	uc, err := NewUseCase(matchRunRepo, matchGroupRepo, matchItemRepo)
	require.NoError(t, err)

	ctx := context.Background()
	contextID := uuid.New()
	filter := matchingRepos.CursorFilter{Limit: 10, SortOrder: "desc"}
	expected := []*matchingEntities.MatchRun{{ID: uuid.New(), ContextID: contextID}}
	expectedPagination := libHTTP.CursorPagination{Next: "next-cursor"}

	matchRunRepo.EXPECT().
		ListByContextID(gomock.Any(), contextID, filter).
		Return(expected, expectedPagination, nil)

	result, pagination, err := uc.ListMatchRuns(ctx, contextID, filter)
	require.NoError(t, err)
	require.Equal(t, expected, result)
	require.Equal(t, expectedPagination, pagination)
}

func TestListMatchRuns_RepoError(t *testing.T) {
	t.Parallel()

	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	matchRunRepo := mocks.NewMockMatchRunRepository(ctrl)
	matchGroupRepo := mocks.NewMockMatchGroupRepository(ctrl)
	matchItemRepo := mocks.NewMockMatchItemRepository(ctrl)

	uc, err := NewUseCase(matchRunRepo, matchGroupRepo, matchItemRepo)
	require.NoError(t, err)

	ctx := context.Background()
	contextID := uuid.New()
	filter := matchingRepos.CursorFilter{Limit: 10, SortOrder: "desc"}

	matchRunRepo.EXPECT().
		ListByContextID(gomock.Any(), contextID, filter).
		Return(nil, libHTTP.CursorPagination{}, errDBError)

	result, pagination, err := uc.ListMatchRuns(ctx, contextID, filter)
	require.Nil(t, result)
	require.Equal(t, libHTTP.CursorPagination{}, pagination)
	require.ErrorIs(t, err, errDBError)
}

func TestListMatchRunGroups_Success(t *testing.T) {
	t.Parallel()

	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	matchRunRepo := mocks.NewMockMatchRunRepository(ctrl)
	matchGroupRepo := mocks.NewMockMatchGroupRepository(ctrl)
	matchItemRepo := mocks.NewMockMatchItemRepository(ctrl)

	uc, err := NewUseCase(matchRunRepo, matchGroupRepo, matchItemRepo)
	require.NoError(t, err)

	ctx := context.Background()
	contextID := uuid.New()
	runID := uuid.New()
	filter := matchingRepos.CursorFilter{Limit: 10, SortOrder: "desc"}
	expected := []*matchingEntities.MatchGroup{{ID: uuid.New(), ContextID: contextID}}
	expectedPagination := libHTTP.CursorPagination{Next: "next-cursor"}

	matchGroupRepo.EXPECT().
		ListByRunID(gomock.Any(), contextID, runID, filter).
		Return(expected, expectedPagination, nil)

	matchItemRepo.EXPECT().
		ListByMatchGroupIDs(gomock.Any(), gomock.Any()).
		Return(make(map[uuid.UUID][]*matchingEntities.MatchItem), nil)

	result, pagination, err := uc.ListMatchRunGroups(ctx, contextID, runID, filter)
	require.NoError(t, err)
	require.Equal(t, expected, result)
	require.Equal(t, expectedPagination, pagination)
}

func TestListMatchRunGroups_RepoError(t *testing.T) {
	t.Parallel()

	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	matchRunRepo := mocks.NewMockMatchRunRepository(ctrl)
	matchGroupRepo := mocks.NewMockMatchGroupRepository(ctrl)
	matchItemRepo := mocks.NewMockMatchItemRepository(ctrl)

	uc, err := NewUseCase(matchRunRepo, matchGroupRepo, matchItemRepo)
	require.NoError(t, err)

	ctx := context.Background()
	contextID := uuid.New()
	runID := uuid.New()
	filter := matchingRepos.CursorFilter{Limit: 10, SortOrder: "desc"}

	matchGroupRepo.EXPECT().
		ListByRunID(gomock.Any(), contextID, runID, filter).
		Return(nil, libHTTP.CursorPagination{}, errDBError)

	result, pagination, err := uc.ListMatchRunGroups(ctx, contextID, runID, filter)
	require.Nil(t, result)
	require.Equal(t, libHTTP.CursorPagination{}, pagination)
	require.ErrorIs(t, err, errDBError)
}

func TestFindMatchGroupByID_Success(t *testing.T) {
	t.Parallel()

	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	matchRunRepo := mocks.NewMockMatchRunRepository(ctrl)
	matchGroupRepo := mocks.NewMockMatchGroupRepository(ctrl)
	matchItemRepo := mocks.NewMockMatchItemRepository(ctrl)

	uc, err := NewUseCase(matchRunRepo, matchGroupRepo, matchItemRepo)
	require.NoError(t, err)

	ctx := context.Background()
	contextID := uuid.New()
	groupID := uuid.New()
	expected := &matchingEntities.MatchGroup{ID: groupID, ContextID: contextID}

	matchGroupRepo.EXPECT().FindByID(gomock.Any(), contextID, groupID).Return(expected, nil)

	result, err := uc.FindMatchGroupByID(ctx, contextID, groupID)
	require.NoError(t, err)
	require.Equal(t, expected, result)
}

func TestFindMatchGroupByID_RepoError(t *testing.T) {
	t.Parallel()

	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	matchRunRepo := mocks.NewMockMatchRunRepository(ctrl)
	matchGroupRepo := mocks.NewMockMatchGroupRepository(ctrl)
	matchItemRepo := mocks.NewMockMatchItemRepository(ctrl)

	uc, err := NewUseCase(matchRunRepo, matchGroupRepo, matchItemRepo)
	require.NoError(t, err)

	ctx := context.Background()
	contextID := uuid.New()
	groupID := uuid.New()

	matchGroupRepo.EXPECT().FindByID(gomock.Any(), contextID, groupID).Return(nil, errDBError)

	result, err := uc.FindMatchGroupByID(ctx, contextID, groupID)
	require.Nil(t, result)
	require.ErrorIs(t, err, errDBError)
}

func TestListMatchRunGroups_EmptyGroups(t *testing.T) {
	t.Parallel()

	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	matchRunRepo := mocks.NewMockMatchRunRepository(ctrl)
	matchGroupRepo := mocks.NewMockMatchGroupRepository(ctrl)
	matchItemRepo := mocks.NewMockMatchItemRepository(ctrl)

	uc, err := NewUseCase(matchRunRepo, matchGroupRepo, matchItemRepo)
	require.NoError(t, err)

	ctx := context.Background()
	contextID := uuid.New()
	runID := uuid.New()
	filter := matchingRepos.CursorFilter{Limit: 10, SortOrder: "desc"}
	expectedPagination := libHTTP.CursorPagination{Next: "next-cursor"}

	// Group repo returns empty slice.
	matchGroupRepo.EXPECT().
		ListByRunID(gomock.Any(), contextID, runID, filter).
		Return([]*matchingEntities.MatchGroup{}, expectedPagination, nil)

	// ListByMatchGroupIDs must NOT be called when groups is empty.
	// gomock will fail if an unexpected call is made.

	result, pagination, err := uc.ListMatchRunGroups(ctx, contextID, runID, filter)
	require.NoError(t, err)
	require.Empty(t, result)
	require.Equal(t, expectedPagination, pagination)
}

func TestListMatchRunGroups_NilItemRepo(t *testing.T) {
	t.Parallel()

	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	matchRunRepo := mocks.NewMockMatchRunRepository(ctrl)
	matchGroupRepo := mocks.NewMockMatchGroupRepository(ctrl)

	// Bypass NewUseCase validation to set matchItemRepo to nil.
	uc := &UseCase{
		matchRunRepo:   matchRunRepo,
		matchGroupRepo: matchGroupRepo,
		matchItemRepo:  nil,
	}

	ctx := context.Background()
	contextID := uuid.New()
	runID := uuid.New()
	filter := matchingRepos.CursorFilter{Limit: 10, SortOrder: "desc"}

	groupID := uuid.New()
	groups := []*matchingEntities.MatchGroup{{ID: groupID, ContextID: contextID}}
	expectedPagination := libHTTP.CursorPagination{Next: "next-cursor"}

	matchGroupRepo.EXPECT().
		ListByRunID(gomock.Any(), contextID, runID, filter).
		Return(groups, expectedPagination, nil)

	// No item repo calls expected — matchItemRepo is nil.
	result, pagination, err := uc.ListMatchRunGroups(ctx, contextID, runID, filter)
	require.NoError(t, err)
	require.Len(t, result, 1)
	require.Equal(t, groupID, result[0].ID)
	// Items should remain nil (not enriched).
	assert.Nil(t, result[0].Items)
	require.Equal(t, expectedPagination, pagination)
}

func TestListMatchRunGroups_ItemFetchError(t *testing.T) {
	t.Parallel()

	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	matchRunRepo := mocks.NewMockMatchRunRepository(ctrl)
	matchGroupRepo := mocks.NewMockMatchGroupRepository(ctrl)
	matchItemRepo := mocks.NewMockMatchItemRepository(ctrl)

	uc, err := NewUseCase(matchRunRepo, matchGroupRepo, matchItemRepo)
	require.NoError(t, err)

	ctx := context.Background()
	contextID := uuid.New()
	runID := uuid.New()
	filter := matchingRepos.CursorFilter{Limit: 10, SortOrder: "desc"}

	groupID := uuid.New()
	groups := []*matchingEntities.MatchGroup{{ID: groupID, ContextID: contextID}}
	expectedPagination := libHTTP.CursorPagination{Next: "next-cursor"}

	matchGroupRepo.EXPECT().
		ListByRunID(gomock.Any(), contextID, runID, filter).
		Return(groups, expectedPagination, nil)

	// Item repo returns an error — enrichment should degrade gracefully.
	matchItemRepo.EXPECT().
		ListByMatchGroupIDs(gomock.Any(), []uuid.UUID{groupID}).
		Return(nil, errDBError)

	result, pagination, err := uc.ListMatchRunGroups(ctx, contextID, runID, filter)
	// No error propagated — groups are returned without items.
	require.NoError(t, err)
	require.Len(t, result, 1)
	require.Equal(t, groupID, result[0].ID)
	// Items not enriched due to error.
	assert.Nil(t, result[0].Items)
	require.Equal(t, expectedPagination, pagination)
}

func TestListMatchRunGroups_ItemEnrichment(t *testing.T) {
	t.Parallel()

	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	matchRunRepo := mocks.NewMockMatchRunRepository(ctrl)
	matchGroupRepo := mocks.NewMockMatchGroupRepository(ctrl)
	matchItemRepo := mocks.NewMockMatchItemRepository(ctrl)

	uc, err := NewUseCase(matchRunRepo, matchGroupRepo, matchItemRepo)
	require.NoError(t, err)

	ctx := context.Background()
	contextID := uuid.New()
	runID := uuid.New()
	filter := matchingRepos.CursorFilter{Limit: 10, SortOrder: "desc"}

	groupID1 := uuid.New()
	groupID2 := uuid.New()
	groups := []*matchingEntities.MatchGroup{
		{ID: groupID1, ContextID: contextID},
		{ID: groupID2, ContextID: contextID},
	}
	expectedPagination := libHTTP.CursorPagination{Next: "next-cursor"}

	// Build items to be returned by the item repo.
	item1 := &matchingEntities.MatchItem{ID: uuid.New(), MatchGroupID: groupID1, TransactionID: uuid.New()}
	item2 := &matchingEntities.MatchItem{ID: uuid.New(), MatchGroupID: groupID1, TransactionID: uuid.New()}
	item3 := &matchingEntities.MatchItem{ID: uuid.New(), MatchGroupID: groupID2, TransactionID: uuid.New()}

	itemsByGroup := map[uuid.UUID][]*matchingEntities.MatchItem{
		groupID1: {item1, item2},
		groupID2: {item3},
	}

	matchGroupRepo.EXPECT().
		ListByRunID(gomock.Any(), contextID, runID, filter).
		Return(groups, expectedPagination, nil)

	matchItemRepo.EXPECT().
		ListByMatchGroupIDs(gomock.Any(), gomock.Any()).
		Return(itemsByGroup, nil)

	result, pagination, err := uc.ListMatchRunGroups(ctx, contextID, runID, filter)
	require.NoError(t, err)
	require.Len(t, result, 2)
	require.Equal(t, expectedPagination, pagination)

	// Verify items are correctly attached to their respective groups.
	require.Len(t, result[0].Items, 2)
	assert.Equal(t, item1.ID, result[0].Items[0].ID)
	assert.Equal(t, item2.ID, result[0].Items[1].ID)

	require.Len(t, result[1].Items, 1)
	assert.Equal(t, item3.ID, result[1].Items[0].ID)
}

func TestSentinelErrors(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name    string
		err     error
		message string
	}{
		{
			name:    "ErrNilMatchRunRepository",
			err:     ErrNilMatchRunRepository,
			message: "match run repository is required",
		},
		{
			name:    "ErrNilMatchGroupRepository",
			err:     ErrNilMatchGroupRepository,
			message: "match group repository is required",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			require.Error(t, tt.err)
			assert.Equal(t, tt.message, tt.err.Error())
		})
	}
}
