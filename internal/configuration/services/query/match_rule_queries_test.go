// Copyright 2025 Lerian Studio. All rights reserved.
// Use of this source code is governed by an Elastic License 2.0
// that can be found in the LICENSE.md file.

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
	shared "github.com/LerianStudio/matcher/internal/shared/domain"
)

var errMatchRuleRepositoryFailure = errors.New("repository failure")

func TestListMatchRules_NilUseCase(t *testing.T) {
	t.Parallel()

	var uc *UseCase

	_, _, err := uc.ListMatchRules(context.Background(), uuid.New(), "", 10, nil)
	require.ErrorIs(t, err, ErrNilMatchRuleRepository)
}

func TestListMatchRules_NilMatchRuleRepo(t *testing.T) {
	t.Parallel()

	uc := &UseCase{}

	_, _, err := uc.ListMatchRules(context.Background(), uuid.New(), "", 10, nil)
	require.ErrorIs(t, err, ErrNilMatchRuleRepository)
}

func TestListMatchRules_NilRuleTypeCallsFindByContextID(t *testing.T) {
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
	expected := entities.MatchRules{{ID: uuid.New()}}

	matchRuleRepo.EXPECT().FindByContextID(gomock.Any(), contextID, "", 10).Return(expected, libHTTP.CursorPagination{}, nil)

	result, _, err := uc.ListMatchRules(ctx, contextID, "", 10, nil)
	require.NoError(t, err)
	require.Equal(t, expected, result)
}

func TestListMatchRules_WithRuleTypeCallsFindByContextIDAndType(t *testing.T) {
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
	ruleType := shared.RuleTypeExact
	expected := entities.MatchRules{{ID: uuid.New()}}

	matchRuleRepo.EXPECT().
		FindByContextIDAndType(gomock.Any(), contextID, ruleType, "", 10).
		Return(expected, libHTTP.CursorPagination{}, nil)

	result, _, err := uc.ListMatchRules(ctx, contextID, "", 10, &ruleType)
	require.NoError(t, err)
	require.Equal(t, expected, result)
}

func TestListMatchRules_RepositoryError(t *testing.T) {
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

	matchRuleRepo.EXPECT().
		FindByContextID(gomock.Any(), contextID, "", 10).
		Return(nil, libHTTP.CursorPagination{}, errMatchRuleRepositoryFailure)

	_, _, err = uc.ListMatchRules(ctx, contextID, "", 10, nil)
	require.Error(t, err)
	require.ErrorIs(t, err, errMatchRuleRepositoryFailure)
	require.Contains(t, err.Error(), "listing match rules")
}

func TestListMatchRules_WithTypeFilter_RepositoryError(t *testing.T) {
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
	ruleType := shared.RuleTypeTolerance

	matchRuleRepo.EXPECT().
		FindByContextIDAndType(gomock.Any(), contextID, ruleType, "", 10).
		Return(nil, libHTTP.CursorPagination{}, errMatchRuleRepositoryFailure)

	_, _, err = uc.ListMatchRules(ctx, contextID, "", 10, &ruleType)
	require.Error(t, err)
	require.ErrorIs(t, err, errMatchRuleRepositoryFailure)
	require.Contains(t, err.Error(), "listing match rules")
}

func TestListMatchRules_WithCursor(t *testing.T) {
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
	expected := entities.MatchRules{{ID: uuid.New()}, {ID: uuid.New()}}

	matchRuleRepo.EXPECT().
		FindByContextID(gomock.Any(), contextID, "cursor-value", 10).
		Return(expected, libHTTP.CursorPagination{}, nil)

	result, _, err := uc.ListMatchRules(ctx, contextID, "cursor-value", 10, nil)
	require.NoError(t, err)
	require.Equal(t, expected, result)
	require.Len(t, result, 2)
}

func TestListMatchRules_WithCursorAndType(t *testing.T) {
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
	ruleType := shared.RuleTypeDateLag
	expected := entities.MatchRules{{ID: uuid.New()}}

	matchRuleRepo.EXPECT().
		FindByContextIDAndType(gomock.Any(), contextID, ruleType, cursor, 25).
		Return(expected, libHTTP.CursorPagination{}, nil)

	result, _, err := uc.ListMatchRules(ctx, contextID, cursor, 25, &ruleType)
	require.NoError(t, err)
	require.Equal(t, expected, result)
}

func TestListMatchRules_EmptyResult(t *testing.T) {
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

	matchRuleRepo.EXPECT().
		FindByContextID(gomock.Any(), contextID, "", 10).
		Return(entities.MatchRules{}, libHTTP.CursorPagination{}, nil)

	result, _, err := uc.ListMatchRules(ctx, contextID, "", 10, nil)
	require.NoError(t, err)
	require.Empty(t, result)
}

func TestListMatchRules_MultipleResults(t *testing.T) {
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
	expected := entities.MatchRules{
		{ID: uuid.New()},
		{ID: uuid.New()},
		{ID: uuid.New()},
		{ID: uuid.New()},
	}

	matchRuleRepo.EXPECT().FindByContextID(gomock.Any(), contextID, "", 50).Return(expected, libHTTP.CursorPagination{}, nil)

	result, _, err := uc.ListMatchRules(ctx, contextID, "", 50, nil)
	require.NoError(t, err)
	require.Len(t, result, 4)
}
