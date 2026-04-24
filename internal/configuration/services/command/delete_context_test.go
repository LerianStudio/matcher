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
	"github.com/stretchr/testify/require"
	"go.uber.org/mock/gomock"

	libHTTP "github.com/LerianStudio/lib-commons/v5/commons/net/http"

	"github.com/LerianStudio/matcher/internal/configuration/domain/entities"
	repoMocks "github.com/LerianStudio/matcher/internal/configuration/domain/repositories/mocks"
	portMocks "github.com/LerianStudio/matcher/internal/configuration/ports/mocks"
	shared "github.com/LerianStudio/matcher/internal/shared/domain"
)

func TestDeleteContext_NilUseCase(t *testing.T) {
	t.Parallel()

	var uc *UseCase

	err := uc.DeleteContext(context.Background(), uuid.New())
	require.ErrorIs(t, err, ErrNilContextRepository)
}

func TestDeleteContext_NilContextRepo(t *testing.T) {
	t.Parallel()

	uc := &UseCase{
		contextRepo: nil,
	}

	err := uc.DeleteContext(context.Background(), uuid.New())
	require.ErrorIs(t, err, ErrNilContextRepository)
}

func TestDeleteContext_FindByIDError(t *testing.T) {
	t.Parallel()

	ctrl := gomock.NewController(t)

	mockCtxRepo := repoMocks.NewMockContextRepository(ctrl)
	mockSrcRepo := repoMocks.NewMockSourceRepository(ctrl)
	mockFmRepo := repoMocks.NewMockFieldMapRepository(ctrl)
	mockMrRepo := repoMocks.NewMockMatchRuleRepository(ctrl)

	contextID := uuid.New()
	findErr := errors.New("context not found")

	mockCtxRepo.EXPECT().
		FindByID(gomock.Any(), contextID).
		Return(nil, findErr)

	uc, err := NewUseCase(mockCtxRepo, mockSrcRepo, mockFmRepo, mockMrRepo)
	require.NoError(t, err)

	err = uc.DeleteContext(context.Background(), contextID)
	require.Error(t, err)
	require.ErrorContains(t, err, "finding reconciliation context")
}

func TestDeleteContext_DeleteError(t *testing.T) {
	t.Parallel()

	ctrl := gomock.NewController(t)

	mockCtxRepo := repoMocks.NewMockContextRepository(ctrl)
	mockSrcRepo := repoMocks.NewMockSourceRepository(ctrl)
	mockFmRepo := repoMocks.NewMockFieldMapRepository(ctrl)
	mockMrRepo := repoMocks.NewMockMatchRuleRepository(ctrl)

	tenantID := uuid.New()
	existing, err := entities.NewReconciliationContext(
		context.Background(),
		tenantID,
		entities.CreateReconciliationContextInput{
			Name:     "To Delete",
			Type:     shared.ContextTypeOneToOne,
			Interval: "0 0 * * *",
		},
	)
	require.NoError(t, err)

	deleteErr := errors.New("delete failed")

	mockCtxRepo.EXPECT().
		FindByID(gomock.Any(), existing.ID).
		Return(existing, nil)

	// Child entity checks: no children exist.
	mockSrcRepo.EXPECT().
		FindByContextID(gomock.Any(), existing.ID, "", 1).
		Return(nil, libHTTP.CursorPagination{}, nil)

	mockMrRepo.EXPECT().
		FindByContextID(gomock.Any(), existing.ID, "", 1).
		Return(nil, libHTTP.CursorPagination{}, nil)

	mockCtxRepo.EXPECT().
		Delete(gomock.Any(), existing.ID).
		Return(deleteErr)

	uc, err := NewUseCase(mockCtxRepo, mockSrcRepo, mockFmRepo, mockMrRepo)
	require.NoError(t, err)

	err = uc.DeleteContext(context.Background(), existing.ID)
	require.Error(t, err)
	require.ErrorContains(t, err, "deleting reconciliation context")
}

func TestDeleteContext_Success(t *testing.T) {
	t.Parallel()

	ctrl := gomock.NewController(t)

	mockCtxRepo := repoMocks.NewMockContextRepository(ctrl)
	mockSrcRepo := repoMocks.NewMockSourceRepository(ctrl)
	mockFmRepo := repoMocks.NewMockFieldMapRepository(ctrl)
	mockMrRepo := repoMocks.NewMockMatchRuleRepository(ctrl)

	tenantID := uuid.New()
	existing, err := entities.NewReconciliationContext(
		context.Background(),
		tenantID,
		entities.CreateReconciliationContextInput{
			Name:     "To Delete",
			Type:     shared.ContextTypeOneToOne,
			Interval: "0 0 * * *",
		},
	)
	require.NoError(t, err)

	mockCtxRepo.EXPECT().
		FindByID(gomock.Any(), existing.ID).
		Return(existing, nil)

	// Child entity checks: no children exist.
	mockSrcRepo.EXPECT().
		FindByContextID(gomock.Any(), existing.ID, "", 1).
		Return(nil, libHTTP.CursorPagination{}, nil)

	mockMrRepo.EXPECT().
		FindByContextID(gomock.Any(), existing.ID, "", 1).
		Return(nil, libHTTP.CursorPagination{}, nil)

	mockCtxRepo.EXPECT().
		Delete(gomock.Any(), existing.ID).
		Return(nil)

	uc, err := NewUseCase(mockCtxRepo, mockSrcRepo, mockFmRepo, mockMrRepo)
	require.NoError(t, err)

	err = uc.DeleteContext(context.Background(), existing.ID)
	require.NoError(t, err)
}

func TestDeleteContext_WithAuditPublisher(t *testing.T) {
	t.Parallel()

	ctrl := gomock.NewController(t)

	mockCtxRepo := repoMocks.NewMockContextRepository(ctrl)
	mockSrcRepo := repoMocks.NewMockSourceRepository(ctrl)
	mockFmRepo := repoMocks.NewMockFieldMapRepository(ctrl)
	mockMrRepo := repoMocks.NewMockMatchRuleRepository(ctrl)
	mockAuditPub := portMocks.NewMockAuditPublisher(ctrl)

	tenantID := uuid.New()
	existing, err := entities.NewReconciliationContext(
		context.Background(),
		tenantID,
		entities.CreateReconciliationContextInput{
			Name:     "To Delete",
			Type:     shared.ContextTypeOneToOne,
			Interval: "0 0 * * *",
		},
	)
	require.NoError(t, err)

	mockCtxRepo.EXPECT().
		FindByID(gomock.Any(), existing.ID).
		Return(existing, nil)

	// Child entity checks: no children exist.
	mockSrcRepo.EXPECT().
		FindByContextID(gomock.Any(), existing.ID, "", 1).
		Return(nil, libHTTP.CursorPagination{}, nil)

	mockMrRepo.EXPECT().
		FindByContextID(gomock.Any(), existing.ID, "", 1).
		Return(nil, libHTTP.CursorPagination{}, nil)

	mockCtxRepo.EXPECT().
		Delete(gomock.Any(), existing.ID).
		Return(nil)

	mockAuditPub.EXPECT().
		Publish(gomock.Any(), gomock.Any()).
		Return(nil)

	uc, err := NewUseCase(
		mockCtxRepo,
		mockSrcRepo,
		mockFmRepo,
		mockMrRepo,
		WithAuditPublisher(mockAuditPub),
	)
	require.NoError(t, err)

	err = uc.DeleteContext(context.Background(), existing.ID)
	require.NoError(t, err)
}

func TestDeleteContext_BlockedBySources(t *testing.T) {
	t.Parallel()

	ctrl := gomock.NewController(t)

	mockCtxRepo := repoMocks.NewMockContextRepository(ctrl)
	mockSrcRepo := repoMocks.NewMockSourceRepository(ctrl)
	mockFmRepo := repoMocks.NewMockFieldMapRepository(ctrl)
	mockMrRepo := repoMocks.NewMockMatchRuleRepository(ctrl)

	tenantID := uuid.New()
	existing, err := entities.NewReconciliationContext(
		context.Background(),
		tenantID,
		entities.CreateReconciliationContextInput{
			Name:     "Has Sources",
			Type:     shared.ContextTypeOneToOne,
			Interval: "0 0 * * *",
		},
	)
	require.NoError(t, err)

	mockCtxRepo.EXPECT().
		FindByID(gomock.Any(), existing.ID).
		Return(existing, nil)

	childSource := &entities.ReconciliationSource{ID: uuid.New(), ContextID: existing.ID}
	mockSrcRepo.EXPECT().
		FindByContextID(gomock.Any(), existing.ID, "", 1).
		Return([]*entities.ReconciliationSource{childSource}, libHTTP.CursorPagination{}, nil)

	uc, err := NewUseCase(mockCtxRepo, mockSrcRepo, mockFmRepo, mockMrRepo)
	require.NoError(t, err)

	err = uc.DeleteContext(context.Background(), existing.ID)
	require.ErrorIs(t, err, ErrContextHasChildEntities)
}

func TestDeleteContext_BlockedByMatchRules(t *testing.T) {
	t.Parallel()

	ctrl := gomock.NewController(t)

	mockCtxRepo := repoMocks.NewMockContextRepository(ctrl)
	mockSrcRepo := repoMocks.NewMockSourceRepository(ctrl)
	mockFmRepo := repoMocks.NewMockFieldMapRepository(ctrl)
	mockMrRepo := repoMocks.NewMockMatchRuleRepository(ctrl)

	tenantID := uuid.New()
	existing, err := entities.NewReconciliationContext(
		context.Background(),
		tenantID,
		entities.CreateReconciliationContextInput{
			Name:     "Has Rules",
			Type:     shared.ContextTypeOneToOne,
			Interval: "0 0 * * *",
		},
	)
	require.NoError(t, err)

	mockCtxRepo.EXPECT().
		FindByID(gomock.Any(), existing.ID).
		Return(existing, nil)

	mockSrcRepo.EXPECT().
		FindByContextID(gomock.Any(), existing.ID, "", 1).
		Return(nil, libHTTP.CursorPagination{}, nil)

	childRule := &entities.MatchRule{ID: uuid.New()}
	mockMrRepo.EXPECT().
		FindByContextID(gomock.Any(), existing.ID, "", 1).
		Return(entities.MatchRules{childRule}, libHTTP.CursorPagination{}, nil)

	uc, err := NewUseCase(mockCtxRepo, mockSrcRepo, mockFmRepo, mockMrRepo)
	require.NoError(t, err)

	err = uc.DeleteContext(context.Background(), existing.ID)
	require.ErrorIs(t, err, ErrContextHasChildEntities)
}

func TestDeleteContext_SourceCheckError(t *testing.T) {
	t.Parallel()

	ctrl := gomock.NewController(t)

	mockCtxRepo := repoMocks.NewMockContextRepository(ctrl)
	mockSrcRepo := repoMocks.NewMockSourceRepository(ctrl)
	mockFmRepo := repoMocks.NewMockFieldMapRepository(ctrl)
	mockMrRepo := repoMocks.NewMockMatchRuleRepository(ctrl)

	tenantID := uuid.New()
	existing, err := entities.NewReconciliationContext(
		context.Background(),
		tenantID,
		entities.CreateReconciliationContextInput{
			Name:     "Source Error",
			Type:     shared.ContextTypeOneToOne,
			Interval: "0 0 * * *",
		},
	)
	require.NoError(t, err)

	mockCtxRepo.EXPECT().
		FindByID(gomock.Any(), existing.ID).
		Return(existing, nil)

	checkErr := errors.New("source repo unavailable")
	mockSrcRepo.EXPECT().
		FindByContextID(gomock.Any(), existing.ID, "", 1).
		Return(nil, libHTTP.CursorPagination{}, checkErr)

	uc, err := NewUseCase(mockCtxRepo, mockSrcRepo, mockFmRepo, mockMrRepo)
	require.NoError(t, err)

	err = uc.DeleteContext(context.Background(), existing.ID)
	require.Error(t, err)
	require.ErrorContains(t, err, "checking context sources")
	require.ErrorIs(t, err, checkErr)
}
