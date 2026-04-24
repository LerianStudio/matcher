// Copyright 2025 Lerian Studio. All rights reserved.
// Use of this source code is governed by an Elastic License 2.0
// that can be found in the LICENSE.md file.

//go:build unit

package command

import (
	"context"
	"database/sql"
	"errors"
	"testing"

	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.uber.org/mock/gomock"

	"github.com/LerianStudio/matcher/internal/configuration/domain/entities"
	repoMocks "github.com/LerianStudio/matcher/internal/configuration/domain/repositories/mocks"
	portMocks "github.com/LerianStudio/matcher/internal/configuration/ports/mocks"
	shared "github.com/LerianStudio/matcher/internal/shared/domain"
)

func TestUpdateContext_CommandValidation(t *testing.T) {
	t.Parallel()

	tenantID := uuid.New()
	ctx := context.Background()
	existing, err := entities.NewReconciliationContext(
		ctx,
		tenantID,
		entities.CreateReconciliationContextInput{
			Name:     "Context",
			Type:     shared.ContextTypeOneToOne,
			Interval: "0 0 * * *",
		},
	)
	require.NoError(t, err)

	repo := &contextRepoStub{
		findByIDFn: func(_ context.Context, _ uuid.UUID) (*entities.ReconciliationContext, error) {
			return existing, nil
		},
		findByNameFn: func(_ context.Context, _ string) (*entities.ReconciliationContext, error) {
			return nil, nil // No duplicate
		},
		updateFn: func(_ context.Context, entity *entities.ReconciliationContext) (*entities.ReconciliationContext, error) {
			return entity, nil
		},
	}
	useCase, err := NewUseCase(repo, &sourceRepoStub{}, &fieldMapRepoStub{}, &matchRuleRepoStub{})
	require.NoError(t, err)

	emptyName := ""
	_, err = useCase.UpdateContext(
		context.Background(),
		existing.ID,
		entities.UpdateReconciliationContextInput{Name: &emptyName},
	)
	require.Error(t, err)
	assert.Equal(t, entities.ErrContextNameRequired, err)
}

func TestUpdateContext_CommandSuccess(t *testing.T) {
	t.Parallel()

	tenantID := uuid.New()
	ctx := context.Background()
	existing, err := entities.NewReconciliationContext(
		ctx,
		tenantID,
		entities.CreateReconciliationContextInput{
			Name:     "Original",
			Type:     shared.ContextTypeOneToOne,
			Interval: "0 0 * * *",
		},
	)
	require.NoError(t, err)

	repo := &contextRepoStub{
		findByIDFn: func(_ context.Context, _ uuid.UUID) (*entities.ReconciliationContext, error) {
			return existing, nil
		},
		findByNameFn: func(_ context.Context, _ string) (*entities.ReconciliationContext, error) {
			return nil, nil // No duplicate
		},
		updateFn: func(_ context.Context, entity *entities.ReconciliationContext) (*entities.ReconciliationContext, error) {
			return entity, nil
		},
	}
	useCase, err := NewUseCase(repo, &sourceRepoStub{}, &fieldMapRepoStub{}, &matchRuleRepoStub{})
	require.NoError(t, err)

	newName := "Updated Name"
	updated, err := useCase.UpdateContext(
		context.Background(),
		existing.ID,
		entities.UpdateReconciliationContextInput{Name: &newName},
	)
	require.NoError(t, err)
	assert.Equal(t, newName, updated.Name)
}

func TestUpdateContext_NilUseCase(t *testing.T) {
	t.Parallel()

	var uc *UseCase

	_, err := uc.UpdateContext(
		context.Background(),
		uuid.New(),
		entities.UpdateReconciliationContextInput{},
	)
	require.ErrorIs(t, err, ErrNilContextRepository)
}

func TestUpdateContext_NilContextRepo(t *testing.T) {
	t.Parallel()

	uc := &UseCase{
		contextRepo: nil,
	}

	_, err := uc.UpdateContext(
		context.Background(),
		uuid.New(),
		entities.UpdateReconciliationContextInput{},
	)
	require.ErrorIs(t, err, ErrNilContextRepository)
}

func TestUpdateContext_FindByIDError(t *testing.T) {
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

	_, err = uc.UpdateContext(
		context.Background(),
		contextID,
		entities.UpdateReconciliationContextInput{},
	)
	require.Error(t, err)
	require.ErrorContains(t, err, "finding reconciliation context")
}

func TestUpdateContext_RepositoryUpdateError(t *testing.T) {
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
			Name:     "Original",
			Type:     shared.ContextTypeOneToOne,
			Interval: "0 0 * * *",
		},
	)
	require.NoError(t, err)

	updateErr := errors.New("update failed")

	mockCtxRepo.EXPECT().
		FindByID(gomock.Any(), existing.ID).
		Return(existing, nil)

	mockCtxRepo.EXPECT().
		FindByName(gomock.Any(), "Updated Name").
		Return(nil, sql.ErrNoRows)

	mockCtxRepo.EXPECT().
		Update(gomock.Any(), gomock.Any()).
		Return(nil, updateErr)

	uc, err := NewUseCase(mockCtxRepo, mockSrcRepo, mockFmRepo, mockMrRepo)
	require.NoError(t, err)

	newName := "Updated Name"
	_, err = uc.UpdateContext(
		context.Background(),
		existing.ID,
		entities.UpdateReconciliationContextInput{Name: &newName},
	)
	require.Error(t, err)
	require.ErrorContains(t, err, "updating reconciliation context")
}

func TestUpdateContext_WithAuditPublisher(t *testing.T) {
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
			Name:     "Original",
			Type:     shared.ContextTypeOneToOne,
			Interval: "0 0 * * *",
		},
	)
	require.NoError(t, err)

	mockCtxRepo.EXPECT().
		FindByID(gomock.Any(), existing.ID).
		Return(existing, nil)

	mockCtxRepo.EXPECT().
		FindByName(gomock.Any(), "Updated Name").
		Return(nil, sql.ErrNoRows)

	mockCtxRepo.EXPECT().
		Update(gomock.Any(), gomock.Any()).
		DoAndReturn(func(ctx context.Context, entity *entities.ReconciliationContext) (*entities.ReconciliationContext, error) {
			return entity, nil
		})

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

	newName := "Updated Name"
	result, err := uc.UpdateContext(
		context.Background(),
		existing.ID,
		entities.UpdateReconciliationContextInput{Name: &newName},
	)
	require.NoError(t, err)
	assert.Equal(t, newName, result.Name)
}
