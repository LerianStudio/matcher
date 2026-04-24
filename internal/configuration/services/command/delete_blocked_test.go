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
	shared "github.com/LerianStudio/matcher/internal/shared/domain"
	sharedfee "github.com/LerianStudio/matcher/internal/shared/domain/fee"
)

func TestDeleteContext_BlockedBySchedules(t *testing.T) {
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
			Name:     "Has Schedules",
			Type:     shared.ContextTypeOneToOne,
			Interval: "0 0 * * *",
		},
	)
	require.NoError(t, err)

	mockCtxRepo.EXPECT().
		FindByID(gomock.Any(), existing.ID).
		Return(existing, nil)

	// No sources or rules — those checks pass.
	mockSrcRepo.EXPECT().
		FindByContextID(gomock.Any(), existing.ID, "", 1).
		Return(nil, libHTTP.CursorPagination{}, nil)

	mockMrRepo.EXPECT().
		FindByContextID(gomock.Any(), existing.ID, "", 1).
		Return(nil, libHTTP.CursorPagination{}, nil)

	scheduleRepo := &mockScheduleRepo{
		findByContextIDFn: func(_ context.Context, _ uuid.UUID) ([]*entities.ReconciliationSchedule, error) {
			return []*entities.ReconciliationSchedule{
				{ID: uuid.New(), ContextID: existing.ID},
			}, nil
		},
	}

	uc, err := NewUseCase(
		mockCtxRepo,
		mockSrcRepo,
		mockFmRepo,
		mockMrRepo,
		WithScheduleRepository(scheduleRepo),
	)
	require.NoError(t, err)

	err = uc.DeleteContext(context.Background(), existing.ID)
	require.ErrorIs(t, err, ErrContextHasChildEntities)
}

func TestDeleteContext_BlockedByFeeRules(t *testing.T) {
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
			Name:     "Has Fee Rules",
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

	mockMrRepo.EXPECT().
		FindByContextID(gomock.Any(), existing.ID, "", 1).
		Return(nil, libHTTP.CursorPagination{}, nil)

	feeRuleRepo := &feeRuleRepoStub{
		findByContextIDFn: func(_ context.Context, _ uuid.UUID) ([]*sharedfee.FeeRule, error) {
			return []*sharedfee.FeeRule{{ID: uuid.New(), ContextID: existing.ID}}, nil
		},
	}

	uc, err := NewUseCase(
		mockCtxRepo,
		mockSrcRepo,
		mockFmRepo,
		mockMrRepo,
		WithFeeRuleRepository(feeRuleRepo),
	)
	require.NoError(t, err)

	err = uc.DeleteContext(context.Background(), existing.ID)
	require.ErrorIs(t, err, ErrContextHasChildEntities)
}

func TestDeleteContext_FeeRuleCheckError(t *testing.T) {
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
			Name:     "Fee Rule Error",
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

	mockMrRepo.EXPECT().
		FindByContextID(gomock.Any(), existing.ID, "", 1).
		Return(nil, libHTTP.CursorPagination{}, nil)

	checkErr := errors.New("fee rule repo unavailable")
	feeRuleRepo := &feeRuleRepoStub{
		findByContextIDFn: func(_ context.Context, _ uuid.UUID) ([]*sharedfee.FeeRule, error) {
			return nil, checkErr
		},
	}

	uc, err := NewUseCase(
		mockCtxRepo,
		mockSrcRepo,
		mockFmRepo,
		mockMrRepo,
		WithFeeRuleRepository(feeRuleRepo),
	)
	require.NoError(t, err)

	err = uc.DeleteContext(context.Background(), existing.ID)
	require.Error(t, err)
	require.ErrorContains(t, err, "checking context fee rules")
	require.ErrorIs(t, err, checkErr)
}

func TestDeleteContext_ScheduleCheckError(t *testing.T) {
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
			Name:     "Schedule Error",
			Type:     shared.ContextTypeOneToOne,
			Interval: "0 0 * * *",
		},
	)
	require.NoError(t, err)

	mockCtxRepo.EXPECT().
		FindByID(gomock.Any(), existing.ID).
		Return(existing, nil)

	// No sources or rules — those checks pass.
	mockSrcRepo.EXPECT().
		FindByContextID(gomock.Any(), existing.ID, "", 1).
		Return(nil, libHTTP.CursorPagination{}, nil)

	mockMrRepo.EXPECT().
		FindByContextID(gomock.Any(), existing.ID, "", 1).
		Return(nil, libHTTP.CursorPagination{}, nil)

	scheduleCheckErr := errors.New("schedule repo unavailable")
	scheduleRepo := &mockScheduleRepo{
		findByContextIDFn: func(_ context.Context, _ uuid.UUID) ([]*entities.ReconciliationSchedule, error) {
			return nil, scheduleCheckErr
		},
	}

	uc, err := NewUseCase(
		mockCtxRepo,
		mockSrcRepo,
		mockFmRepo,
		mockMrRepo,
		WithScheduleRepository(scheduleRepo),
	)
	require.NoError(t, err)

	err = uc.DeleteContext(context.Background(), existing.ID)
	require.Error(t, err)
	require.ErrorContains(t, err, "checking context schedules")
	require.ErrorIs(t, err, scheduleCheckErr)
}
