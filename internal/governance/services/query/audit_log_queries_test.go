// Copyright 2025 Lerian Studio. All rights reserved.
// Use of this source code is governed by an Elastic License 2.0
// that can be found in the LICENSE.md file.

//go:build unit

package query

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.uber.org/mock/gomock"

	sharedhttp "github.com/LerianStudio/lib-commons/v5/commons/net/http"

	governanceErrors "github.com/LerianStudio/matcher/internal/governance/domain/errors"
	"github.com/LerianStudio/matcher/internal/governance/domain/repositories/mocks"
	sharedDomain "github.com/LerianStudio/matcher/internal/shared/domain"
)

var errQueryDBFailure = errors.New("audit log repository failure")

func TestGetAuditLog(t *testing.T) {
	t.Parallel()

	t.Run("success", func(t *testing.T) {
		t.Parallel()

		ctrl := gomock.NewController(t)
		repo := mocks.NewMockAuditLogRepository(ctrl)

		id := uuid.New()
		expected := &sharedDomain.AuditLog{
			ID:         id,
			TenantID:   uuid.New(),
			EntityType: "reconciliation_context",
			EntityID:   uuid.New(),
			Action:     "CREATE",
			CreatedAt:  time.Now().UTC(),
		}

		repo.EXPECT().GetByID(gomock.Any(), id).Return(expected, nil)

		uc, err := NewUseCase(repo)
		require.NoError(t, err)

		got, err := uc.GetAuditLog(context.Background(), GetAuditLogInput{ID: id})

		require.NoError(t, err)
		assert.Equal(t, expected, got)
	})

	t.Run("missing id", func(t *testing.T) {
		t.Parallel()

		ctrl := gomock.NewController(t)
		repo := mocks.NewMockAuditLogRepository(ctrl)

		uc, err := NewUseCase(repo)
		require.NoError(t, err)

		got, err := uc.GetAuditLog(context.Background(), GetAuditLogInput{})

		require.ErrorIs(t, err, ErrAuditLogIDRequired)
		assert.Nil(t, got)
	})

	t.Run("not found error from repo", func(t *testing.T) {
		t.Parallel()

		ctrl := gomock.NewController(t)
		repo := mocks.NewMockAuditLogRepository(ctrl)

		id := uuid.New()
		repo.EXPECT().GetByID(gomock.Any(), id).Return(nil, governanceErrors.ErrAuditLogNotFound)

		uc, err := NewUseCase(repo)
		require.NoError(t, err)

		got, err := uc.GetAuditLog(context.Background(), GetAuditLogInput{ID: id})

		require.ErrorIs(t, err, governanceErrors.ErrAuditLogNotFound)
		assert.Nil(t, got)
	})

	t.Run("not found nil result mapped to sentinel", func(t *testing.T) {
		t.Parallel()

		ctrl := gomock.NewController(t)
		repo := mocks.NewMockAuditLogRepository(ctrl)

		id := uuid.New()
		repo.EXPECT().GetByID(gomock.Any(), id).Return(nil, nil)

		uc, err := NewUseCase(repo)
		require.NoError(t, err)

		got, err := uc.GetAuditLog(context.Background(), GetAuditLogInput{ID: id})

		require.ErrorIs(t, err, governanceErrors.ErrAuditLogNotFound)
		assert.Nil(t, got)
	})

	t.Run("repository error wrapped", func(t *testing.T) {
		t.Parallel()

		ctrl := gomock.NewController(t)
		repo := mocks.NewMockAuditLogRepository(ctrl)

		id := uuid.New()
		repo.EXPECT().GetByID(gomock.Any(), id).Return(nil, errQueryDBFailure)

		uc, err := NewUseCase(repo)
		require.NoError(t, err)

		got, err := uc.GetAuditLog(context.Background(), GetAuditLogInput{ID: id})

		require.Error(t, err)
		require.ErrorIs(t, err, errQueryDBFailure)
		assert.Nil(t, got)
	})
}

func TestListAuditLogs(t *testing.T) {
	t.Parallel()

	t.Run("success returns logs and cursor", func(t *testing.T) {
		t.Parallel()

		ctrl := gomock.NewController(t)
		repo := mocks.NewMockAuditLogRepository(ctrl)

		actor := "user@example.com"
		filter := sharedDomain.AuditLogFilter{Actor: &actor}
		cursor := &sharedhttp.TimestampCursor{}
		expected := []*sharedDomain.AuditLog{
			{ID: uuid.New(), CreatedAt: time.Now().UTC()},
		}

		repo.EXPECT().
			List(gomock.Any(), filter, cursor, 20).
			Return(expected, "next-cursor", nil)

		uc, err := NewUseCase(repo)
		require.NoError(t, err)

		got, nextCursor, err := uc.ListAuditLogs(
			context.Background(),
			ListAuditLogsInput{Filter: filter, Cursor: cursor, Limit: 20},
		)

		require.NoError(t, err)
		assert.Equal(t, expected, got)
		assert.Equal(t, "next-cursor", nextCursor)
	})

	t.Run("empty result", func(t *testing.T) {
		t.Parallel()

		ctrl := gomock.NewController(t)
		repo := mocks.NewMockAuditLogRepository(ctrl)

		repo.EXPECT().
			List(gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any()).
			Return([]*sharedDomain.AuditLog{}, "", nil)

		uc, err := NewUseCase(repo)
		require.NoError(t, err)

		got, nextCursor, err := uc.ListAuditLogs(
			context.Background(),
			ListAuditLogsInput{Limit: 10},
		)

		require.NoError(t, err)
		assert.Empty(t, got)
		assert.Empty(t, nextCursor)
	})

	t.Run("repository error wrapped", func(t *testing.T) {
		t.Parallel()

		ctrl := gomock.NewController(t)
		repo := mocks.NewMockAuditLogRepository(ctrl)

		repo.EXPECT().
			List(gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any()).
			Return(nil, "", errQueryDBFailure)

		uc, err := NewUseCase(repo)
		require.NoError(t, err)

		got, nextCursor, err := uc.ListAuditLogs(
			context.Background(),
			ListAuditLogsInput{Limit: 10},
		)

		require.Error(t, err)
		require.ErrorIs(t, err, errQueryDBFailure)
		assert.Nil(t, got)
		assert.Empty(t, nextCursor)
	})
}

func TestListAuditLogsByEntity(t *testing.T) {
	t.Parallel()

	t.Run("success returns logs and cursor", func(t *testing.T) {
		t.Parallel()

		ctrl := gomock.NewController(t)
		repo := mocks.NewMockAuditLogRepository(ctrl)

		entityType := "reconciliation_context"
		entityID := uuid.New()
		cursor := &sharedhttp.TimestampCursor{}
		expected := []*sharedDomain.AuditLog{
			{ID: uuid.New(), EntityType: entityType, EntityID: entityID, CreatedAt: time.Now().UTC()},
		}

		repo.EXPECT().
			ListByEntity(gomock.Any(), entityType, entityID, cursor, 20).
			Return(expected, "next-cursor", nil)

		uc, err := NewUseCase(repo)
		require.NoError(t, err)

		got, nextCursor, err := uc.ListAuditLogsByEntity(
			context.Background(),
			ListAuditLogsByEntityInput{
				EntityType: entityType,
				EntityID:   entityID,
				Cursor:     cursor,
				Limit:      20,
			},
		)

		require.NoError(t, err)
		assert.Equal(t, expected, got)
		assert.Equal(t, "next-cursor", nextCursor)
	})

	t.Run("missing entity type", func(t *testing.T) {
		t.Parallel()

		ctrl := gomock.NewController(t)
		repo := mocks.NewMockAuditLogRepository(ctrl)

		uc, err := NewUseCase(repo)
		require.NoError(t, err)

		got, nextCursor, err := uc.ListAuditLogsByEntity(
			context.Background(),
			ListAuditLogsByEntityInput{EntityID: uuid.New(), Limit: 10},
		)

		require.ErrorIs(t, err, ErrEntityTypeRequired)
		assert.Nil(t, got)
		assert.Empty(t, nextCursor)
	})

	t.Run("missing entity id", func(t *testing.T) {
		t.Parallel()

		ctrl := gomock.NewController(t)
		repo := mocks.NewMockAuditLogRepository(ctrl)

		uc, err := NewUseCase(repo)
		require.NoError(t, err)

		got, nextCursor, err := uc.ListAuditLogsByEntity(
			context.Background(),
			ListAuditLogsByEntityInput{EntityType: "reconciliation_context", Limit: 10},
		)

		require.ErrorIs(t, err, ErrEntityIDRequired)
		assert.Nil(t, got)
		assert.Empty(t, nextCursor)
	})

	t.Run("repository error wrapped", func(t *testing.T) {
		t.Parallel()

		ctrl := gomock.NewController(t)
		repo := mocks.NewMockAuditLogRepository(ctrl)

		entityType := "reconciliation_context"
		entityID := uuid.New()

		repo.EXPECT().
			ListByEntity(gomock.Any(), entityType, entityID, gomock.Any(), gomock.Any()).
			Return(nil, "", errQueryDBFailure)

		uc, err := NewUseCase(repo)
		require.NoError(t, err)

		got, nextCursor, err := uc.ListAuditLogsByEntity(
			context.Background(),
			ListAuditLogsByEntityInput{
				EntityType: entityType,
				EntityID:   entityID,
				Limit:      10,
			},
		)

		require.Error(t, err)
		require.ErrorIs(t, err, errQueryDBFailure)
		assert.Nil(t, got)
		assert.Empty(t, nextCursor)
	})
}

func TestQuerySentinelErrors_AreDistinct(t *testing.T) {
	t.Parallel()

	errs := []error{
		ErrQueryRepoRequired,
		ErrAuditLogIDRequired,
		ErrEntityTypeRequired,
		ErrEntityIDRequired,
	}

	for i := range errs {
		for j := i + 1; j < len(errs); j++ {
			assert.NotEqual(t, errs[i].Error(), errs[j].Error(),
				"sentinel errors must have distinct messages: %q vs %q", errs[i], errs[j])
		}
	}
}
