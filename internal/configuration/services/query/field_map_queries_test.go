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

	"github.com/LerianStudio/matcher/internal/configuration/domain/repositories/mocks"
	shared "github.com/LerianStudio/matcher/internal/shared/domain"
)

var errFieldMapRepositoryFailure = errors.New("repository failure")

func TestGetFieldMap_NilUseCase(t *testing.T) {
	t.Parallel()

	var uc *UseCase

	_, err := uc.GetFieldMap(context.Background(), uuid.New())
	require.ErrorIs(t, err, ErrNilFieldMapRepository)
}

func TestGetFieldMap_NilFieldMapRepo(t *testing.T) {
	t.Parallel()

	uc := &UseCase{}

	_, err := uc.GetFieldMap(context.Background(), uuid.New())
	require.ErrorIs(t, err, ErrNilFieldMapRepository)
}

func TestGetFieldMap_RepositoryError(t *testing.T) {
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

	fieldMapRepo.EXPECT().
		FindByID(gomock.Any(), fieldMapID).
		Return(nil, errFieldMapRepositoryFailure)

	_, err = uc.GetFieldMap(ctx, fieldMapID)
	require.Error(t, err)
	require.ErrorIs(t, err, errFieldMapRepositoryFailure)
	require.Contains(t, err.Error(), "finding field map")
}

func TestGetFieldMap_Success(t *testing.T) {
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
	expected := &shared.FieldMap{ID: fieldMapID}

	fieldMapRepo.EXPECT().FindByID(gomock.Any(), fieldMapID).Return(expected, nil)

	result, err := uc.GetFieldMap(ctx, fieldMapID)
	require.NoError(t, err)
	require.Equal(t, expected, result)
}

func TestGetFieldMapBySource_NilUseCase(t *testing.T) {
	t.Parallel()

	var uc *UseCase

	_, err := uc.GetFieldMapBySource(context.Background(), uuid.New())
	require.ErrorIs(t, err, ErrNilFieldMapRepository)
}

func TestGetFieldMapBySource_NilFieldMapRepo(t *testing.T) {
	t.Parallel()

	uc := &UseCase{}

	_, err := uc.GetFieldMapBySource(context.Background(), uuid.New())
	require.ErrorIs(t, err, ErrNilFieldMapRepository)
}

func TestGetFieldMapBySource_RepositoryError(t *testing.T) {
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
	sourceID := uuid.New()

	fieldMapRepo.EXPECT().
		FindBySourceID(gomock.Any(), sourceID).
		Return(nil, errFieldMapRepositoryFailure)

	_, err = uc.GetFieldMapBySource(ctx, sourceID)
	require.Error(t, err)
	require.ErrorIs(t, err, errFieldMapRepositoryFailure)
	require.Contains(t, err.Error(), "finding field map by source")
}

func TestGetFieldMapBySource_Success(t *testing.T) {
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
	sourceID := uuid.New()
	expected := &shared.FieldMap{ID: uuid.New()}

	fieldMapRepo.EXPECT().FindBySourceID(gomock.Any(), sourceID).Return(expected, nil)

	result, err := uc.GetFieldMapBySource(ctx, sourceID)
	require.NoError(t, err)
	require.Equal(t, expected, result)
}
