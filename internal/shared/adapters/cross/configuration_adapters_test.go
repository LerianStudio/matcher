//go:build unit

package cross

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.uber.org/mock/gomock"

	configEntities "github.com/LerianStudio/matcher/internal/configuration/domain/entities"
	"github.com/LerianStudio/matcher/internal/configuration/domain/repositories/mocks"
	"github.com/LerianStudio/matcher/internal/configuration/domain/value_objects"
	shared "github.com/LerianStudio/matcher/internal/shared/domain"
)

// errTestDatabase is a sentinel error used for testing failure scenarios.
var errTestDatabase = errors.New("database error")

func TestNewFieldMapRepositoryAdapter(t *testing.T) {
	t.Parallel()

	t.Run("returns error when repo is nil", func(t *testing.T) {
		t.Parallel()

		adapter, err := NewFieldMapRepositoryAdapter(nil)

		require.Nil(t, adapter)
		require.ErrorIs(t, err, ErrNilFieldMapRepository)
	})

	t.Run("returns adapter when repo is provided", func(t *testing.T) {
		t.Parallel()

		ctrl := gomock.NewController(t)
		defer ctrl.Finish()

		mockRepo := mocks.NewMockFieldMapRepository(ctrl)
		adapter, err := NewFieldMapRepositoryAdapter(mockRepo)

		require.NoError(t, err)
		require.NotNil(t, adapter)
		assert.Equal(t, mockRepo, adapter.repo)
	})
}

func TestFieldMapRepositoryAdapter_FindBySourceID_Success(t *testing.T) {
	t.Parallel()

	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	mockRepo := mocks.NewMockFieldMapRepository(ctrl)
	adapter, err := NewFieldMapRepositoryAdapter(mockRepo)
	require.NoError(t, err)

	ctx := context.Background()
	sourceID := uuid.New()
	now := time.Now().UTC()

	configFieldMap := &shared.FieldMap{
		ID:        uuid.New(),
		ContextID: uuid.New(),
		SourceID:  sourceID,
		Mapping:   map[string]any{"amount": "value"},
		Version:   1,
		CreatedAt: now,
		UpdatedAt: now,
	}

	mockRepo.EXPECT().
		FindBySourceID(ctx, sourceID).
		Return(configFieldMap, nil)

	result, err := adapter.FindBySourceID(ctx, sourceID)

	require.NoError(t, err)
	require.NotNil(t, result)
	assert.Equal(t, configFieldMap.ID, result.ID)
	assert.Equal(t, configFieldMap.ContextID, result.ContextID)
	assert.Equal(t, configFieldMap.SourceID, result.SourceID)
	assert.Equal(t, configFieldMap.Mapping, result.Mapping)
	assert.Equal(t, configFieldMap.Version, result.Version)
	assert.Equal(t, configFieldMap.CreatedAt, result.CreatedAt)
	assert.Equal(t, configFieldMap.UpdatedAt, result.UpdatedAt)
}

func TestFieldMapRepositoryAdapter_FindBySourceID_NilFromRepo(t *testing.T) {
	t.Parallel()

	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	mockRepo := mocks.NewMockFieldMapRepository(ctrl)
	adapter, err := NewFieldMapRepositoryAdapter(mockRepo)
	require.NoError(t, err)

	ctx := context.Background()
	sourceID := uuid.New()

	mockRepo.EXPECT().
		FindBySourceID(ctx, sourceID).
		Return(nil, nil)

	result, err := adapter.FindBySourceID(ctx, sourceID)

	require.NoError(t, err)
	assert.Nil(t, result)
}

func TestFieldMapRepositoryAdapter_FindBySourceID_ErrorFromRepo(t *testing.T) {
	t.Parallel()

	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	mockRepo := mocks.NewMockFieldMapRepository(ctrl)
	adapter, err := NewFieldMapRepositoryAdapter(mockRepo)
	require.NoError(t, err)

	ctx := context.Background()
	sourceID := uuid.New()

	mockRepo.EXPECT().
		FindBySourceID(ctx, sourceID).
		Return(nil, errTestDatabase)

	result, err := adapter.FindBySourceID(ctx, sourceID)

	require.Error(t, err)
	assert.Nil(t, result)
	assert.Contains(t, err.Error(), "finding field map by source ID")
	require.ErrorIs(t, err, errTestDatabase)
}

func TestNewSourceRepositoryAdapter(t *testing.T) {
	t.Parallel()

	t.Run("returns error when repo is nil", func(t *testing.T) {
		t.Parallel()

		adapter, err := NewSourceRepositoryAdapter(nil)

		require.Nil(t, adapter)
		require.ErrorIs(t, err, ErrNilSourceRepository)
	})

	t.Run("returns adapter when repo is provided", func(t *testing.T) {
		t.Parallel()

		ctrl := gomock.NewController(t)
		defer ctrl.Finish()

		mockRepo := mocks.NewMockSourceRepository(ctrl)
		adapter, err := NewSourceRepositoryAdapter(mockRepo)

		require.NoError(t, err)
		require.NotNil(t, adapter)
		assert.Equal(t, mockRepo, adapter.repo)
	})
}

func TestSourceRepositoryAdapter_FindByID_Success(t *testing.T) {
	t.Parallel()

	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	mockRepo := mocks.NewMockSourceRepository(ctrl)
	adapter, err := NewSourceRepositoryAdapter(mockRepo)
	require.NoError(t, err)

	ctx := context.Background()
	contextID := uuid.New()
	sourceID := uuid.New()
	now := time.Now().UTC()

	configSource := &configEntities.ReconciliationSource{
		ID:        sourceID,
		ContextID: contextID,
		Name:      "Test Source",
		Type:      value_objects.SourceTypeLedger,
		Config:    map[string]any{"key": "value"},
		CreatedAt: now,
		UpdatedAt: now,
	}

	mockRepo.EXPECT().
		FindByID(ctx, contextID, sourceID).
		Return(configSource, nil)

	result, err := adapter.FindByID(ctx, contextID, sourceID)

	require.NoError(t, err)
	require.NotNil(t, result)
	assert.Equal(t, configSource.ID, result.ID)
	assert.Equal(t, configSource.ContextID, result.ContextID)
	assert.Equal(t, configSource.Name, result.Name)
	assert.Equal(t, string(configSource.Type), result.Type)
	assert.Equal(t, configSource.Config, result.Config)
	assert.Equal(t, configSource.CreatedAt, result.CreatedAt)
	assert.Equal(t, configSource.UpdatedAt, result.UpdatedAt)
}

func TestSourceRepositoryAdapter_FindByID_NilFromRepo(t *testing.T) {
	t.Parallel()

	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	mockRepo := mocks.NewMockSourceRepository(ctrl)
	adapter, err := NewSourceRepositoryAdapter(mockRepo)
	require.NoError(t, err)

	ctx := context.Background()
	contextID := uuid.New()
	sourceID := uuid.New()

	mockRepo.EXPECT().
		FindByID(ctx, contextID, sourceID).
		Return(nil, nil)

	result, err := adapter.FindByID(ctx, contextID, sourceID)

	require.NoError(t, err)
	assert.Nil(t, result)
}

func TestSourceRepositoryAdapter_FindByID_ErrorFromRepo(t *testing.T) {
	t.Parallel()

	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	mockRepo := mocks.NewMockSourceRepository(ctrl)
	adapter, err := NewSourceRepositoryAdapter(mockRepo)
	require.NoError(t, err)

	ctx := context.Background()
	contextID := uuid.New()
	sourceID := uuid.New()

	mockRepo.EXPECT().
		FindByID(ctx, contextID, sourceID).
		Return(nil, errTestDatabase)

	result, err := adapter.FindByID(ctx, contextID, sourceID)

	require.Error(t, err)
	assert.Nil(t, result)
	assert.Contains(t, err.Error(), "finding source by ID")
	require.ErrorIs(t, err, errTestDatabase)
}
