// Copyright 2025 Lerian Studio. All rights reserved.
// Use of this source code is governed by an Elastic License 2.0
// that can be found in the LICENSE.md file.

//go:build unit

package repositories

import (
	"context"
	"testing"

	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	shared "github.com/LerianStudio/matcher/internal/shared/domain"
)

func TestFieldMapRepositoryInterfaceCompiles(t *testing.T) {
	t.Parallel()

	var _ FieldMapRepository = (*mockFieldMapRepository)(nil)
}

type mockFieldMapRepository struct {
	fieldMaps map[uuid.UUID]*shared.FieldMap
}

func (m *mockFieldMapRepository) Create(
	_ context.Context,
	entity *shared.FieldMap,
) (*shared.FieldMap, error) {
	if m.fieldMaps == nil {
		m.fieldMaps = make(map[uuid.UUID]*shared.FieldMap)
	}

	m.fieldMaps[entity.ID] = entity

	return entity, nil
}

func (m *mockFieldMapRepository) FindByID(
	_ context.Context,
	id uuid.UUID,
) (*shared.FieldMap, error) {
	if fm, ok := m.fieldMaps[id]; ok {
		return fm, nil
	}

	return nil, nil
}

func (m *mockFieldMapRepository) FindBySourceID(
	_ context.Context,
	sourceID uuid.UUID,
) (*shared.FieldMap, error) {
	for _, fm := range m.fieldMaps {
		if fm.SourceID == sourceID {
			return fm, nil
		}
	}

	return nil, nil
}

func (m *mockFieldMapRepository) Update(
	_ context.Context,
	entity *shared.FieldMap,
) (*shared.FieldMap, error) {
	if m.fieldMaps == nil {
		m.fieldMaps = make(map[uuid.UUID]*shared.FieldMap)
	}

	m.fieldMaps[entity.ID] = entity

	return entity, nil
}

func (m *mockFieldMapRepository) ExistsBySourceIDs(
	_ context.Context,
	sourceIDs []uuid.UUID,
) (map[uuid.UUID]bool, error) {
	result := make(map[uuid.UUID]bool, len(sourceIDs))

	for _, sourceID := range sourceIDs {
		for _, fm := range m.fieldMaps {
			if fm.SourceID == sourceID {
				result[sourceID] = true

				break
			}
		}
	}

	return result, nil
}

func (m *mockFieldMapRepository) Delete(_ context.Context, id uuid.UUID) error {
	delete(m.fieldMaps, id)
	return nil
}

func TestMockFieldMapRepositoryOperations(t *testing.T) {
	t.Parallel()

	t.Run("Create stores field map", func(t *testing.T) {
		t.Parallel()

		repo := &mockFieldMapRepository{}
		fieldMapID := uuid.New()
		fieldMap := &shared.FieldMap{
			ID:      fieldMapID,
			Mapping: map[string]any{"amount": "txn_amount"},
		}

		created, err := repo.Create(context.Background(), fieldMap)

		require.NoError(t, err)
		assert.Equal(t, fieldMapID, created.ID)
	})

	t.Run("FindByID retrieves field map", func(t *testing.T) {
		t.Parallel()

		repo := &mockFieldMapRepository{}
		fieldMapID := uuid.New()
		fieldMap := &shared.FieldMap{ID: fieldMapID}

		_, err := repo.Create(context.Background(), fieldMap)

		require.NoError(t, err)

		found, err := repo.FindByID(context.Background(), fieldMapID)
		require.NoError(t, err)
		require.NotNil(t, found)
		assert.Equal(t, fieldMapID, found.ID)
	})

	t.Run("FindBySourceID retrieves field map by source", func(t *testing.T) {
		t.Parallel()

		repo := &mockFieldMapRepository{}
		fieldMapID := uuid.New()
		sourceID := uuid.New()
		fieldMap := &shared.FieldMap{ID: fieldMapID, SourceID: sourceID}

		_, err := repo.Create(context.Background(), fieldMap)

		require.NoError(t, err)

		found, err := repo.FindBySourceID(context.Background(), sourceID)
		require.NoError(t, err)
		require.NotNil(t, found)
		assert.Equal(t, sourceID, found.SourceID)
	})

	t.Run("Delete removes field map", func(t *testing.T) {
		t.Parallel()

		repo := &mockFieldMapRepository{}
		fieldMapID := uuid.New()
		fieldMap := &shared.FieldMap{ID: fieldMapID}

		_, err := repo.Create(context.Background(), fieldMap)

		require.NoError(t, err)

		err = repo.Delete(context.Background(), fieldMapID)
		require.NoError(t, err)

		found, err := repo.FindByID(context.Background(), fieldMapID)
		require.NoError(t, err)
		assert.Nil(t, found)
	})
}
