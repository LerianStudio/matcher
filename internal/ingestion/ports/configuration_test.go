// Copyright 2025 Lerian Studio. All rights reserved.
// Use of this source code is governed by an Elastic License 2.0
// that can be found in the LICENSE.md file.

//go:build unit

package ports

import (
	"context"
	"errors"
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

var errFieldMapNotFound = errors.New("field map not found")

func (m *mockFieldMapRepository) FindBySourceID(
	_ context.Context,
	sourceID uuid.UUID,
) (*shared.FieldMap, error) {
	if fm, ok := m.fieldMaps[sourceID]; ok {
		return fm, nil
	}

	return nil, errFieldMapNotFound
}

func TestMockFieldMapRepositoryFindBySourceID(t *testing.T) {
	t.Parallel()

	t.Run("returns field map when found", func(t *testing.T) {
		t.Parallel()

		sourceID := uuid.New()
		fieldMap := &shared.FieldMap{
			ID:       uuid.New(),
			SourceID: sourceID,
			Mapping:  map[string]any{"amount": "txn_amount"},
		}
		repo := &mockFieldMapRepository{
			fieldMaps: map[uuid.UUID]*shared.FieldMap{sourceID: fieldMap},
		}

		ctx := t.Context()

		result, err := repo.FindBySourceID(ctx, sourceID)
		require.NoError(t, err)
		assert.Equal(t, fieldMap.ID, result.ID)
		assert.Equal(t, sourceID, result.SourceID)
	})

	t.Run("returns error when not found", func(t *testing.T) {
		t.Parallel()

		repo := &mockFieldMapRepository{
			fieldMaps: make(map[uuid.UUID]*shared.FieldMap),
		}

		ctx := t.Context()

		_, err := repo.FindBySourceID(ctx, uuid.New())
		require.ErrorIs(t, err, errFieldMapNotFound)
	})
}

func TestSourceRepositoryInterfaceCompiles(t *testing.T) {
	t.Parallel()

	var _ SourceRepository = (*mockSourceRepository)(nil)
}

type mockSourceRepository struct {
	sources map[uuid.UUID]*shared.ReconciliationSource
}

var errSourceNotFound = errors.New("source not found")

func (m *mockSourceRepository) FindByID(
	_ context.Context,
	_, id uuid.UUID,
) (*shared.ReconciliationSource, error) {
	if src, ok := m.sources[id]; ok {
		return src, nil
	}

	return nil, errSourceNotFound
}

func TestMockSourceRepositoryFindByID(t *testing.T) {
	t.Parallel()

	t.Run("returns source when found", func(t *testing.T) {
		t.Parallel()

		contextID := uuid.New()
		sourceID := uuid.New()
		source := &shared.ReconciliationSource{
			ID:        sourceID,
			ContextID: contextID,
			Name:      "Bank Statement",
		}
		repo := &mockSourceRepository{
			sources: map[uuid.UUID]*shared.ReconciliationSource{sourceID: source},
		}

		ctx := t.Context()

		result, err := repo.FindByID(ctx, contextID, sourceID)
		require.NoError(t, err)
		assert.Equal(t, source.ID, result.ID)
		assert.Equal(t, source.Name, result.Name)
	})

	t.Run("returns error when not found", func(t *testing.T) {
		t.Parallel()

		repo := &mockSourceRepository{
			sources: make(map[uuid.UUID]*shared.ReconciliationSource),
		}

		ctx := t.Context()

		_, err := repo.FindByID(ctx, uuid.New(), uuid.New())
		require.ErrorIs(t, err, errSourceNotFound)
	})
}

func TestRepositoryInterfacesAreIndependent(t *testing.T) {
	t.Parallel()

	var fieldMapRepo FieldMapRepository = &mockFieldMapRepository{}

	var sourceRepo SourceRepository = &mockSourceRepository{}

	assert.NotNil(t, fieldMapRepo)
	assert.NotNil(t, sourceRepo)
}
