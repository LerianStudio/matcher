//go:build unit

package repositories

import (
	"context"
	"testing"

	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	libHTTP "github.com/LerianStudio/lib-commons/v4/commons/net/http"

	"github.com/LerianStudio/matcher/internal/configuration/domain/entities"
	"github.com/LerianStudio/matcher/internal/configuration/domain/value_objects"
)

func TestSourceRepositoryInterfaceCompiles(t *testing.T) {
	t.Parallel()

	var _ SourceRepository = (*mockSourceRepository)(nil)
}

type mockSourceRepository struct {
	sources map[uuid.UUID]*entities.ReconciliationSource
}

func (m *mockSourceRepository) Create(
	_ context.Context,
	entity *entities.ReconciliationSource,
) (*entities.ReconciliationSource, error) {
	if m.sources == nil {
		m.sources = make(map[uuid.UUID]*entities.ReconciliationSource)
	}

	m.sources[entity.ID] = entity

	return entity, nil
}

func (m *mockSourceRepository) FindByID(
	_ context.Context,
	_, id uuid.UUID,
) (*entities.ReconciliationSource, error) {
	if src, ok := m.sources[id]; ok {
		return src, nil
	}

	return nil, nil
}

func (m *mockSourceRepository) FindByContextID(
	_ context.Context,
	contextID uuid.UUID,
	_ string,
	limit int,
) ([]*entities.ReconciliationSource, libHTTP.CursorPagination, error) {
	result := make([]*entities.ReconciliationSource, 0, len(m.sources))

	for _, src := range m.sources {
		if src.ContextID == contextID {
			result = append(result, src)
		}
	}

	if limit > 0 && limit < len(result) {
		return result[:limit], libHTTP.CursorPagination{}, nil
	}

	return result, libHTTP.CursorPagination{}, nil
}

func (m *mockSourceRepository) FindByContextIDAndType(
	_ context.Context,
	contextID uuid.UUID,
	sourceType value_objects.SourceType,
	_ string,
	limit int,
) ([]*entities.ReconciliationSource, libHTTP.CursorPagination, error) {
	result := make([]*entities.ReconciliationSource, 0, len(m.sources))

	for _, src := range m.sources {
		if src.ContextID == contextID && src.Type == sourceType {
			result = append(result, src)
		}
	}

	if limit > 0 && limit < len(result) {
		return result[:limit], libHTTP.CursorPagination{}, nil
	}

	return result, libHTTP.CursorPagination{}, nil
}

func (m *mockSourceRepository) Update(
	_ context.Context,
	entity *entities.ReconciliationSource,
) (*entities.ReconciliationSource, error) {
	if m.sources == nil {
		m.sources = make(map[uuid.UUID]*entities.ReconciliationSource)
	}

	m.sources[entity.ID] = entity

	return entity, nil
}

func (m *mockSourceRepository) Delete(_ context.Context, _, id uuid.UUID) error {
	delete(m.sources, id)
	return nil
}

func TestMockSourceRepositoryOperations(t *testing.T) {
	t.Parallel()

	t.Run("Create stores source", func(t *testing.T) {
		t.Parallel()

		repo := &mockSourceRepository{}
		sourceID := uuid.New()
		source := &entities.ReconciliationSource{
			ID:   sourceID,
			Name: "Test Source",
			Type: value_objects.SourceTypeLedger,
		}

		created, err := repo.Create(context.Background(), source)

		require.NoError(t, err)
		assert.Equal(t, sourceID, created.ID)
	})

	t.Run("FindByID retrieves source", func(t *testing.T) {
		t.Parallel()

		repo := &mockSourceRepository{}
		sourceID := uuid.New()
		contextID := uuid.New()
		source := &entities.ReconciliationSource{ID: sourceID, ContextID: contextID}

		_, err := repo.Create(context.Background(), source)

		require.NoError(t, err)

		found, err := repo.FindByID(context.Background(), contextID, sourceID)
		require.NoError(t, err)
		require.NotNil(t, found)
		assert.Equal(t, sourceID, found.ID)
	})

	t.Run("FindByContextID retrieves sources by context", func(t *testing.T) {
		t.Parallel()

		repo := &mockSourceRepository{}
		contextID := uuid.New()
		sourceID := uuid.New()
		source := &entities.ReconciliationSource{ID: sourceID, ContextID: contextID}

		_, err := repo.Create(context.Background(), source)

		require.NoError(t, err)

		sources, _, err := repo.FindByContextID(context.Background(), contextID, "", 100)
		require.NoError(t, err)
		assert.Len(t, sources, 1)
	})

	t.Run("Delete removes source", func(t *testing.T) {
		t.Parallel()

		repo := &mockSourceRepository{}
		sourceID := uuid.New()
		contextID := uuid.New()
		source := &entities.ReconciliationSource{ID: sourceID, ContextID: contextID}

		_, err := repo.Create(context.Background(), source)

		require.NoError(t, err)

		err = repo.Delete(context.Background(), contextID, sourceID)
		require.NoError(t, err)

		found, err := repo.FindByID(context.Background(), contextID, sourceID)
		require.NoError(t, err)
		assert.Nil(t, found)
	})
}
