//go:build unit

package repositories

import (
	"context"
	"testing"

	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	libHTTP "github.com/LerianStudio/lib-commons/v5/commons/net/http"

	"github.com/LerianStudio/matcher/internal/configuration/domain/entities"
	"github.com/LerianStudio/matcher/internal/configuration/domain/value_objects"
)

func TestContextRepositoryInterfaceCompiles(t *testing.T) {
	t.Parallel()

	var _ ContextRepository = (*mockContextRepository)(nil)
}

type mockContextRepository struct {
	contexts map[uuid.UUID]*entities.ReconciliationContext
}

func (m *mockContextRepository) Create(
	_ context.Context,
	entity *entities.ReconciliationContext,
) (*entities.ReconciliationContext, error) {
	if m.contexts == nil {
		m.contexts = make(map[uuid.UUID]*entities.ReconciliationContext)
	}

	m.contexts[entity.ID] = entity

	return entity, nil
}

func (m *mockContextRepository) FindByID(
	_ context.Context,
	id uuid.UUID,
) (*entities.ReconciliationContext, error) {
	if ctx, ok := m.contexts[id]; ok {
		return ctx, nil
	}

	return nil, nil
}

func (m *mockContextRepository) FindByName(
	_ context.Context,
	name string,
) (*entities.ReconciliationContext, error) {
	for _, ctx := range m.contexts {
		if ctx.Name == name {
			return ctx, nil
		}
	}

	return nil, nil
}

func (m *mockContextRepository) FindAll(
	_ context.Context,
	_ string,
	limit int,
	_ *value_objects.ContextType,
	_ *value_objects.ContextStatus,
) ([]*entities.ReconciliationContext, libHTTP.CursorPagination, error) {
	result := make([]*entities.ReconciliationContext, 0, len(m.contexts))

	for _, ctx := range m.contexts {
		result = append(result, ctx)
	}

	if limit > 0 && limit < len(result) {
		return result[:limit], libHTTP.CursorPagination{}, nil
	}

	return result, libHTTP.CursorPagination{}, nil
}

func (m *mockContextRepository) Update(
	_ context.Context,
	entity *entities.ReconciliationContext,
) (*entities.ReconciliationContext, error) {
	if m.contexts == nil {
		m.contexts = make(map[uuid.UUID]*entities.ReconciliationContext)
	}

	m.contexts[entity.ID] = entity

	return entity, nil
}

func (m *mockContextRepository) Delete(_ context.Context, id uuid.UUID) error {
	delete(m.contexts, id)
	return nil
}

func (m *mockContextRepository) Count(_ context.Context) (int64, error) {
	return int64(len(m.contexts)), nil
}

func TestMockContextRepositoryOperations(t *testing.T) {
	t.Parallel()

	t.Run("Create stores context", func(t *testing.T) {
		t.Parallel()

		repo := &mockContextRepository{}
		ctxID := uuid.New()
		reconciliationCtx := &entities.ReconciliationContext{
			ID:   ctxID,
			Name: "Test Context",
		}

		created, err := repo.Create(context.Background(), reconciliationCtx)
		require.NoError(t, err)
		assert.Equal(t, ctxID, created.ID)
	})

	t.Run("FindByID retrieves context", func(t *testing.T) {
		t.Parallel()

		repo := &mockContextRepository{}
		ctxID := uuid.New()
		reconciliationCtx := &entities.ReconciliationContext{
			ID:   ctxID,
			Name: "Test Context",
		}

		_, err := repo.Create(context.Background(), reconciliationCtx)
		require.NoError(t, err)

		found, err := repo.FindByID(context.Background(), ctxID)
		require.NoError(t, err)
		require.NotNil(t, found)
		assert.Equal(t, ctxID, found.ID)
	})

	t.Run("FindByName retrieves context by name", func(t *testing.T) {
		t.Parallel()

		repo := &mockContextRepository{}
		ctxID := uuid.New()
		reconciliationCtx := &entities.ReconciliationContext{
			ID:   ctxID,
			Name: "Test Context",
		}

		_, err := repo.Create(context.Background(), reconciliationCtx)
		require.NoError(t, err)

		found, err := repo.FindByName(context.Background(), "Test Context")
		require.NoError(t, err)
		require.NotNil(t, found)
		assert.Equal(t, "Test Context", found.Name)
	})

	t.Run("Count returns number of contexts", func(t *testing.T) {
		t.Parallel()

		repo := &mockContextRepository{}
		ctxID := uuid.New()
		reconciliationCtx := &entities.ReconciliationContext{ID: ctxID}

		_, err := repo.Create(context.Background(), reconciliationCtx)
		require.NoError(t, err)

		count, err := repo.Count(context.Background())
		require.NoError(t, err)
		assert.Equal(t, int64(1), count)
	})

	t.Run("Delete removes context", func(t *testing.T) {
		t.Parallel()

		repo := &mockContextRepository{}
		ctxID := uuid.New()
		reconciliationCtx := &entities.ReconciliationContext{ID: ctxID}

		_, err := repo.Create(context.Background(), reconciliationCtx)
		require.NoError(t, err)

		err = repo.Delete(context.Background(), ctxID)
		require.NoError(t, err)

		found, err := repo.FindByID(context.Background(), ctxID)
		require.NoError(t, err)
		assert.Nil(t, found)
	})
}
