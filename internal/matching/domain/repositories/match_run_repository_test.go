//go:build unit

package repositories

import (
	"context"
	"testing"

	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	libHTTP "github.com/LerianStudio/lib-uncommons/v2/uncommons/net/http"

	"github.com/LerianStudio/matcher/internal/matching/domain/entities"
)

func TestMatchRunRepositoryInterfaceCompiles(t *testing.T) {
	t.Parallel()

	var _ MatchRunRepository = (*mockMatchRunRepository)(nil)
}

type mockMatchRunRepository struct {
	runs map[uuid.UUID]*entities.MatchRun
}

func (repo *mockMatchRunRepository) Create(
	_ context.Context,
	entity *entities.MatchRun,
) (*entities.MatchRun, error) {
	if repo.runs == nil {
		repo.runs = make(map[uuid.UUID]*entities.MatchRun)
	}

	repo.runs[entity.ID] = entity

	return entity, nil
}

func (repo *mockMatchRunRepository) CreateWithTx(
	ctx context.Context,
	_ Tx,
	entity *entities.MatchRun,
) (*entities.MatchRun, error) {
	return repo.Create(ctx, entity)
}

func (repo *mockMatchRunRepository) Update(
	_ context.Context,
	entity *entities.MatchRun,
) (*entities.MatchRun, error) {
	if repo.runs == nil {
		repo.runs = make(map[uuid.UUID]*entities.MatchRun)
	}

	repo.runs[entity.ID] = entity

	return entity, nil
}

func (repo *mockMatchRunRepository) UpdateWithTx(
	ctx context.Context,
	_ Tx,
	entity *entities.MatchRun,
) (*entities.MatchRun, error) {
	return repo.Update(ctx, entity)
}

func (repo *mockMatchRunRepository) FindByID(
	_ context.Context,
	_, runID uuid.UUID,
) (*entities.MatchRun, error) {
	if run, ok := repo.runs[runID]; ok {
		return run, nil
	}

	return nil, nil
}

func (repo *mockMatchRunRepository) WithTx(_ context.Context, fn func(Tx) error) error {
	return fn(nil)
}

func (repo *mockMatchRunRepository) ListByContextID(
	_ context.Context,
	_ uuid.UUID,
	_ CursorFilter,
) ([]*entities.MatchRun, libHTTP.CursorPagination, error) {
	runs := make([]*entities.MatchRun, 0, len(repo.runs))

	for _, run := range repo.runs {
		runs = append(runs, run)
	}

	return runs, libHTTP.CursorPagination{}, nil
}

func TestMockMatchRunRepositoryOperations(t *testing.T) {
	t.Parallel()

	t.Run("Create stores run", func(t *testing.T) {
		t.Parallel()

		repo := &mockMatchRunRepository{}
		runID := uuid.New()
		contextID := uuid.New()
		run := &entities.MatchRun{
			ID:        runID,
			ContextID: contextID,
		}

		created, err := repo.Create(context.Background(), run)
		require.NoError(t, err)
		assert.Equal(t, runID, created.ID)
	})

	t.Run("FindByID retrieves run", func(t *testing.T) {
		t.Parallel()

		repo := &mockMatchRunRepository{}
		runID := uuid.New()
		contextID := uuid.New()
		run := &entities.MatchRun{
			ID:        runID,
			ContextID: contextID,
		}

		_, err := repo.Create(context.Background(), run)
		require.NoError(t, err)

		found, err := repo.FindByID(context.Background(), contextID, runID)
		require.NoError(t, err)
		require.NotNil(t, found)
		assert.Equal(t, runID, found.ID)
	})

	t.Run("ListByContextID returns runs", func(t *testing.T) {
		t.Parallel()

		repo := &mockMatchRunRepository{}
		runID := uuid.New()
		contextID := uuid.New()
		run := &entities.MatchRun{
			ID:        runID,
			ContextID: contextID,
		}

		_, err := repo.Create(context.Background(), run)
		require.NoError(t, err)

		runs, _, err := repo.ListByContextID(
			context.Background(),
			contextID,
			CursorFilter{Limit: 10},
		)
		require.NoError(t, err)
		assert.Len(t, runs, 1)
	})
}
