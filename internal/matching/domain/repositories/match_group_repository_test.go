//go:build unit

package repositories

import (
	"context"
	"testing"

	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	libHTTP "github.com/LerianStudio/lib-commons/v4/commons/net/http"

	"github.com/LerianStudio/matcher/internal/matching/domain/entities"
)

func TestMatchGroupRepositoryInterfaceCompiles(t *testing.T) {
	t.Parallel()

	var _ MatchGroupRepository = (*mockMatchGroupRepository)(nil)
}

type mockMatchGroupRepository struct {
	groups []*entities.MatchGroup
}

func (repo *mockMatchGroupRepository) CreateBatch(
	_ context.Context,
	groups []*entities.MatchGroup,
) ([]*entities.MatchGroup, error) {
	repo.groups = append(repo.groups, groups...)
	return groups, nil
}

func (repo *mockMatchGroupRepository) CreateBatchWithTx(
	ctx context.Context,
	_ Tx,
	groups []*entities.MatchGroup,
) ([]*entities.MatchGroup, error) {
	return repo.CreateBatch(ctx, groups)
}

func (repo *mockMatchGroupRepository) ListByRunID(
	_ context.Context,
	_, _ uuid.UUID,
	filter CursorFilter,
) ([]*entities.MatchGroup, libHTTP.CursorPagination, error) {
	limit := filter.Limit
	if limit <= 0 {
		limit = 20
	}

	if len(repo.groups) == 0 {
		return nil, libHTTP.CursorPagination{}, nil
	}

	end := limit
	if end > len(repo.groups) {
		end = len(repo.groups)
	}

	return repo.groups[:end], libHTTP.CursorPagination{}, nil
}

func (repo *mockMatchGroupRepository) FindByID(
	_ context.Context,
	_, id uuid.UUID,
) (*entities.MatchGroup, error) {
	for _, g := range repo.groups {
		if g.ID == id {
			return g, nil
		}
	}

	return nil, nil
}

func (repo *mockMatchGroupRepository) Update(
	_ context.Context,
	group *entities.MatchGroup,
) (*entities.MatchGroup, error) {
	for i, g := range repo.groups {
		if g.ID == group.ID {
			repo.groups[i] = group
			return group, nil
		}
	}

	return group, nil
}

func (repo *mockMatchGroupRepository) UpdateWithTx(
	ctx context.Context,
	_ Tx,
	group *entities.MatchGroup,
) (*entities.MatchGroup, error) {
	return repo.Update(ctx, group)
}

func TestMockMatchGroupRepositoryOperations(t *testing.T) {
	t.Parallel()

	t.Run("CreateBatch stores groups", func(t *testing.T) {
		t.Parallel()

		repo := &mockMatchGroupRepository{}
		groupOne := &entities.MatchGroup{ID: uuid.New(), RunID: uuid.New()}
		groupTwo := &entities.MatchGroup{ID: uuid.New(), RunID: uuid.New()}

		created, err := repo.CreateBatch(
			context.Background(),
			[]*entities.MatchGroup{groupOne, groupTwo},
		)
		require.NoError(t, err)
		assert.Len(t, created, 2)
	})

	t.Run("ListByRunID returns groups", func(t *testing.T) {
		t.Parallel()

		repo := &mockMatchGroupRepository{}
		groupOne := &entities.MatchGroup{ID: uuid.New(), RunID: uuid.New()}
		groupTwo := &entities.MatchGroup{ID: uuid.New(), RunID: uuid.New()}

		_, err := repo.CreateBatch(context.Background(), []*entities.MatchGroup{groupOne, groupTwo})
		require.NoError(t, err)

		groups, _, err := repo.ListByRunID(
			context.Background(),
			uuid.New(),
			uuid.New(),
			CursorFilter{Limit: 10},
		)
		require.NoError(t, err)
		assert.Len(t, groups, 2)
	})

	t.Run("ListByRunID respects limit", func(t *testing.T) {
		t.Parallel()

		repo := &mockMatchGroupRepository{}
		groupOne := &entities.MatchGroup{ID: uuid.New(), RunID: uuid.New()}
		groupTwo := &entities.MatchGroup{ID: uuid.New(), RunID: uuid.New()}

		_, err := repo.CreateBatch(context.Background(), []*entities.MatchGroup{groupOne, groupTwo})
		require.NoError(t, err)

		groups, _, err := repo.ListByRunID(
			context.Background(),
			uuid.New(),
			uuid.New(),
			CursorFilter{Limit: 1},
		)
		require.NoError(t, err)
		assert.Len(t, groups, 1)
	})
}
