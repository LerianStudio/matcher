//go:build unit

package repositories

import (
	"context"
	"testing"

	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/LerianStudio/matcher/internal/matching/domain/entities"
)

func TestMatchItemRepositoryInterfaceCompiles(t *testing.T) {
	t.Parallel()

	var _ MatchItemRepository = (*mockMatchItemRepository)(nil)
}

type mockMatchItemRepository struct {
	items []*entities.MatchItem
}

func (repo *mockMatchItemRepository) CreateBatch(
	_ context.Context,
	items []*entities.MatchItem,
) ([]*entities.MatchItem, error) {
	repo.items = append(repo.items, items...)
	return items, nil
}

func (repo *mockMatchItemRepository) CreateBatchWithTx(
	ctx context.Context,
	_ Tx,
	items []*entities.MatchItem,
) ([]*entities.MatchItem, error) {
	return repo.CreateBatch(ctx, items)
}

func (repo *mockMatchItemRepository) ListByMatchGroupID(
	_ context.Context,
	matchGroupID uuid.UUID,
) ([]*entities.MatchItem, error) {
	var result []*entities.MatchItem

	for _, item := range repo.items {
		if item.MatchGroupID == matchGroupID {
			result = append(result, item)
		}
	}

	return result, nil
}

func (repo *mockMatchItemRepository) ListByMatchGroupIDs(
	_ context.Context,
	matchGroupIDs []uuid.UUID,
) (map[uuid.UUID][]*entities.MatchItem, error) {
	idSet := make(map[uuid.UUID]bool, len(matchGroupIDs))
	for _, id := range matchGroupIDs {
		idSet[id] = true
	}

	result := make(map[uuid.UUID][]*entities.MatchItem)

	for _, item := range repo.items {
		if idSet[item.MatchGroupID] {
			result[item.MatchGroupID] = append(result[item.MatchGroupID], item)
		}
	}

	return result, nil
}

func TestMockMatchItemRepositoryOperations(t *testing.T) {
	t.Parallel()

	t.Run("CreateBatch stores items", func(t *testing.T) {
		t.Parallel()

		repo := &mockMatchItemRepository{}
		itemOne := &entities.MatchItem{ID: uuid.New(), MatchGroupID: uuid.New()}
		itemTwo := &entities.MatchItem{ID: uuid.New(), MatchGroupID: uuid.New()}

		created, err := repo.CreateBatch(
			context.Background(),
			[]*entities.MatchItem{itemOne, itemTwo},
		)
		require.NoError(t, err)
		assert.Len(t, created, 2)
	})

	t.Run("ListByMatchGroupIDs groups items by group ID", func(t *testing.T) {
		t.Parallel()

		repo := &mockMatchItemRepository{}
		groupID1 := uuid.New()
		groupID2 := uuid.New()

		items := []*entities.MatchItem{
			{ID: uuid.New(), MatchGroupID: groupID1},
			{ID: uuid.New(), MatchGroupID: groupID1},
			{ID: uuid.New(), MatchGroupID: groupID2},
		}

		_, err := repo.CreateBatch(context.Background(), items)
		require.NoError(t, err)

		result, err := repo.ListByMatchGroupIDs(
			context.Background(),
			[]uuid.UUID{groupID1, groupID2},
		)
		require.NoError(t, err)
		assert.Len(t, result[groupID1], 2)
		assert.Len(t, result[groupID2], 1)
	})

	t.Run("CreateBatchWithTx uses same logic", func(t *testing.T) {
		t.Parallel()

		repo := &mockMatchItemRepository{}
		firstItem := &entities.MatchItem{ID: uuid.New(), MatchGroupID: uuid.New()}
		secondItem := &entities.MatchItem{ID: uuid.New(), MatchGroupID: uuid.New()}
		additionalItem := &entities.MatchItem{ID: uuid.New(), MatchGroupID: uuid.New()}

		created, err := repo.CreateBatch(
			context.Background(),
			[]*entities.MatchItem{firstItem, secondItem},
		)
		require.NoError(t, err)
		assert.Len(t, created, 2)

		created, err = repo.CreateBatchWithTx(
			context.Background(),
			nil,
			[]*entities.MatchItem{additionalItem},
		)
		require.NoError(t, err)
		assert.Len(t, created, 1)
		assert.Len(t, repo.items, 3)
	})
}
