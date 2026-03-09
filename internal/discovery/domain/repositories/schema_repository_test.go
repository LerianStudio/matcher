//go:build unit

package repositories

import (
	"context"
	"database/sql"
	"errors"
	"testing"

	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/LerianStudio/matcher/internal/discovery/domain/entities"
)

// errSchemaNotFound is a local sentinel for mock testing.
var errSchemaNotFound = errors.New("discovered schema not found")

func TestSchemaRepositoryInterfaceCompiles(t *testing.T) {
	t.Parallel()

	var _ SchemaRepository = (*mockSchemaRepository)(nil)
}

type mockSchemaRepository struct {
	schemas map[uuid.UUID][]*entities.DiscoveredSchema
}

func (m *mockSchemaRepository) UpsertBatch(
	ctx context.Context,
	schemas []*entities.DiscoveredSchema,
) error {
	return m.UpsertBatchWithTx(ctx, nil, schemas)
}

func (m *mockSchemaRepository) UpsertBatchWithTx(
	_ context.Context,
	_ *sql.Tx,
	schemas []*entities.DiscoveredSchema,
) error {
	if m.schemas == nil {
		m.schemas = make(map[uuid.UUID][]*entities.DiscoveredSchema)
	}

	for _, s := range schemas {
		m.schemas[s.ConnectionID] = append(m.schemas[s.ConnectionID], s)
	}

	return nil
}

func (m *mockSchemaRepository) FindByConnectionID(
	_ context.Context,
	connectionID uuid.UUID,
) ([]*entities.DiscoveredSchema, error) {
	if schemas, ok := m.schemas[connectionID]; ok {
		return schemas, nil
	}

	return nil, nil
}

func (m *mockSchemaRepository) DeleteByConnectionID(
	ctx context.Context,
	connectionID uuid.UUID,
) error {
	return m.DeleteByConnectionIDWithTx(ctx, nil, connectionID)
}

func (m *mockSchemaRepository) DeleteByConnectionIDWithTx(
	_ context.Context,
	_ *sql.Tx,
	connectionID uuid.UUID,
) error {
	delete(m.schemas, connectionID)

	return nil
}

func TestMockSchemaRepositoryOperations(t *testing.T) {
	t.Parallel()

	t.Run("UpsertBatch stores schemas", func(t *testing.T) {
		t.Parallel()

		repo := &mockSchemaRepository{}
		connID := uuid.New()
		schemas := []*entities.DiscoveredSchema{
			{ID: uuid.New(), ConnectionID: connID, TableName: "table1"},
			{ID: uuid.New(), ConnectionID: connID, TableName: "table2"},
		}

		err := repo.UpsertBatch(context.Background(), schemas)
		require.NoError(t, err)

		found, err := repo.FindByConnectionID(context.Background(), connID)
		require.NoError(t, err)
		assert.Len(t, found, 2)
	})

	t.Run("FindByConnectionID returns empty for missing connection", func(t *testing.T) {
		t.Parallel()

		repo := &mockSchemaRepository{}

		found, err := repo.FindByConnectionID(context.Background(), uuid.New())
		require.NoError(t, err)
		assert.Nil(t, found)
	})

	t.Run("DeleteByConnectionID removes all schemas for connection", func(t *testing.T) {
		t.Parallel()

		repo := &mockSchemaRepository{}
		connID := uuid.New()
		schemas := []*entities.DiscoveredSchema{
			{ID: uuid.New(), ConnectionID: connID, TableName: "table1"},
		}

		err := repo.UpsertBatch(context.Background(), schemas)
		require.NoError(t, err)

		err = repo.DeleteByConnectionID(context.Background(), connID)
		require.NoError(t, err)

		found, err := repo.FindByConnectionID(context.Background(), connID)
		require.NoError(t, err)
		assert.Nil(t, found)
	})

	t.Run("UpsertBatch appends to existing schemas", func(t *testing.T) {
		t.Parallel()

		repo := &mockSchemaRepository{}
		connID := uuid.New()

		batch1 := []*entities.DiscoveredSchema{
			{ID: uuid.New(), ConnectionID: connID, TableName: "table1"},
		}

		batch2 := []*entities.DiscoveredSchema{
			{ID: uuid.New(), ConnectionID: connID, TableName: "table2"},
		}

		err := repo.UpsertBatch(context.Background(), batch1)
		require.NoError(t, err)

		err = repo.UpsertBatch(context.Background(), batch2)
		require.NoError(t, err)

		found, err := repo.FindByConnectionID(context.Background(), connID)
		require.NoError(t, err)
		assert.Len(t, found, 2)
	})
}

// Ensure errSchemaNotFound is used to avoid unused variable warning.
func TestSchemaNotFoundErrorExists(t *testing.T) {
	t.Parallel()

	assert.NotNil(t, errSchemaNotFound)
	assert.Equal(t, "discovered schema not found", errSchemaNotFound.Error())
}
