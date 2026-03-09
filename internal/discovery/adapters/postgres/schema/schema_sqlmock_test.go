//go:build unit

package schema

import (
	"context"
	"encoding/json"
	"errors"
	"testing"
	"time"

	sqlmock "github.com/DATA-DOG/go-sqlmock"
	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/LerianStudio/matcher/internal/discovery/domain/entities"
	"github.com/LerianStudio/matcher/internal/shared/infrastructure/testutil"
)

var (
	errTestQuery = errors.New("query error")
	errTestExec  = errors.New("exec error")
)

func setupMockRepository(t *testing.T) (*Repository, sqlmock.Sqlmock, func()) {
	t.Helper()

	db, mock, err := sqlmock.New()
	require.NoError(t, err)

	provider := testutil.NewMockProviderFromDB(t, db)
	repo := NewRepository(provider)

	finish := func() {
		mock.ExpectClose()
		require.NoError(t, db.Close())
		require.NoError(t, mock.ExpectationsWereMet())
	}

	return repo, mock, finish
}

func createTestSchema() *entities.DiscoveredSchema {
	return &entities.DiscoveredSchema{
		ID:           uuid.New(),
		ConnectionID: uuid.New(),
		TableName:    "transactions",
		Columns: []entities.ColumnInfo{
			{Name: "id", Type: "uuid", Nullable: false},
			{Name: "amount", Type: "numeric", Nullable: false},
			{Name: "description", Type: "text", Nullable: true},
		},
		DiscoveredAt: time.Now().UTC(),
	}
}

func schemaColumns() []string {
	return []string{"id", "connection_id", "table_name", "columns", "discovered_at"}
}

func TestNewRepository(t *testing.T) {
	t.Parallel()

	t.Run("with valid provider", func(t *testing.T) {
		t.Parallel()

		provider := &testutil.MockInfrastructureProvider{}
		repo := NewRepository(provider)

		require.NotNil(t, repo)
		assert.Equal(t, provider, repo.provider)
	})

	t.Run("with nil provider", func(t *testing.T) {
		t.Parallel()

		repo := NewRepository(nil)

		require.NotNil(t, repo)
		assert.Nil(t, repo.provider)
	})
}

func TestRepository_UpsertBatch(t *testing.T) {
	t.Parallel()

	t.Run("nil repository", func(t *testing.T) {
		t.Parallel()

		var repo *Repository
		err := repo.UpsertBatch(context.Background(), []*entities.DiscoveredSchema{createTestSchema()})

		assert.ErrorIs(t, err, ErrRepoNotInitialized)
	})

	t.Run("nil provider", func(t *testing.T) {
		t.Parallel()

		repo := NewRepository(nil)
		err := repo.UpsertBatch(context.Background(), []*entities.DiscoveredSchema{createTestSchema()})

		assert.ErrorIs(t, err, ErrRepoNotInitialized)
	})

	t.Run("empty schemas", func(t *testing.T) {
		t.Parallel()

		repo, _, finish := setupMockRepository(t)
		defer finish()

		err := repo.UpsertBatch(context.Background(), []*entities.DiscoveredSchema{})

		assert.NoError(t, err)
	})

	t.Run("successful upsert batch", func(t *testing.T) {
		t.Parallel()

		repo, mock, finish := setupMockRepository(t)
		defer finish()

		schema := createTestSchema()
		columnsJSON, err := schema.ColumnsJSON()
		require.NoError(t, err)

		mock.ExpectBegin()
		mock.ExpectExec("INSERT INTO discovered_schemas").
			WithArgs(
				schema.ID,
				schema.ConnectionID,
				schema.TableName,
				columnsJSON,
				schema.DiscoveredAt,
			).
			WillReturnResult(sqlmock.NewResult(1, 1))
		mock.ExpectCommit()

		err = repo.UpsertBatch(context.Background(), []*entities.DiscoveredSchema{schema})

		assert.NoError(t, err)
	})

	t.Run("exec error", func(t *testing.T) {
		t.Parallel()

		repo, mock, finish := setupMockRepository(t)
		defer finish()

		schema := createTestSchema()

		mock.ExpectBegin()
		mock.ExpectExec("INSERT INTO discovered_schemas").
			WillReturnError(errTestExec)
		mock.ExpectRollback()

		err := repo.UpsertBatch(context.Background(), []*entities.DiscoveredSchema{schema})

		assert.Error(t, err)
	})
}

func TestRepository_FindByConnectionID(t *testing.T) {
	t.Parallel()

	t.Run("nil repository", func(t *testing.T) {
		t.Parallel()

		var repo *Repository
		result, err := repo.FindByConnectionID(context.Background(), uuid.New())

		assert.ErrorIs(t, err, ErrRepoNotInitialized)
		assert.Nil(t, result)
	})

	t.Run("successful find by connection id", func(t *testing.T) {
		t.Parallel()

		repo, mock, finish := setupMockRepository(t)
		defer finish()

		schema := createTestSchema()
		columnsJSON, err := json.Marshal(schema.Columns)
		require.NoError(t, err)

		rows := sqlmock.NewRows(schemaColumns()).
			AddRow(
				schema.ID,
				schema.ConnectionID,
				schema.TableName,
				columnsJSON,
				schema.DiscoveredAt,
			)

		mock.ExpectBegin()
		mock.ExpectQuery("SELECT .+ FROM discovered_schemas WHERE connection_id").
			WithArgs(schema.ConnectionID.String()).
			WillReturnRows(rows)
		mock.ExpectCommit()

		result, err := repo.FindByConnectionID(context.Background(), schema.ConnectionID)

		require.NoError(t, err)
		require.Len(t, result, 1)
		assert.Equal(t, schema.TableName, result[0].TableName)
		assert.Len(t, result[0].Columns, 3)
	})

	t.Run("query error", func(t *testing.T) {
		t.Parallel()

		repo, mock, finish := setupMockRepository(t)
		defer finish()

		mock.ExpectBegin()
		mock.ExpectQuery("SELECT .+ FROM discovered_schemas").
			WillReturnError(errTestQuery)
		mock.ExpectRollback()

		result, err := repo.FindByConnectionID(context.Background(), uuid.New())

		assert.Error(t, err)
		assert.Nil(t, result)
	})

	t.Run("empty result set", func(t *testing.T) {
		t.Parallel()

		repo, mock, finish := setupMockRepository(t)
		defer finish()

		rows := sqlmock.NewRows(schemaColumns())

		mock.ExpectBegin()
		mock.ExpectQuery("SELECT .+ FROM discovered_schemas WHERE connection_id").
			WillReturnRows(rows)
		mock.ExpectCommit()

		result, err := repo.FindByConnectionID(context.Background(), uuid.New())

		require.NoError(t, err)
		assert.Empty(t, result)
	})
}

func TestRepository_DeleteByConnectionID(t *testing.T) {
	t.Parallel()

	t.Run("nil repository", func(t *testing.T) {
		t.Parallel()

		var repo *Repository
		err := repo.DeleteByConnectionID(context.Background(), uuid.New())

		assert.ErrorIs(t, err, ErrRepoNotInitialized)
	})

	t.Run("successful delete", func(t *testing.T) {
		t.Parallel()

		repo, mock, finish := setupMockRepository(t)
		defer finish()

		connID := uuid.New()

		mock.ExpectBegin()
		mock.ExpectExec("DELETE FROM discovered_schemas WHERE connection_id").
			WithArgs(connID.String()).
			WillReturnResult(sqlmock.NewResult(0, 2))
		mock.ExpectCommit()

		err := repo.DeleteByConnectionID(context.Background(), connID)

		assert.NoError(t, err)
	})

	t.Run("exec error", func(t *testing.T) {
		t.Parallel()

		repo, mock, finish := setupMockRepository(t)
		defer finish()

		mock.ExpectBegin()
		mock.ExpectExec("DELETE FROM discovered_schemas").
			WillReturnError(errTestExec)
		mock.ExpectRollback()

		err := repo.DeleteByConnectionID(context.Background(), uuid.New())

		assert.Error(t, err)
	})
}

func TestRepository_UpsertBatchWithTx_NilTransaction(t *testing.T) {
	t.Parallel()

	repo, _, finish := setupMockRepository(t)
	defer finish()

	err := repo.UpsertBatchWithTx(context.Background(), nil, []*entities.DiscoveredSchema{createTestSchema()})

	assert.ErrorIs(t, err, ErrTransactionRequired)
}

func TestRepository_DeleteByConnectionIDWithTx_NilTransaction(t *testing.T) {
	t.Parallel()

	repo, _, finish := setupMockRepository(t)
	defer finish()

	err := repo.DeleteByConnectionIDWithTx(context.Background(), nil, uuid.New())

	assert.ErrorIs(t, err, ErrTransactionRequired)
}

func TestSchemaModel_ToDomain_Nil(t *testing.T) {
	t.Parallel()

	var model *SchemaModel
	result, err := model.ToDomain()

	assert.ErrorIs(t, err, ErrModelRequired)
	assert.Nil(t, result)
}

func TestFromDomain_Nil(t *testing.T) {
	t.Parallel()

	result, err := FromDomain(nil)

	assert.ErrorIs(t, err, ErrEntityRequired)
	assert.Nil(t, result)
}

func TestSchemaModel_RoundTrip(t *testing.T) {
	t.Parallel()

	original := createTestSchema()
	model, err := FromDomain(original)

	require.NoError(t, err)
	require.NotNil(t, model)

	entity, err := model.ToDomain()

	require.NoError(t, err)
	require.NotNil(t, entity)
	assert.Equal(t, original.ID, entity.ID)
	assert.Equal(t, original.ConnectionID, entity.ConnectionID)
	assert.Equal(t, original.TableName, entity.TableName)
	assert.Len(t, entity.Columns, 3)
	assert.Equal(t, "id", entity.Columns[0].Name)
	assert.Equal(t, "uuid", entity.Columns[0].Type)
	assert.False(t, entity.Columns[0].Nullable)
	assert.True(t, entity.Columns[2].Nullable)
}

func TestSchemaModel_ToDomain_InvalidJSON(t *testing.T) {
	t.Parallel()

	model := &SchemaModel{
		ID:           uuid.New(),
		ConnectionID: uuid.New(),
		TableName:    "test",
		Columns:      []byte("invalid json"),
		DiscoveredAt: time.Now().UTC(),
	}

	result, err := model.ToDomain()

	assert.Error(t, err)
	assert.Nil(t, result)
	assert.Contains(t, err.Error(), "unmarshal columns")
}
