//go:build unit

package connection

import (
	"context"
	"database/sql"
	"errors"
	"testing"
	"time"

	sqlmock "github.com/DATA-DOG/go-sqlmock"
	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/LerianStudio/matcher/internal/discovery/domain/entities"
	vo "github.com/LerianStudio/matcher/internal/discovery/domain/value_objects"
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

func createTestConnection() *entities.FetcherConnection {
	now := time.Now().UTC()

	return &entities.FetcherConnection{
		ID:               uuid.New(),
		FetcherConnID:    "fetcher-conn-123",
		ConfigName:       "test-config",
		DatabaseType:     "POSTGRESQL",
		Host:             "localhost",
		Port:             5432,
		DatabaseName:     "testdb",
		ProductName:      "PostgreSQL 17",
		Status:           vo.ConnectionStatusAvailable,
		LastSeenAt:       now,
		SchemaDiscovered: true,
		CreatedAt:        now,
		UpdatedAt:        now,
	}
}

func connectionColumns() []string {
	return []string{
		"id", "fetcher_conn_id", "config_name", "database_type",
		"host", "port", "database_name", "product_name",
		"status", "last_seen_at", "schema_discovered",
		"created_at", "updated_at",
	}
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

func TestRepository_Upsert(t *testing.T) {
	t.Parallel()

	t.Run("nil repository", func(t *testing.T) {
		t.Parallel()

		var repo *Repository
		err := repo.Upsert(context.Background(), createTestConnection())

		assert.ErrorIs(t, err, ErrRepoNotInitialized)
	})

	t.Run("nil provider", func(t *testing.T) {
		t.Parallel()

		repo := NewRepository(nil)
		err := repo.Upsert(context.Background(), createTestConnection())

		assert.ErrorIs(t, err, ErrRepoNotInitialized)
	})

	t.Run("nil entity", func(t *testing.T) {
		t.Parallel()

		repo, _, finish := setupMockRepository(t)
		defer finish()

		err := repo.Upsert(context.Background(), nil)

		assert.ErrorIs(t, err, ErrEntityRequired)
	})

	t.Run("successful upsert", func(t *testing.T) {
		t.Parallel()

		repo, mock, finish := setupMockRepository(t)
		defer finish()

		conn := createTestConnection()

		mock.ExpectBegin()
		mock.ExpectExec("INSERT INTO fetcher_connections").
			WithArgs(
				conn.ID,
				conn.FetcherConnID,
				conn.ConfigName,
				conn.DatabaseType,
				conn.Host,
				conn.Port,
				conn.DatabaseName,
				conn.ProductName,
				conn.Status.String(),
				conn.LastSeenAt,
				conn.SchemaDiscovered,
				conn.CreatedAt,
				conn.UpdatedAt,
			).
			WillReturnResult(sqlmock.NewResult(1, 1))
		mock.ExpectCommit()

		err := repo.Upsert(context.Background(), conn)

		assert.NoError(t, err)
	})

	t.Run("exec error", func(t *testing.T) {
		t.Parallel()

		repo, mock, finish := setupMockRepository(t)
		defer finish()

		conn := createTestConnection()

		mock.ExpectBegin()
		mock.ExpectExec("INSERT INTO fetcher_connections").
			WillReturnError(errTestExec)
		mock.ExpectRollback()

		err := repo.Upsert(context.Background(), conn)

		assert.Error(t, err)
	})
}

func TestRepository_FindAll(t *testing.T) {
	t.Parallel()

	t.Run("nil repository", func(t *testing.T) {
		t.Parallel()

		var repo *Repository
		result, err := repo.FindAll(context.Background())

		assert.ErrorIs(t, err, ErrRepoNotInitialized)
		assert.Nil(t, result)
	})

	t.Run("successful find all", func(t *testing.T) {
		t.Parallel()

		repo, mock, finish := setupMockRepository(t)
		defer finish()

		conn := createTestConnection()
		rows := sqlmock.NewRows(connectionColumns()).
			AddRow(
				conn.ID,
				conn.FetcherConnID,
				conn.ConfigName,
				conn.DatabaseType,
				conn.Host,
				conn.Port,
				conn.DatabaseName,
				conn.ProductName,
				conn.Status.String(),
				conn.LastSeenAt,
				conn.SchemaDiscovered,
				conn.CreatedAt,
				conn.UpdatedAt,
			)

		mock.ExpectBegin()
		mock.ExpectQuery("SELECT .+ FROM fetcher_connections").
			WillReturnRows(rows)
		mock.ExpectCommit()

		result, err := repo.FindAll(context.Background())

		require.NoError(t, err)
		require.Len(t, result, 1)
		assert.Equal(t, conn.FetcherConnID, result[0].FetcherConnID)
		assert.Equal(t, conn.ConfigName, result[0].ConfigName)
		assert.Equal(t, conn.DatabaseType, result[0].DatabaseType)
	})

	t.Run("query error", func(t *testing.T) {
		t.Parallel()

		repo, mock, finish := setupMockRepository(t)
		defer finish()

		mock.ExpectBegin()
		mock.ExpectQuery("SELECT .+ FROM fetcher_connections").
			WillReturnError(errTestQuery)
		mock.ExpectRollback()

		result, err := repo.FindAll(context.Background())

		assert.Error(t, err)
		assert.Nil(t, result)
	})

	t.Run("empty result set", func(t *testing.T) {
		t.Parallel()

		repo, mock, finish := setupMockRepository(t)
		defer finish()

		rows := sqlmock.NewRows(connectionColumns())

		mock.ExpectBegin()
		mock.ExpectQuery("SELECT .+ FROM fetcher_connections").
			WillReturnRows(rows)
		mock.ExpectCommit()

		result, err := repo.FindAll(context.Background())

		require.NoError(t, err)
		assert.Empty(t, result)
	})
}

func TestRepository_FindByID(t *testing.T) {
	t.Parallel()

	t.Run("nil repository", func(t *testing.T) {
		t.Parallel()

		var repo *Repository
		result, err := repo.FindByID(context.Background(), uuid.New())

		assert.ErrorIs(t, err, ErrRepoNotInitialized)
		assert.Nil(t, result)
	})

	t.Run("successful find by id", func(t *testing.T) {
		t.Parallel()

		repo, mock, finish := setupMockRepository(t)
		defer finish()

		conn := createTestConnection()
		rows := sqlmock.NewRows(connectionColumns()).
			AddRow(
				conn.ID,
				conn.FetcherConnID,
				conn.ConfigName,
				conn.DatabaseType,
				conn.Host,
				conn.Port,
				conn.DatabaseName,
				conn.ProductName,
				conn.Status.String(),
				conn.LastSeenAt,
				conn.SchemaDiscovered,
				conn.CreatedAt,
				conn.UpdatedAt,
			)

		mock.ExpectBegin()
		mock.ExpectQuery("SELECT .+ FROM fetcher_connections WHERE id").
			WithArgs(conn.ID.String()).
			WillReturnRows(rows)
		mock.ExpectCommit()

		result, err := repo.FindByID(context.Background(), conn.ID)

		require.NoError(t, err)
		require.NotNil(t, result)
		assert.Equal(t, conn.FetcherConnID, result.FetcherConnID)
	})

	t.Run("not found", func(t *testing.T) {
		t.Parallel()

		repo, mock, finish := setupMockRepository(t)
		defer finish()

		mock.ExpectBegin()
		mock.ExpectQuery("SELECT .+ FROM fetcher_connections WHERE id").
			WillReturnError(sql.ErrNoRows)
		mock.ExpectRollback()

		result, err := repo.FindByID(context.Background(), uuid.New())

		assert.ErrorIs(t, err, ErrConnectionNotFound)
		assert.Nil(t, result)
	})
}

func TestRepository_FindByFetcherID(t *testing.T) {
	t.Parallel()

	t.Run("nil repository", func(t *testing.T) {
		t.Parallel()

		var repo *Repository
		result, err := repo.FindByFetcherID(context.Background(), "some-id")

		assert.ErrorIs(t, err, ErrRepoNotInitialized)
		assert.Nil(t, result)
	})

	t.Run("successful find by fetcher id", func(t *testing.T) {
		t.Parallel()

		repo, mock, finish := setupMockRepository(t)
		defer finish()

		conn := createTestConnection()
		rows := sqlmock.NewRows(connectionColumns()).
			AddRow(
				conn.ID,
				conn.FetcherConnID,
				conn.ConfigName,
				conn.DatabaseType,
				conn.Host,
				conn.Port,
				conn.DatabaseName,
				conn.ProductName,
				conn.Status.String(),
				conn.LastSeenAt,
				conn.SchemaDiscovered,
				conn.CreatedAt,
				conn.UpdatedAt,
			)

		mock.ExpectBegin()
		mock.ExpectQuery("SELECT .+ FROM fetcher_connections WHERE fetcher_conn_id").
			WithArgs(conn.FetcherConnID).
			WillReturnRows(rows)
		mock.ExpectCommit()

		result, err := repo.FindByFetcherID(context.Background(), conn.FetcherConnID)

		require.NoError(t, err)
		require.NotNil(t, result)
		assert.Equal(t, conn.FetcherConnID, result.FetcherConnID)
	})

	t.Run("not found", func(t *testing.T) {
		t.Parallel()

		repo, mock, finish := setupMockRepository(t)
		defer finish()

		mock.ExpectBegin()
		mock.ExpectQuery("SELECT .+ FROM fetcher_connections WHERE fetcher_conn_id").
			WillReturnError(sql.ErrNoRows)
		mock.ExpectRollback()

		result, err := repo.FindByFetcherID(context.Background(), "unknown-id")

		assert.ErrorIs(t, err, ErrConnectionNotFound)
		assert.Nil(t, result)
	})
}

func TestRepository_DeleteStale(t *testing.T) {
	t.Parallel()

	t.Run("nil repository", func(t *testing.T) {
		t.Parallel()

		var repo *Repository
		count, err := repo.DeleteStale(context.Background(), 24*time.Hour)

		assert.ErrorIs(t, err, ErrRepoNotInitialized)
		assert.Equal(t, int64(0), count)
	})

	t.Run("successful delete stale", func(t *testing.T) {
		t.Parallel()

		repo, mock, finish := setupMockRepository(t)
		defer finish()

		mock.ExpectBegin()
		mock.ExpectExec("DELETE FROM fetcher_connections WHERE last_seen_at").
			WithArgs("86400 seconds").
			WillReturnResult(sqlmock.NewResult(0, 3))
		mock.ExpectCommit()

		count, err := repo.DeleteStale(context.Background(), 24*time.Hour)

		require.NoError(t, err)
		assert.Equal(t, int64(3), count)
	})

	t.Run("exec error", func(t *testing.T) {
		t.Parallel()

		repo, mock, finish := setupMockRepository(t)
		defer finish()

		mock.ExpectBegin()
		mock.ExpectExec("DELETE FROM fetcher_connections").
			WillReturnError(errTestExec)
		mock.ExpectRollback()

		count, err := repo.DeleteStale(context.Background(), 24*time.Hour)

		assert.Error(t, err)
		assert.Equal(t, int64(0), count)
	})
}

func TestRepository_UpsertWithTx_NilTransaction(t *testing.T) {
	t.Parallel()

	repo, _, finish := setupMockRepository(t)
	defer finish()

	err := repo.UpsertWithTx(context.Background(), nil, createTestConnection())

	assert.ErrorIs(t, err, ErrTransactionRequired)
}

func TestRepository_DeleteStaleWithTx_NilTransaction(t *testing.T) {
	t.Parallel()

	repo, _, finish := setupMockRepository(t)
	defer finish()

	count, err := repo.DeleteStaleWithTx(context.Background(), nil, 24*time.Hour)

	assert.ErrorIs(t, err, ErrTransactionRequired)
	assert.Equal(t, int64(0), count)
}

func TestConnectionModel_ToDomain_Nil(t *testing.T) {
	t.Parallel()

	var model *ConnectionModel
	result := model.ToDomain()

	assert.Nil(t, result)
}

func TestFromDomain_Nil(t *testing.T) {
	t.Parallel()

	result := FromDomain(nil)

	assert.Nil(t, result)
}

func TestConnectionModel_RoundTrip(t *testing.T) {
	t.Parallel()

	original := createTestConnection()
	model := FromDomain(original)

	require.NotNil(t, model)

	entity := model.ToDomain()

	require.NotNil(t, entity)
	assert.Equal(t, original.ID, entity.ID)
	assert.Equal(t, original.FetcherConnID, entity.FetcherConnID)
	assert.Equal(t, original.ConfigName, entity.ConfigName)
	assert.Equal(t, original.DatabaseType, entity.DatabaseType)
	assert.Equal(t, original.Host, entity.Host)
	assert.Equal(t, original.Port, entity.Port)
	assert.Equal(t, original.DatabaseName, entity.DatabaseName)
	assert.Equal(t, original.ProductName, entity.ProductName)
	assert.Equal(t, original.Status, entity.Status)
	assert.Equal(t, original.SchemaDiscovered, entity.SchemaDiscovered)
}
