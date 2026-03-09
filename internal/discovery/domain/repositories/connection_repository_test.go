//go:build unit

package repositories

import (
	"context"
	"database/sql"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/LerianStudio/matcher/internal/discovery/domain/entities"
)

func TestConnectionRepositoryInterfaceCompiles(t *testing.T) {
	t.Parallel()

	var _ ConnectionRepository = (*mockConnectionRepository)(nil)
}

func TestConnectionSentinelErrors(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name    string
		err     error
		message string
	}{
		{
			name:    "ErrConnectionNotFound",
			err:     ErrConnectionNotFound,
			message: "fetcher connection not found",
		},
		{
			name:    "ErrProviderRequired",
			err:     ErrProviderRequired,
			message: "infrastructure provider is required",
		},
		{
			name:    "ErrRepoNotInitialized",
			err:     ErrRepoNotInitialized,
			message: "connection repository not initialized",
		},
		{
			name:    "ErrEntityRequired",
			err:     ErrEntityRequired,
			message: "fetcher connection entity is required",
		},
		{
			name:    "ErrModelRequired",
			err:     ErrModelRequired,
			message: "fetcher connection model is required",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			assert.EqualError(t, tt.err, tt.message)
		})
	}
}

type mockConnectionRepository struct {
	connections map[uuid.UUID]*entities.FetcherConnection
}

func (m *mockConnectionRepository) Upsert(
	ctx context.Context,
	conn *entities.FetcherConnection,
) error {
	return m.UpsertWithTx(ctx, nil, conn)
}

func (m *mockConnectionRepository) UpsertWithTx(
	_ context.Context,
	_ *sql.Tx,
	conn *entities.FetcherConnection,
) error {
	if m.connections == nil {
		m.connections = make(map[uuid.UUID]*entities.FetcherConnection)
	}

	m.connections[conn.ID] = conn

	return nil
}

func (m *mockConnectionRepository) FindAll(
	_ context.Context,
) ([]*entities.FetcherConnection, error) {
	result := make([]*entities.FetcherConnection, 0, len(m.connections))

	for _, conn := range m.connections {
		result = append(result, conn)
	}

	return result, nil
}

func (m *mockConnectionRepository) FindByID(
	_ context.Context,
	id uuid.UUID,
) (*entities.FetcherConnection, error) {
	if conn, ok := m.connections[id]; ok {
		return conn, nil
	}

	return nil, ErrConnectionNotFound
}

func (m *mockConnectionRepository) FindByFetcherID(
	_ context.Context,
	fetcherConnID string,
) (*entities.FetcherConnection, error) {
	for _, conn := range m.connections {
		if conn.FetcherConnID == fetcherConnID {
			return conn, nil
		}
	}

	return nil, ErrConnectionNotFound
}

func (m *mockConnectionRepository) DeleteStale(
	ctx context.Context,
	notSeenSince time.Duration,
) (int64, error) {
	return m.DeleteStaleWithTx(ctx, nil, notSeenSince)
}

func (m *mockConnectionRepository) DeleteStaleWithTx(
	_ context.Context,
	_ *sql.Tx,
	_ time.Duration,
) (int64, error) {
	return 0, nil
}

func TestMockConnectionRepositoryOperations(t *testing.T) {
	t.Parallel()

	t.Run("Upsert stores connection", func(t *testing.T) {
		t.Parallel()

		repo := &mockConnectionRepository{}
		connID := uuid.New()
		conn := &entities.FetcherConnection{
			ID:            connID,
			FetcherConnID: "fetcher-123",
		}

		err := repo.Upsert(context.Background(), conn)
		require.NoError(t, err)

		found, err := repo.FindByID(context.Background(), connID)
		require.NoError(t, err)
		assert.Equal(t, connID, found.ID)
	})

	t.Run("FindByID returns error for missing connection", func(t *testing.T) {
		t.Parallel()

		repo := &mockConnectionRepository{}

		_, err := repo.FindByID(context.Background(), uuid.New())
		assert.ErrorIs(t, err, ErrConnectionNotFound)
	})

	t.Run("FindByFetcherID retrieves connection by external ID", func(t *testing.T) {
		t.Parallel()

		repo := &mockConnectionRepository{}
		connID := uuid.New()
		fetcherID := "fetcher-456"
		conn := &entities.FetcherConnection{
			ID:            connID,
			FetcherConnID: fetcherID,
		}

		err := repo.Upsert(context.Background(), conn)
		require.NoError(t, err)

		found, err := repo.FindByFetcherID(context.Background(), fetcherID)
		require.NoError(t, err)
		assert.Equal(t, fetcherID, found.FetcherConnID)
	})

	t.Run("FindByFetcherID returns error for missing connection", func(t *testing.T) {
		t.Parallel()

		repo := &mockConnectionRepository{}

		_, err := repo.FindByFetcherID(context.Background(), "nonexistent")
		assert.ErrorIs(t, err, ErrConnectionNotFound)
	})

	t.Run("FindAll returns all connections", func(t *testing.T) {
		t.Parallel()

		repo := &mockConnectionRepository{}
		conn1 := &entities.FetcherConnection{ID: uuid.New(), FetcherConnID: "conn-1"}
		conn2 := &entities.FetcherConnection{ID: uuid.New(), FetcherConnID: "conn-2"}

		err := repo.Upsert(context.Background(), conn1)
		require.NoError(t, err)

		err = repo.Upsert(context.Background(), conn2)
		require.NoError(t, err)

		all, err := repo.FindAll(context.Background())
		require.NoError(t, err)
		assert.Len(t, all, 2)
	})
}
