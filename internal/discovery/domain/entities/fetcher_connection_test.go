//go:build unit

package entities_test

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/LerianStudio/matcher/internal/discovery/domain/entities"
	vo "github.com/LerianStudio/matcher/internal/discovery/domain/value_objects"
)

func TestNewFetcherConnection_ValidInput(t *testing.T) {
	t.Parallel()

	ctx := context.Background()

	conn, err := entities.NewFetcherConnection(ctx, "fetcher-123", "primary-db", "POSTGRESQL")
	require.NoError(t, err)
	require.NotNil(t, conn)

	assert.NotEmpty(t, conn.ID)
	assert.Equal(t, "fetcher-123", conn.FetcherConnID)
	assert.Equal(t, "primary-db", conn.ConfigName)
	assert.Equal(t, "POSTGRESQL", conn.DatabaseType)
	assert.Equal(t, vo.ConnectionStatusUnknown, conn.Status)
	assert.False(t, conn.SchemaDiscovered)
	assert.False(t, conn.CreatedAt.IsZero())
	assert.False(t, conn.UpdatedAt.IsZero())
	assert.False(t, conn.LastSeenAt.IsZero())
	assert.Equal(t, conn.CreatedAt, conn.UpdatedAt)
}

func TestNewFetcherConnection_EmptyFetcherConnID(t *testing.T) {
	t.Parallel()

	ctx := context.Background()

	conn, err := entities.NewFetcherConnection(ctx, "", "primary-db", "POSTGRESQL")
	require.Error(t, err)
	assert.Nil(t, conn)
	assert.Contains(t, err.Error(), "fetcher connection id")
}

func TestNewFetcherConnection_EmptyConfigName(t *testing.T) {
	t.Parallel()

	ctx := context.Background()

	conn, err := entities.NewFetcherConnection(ctx, "fetcher-123", "", "POSTGRESQL")
	require.Error(t, err)
	assert.Nil(t, conn)
	assert.Contains(t, err.Error(), "config name")
}

func TestNewFetcherConnection_EmptyDatabaseType(t *testing.T) {
	t.Parallel()

	ctx := context.Background()

	conn, err := entities.NewFetcherConnection(ctx, "fetcher-123", "primary-db", "")
	require.Error(t, err)
	assert.Nil(t, conn)
	assert.Contains(t, err.Error(), "database type")
}

func TestNewFetcherConnection_AllFieldsEmpty(t *testing.T) {
	t.Parallel()

	ctx := context.Background()

	conn, err := entities.NewFetcherConnection(ctx, "", "", "")
	require.Error(t, err)
	assert.Nil(t, conn)
}

func TestFetcherConnection_ApplyFetcherStatus(t *testing.T) {
	t.Parallel()

	ctx := context.Background()

	conn, err := entities.NewFetcherConnection(ctx, "fetcher-123", "primary-db", "POSTGRESQL")
	require.NoError(t, err)

	originalUpdatedAt := conn.UpdatedAt
	originalLastSeenAt := conn.LastSeenAt

	recognized := conn.ApplyFetcherStatus("UNREACHABLE")

	assert.True(t, recognized)
	assert.Equal(t, vo.ConnectionStatusUnreachable, conn.Status)
	assert.False(t, conn.LastSeenAt.Before(originalLastSeenAt))
	assert.True(t, conn.UpdatedAt.After(originalUpdatedAt) || conn.UpdatedAt.Equal(originalUpdatedAt))
}

func TestFetcherConnection_ApplyFetcherStatus_InvalidStatusFallsBackToUnknown(t *testing.T) {
	t.Parallel()

	ctx := context.Background()

	conn, err := entities.NewFetcherConnection(ctx, "fetcher-123", "primary-db", "POSTGRESQL")
	require.NoError(t, err)

	recognized := conn.ApplyFetcherStatus("BROKEN")

	assert.False(t, recognized)
	assert.Equal(t, vo.ConnectionStatusUnknown, conn.Status)
}

func TestFetcherConnection_ApplyFetcherStatus_NilReceiverReturnsFalse(t *testing.T) {
	t.Parallel()

	var conn *entities.FetcherConnection
	assert.False(t, conn.ApplyFetcherStatus("AVAILABLE"))
}

func TestFetcherConnection_MarkUnreachable(t *testing.T) {
	t.Parallel()

	ctx := context.Background()

	conn, err := entities.NewFetcherConnection(ctx, "fetcher-123", "primary-db", "POSTGRESQL")
	require.NoError(t, err)

	conn.ApplyFetcherStatus("AVAILABLE")
	conn.MarkUnreachable()

	assert.Equal(t, vo.ConnectionStatusUnreachable, conn.Status)
}

func TestFetcherConnection_MarkUnreachable_NilReceiver(t *testing.T) {
	t.Parallel()

	var conn *entities.FetcherConnection
	assert.NotPanics(t, func() {
		conn.MarkUnreachable()
	})
}

func TestFetcherConnection_MarkSchemaDiscovered(t *testing.T) {
	t.Parallel()

	ctx := context.Background()

	conn, err := entities.NewFetcherConnection(ctx, "fetcher-123", "primary-db", "POSTGRESQL")
	require.NoError(t, err)

	assert.False(t, conn.SchemaDiscovered)

	conn.MarkSchemaDiscovered()

	assert.True(t, conn.SchemaDiscovered)
}

func TestFetcherConnection_MarkSchemaDiscovered_NilReceiver(t *testing.T) {
	t.Parallel()

	var conn *entities.FetcherConnection
	assert.NotPanics(t, func() {
		conn.MarkSchemaDiscovered()
	})
}

func TestFetcherConnection_UpdateDetails(t *testing.T) {
	t.Parallel()

	ctx := context.Background()

	conn, err := entities.NewFetcherConnection(ctx, "fetcher-123", "primary-db", "POSTGRESQL")
	require.NoError(t, err)

	require.NoError(t, conn.UpdateDetails("db.example.com", 5432, "transactions", "PostgreSQL 17.1"))

	assert.Equal(t, "db.example.com", conn.Host)
	assert.Equal(t, 5432, conn.Port)
	assert.Equal(t, "transactions", conn.DatabaseName)
	assert.Equal(t, "PostgreSQL 17.1", conn.ProductName)
}

func TestFetcherConnection_UpdateDetails_NilReceiver(t *testing.T) {
	t.Parallel()

	var conn *entities.FetcherConnection

	var err error
	assert.NotPanics(t, func() {
		err = conn.UpdateDetails("db.example.com", 5432, "transactions", "PostgreSQL 17.1")
	})
	assert.NoError(t, err)
}

func TestFetcherConnection_UpdateDetails_InvalidPort(t *testing.T) {
	t.Parallel()

	ctx := context.Background()

	conn, err := entities.NewFetcherConnection(ctx, "fetcher-123", "primary-db", "POSTGRESQL")
	require.NoError(t, err)

	err = conn.UpdateDetails("db.example.com", 70000, "transactions", "PostgreSQL 17.1")

	require.Error(t, err)
	assert.ErrorIs(t, err, entities.ErrInvalidConnectionPort)
	assert.Empty(t, conn.Host)
	assert.Zero(t, conn.Port)
}

func TestFetcherConnection_UpdateDetails_NegativePort(t *testing.T) {
	t.Parallel()

	ctx := context.Background()

	conn, err := entities.NewFetcherConnection(ctx, "fetcher-123", "primary-db", "POSTGRESQL")
	require.NoError(t, err)

	err = conn.UpdateDetails("db.example.com", -1, "transactions", "PostgreSQL 17.1")

	require.Error(t, err)
	assert.ErrorIs(t, err, entities.ErrInvalidConnectionPort)
}

func TestFetcherConnection_UpdateDetails_PortBoundariesAccepted(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name string
		port int
	}{
		{name: "zero_port", port: 0},
		{name: "max_port", port: 65535},
	}

	for _, tt := range tests {
		tt := tt
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			conn, err := entities.NewFetcherConnection(context.Background(), "fetcher-123", "primary-db", "POSTGRESQL")
			require.NoError(t, err)

			err = conn.UpdateDetails("db.example.com", tt.port, "transactions", "PostgreSQL 17.1")

			require.NoError(t, err)
			assert.Equal(t, tt.port, conn.Port)
			assert.Equal(t, "db.example.com", conn.Host)
		})
	}
}

func TestFetcherConnection_StatusTransitions(t *testing.T) {
	t.Parallel()

	ctx := context.Background()

	conn, err := entities.NewFetcherConnection(ctx, "fetcher-123", "primary-db", "POSTGRESQL")
	require.NoError(t, err)

	assert.Equal(t, vo.ConnectionStatusUnknown, conn.Status)

	conn.ApplyFetcherStatus("AVAILABLE")
	assert.Equal(t, vo.ConnectionStatusAvailable, conn.Status)

	conn.MarkUnreachable()
	assert.Equal(t, vo.ConnectionStatusUnreachable, conn.Status)

	conn.ApplyFetcherStatus("AVAILABLE")
	assert.Equal(t, vo.ConnectionStatusAvailable, conn.Status)
}

func TestFetcherConnection_UpdateDetailsUpdatesTimestamp(t *testing.T) {
	t.Parallel()

	ctx := context.Background()

	conn, err := entities.NewFetcherConnection(ctx, "fetcher-123", "primary-db", "POSTGRESQL")
	require.NoError(t, err)

	originalUpdatedAt := conn.UpdatedAt

	require.NoError(t, conn.UpdateDetails("host", 3306, "mydb", "MySQL 8.0"))

	assert.True(t, conn.UpdatedAt.After(originalUpdatedAt) || conn.UpdatedAt.Equal(originalUpdatedAt))
}
