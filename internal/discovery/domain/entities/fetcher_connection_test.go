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

func TestFetcherConnection_MarkAvailable(t *testing.T) {
	t.Parallel()

	ctx := context.Background()

	conn, err := entities.NewFetcherConnection(ctx, "fetcher-123", "primary-db", "POSTGRESQL")
	require.NoError(t, err)

	originalUpdatedAt := conn.UpdatedAt

	conn.MarkAvailable()

	assert.Equal(t, vo.ConnectionStatusAvailable, conn.Status)
	assert.False(t, conn.LastSeenAt.IsZero())
	assert.True(t, conn.UpdatedAt.After(originalUpdatedAt) || conn.UpdatedAt.Equal(originalUpdatedAt))
}

func TestFetcherConnection_MarkAvailable_NilReceiver(t *testing.T) {
	t.Parallel()

	var conn *entities.FetcherConnection
	assert.NotPanics(t, func() {
		conn.MarkAvailable()
	})
}

func TestFetcherConnection_ApplyFetcherStatus(t *testing.T) {
	t.Parallel()

	ctx := context.Background()

	conn, err := entities.NewFetcherConnection(ctx, "fetcher-123", "primary-db", "POSTGRESQL")
	require.NoError(t, err)

	originalLastSeenAt := conn.LastSeenAt

	conn.ApplyFetcherStatus("UNREACHABLE")

	assert.Equal(t, vo.ConnectionStatusUnreachable, conn.Status)
	assert.False(t, conn.LastSeenAt.Before(originalLastSeenAt))
}

func TestFetcherConnection_ApplyFetcherStatus_InvalidStatusFallsBackToUnknown(t *testing.T) {
	t.Parallel()

	ctx := context.Background()

	conn, err := entities.NewFetcherConnection(ctx, "fetcher-123", "primary-db", "POSTGRESQL")
	require.NoError(t, err)

	conn.ApplyFetcherStatus("BROKEN")

	assert.Equal(t, vo.ConnectionStatusUnknown, conn.Status)
}

func TestFetcherConnection_MarkUnreachable(t *testing.T) {
	t.Parallel()

	ctx := context.Background()

	conn, err := entities.NewFetcherConnection(ctx, "fetcher-123", "primary-db", "POSTGRESQL")
	require.NoError(t, err)

	conn.MarkAvailable()
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

	conn.UpdateDetails("db.example.com", 5432, "transactions", "PostgreSQL 17.1")

	assert.Equal(t, "db.example.com", conn.Host)
	assert.Equal(t, 5432, conn.Port)
	assert.Equal(t, "transactions", conn.DatabaseName)
	assert.Equal(t, "PostgreSQL 17.1", conn.ProductName)
}

func TestFetcherConnection_UpdateDetails_NilReceiver(t *testing.T) {
	t.Parallel()

	var conn *entities.FetcherConnection
	assert.NotPanics(t, func() {
		conn.UpdateDetails("db.example.com", 5432, "transactions", "PostgreSQL 17.1")
	})
}

func TestFetcherConnection_StatusTransitions(t *testing.T) {
	t.Parallel()

	ctx := context.Background()

	conn, err := entities.NewFetcherConnection(ctx, "fetcher-123", "primary-db", "POSTGRESQL")
	require.NoError(t, err)

	assert.Equal(t, vo.ConnectionStatusUnknown, conn.Status)

	conn.MarkAvailable()
	assert.Equal(t, vo.ConnectionStatusAvailable, conn.Status)

	conn.MarkUnreachable()
	assert.Equal(t, vo.ConnectionStatusUnreachable, conn.Status)

	conn.MarkAvailable()
	assert.Equal(t, vo.ConnectionStatusAvailable, conn.Status)
}

func TestFetcherConnection_UpdateDetailsUpdatesTimestamp(t *testing.T) {
	t.Parallel()

	ctx := context.Background()

	conn, err := entities.NewFetcherConnection(ctx, "fetcher-123", "primary-db", "POSTGRESQL")
	require.NoError(t, err)

	originalUpdatedAt := conn.UpdatedAt

	conn.UpdateDetails("host", 3306, "mydb", "MySQL 8.0")

	assert.True(t, conn.UpdatedAt.After(originalUpdatedAt) || conn.UpdatedAt.Equal(originalUpdatedAt))
}
