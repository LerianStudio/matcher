//go:build unit

package redis

import (
	"context"
	"encoding/json"
	"testing"
	"time"

	"github.com/alicebob/miniredis/v2"
	goredis "github.com/redis/go-redis/v9"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/LerianStudio/matcher/internal/auth"
	sharedPorts "github.com/LerianStudio/matcher/internal/shared/ports"
)

func testCacheContext() context.Context {
	return context.WithValue(context.Background(), auth.TenantIDKey, "tenant-1")
}

func mustConnectionsKey(t *testing.T, ctx context.Context) string {
	t.Helper()

	key, err := connectionsKeyForContext(ctx)
	require.NoError(t, err)

	return key
}

func mustSchemaKey(t *testing.T, ctx context.Context, connectionID string) string {
	t.Helper()

	key, err := schemaKey(ctx, connectionID)
	require.NoError(t, err)

	return key
}

func setupRedis(t *testing.T) (*goredis.Client, *miniredis.Miniredis, func()) {
	t.Helper()

	srv := miniredis.RunT(t)
	client := goredis.NewClient(&goredis.Options{Addr: srv.Addr()})

	cleanup := func() {
		if err := client.Close(); err != nil {
			t.Logf("failed to close redis client: %v", err)
		}

		srv.Close()
	}

	return client, srv, cleanup
}

func createTestConnections() []*sharedPorts.FetcherConnection {
	return []*sharedPorts.FetcherConnection{
		{
			ID:           "conn-1",
			ConfigName:   "test-config-1",
			DatabaseType: "POSTGRESQL",
			Host:         "localhost",
			Port:         5432,
			DatabaseName: "testdb1",
			ProductName:  "PostgreSQL 17",
			Status:       "AVAILABLE",
		},
		{
			ID:           "conn-2",
			ConfigName:   "test-config-2",
			DatabaseType: "MYSQL",
			Host:         "localhost",
			Port:         3306,
			DatabaseName: "testdb2",
			ProductName:  "MySQL 8",
			Status:       "AVAILABLE",
		},
	}
}

func createTestFetcherSchema() *sharedPorts.FetcherSchema {
	return &sharedPorts.FetcherSchema{
		ConnectionID: "conn-1",
		Tables: []sharedPorts.FetcherTableSchema{
			{
				TableName: "transactions",
				Columns: []sharedPorts.FetcherColumnInfo{
					{Name: "id", Type: "uuid", Nullable: false},
					{Name: "amount", Type: "numeric", Nullable: false},
				},
			},
		},
		DiscoveredAt: time.Now().UTC(),
	}
}

func TestNewSchemaCache(t *testing.T) {
	t.Parallel()

	t.Run("with valid client", func(t *testing.T) {
		t.Parallel()

		client, _, cleanup := setupRedis(t)
		defer cleanup()

		cache, err := NewSchemaCache(client)

		require.NoError(t, err)
		require.NotNil(t, cache)
	})

	t.Run("with nil client", func(t *testing.T) {
		t.Parallel()

		cache, err := NewSchemaCache(nil)

		assert.ErrorIs(t, err, ErrRedisClientRequired)
		assert.Nil(t, cache)
	})
}

func TestSchemaCache_GetConnections(t *testing.T) {
	t.Parallel()

	t.Run("nil cache", func(t *testing.T) {
		t.Parallel()

		var cache *SchemaCache
		result, err := cache.GetConnections(context.Background())

		assert.ErrorIs(t, err, ErrCacheNotInitialized)
		assert.Nil(t, result)
	})

	t.Run("cache miss", func(t *testing.T) {
		t.Parallel()

		client, _, cleanup := setupRedis(t)
		defer cleanup()

		cache, err := NewSchemaCache(client)
		require.NoError(t, err)

		result, err := cache.GetConnections(testCacheContext())

		assert.ErrorIs(t, err, ErrCacheMiss)
		assert.Nil(t, result)
	})

	t.Run("cache hit", func(t *testing.T) {
		t.Parallel()

		client, _, cleanup := setupRedis(t)
		defer cleanup()

		cache, err := NewSchemaCache(client)
		require.NoError(t, err)

		conns := createTestConnections()
		err = cache.SetConnections(testCacheContext(), conns, 5*time.Minute)
		require.NoError(t, err)

		result, err := cache.GetConnections(testCacheContext())

		require.NoError(t, err)
		require.Len(t, result, 2)
		assert.Equal(t, "conn-1", result[0].ID)
		assert.Equal(t, "conn-2", result[1].ID)
	})

	t.Run("invalid json in cache", func(t *testing.T) {
		t.Parallel()

		client, _, cleanup := setupRedis(t)
		defer cleanup()

		// Seed invalid JSON directly
		ctx := testCacheContext()
		err := client.Set(ctx, mustConnectionsKey(t, ctx), "not-json", 5*time.Minute).Err()
		require.NoError(t, err)

		cache, err := NewSchemaCache(client)
		require.NoError(t, err)

		result, err := cache.GetConnections(ctx)

		assert.Error(t, err)
		assert.Nil(t, result)
		assert.Contains(t, err.Error(), "unmarshal cached connections")
	})
}

func TestSchemaCache_SetConnections(t *testing.T) {
	t.Parallel()

	t.Run("nil cache", func(t *testing.T) {
		t.Parallel()

		var cache *SchemaCache
		err := cache.SetConnections(testCacheContext(), createTestConnections(), 5*time.Minute)

		assert.ErrorIs(t, err, ErrCacheNotInitialized)
	})

	t.Run("successful set", func(t *testing.T) {
		t.Parallel()

		client, srv, cleanup := setupRedis(t)
		defer cleanup()

		cache, err := NewSchemaCache(client)
		require.NoError(t, err)

		conns := createTestConnections()
		ctx := testCacheContext()
		err = cache.SetConnections(ctx, conns, 5*time.Minute)

		require.NoError(t, err)

		// Verify data was stored
		assert.True(t, srv.Exists(mustConnectionsKey(t, ctx)))
	})

	t.Run("with ttl expiration", func(t *testing.T) {
		t.Parallel()

		client, srv, cleanup := setupRedis(t)
		defer cleanup()

		cache, err := NewSchemaCache(client)
		require.NoError(t, err)

		conns := createTestConnections()
		ctx := testCacheContext()
		err = cache.SetConnections(ctx, conns, 1*time.Second)
		require.NoError(t, err)

		// Fast-forward time in miniredis
		srv.FastForward(2 * time.Second)

		result, err := cache.GetConnections(ctx)

		assert.ErrorIs(t, err, ErrCacheMiss)
		assert.Nil(t, result)
	})
}

func TestSchemaCache_GetSchema(t *testing.T) {
	t.Parallel()

	t.Run("nil cache", func(t *testing.T) {
		t.Parallel()

		var cache *SchemaCache
		result, err := cache.GetSchema(testCacheContext(), "conn-1")

		assert.ErrorIs(t, err, ErrCacheNotInitialized)
		assert.Nil(t, result)
	})

	t.Run("cache miss", func(t *testing.T) {
		t.Parallel()

		client, _, cleanup := setupRedis(t)
		defer cleanup()

		cache, err := NewSchemaCache(client)
		require.NoError(t, err)

		result, err := cache.GetSchema(context.Background(), "conn-1")

		assert.ErrorIs(t, err, ErrCacheMiss)
		assert.Nil(t, result)
	})

	t.Run("cache hit", func(t *testing.T) {
		t.Parallel()

		client, _, cleanup := setupRedis(t)
		defer cleanup()

		cache, err := NewSchemaCache(client)
		require.NoError(t, err)

		schema := createTestFetcherSchema()
		err = cache.SetSchema(testCacheContext(), "conn-1", schema, 5*time.Minute)
		require.NoError(t, err)

		result, err := cache.GetSchema(testCacheContext(), "conn-1")

		require.NoError(t, err)
		require.NotNil(t, result)
		assert.Equal(t, "conn-1", result.ConnectionID)
		assert.Len(t, result.Tables, 1)
		assert.Equal(t, "transactions", result.Tables[0].TableName)
	})

	t.Run("invalid json in cache", func(t *testing.T) {
		t.Parallel()

		client, _, cleanup := setupRedis(t)
		defer cleanup()

		// Seed invalid JSON directly
		ctx := testCacheContext()
		err := client.Set(ctx, mustSchemaKey(t, ctx, "conn-1"), "not-json", 5*time.Minute).Err()
		require.NoError(t, err)

		cache, err := NewSchemaCache(client)
		require.NoError(t, err)

		result, err := cache.GetSchema(ctx, "conn-1")

		assert.Error(t, err)
		assert.Nil(t, result)
		assert.Contains(t, err.Error(), "unmarshal cached schema")
	})
}

func TestSchemaCache_SetSchema(t *testing.T) {
	t.Parallel()

	t.Run("nil cache", func(t *testing.T) {
		t.Parallel()

		var cache *SchemaCache
		err := cache.SetSchema(testCacheContext(), "conn-1", createTestFetcherSchema(), 5*time.Minute)

		assert.ErrorIs(t, err, ErrCacheNotInitialized)
	})

	t.Run("successful set", func(t *testing.T) {
		t.Parallel()

		client, srv, cleanup := setupRedis(t)
		defer cleanup()

		cache, err := NewSchemaCache(client)
		require.NoError(t, err)

		schema := createTestFetcherSchema()
		ctx := testCacheContext()
		err = cache.SetSchema(ctx, "conn-1", schema, 5*time.Minute)

		require.NoError(t, err)

		// Verify data was stored
		assert.True(t, srv.Exists(mustSchemaKey(t, ctx, "conn-1")))

		// Verify JSON structure
		data, err := client.Get(ctx, mustSchemaKey(t, ctx, "conn-1")).Bytes()
		require.NoError(t, err)

		var stored sharedPorts.FetcherSchema
		require.NoError(t, json.Unmarshal(data, &stored))
		assert.Equal(t, "conn-1", stored.ConnectionID)
	})
}

func TestSchemaCache_InvalidateAll(t *testing.T) {
	t.Parallel()

	t.Run("nil cache", func(t *testing.T) {
		t.Parallel()

		var cache *SchemaCache
		err := cache.InvalidateAll(context.Background())

		assert.ErrorIs(t, err, ErrCacheNotInitialized)
	})

	t.Run("successful invalidation", func(t *testing.T) {
		t.Parallel()

		client, srv, cleanup := setupRedis(t)
		defer cleanup()

		cache, err := NewSchemaCache(client)
		require.NoError(t, err)

		// Populate cache
		ctx := testCacheContext()
		err = cache.SetConnections(ctx, createTestConnections(), 5*time.Minute)
		require.NoError(t, err)

		err = cache.SetSchema(ctx, "conn-1", createTestFetcherSchema(), 5*time.Minute)
		require.NoError(t, err)

		err = cache.SetSchema(ctx, "conn-2", createTestFetcherSchema(), 5*time.Minute)
		require.NoError(t, err)

		// Verify data exists
		assert.True(t, srv.Exists(mustConnectionsKey(t, ctx)))
		assert.True(t, srv.Exists(mustSchemaKey(t, ctx, "conn-1")))
		assert.True(t, srv.Exists(mustSchemaKey(t, ctx, "conn-2")))

		// Invalidate
		err = cache.InvalidateAll(ctx)
		require.NoError(t, err)

		// Verify all data is gone
		assert.False(t, srv.Exists(mustConnectionsKey(t, ctx)))
		assert.False(t, srv.Exists(mustSchemaKey(t, ctx, "conn-1")))
		assert.False(t, srv.Exists(mustSchemaKey(t, ctx, "conn-2")))
	})

	t.Run("empty cache", func(t *testing.T) {
		t.Parallel()

		client, _, cleanup := setupRedis(t)
		defer cleanup()

		cache, err := NewSchemaCache(client)
		require.NoError(t, err)

		err = cache.InvalidateAll(testCacheContext())

		assert.NoError(t, err)
	})
}

func TestSchemaCache_InvalidateSchema(t *testing.T) {
	t.Parallel()

	t.Run("nil cache", func(t *testing.T) {
		t.Parallel()

		var cache *SchemaCache
		err := cache.InvalidateSchema(context.Background(), "conn-1")

		assert.ErrorIs(t, err, ErrCacheNotInitialized)
	})

	t.Run("successful invalidation", func(t *testing.T) {
		t.Parallel()

		client, srv, cleanup := setupRedis(t)
		defer cleanup()

		cache, err := NewSchemaCache(client)
		require.NoError(t, err)

		// Populate schema caches
		ctx := testCacheContext()
		err = cache.SetSchema(ctx, "conn-1", createTestFetcherSchema(), 5*time.Minute)
		require.NoError(t, err)

		err = cache.SetSchema(ctx, "conn-2", createTestFetcherSchema(), 5*time.Minute)
		require.NoError(t, err)

		// Invalidate only conn-1
		err = cache.InvalidateSchema(ctx, "conn-1")
		require.NoError(t, err)

		// Verify conn-1 is gone but conn-2 remains
		assert.False(t, srv.Exists(mustSchemaKey(t, ctx, "conn-1")))
		assert.True(t, srv.Exists(mustSchemaKey(t, ctx, "conn-2")))
	})

	t.Run("key does not exist", func(t *testing.T) {
		t.Parallel()

		client, _, cleanup := setupRedis(t)
		defer cleanup()

		cache, err := NewSchemaCache(client)
		require.NoError(t, err)

		// Invalidating non-existent key should not error
		err = cache.InvalidateSchema(testCacheContext(), "nonexistent")

		assert.NoError(t, err)
	})
}

func TestSchemaCache_TenantIsolation(t *testing.T) {
	t.Parallel()

	client, _, cleanup := setupRedis(t)
	defer cleanup()

	cache, err := NewSchemaCache(client)
	require.NoError(t, err)

	tenantA := context.WithValue(context.Background(), auth.TenantIDKey, "tenant-a")
	tenantB := context.WithValue(context.Background(), auth.TenantIDKey, "tenant-b")

	err = cache.SetSchema(tenantA, "conn-1", createTestFetcherSchema(), 5*time.Minute)
	require.NoError(t, err)

	_, err = cache.GetSchema(tenantB, "conn-1")
	assert.ErrorIs(t, err, ErrCacheMiss, "tenant-b must not see tenant-a cache entry")

	result, err := cache.GetSchema(tenantA, "conn-1")
	require.NoError(t, err)
	require.NotNil(t, result)
	assert.Equal(t, "conn-1", result.ConnectionID)
}
