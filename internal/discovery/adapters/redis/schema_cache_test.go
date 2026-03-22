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

func mustSchemaKey(t *testing.T, ctx context.Context, connectionID string) string {
	t.Helper()

	key, err := (&SchemaCache{allowSingleTenantFallback: true}).schemaKey(ctx, connectionID)
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

		cache, err := NewSchemaCache(client, true)

		require.NoError(t, err)
		require.NotNil(t, cache)
	})

	t.Run("with nil client", func(t *testing.T) {
		t.Parallel()

		cache, err := NewSchemaCache(nil, true)

		assert.ErrorIs(t, err, ErrRedisClientRequired)
		assert.Nil(t, cache)
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

		cache, err := NewSchemaCache(client, true)
		require.NoError(t, err)

		result, err := cache.GetSchema(context.Background(), "conn-1")

		assert.ErrorIs(t, err, ErrCacheMiss)
		assert.Nil(t, result)
	})

	t.Run("cache hit", func(t *testing.T) {
		t.Parallel()

		client, _, cleanup := setupRedis(t)
		defer cleanup()

		cache, err := NewSchemaCache(client, true)
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

		cache, err := NewSchemaCache(client, true)
		require.NoError(t, err)

		result, err := cache.GetSchema(ctx, "conn-1")

		assert.Error(t, err)
		assert.Nil(t, result)
		assert.Contains(t, err.Error(), "unmarshal cached schema")
	})

	t.Run("cached null is treated as miss", func(t *testing.T) {
		t.Parallel()

		client, _, cleanup := setupRedis(t)
		defer cleanup()

		ctx := testCacheContext()
		err := client.Set(ctx, mustSchemaKey(t, ctx, "conn-1"), "null", 5*time.Minute).Err()
		require.NoError(t, err)

		cache, err := NewSchemaCache(client, true)
		require.NoError(t, err)

		result, err := cache.GetSchema(ctx, "conn-1")

		assert.ErrorIs(t, err, ErrCacheMiss)
		assert.Nil(t, result)
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

		cache, err := NewSchemaCache(client, true)
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

	t.Run("nil schema rejected", func(t *testing.T) {
		t.Parallel()

		client, _, cleanup := setupRedis(t)
		defer cleanup()

		cache, err := NewSchemaCache(client, true)
		require.NoError(t, err)

		err = cache.SetSchema(testCacheContext(), "conn-1", nil, 5*time.Minute)

		assert.ErrorIs(t, err, ErrSchemaRequired)
	})
}

func TestSchemaCache_TenantIsolation(t *testing.T) {
	t.Parallel()

	client, _, cleanup := setupRedis(t)
	defer cleanup()

	cache, err := NewSchemaCache(client, true)
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

func TestSchemaCache_StrictTenantContextFailsClosed(t *testing.T) {
	t.Parallel()

	client, _, cleanup := setupRedis(t)
	defer cleanup()

	cache, err := NewSchemaCache(client, false)
	require.NoError(t, err)

	err = cache.SetSchema(context.Background(), "conn-1", createTestFetcherSchema(), 5*time.Minute)
	require.Error(t, err)
	assert.ErrorIs(t, err, ErrTenantContextRequired)
}

func TestSchemaCache_UnsafeKeyInputsRejected(t *testing.T) {
	t.Parallel()

	client, _, cleanup := setupRedis(t)
	defer cleanup()

	cache, err := NewSchemaCache(client, true)
	require.NoError(t, err)

	err = cache.SetSchema(context.WithValue(context.Background(), auth.TenantIDKey, "tenant bad"), "conn-1", createTestFetcherSchema(), time.Minute)
	require.Error(t, err)
	assert.ErrorIs(t, err, ErrUnsafeTenantID)

	err = cache.SetSchema(testCacheContext(), "conn bad", createTestFetcherSchema(), time.Minute)
	require.Error(t, err)
	assert.ErrorIs(t, err, ErrUnsafeConnectionID)

	err = cache.SetSchema(testCacheContext(), "", createTestFetcherSchema(), time.Minute)
	require.Error(t, err)
	assert.ErrorIs(t, err, ErrEmptyConnectionID)
}

func TestSchemaCache_InvalidateSchema(t *testing.T) {
	t.Parallel()

	t.Run("nil cache", func(t *testing.T) {
		t.Parallel()

		var cache *SchemaCache
		err := cache.InvalidateSchema(testCacheContext(), "conn-1")

		assert.ErrorIs(t, err, ErrCacheNotInitialized)
	})

	t.Run("successful invalidate", func(t *testing.T) {
		t.Parallel()

		client, srv, cleanup := setupRedis(t)
		defer cleanup()

		cache, err := NewSchemaCache(client, true)
		require.NoError(t, err)

		ctx := testCacheContext()
		err = cache.SetSchema(ctx, "conn-1", createTestFetcherSchema(), 5*time.Minute)
		require.NoError(t, err)
		assert.True(t, srv.Exists(mustSchemaKey(t, ctx, "conn-1")))

		err = cache.InvalidateSchema(ctx, "conn-1")
		require.NoError(t, err)
		assert.False(t, srv.Exists(mustSchemaKey(t, ctx, "conn-1")))
	})
}
