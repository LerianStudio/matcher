//go:build unit

package redis

import (
	"context"
	"encoding/json"
	"testing"
	"time"

	"github.com/alicebob/miniredis/v2"
	"github.com/google/uuid"
	goredis "github.com/redis/go-redis/v9"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	shared "github.com/LerianStudio/matcher/internal/shared/domain"
	"github.com/LerianStudio/matcher/internal/shared/infrastructure/testutil"
)

// --- MarkComplete with response data ---

func TestMarkComplete_WithResponseData(t *testing.T) {
	t.Parallel()

	conn, cleanup := setupRedis(t)
	t.Cleanup(cleanup)

	provider := &testutil.MockInfrastructureProvider{RedisConn: conn}
	repo := newTestIdempotencyRepo(t, provider)

	ctx := context.Background()
	key, err := shared.ParseIdempotencyKey("callback-" + uuid.New().String())
	require.NoError(t, err)

	_, err = repo.TryAcquire(ctx, key)
	require.NoError(t, err)

	response := []byte(`{"status":"ok","id":"123"}`)

	err = repo.MarkComplete(ctx, key, response, 200)
	require.NoError(t, err)

	result, err := repo.GetCachedResult(ctx, key)
	require.NoError(t, err)
	assert.Equal(t, shared.IdempotencyStatusComplete, result.Status)
	assert.Equal(t, response, result.Response)
	assert.Equal(t, 200, result.HTTPStatus)
}

func TestMarkComplete_NilRepo(t *testing.T) {
	t.Parallel()

	var repo *IdempotencyRepository

	key, err := shared.ParseIdempotencyKey("callback-" + uuid.New().String())
	require.NoError(t, err)

	err = repo.MarkComplete(context.Background(), key, nil, 200)
	require.ErrorIs(t, err, ErrRepoNotInitialized)
}

func TestMarkComplete_NilProvider(t *testing.T) {
	t.Parallel()

	repo := &IdempotencyRepository{provider: nil}

	key, err := shared.ParseIdempotencyKey("callback-" + uuid.New().String())
	require.NoError(t, err)

	err = repo.MarkComplete(context.Background(), key, nil, 200)
	require.ErrorIs(t, err, ErrRepoNotInitialized)
}

// --- MarkFailed Tests ---

func TestMarkFailed_NilRepo(t *testing.T) {
	t.Parallel()

	var repo *IdempotencyRepository

	key, err := shared.ParseIdempotencyKey("callback-" + uuid.New().String())
	require.NoError(t, err)

	err = repo.MarkFailed(context.Background(), key)
	require.ErrorIs(t, err, ErrRepoNotInitialized)
}

func TestMarkFailed_NilProvider(t *testing.T) {
	t.Parallel()

	repo := &IdempotencyRepository{provider: nil}

	key, err := shared.ParseIdempotencyKey("callback-" + uuid.New().String())
	require.NoError(t, err)

	err = repo.MarkFailed(context.Background(), key)
	require.ErrorIs(t, err, ErrRepoNotInitialized)
}

func TestMarkFailed_ThenGetCachedResult(t *testing.T) {
	t.Parallel()

	conn, cleanup := setupRedis(t)
	t.Cleanup(cleanup)

	provider := &testutil.MockInfrastructureProvider{RedisConn: conn}
	repo := newTestIdempotencyRepo(t, provider)

	ctx := context.Background()
	key, err := shared.ParseIdempotencyKey("callback-" + uuid.New().String())
	require.NoError(t, err)

	_, err = repo.TryAcquire(ctx, key)
	require.NoError(t, err)

	err = repo.MarkFailed(ctx, key)
	require.NoError(t, err)

	result, err := repo.GetCachedResult(ctx, key)
	require.NoError(t, err)
	assert.Equal(t, shared.IdempotencyStatusFailed, result.Status)
}

// --- TryAcquire Tests ---

func TestTryAcquire_NilRepo(t *testing.T) {
	t.Parallel()

	var repo *IdempotencyRepository

	key, err := shared.ParseIdempotencyKey("callback-" + uuid.New().String())
	require.NoError(t, err)

	acquired, err := repo.TryAcquire(context.Background(), key)
	require.ErrorIs(t, err, ErrRepoNotInitialized)
	assert.False(t, acquired)
}

func TestTryAcquire_NilRedisClient(t *testing.T) {
	t.Parallel()

	provider := &testutil.MockInfrastructureProvider{RedisConn: nil}
	repo := newTestIdempotencyRepo(t, provider)

	key, err := shared.ParseIdempotencyKey("callback-" + uuid.New().String())
	require.NoError(t, err)

	acquired, err := repo.TryAcquire(context.Background(), key)
	require.Error(t, err)
	assert.False(t, acquired)
}

func TestTryAcquire_NilRedisClientInConn(t *testing.T) {
	t.Parallel()

	conn := testutil.NewRedisClientWithMock(nil)
	provider := &testutil.MockInfrastructureProvider{RedisConn: conn}
	repo := newTestIdempotencyRepo(t, provider)

	key, err := shared.ParseIdempotencyKey("callback-" + uuid.New().String())
	require.NoError(t, err)

	acquired, err := repo.TryAcquire(context.Background(), key)
	require.ErrorIs(t, err, ErrRedisClientNil)
	assert.False(t, acquired)
}

// --- GetCachedResult Tests ---

func TestGetCachedResult_NilRepo(t *testing.T) {
	t.Parallel()

	var repo *IdempotencyRepository

	key, err := shared.ParseIdempotencyKey("callback-" + uuid.New().String())
	require.NoError(t, err)

	result, err := repo.GetCachedResult(context.Background(), key)
	require.ErrorIs(t, err, ErrRepoNotInitialized)
	assert.Nil(t, result)
}

func TestGetCachedResult_KeyNotFound(t *testing.T) {
	t.Parallel()

	conn, cleanup := setupRedis(t)
	t.Cleanup(cleanup)

	provider := &testutil.MockInfrastructureProvider{RedisConn: conn}
	repo := newTestIdempotencyRepo(t, provider)

	key, err := shared.ParseIdempotencyKey("callback-" + uuid.New().String())
	require.NoError(t, err)

	result, err := repo.GetCachedResult(context.Background(), key)
	require.NoError(t, err)
	assert.Equal(t, shared.IdempotencyStatusUnknown, result.Status)
}

func TestGetCachedResult_LegacyPendingMarker(t *testing.T) {
	t.Parallel()

	srv := miniredis.RunT(t)
	client := goredis.NewClient(&goredis.Options{Addr: srv.Addr()})
	conn := testutil.NewRedisClientWithMock(client)
	t.Cleanup(func() {
		require.NoError(t, client.Close())
		srv.Close()
	})

	provider := &testutil.MockInfrastructureProvider{RedisConn: conn}
	repo := newTestIdempotencyRepo(t, provider)

	key, err := shared.ParseIdempotencyKey("callback-" + uuid.New().String())
	require.NoError(t, err)

	// Manually set legacy pending marker.
	redisKey := repo.storageKey(context.Background(), key)
	srv.Set(redisKey, "pending")

	result, err := repo.GetCachedResult(context.Background(), key)
	require.NoError(t, err)
	assert.Equal(t, shared.IdempotencyStatusPending, result.Status)
}

func TestGetCachedResult_InvalidJSON(t *testing.T) {
	t.Parallel()

	srv := miniredis.RunT(t)
	client := goredis.NewClient(&goredis.Options{Addr: srv.Addr()})
	conn := testutil.NewRedisClientWithMock(client)
	t.Cleanup(func() {
		require.NoError(t, client.Close())
		srv.Close()
	})

	provider := &testutil.MockInfrastructureProvider{RedisConn: conn}
	repo := newTestIdempotencyRepo(t, provider)

	key, err := shared.ParseIdempotencyKey("callback-" + uuid.New().String())
	require.NoError(t, err)

	// Set invalid JSON data.
	redisKey := repo.storageKey(context.Background(), key)
	srv.Set(redisKey, "{invalid")

	result, err := repo.GetCachedResult(context.Background(), key)
	require.Error(t, err)
	assert.Nil(t, result)
}

func TestGetCachedResult_EmptyStatus(t *testing.T) {
	t.Parallel()

	srv := miniredis.RunT(t)
	client := goredis.NewClient(&goredis.Options{Addr: srv.Addr()})
	conn := testutil.NewRedisClientWithMock(client)
	t.Cleanup(func() {
		require.NoError(t, client.Close())
		srv.Close()
	})

	provider := &testutil.MockInfrastructureProvider{RedisConn: conn}
	repo := newTestIdempotencyRepo(t, provider)

	key, err := shared.ParseIdempotencyKey("callback-" + uuid.New().String())
	require.NoError(t, err)

	// Set entry with empty status.
	entry := idempotencyCacheEntry{Status: ""}
	data, err := json.Marshal(entry)
	require.NoError(t, err)

	redisKey := repo.storageKey(context.Background(), key)
	srv.Set(redisKey, string(data))

	result, err := repo.GetCachedResult(context.Background(), key)
	require.ErrorIs(t, err, ErrCacheEntryNoStatus)
	assert.Nil(t, result)
}

func TestGetCachedResult_NilRedisClient(t *testing.T) {
	t.Parallel()

	conn := testutil.NewRedisClientWithMock(nil)
	provider := &testutil.MockInfrastructureProvider{RedisConn: conn}
	repo := newTestIdempotencyRepo(t, provider)

	key, err := shared.ParseIdempotencyKey("callback-" + uuid.New().String())
	require.NoError(t, err)

	result, err := repo.GetCachedResult(context.Background(), key)
	require.ErrorIs(t, err, ErrRedisClientNil)
	assert.Nil(t, result)
}

// --- NewIdempotencyRepositoryWithConfig Tests ---

func TestNewIdempotencyRepositoryWithConfig_CustomValues(t *testing.T) {
	t.Parallel()

	conn, cleanup := setupRedis(t)
	t.Cleanup(cleanup)

	provider := &testutil.MockInfrastructureProvider{RedisConn: conn}
	repo := newTestIdempotencyRepoWithConfig(t, provider, 10*time.Second, 24*time.Hour, "")

	assert.Equal(t, 10*time.Second, repo.failedRetryWindow)
	assert.Equal(t, 24*time.Hour, repo.successTTL)
}

func TestNewIdempotencyRepositoryWithConfig_NegativeValues(t *testing.T) {
	t.Parallel()

	conn, cleanup := setupRedis(t)
	t.Cleanup(cleanup)

	provider := &testutil.MockInfrastructureProvider{RedisConn: conn}
	repo := newTestIdempotencyRepoWithConfig(t, provider, -1, -1, "")

	assert.Equal(t, DefaultFailedRetryWindow, repo.failedRetryWindow)
	assert.Equal(t, DefaultSuccessTTL, repo.successTTL)
}

func TestNewIdempotencyRepositoryWithConfig_ZeroValues(t *testing.T) {
	t.Parallel()

	conn, cleanup := setupRedis(t)
	t.Cleanup(cleanup)

	provider := &testutil.MockInfrastructureProvider{RedisConn: conn}
	repo := newTestIdempotencyRepoWithConfig(t, provider, 0, 0, "")

	assert.Equal(t, DefaultFailedRetryWindow, repo.failedRetryWindow)
	assert.Equal(t, DefaultSuccessTTL, repo.successTTL)
}

// --- MarkComplete/MarkFailed nil Redis client ---

func TestMarkComplete_NilRedisClientInConn(t *testing.T) {
	t.Parallel()

	conn := testutil.NewRedisClientWithMock(nil)
	provider := &testutil.MockInfrastructureProvider{RedisConn: conn}
	repo := newTestIdempotencyRepo(t, provider)

	key, err := shared.ParseIdempotencyKey("callback-" + uuid.New().String())
	require.NoError(t, err)

	err = repo.MarkComplete(context.Background(), key, nil, 200)
	require.ErrorIs(t, err, ErrRedisClientNil)
}

func TestMarkFailed_NilRedisClientInConn(t *testing.T) {
	t.Parallel()

	conn := testutil.NewRedisClientWithMock(nil)
	provider := &testutil.MockInfrastructureProvider{RedisConn: conn}
	repo := newTestIdempotencyRepo(t, provider)

	key, err := shared.ParseIdempotencyKey("callback-" + uuid.New().String())
	require.NoError(t, err)

	err = repo.MarkFailed(context.Background(), key)
	require.ErrorIs(t, err, ErrRedisClientNil)
}
