//go:build unit

package redis

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/alicebob/miniredis/v2"
	"github.com/google/uuid"
	goredis "github.com/redis/go-redis/v9"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	libRedis "github.com/LerianStudio/lib-commons/v4/commons/redis"

	"github.com/LerianStudio/matcher/internal/auth"
	"github.com/LerianStudio/matcher/internal/exception/domain/value_objects"
	"github.com/LerianStudio/matcher/internal/shared/infrastructure/testutil"
)

func setupRedis(t *testing.T) (*libRedis.Client, func()) {
	t.Helper()

	srv := miniredis.RunT(t)
	client := goredis.NewClient(&goredis.Options{Addr: srv.Addr()})
	conn := testutil.NewRedisClientWithMock(client)

	cleanup := func() {
		if err := client.Close(); err != nil {
			t.Logf("failed to close redis client: %v", err)
		}

		srv.Close()
	}

	return conn, cleanup
}

// setupRedisWithClient returns the raw go-redis client alongside the libRedis wrapper,
// for tests that need to seed data directly (e.g., setting invalid JSON).
func setupRedisWithClient(t *testing.T) (*libRedis.Client, goredis.UniversalClient, func()) {
	t.Helper()

	srv := miniredis.RunT(t)
	client := goredis.NewClient(&goredis.Options{Addr: srv.Addr()})
	conn := testutil.NewRedisClientWithMock(client)

	cleanup := func() {
		if err := client.Close(); err != nil {
			t.Logf("failed to close redis client: %v", err)
		}

		srv.Close()
	}

	return conn, client, cleanup
}

// newTestIdempotencyRepo creates an idempotency repository for tests, failing on constructor error.
func newTestIdempotencyRepo(t *testing.T, provider *testutil.MockInfrastructureProvider) *IdempotencyRepository {
	t.Helper()

	repo, err := NewIdempotencyRepository(provider)
	require.NoError(t, err)

	return repo
}

// newTestIdempotencyRepoWithConfig creates an idempotency repository with config for tests.
func newTestIdempotencyRepoWithConfig(
	t *testing.T,
	provider *testutil.MockInfrastructureProvider,
	failedRetryWindow time.Duration,
	successTTL time.Duration,
	hmacSecret string,
) *IdempotencyRepository {
	t.Helper()

	repo, err := NewIdempotencyRepositoryWithConfig(provider, failedRetryWindow, successTTL, hmacSecret)
	require.NoError(t, err)

	return repo
}

func TestNewIdempotencyRepository_NilProvider(t *testing.T) {
	t.Parallel()

	repo, err := NewIdempotencyRepository(nil)
	require.ErrorIs(t, err, ErrRepoNilProvider)
	require.Nil(t, repo)
}

func TestNewIdempotencyRepositoryWithConfig_NilProvider(t *testing.T) {
	t.Parallel()

	repo, err := NewIdempotencyRepositoryWithConfig(nil, DefaultFailedRetryWindow, DefaultSuccessTTL, "")
	require.ErrorIs(t, err, ErrRepoNilProvider)
	require.Nil(t, repo)
}

func TestIdempotencyRepository_CurrentRuntimeConfig_PrefersResolvers(t *testing.T) {
	t.Parallel()

	repo := &IdempotencyRepository{
		failedRetryWindow: time.Minute,
		successTTL:        time.Hour,
		hmacSecret:        "base-secret",
	}
	repo.SetRuntimeConfigResolvers(
		func(context.Context) time.Duration { return 2 * time.Minute },
		func(context.Context) time.Duration { return 2 * time.Hour },
		func(context.Context) string { return "runtime-secret" },
	)

	ctx := context.Background()
	require.Equal(t, 2*time.Minute, repo.currentFailedRetryWindow(ctx))
	require.Equal(t, 2*time.Hour, repo.currentSuccessTTL(ctx))
	require.Equal(t, "runtime-secret", repo.currentHMACSecret(ctx))
}

func TestIdempotencyRepository_RuntimeResolversAffectPublicBehavior(t *testing.T) {
	t.Parallel()

	conn, rawClient, cleanup := setupRedisWithClient(t)
	t.Cleanup(cleanup)

	provider := &testutil.MockInfrastructureProvider{RedisConn: conn}
	repo := newTestIdempotencyRepoWithConfig(t, provider, time.Minute, time.Hour, "base-secret")
	repo.SetRuntimeConfigResolvers(
		func(context.Context) time.Duration { return 2 * time.Minute },
		func(context.Context) time.Duration { return 2 * time.Hour },
		func(context.Context) string { return "runtime-secret" },
	)

	ctx := context.WithValue(context.Background(), auth.TenantIDKey, "tenant-a")
	key, err := value_objects.ParseIdempotencyKey("callback-" + uuid.New().String())
	require.NoError(t, err)

	acquired, err := repo.TryAcquire(ctx, key)
	require.NoError(t, err)
	require.True(t, acquired)

	redisKey := repo.storageKey(ctx, key)
	expectedKey := idempotencyKeyPrefix + "tenant-a:" + key.SignKey("runtime-secret")
	require.Equal(t, expectedKey, redisKey)

	pendingTTL, err := rawClient.TTL(ctx, redisKey).Result()
	require.NoError(t, err)
	require.Greater(t, pendingTTL, time.Minute)

	require.NoError(t, repo.MarkComplete(ctx, key, []byte(`{"ok":true}`), 200))

	completeTTL, err := rawClient.TTL(ctx, redisKey).Result()
	require.NoError(t, err)
	require.Greater(t, completeTTL, time.Hour)
}

func TestIdempotencyRepository_TryAcquireAndMarkComplete(t *testing.T) {
	t.Parallel()

	conn, cleanup := setupRedis(t)
	t.Cleanup(cleanup)

	provider := &testutil.MockInfrastructureProvider{RedisConn: conn}
	repo := newTestIdempotencyRepo(t, provider)

	ctx := context.Background()
	key, err := value_objects.ParseIdempotencyKey("callback-" + uuid.New().String())
	require.NoError(t, err)

	acquired, err := repo.TryAcquire(ctx, key)
	require.NoError(t, err)
	require.True(t, acquired)

	require.NoError(t, repo.MarkComplete(ctx, key, nil, 0))

	acquiredAgain, err := repo.TryAcquire(ctx, key)
	require.NoError(t, err)
	require.False(t, acquiredAgain)
}

func TestIdempotencyRepository_MarkFailedSetsFailedStatus(t *testing.T) {
	t.Parallel()

	conn, cleanup := setupRedis(t)
	t.Cleanup(cleanup)

	provider := &testutil.MockInfrastructureProvider{RedisConn: conn}
	repo := newTestIdempotencyRepo(t, provider)

	ctx := context.Background()
	key, err := value_objects.ParseIdempotencyKey("callback-" + uuid.New().String())
	require.NoError(t, err)

	acquired, err := repo.TryAcquire(ctx, key)
	require.NoError(t, err)
	require.True(t, acquired)

	require.NoError(t, repo.MarkFailed(ctx, key))

	// Verify status via GetCachedResult
	result, err := repo.GetCachedResult(ctx, key)
	require.NoError(t, err)
	require.Equal(
		t,
		value_objects.IdempotencyStatusFailed,
		result.Status,
		"key should have 'failed' status after MarkFailed",
	)

	acquiredAgain, err := repo.TryAcquire(ctx, key)
	require.NoError(t, err)
	require.False(t, acquiredAgain, "should not acquire while in failed cooldown period")
}

func TestIdempotencyRepository_TryReacquireFromFailed_Success(t *testing.T) {
	t.Parallel()

	conn, cleanup := setupRedis(t)
	t.Cleanup(cleanup)

	provider := &testutil.MockInfrastructureProvider{RedisConn: conn}
	repo := newTestIdempotencyRepo(t, provider)

	ctx := context.Background()
	key, err := value_objects.ParseIdempotencyKey("callback-" + uuid.New().String())
	require.NoError(t, err)

	acquired, err := repo.TryAcquire(ctx, key)
	require.NoError(t, err)
	require.True(t, acquired)

	require.NoError(t, repo.MarkFailed(ctx, key))

	reacquired, err := repo.TryReacquireFromFailed(ctx, key)
	require.NoError(t, err)
	require.True(t, reacquired)

	result, err := repo.GetCachedResult(ctx, key)
	require.NoError(t, err)
	require.Equal(t, value_objects.IdempotencyStatusPending, result.Status)
}

func TestIdempotencyRepository_TryReacquireFromFailed_NotFailed(t *testing.T) {
	t.Parallel()

	conn, cleanup := setupRedis(t)
	t.Cleanup(cleanup)

	provider := &testutil.MockInfrastructureProvider{RedisConn: conn}
	repo := newTestIdempotencyRepo(t, provider)

	ctx := context.Background()
	key, err := value_objects.ParseIdempotencyKey("callback-" + uuid.New().String())
	require.NoError(t, err)

	reacquired, err := repo.TryReacquireFromFailed(ctx, key)
	require.NoError(t, err)
	require.False(t, reacquired)
}

func TestIdempotencyRepository_TenantScopedKeys(t *testing.T) {
	t.Parallel()

	conn, cleanup := setupRedis(t)
	t.Cleanup(cleanup)

	provider := &testutil.MockInfrastructureProvider{RedisConn: conn}
	repo := newTestIdempotencyRepo(t, provider)

	key, err := value_objects.ParseIdempotencyKey("callback-" + uuid.New().String())
	require.NoError(t, err)

	ctxTenantA := context.WithValue(context.Background(), auth.TenantIDKey, "tenant-a")
	ctxTenantB := context.WithValue(context.Background(), auth.TenantIDKey, "tenant-b")

	acquired, err := repo.TryAcquire(ctxTenantA, key)
	require.NoError(t, err)
	require.True(t, acquired)

	acquired, err = repo.TryAcquire(ctxTenantB, key)
	require.NoError(t, err)
	require.True(t, acquired)
}

func TestIdempotencyRepository_NilProvider(t *testing.T) {
	t.Parallel()

	var repo *IdempotencyRepository

	key, err := value_objects.ParseIdempotencyKey("callback-" + uuid.New().String())
	require.NoError(t, err)

	_, err = repo.TryAcquire(context.Background(), key)
	require.ErrorIs(t, err, ErrRepoNotInitialized)

	require.ErrorIs(t, repo.MarkComplete(context.Background(), key, nil, 0), ErrRepoNotInitialized)
	require.ErrorIs(t, repo.MarkFailed(context.Background(), key), ErrRepoNotInitialized)
}

func TestDefaultFailedRetryWindow(t *testing.T) {
	t.Parallel()

	assert.Equal(t, 5*time.Minute, DefaultFailedRetryWindow,
		"DefaultFailedRetryWindow should be 5 minutes")
}

func TestNewIdempotencyRepository_DefaultRetryWindow(t *testing.T) {
	t.Parallel()

	provider := &testutil.MockInfrastructureProvider{}
	repo := newTestIdempotencyRepo(t, provider)

	assert.Equal(t, DefaultFailedRetryWindow, repo.failedRetryWindow)
	assert.Equal(t, DefaultSuccessTTL, repo.successTTL)
}

func TestNewIdempotencyRepositoryWithConfig_CustomRetryWindow(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name     string
		window   time.Duration
		expected time.Duration
	}{
		{
			name:     "positive window uses configured value",
			window:   10 * time.Minute,
			expected: 10 * time.Minute,
		},
		{
			name:     "zero window uses default",
			window:   0,
			expected: DefaultFailedRetryWindow,
		},
		{
			name:     "negative window uses default",
			window:   -5 * time.Minute,
			expected: DefaultFailedRetryWindow,
		},
		{
			name:     "short window uses configured value",
			window:   30 * time.Second,
			expected: 30 * time.Second,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			provider := &testutil.MockInfrastructureProvider{}
			repo := newTestIdempotencyRepoWithConfig(t, provider, tt.window, DefaultSuccessTTL, "")

			assert.Equal(t, tt.expected, repo.failedRetryWindow)
		})
	}
}

func TestNewIdempotencyRepositoryWithConfig_CustomSuccessTTL(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name     string
		ttl      time.Duration
		expected time.Duration
	}{
		{
			name:     "positive TTL uses configured value",
			ttl:      48 * time.Hour,
			expected: 48 * time.Hour,
		},
		{
			name:     "zero TTL uses default",
			ttl:      0,
			expected: DefaultSuccessTTL,
		},
		{
			name:     "negative TTL uses default",
			ttl:      -1 * time.Hour,
			expected: DefaultSuccessTTL,
		},
		{
			name:     "default 7 days",
			ttl:      7 * 24 * time.Hour,
			expected: 7 * 24 * time.Hour,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			provider := &testutil.MockInfrastructureProvider{}
			repo := newTestIdempotencyRepoWithConfig(t, provider, DefaultFailedRetryWindow, tt.ttl, "")

			assert.Equal(t, tt.expected, repo.successTTL)
		})
	}
}

func TestIdempotencyRepositoryWithConfig_UsesCustomRetryWindow(t *testing.T) {
	t.Parallel()

	conn, cleanup := setupRedis(t)
	t.Cleanup(cleanup)

	customWindow := 10 * time.Minute
	provider := &testutil.MockInfrastructureProvider{RedisConn: conn}
	repo := newTestIdempotencyRepoWithConfig(t, provider, customWindow, DefaultSuccessTTL, "")

	ctx := context.Background()
	key, err := value_objects.ParseIdempotencyKey("callback-" + uuid.New().String())
	require.NoError(t, err)

	acquired, err := repo.TryAcquire(ctx, key)
	require.NoError(t, err)
	require.True(t, acquired)

	require.NoError(t, repo.MarkFailed(ctx, key))

	// Verify status via GetCachedResult
	result, err := repo.GetCachedResult(ctx, key)
	require.NoError(t, err)
	require.Equal(t, value_objects.IdempotencyStatusFailed, result.Status)
}

func TestIdempotencyRepository_GetCachedResult_NilRepo(t *testing.T) {
	t.Parallel()

	var repo *IdempotencyRepository

	key, err := value_objects.ParseIdempotencyKey("callback-" + uuid.New().String())
	require.NoError(t, err)

	result, err := repo.GetCachedResult(context.Background(), key)
	require.ErrorIs(t, err, ErrRepoNotInitialized)
	require.Nil(t, result)
}

func TestIdempotencyRepository_GetCachedResult_NilProvider(t *testing.T) {
	t.Parallel()

	repo := &IdempotencyRepository{provider: nil}

	key, err := value_objects.ParseIdempotencyKey("callback-" + uuid.New().String())
	require.NoError(t, err)

	result, err := repo.GetCachedResult(context.Background(), key)
	require.ErrorIs(t, err, ErrRepoNotInitialized)
	require.Nil(t, result)
}

func TestIdempotencyRepository_GetCachedResult_KeyNotFound(t *testing.T) {
	t.Parallel()

	conn, cleanup := setupRedis(t)
	t.Cleanup(cleanup)

	provider := &testutil.MockInfrastructureProvider{RedisConn: conn}
	repo := newTestIdempotencyRepo(t, provider)

	ctx := context.Background()
	key, err := value_objects.ParseIdempotencyKey("callback-nonexistent-" + uuid.New().String())
	require.NoError(t, err)

	result, err := repo.GetCachedResult(ctx, key)
	require.NoError(t, err)
	require.NotNil(t, result)
	require.Equal(t, value_objects.IdempotencyStatusUnknown, result.Status)
}

func TestIdempotencyRepository_GetCachedResult_PendingStatus(t *testing.T) {
	t.Parallel()

	conn, cleanup := setupRedis(t)
	t.Cleanup(cleanup)

	provider := &testutil.MockInfrastructureProvider{RedisConn: conn}
	repo := newTestIdempotencyRepo(t, provider)

	ctx := context.Background()
	key, err := value_objects.ParseIdempotencyKey("callback-" + uuid.New().String())
	require.NoError(t, err)

	// Acquire sets status to "pending"
	acquired, err := repo.TryAcquire(ctx, key)
	require.NoError(t, err)
	require.True(t, acquired)

	// GetCachedResult should return pending status
	result, err := repo.GetCachedResult(ctx, key)
	require.NoError(t, err)
	require.NotNil(t, result)
	require.Equal(t, value_objects.IdempotencyStatusPending, result.Status)
}

func TestIdempotencyRepository_GetCachedResult_CompleteWithResponse(t *testing.T) {
	t.Parallel()

	conn, cleanup := setupRedis(t)
	t.Cleanup(cleanup)

	provider := &testutil.MockInfrastructureProvider{RedisConn: conn}
	repo := newTestIdempotencyRepo(t, provider)

	ctx := context.Background()
	key, err := value_objects.ParseIdempotencyKey("callback-" + uuid.New().String())
	require.NoError(t, err)

	// Acquire and mark complete with response
	acquired, err := repo.TryAcquire(ctx, key)
	require.NoError(t, err)
	require.True(t, acquired)

	responseBody := []byte(`{"id":"123","status":"success"}`)
	httpStatus := 200

	err = repo.MarkComplete(ctx, key, responseBody, httpStatus)
	require.NoError(t, err)

	// GetCachedResult should return complete status with response
	result, err := repo.GetCachedResult(ctx, key)
	require.NoError(t, err)
	require.NotNil(t, result)
	require.Equal(t, value_objects.IdempotencyStatusComplete, result.Status)
	require.Equal(t, responseBody, result.Response)
	require.Equal(t, httpStatus, result.HTTPStatus)
}

func TestIdempotencyRepository_GetCachedResult_InvalidJSON(t *testing.T) {
	t.Parallel()

	conn, rawClient, cleanup := setupRedisWithClient(t)
	t.Cleanup(cleanup)

	provider := &testutil.MockInfrastructureProvider{RedisConn: conn}
	repo := newTestIdempotencyRepo(t, provider)

	ctx := context.Background()
	key, err := value_objects.ParseIdempotencyKey("callback-" + uuid.New().String())
	require.NoError(t, err)

	// Manually set invalid JSON in Redis
	redisKey := repo.storageKey(ctx, key)
	err = rawClient.Set(ctx, redisKey, "invalid-json{", DefaultSuccessTTL).Err()
	require.NoError(t, err)

	// GetCachedResult should return unmarshal error
	result, err := repo.GetCachedResult(ctx, key)
	require.Error(t, err)
	require.Contains(t, err.Error(), "unmarshal cache entry")
	require.Nil(t, result)
}

func TestIdempotencyRepository_TryAcquire_RedisConnectionError(t *testing.T) {
	t.Parallel()

	connErr := errors.New("redis connection failed")
	provider := &testutil.MockInfrastructureProvider{RedisErr: connErr}
	repo := newTestIdempotencyRepo(t, provider)

	ctx := context.Background()
	key, err := value_objects.ParseIdempotencyKey("callback-" + uuid.New().String())
	require.NoError(t, err)

	acquired, err := repo.TryAcquire(ctx, key)
	require.Error(t, err)
	require.Contains(t, err.Error(), "get redis connection")
	require.False(t, acquired)
}

func TestIdempotencyRepository_MarkComplete_RedisConnectionError(t *testing.T) {
	t.Parallel()

	connErr := errors.New("redis connection failed")
	provider := &testutil.MockInfrastructureProvider{RedisErr: connErr}
	repo := newTestIdempotencyRepo(t, provider)

	ctx := context.Background()
	key, err := value_objects.ParseIdempotencyKey("callback-" + uuid.New().String())
	require.NoError(t, err)

	err = repo.MarkComplete(ctx, key, nil, 200)
	require.Error(t, err)
	require.Contains(t, err.Error(), "get redis connection")
}

func TestIdempotencyRepository_MarkFailed_RedisConnectionError(t *testing.T) {
	t.Parallel()

	connErr := errors.New("redis connection failed")
	provider := &testutil.MockInfrastructureProvider{RedisErr: connErr}
	repo := newTestIdempotencyRepo(t, provider)

	ctx := context.Background()
	key, err := value_objects.ParseIdempotencyKey("callback-" + uuid.New().String())
	require.NoError(t, err)

	err = repo.MarkFailed(ctx, key)
	require.Error(t, err)
	require.Contains(t, err.Error(), "get redis connection")
}

func TestIdempotencyRepository_GetCachedResult_RedisConnectionError(t *testing.T) {
	t.Parallel()

	connErr := errors.New("redis connection failed")
	provider := &testutil.MockInfrastructureProvider{RedisErr: connErr}
	repo := newTestIdempotencyRepo(t, provider)

	ctx := context.Background()
	key, err := value_objects.ParseIdempotencyKey("callback-" + uuid.New().String())
	require.NoError(t, err)

	result, err := repo.GetCachedResult(ctx, key)
	require.Error(t, err)
	require.Contains(t, err.Error(), "get redis connection")
	require.Nil(t, result)
}

func TestIdempotencyRepository_TryAcquire_NilRedisClient(t *testing.T) {
	t.Parallel()

	provider := &testutil.MockInfrastructureProvider{
		RedisConn: testutil.NewRedisClientWithMock(nil),
	}
	repo := newTestIdempotencyRepo(t, provider)

	ctx := context.Background()
	key, err := value_objects.ParseIdempotencyKey("callback-" + uuid.New().String())
	require.NoError(t, err)

	acquired, err := repo.TryAcquire(ctx, key)
	require.ErrorIs(t, err, ErrRedisClientNil)
	require.False(t, acquired)
}

func TestIdempotencyRepository_MarkComplete_NilRedisClient(t *testing.T) {
	t.Parallel()

	provider := &testutil.MockInfrastructureProvider{
		RedisConn: testutil.NewRedisClientWithMock(nil),
	}
	repo := newTestIdempotencyRepo(t, provider)

	ctx := context.Background()
	key, err := value_objects.ParseIdempotencyKey("callback-" + uuid.New().String())
	require.NoError(t, err)

	err = repo.MarkComplete(ctx, key, nil, 200)
	require.ErrorIs(t, err, ErrRedisClientNil)
}

func TestIdempotencyRepository_MarkFailed_NilRedisClient(t *testing.T) {
	t.Parallel()

	provider := &testutil.MockInfrastructureProvider{
		RedisConn: testutil.NewRedisClientWithMock(nil),
	}
	repo := newTestIdempotencyRepo(t, provider)

	ctx := context.Background()
	key, err := value_objects.ParseIdempotencyKey("callback-" + uuid.New().String())
	require.NoError(t, err)

	err = repo.MarkFailed(ctx, key)
	require.ErrorIs(t, err, ErrRedisClientNil)
}

func TestIdempotencyRepository_GetCachedResult_NilRedisClient(t *testing.T) {
	t.Parallel()

	provider := &testutil.MockInfrastructureProvider{
		RedisConn: testutil.NewRedisClientWithMock(nil),
	}
	repo := newTestIdempotencyRepo(t, provider)

	ctx := context.Background()
	key, err := value_objects.ParseIdempotencyKey("callback-" + uuid.New().String())
	require.NoError(t, err)

	result, err := repo.GetCachedResult(ctx, key)
	require.ErrorIs(t, err, ErrRedisClientNil)
	require.Nil(t, result)
}

func TestIdempotencyRepository_TryAcquire_NilRedisConnection(t *testing.T) {
	t.Parallel()

	provider := &testutil.MockInfrastructureProvider{RedisConn: nil}
	repo := newTestIdempotencyRepo(t, provider)

	ctx := context.Background()
	key, err := value_objects.ParseIdempotencyKey("callback-" + uuid.New().String())
	require.NoError(t, err)

	acquired, err := repo.TryAcquire(ctx, key)
	require.ErrorIs(t, err, ErrRedisClientNil)
	require.False(t, acquired)
}

func TestIdempotencyRepository_MarkComplete_NilRedisConnection(t *testing.T) {
	t.Parallel()

	provider := &testutil.MockInfrastructureProvider{RedisConn: nil}
	repo := newTestIdempotencyRepo(t, provider)

	ctx := context.Background()
	key, err := value_objects.ParseIdempotencyKey("callback-" + uuid.New().String())
	require.NoError(t, err)

	err = repo.MarkComplete(ctx, key, nil, 200)
	require.ErrorIs(t, err, ErrRedisClientNil)
}

func TestIdempotencyRepository_MarkFailed_NilRedisConnection(t *testing.T) {
	t.Parallel()

	provider := &testutil.MockInfrastructureProvider{RedisConn: nil}
	repo := newTestIdempotencyRepo(t, provider)

	ctx := context.Background()
	key, err := value_objects.ParseIdempotencyKey("callback-" + uuid.New().String())
	require.NoError(t, err)

	err = repo.MarkFailed(ctx, key)
	require.ErrorIs(t, err, ErrRedisClientNil)
}

func TestIdempotencyRepository_GetCachedResult_NilRedisConnection(t *testing.T) {
	t.Parallel()

	provider := &testutil.MockInfrastructureProvider{RedisConn: nil}
	repo := newTestIdempotencyRepo(t, provider)

	ctx := context.Background()
	key, err := value_objects.ParseIdempotencyKey("callback-" + uuid.New().String())
	require.NoError(t, err)

	result, err := repo.GetCachedResult(ctx, key)
	require.ErrorIs(t, err, ErrRedisClientNil)
	require.Nil(t, result)
}

func TestIdempotencyRepository_MarkComplete_WithEmptyResponse(t *testing.T) {
	t.Parallel()

	conn, cleanup := setupRedis(t)
	t.Cleanup(cleanup)

	provider := &testutil.MockInfrastructureProvider{RedisConn: conn}
	repo := newTestIdempotencyRepo(t, provider)

	ctx := context.Background()
	key, err := value_objects.ParseIdempotencyKey("callback-" + uuid.New().String())
	require.NoError(t, err)

	acquired, err := repo.TryAcquire(ctx, key)
	require.NoError(t, err)
	require.True(t, acquired)

	err = repo.MarkComplete(ctx, key, nil, 204)
	require.NoError(t, err)

	result, err := repo.GetCachedResult(ctx, key)
	require.NoError(t, err)
	require.Equal(t, value_objects.IdempotencyStatusComplete, result.Status)
	require.Nil(t, result.Response)
	require.Equal(t, 204, result.HTTPStatus)
}

func TestIdempotencyRepository_Constants(t *testing.T) {
	t.Parallel()

	assert.Equal(t, "matcher:idempotency:", idempotencyKeyPrefix, "key prefix should match")
	assert.Equal(t, 7*24*time.Hour, DefaultSuccessTTL, "default success TTL should be 7 days")
	assert.Equal(t, "pending", statusPending, "pending status should match")
}

func TestIdempotencyRepository_Errors(t *testing.T) {
	t.Parallel()

	assert.EqualError(t, ErrRepoNotInitialized, "idempotency repository not initialized")
	assert.EqualError(t, ErrRedisClientNil, "redis client is nil")
	assert.EqualError(t, ErrCacheEntryNoStatus, "cache entry missing status field")
	assert.EqualError(t, ErrRepoNilProvider, "idempotency repository: infrastructure provider is nil")
}

func TestIdempotencyRepository_GetCachedResult_MissingStatusField(t *testing.T) {
	t.Parallel()

	conn, rawClient, cleanup := setupRedisWithClient(t)
	t.Cleanup(cleanup)

	provider := &testutil.MockInfrastructureProvider{RedisConn: conn}
	repo := newTestIdempotencyRepo(t, provider)

	ctx := context.Background()
	key, err := value_objects.ParseIdempotencyKey("callback-" + uuid.New().String())
	require.NoError(t, err)

	redisKey := repo.storageKey(ctx, key)
	err = rawClient.Set(ctx, redisKey, `{"response":"data"}`, DefaultSuccessTTL).Err()
	require.NoError(t, err)

	result, err := repo.GetCachedResult(ctx, key)
	require.Error(t, err)
	require.ErrorIs(t, err, ErrCacheEntryNoStatus)
	require.Nil(t, result)
}

func TestIdempotencyRepository_GetCachedResult_EmptyStatusField(t *testing.T) {
	t.Parallel()

	conn, rawClient, cleanup := setupRedisWithClient(t)
	t.Cleanup(cleanup)

	provider := &testutil.MockInfrastructureProvider{RedisConn: conn}
	repo := newTestIdempotencyRepo(t, provider)

	ctx := context.Background()
	key, err := value_objects.ParseIdempotencyKey("callback-" + uuid.New().String())
	require.NoError(t, err)

	redisKey := repo.storageKey(ctx, key)
	err = rawClient.Set(ctx, redisKey, `{"status":"","response":"data"}`, DefaultSuccessTTL).Err()
	require.NoError(t, err)

	result, err := repo.GetCachedResult(ctx, key)
	require.Error(t, err)
	require.ErrorIs(t, err, ErrCacheEntryNoStatus)
	require.Nil(t, result)
}
