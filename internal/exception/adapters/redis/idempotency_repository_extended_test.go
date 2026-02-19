//go:build unit

package redis

import (
	"context"
	"testing"
	"time"

	"github.com/alicebob/miniredis/v2"
	"github.com/google/uuid"
	goredis "github.com/redis/go-redis/v9"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	libRedis "github.com/LerianStudio/lib-uncommons/v2/uncommons/redis"

	"github.com/LerianStudio/matcher/internal/exception/domain/value_objects"
	"github.com/LerianStudio/matcher/internal/shared/infrastructure/testutil"
)

func setupRedisExtended(t *testing.T) (*libRedis.Client, *miniredis.Miniredis, func()) {
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

	return conn, srv, cleanup
}

func TestIdempotencyRepository_MarkComplete_WithLargeResponse(t *testing.T) {
	t.Parallel()

	conn, _, cleanup := setupRedisExtended(t)
	t.Cleanup(cleanup)

	provider := &testutil.MockInfrastructureProvider{RedisConn: conn}
	repo := newTestIdempotencyRepo(t, provider)

	ctx := context.Background()
	key, err := value_objects.ParseIdempotencyKey("callback-" + uuid.New().String())
	require.NoError(t, err)

	acquired, err := repo.TryAcquire(ctx, key)
	require.NoError(t, err)
	require.True(t, acquired)

	// Large response body
	largeResponse := make([]byte, 10*1024)
	for i := range largeResponse {
		largeResponse[i] = byte('A' + i%26)
	}

	err = repo.MarkComplete(ctx, key, largeResponse, 200)
	require.NoError(t, err)

	result, err := repo.GetCachedResult(ctx, key)
	require.NoError(t, err)
	require.Equal(t, value_objects.IdempotencyStatusComplete, result.Status)
	require.Equal(t, largeResponse, result.Response)
	require.Equal(t, 200, result.HTTPStatus)
}

func TestIdempotencyRepository_MarkComplete_WithDifferentHTTPStatus(t *testing.T) {
	t.Parallel()

	conn, _, cleanup := setupRedisExtended(t)
	t.Cleanup(cleanup)

	provider := &testutil.MockInfrastructureProvider{RedisConn: conn}
	repo := newTestIdempotencyRepo(t, provider)

	ctx := context.Background()

	tests := []struct {
		name       string
		httpStatus int
	}{
		{"success 200", 200},
		{"created 201", 201},
		{"no content 204", 204},
		{"bad request 400", 400},
		{"not found 404", 404},
		{"internal error 500", 500},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			key, err := value_objects.ParseIdempotencyKey("callback-" + uuid.New().String())
			require.NoError(t, err)

			acquired, err := repo.TryAcquire(ctx, key)
			require.NoError(t, err)
			require.True(t, acquired)

			err = repo.MarkComplete(ctx, key, []byte(`{"status":"ok"}`), tt.httpStatus)
			require.NoError(t, err)

			result, err := repo.GetCachedResult(ctx, key)
			require.NoError(t, err)
			require.Equal(t, tt.httpStatus, result.HTTPStatus)
		})
	}
}

func TestIdempotencyRepository_TryAcquire_RepeatedAttempts(t *testing.T) {
	t.Parallel()

	conn, _, cleanup := setupRedisExtended(t)
	t.Cleanup(cleanup)

	provider := &testutil.MockInfrastructureProvider{RedisConn: conn}
	repo := newTestIdempotencyRepo(t, provider)

	ctx := context.Background()
	key, err := value_objects.ParseIdempotencyKey("callback-" + uuid.New().String())
	require.NoError(t, err)

	// First acquire should succeed
	acquired1, err := repo.TryAcquire(ctx, key)
	require.NoError(t, err)
	require.True(t, acquired1)

	// Second acquire should fail (key already exists)
	acquired2, err := repo.TryAcquire(ctx, key)
	require.NoError(t, err)
	require.False(t, acquired2)

	// Third acquire should also fail
	acquired3, err := repo.TryAcquire(ctx, key)
	require.NoError(t, err)
	require.False(t, acquired3)
}

func TestIdempotencyRepository_FullLifecycle_Success(t *testing.T) {
	t.Parallel()

	conn, _, cleanup := setupRedisExtended(t)
	t.Cleanup(cleanup)

	provider := &testutil.MockInfrastructureProvider{RedisConn: conn}
	repo := newTestIdempotencyRepo(t, provider)

	ctx := context.Background()
	key, err := value_objects.ParseIdempotencyKey("callback-" + uuid.New().String())
	require.NoError(t, err)

	// Step 1: Check - should be unknown
	result, err := repo.GetCachedResult(ctx, key)
	require.NoError(t, err)
	require.Equal(t, value_objects.IdempotencyStatusUnknown, result.Status)

	// Step 2: Acquire - should succeed
	acquired, err := repo.TryAcquire(ctx, key)
	require.NoError(t, err)
	require.True(t, acquired)

	// Step 3: Check again - should be pending
	result, err = repo.GetCachedResult(ctx, key)
	require.NoError(t, err)
	require.Equal(t, value_objects.IdempotencyStatusPending, result.Status)

	// Step 4: Mark complete
	response := []byte(`{"id":"test-123","status":"completed"}`)
	err = repo.MarkComplete(ctx, key, response, 201)
	require.NoError(t, err)

	// Step 5: Check - should be complete with response
	result, err = repo.GetCachedResult(ctx, key)
	require.NoError(t, err)
	require.Equal(t, value_objects.IdempotencyStatusComplete, result.Status)
	require.Equal(t, response, result.Response)
	require.Equal(t, 201, result.HTTPStatus)
}

func TestIdempotencyRepository_FullLifecycle_Failed(t *testing.T) {
	t.Parallel()

	conn, _, cleanup := setupRedisExtended(t)
	t.Cleanup(cleanup)

	provider := &testutil.MockInfrastructureProvider{RedisConn: conn}
	repo := newTestIdempotencyRepo(t, provider)

	ctx := context.Background()
	key, err := value_objects.ParseIdempotencyKey("callback-" + uuid.New().String())
	require.NoError(t, err)

	// Acquire
	acquired, err := repo.TryAcquire(ctx, key)
	require.NoError(t, err)
	require.True(t, acquired)

	// Mark failed
	err = repo.MarkFailed(ctx, key)
	require.NoError(t, err)

	// Check - should be failed
	result, err := repo.GetCachedResult(ctx, key)
	require.NoError(t, err)
	require.Equal(t, value_objects.IdempotencyStatusFailed, result.Status)
	require.Nil(t, result.Response)
	require.Equal(t, 0, result.HTTPStatus)
}

func TestNewIdempotencyRepositoryWithConfig_BothDefaults(t *testing.T) {
	t.Parallel()

	provider := &testutil.MockInfrastructureProvider{}
	repo := newTestIdempotencyRepoWithConfig(t, provider, -1, -1, "")

	assert.Equal(t, DefaultFailedRetryWindow, repo.failedRetryWindow)
	assert.Equal(t, DefaultSuccessTTL, repo.successTTL)
}

func TestNewIdempotencyRepositoryWithConfig_BothCustom(t *testing.T) {
	t.Parallel()

	provider := &testutil.MockInfrastructureProvider{}
	customWindow := 15 * time.Minute
	customTTL := 72 * time.Hour
	repo := newTestIdempotencyRepoWithConfig(t, provider, customWindow, customTTL, "")

	assert.Equal(t, customWindow, repo.failedRetryWindow)
	assert.Equal(t, customTTL, repo.successTTL)
}

func TestIdempotencyRepository_MarkComplete_OverwritesPending(t *testing.T) {
	t.Parallel()

	conn, _, cleanup := setupRedisExtended(t)
	t.Cleanup(cleanup)

	provider := &testutil.MockInfrastructureProvider{RedisConn: conn}
	repo := newTestIdempotencyRepo(t, provider)

	ctx := context.Background()
	key, err := value_objects.ParseIdempotencyKey("callback-" + uuid.New().String())
	require.NoError(t, err)

	// Acquire sets pending
	_, err = repo.TryAcquire(ctx, key)
	require.NoError(t, err)

	// MarkComplete overwrites pending
	err = repo.MarkComplete(ctx, key, []byte(`{"done":true}`), 200)
	require.NoError(t, err)

	// Should be complete now
	result, err := repo.GetCachedResult(ctx, key)
	require.NoError(t, err)
	require.Equal(t, value_objects.IdempotencyStatusComplete, result.Status)
}

func TestIdempotencyRepository_MarkFailed_OverwritesPending(t *testing.T) {
	t.Parallel()

	conn, _, cleanup := setupRedisExtended(t)
	t.Cleanup(cleanup)

	provider := &testutil.MockInfrastructureProvider{RedisConn: conn}
	repo := newTestIdempotencyRepo(t, provider)

	ctx := context.Background()
	key, err := value_objects.ParseIdempotencyKey("callback-" + uuid.New().String())
	require.NoError(t, err)

	// Acquire sets pending
	_, err = repo.TryAcquire(ctx, key)
	require.NoError(t, err)

	// MarkFailed overwrites pending
	err = repo.MarkFailed(ctx, key)
	require.NoError(t, err)

	// Should be failed now
	result, err := repo.GetCachedResult(ctx, key)
	require.NoError(t, err)
	require.Equal(t, value_objects.IdempotencyStatusFailed, result.Status)
}

func TestIdempotencyRepository_InterfaceCompliance(t *testing.T) {
	t.Parallel()

	provider := &testutil.MockInfrastructureProvider{}
	repo := newTestIdempotencyRepo(t, provider)

	require.NotNil(t, repo)
}
