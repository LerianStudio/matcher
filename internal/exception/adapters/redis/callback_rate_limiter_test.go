//go:build unit

package redis

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/alicebob/miniredis/v2"
	goredis "github.com/redis/go-redis/v9"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/LerianStudio/matcher/internal/auth"
	"github.com/LerianStudio/matcher/internal/shared/infrastructure/testutil"
)

// newTestRateLimiter creates a rate limiter for tests, failing on constructor error.
func newTestRateLimiter(t *testing.T, provider *testutil.MockInfrastructureProvider, limit int, window time.Duration) *CallbackRateLimiter {
	t.Helper()

	limiter, err := NewCallbackRateLimiter(provider, limit, window)
	require.NoError(t, err)

	return limiter
}

func TestCallbackRateLimiter_AllowWithinLimit(t *testing.T) {
	t.Parallel()

	conn, cleanup := setupRedis(t)
	t.Cleanup(cleanup)

	provider := &testutil.MockInfrastructureProvider{RedisConn: conn}
	limiter := newTestRateLimiter(t, provider, 5, time.Minute)

	ctx := context.Background()

	for i := 0; i < 5; i++ {
		allowed, err := limiter.Allow(ctx, "test-system")
		require.NoError(t, err)
		require.True(t, allowed, "request %d should be allowed", i+1)
	}
}

func TestCallbackRateLimiter_DenyExceedLimit(t *testing.T) {
	t.Parallel()

	conn, cleanup := setupRedis(t)
	t.Cleanup(cleanup)

	provider := &testutil.MockInfrastructureProvider{RedisConn: conn}
	limiter := newTestRateLimiter(t, provider, 3, time.Minute)

	ctx := context.Background()

	// Use up the limit
	for i := 0; i < 3; i++ {
		allowed, err := limiter.Allow(ctx, "test-system")
		require.NoError(t, err)
		require.True(t, allowed)
	}

	// The 4th request should be denied
	allowed, err := limiter.Allow(ctx, "test-system")
	require.NoError(t, err)
	require.False(t, allowed, "request beyond limit should be denied")
}

func TestCallbackRateLimiter_SeparateKeysIndependent(t *testing.T) {
	t.Parallel()

	conn, cleanup := setupRedis(t)
	t.Cleanup(cleanup)

	provider := &testutil.MockInfrastructureProvider{RedisConn: conn}
	limiter := newTestRateLimiter(t, provider, 2, time.Minute)

	ctx := context.Background()

	// Exhaust limit for key "A"
	for i := 0; i < 2; i++ {
		allowed, err := limiter.Allow(ctx, "system-A")
		require.NoError(t, err)
		require.True(t, allowed)
	}

	allowed, err := limiter.Allow(ctx, "system-A")
	require.NoError(t, err)
	require.False(t, allowed, "system-A should be rate limited")

	// Key "B" should still be allowed
	allowed, err = limiter.Allow(ctx, "system-B")
	require.NoError(t, err)
	require.True(t, allowed, "system-B should not be affected by system-A limit")
}

func TestCallbackRateLimiter_SameExternalKeyDifferentTenants(t *testing.T) {
	t.Parallel()

	conn, cleanup := setupRedis(t)
	t.Cleanup(cleanup)

	provider := &testutil.MockInfrastructureProvider{RedisConn: conn}
	limiter := newTestRateLimiter(t, provider, 1, time.Minute)

	tenantA := context.WithValue(context.Background(), auth.TenantIDKey, "tenant-a")
	tenantB := context.WithValue(context.Background(), auth.TenantIDKey, "tenant-b")

	allowed, err := limiter.Allow(tenantA, "JIRA")
	require.NoError(t, err)
	require.True(t, allowed)

	allowed, err = limiter.Allow(tenantB, "JIRA")
	require.NoError(t, err)
	require.True(t, allowed)

	allowed, err = limiter.Allow(tenantA, "JIRA")
	require.NoError(t, err)
	require.False(t, allowed)

	allowed, err = limiter.Allow(tenantB, "JIRA")
	require.NoError(t, err)
	require.False(t, allowed)
}

func TestCallbackRateLimiter_WindowExpiry(t *testing.T) {
	t.Parallel()

	srv := miniredis.RunT(t)
	client := goredis.NewClient(&goredis.Options{Addr: srv.Addr()})
	conn := testutil.NewRedisClientWithMock(client)

	t.Cleanup(func() {
		if err := client.Close(); err != nil {
			t.Logf("failed to close redis client: %v", err)
		}
		srv.Close()
	})

	provider := &testutil.MockInfrastructureProvider{RedisConn: conn}
	limiter := newTestRateLimiter(t, provider, 1, time.Minute)

	ctx := context.Background()

	allowed, err := limiter.Allow(ctx, "test-expiry")
	require.NoError(t, err)
	require.True(t, allowed)

	// Should be denied now
	allowed, err = limiter.Allow(ctx, "test-expiry")
	require.NoError(t, err)
	require.False(t, allowed)

	// Fast-forward time to expire the window
	srv.FastForward(2 * time.Minute)

	// Should be allowed again after window expires
	allowed, err = limiter.Allow(ctx, "test-expiry")
	require.NoError(t, err)
	require.True(t, allowed)
}

func TestCallbackRateLimiter_NilRateLimiter(t *testing.T) {
	t.Parallel()

	var limiter *CallbackRateLimiter

	allowed, err := limiter.Allow(context.Background(), "test")
	require.ErrorIs(t, err, ErrRateLimiterNotInitialized)
	require.False(t, allowed)
}

func TestCallbackRateLimiter_NilProvider(t *testing.T) {
	t.Parallel()

	limiter := &CallbackRateLimiter{provider: nil}

	allowed, err := limiter.Allow(context.Background(), "test")
	require.ErrorIs(t, err, ErrRateLimiterNotInitialized)
	require.False(t, allowed)
}

func TestCallbackRateLimiter_NilProviderConstructor(t *testing.T) {
	t.Parallel()

	limiter, err := NewCallbackRateLimiter(nil, 10, time.Minute)
	require.ErrorIs(t, err, ErrRateLimiterNilProvider)
	require.Nil(t, limiter)
}

func TestCallbackRateLimiter_RedisConnectionError(t *testing.T) {
	t.Parallel()

	connErr := errors.New("redis connection failed")
	provider := &testutil.MockInfrastructureProvider{RedisErr: connErr}
	limiter := newTestRateLimiter(t, provider, 10, time.Minute)

	allowed, err := limiter.Allow(context.Background(), "test")
	require.Error(t, err)
	require.Contains(t, err.Error(), "rate limiter get redis connection")
	require.False(t, allowed)
}

func TestCallbackRateLimiter_NilRedisClient(t *testing.T) {
	t.Parallel()

	provider := &testutil.MockInfrastructureProvider{
		RedisConn: testutil.NewRedisClientWithMock(nil),
	}
	limiter := newTestRateLimiter(t, provider, 10, time.Minute)

	allowed, err := limiter.Allow(context.Background(), "test")
	require.ErrorIs(t, err, ErrRateLimiterRedisClientNil)
	require.False(t, allowed)
}

func TestCallbackRateLimiter_NilRedisConnection(t *testing.T) {
	t.Parallel()

	provider := &testutil.MockInfrastructureProvider{RedisConn: nil}
	limiter := newTestRateLimiter(t, provider, 10, time.Minute)

	allowed, err := limiter.Allow(context.Background(), "test")
	require.ErrorIs(t, err, ErrRateLimiterRedisClientNil)
	require.False(t, allowed)
}

func TestNewCallbackRateLimiter_DefaultValues(t *testing.T) {
	t.Parallel()

	provider := &testutil.MockInfrastructureProvider{}

	t.Run("negative limit defaults", func(t *testing.T) {
		t.Parallel()

		limiter := newTestRateLimiter(t, provider, -1, time.Minute)
		assert.Equal(t, DefaultCallbackRateLimitPerMin, limiter.limit)
	})

	t.Run("zero limit defaults", func(t *testing.T) {
		t.Parallel()

		limiter := newTestRateLimiter(t, provider, 0, time.Minute)
		assert.Equal(t, DefaultCallbackRateLimitPerMin, limiter.limit)
	})

	t.Run("negative window defaults to 1 minute", func(t *testing.T) {
		t.Parallel()

		limiter := newTestRateLimiter(t, provider, 10, -1*time.Second)
		assert.Equal(t, time.Minute, limiter.window)
	})

	t.Run("zero window defaults to 1 minute", func(t *testing.T) {
		t.Parallel()

		limiter := newTestRateLimiter(t, provider, 10, 0)
		assert.Equal(t, time.Minute, limiter.window)
	})

	t.Run("custom values used", func(t *testing.T) {
		t.Parallel()

		limiter := newTestRateLimiter(t, provider, 100, 5*time.Minute)
		assert.Equal(t, 100, limiter.limit)
		assert.Equal(t, 5*time.Minute, limiter.window)
	})
}

func TestCallbackRateLimiter_Constants(t *testing.T) {
	t.Parallel()

	assert.Equal(t, "matcher:callback:ratelimit:", callbackRateLimitKeyPrefix)
	assert.Equal(t, 60, DefaultCallbackRateLimitPerMin)
}

func TestCallbackRateLimiter_Errors(t *testing.T) {
	t.Parallel()

	assert.EqualError(t, ErrRateLimiterNotInitialized, "callback rate limiter not initialized")
	assert.EqualError(t, ErrRateLimiterRedisClientNil, "callback rate limiter: redis client is nil")
	assert.EqualError(t, ErrRateLimiterNilProvider, "callback rate limiter: infrastructure provider is nil")
}
