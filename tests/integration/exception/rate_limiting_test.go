//go:build integration

package exception

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/LerianStudio/matcher/internal/auth"
	exceptionRedis "github.com/LerianStudio/matcher/internal/exception/adapters/redis"
	infraTestutil "github.com/LerianStudio/matcher/internal/shared/infrastructure/testutil"
	"github.com/LerianStudio/matcher/internal/shared/ports"
	"github.com/LerianStudio/matcher/tests/integration"
)

// wireRateLimiter creates a CallbackRateLimiter backed by the real testcontainer
// Redis instance. The returned provider is also available for callers that need
// to construct additional adapters.
func wireRateLimiter(
	t *testing.T,
	h *integration.TestHarness,
	limit int,
	window time.Duration,
) (*exceptionRedis.CallbackRateLimiter, ports.InfrastructureProvider) {
	t.Helper()

	redisConn := mustRedisConn(t, h.RedisAddr)
	provider := infraTestutil.NewSingleTenantInfrastructureProvider(h.Connection, redisConn)

	limiter, err := exceptionRedis.NewCallbackRateLimiter(provider, limit, window)
	require.NoError(t, err)

	return limiter, provider
}

// rateLimitCtx returns a context with both tenant-ID and tenant-slug set, which
// is required by the scoped rate-limit key derivation.
func rateLimitCtx(t *testing.T, h *integration.TestHarness) context.Context {
	t.Helper()

	ctx := testCtx(t, h) // sets TenantSlugKey
	ctx = context.WithValue(ctx, auth.TenantIDKey, h.Seed.TenantID.String())

	return ctx
}

// --- Tests ----------------------------------------------------------------

//nolint:paralleltest // integration tests share testcontainer infrastructure
func TestRateLimiter_AllowsWithinLimit(t *testing.T) {
	integration.RunWithDatabase(t, func(t *testing.T, h *integration.TestHarness) { //nolint:thelper
		const rateLimit = 5

		limiter, _ := wireRateLimiter(t, h, rateLimit, time.Minute)
		ctx := rateLimitCtx(t, h)

		for i := range rateLimit {
			allowed, err := limiter.Allow(ctx, "within-limit-key")
			require.NoError(t, err)
			require.True(
				t,
				allowed,
				"request %d of %d should be allowed",
				i+1,
				rateLimit,
			)
		}
	})
}

//nolint:paralleltest // integration tests share testcontainer infrastructure
func TestRateLimiter_BlocksAfterLimit(t *testing.T) {
	integration.RunWithDatabase(t, func(t *testing.T, h *integration.TestHarness) { //nolint:thelper
		const rateLimit = 3

		limiter, _ := wireRateLimiter(t, h, rateLimit, time.Minute)
		ctx := rateLimitCtx(t, h)

		// Exhaust the limit.
		for i := range rateLimit {
			allowed, err := limiter.Allow(ctx, "block-after-limit-key")
			require.NoError(t, err)
			require.True(t, allowed, "request %d should be allowed", i+1)
		}

		// Every subsequent request within the window must be denied.
		for i := range 3 {
			allowed, err := limiter.Allow(ctx, "block-after-limit-key")
			require.NoError(t, err)
			require.False(
				t,
				allowed,
				"request %d after limit should be blocked",
				rateLimit+i+1,
			)
		}
	})
}

//nolint:paralleltest // integration tests share testcontainer infrastructure
func TestRateLimiter_ResetsAfterWindow(t *testing.T) {
	integration.RunWithDatabase(t, func(t *testing.T, h *integration.TestHarness) { //nolint:thelper
		// Use a very short window so the test completes quickly.
		const (
			rateLimit = 2
			window    = 2 * time.Second
		)

		limiter, _ := wireRateLimiter(t, h, rateLimit, window)
		ctx := rateLimitCtx(t, h)

		key := fmt.Sprintf("reset-window-key-%d", time.Now().UTC().UnixNano())

		// Exhaust the limit.
		for i := range rateLimit {
			allowed, err := limiter.Allow(ctx, key)
			require.NoError(t, err)
			require.True(t, allowed, "request %d should be allowed", i+1)
		}

		// Confirm we are now blocked.
		allowed, err := limiter.Allow(ctx, key)
		require.NoError(t, err)
		require.False(t, allowed, "request after limit should be blocked")

		// Wait for the window to expire (add a small buffer for Redis PEXPIRE jitter).
		time.Sleep(window + 500*time.Millisecond) //nolint:mnd // test timing buffer

		// After the window the counter must have expired; a new request should succeed.
		allowed, err = limiter.Allow(ctx, key)
		require.NoError(t, err)
		require.True(t, allowed, "request after window expiry should be allowed again")
	})
}

//nolint:paralleltest // integration tests share testcontainer infrastructure
func TestRateLimiter_DifferentKeysIndependent(t *testing.T) {
	integration.RunWithDatabase(t, func(t *testing.T, h *integration.TestHarness) { //nolint:thelper
		const rateLimit = 2

		limiter, _ := wireRateLimiter(t, h, rateLimit, time.Minute)
		ctx := rateLimitCtx(t, h)

		keyA := fmt.Sprintf("independent-key-A-%d", time.Now().UTC().UnixNano())
		keyB := fmt.Sprintf("independent-key-B-%d", time.Now().UTC().UnixNano())

		// Exhaust limit for key A.
		for i := range rateLimit {
			allowed, err := limiter.Allow(ctx, keyA)
			require.NoError(t, err)
			require.True(t, allowed, "key-A request %d should be allowed", i+1)
		}

		// Confirm key A is now rate-limited.
		allowed, err := limiter.Allow(ctx, keyA)
		require.NoError(t, err)
		require.False(t, allowed, "key-A should be rate-limited after exhausting limit")

		// Key B must remain completely unaffected.
		for i := range rateLimit {
			allowed, err = limiter.Allow(ctx, keyB)
			require.NoError(t, err)
			require.True(
				t,
				allowed,
				"key-B request %d should be allowed (independent from key-A)",
				i+1,
			)
		}
	})
}
