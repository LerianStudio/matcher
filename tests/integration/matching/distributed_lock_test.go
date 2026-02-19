//go:build integration

package matching

import (
	"context"
	"errors"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/stretchr/testify/require"

	"github.com/LerianStudio/matcher/internal/auth"
	matchLockManager "github.com/LerianStudio/matcher/internal/matching/adapters/redis"
	matchingPorts "github.com/LerianStudio/matcher/internal/matching/ports"
	tenantAdapters "github.com/LerianStudio/matcher/internal/shared/infrastructure/tenant/adapters"
	"github.com/LerianStudio/matcher/tests/integration"
)

// mustLockManager creates a LockManager backed by the testcontainer Redis instance.
// It reuses the existing mustRedisConn helper (from helpers_test.go) to build the
// libRedis.Client, then wraps it with the tenant-aware provider that the lock manager
// requires for key scoping.
func mustLockManager(t *testing.T, h *integration.TestHarness) *matchLockManager.LockManager {
	t.Helper()

	redisConn := mustRedisConn(t, h.RedisAddr)
	provider := tenantAdapters.NewSingleTenantInfrastructureProvider(h.Connection, redisConn)

	return matchLockManager.NewLockManager(provider)
}

// lockCtx returns a context enriched with tenant ID and slug—required by the lock
// manager to build tenant-scoped Redis keys.
func lockCtx(t *testing.T, h *integration.TestHarness) context.Context {
	t.Helper()

	ctx := h.Ctx()
	ctx = context.WithValue(ctx, auth.TenantSlugKey, auth.DefaultTenantSlug)

	return ctx
}

func TestDistributedLock_AcquireAndRelease(t *testing.T) {
	t.Parallel()

	integration.RunWithDatabase(t, func(t *testing.T, h *integration.TestHarness) {
		lm := mustLockManager(t, h)
		ctx := lockCtx(t, h)
		contextID := uuid.New()

		lock, err := lm.AcquireContextLock(ctx, contextID, 30*time.Second)
		require.NoError(t, err, "first acquire should succeed")
		require.NotNil(t, lock)

		err = lock.Release(ctx)
		require.NoError(t, err, "release with correct token should succeed")
	})
}

func TestDistributedLock_DoubleAcquireFails(t *testing.T) {
	t.Parallel()

	integration.RunWithDatabase(t, func(t *testing.T, h *integration.TestHarness) {
		lm := mustLockManager(t, h)
		ctx := lockCtx(t, h)
		contextID := uuid.New()

		first, err := lm.AcquireContextLock(ctx, contextID, 30*time.Second)
		require.NoError(t, err)

		// Second acquire on the same context must fail while the first is held.
		_, err = lm.AcquireContextLock(ctx, contextID, 30*time.Second)
		require.Error(t, err, "second acquire should fail while first lock is held")
		require.True(t,
			errors.Is(err, matchingPorts.ErrLockAlreadyHeld),
			"expected ErrLockAlreadyHeld, got: %v", err,
		)

		// After releasing the first lock, a new acquire should succeed.
		require.NoError(t, first.Release(ctx))

		second, err := lm.AcquireContextLock(ctx, contextID, 30*time.Second)
		require.NoError(t, err, "acquire after release should succeed")
		require.NotNil(t, second)

		require.NoError(t, second.Release(ctx))
	})
}

func TestDistributedLock_DifferentContextsIndependent(t *testing.T) {
	t.Parallel()

	integration.RunWithDatabase(t, func(t *testing.T, h *integration.TestHarness) {
		lm := mustLockManager(t, h)
		ctx := lockCtx(t, h)
		contextA := uuid.New()
		contextB := uuid.New()

		lockA, err := lm.AcquireContextLock(ctx, contextA, 30*time.Second)
		require.NoError(t, err, "acquire on context A should succeed")

		lockB, err := lm.AcquireContextLock(ctx, contextB, 30*time.Second)
		require.NoError(t, err, "acquire on context B should succeed while A is held")

		require.NoError(t, lockA.Release(ctx))
		require.NoError(t, lockB.Release(ctx))
	})
}

func TestDistributedLock_DoubleReleaseFails(t *testing.T) {
	t.Parallel()

	integration.RunWithDatabase(t, func(t *testing.T, h *integration.TestHarness) {
		lm := mustLockManager(t, h)
		ctx := lockCtx(t, h)
		contextID := uuid.New()

		// Acquire two locks on different contexts to obtain two distinct Lock handles
		// with different tokens.
		lockReal, err := lm.AcquireContextLock(ctx, contextID, 30*time.Second)
		require.NoError(t, err)

		otherCtxID := uuid.New()
		lockOther, err := lm.AcquireContextLock(ctx, otherCtxID, 30*time.Second)
		require.NoError(t, err)

		// First release succeeds.
		require.NoError(t, lockReal.Release(ctx))

		// Second release on the same handle should fail because the key is gone.
		err = lockReal.Release(ctx)
		require.Error(t, err, "double release should fail (token no longer matches)")
		require.True(t,
			errors.Is(err, matchingPorts.ErrLockAlreadyHeld),
			"expected ErrLockAlreadyHeld on double release, got: %v", err,
		)

		// Cleanup the other lock.
		require.NoError(t, lockOther.Release(ctx))
	})
}

func TestDistributedLock_LockExpires(t *testing.T) {
	t.Parallel()

	integration.RunWithDatabase(t, func(t *testing.T, h *integration.TestHarness) {
		lm := mustLockManager(t, h)
		ctx := lockCtx(t, h)
		contextID := uuid.New()

		// Acquire with a very short TTL so Redis expires the key quickly.
		shortTTL := 200 * time.Millisecond

		lock, err := lm.AcquireContextLock(ctx, contextID, shortTTL)
		require.NoError(t, err)
		require.NotNil(t, lock)

		// Wait for the key to expire.
		time.Sleep(400 * time.Millisecond)

		// A fresh acquire should succeed because the previous lock expired.
		freshLock, err := lm.AcquireContextLock(ctx, contextID, 30*time.Second)
		require.NoError(t, err, "acquire should succeed after TTL expiry")
		require.NotNil(t, freshLock)

		require.NoError(t, freshLock.Release(ctx))
	})
}

func TestDistributedLock_ConcurrentAcquire(t *testing.T) {
	t.Parallel()

	integration.RunWithDatabase(t, func(t *testing.T, h *integration.TestHarness) {
		lm := mustLockManager(t, h)
		ctx := lockCtx(t, h)
		contextID := uuid.New()

		const goroutines = 10

		var (
			wg      sync.WaitGroup
			wins    atomic.Int32
			winLock matchingPorts.Lock
			winMu   sync.Mutex
		)

		wg.Add(goroutines)

		for range goroutines {
			go func() {
				defer wg.Done()

				lock, err := lm.AcquireContextLock(ctx, contextID, 30*time.Second)
				if err == nil {
					wins.Add(1)

					winMu.Lock()
					winLock = lock
					winMu.Unlock()
				}
			}()
		}

		wg.Wait()

		require.Equal(t, int32(1), wins.Load(),
			"exactly 1 goroutine should acquire the lock; got %d", wins.Load())

		// Cleanup the winning lock.
		winMu.Lock()
		defer winMu.Unlock()

		require.NotNil(t, winLock)
		require.NoError(t, winLock.Release(ctx))
	})
}

func TestDistributedLock_RefreshExtendsTTL(t *testing.T) {
	t.Parallel()

	integration.RunWithDatabase(t, func(t *testing.T, h *integration.TestHarness) {
		lm := mustLockManager(t, h)
		ctx := lockCtx(t, h)
		contextID := uuid.New()

		// Acquire with a short TTL.
		lock, err := lm.AcquireContextLock(ctx, contextID, 500*time.Millisecond)
		require.NoError(t, err)

		// The Redis lock implements RefreshableLock; assert that and refresh.
		refreshable, ok := lock.(matchingPorts.RefreshableLock)
		require.True(t, ok, "lock should implement RefreshableLock")

		// Refresh extends the TTL well beyond the original 500ms.
		err = refreshable.Refresh(ctx, 30*time.Second)
		require.NoError(t, err)

		// Wait past the original TTL — the key should still exist because we refreshed.
		time.Sleep(700 * time.Millisecond)

		// A competing acquire must fail, proving the refresh kept the lock alive.
		_, err = lm.AcquireContextLock(ctx, contextID, 5*time.Second)
		require.Error(t, err, "lock should still be held after refresh")
		require.True(t,
			errors.Is(err, matchingPorts.ErrLockAlreadyHeld),
			"expected ErrLockAlreadyHeld, got: %v", err,
		)

		require.NoError(t, lock.Release(ctx))
	})
}
