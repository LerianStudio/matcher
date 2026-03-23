// Copyright 2025 Lerian Studio. All rights reserved.
// Use of this source code is governed by an Elastic License 2.0
// that can be found in the LICENSE.md file.

package bootstrap

import (
	"context"
	"errors"
	"fmt"

	libRedis "github.com/LerianStudio/lib-commons/v4/commons/redis"

	sharedPorts "github.com/LerianStudio/matcher/internal/shared/ports"
)

var errLockManagerProviderNil = errors.New("lock manager: infrastructure provider not available")

// providerBackedLockManager implements libRedis.LockManager by lazily resolving
// a Redis connection from the InfrastructureProvider on every lock operation.
//
// This makes the scheduler worker resilient to:
//   - Redis being temporarily unavailable at boot (lazy init instead of fail-fast)
//   - Runtime infrastructure bundle swaps (always uses the current Redis connection)
//   - Transient Redis outages (each attempt independently resolves a connection)
type providerBackedLockManager struct {
	provider sharedPorts.InfrastructureProvider
}

// Compile-time interface assertion.
var _ libRedis.LockManager = (*providerBackedLockManager)(nil)

// newProviderBackedLockManager creates a lock manager that resolves Redis lazily.
// Returns nil if provider is nil.
func newProviderBackedLockManager(provider sharedPorts.InfrastructureProvider) libRedis.LockManager {
	if provider == nil {
		return nil
	}

	return &providerBackedLockManager{provider: provider}
}

// resolveManager obtains a Redis lease from the provider and constructs a
// RedisLockManager from it. The caller MUST release the lease when done.
func (m *providerBackedLockManager) resolveManager(ctx context.Context) (*libRedis.RedisLockManager, *sharedPorts.RedisConnectionLease, error) {
	if m.provider == nil {
		return nil, nil, errLockManagerProviderNil
	}

	lease, err := m.provider.GetRedisConnection(ctx)
	if err != nil {
		return nil, nil, fmt.Errorf("redis connection for lock: %w", err)
	}

	mgr, err := libRedis.NewRedisLockManager(lease.Connection())
	if err != nil {
		lease.Release()
		return nil, nil, fmt.Errorf("create lock manager: %w", err)
	}

	return mgr, lease, nil
}

// WithLock executes fn while holding a distributed lock with default options.
func (m *providerBackedLockManager) WithLock(ctx context.Context, lockKey string, fn func(context.Context) error) error {
	mgr, lease, err := m.resolveManager(ctx)
	if err != nil {
		return fmt.Errorf("resolve lock manager: %w", err)
	}
	defer lease.Release()

	if err = mgr.WithLock(ctx, lockKey, fn); err != nil {
		return fmt.Errorf("lock manager: with lock: %w", err)
	}

	return nil
}

// WithLockOptions executes fn while holding a distributed lock with custom options.
func (m *providerBackedLockManager) WithLockOptions(ctx context.Context, lockKey string, opts libRedis.LockOptions, fn func(context.Context) error) error {
	mgr, lease, err := m.resolveManager(ctx)
	if err != nil {
		return fmt.Errorf("resolve lock manager: %w", err)
	}
	defer lease.Release()

	if err = mgr.WithLockOptions(ctx, lockKey, opts, fn); err != nil {
		return fmt.Errorf("lock manager: with lock options: %w", err)
	}

	return nil
}

// TryLock attempts to acquire a lock without retrying.
func (m *providerBackedLockManager) TryLock(ctx context.Context, lockKey string) (libRedis.LockHandle, bool, error) {
	mgr, lease, err := m.resolveManager(ctx)
	if err != nil {
		return nil, false, fmt.Errorf("resolve lock manager: %w", err)
	}

	handle, acquired, tryErr := mgr.TryLock(ctx, lockKey)
	if tryErr != nil || !acquired {
		// Lock not acquired or error — release the lease immediately.
		lease.Release()

		if tryErr != nil {
			return nil, acquired, fmt.Errorf("lock manager: try lock: %w", tryErr)
		}

		return nil, acquired, nil
	}

	// Lock acquired — wrap the handle to release the lease on Unlock.
	return &leaseAwareLockHandle{inner: handle, lease: lease}, true, nil
}

// leaseAwareLockHandle wraps a LockHandle and releases the Redis connection
// lease when the lock is unlocked. This ensures the lease stays alive for
// the duration the lock is held, preventing the underlying connection from
// being reclaimed while the lock's TTL is still valid.
type leaseAwareLockHandle struct {
	inner libRedis.LockHandle
	lease *sharedPorts.RedisConnectionLease
}

// Unlock releases the distributed lock and then releases the Redis connection lease.
func (h *leaseAwareLockHandle) Unlock(ctx context.Context) error {
	defer h.lease.Release()

	if err := h.inner.Unlock(ctx); err != nil {
		return fmt.Errorf("lock manager: unlock: %w", err)
	}

	return nil
}
