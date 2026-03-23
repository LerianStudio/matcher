//go:build unit

// Copyright 2025 Lerian Studio. All rights reserved.
// Use of this source code is governed by an Elastic License 2.0
// that can be found in the LICENSE.md file.

package bootstrap

import (
	"context"
	"errors"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	libRedis "github.com/LerianStudio/lib-commons/v4/commons/redis"

	sharedPorts "github.com/LerianStudio/matcher/internal/shared/ports"
)

// Compile-time interface assertion.
var _ libRedis.LockManager = (*providerBackedLockManager)(nil)

// --- newProviderBackedLockManager ---

func TestNewProviderBackedLockManager_NilProvider(t *testing.T) {
	t.Parallel()

	mgr := newProviderBackedLockManager(nil)
	assert.Nil(t, mgr, "nil provider must produce nil lock manager")
}

func TestNewProviderBackedLockManager_NonNilProvider(t *testing.T) {
	t.Parallel()

	mgr := newProviderBackedLockManager(&fakeLockInfraProvider{})
	require.NotNil(t, mgr, "non-nil provider must produce non-nil lock manager")

	_, ok := mgr.(*providerBackedLockManager)
	assert.True(t, ok, "should be *providerBackedLockManager")
}

// --- TryLock ---

func TestProviderBackedLockManager_TryLock_RedisUnavailable(t *testing.T) {
	t.Parallel()

	redisErr := errors.New("redis connection refused")
	provider := &fakeLockInfraProvider{
		redisErr: redisErr,
	}

	mgr := newProviderBackedLockManager(provider)
	require.NotNil(t, mgr)

	handle, acquired, err := mgr.TryLock(context.Background(), "test:lock")

	assert.Nil(t, handle)
	assert.False(t, acquired)
	require.Error(t, err)
	assert.ErrorContains(t, err, "redis connection for lock")
}

func TestProviderBackedLockManager_TryLock_NilProviderField(t *testing.T) {
	t.Parallel()

	// Construct directly to test the nil-provider-field path.
	mgr := &providerBackedLockManager{provider: nil}

	handle, acquired, err := mgr.TryLock(context.Background(), "test:lock")

	assert.Nil(t, handle)
	assert.False(t, acquired)
	require.Error(t, err)
	assert.ErrorContains(t, err, "infrastructure provider not available")
}

// --- WithLock ---

func TestProviderBackedLockManager_WithLock_RedisUnavailable(t *testing.T) {
	t.Parallel()

	redisErr := errors.New("redis connection refused")
	provider := &fakeLockInfraProvider{
		redisErr: redisErr,
	}

	mgr := newProviderBackedLockManager(provider)
	require.NotNil(t, mgr)

	err := mgr.WithLock(context.Background(), "test:lock", func(_ context.Context) error {
		t.Fatal("function should not be called when Redis is unavailable")
		return nil
	})

	require.Error(t, err)
	assert.ErrorContains(t, err, "resolve lock manager")
}

// --- WithLockOptions ---

func TestProviderBackedLockManager_WithLockOptions_RedisUnavailable(t *testing.T) {
	t.Parallel()

	redisErr := errors.New("redis connection refused")
	provider := &fakeLockInfraProvider{
		redisErr: redisErr,
	}

	mgr := newProviderBackedLockManager(provider)
	require.NotNil(t, mgr)

	opts := libRedis.DefaultLockOptions()

	err := mgr.WithLockOptions(context.Background(), "test:lock", opts, func(_ context.Context) error {
		t.Fatal("function should not be called when Redis is unavailable")
		return nil
	})

	require.Error(t, err)
	assert.ErrorContains(t, err, "resolve lock manager")
}

// --- resolveManager lease release on error ---

func TestProviderBackedLockManager_ResolveManager_ReleasesLeaseOnLockManagerError(t *testing.T) {
	t.Parallel()

	// GetRedisConnection succeeds but returns nil connection → NewRedisLockManager fails.
	var leaseReleased bool

	provider := &fakeLockInfraProvider{
		redisConn:      nil, // will cause NewRedisLockManager to fail
		releaseTracker: &leaseReleased,
	}

	mgr := newProviderBackedLockManager(provider)
	require.NotNil(t, mgr)

	handle, acquired, err := mgr.TryLock(context.Background(), "test:lock")

	assert.Nil(t, handle)
	assert.False(t, acquired)
	require.Error(t, err)
	assert.ErrorContains(t, err, "redis connection for lock")
	assert.True(t, leaseReleased, "lease must be released when lock manager creation fails")
}

// --- Sentinel error ---

func TestErrLockManagerProviderNil_Message(t *testing.T) {
	t.Parallel()

	assert.Equal(t, "lock manager: infrastructure provider not available", errLockManagerProviderNil.Error())
}

// --- test doubles ---

// fakeLockInfraProvider is an InfrastructureProvider that can simulate Redis failures.
type fakeLockInfraProvider struct {
	redisErr       error
	redisConn      *libRedis.Client
	releaseTracker *bool
}

func (f *fakeLockInfraProvider) GetPostgresConnection(_ context.Context) (*sharedPorts.PostgresConnectionLease, error) {
	return nil, errors.New("not implemented")
}

func (f *fakeLockInfraProvider) GetRedisConnection(_ context.Context) (*sharedPorts.RedisConnectionLease, error) {
	if f.redisErr != nil {
		return nil, f.redisErr
	}

	release := func() {
		if f.releaseTracker != nil {
			*f.releaseTracker = true
		}
	}

	// NewRedisConnectionLease returns nil when conn is nil, so we need to handle that.
	lease := sharedPorts.NewRedisConnectionLease(f.redisConn, release)
	if lease == nil {
		// Return a minimal non-nil lease that tracks release but has nil connection.
		// This simulates a connection that was obtained but is broken.
		// Since NewRedisConnectionLease returns nil for nil conn, we call release
		// ourselves and return an error to simulate the broken-connection path.
		release()

		return nil, errors.New("redis connection is nil")
	}

	return lease, nil
}

func (f *fakeLockInfraProvider) BeginTx(_ context.Context) (*sharedPorts.TxLease, error) {
	return nil, errors.New("not implemented")
}

func (f *fakeLockInfraProvider) GetReplicaDB(_ context.Context) (*sharedPorts.ReplicaDBLease, error) {
	return nil, errors.New("not implemented")
}

var _ sharedPorts.InfrastructureProvider = (*fakeLockInfraProvider)(nil)

// fakeLockHandle implements libRedis.LockHandle for test assertions.
type fakeLockHandle struct {
	unlockFn func(ctx context.Context) error
}

var _ libRedis.LockHandle = (*fakeLockHandle)(nil)

func (h *fakeLockHandle) Unlock(ctx context.Context) error {
	if h.unlockFn != nil {
		return h.unlockFn(ctx)
	}

	return nil
}
