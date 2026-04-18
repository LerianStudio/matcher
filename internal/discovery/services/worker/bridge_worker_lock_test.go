// Copyright 2025 Lerian Studio. All rights reserved.
// Use of this source code is governed by an Elastic License 2.0
// that can be found in the LICENSE.md file.

//go:build unit

package worker

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/alicebob/miniredis/v2"
	"github.com/redis/go-redis/v9"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/LerianStudio/matcher/internal/shared/infrastructure/testutil"
)

// newBridgeWorkerWithMiniredis wires a miniredis-backed infra provider to a
// real BridgeWorker so acquireLock / releaseLock exercise the production
// SetNX + Lua-release code paths end-to-end in-process. Mirrors
// newMiniredisInfraProvider in custody_retention_worker_test.go so the
// lock tests stay consistent across workers.
func newBridgeWorkerWithMiniredis(t *testing.T) (*BridgeWorker, *miniredis.Miniredis) {
	t.Helper()

	srv := miniredis.RunT(t)
	client := redis.NewClient(&redis.Options{Addr: srv.Addr()})
	conn := testutil.NewRedisClientWithMock(client)

	provider := &testutil.MockInfrastructureProvider{RedisConn: conn}

	w, err := NewBridgeWorker(
		&stubBridgeOrchestrator{},
		&stubBridgeExtractionRepo{},
		&stubBridgeTenantLister{},
		provider,
		BridgeWorkerConfig{Interval: 30 * time.Second, BatchSize: 10},
		&stubLogger{},
	)
	require.NoError(t, err)

	return w, srv
}

// TestAcquireLock_FirstCallerWins is the happy path: on an empty lock key
// SetNX succeeds, the caller receives a non-empty token, and the key is
// present in Redis with a TTL proportional to the poll interval.
func TestAcquireLock_FirstCallerWins(t *testing.T) {
	t.Parallel()

	w, srv := newBridgeWorkerWithMiniredis(t)

	ok, token, err := w.acquireLock(context.Background())
	require.NoError(t, err)
	assert.True(t, ok, "first caller on an empty key must win the lock")
	assert.NotEmpty(t, token, "winner must receive a non-empty token so release can match")

	assert.True(t, srv.Exists(bridgeWorkerLockKey), "SetNX must have persisted the lock key")

	// TTL on the stored key must fall within (0, lock TTL] — miniredis
	// exposes the TTL so we can pin the SetNX-with-expiry contract.
	ttl := srv.TTL(bridgeWorkerLockKey)
	expected := bridgeLockTTL(w.cfg.Interval)
	assert.Greater(t, ttl, time.Duration(0), "lock must have a positive TTL")
	assert.LessOrEqual(t, ttl, expected, "TTL must not exceed bridgeLockTTL(interval)")
}

// TestAcquireLock_SecondCallerLoses pins the mutual-exclusion contract:
// once a peer has the key, SetNX returns false and the caller MUST NOT be
// handed a token it could accidentally use to release someone else's
// lock.
func TestAcquireLock_SecondCallerLoses(t *testing.T) {
	t.Parallel()

	w, srv := newBridgeWorkerWithMiniredis(t)

	// Pre-seed the lock with a peer-owned token and a healthy TTL so
	// SetNX observes the key and returns false.
	require.NoError(t, srv.Set(bridgeWorkerLockKey, "peer-token"))
	srv.SetTTL(bridgeWorkerLockKey, time.Hour)

	ok, _, err := w.acquireLock(context.Background())
	require.NoError(t, err)
	assert.False(t, ok, "second caller must not win when the key is already held")

	// Peer's value must remain untouched — the load-bearing guarantee is
	// that SetNX did NOT clobber the existing key. The returned token is
	// intentionally not asserted on: callers gate use on `ok` and only
	// pass the token through to releaseLock on the winning branch.
	peer, err := srv.Get(bridgeWorkerLockKey)
	require.NoError(t, err)
	assert.Equal(t, "peer-token", peer)
}

// TestAcquireLock_RedisInfraError returns the wrapped infra-provider error
// verbatim so callers can classify this against transient Redis
// unavailability. The contract is "no ok, no token, propagated error".
func TestAcquireLock_RedisInfraError(t *testing.T) {
	t.Parallel()

	sentinel := errors.New("redis infra down")
	provider := &testutil.MockInfrastructureProvider{RedisErr: sentinel}

	w, err := NewBridgeWorker(
		&stubBridgeOrchestrator{},
		&stubBridgeExtractionRepo{},
		&stubBridgeTenantLister{},
		provider,
		BridgeWorkerConfig{Interval: 30 * time.Second, BatchSize: 10},
		&stubLogger{},
	)
	require.NoError(t, err)

	ok, token, err := w.acquireLock(context.Background())
	require.Error(t, err)
	assert.ErrorIs(t, err, sentinel, "infra error must surface via wrapping so callers can errors.Is it")
	assert.False(t, ok)
	assert.Empty(t, token)
}

// TestReleaseLock_HappyPath releases a lock we own: the key disappears
// from Redis after the Lua eval.
func TestReleaseLock_HappyPath(t *testing.T) {
	t.Parallel()

	w, srv := newBridgeWorkerWithMiniredis(t)

	ctx := context.Background()

	ok, token, err := w.acquireLock(ctx)
	require.NoError(t, err)
	require.True(t, ok)
	require.NotEmpty(t, token)
	require.True(t, srv.Exists(bridgeWorkerLockKey))

	w.releaseLock(ctx, token)

	assert.False(t, srv.Exists(bridgeWorkerLockKey),
		"releaseLock with the owning token must delete the key")
}

// TestReleaseLock_WrongTokenIsNoOp pins the Lua-script guarantee: if
// someone else now owns the key (expired + reacquired), our stale token
// must NOT delete their lock. Without this guard, a slow replica could
// erase an active peer's lock and let two cycles run in parallel.
func TestReleaseLock_WrongTokenIsNoOp(t *testing.T) {
	t.Parallel()

	w, srv := newBridgeWorkerWithMiniredis(t)

	// Put a fresh peer-owned lock in place.
	require.NoError(t, srv.Set(bridgeWorkerLockKey, "peer-token"))
	srv.SetTTL(bridgeWorkerLockKey, time.Hour)

	// Attempt to release with a token we don't own. releaseLock has no
	// return value — we verify by asserting the peer's value survives.
	w.releaseLock(context.Background(), "stale-token")

	peer, err := srv.Get(bridgeWorkerLockKey)
	require.NoError(t, err)
	assert.Equal(t, "peer-token", peer,
		"releaseLock with a non-owning token MUST NOT delete the peer's key — Lua CAS guard")
}

// TestReleaseLock_RedisInfraErrorIsNonFatal pins the "best-effort
// release" contract: if Redis is unreachable mid-cycle, releaseLock logs
// a warning and returns without panicking. The lock's own TTL is the
// backstop — a stuck replica auto-releases within 2× the poll interval.
func TestReleaseLock_RedisInfraErrorIsNonFatal(t *testing.T) {
	t.Parallel()

	provider := &testutil.MockInfrastructureProvider{RedisErr: errors.New("redis down")}

	w, err := NewBridgeWorker(
		&stubBridgeOrchestrator{},
		&stubBridgeExtractionRepo{},
		&stubBridgeTenantLister{},
		provider,
		BridgeWorkerConfig{Interval: 30 * time.Second, BatchSize: 10},
		&stubLogger{},
	)
	require.NoError(t, err)

	// Must not panic — infra errors are logged at WARN and swallowed so
	// the poll loop can keep ticking on the next cycle.
	assert.NotPanics(t, func() {
		w.releaseLock(context.Background(), "any-token")
	})
}

// TestAcquireLock_SecondCallerWinsAfterTTL pins the "stuck-replica
// self-heals" contract. A peer holds the lock but its TTL expires without
// a proper release (e.g. the peer crashed); the next acquireLock must
// succeed rather than stay deadlocked.
func TestAcquireLock_SecondCallerWinsAfterTTL(t *testing.T) {
	t.Parallel()

	w, srv := newBridgeWorkerWithMiniredis(t)

	// Peer-owned lock with a 1s TTL.
	require.NoError(t, srv.Set(bridgeWorkerLockKey, "peer-token"))
	srv.SetTTL(bridgeWorkerLockKey, time.Second)

	// Simulate wall-clock advance past the TTL — the lock auto-expires.
	srv.FastForward(2 * time.Second)
	require.False(t, srv.Exists(bridgeWorkerLockKey), "peer lock must have expired")

	ok, token, err := w.acquireLock(context.Background())
	require.NoError(t, err)
	assert.True(t, ok, "after TTL expiry the next caller must win — prevents eternal deadlocks")
	assert.NotEmpty(t, token)
}
