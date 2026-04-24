// Copyright 2025 Lerian Studio. All rights reserved.
// Use of this source code is governed by an Elastic License 2.0
// that can be found in the LICENSE.md file.

// Package redis provides Redis adapters for matching.
package redis

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/google/uuid"

	"github.com/LerianStudio/matcher/internal/auth"
	"github.com/LerianStudio/matcher/internal/matching/ports"
	sharedPorts "github.com/LerianStudio/matcher/internal/shared/ports"
)

var (
	errRedisConnRequired     = errors.New("redis connection is required")
	errLockReleaseUnexpected = errors.New("release lock returned unexpected result")
	errLockRefreshUnexpected = errors.New("refresh lock returned unexpected result")
	errLockTTLInvalid        = errors.New("lock ttl must be greater than zero")
	errLockTenantRequired    = errors.New("tenant id is required for lock scoping")
	errLockTokenReadFailed   = errors.New("read random lock token")
	errLockContextIDInvalid  = errors.New("context id must be a valid non-nil UUID")
)

const (
	lockKeyPrefix  = "matcher:matchrun:lock"
	lockTokenBytes = 16
)

// LockManager manages Redis-backed locks for match runs.
type LockManager struct {
	provider sharedPorts.InfrastructureProvider
}

// NewLockManager creates a LockManager backed by Redis.
// If provider is nil, methods will return errRedisConnRequired on use.
func NewLockManager(provider sharedPorts.InfrastructureProvider) *LockManager {
	return &LockManager{provider: provider}
}

type lock struct {
	lease *sharedPorts.RedisConnectionLease
	key   string
	token string
}

// Release releases the held Redis lock.
func (lck *lock) Release(ctx context.Context) error {
	if lck == nil || lck.lease == nil || lck.lease.Connection() == nil {
		return errRedisConnRequired
	}
	defer lck.lease.Release()

	if strings.TrimSpace(lck.key) == "" {
		return nil
	}

	rdb, err := lck.lease.Connection().GetClient(ctx)
	if err != nil {
		return fmt.Errorf("%w: %w", errRedisConnRequired, err)
	}

	script := `
if redis.call("GET", KEYS[1]) == ARGV[1] then
  return redis.call("DEL", KEYS[1])
else
  return 0
end
`

	released, err := rdb.Eval(ctx, script, []string{lck.key}, lck.token).Result()
	if err != nil {
		return fmt.Errorf("release lock: %w", err)
	}

	releasedCount, ok := released.(int64)
	if !ok {
		return fmt.Errorf("%w: %T", errLockReleaseUnexpected, released)
	}

	if releasedCount == 0 {
		return ports.ErrLockAlreadyHeld
	}

	return nil
}

// Refresh extends the Redis lock TTL if it is still owned.
func (lck *lock) Refresh(ctx context.Context, ttl time.Duration) error {
	if lck == nil || lck.lease == nil || lck.lease.Connection() == nil {
		return errRedisConnRequired
	}

	if strings.TrimSpace(lck.key) == "" {
		return nil
	}

	rdb, err := lck.lease.Connection().GetClient(ctx)
	if err != nil {
		return fmt.Errorf("%w: %w", errRedisConnRequired, err)
	}

	script := `
if redis.call("GET", KEYS[1]) == ARGV[1] then
  return redis.call("PEXPIRE", KEYS[1], ARGV[2])
else
  return 0
end
`

	refreshed, err := rdb.Eval(ctx, script, []string{lck.key}, lck.token, ttl.Milliseconds()).
		Result()
	if err != nil {
		return fmt.Errorf("refresh lock: %w", err)
	}

	refreshedCount, ok := refreshed.(int64)
	if !ok {
		return fmt.Errorf("%w: %T", errLockRefreshUnexpected, refreshed)
	}

	if refreshedCount == 0 {
		return ports.ErrLockAlreadyHeld
	}

	return nil
}

// AcquireTransactionsLock acquires a lock for a matching run.
// The transactionIDs parameter is reserved for future granular locking and is currently unused.
func (mgr *LockManager) AcquireTransactionsLock(
	ctx context.Context,
	contextID uuid.UUID,
	transactionIDs []uuid.UUID,
	ttl time.Duration,
) (ports.Lock, error) {
	if mgr == nil || mgr.provider == nil {
		return nil, errRedisConnRequired
	}

	if ttl <= 0 {
		return nil, errLockTTLInvalid
	}

	if contextID == uuid.Nil {
		return nil, errLockContextIDInvalid
	}

	tenantID := strings.TrimSpace(auth.GetTenantID(ctx))
	if tenantID == "" {
		return nil, errLockTenantRequired
	}

	key := fmt.Sprintf("%s:%s:%s", lockKeyPrefix, tenantID, contextID.String())

	return mgr.acquireLock(ctx, key, ttl)
}

// AcquireContextLock acquires a lock for a matching context scoped to tenant ID.
// Tenant ID is extracted from context via auth.GetTenantID(ctx).
func (mgr *LockManager) AcquireContextLock(
	ctx context.Context,
	contextID uuid.UUID,
	ttl time.Duration,
) (ports.Lock, error) {
	if mgr == nil || mgr.provider == nil {
		return nil, errRedisConnRequired
	}

	if ttl <= 0 {
		return nil, errLockTTLInvalid
	}

	if contextID == uuid.Nil {
		return nil, errLockContextIDInvalid
	}

	tenantID := strings.TrimSpace(auth.GetTenantID(ctx))
	if tenantID == "" {
		return nil, errLockTenantRequired
	}

	key := fmt.Sprintf("%s:%s:%s", lockKeyPrefix, tenantID, contextID.String())

	return mgr.acquireLock(ctx, key, ttl)
}

func (mgr *LockManager) acquireLock(
	ctx context.Context,
	key string,
	ttl time.Duration,
) (ports.Lock, error) {
	token, err := randomToken(lockTokenBytes)
	if err != nil {
		return nil, fmt.Errorf("generate lock token: %w", err)
	}

	connLease, err := mgr.provider.GetRedisConnection(ctx)
	if err != nil {
		return nil, fmt.Errorf("get redis connection: %w", err)
	}

	if connLease == nil || connLease.Connection() == nil {
		return nil, errRedisConnRequired
	}

	rdb, err := connLease.Connection().GetClient(ctx)
	if err != nil {
		connLease.Release()

		return nil, fmt.Errorf("%w: %w", errRedisConnRequired, err)
	}

	ok, err := rdb.SetNX(ctx, key, token, ttl).Result()
	if err != nil {
		connLease.Release()

		return nil, fmt.Errorf("acquire lock: %w", err)
	}

	if !ok {
		connLease.Release()

		return nil, ports.ErrLockAlreadyHeld
	}

	return &lock{lease: connLease, key: key, token: token}, nil
}

func randomToken(nBytes int) (string, error) {
	b := make([]byte, nBytes)
	if _, err := rand.Read(b); err != nil {
		return "", fmt.Errorf("%w: %w", errLockTokenReadFailed, err)
	}

	return hex.EncodeToString(b), nil
}
