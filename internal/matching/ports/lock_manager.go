package ports

import (
	"context"
	"errors"
	"time"

	"github.com/google/uuid"
)

// ErrLockAlreadyHeld indicates another match run holds a conflicting lock.
var ErrLockAlreadyHeld = errors.New("lock already held")

// Lock represents a held lock for transactions.
// Contract: non-nil when err == nil.
type Lock interface {
	Release(ctx context.Context) error
}

// RefreshableLock extends Lock with TTL refresh support.
// Contract:
// - Refresh should extend the lock TTL by the supplied duration.
// - If Refresh fails, the lock should be treated as unsafe to use.
type RefreshableLock interface {
	Lock
	Refresh(ctx context.Context, ttl time.Duration) error
}

// LockManager prevents double-matching across concurrent runs.
//
// Contract:
// - lock keys must be tenant-scoped based on auth context.
// - implementations should extract tenant via auth.GetTenantID(ctx).
type LockManager interface {
	AcquireTransactionsLock(
		ctx context.Context,
		contextID uuid.UUID,
		transactionIDs []uuid.UUID,
		ttl time.Duration,
	) (Lock, error)
	AcquireContextLock(ctx context.Context, contextID uuid.UUID, ttl time.Duration) (Lock, error)
}
