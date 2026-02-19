package ports

import (
	"context"
	"errors"
	"time"

	"github.com/google/uuid"
)

// ErrDuplicateTransaction indicates a duplicate transaction was detected.
var ErrDuplicateTransaction = errors.New("duplicate transaction detected")

// DedupeService handles transaction deduplication.
type DedupeService interface {
	// CalculateHash generates a unique hash for deduplication
	// Uses source_id + external_id as the idempotency key per data model spec
	CalculateHash(sourceID uuid.UUID, externalID string) string

	// IsDuplicate checks if a transaction hash has been seen
	IsDuplicate(ctx context.Context, contextID uuid.UUID, hash string) (bool, error)

	// MarkSeen records a transaction hash with TTL (TTL=0 means no expiration).
	MarkSeen(ctx context.Context, contextID uuid.UUID, hash string, ttl time.Duration) error

	// MarkSeenWithRetry handles retry-safe marking for collision scenarios.
	// TTL=0 means no expiration.
	MarkSeenWithRetry(
		ctx context.Context,
		contextID uuid.UUID,
		hash string,
		ttl time.Duration,
		maxRetries int,
	) error

	// Clear removes a deduplication key, allowing the transaction to be retried.
	// Used to clean up on failure to prevent zombie locks.
	Clear(ctx context.Context, contextID uuid.UUID, hash string) error

	// ClearBatch removes multiple deduplication keys.
	ClearBatch(ctx context.Context, contextID uuid.UUID, hashes []string) error
}
