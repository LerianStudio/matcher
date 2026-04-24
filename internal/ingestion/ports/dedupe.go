// Copyright 2025 Lerian Studio. All rights reserved.
// Use of this source code is governed by an Elastic License 2.0
// that can be found in the LICENSE.md file.

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

	// MarkSeenBulk attempts to mark all hashes in a single round trip using a
	// Lua script. Returns a map keyed by hash where the value is true if the
	// hash was newly set (the caller should process the transaction) and false
	// if it was already present (duplicate). TTL=0 means no expiration.
	MarkSeenBulk(
		ctx context.Context,
		contextID uuid.UUID,
		hashes []string,
		ttl time.Duration,
	) (map[string]bool, error)

	// Clear removes a deduplication key, allowing the transaction to be retried.
	// Used to clean up on failure to prevent zombie locks.
	Clear(ctx context.Context, contextID uuid.UUID, hash string) error

	// ClearBatch removes multiple deduplication keys.
	ClearBatch(ctx context.Context, contextID uuid.UUID, hashes []string) error
}
