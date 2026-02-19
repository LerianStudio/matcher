package testutil

import (
	"crypto/sha256"
	"fmt"
	"time"

	"github.com/google/uuid"
)

// FixedTime returns a deterministic UTC timestamp for use in tests.
// Using a fixed time instead of time.Now().UTC() prevents flaky assertions
// caused by clock drift between entity creation and test assertion.
//
// The chosen date (2026-01-15T10:30:00Z) is arbitrary but stable.
func FixedTime() time.Time {
	return time.Date(2026, 1, 15, 10, 30, 0, 0, time.UTC)
}

// DeterministicUUID generates a reproducible UUID from a string seed.
// Given the same seed, it always returns the same UUID across runs.
// Use descriptive seeds like "ledger-source", "test-context", "exception-1".
//
// The implementation uses UUID v5 (SHA-1 based, RFC 4122) with a fixed
// namespace UUID, ensuring deterministic output without collisions
// across different seeds.
func DeterministicUUID(seed string) uuid.UUID {
	return uuid.NewSHA1(deterministicNamespace, []byte(seed))
}

// deterministicNamespace is a fixed namespace UUID for DeterministicUUID.
// Generated from SHA-256("matcher-test-namespace") truncated to 16 bytes.
var deterministicNamespace = func() uuid.UUID {
	h := sha256.Sum256([]byte("matcher-test-namespace"))
	id, _ := uuid.FromBytes(h[:16])

	return id
}()

// DeterministicUUIDs generates n reproducible UUIDs using sequential seeds.
// Useful for populating slices where each element needs a unique but stable ID.
//
// Example: DeterministicUUIDs("item", 3) returns UUIDs for seeds
// "item-0", "item-1", "item-2".
func DeterministicUUIDs(prefix string, n int) []uuid.UUID {
	ids := make([]uuid.UUID, n)
	for i := range n {
		ids[i] = DeterministicUUID(fmt.Sprintf("%s-%d", prefix, i))
	}

	return ids
}
