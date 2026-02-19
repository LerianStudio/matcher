// Package testutil provides helper functions for writing tests.
package testutil

import (
	"crypto/sha256"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/shopspring/decimal"
)

// MustDeterministicUUID returns a stable UUID derived from the provided seed.
// Useful for creating reproducible test data.
// Fails the test if UUID creation fails (which should never happen with valid hash input).
func MustDeterministicUUID(tb testing.TB, seed string) uuid.UUID {
	tb.Helper()

	hash := sha256.Sum256([]byte(seed))

	id, err := uuid.FromBytes(hash[:16])
	if err != nil {
		tb.Fatalf(
			"testutil: MustDeterministicUUID failed to build UUID from hash slice %x: %v",
			hash[:16],
			err,
		)
	}

	return id
}

// StringPtr returns a pointer to the given string.
// Useful for constructing test data with optional string fields.
func StringPtr(s string) *string {
	return &s
}

// TimePtr returns a pointer to the given time.Time.
// Useful for constructing test data with optional time fields.
func TimePtr(t time.Time) *time.Time {
	return &t
}

// UUIDPtr returns a pointer to the given uuid.UUID.
// Useful for constructing test data with optional UUID fields.
func UUIDPtr(u uuid.UUID) *uuid.UUID {
	return &u
}

// IntPtr returns a pointer to the given int.
// Useful for constructing test data with optional int fields.
func IntPtr(i int) *int {
	return &i
}

// DecimalFromInt creates a decimal.Decimal from an int64 and returns a pointer to it.
// Useful for constructing test data with optional decimal fields from integer literals.
func DecimalFromInt(val int64) *decimal.Decimal {
	d := decimal.NewFromInt(val)
	return &d
}

// Ptr returns a pointer to the given value.
// Generic helper for constructing test data with optional fields of any type.
func Ptr[T any](v T) *T {
	return &v
}
