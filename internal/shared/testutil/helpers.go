package testutil

import (
	"math/rand"
	"time"
)

// Clone returns a shallow copy of the input slice.
func Clone[T any](in []T) []T {
	out := make([]T, len(in))
	copy(out, in)

	return out
}

// Permute returns a shuffled copy of the input slice.
func Permute[T any](in []T, rng *rand.Rand) []T {
	out := Clone(in)
	rng.Shuffle(len(out), func(i, j int) {
		out[i], out[j] = out[j], out[i]
	})

	return out
}

// CloneRules returns a shallow copy of the input slice.
func CloneRules[T any](in []T) []T {
	return Clone(in)
}

// CloneTransactions returns a shallow copy of the input slice.
func CloneTransactions[T any](in []T) []T {
	return Clone(in)
}

// PermuteRules returns a shuffled copy of the input slice.
func PermuteRules[T any](in []T, rng *rand.Rand) []T {
	return Permute(in, rng)
}

// PermuteTransactions returns a shuffled copy of the input slice.
func PermuteTransactions[T any](in []T, rng *rand.Rand) []T {
	return Permute(in, rng)
}

// FixedTime provides a deterministic timestamp for tests.
func FixedTime() time.Time {
	return time.Date(2026, 1, 1, 11, 0, 0, 0, time.UTC)
}
