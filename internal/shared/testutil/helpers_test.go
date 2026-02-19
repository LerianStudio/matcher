//go:build unit

package testutil

import (
	"math/rand"
	"sort"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

const (
	seedDefault = 42
	seedStable  = 12345
)

func TestClone(t *testing.T) {
	t.Parallel()

	t.Run("empty slice", func(t *testing.T) {
		t.Parallel()

		original := []int{}
		cloned := Clone(original)

		require.Empty(t, cloned)
	})

	t.Run("single element", func(t *testing.T) {
		t.Parallel()

		original := []int{42}
		cloned := Clone(original)

		require.Equal(t, []int{42}, cloned)
	})

	t.Run("original unchanged", func(t *testing.T) {
		t.Parallel()

		original := []int{1, 2, 3, 4, 5}
		cloned := Clone(original)

		cloned[0] = 999

		require.Equal(t, []int{1, 2, 3, 4, 5}, original)
		require.Equal(t, 999, cloned[0])
	})

	t.Run("same elements after permute", func(t *testing.T) {
		t.Parallel()

		rng := rand.New(rand.NewSource(seedDefault))
		original := []int{5, 3, 1, 4, 2}
		permuted := Permute(original, rng)

		sortedOriginal := Clone(original)
		sortedPermuted := Clone(permuted)

		sort.Ints(sortedOriginal)
		sort.Ints(sortedPermuted)

		require.Equal(t, sortedOriginal, sortedPermuted)
	})

	t.Run("fixed seed produces same result", func(t *testing.T) {
		t.Parallel()

		original := []int{1, 2, 3, 4, 5, 6, 7, 8, 9, 10}

		rng1 := rand.New(rand.NewSource(seedStable))
		permuted1 := Permute(original, rng1)

		rng2 := rand.New(rand.NewSource(seedStable))
		permuted2 := Permute(original, rng2)

		require.Equal(t, permuted1, permuted2)
	})
}

func TestCloneRules(t *testing.T) {
	t.Parallel()

	t.Run("empty slice", func(t *testing.T) {
		t.Parallel()

		original := []string{}
		cloned := CloneRules(original)

		require.Empty(t, cloned)
	})

	t.Run("multiple elements", func(t *testing.T) {
		t.Parallel()

		original := []string{"rule1", "rule2", "rule3"}
		cloned := CloneRules(original)

		require.Equal(t, original, cloned)

		cloned[0] = "modified"

		require.Equal(t, "rule1", original[0])
	})
}

func TestCloneTransactions(t *testing.T) {
	t.Parallel()

	t.Run("empty slice", func(t *testing.T) {
		t.Parallel()

		original := []int{}
		cloned := CloneTransactions(original)

		require.Empty(t, cloned)
	})

	t.Run("multiple elements", func(t *testing.T) {
		t.Parallel()

		original := []int{100, 200, 300}
		cloned := CloneTransactions(original)

		require.Equal(t, original, cloned)

		cloned[0] = 999

		require.Equal(t, 100, original[0])
	})
}

func TestPermuteRules(t *testing.T) {
	t.Parallel()

	t.Run("empty slice", func(t *testing.T) {
		t.Parallel()

		rng := rand.New(rand.NewSource(seedDefault))
		original := []string{}
		permuted := PermuteRules(original, rng)

		require.Empty(t, permuted)
	})

	t.Run("original unchanged", func(t *testing.T) {
		t.Parallel()

		rng := rand.New(rand.NewSource(seedDefault))
		original := []string{"a", "b", "c", "d"}
		originalCopy := Clone(original)

		_ = PermuteRules(original, rng)

		require.Equal(t, originalCopy, original)
	})
}

func TestPermuteTransactions(t *testing.T) {
	t.Parallel()

	t.Run("empty slice", func(t *testing.T) {
		t.Parallel()

		rng := rand.New(rand.NewSource(seedDefault))
		original := []int{}
		permuted := PermuteTransactions(original, rng)

		require.Empty(t, permuted)
	})

	t.Run("original unchanged", func(t *testing.T) {
		t.Parallel()

		rng := rand.New(rand.NewSource(seedDefault))
		original := []int{10, 20, 30, 40}
		originalCopy := Clone(original)

		_ = PermuteTransactions(original, rng)

		require.Equal(t, originalCopy, original)
	})
}

func TestFixedTime(t *testing.T) {
	t.Parallel()

	t.Run("returns deterministic time", func(t *testing.T) {
		t.Parallel()

		expected := time.Date(2026, 1, 1, 11, 0, 0, 0, time.UTC)
		result := FixedTime()

		require.Equal(t, expected, result)
	})

	t.Run("multiple calls return same time", func(t *testing.T) {
		t.Parallel()

		result1 := FixedTime()
		result2 := FixedTime()

		require.Equal(t, result1, result2)
	})

	t.Run("has correct components", func(t *testing.T) {
		t.Parallel()

		result := FixedTime()

		require.Equal(t, 2026, result.Year())
		require.Equal(t, time.January, result.Month())
		require.Equal(t, 1, result.Day())
		require.Equal(t, 11, result.Hour())
		require.Equal(t, 0, result.Minute())
		require.Equal(t, 0, result.Second())
		require.Equal(t, time.UTC, result.Location())
	})
}
