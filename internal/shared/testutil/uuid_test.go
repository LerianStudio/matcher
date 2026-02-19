//go:build unit

package testutil

import (
	"regexp"
	"testing"

	"github.com/stretchr/testify/require"
)

var uuidRegex = regexp.MustCompile(`^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$`)

func TestMustDeterministicUUID(t *testing.T) {
	t.Parallel()

	t.Run("same seed produces same UUID", func(t *testing.T) {
		t.Parallel()

		seed := "test-seed-123"

		uuid1 := MustDeterministicUUID(seed)
		uuid2 := MustDeterministicUUID(seed)

		require.Equal(t, uuid1, uuid2)
	})

	t.Run("different seeds produce different UUIDs", func(t *testing.T) {
		t.Parallel()

		uuid1 := MustDeterministicUUID("seed-one")
		uuid2 := MustDeterministicUUID("seed-two")

		require.NotEqual(t, uuid1, uuid2)
	})

	t.Run("empty seed works", func(t *testing.T) {
		t.Parallel()

		uuid1 := MustDeterministicUUID("")
		uuid2 := MustDeterministicUUID("")

		require.Equal(t, uuid1, uuid2)
		require.NotEmpty(t, uuid1.String())
	})

	t.Run("long seed works", func(t *testing.T) {
		t.Parallel()

		longSeed := "this-is-a-very-long-seed-that-exceeds-the-typical-uuid-length-and-should-still-work-correctly-for-deterministic-uuid-generation"

		uuid1 := MustDeterministicUUID(longSeed)
		uuid2 := MustDeterministicUUID(longSeed)

		require.Equal(t, uuid1, uuid2)
		require.NotEmpty(t, uuid1.String())
	})

	t.Run("result is valid UUID format", func(t *testing.T) {
		t.Parallel()

		result := MustDeterministicUUID("test-seed")

		require.Regexp(t, uuidRegex, result.String())
	})

	t.Run("special characters in seed work", func(t *testing.T) {
		t.Parallel()

		seed := "special!@#$%^&*()_+-=[]{}|;':\",./<>?"

		uuid1 := MustDeterministicUUID(seed)
		uuid2 := MustDeterministicUUID(seed)

		require.Equal(t, uuid1, uuid2)
		require.NotEmpty(t, uuid1.String())
	})

	t.Run("unicode seed works", func(t *testing.T) {
		t.Parallel()

		seed := "テスト-日本語-🎉"

		uuid1 := MustDeterministicUUID(seed)
		uuid2 := MustDeterministicUUID(seed)

		require.Equal(t, uuid1, uuid2)
		require.NotEmpty(t, uuid1.String())
	})
}
