//go:build unit

package shared

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestParseIdempotencyKey(t *testing.T) {
	t.Parallel()

	t.Run("trims surrounding whitespace", func(t *testing.T) {
		t.Parallel()

		key, err := ParseIdempotencyKey(" callback-123 ")
		require.NoError(t, err)
		assert.Equal(t, IdempotencyKey("callback-123"), key)
	})

	t.Run("rejects empty", func(t *testing.T) {
		t.Parallel()

		_, err := ParseIdempotencyKey("")
		require.ErrorIs(t, err, ErrEmptyIdempotencyKey)
	})

	t.Run("rejects invalid format", func(t *testing.T) {
		t.Parallel()

		_, err := ParseIdempotencyKey("invalid key!")
		require.ErrorIs(t, err, ErrInvalidIdempotencyKey)
	})
}

func TestIdempotencyKeySignAndCompare(t *testing.T) {
	t.Parallel()

	key := IdempotencyKey("callback-123")
	signed := key.SignKey("secret")

	assert.True(t, CompareSignedKey(signed, key, "secret"))
	assert.False(t, CompareSignedKey(signed, key, "wrong-secret"))
	assert.NotEmpty(t, signed)
	assert.NotEqual(t, key.String(), signed)
}

func TestIdempotencyStatusIsValid(t *testing.T) {
	t.Parallel()

	assert.True(t, IdempotencyStatusUnknown.IsValid())
	assert.True(t, IdempotencyStatusPending.IsValid())
	assert.True(t, IdempotencyStatusComplete.IsValid())
	assert.True(t, IdempotencyStatusFailed.IsValid())
	assert.False(t, IdempotencyStatus("nope").IsValid())
}
