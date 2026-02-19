//go:build unit

package value_objects

import (
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestIdempotencyKey_IsValid(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name  string
		key   IdempotencyKey
		valid bool
	}{
		{"valid simple", IdempotencyKey("abc-123"), true},
		{"valid with colons", IdempotencyKey("jira:MATCH-123:callback"), true},
		{"valid with underscores", IdempotencyKey("webhook_delivery_456"), true},
		{"valid max length", IdempotencyKey(strings.Repeat("a", 128)), true},
		{"empty", IdempotencyKey(""), false},
		{"too long", IdempotencyKey(strings.Repeat("a", 129)), false},
		{"invalid chars spaces", IdempotencyKey("has spaces"), false},
		{"invalid chars special", IdempotencyKey("has@special"), false},
		{"invalid chars dot", IdempotencyKey("has.dot"), false},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			require.Equal(t, tt.valid, tt.key.IsValid())
		})
	}
}

func TestIdempotencyKey_String(t *testing.T) {
	t.Parallel()

	key := IdempotencyKey("callback-123")
	require.Equal(t, "callback-123", key.String())
}

func TestParseIdempotencyKey(t *testing.T) {
	t.Parallel()

	t.Run("valid key with whitespace trimming", func(t *testing.T) {
		t.Parallel()

		key, err := ParseIdempotencyKey(" callback-123 ")
		require.NoError(t, err)
		require.Equal(t, IdempotencyKey("callback-123"), key)
	})

	t.Run("empty key", func(t *testing.T) {
		t.Parallel()

		_, err := ParseIdempotencyKey("")
		require.ErrorIs(t, err, ErrEmptyIdempotencyKey)
	})

	t.Run("whitespace only", func(t *testing.T) {
		t.Parallel()

		_, err := ParseIdempotencyKey("   ")
		require.ErrorIs(t, err, ErrEmptyIdempotencyKey)
	})

	t.Run("invalid format", func(t *testing.T) {
		t.Parallel()

		_, err := ParseIdempotencyKey("invalid key!")
		require.ErrorIs(t, err, ErrInvalidIdempotencyKey)
	})
}

func TestIdempotencyKey_SignKey(t *testing.T) {
	t.Parallel()

	t.Run("empty secret returns original key", func(t *testing.T) {
		t.Parallel()

		key := IdempotencyKey("jira:MATCH-123:callback")
		signed := key.SignKey("")
		require.Equal(t, "jira:MATCH-123:callback", signed)
	})

	t.Run("non-empty secret returns hex-encoded HMAC", func(t *testing.T) {
		t.Parallel()

		key := IdempotencyKey("jira:MATCH-123:callback")
		signed := key.SignKey("my-secret-key")

		// Should be a 64-char hex string (SHA-256 produces 32 bytes = 64 hex chars)
		require.Len(t, signed, 64)
		require.NotEqual(t, "jira:MATCH-123:callback", signed)
	})

	t.Run("same key and secret produce same signature", func(t *testing.T) {
		t.Parallel()

		key := IdempotencyKey("webhook:DELIVERY-456:callback")
		secret := "consistent-secret"
		signed1 := key.SignKey(secret)
		signed2 := key.SignKey(secret)
		require.Equal(t, signed1, signed2)
	})

	t.Run("different keys produce different signatures", func(t *testing.T) {
		t.Parallel()

		secret := "shared-secret"
		key1 := IdempotencyKey("jira:MATCH-123:callback")
		key2 := IdempotencyKey("jira:MATCH-456:callback")
		signed1 := key1.SignKey(secret)
		signed2 := key2.SignKey(secret)
		require.NotEqual(t, signed1, signed2)
	})

	t.Run("different secrets produce different signatures", func(t *testing.T) {
		t.Parallel()

		key := IdempotencyKey("webhook:DELIVERY-789:callback")
		signed1 := key.SignKey("secret-A")
		signed2 := key.SignKey("secret-B")
		require.NotEqual(t, signed1, signed2)
	})
}

func TestCompareSignedKey(t *testing.T) {
	t.Parallel()

	t.Run("returns true for matching signature", func(t *testing.T) {
		t.Parallel()

		key := IdempotencyKey("jira:MATCH-123:callback")
		secret := "verification-secret"

		signed := key.SignKey(secret)
		assert.True(t, CompareSignedKey(signed, key, secret))
	})

	t.Run("returns false for wrong client key", func(t *testing.T) {
		t.Parallel()

		secret := "test-secret"
		signed := IdempotencyKey("key-A").SignKey(secret)
		assert.False(t, CompareSignedKey(signed, IdempotencyKey("key-B"), secret))
	})

	t.Run("returns false for wrong secret", func(t *testing.T) {
		t.Parallel()

		key := IdempotencyKey("webhook:DELIVERY-456:callback")
		signed := key.SignKey("secret-A")
		assert.False(t, CompareSignedKey(signed, key, "secret-B"))
	})

	t.Run("returns true when secret is empty and key matches", func(t *testing.T) {
		t.Parallel()

		key := IdempotencyKey("webhook:DELIVERY-456:callback")
		signed := key.SignKey("")
		assert.True(t, CompareSignedKey(signed, key, ""))
	})

	t.Run("returns false for tampered signed value", func(t *testing.T) {
		t.Parallel()

		key := IdempotencyKey("jira:MATCH-789:callback")
		assert.False(t, CompareSignedKey("tampered-value", key, "my-secret"))
	})
}
