// Copyright 2025 Lerian Studio.

//go:build unit

package secretcodec

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/LerianStudio/matcher/pkg/systemplane/domain"
)

func TestCodec_EncryptDecrypt_RoundTrip(t *testing.T) {
	t.Parallel()

	codec, err := New("0123456789abcdef0123456789abcdef", []string{"postgres.primary_password"})
	require.NoError(t, err)

	target := domain.Target{Kind: domain.KindConfig, Scope: domain.ScopeGlobal}
	encrypted, err := codec.Encrypt(target, "postgres.primary_password", "super-secret")
	require.NoError(t, err)
	assert.IsType(t, map[string]any{}, normalizeEnvelopeForAssert(encrypted))

	decrypted, err := codec.Decrypt(target, "postgres.primary_password", normalizeEnvelopeForAssert(encrypted))
	require.NoError(t, err)
	assert.Equal(t, "super-secret", decrypted)
}

func TestCodec_Decrypt_LegacyPlaintextPassthrough(t *testing.T) {
	t.Parallel()

	codec, err := New("0123456789abcdef0123456789abcdef", []string{"redis.password"})
	require.NoError(t, err)

	target := domain.Target{Kind: domain.KindConfig, Scope: domain.ScopeGlobal}
	decrypted, err := codec.Decrypt(target, "redis.password", "legacy-plain")
	require.NoError(t, err)
	assert.Equal(t, "legacy-plain", decrypted)
}

func TestCodec_Decrypt_WrongAADFails(t *testing.T) {
	t.Parallel()

	codec, err := New("0123456789abcdef0123456789abcdef", []string{"rabbitmq.password"})
	require.NoError(t, err)

	encrypted, err := codec.Encrypt(domain.Target{Kind: domain.KindConfig, Scope: domain.ScopeGlobal}, "rabbitmq.password", "pw")
	require.NoError(t, err)

	_, err = codec.Decrypt(domain.Target{Kind: domain.KindConfig, Scope: domain.ScopeTenant, SubjectID: "tenant-a"}, "rabbitmq.password", normalizeEnvelopeForAssert(encrypted))
	require.Error(t, err)
	assert.ErrorIs(t, err, ErrDecryptFailed)
}

func TestCodec_Decrypt_InvalidEnvelopeFails(t *testing.T) {
	t.Parallel()

	codec, err := New("0123456789abcdef0123456789abcdef", []string{"rabbitmq.password"})
	require.NoError(t, err)

	_, err = codec.Decrypt(domain.Target{Kind: domain.KindConfig, Scope: domain.ScopeGlobal}, "rabbitmq.password", map[string]any{
		"__systemplane_secret_v": float64(1),
		"alg":                    "aes-256-gcm",
		"nonce":                  "%%%",
		"ciphertext":             "%%%",
	})
	require.Error(t, err)
	assert.ErrorIs(t, err, ErrInvalidEnvelope)
}

func TestCodec_New_RejectsWeakMasterKey(t *testing.T) {
	t.Parallel()

	codec, err := New("too-short", []string{"rabbitmq.password"})
	require.Error(t, err)
	assert.Nil(t, codec)
	assert.ErrorIs(t, err, ErrWeakMasterKey)
}

// ---------------------------------------------------------------------------
// HIGH-16: Secret round-trip — verify raw data is encrypted (not plaintext)
// ---------------------------------------------------------------------------

func TestCodec_EncryptProducesNonPlaintext(t *testing.T) {
	t.Parallel()

	codec, err := New("0123456789abcdef0123456789abcdef", []string{"db.password"})
	require.NoError(t, err)

	target := domain.Target{Kind: domain.KindConfig, Scope: domain.ScopeGlobal}
	original := "my-super-secret-password"

	encrypted, err := codec.Encrypt(target, "db.password", original)
	require.NoError(t, err)

	// The encrypted value should be an envelope struct, not the plaintext string.
	env, ok := encrypted.(envelope)
	require.True(t, ok, "encrypted value should be an envelope type")
	assert.Equal(t, 1, env.Version)
	assert.Equal(t, "aes-256-gcm", env.Algorithm)
	assert.NotEmpty(t, env.Nonce)
	assert.NotEmpty(t, env.Ciphertext)
	assert.NotContains(t, env.Ciphertext, original,
		"ciphertext should not contain the plaintext value")

	// Round-trip: decrypt and verify.
	normalized := normalizeEnvelopeForAssert(encrypted)
	decrypted, err := codec.Decrypt(target, "db.password", normalized)
	require.NoError(t, err)
	assert.Equal(t, original, decrypted)
}

func TestCodec_EncryptDecrypt_MultipleTypes(t *testing.T) {
	t.Parallel()

	codec, err := New("0123456789abcdef0123456789abcdef", []string{"secret.key"})
	require.NoError(t, err)

	target := domain.Target{Kind: domain.KindConfig, Scope: domain.ScopeGlobal}

	tests := []struct {
		name  string
		value any
	}{
		{"string value", "hello-world"},
		{"integer value", 42},
		{"boolean value", true},
		{"float value", 3.14},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			encrypted, err := codec.Encrypt(target, "secret.key", tt.value)
			require.NoError(t, err)

			// Verify it's actually an envelope (not plaintext).
			_, isEnvelope := encrypted.(envelope)
			require.True(t, isEnvelope, "encrypted value should be envelope")

			// Round-trip decrypt.
			normalized := normalizeEnvelopeForAssert(encrypted)
			decrypted, err := codec.Decrypt(target, "secret.key", normalized)
			require.NoError(t, err)
			assert.NotNil(t, decrypted)
		})
	}
}

func TestCodec_NonSecretKey_PassthroughNoEncryption(t *testing.T) {
	t.Parallel()

	codec, err := New("0123456789abcdef0123456789abcdef", []string{"db.password"})
	require.NoError(t, err)

	target := domain.Target{Kind: domain.KindConfig, Scope: domain.ScopeGlobal}

	// A key NOT in the secret list should pass through without encryption.
	encrypted, err := codec.Encrypt(target, "app.timeout", 30)
	require.NoError(t, err)
	assert.Equal(t, 30, encrypted, "non-secret key should not be encrypted")
}

func normalizeEnvelopeForAssert(value any) map[string]any {
	env, ok := value.(envelope)
	if !ok {
		return map[string]any{}
	}

	return map[string]any{
		"__systemplane_secret_v": float64(env.Version),
		"alg":                    env.Algorithm,
		"nonce":                  env.Nonce,
		"ciphertext":             env.Ciphertext,
	}
}
