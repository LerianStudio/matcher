// Copyright 2025 Lerian Studio. All rights reserved.
// Use of this source code is governed by an Elastic License 2.0
// that can be found in the LICENSE.md file.

//go:build unit

package bootstrap

import (
	"errors"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestValidateSystemplaneSecrets_MissingMasterKeyInProd asserts that a production
// deployment without SYSTEMPLANE_SECRET_MASTER_KEY is rejected. This is a critical
// security guardrail: running without a master key would leave systemplane secret
// payloads unencrypted.
func TestValidateSystemplaneSecrets_MissingMasterKeyInProd(t *testing.T) {
	// Cannot be parallel: t.Setenv mutates process env.
	t.Setenv("SYSTEMPLANE_SECRET_MASTER_KEY", "")

	err := ValidateSystemplaneSecrets("production")

	require.Error(t, err)
	assert.True(t, errors.Is(err, errSystemplaneSecretMasterKey),
		"expected errSystemplaneSecretMasterKey, got: %v", err)
}

// TestValidateSystemplaneSecrets_DevDefaultInProd asserts the well-known
// development default key (committed in docker-compose.yml) is rejected in
// production to prevent accidental deployment with a publicly-known key.
func TestValidateSystemplaneSecrets_DevDefaultInProd(t *testing.T) {
	t.Setenv("SYSTEMPLANE_SECRET_MASTER_KEY", wellKnownDevMasterKey)

	err := ValidateSystemplaneSecrets("production")

	require.Error(t, err)
	assert.True(t, errors.Is(err, errSystemplaneDevMasterKeyInProd),
		"expected errSystemplaneDevMasterKeyInProd, got: %v", err)
}

// TestValidateSystemplaneSecrets_ValidKeyInProd asserts a non-default key in
// production passes validation.
func TestValidateSystemplaneSecrets_ValidKeyInProd(t *testing.T) {
	// Random 32-byte base64 key (not the dev default).
	t.Setenv("SYSTEMPLANE_SECRET_MASTER_KEY", "AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA=")

	err := ValidateSystemplaneSecrets("production")

	assert.NoError(t, err)
}

// TestValidateSystemplaneSecrets_NonProdSkipsDevKeyCheck asserts that the dev
// default is explicitly allowed outside production. Non-prod still requires
// *some* key (empty is rejected), but the dev default is fine.
func TestValidateSystemplaneSecrets_NonProdSkipsDevKeyCheck(t *testing.T) {
	t.Setenv("SYSTEMPLANE_SECRET_MASTER_KEY", wellKnownDevMasterKey)

	err := ValidateSystemplaneSecrets("development")

	assert.NoError(t, err, "dev default must be allowed in non-production")
}

// TestValidateSystemplaneSecrets_NonProdMissingKey asserts that non-production
// still requires *some* master key — the empty-key check is universal.
func TestValidateSystemplaneSecrets_NonProdMissingKey(t *testing.T) {
	t.Setenv("SYSTEMPLANE_SECRET_MASTER_KEY", "")

	err := ValidateSystemplaneSecrets("development")

	require.Error(t, err)
	assert.True(t, errors.Is(err, errSystemplaneSecretMasterKey))
}

// TestSystemplaneGetString_NilClient asserts nil-client returns the fallback
// without panicking.
func TestSystemplaneGetString_NilClient(t *testing.T) {
	t.Parallel()

	got := SystemplaneGetString(nil, "some.key", "fallback-value")

	assert.Equal(t, "fallback-value", got)
}

// TestSystemplaneGetInt_NilClient asserts nil-client returns the fallback
// without panicking.
func TestSystemplaneGetInt_NilClient(t *testing.T) {
	t.Parallel()

	got := SystemplaneGetInt(nil, "some.key", 42)

	assert.Equal(t, 42, got)
}

// TestSystemplaneGetBool_NilClient asserts nil-client returns the fallback
// without panicking.
func TestSystemplaneGetBool_NilClient(t *testing.T) {
	t.Parallel()

	got := SystemplaneGetBool(nil, "some.key", true)

	assert.True(t, got)
}
