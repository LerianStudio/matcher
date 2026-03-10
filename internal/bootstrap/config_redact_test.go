//go:build unit

package bootstrap

import (
	"math"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// Tests in this file complement coverage from config_manager_test.go by testing
// redact functions in isolation with table-driven patterns and edge cases.

func TestDiffConfigsRedact_BothNilReturnsEmpty(t *testing.T) {
	t.Parallel()

	changes := diffConfigs(nil, nil)
	assert.Empty(t, changes)
}

func TestDiffConfigsRedact_OldNilReturnsEmpty(t *testing.T) {
	t.Parallel()

	changes := diffConfigs(nil, defaultConfig())
	assert.Empty(t, changes)
}

func TestDiffConfigsRedact_NewNilReturnsEmpty(t *testing.T) {
	t.Parallel()

	changes := diffConfigs(defaultConfig(), nil)
	assert.Empty(t, changes)
}

func TestDiffConfigsRedact_IdenticalReturnsEmpty(t *testing.T) {
	t.Parallel()

	cfg := defaultConfig()
	changes := diffConfigs(cfg, cfg)
	assert.Empty(t, changes)
}

func TestDiffConfigsRedact_DetectsAppChange(t *testing.T) {
	t.Parallel()

	oldCfg := defaultConfig()
	newCfg := defaultConfig()
	newCfg.App.LogLevel = "debug"

	changes := diffConfigs(oldCfg, newCfg)
	require.NotEmpty(t, changes)

	found := false
	for _, c := range changes {
		if c.Key == "app" {
			found = true

			break
		}
	}

	assert.True(t, found, "should detect app config change")
}

func TestDiffConfigsRedact_DetectsMultipleChanges(t *testing.T) {
	t.Parallel()

	oldCfg := defaultConfig()
	newCfg := defaultConfig()
	newCfg.App.LogLevel = "error"
	newCfg.Server.Address = ":9090"

	changes := diffConfigs(oldCfg, newCfg)
	assert.GreaterOrEqual(t, len(changes), 2) //nolint:mnd // expecting at least app + server changes
}

func TestIsSensitiveKeyTable(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name      string
		key       string
		sensitive bool
	}{
		{name: "password_lowercase", key: "password", sensitive: true},
		{name: "password_mixed_case", key: "PrimaryPassword", sensitive: true},
		{name: "secret_key", key: "jwt_secret", sensitive: true},
		{name: "token_key", key: "auth_token", sensitive: true},
		{name: "key_fragment", key: "access_key_id", sensitive: true},
		{name: "cert_fragment", key: "tls_cert_file", sensitive: true},
		{name: "host_not_sensitive", key: "primary_host", sensitive: false},
		{name: "port_not_sensitive", key: "port", sensitive: false},
		{name: "log_level_not_sensitive", key: "log_level", sensitive: false},
		{name: "empty_string", key: "", sensitive: false},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			assert.Equal(t, tt.sensitive, isSensitiveKey(tt.key))
		})
	}
}

func TestRedactIfSensitiveTable(t *testing.T) {
	t.Parallel()

	t.Run("redacts_sensitive_key", func(t *testing.T) {
		t.Parallel()

		result := redactIfSensitive("password", "my-secret")
		assert.Equal(t, "***REDACTED***", result)
	})

	t.Run("passes_through_normal_key", func(t *testing.T) {
		t.Parallel()

		result := redactIfSensitive("log_level", "info")
		assert.Equal(t, "info", result)
	})

	t.Run("redacts_token_key", func(t *testing.T) {
		t.Parallel()

		result := redactIfSensitive("auth_token", "abc123")
		assert.Equal(t, "***REDACTED***", result)
	})
}

func TestSafeUint64ToIntTable(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name     string
		input    uint64
		expected int
	}{
		{name: "zero", input: 0, expected: 0},
		{name: "normal_value", input: 42, expected: 42},
		{name: "max_int_boundary", input: uint64(math.MaxInt), expected: math.MaxInt},
		{name: "exceeds_max_int", input: uint64(math.MaxInt) + 1, expected: math.MaxInt},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			assert.Equal(t, tt.expected, safeUint64ToInt(tt.input))
		})
	}
}

func TestRestoreZeroedFieldsRedact_NilInputs(t *testing.T) {
	t.Parallel()

	// Should not panic with nil inputs.
	restoreZeroedFields(nil, nil)
	restoreZeroedFields(nil, defaultConfig())
	restoreZeroedFields(defaultConfig(), nil)
}

func TestRestoreZeroedFieldsRedact_RestoresZeroed(t *testing.T) {
	t.Parallel()

	snapshot := defaultConfig()
	snapshot.App.LogLevel = "warn"
	snapshot.Server.Address = ":9090"

	dst := defaultConfig()
	dst.App.LogLevel = ""   // zeroed — should be restored
	dst.Server.Address = "" // zeroed — should be restored

	restoreZeroedFields(dst, snapshot)

	assert.Equal(t, "warn", dst.App.LogLevel)
	assert.Equal(t, ":9090", dst.Server.Address)
}

func TestRestoreZeroedFieldsRedact_PreservesExisting(t *testing.T) {
	t.Parallel()

	snapshot := defaultConfig()
	snapshot.App.LogLevel = "info"

	dst := defaultConfig()
	dst.App.LogLevel = "debug" // non-zero — should NOT be overwritten

	restoreZeroedFields(dst, snapshot)

	assert.Equal(t, "debug", dst.App.LogLevel)
}
