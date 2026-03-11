// Copyright 2025 Lerian Studio. All rights reserved.
// Use of this source code is governed by an Elastic License 2.0
// that can be found in the LICENSE.md file.

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
	require.Len(t, changes, 1)
	assert.Equal(t, "app.log_level", changes[0].Key)
	assert.Equal(t, "info", changes[0].OldValue)
	assert.Equal(t, "debug", changes[0].NewValue)
}

func TestDiffConfigsRedact_DetectsMultipleChanges(t *testing.T) {
	t.Parallel()

	oldCfg := defaultConfig()
	newCfg := defaultConfig()
	newCfg.App.LogLevel = "error"
	newCfg.Server.Address = ":9090"

	changes := diffConfigs(oldCfg, newCfg)
	require.Len(t, changes, 2) //nolint:mnd // exactly app.log_level + server.address

	keys := make(map[string]bool, len(changes))
	for _, c := range changes {
		keys[c.Key] = true
	}

	assert.True(t, keys["app.log_level"], "should detect app.log_level change")
	assert.True(t, keys["server.address"], "should detect server.address change")
}

func TestDiffConfigsRedact_FieldLevelNestedStruct(t *testing.T) {
	t.Parallel()

	oldCfg := defaultConfig()
	newCfg := defaultConfig()

	// Change a single leaf field inside a nested struct.
	newCfg.RateLimit.Max = 999

	changes := diffConfigs(oldCfg, newCfg)
	require.Len(t, changes, 1, "should detect exactly one field-level change")
	assert.Equal(t, "rate_limit.max", changes[0].Key)
	assert.Equal(t, 100, changes[0].OldValue)
	assert.Equal(t, 999, changes[0].NewValue)
}

func TestDiffConfigsRedact_FieldLevelSecretRedaction(t *testing.T) {
	t.Parallel()

	oldCfg := defaultConfig()
	newCfg := defaultConfig()
	newCfg.Postgres.PrimaryPassword = "new-secret"

	changes := diffConfigs(oldCfg, newCfg)
	require.Len(t, changes, 1)
	assert.Equal(t, "postgres.primary_password", changes[0].Key)
	assert.Equal(t, "***REDACTED***", changes[0].OldValue)
	assert.Equal(t, "***REDACTED***", changes[0].NewValue)
}

func TestDiffConfigsRedact_MultipleFieldsSameSubStruct(t *testing.T) {
	t.Parallel()

	oldCfg := defaultConfig()
	newCfg := defaultConfig()
	newCfg.RateLimit.Max = 200
	newCfg.RateLimit.ExpirySec = 120

	changes := diffConfigs(oldCfg, newCfg)
	require.Len(t, changes, 2) //nolint:mnd // exactly rate_limit.max + rate_limit.expiry_sec

	keys := make(map[string]bool, len(changes))
	for _, c := range changes {
		keys[c.Key] = true
	}

	assert.True(t, keys["rate_limit.max"], "should detect rate_limit.max change")
	assert.True(t, keys["rate_limit.expiry_sec"], "should detect rate_limit.expiry_sec change")
}

func TestIsSensitiveKeyTable(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name      string
		key       string
		sensitive bool
	}{
		{name: "postgres_password", key: "postgres.primary_password", sensitive: true},
		{name: "auth_token_secret", key: "auth.token_secret", sensitive: true},
		{name: "object_storage_access_key", key: "object_storage.access_key_id", sensitive: true},
		{name: "rabbitmq_uri_not_sensitive", key: "rabbitmq.uri", sensitive: false},
		{name: "tls_cert_file_not_sensitive", key: "server.tls_cert_file", sensitive: false},
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

		result := redactIfSensitive("postgres.primary_password", "my-secret")
		assert.Equal(t, "***REDACTED***", result)
	})

	t.Run("passes_through_normal_key", func(t *testing.T) {
		t.Parallel()

		result := redactIfSensitive("log_level", "info")
		assert.Equal(t, "info", result)
	})

	t.Run("redacts_token_key", func(t *testing.T) {
		t.Parallel()

		result := redactIfSensitive("auth.token_secret", "abc123")
		assert.Equal(t, "***REDACTED***", result)
	})

	t.Run("redacts_uri_credentials_without_masking_field", func(t *testing.T) {
		t.Parallel()

		result := redactIfSensitive("rabbitmq.uri", "amqp://user:pass@localhost:5672/")
		assert.Equal(t, "amqp://%2A%2A%2AREDACTED%2A%2A%2A:%2A%2A%2AREDACTED%2A%2A%2A@localhost:5672/", result)
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
	// Not parallel: clearConfigEnvVars manipulates process env.
	clearConfigEnvVars(t)

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

// --- Test #34: redactCredentialURI tests ---

func TestRedactCredentialURI(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name  string
		input any
		want  any
	}{
		{
			// url.URL.String() percent-encodes '*' in userinfo as %2A.
			name:  "uri_with_user_password",
			input: "postgres://admin:s3cret@db.host:5432/mydb",
			want:  "postgres://%2A%2A%2AREDACTED%2A%2A%2A:%2A%2A%2AREDACTED%2A%2A%2A@db.host:5432/mydb",
		},
		{
			name:  "uri_with_user_only",
			input: "postgres://admin@db.host:5432/mydb",
			want:  "postgres://%2A%2A%2AREDACTED%2A%2A%2A@db.host:5432/mydb",
		},
		{
			name:  "plain_string_not_uri",
			input: "just a plain string",
			want:  "just a plain string",
		},
		{
			name:  "empty_string",
			input: "",
			want:  "",
		},
		{
			name:  "non_string_int",
			input: 42,
			want:  42,
		},
		{
			name:  "uri_without_userinfo",
			input: "https://example.com/path?q=1",
			want:  "https://example.com/path?q=1",
		},
		{
			name:  "malformed_uri_no_scheme",
			input: "not-a-uri-at-all",
			want:  "not-a-uri-at-all",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			got := redactCredentialURI(tt.input)
			assert.Equal(t, tt.want, got)
		})
	}
}

func TestRestoreZeroedFieldsRedact_PreservesExplicitEnvZeroValues(t *testing.T) {
	snapshot := defaultConfig()
	snapshot.ObjectStorage.Endpoint = "http://storage.internal"
	snapshot.RateLimit.Max = 500
	snapshot.Auth.Enabled = true

	dst := defaultConfig()
	dst.ObjectStorage.Endpoint = ""
	dst.RateLimit.Max = 0
	dst.Auth.Enabled = false

	t.Setenv("OBJECT_STORAGE_ENDPOINT", "")
	t.Setenv("RATE_LIMIT_MAX", "0")
	t.Setenv("AUTH_ENABLED", "false")

	restoreZeroedFields(dst, snapshot)

	assert.Empty(t, dst.ObjectStorage.Endpoint)
	assert.Zero(t, dst.RateLimit.Max)
	assert.False(t, dst.Auth.Enabled)
}
