//go:build unit

// Copyright 2025 Lerian Studio. All rights reserved.
// Use of this source code is governed by an Elastic License 2.0
// that can be found in the LICENSE.md file.

package bootstrap

import (
	"os"
	"reflect"
	"testing"

	"github.com/LerianStudio/lib-commons/v4/commons/systemplane/domain"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// --- restoreZeroedFields ---

func TestRestoreZeroedFields_NilDst(t *testing.T) {
	t.Parallel()

	snapshot := &Config{App: AppConfig{EnvName: "production"}}

	// Must not panic with nil dst.
	assert.NotPanics(t, func() {
		restoreZeroedFields(nil, snapshot)
	})
}

func TestRestoreZeroedFields_NilSnapshot(t *testing.T) {
	t.Parallel()

	dst := &Config{App: AppConfig{EnvName: "staging"}}

	// Must not panic with nil snapshot.
	assert.NotPanics(t, func() {
		restoreZeroedFields(dst, nil)
	})

	assert.Equal(t, "staging", dst.App.EnvName)
}

func TestRestoreZeroedFields_BothNil(t *testing.T) {
	t.Parallel()

	assert.NotPanics(t, func() {
		restoreZeroedFields(nil, nil)
	})
}

func TestRestoreZeroedFields_RestoresZeroToNonZero(t *testing.T) {
	t.Parallel()

	dst := &Config{App: AppConfig{EnvName: "", LogLevel: "debug"}}
	snapshot := &Config{App: AppConfig{EnvName: "production", LogLevel: "info"}}

	restoreZeroedFields(dst, snapshot)

	// EnvName was zero in dst, non-zero in snapshot → restored.
	assert.Equal(t, "production", dst.App.EnvName)
	// LogLevel was non-zero in dst → not overwritten.
	assert.Equal(t, "debug", dst.App.LogLevel)
}

func TestRestoreZeroedFields_PreservesNonZeroField(t *testing.T) {
	t.Parallel()

	dst := &Config{App: AppConfig{EnvName: "development"}}
	snapshot := &Config{App: AppConfig{EnvName: "production"}}

	restoreZeroedFields(dst, snapshot)

	assert.Equal(t, "development", dst.App.EnvName)
}

func TestRestoreZeroedFields_RestoresNestedStructField(t *testing.T) {
	t.Parallel()

	dst := &Config{
		App:    AppConfig{EnvName: "development"},
		Server: ServerConfig{Address: ""},
	}
	snapshot := &Config{
		App:    AppConfig{EnvName: "production"},
		Server: ServerConfig{Address: ":4018"},
	}

	restoreZeroedFields(dst, snapshot)

	// Server.Address was zero in dst, non-zero in snapshot → restored.
	assert.Equal(t, ":4018", dst.Server.Address)
	// App.EnvName was non-zero in dst → not overwritten.
	assert.Equal(t, "development", dst.App.EnvName)
}

// --- resolveConfigValue ---

func TestResolveConfigValue_NilConfig(t *testing.T) {
	t.Parallel()

	val, ok := resolveConfigValue(nil, "app.env_name")

	assert.False(t, ok)
	assert.Nil(t, val)
}

func TestResolveConfigValue_EmptyKey(t *testing.T) {
	t.Parallel()

	cfg := &Config{App: AppConfig{EnvName: "production"}}

	val, ok := resolveConfigValue(cfg, "")

	assert.False(t, ok)
	assert.Nil(t, val)
}

func TestResolveConfigValue_WhitespaceKey(t *testing.T) {
	t.Parallel()

	cfg := &Config{App: AppConfig{EnvName: "production"}}

	val, ok := resolveConfigValue(cfg, "   ")

	assert.False(t, ok)
	assert.Nil(t, val)
}

func TestResolveConfigValue_ValidTopLevelField(t *testing.T) {
	t.Parallel()

	cfg := &Config{App: AppConfig{EnvName: "staging"}}

	val, ok := resolveConfigValue(cfg, "app.env_name")

	require.True(t, ok)
	assert.Equal(t, "staging", val)
}

func TestResolveConfigValue_NestedField(t *testing.T) {
	t.Parallel()

	cfg := &Config{Server: ServerConfig{Address: ":8080"}}

	val, ok := resolveConfigValue(cfg, "server.address")

	require.True(t, ok)
	assert.Equal(t, ":8080", val)
}

func TestResolveConfigValue_NonExistentKey(t *testing.T) {
	t.Parallel()

	cfg := &Config{}

	val, ok := resolveConfigValue(cfg, "nonexistent.field")

	assert.False(t, ok)
	assert.Nil(t, val)
}

func TestResolveConfigValue_StructField(t *testing.T) {
	t.Parallel()

	cfg := &Config{App: AppConfig{EnvName: "production"}}

	// "app" is a struct — resolveConfigValue returns it directly.
	val, ok := resolveConfigValue(cfg, "app")

	require.True(t, ok)
	appCfg, isAppConfig := val.(AppConfig)
	require.True(t, isAppConfig)
	assert.Equal(t, "production", appCfg.EnvName)
}

func TestResolveConfigValue_BodyLimitBytes(t *testing.T) {
	t.Parallel()

	cfg := &Config{Server: ServerConfig{BodyLimitBytes: 4096}}

	val, ok := resolveConfigValue(cfg, "server.body_limit_bytes")

	require.True(t, ok)
	assert.Equal(t, 4096, val)
}

func TestResolveConfigValue_RenamedCorsKeyAlias(t *testing.T) {
	t.Parallel()

	cfg := &Config{Server: ServerConfig{CORSAllowedOrigins: "https://app.example.com"}}

	val, ok := resolveConfigValue(cfg, "cors.allowed_origins")

	require.True(t, ok)
	assert.Equal(t, "https://app.example.com", val)
}

func TestResolveConfigValue_RenamedRuntimeKeyAliases(t *testing.T) {
	t.Parallel()

	cfg := &Config{
		Postgres: PostgresConfig{
			MaxOpenConnections: 41,
			MaxIdleConnections: 9,
		},
		Redis:    RedisConfig{MinIdleConn: 4},
		RabbitMQ: RabbitMQConfig{URI: "amqps"},
	}

	tests := []struct {
		name string
		key  string
		want any
	}{
		{name: "postgres max open conns", key: "postgres.max_open_conns", want: 41},
		{name: "postgres max idle conns", key: "postgres.max_idle_conns", want: 9},
		{name: "redis min idle conns", key: "redis.min_idle_conns", want: 4},
		{name: "rabbitmq url", key: "rabbitmq.url", want: "amqps"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			val, ok := resolveConfigValue(cfg, tt.key)

			require.True(t, ok)
			assert.Equal(t, tt.want, val)
		})
	}
}

func TestResolveSnapshotConfigValue_PrefersSettingsForSettingKeys(t *testing.T) {
	t.Parallel()

	snap := domain.Snapshot{
		Configs: map[string]domain.EffectiveValue{
			"rate_limit.max": {Key: "rate_limit.max", Value: 999},
		},
		GlobalSettings: map[string]domain.EffectiveValue{
			"rate_limit.max": {Key: "rate_limit.max", Value: 100},
		},
	}

	val, ok := resolveSnapshotConfigValue(snap, "rate_limit.max")

	require.True(t, ok)
	assert.Equal(t, 100, val)
}

func TestResolveSnapshotConfigValue_SettingKeysFallBackToConfigsForUpgradeCompatibility(t *testing.T) {
	t.Parallel()

	snap := domain.Snapshot{
		Configs: map[string]domain.EffectiveValue{
			"rate_limit.max": {Key: "rate_limit.max", Value: 999},
		},
	}

	val, ok := resolveSnapshotConfigValue(snap, "rate_limit.max")

	require.True(t, ok)
	assert.Equal(t, 999, val)
}

func TestResolveSnapshotConfigValue_PrefersConfigsForConfigKeys(t *testing.T) {
	t.Parallel()

	snap := domain.Snapshot{
		Configs: map[string]domain.EffectiveValue{
			"app.log_level": {Key: "app.log_level", Value: "debug"},
		},
		GlobalSettings: map[string]domain.EffectiveValue{
			"app.log_level": {Key: "app.log_level", Value: "info"},
		},
	}

	val, ok := resolveSnapshotConfigValue(snap, "app.log_level")

	require.True(t, ok)
	assert.Equal(t, "debug", val)
}

// --- derefPointerValue ---

func TestDerefPointerValue_NonPointer(t *testing.T) {
	t.Parallel()

	cfg := Config{App: AppConfig{EnvName: "test"}}
	result, ok := derefPointerValue(reflect.ValueOf(cfg))

	assert.True(t, ok)
	assert.True(t, result.IsValid())
}

func TestDerefPointerValue_ValidPointer(t *testing.T) {
	t.Parallel()

	cfg := &Config{App: AppConfig{EnvName: "test"}}
	result, ok := derefPointerValue(reflect.ValueOf(cfg))

	assert.True(t, ok)
	assert.True(t, result.IsValid())
}

func TestDerefPointerValue_NilPointer(t *testing.T) {
	t.Parallel()

	var cfg *Config
	result, ok := derefPointerValue(reflect.ValueOf(cfg))

	assert.False(t, ok)
	assert.False(t, result.IsValid())
}

// --- findMapstructureField ---

func TestFindMapstructureField_Found(t *testing.T) {
	t.Parallel()

	cfg := Config{App: AppConfig{EnvName: "prod"}}
	val := reflect.ValueOf(cfg)

	field, found := findMapstructureField(val, "app")

	assert.True(t, found)
	assert.True(t, field.IsValid())
}

func TestFindMapstructureField_NotFound(t *testing.T) {
	t.Parallel()

	cfg := Config{}
	val := reflect.ValueOf(cfg)

	_, found := findMapstructureField(val, "nonexistent_tag")

	assert.False(t, found)
}

func TestFindMapstructureField_NestedStruct(t *testing.T) {
	t.Parallel()

	cfg := Config{Server: ServerConfig{Address: ":8080"}}
	val := reflect.ValueOf(cfg)

	field, found := findMapstructureField(val, "server")

	require.True(t, found)
	assert.True(t, field.IsValid())
}

// --- hasExplicitEnvOverride ---

func TestHasExplicitEnvOverride_NoEnvTag(t *testing.T) {
	t.Parallel()

	// Use a struct field with no `env` tag.
	type testStruct struct {
		Field string `mapstructure:"field"`
	}

	fieldType := reflect.TypeOf(testStruct{}).Field(0)

	result := hasExplicitEnvOverride(fieldType)

	assert.False(t, result)
}

func TestHasExplicitEnvOverride_EmptyEnvTag(t *testing.T) {
	t.Parallel()

	type testStruct struct {
		Field string `env:"" mapstructure:"field"`
	}

	fieldType := reflect.TypeOf(testStruct{}).Field(0)

	result := hasExplicitEnvOverride(fieldType)

	assert.False(t, result)
}

func TestOverlayExplicitEnvOverrides_OnlyCopiesEnvControlledFields(t *testing.T) {
	t.Setenv("WEBHOOK_TIMEOUT_SEC", "45")

	dst := defaultConfig()
	dst.Webhook.TimeoutSec = 15
	dst.RateLimit.Max = 100

	source := defaultConfig()
	source.Webhook.TimeoutSec = 45
	source.RateLimit.Max = 999

	overlayExplicitEnvOverrides(dst, source)

	assert.Equal(t, 45, dst.Webhook.TimeoutSec)
	assert.Equal(t, 100, dst.RateLimit.Max)
}

func TestResolveConfigEnvVar_AliasKeyReturnsCanonicalEnvVar(t *testing.T) {
	t.Parallel()

	envVar, ok := resolveConfigEnvVar("cors.allowed_origins")

	require.True(t, ok)
	assert.Equal(t, "CORS_ALLOWED_ORIGINS", envVar)
}

func TestResolveConfigEnvVar_UnknownKeyReturnsFalse(t *testing.T) {
	t.Parallel()

	envVar, ok := resolveConfigEnvVar("unknown.key")

	assert.False(t, ok)
	assert.Empty(t, envVar)
}

func TestHasExplicitEnvOverrideForKey_UsesAliasMapping(t *testing.T) {
	t.Setenv("CORS_ALLOWED_ORIGINS", "https://app.example.com")

	assert.True(t, hasExplicitEnvOverrideForKey("cors.allowed_origins"))
	assert.False(t, hasExplicitEnvOverrideForKey("rate_limit.max"))
}

func TestHasExplicitEnvOverride_ReadsProcessEnvironment(t *testing.T) {
	t.Setenv("WEBHOOK_TIMEOUT_SEC", "45")

	field, ok := reflect.TypeOf(Config{}).FieldByName("Webhook")
	require.True(t, ok)

	timeoutField, found := field.Type.FieldByName("TimeoutSec")
	require.True(t, found)

	assert.True(t, hasExplicitEnvOverride(timeoutField))
	require.NoError(t, os.Unsetenv("WEBHOOK_TIMEOUT_SEC"))
	assert.False(t, hasExplicitEnvOverride(timeoutField))
}
