//go:build unit

// Copyright 2025 Lerian Studio. All rights reserved.
// Use of this source code is governed by an Elastic License 2.0
// that can be found in the LICENSE.md file.

package bootstrap

import (
	"reflect"
	"testing"

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
