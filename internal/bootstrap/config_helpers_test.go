//go:build unit

// Copyright 2025 Lerian Studio. All rights reserved.
// Use of this source code is governed by an Elastic License 2.0
// that can be found in the LICENSE.md file.

package bootstrap

import (
	"os"
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
