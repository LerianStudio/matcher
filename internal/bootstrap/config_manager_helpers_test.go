// Copyright 2025 Lerian Studio. All rights reserved.
// Use of this source code is governed by an Elastic License 2.0
// that can be found in the LICENSE.md file.

//go:build unit

package bootstrap

import (
	"os"
	"path/filepath"
	"testing"

	libLog "github.com/LerianStudio/lib-commons/v4/commons/log"
	"github.com/spf13/viper"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// --- Test #26: writeViperConfigAtomically dedicated tests ---

// newWriteTestConfigManager creates a minimal ConfigManager with a real viper
// instance pointing at the given filePath. This is the minimum viable setup
// for exercising writeViperConfigAtomically without needing a full NewConfigManager.
func newWriteTestConfigManager(t *testing.T, filePath string) *ConfigManager {
	t.Helper()

	v := viper.New()
	v.Set("test.key", "value")

	if filePath != "" {
		v.SetConfigFile(filePath)
	}

	cm := &ConfigManager{
		viper:    v,
		filePath: filePath,
		logger:   &libLog.NopLogger{},
	}

	return cm
}

func TestWriteConfigAtomically_InvalidPath_EmptyString(t *testing.T) {
	t.Parallel()

	// filepath.Clean("") → ".", which fails the YAML extension check before
	// the empty-path check in validateAtomicWritePath.
	cm := newWriteTestConfigManager(t, "")

	err := writeViperConfigAtomically(cm.viper, cm.filePath)
	require.Error(t, err)
	assert.ErrorIs(t, err, errUnsafeConfigFileExtension)
}

func TestWriteConfigAtomically_InvalidPath_NullBytes(t *testing.T) {
	t.Parallel()

	cm := newWriteTestConfigManager(t, "config\x00evil.yaml")

	err := writeViperConfigAtomically(cm.viper, cm.filePath)
	require.Error(t, err)
	assert.ErrorIs(t, err, errUnsafeConfigFilePath)
}

func TestWriteConfigAtomically_PreservesPermissions(t *testing.T) {
	t.Parallel()

	dir := t.TempDir()
	filePath := filepath.Join(dir, "preserve_perms.yaml")

	// Create the file with restrictive permissions (0o640).
	const wantPerm os.FileMode = 0o640

	err := os.WriteFile(filePath, []byte("initial: true\n"), wantPerm)
	require.NoError(t, err)

	// Build a ConfigManager whose viper has a key to write.
	cm := newWriteTestConfigManager(t, filePath)
	cm.viper.SetConfigFile(filePath)
	cm.viper.Set("app.log_level", "debug")

	err = writeViperConfigAtomically(cm.viper, cm.filePath)
	require.NoError(t, err)

	info, err := os.Stat(filePath)
	require.NoError(t, err)

	assert.Equal(t, wantPerm, info.Mode().Perm(),
		"file permissions should be preserved after atomic write")
}

// --- Test #29: valuesEquivalent and toFloat64 tests ---

func TestValuesEquivalent(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name  string
		left  any
		right any
		want  bool
	}{
		{"int_vs_float64", 100, float64(100), true},
		{"int64_vs_float64", int64(42), float64(42), true},
		{"string_equal", "hello", "hello", true},
		{"int_not_equal", 100, 200, false},
		{"string_not_equal", "a", "b", false},
		{"nil_vs_nil", nil, nil, true},
		{"nil_vs_zero", nil, 0, false},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			got := valuesEquivalent(tt.left, tt.right)
			assert.Equal(t, tt.want, got)
		})
	}
}

func TestToFloat64(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name      string
		input     any
		wantValue float64
		wantOK    bool
	}{
		{"int", int(42), 42.0, true},
		{"int64", int64(100), 100.0, true},
		{"float64", float64(3.14), 3.14, true},
		{"uint", uint(10), 10.0, true},
		{"string", "hello", 0, false},
		{"nil", nil, 0, false},
		{"bool", true, 0, false},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			gotValue, gotOK := toFloat64(tt.input)
			assert.Equal(t, tt.wantOK, gotOK)
			assert.InDelta(t, tt.wantValue, gotValue, floatEqualityTolerance)
		})
	}
}
