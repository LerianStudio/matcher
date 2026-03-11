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
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestReloadLocked_StoppedManager_ReturnsError(t *testing.T) {
	t.Parallel()

	cfg := defaultConfig()
	logger := &libLog.NopLogger{}

	cm, err := NewConfigManager(cfg, "", logger)
	require.NoError(t, err)

	cm.Stop()

	cm.mu.Lock()
	_, reloadErr := cm.reloadLocked("")
	cm.mu.Unlock()

	require.Error(t, reloadErr)
	assert.Contains(t, reloadErr.Error(), "config manager stopped")
}

func TestReloadLocked_ValidYAMLChange_PicksUpNewValues(t *testing.T) {
	// This test writes a YAML file, creates a ConfigManager pointing at it,
	// updates the YAML, then calls reloadLocked to verify the new value is picked up.
	tmpDir := t.TempDir()
	yamlPath := filepath.Join(tmpDir, "matcher.yaml")

	initialYAML := `app:
  log_level: info
`
	require.NoError(t, os.WriteFile(yamlPath, []byte(initialYAML), 0o600))

	cfg := defaultConfig()
	logger := &libLog.NopLogger{}

	cm, err := NewConfigManager(cfg, yamlPath, logger)
	require.NoError(t, err)

	// Overwrite with new log level.
	updatedYAML := `app:
  log_level: debug
`
	require.NoError(t, os.WriteFile(yamlPath, []byte(updatedYAML), 0o600))

	cm.mu.Lock()
	result, reloadErr := cm.reloadLocked("test")
	cm.mu.Unlock()

	require.NoError(t, reloadErr)
	require.NotNil(t, result)
	assert.Equal(t, uint64(1), result.Version)

	got := cm.Get()
	assert.Equal(t, "debug", got.App.LogLevel)
}

func TestReloadLocked_InvalidYAML_PreservesExistingConfig(t *testing.T) {
	tmpDir := t.TempDir()
	yamlPath := filepath.Join(tmpDir, "matcher.yaml")

	initialYAML := `app:
  log_level: info
`
	require.NoError(t, os.WriteFile(yamlPath, []byte(initialYAML), 0o600))

	cfg := defaultConfig()
	logger := &libLog.NopLogger{}

	cm, err := NewConfigManager(cfg, yamlPath, logger)
	require.NoError(t, err)

	// Write invalid YAML.
	require.NoError(t, os.WriteFile(yamlPath, []byte("{{{{invalid yaml"), 0o600))

	cm.mu.Lock()
	_, reloadErr := cm.reloadLocked("test")
	cm.mu.Unlock()

	require.Error(t, reloadErr)

	// Existing config preserved — still has original log level.
	got := cm.Get()
	assert.Equal(t, "info", got.App.LogLevel)
}

func TestReloadLocked_EmptySource_DefaultsToReload(t *testing.T) {
	t.Parallel()

	cfg := defaultConfig()
	logger := &libLog.NopLogger{}

	cm, err := NewConfigManager(cfg, "", logger)
	require.NoError(t, err)

	cm.mu.Lock()
	result, reloadErr := cm.reloadLocked("")
	cm.mu.Unlock()

	require.NoError(t, reloadErr)
	require.NotNil(t, result)
	assert.Equal(t, uint64(1), result.Version)
}
