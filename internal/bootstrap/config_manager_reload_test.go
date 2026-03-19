// Copyright 2025 Lerian Studio. All rights reserved.
// Use of this source code is governed by an Elastic License 2.0
// that can be found in the LICENSE.md file.

//go:build unit

package bootstrap

import (
	"testing"

	libLog "github.com/LerianStudio/lib-commons/v4/commons/log"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestReload_InSeedMode_ReturnsSuperseded(t *testing.T) {
	t.Parallel()

	cfg := defaultConfig()
	logger := &libLog.NopLogger{}

	cm, err := NewConfigManager(cfg, logger)
	require.NoError(t, err)

	cm.enterSeedMode()

	result, reloadErr := cm.reload()
	require.NoError(t, reloadErr)
	require.NotNil(t, result)

	assert.True(t, result.Skipped)
	assert.Equal(t, "superseded by systemplane", result.Reason)
	assert.Equal(t, uint64(0), result.Version)
}

func TestReload_NotInSeedMode_ReturnsFileReloadNotSupported(t *testing.T) {
	t.Parallel()

	cfg := defaultConfig()
	logger := &libLog.NopLogger{}

	cm, err := NewConfigManager(cfg, logger)
	require.NoError(t, err)

	result, reloadErr := cm.reload()
	require.NoError(t, reloadErr)
	require.NotNil(t, result)

	assert.True(t, result.Skipped)
	assert.Contains(t, result.Reason, "file reload not supported")
	assert.Equal(t, uint64(0), result.Version)
}

func TestReload_ViaPublicAPI_ReturnsSkipped(t *testing.T) {
	t.Parallel()

	cfg := defaultConfig()
	logger := &libLog.NopLogger{}

	cm, err := NewConfigManager(cfg, logger)
	require.NoError(t, err)

	result, reloadErr := cm.Reload()
	require.NoError(t, reloadErr)
	require.NotNil(t, result)

	assert.True(t, result.Skipped)
	assert.Equal(t, uint64(0), result.Version, "version should not increment on skipped reload")
}

func TestReload_ReportsCurrentVersion(t *testing.T) {
	t.Parallel()

	cfg := defaultConfig()
	logger := &libLog.NopLogger{}

	cm, err := NewConfigManager(cfg, logger)
	require.NoError(t, err)

	// Manually bump version to simulate a prior update.
	cm.version.Add(5)

	result, reloadErr := cm.reload()
	require.NoError(t, reloadErr)
	require.NotNil(t, result)

	assert.Equal(t, uint64(5), result.Version, "should report current version")
}
