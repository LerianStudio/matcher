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

func TestStartWatcher_NoFilePath_IsNoop(t *testing.T) {
	t.Parallel()

	cfg := defaultConfig()
	logger := &libLog.NopLogger{}

	cm, err := NewConfigManager(cfg, "", logger)
	require.NoError(t, err)

	defer cm.Stop()

	// Should be a no-op — no panic, no watcher goroutine spawned.
	cm.StartWatcher()

	// Version should be unchanged (no reload triggered).
	assert.Equal(t, uint64(0), cm.Version())
}

func TestStartWatcher_AfterStop_IsNoop(t *testing.T) {
	t.Parallel()

	cfg := defaultConfig()
	logger := &libLog.NopLogger{}

	cm, err := NewConfigManager(cfg, "", logger)
	require.NoError(t, err)

	cm.Stop()

	// After Stop(), StartWatcher should be a no-op — the stopCh is closed.
	cm.StartWatcher()

	assert.Equal(t, uint64(0), cm.Version())
}

func TestReloadDebounced_AfterStop_DoesNotPanic(t *testing.T) {
	t.Parallel()

	cfg := defaultConfig()
	logger := &libLog.NopLogger{}

	cm, err := NewConfigManager(cfg, "", logger)
	require.NoError(t, err)

	cm.Stop()

	// Should not panic even after manager is stopped.
	assert.NotPanics(t, func() {
		cm.reloadDebounced()
	})
}
