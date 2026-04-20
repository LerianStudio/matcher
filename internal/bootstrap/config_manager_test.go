// Copyright 2025 Lerian Studio. All rights reserved.
// Use of this source code is governed by an Elastic License 2.0
// that can be found in the LICENSE.md file.

//go:build unit

package bootstrap

import (
	"context"
	"sync"
	"testing"

	libLog "github.com/LerianStudio/lib-commons/v5/commons/log"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// testLogger is a minimal logger for config manager tests that records messages.
type testLogger struct {
	mu       sync.Mutex
	messages []string
}

func (l *testLogger) Log(_ context.Context, _ libLog.Level, msg string, _ ...libLog.Field) {
	l.mu.Lock()
	defer l.mu.Unlock()

	l.messages = append(l.messages, msg)
}

func (l *testLogger) With(_ ...libLog.Field) libLog.Logger { return l }
func (l *testLogger) WithGroup(_ string) libLog.Logger     { return l }
func (l *testLogger) Enabled(_ libLog.Level) bool          { return true }
func (l *testLogger) Sync(_ context.Context) error         { return nil }

// newTestConfigManager creates a ConfigManager for tests.
func newTestConfigManager(t *testing.T, cfg *Config, logger libLog.Logger) *ConfigManager {
	t.Helper()

	cm, err := NewConfigManager(cfg, logger)
	require.NoError(t, err)

	t.Cleanup(cm.Stop)

	return cm
}

func TestNewConfigManager_Success(t *testing.T) {
	t.Parallel()

	logger := &testLogger{}

	cfg := defaultConfig()
	cm := newTestConfigManager(t, cfg, logger)

	assert.NotNil(t, cm)
	assert.Equal(t, cfg, cm.Get())
}

func TestNewConfigManager_NilConfig(t *testing.T) {
	t.Parallel()

	_, err := NewConfigManager(nil, nil)
	require.Error(t, err)
	assert.ErrorIs(t, err, ErrConfigNil)
}

func TestNewConfigManager_NilLogger(t *testing.T) {
	t.Parallel()

	cfg := defaultConfig()
	cm, err := NewConfigManager(cfg, nil)
	require.NoError(t, err)

	t.Cleanup(cm.Stop)

	// Should use NopLogger internally — no panic.
	assert.NotNil(t, cm)
}

func TestNewConfigManager_TypedNilLogger(t *testing.T) {
	t.Parallel()

	cfg := defaultConfig()
	var typedNil *libLog.NopLogger

	cm, err := NewConfigManager(cfg, typedNil)
	require.NoError(t, err)
	t.Cleanup(cm.Stop)
	assert.NotNil(t, cm)
}

func TestNewConfigManager_NoFile(t *testing.T) {
	t.Parallel()

	cfg := defaultConfig()
	cm := newTestConfigManager(t, cfg, &testLogger{})

	// Manager still works in env-only mode.
	assert.Equal(t, cfg, cm.Get())
}

func TestNewConfigManager_MissingFile(t *testing.T) {
	t.Parallel()

	cfg := defaultConfig()
	cm, err := NewConfigManager(cfg, &testLogger{})
	require.NoError(t, err)

	t.Cleanup(cm.Stop)

	// File-not-found is graceful — manager works with defaults.
	assert.NotNil(t, cm)
}

func TestNewConfigManager_EnvOnly_Works(t *testing.T) {
	t.Parallel()

	cfg := defaultConfig()
	cm, err := NewConfigManager(cfg, &testLogger{})
	require.NoError(t, err)
	t.Cleanup(cm.Stop)
	assert.NotNil(t, cm)
}

func TestConfigManager_Get_ReturnsCurrentConfig(t *testing.T) {
	t.Parallel()

	cfg := defaultConfig()
	cfg.App.LogLevel = "debug"

	cm := newTestConfigManager(t, cfg, &testLogger{})

	got := cm.Get()
	assert.Equal(t, "debug", got.App.LogLevel)
	assert.Same(t, cfg, got)
}

func TestConfigManager_Get_Concurrent(t *testing.T) {
	t.Parallel()

	cfg := defaultConfig()

	cm := newTestConfigManager(t, cfg, &testLogger{})

	const goroutines = 100

	var wg sync.WaitGroup

	wg.Add(goroutines)

	for range goroutines {
		go func() {
			defer wg.Done()

			got := cm.Get()
			assert.NotNil(t, got)
			assert.Equal(t, "info", got.App.LogLevel)
		}()
	}

	wg.Wait()
}

func TestConfigManager_Stop_Idempotent(t *testing.T) {
	t.Parallel()

	cfg := defaultConfig()
	cm, err := NewConfigManager(cfg, &testLogger{})
	require.NoError(t, err)

	// Call Stop() multiple times — should not panic.
	cm.Stop()
	cm.Stop()
	cm.Stop()

	// Get() still works after Stop().
	assert.NotNil(t, cm.Get())
}

func TestConfigManager_BootstrapWiring(t *testing.T) {
	t.Parallel()

	t.Run("ConfigManager.Get returns the same snapshot used at init", func(t *testing.T) {
		t.Parallel()

		cfg := defaultConfig()
		cfg.App.LogLevel = "warn"

		cm := newTestConfigManager(t, cfg, &testLogger{})

		// Simulate what InitServersWithOptions does:
		// Store both cfg and cm on the Service struct.
		svc := &Service{
			Config:        cfg,
			ConfigManager: cm,
		}

		// Verify that the static snapshot and dynamic Get() agree at init time.
		assert.Same(t, svc.Config, svc.ConfigManager.Get(),
			"at init time, Config and ConfigManager.Get() should be the same pointer")
		assert.Equal(t, "warn", svc.ConfigManager.Get().App.LogLevel)
	})

	t.Run("cleanup stops ConfigManager cleanly", func(t *testing.T) {
		t.Parallel()

		cfg := defaultConfig()
		cm, err := NewConfigManager(cfg, &testLogger{})
		require.NoError(t, err)

		// Simulate the cleanup function that InitServersWithOptions registers.
		cleanupFuncs := []func(){cm.Stop}

		// Execute cleanups in reverse order (same as Service.runCleanupFuncs).
		for i := len(cleanupFuncs) - 1; i >= 0; i-- {
			cleanupFuncs[i]()
		}

		// Calling Stop() again should be idempotent.
		cm.Stop()

		// Get() still works after Stop().
		assert.NotNil(t, cm.Get())
		assert.Same(t, cfg, cm.Get())
	})
}

func TestConfigManager_WatchSystemplane_InitialHydrateAppliesOverrides(t *testing.T) {
	t.Parallel()

	base := defaultConfig()
	base.RateLimit.Max = 100

	client := newStartedTestClient(t, base)
	setMatcherKey(t, client, "rate_limit.max", 777)

	cm := newTestConfigManager(t, base, &testLogger{})
	require.Equal(t, 100, cm.Get().RateLimit.Max)

	require.NoError(t, cm.WatchSystemplane(client))
	require.Equal(t, 777, cm.Get().RateLimit.Max)
}

func TestRestoreZeroedFields_RestoresBlankString(t *testing.T) {
	// Not parallel: clearConfigEnvVars manipulates process env.
	clearConfigEnvVars(t)

	dst := defaultConfig()
	snapshot := defaultConfig()

	// Simulate what loadConfigFromEnv does: blanks out a field whose env var is absent.
	snapshot.Postgres.PrimaryHost = "db.example.com"
	dst.Postgres.PrimaryHost = "" // zeroed by env overlay

	restoreZeroedFields(dst, snapshot)

	assert.Equal(t, "db.example.com", dst.Postgres.PrimaryHost,
		"zeroed string should be restored from snapshot")
}

func TestRestoreZeroedFields_PreservesNonZero(t *testing.T) {
	t.Parallel()

	dst := defaultConfig()
	snapshot := defaultConfig()

	dst.Postgres.PrimaryHost = "actual-host"
	snapshot.Postgres.PrimaryHost = "snapshot-host"

	restoreZeroedFields(dst, snapshot)

	assert.Equal(t, "actual-host", dst.Postgres.PrimaryHost,
		"non-zero dst field should NOT be overwritten by snapshot")
}

func TestRestoreZeroedFields_RestoresZeroedInt(t *testing.T) {
	// Not parallel: clearConfigEnvVars manipulates process env.
	clearConfigEnvVars(t)

	dst := defaultConfig()
	snapshot := defaultConfig()

	snapshot.RateLimit.Max = 200
	dst.RateLimit.Max = 0 // zeroed by env overlay

	restoreZeroedFields(dst, snapshot)

	assert.Equal(t, 200, dst.RateLimit.Max,
		"zeroed int should be restored from snapshot")
}

func TestRestoreZeroedFields_BothZero(t *testing.T) {
	t.Parallel()

	dst := defaultConfig()
	snapshot := defaultConfig()

	dst.Postgres.PrimaryPassword = ""
	snapshot.Postgres.PrimaryPassword = ""

	restoreZeroedFields(dst, snapshot)

	assert.Empty(t, dst.Postgres.PrimaryPassword,
		"both-zero field should remain zero")
}

func TestRestoreZeroedFields_RestoresBool(t *testing.T) {
	// Not parallel: clearConfigEnvVars manipulates process env.
	clearConfigEnvVars(t)

	dst := defaultConfig()
	snapshot := defaultConfig()

	snapshot.Auth.Enabled = true
	dst.Auth.Enabled = false // zeroed

	restoreZeroedFields(dst, snapshot)

	assert.True(t, dst.Auth.Enabled,
		"zeroed bool should be restored from snapshot")
}
