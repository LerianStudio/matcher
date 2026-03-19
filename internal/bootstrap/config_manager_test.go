// Copyright 2025 Lerian Studio. All rights reserved.
// Use of this source code is governed by an Elastic License 2.0
// that can be found in the LICENSE.md file.

//go:build unit

package bootstrap

import (
	"context"
	"math"
	"sync"
	"testing"
	"time"

	libLog "github.com/LerianStudio/lib-commons/v4/commons/log"

	"github.com/LerianStudio/matcher/pkg/systemplane/domain"

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

func (l *testLogger) getMessages() []string {
	l.mu.Lock()
	defer l.mu.Unlock()

	out := make([]string, len(l.messages))
	copy(out, l.messages)

	return out
}

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
	assert.Equal(t, uint64(0), cm.Version())
	assert.False(t, cm.LastReloadAt().IsZero())
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

func TestConfigManager_Reload_ReturnsSkipped(t *testing.T) {
	t.Parallel()

	cfg := defaultConfig()
	cm := newTestConfigManager(t, cfg, &testLogger{})

	// Reload always returns skipped now that Viper is removed from the runtime path.
	result, err := cm.Reload()
	require.NoError(t, err)

	assert.True(t, result.Skipped)
	assert.Contains(t, result.Reason, "file reload not supported")
	assert.Equal(t, uint64(0), result.Version, "version should not increment on skipped reload")

	// Config should remain unchanged.
	assert.Equal(t, "info", cm.Get().App.LogLevel)
}

func TestConfigManager_Reload_InSeedMode_ReturnsSuperseded(t *testing.T) {
	t.Parallel()

	cfg := defaultConfig()
	cm := newTestConfigManager(t, cfg, &testLogger{})
	cm.enterSeedMode()

	result, err := cm.Reload()
	require.NoError(t, err)

	assert.True(t, result.Skipped)
	assert.Equal(t, "superseded by systemplane", result.Reason)
}

func TestConfigManager_Reload_PreservesConfig(t *testing.T) {
	t.Parallel()

	logger := &testLogger{}
	cfg := defaultConfig()
	cfg.Logger = logger
	cfg.ShutdownGracePeriod = 10 * time.Second

	cm := newTestConfigManager(t, cfg, logger)

	_, err := cm.Reload()
	require.NoError(t, err)

	// Config should be identical — reload is a no-op.
	newCfg := cm.Get()
	assert.Equal(t, logger, newCfg.Logger)
	assert.Equal(t, 10*time.Second, newCfg.ShutdownGracePeriod)
}

func TestConfigManager_Version_DoesNotIncrementOnSkippedReload(t *testing.T) {
	t.Parallel()

	cfg := defaultConfig()
	cm := newTestConfigManager(t, cfg, &testLogger{})

	assert.Equal(t, uint64(0), cm.Version())

	// Reload is now skipped — version should NOT increment.
	_, err := cm.Reload()
	require.NoError(t, err)
	assert.Equal(t, uint64(0), cm.Version())
}

func TestConfigManager_Version_IncrementedByUpdateFromSystemplane(t *testing.T) {
	t.Parallel()

	cfg := defaultConfig()
	cm := newTestConfigManager(t, cfg, &testLogger{})
	cm.enterSeedMode()

	assert.Equal(t, uint64(0), cm.Version())

	// UpdateFromSystemplane increments the version.
	snap := domain.Snapshot{}

	err := cm.UpdateFromSystemplane(snap)
	require.NoError(t, err)
	assert.Equal(t, uint64(1), cm.Version())
}

func TestConfigManager_LastReloadAt_SetAtConstruction(t *testing.T) {
	t.Parallel()

	cfg := defaultConfig()
	cm := newTestConfigManager(t, cfg, &testLogger{})

	initialTime := cm.LastReloadAt()
	assert.False(t, initialTime.IsZero(), "LastReloadAt should be set at construction")
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

func TestConfigManager_Reload_ReturnsSkippedWhenNotInSeedMode(t *testing.T) {
	t.Parallel()

	cfg := defaultConfig()
	cm := newTestConfigManager(t, cfg, &testLogger{})

	// Reload outside seed mode returns skipped (file reload not supported).
	result, err := cm.Reload()
	require.NoError(t, err)
	assert.True(t, result.Skipped)
	assert.Contains(t, result.Reason, "file reload not supported")
	assert.Equal(t, uint64(0), cm.Version(), "version should not increment on skipped reload")
}

func TestConfigManager_Reload_ConcurrentSafety(t *testing.T) {
	t.Parallel()

	cfg := defaultConfig()
	cm := newTestConfigManager(t, cfg, &testLogger{})

	const goroutines = 20

	var wg sync.WaitGroup
	reloadErrs := make(chan error, goroutines/2)

	// Half goroutines reading, half reloading — should not race.
	wg.Add(goroutines)

	for i := range goroutines {
		if i%2 == 0 {
			go func() {
				defer wg.Done()

				got := cm.Get()
				assert.NotNil(t, got)
			}()
		} else {
			go func() {
				defer wg.Done()

				_, reloadErr := cm.Reload()
				reloadErrs <- reloadErr
			}()
		}
	}

	wg.Wait()
	close(reloadErrs)

	for reloadErr := range reloadErrs {
		require.NoError(t, reloadErr)
	}

	// Post-condition: config should still be non-nil and structurally valid
	// after concurrent access. This catches subtle corruption that doesn't panic.
	finalCfg := cm.Get()
	assert.NotNil(t, finalCfg, "config should not be nil after concurrent reloads")
	assert.NotEmpty(t, finalCfg.App.EnvName, "config should not be corrupted by concurrent access")
}

func TestDiffConfigs_DetectsChanges(t *testing.T) {
	t.Parallel()

	old := defaultConfig()
	newCfg := defaultConfig()

	newCfg.App.LogLevel = "debug"
	newCfg.RateLimit.Max = 999

	changes := diffConfigs(old, newCfg)
	assert.NotEmpty(t, changes)

	// Should detect field-level changes with dotted keys.
	keys := make(map[string]bool, len(changes))
	for _, c := range changes {
		keys[c.Key] = true
	}

	assert.True(t, keys["app.log_level"], "should detect app.log_level change")
	assert.True(t, keys["rate_limit.max"], "should detect rate_limit.max change")
}

func TestDiffConfigs_NoChanges(t *testing.T) {
	t.Parallel()

	cfg1 := defaultConfig()
	cfg2 := defaultConfig()

	changes := diffConfigs(cfg1, cfg2)
	assert.Empty(t, changes, "identical configs should produce no diff")
}

func TestDiffConfigs_NilInputs(t *testing.T) {
	t.Parallel()

	assert.Empty(t, diffConfigs(nil, defaultConfig()))
	assert.Empty(t, diffConfigs(defaultConfig(), nil))
	assert.Empty(t, diffConfigs(nil, nil))
}

func TestConfigManager_Reload_NoFile(t *testing.T) {
	t.Parallel()

	cfg := defaultConfig()
	cm := newTestConfigManager(t, cfg, &testLogger{})

	// Reload without a file returns skipped (file reload not supported).
	result, err := cm.Reload()
	require.NoError(t, err)
	assert.True(t, result.Skipped)
	assert.Contains(t, result.Reason, "file reload not supported")
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

	t.Run("ConfigManager survives reload while Config stays static", func(t *testing.T) {
		t.Parallel()

		cfg := defaultConfig()
		cm := newTestConfigManager(t, cfg, &testLogger{})

		svc := &Service{
			Config:        cfg,
			ConfigManager: cm,
		}

		// Simulate a config reload (now a no-op; systemplane handles runtime changes).
		_, err := svc.ConfigManager.Reload()
		require.NoError(t, err)

		// After reload, ConfigManager.Get() may return a NEW pointer...
		newCfg := svc.ConfigManager.Get()
		assert.NotNil(t, newCfg)

		// ...but the static snapshot is unchanged.
		assert.Same(t, cfg, svc.Config,
			"static Config snapshot must not change after ConfigManager reload")
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

// --- Helper function tests (H8) ---

func TestRedactIfSensitive_PasswordKey(t *testing.T) {
	t.Parallel()

	result := redactIfSensitive("postgres.primary_password", "s3cret")
	assert.Equal(t, "***REDACTED***", result)
}

func TestRedactIfSensitive_NormalKey(t *testing.T) {
	t.Parallel()

	result := redactIfSensitive("rate_limit.max", 100)
	assert.Equal(t, 100, result)
}

func TestRedactIfSensitive_TokenKey(t *testing.T) {
	t.Parallel()

	result := redactIfSensitive("auth.token_secret", "jwt-tok")
	assert.Equal(t, "***REDACTED***", result)
}

func TestRedactIfSensitive_SecretKey(t *testing.T) {
	t.Parallel()

	result := redactIfSensitive("idempotency.hmac_secret", "hmac-val")
	assert.Equal(t, "***REDACTED***", result)
}

func TestIsSensitiveKey_MatchesExpectedPatterns(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name     string
		key      string
		expected bool
	}{
		{name: "postgres password", key: "postgres.primary_password", expected: true},
		{name: "rabbitmq password", key: "rabbitmq.password", expected: true},
		{name: "auth token secret", key: "auth.token_secret", expected: true},
		{name: "idempotency hmac secret", key: "idempotency.hmac_secret", expected: true},
		{name: "object storage access key", key: "object_storage.access_key_id", expected: true},
		{name: "multi-tenant service api key", key: "tenancy.multi_tenant_service_api_key", expected: true},
		{name: "normal key", key: "rate_limit.max", expected: false},
		{name: "host key", key: "postgres.primary_host", expected: false},
		{name: "rabbitmq uri", key: "rabbitmq.uri", expected: false},
		{name: "enabled key", key: "auth.enabled", expected: false},
		{name: "empty string", key: "", expected: false},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			assert.Equal(t, tt.expected, isSensitiveKey(tt.key))
		})
	}
}

func TestSafeUint64ToInt_NormalValue(t *testing.T) {
	t.Parallel()

	assert.Equal(t, 42, safeUint64ToInt(42))
	assert.Equal(t, 0, safeUint64ToInt(0))
	assert.Equal(t, 1000, safeUint64ToInt(1000))
}

func TestSafeUint64ToInt_Overflow(t *testing.T) {
	t.Parallel()

	result := safeUint64ToInt(math.MaxUint64)
	assert.Equal(t, math.MaxInt, result)
}

func TestSafeUint64ToInt_BoundaryValue(t *testing.T) {
	t.Parallel()

	// math.MaxInt as uint64 should convert exactly.
	result := safeUint64ToInt(uint64(math.MaxInt))
	assert.Equal(t, math.MaxInt, result)
}

func TestSafeUint64ToInt_JustAboveBoundary(t *testing.T) {
	t.Parallel()

	// One above math.MaxInt should cap at math.MaxInt.
	result := safeUint64ToInt(uint64(math.MaxInt) + 1)
	assert.Equal(t, math.MaxInt, result)
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
