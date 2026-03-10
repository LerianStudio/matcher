//go:build unit

package bootstrap

import (
	"context"
	"math"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	libLog "github.com/LerianStudio/lib-commons/v4/commons/log"

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

// writeTestYAML writes YAML content to a temp dir and returns the file path.
func writeTestYAML(t *testing.T, dir, content string) string {
	t.Helper()

	yamlPath := filepath.Join(dir, "matcher.yaml")
	require.NoError(t, os.WriteFile(yamlPath, []byte(content), 0o600))

	return yamlPath
}

// validTestYAML returns minimal YAML that passes Validate().
// Must include tenancy.default_tenant_id because loadConfigFromEnv() zeroes
// fields whose env vars are absent, overwriting viper defaults.
const validTestYAML = `
app:
  env_name: "development"
  log_level: "info"
server:
  address: ":4018"
  body_limit_bytes: 104857600
tenancy:
  default_tenant_id: "11111111-1111-1111-1111-111111111111"
  default_tenant_slug: "default"
infrastructure:
  connect_timeout_sec: 30
rate_limit:
  enabled: true
  max: 100
  expiry_sec: 60
  export_max: 10
  export_expiry_sec: 60
  dispatch_max: 50
  dispatch_expiry_sec: 60
`

// newTestConfigManager creates a ConfigManager for tests without starting the
// file watcher (which would race with manual Reload calls via viper internals).
func newTestConfigManager(t *testing.T, cfg *Config, filePath string, logger libLog.Logger) *ConfigManager {
	t.Helper()

	cm, err := NewConfigManager(cfg, filePath, logger)
	require.NoError(t, err)

	t.Cleanup(cm.Stop)

	return cm
}

func TestNewConfigManager_Success(t *testing.T) {
	t.Parallel()

	tmpDir := t.TempDir()
	yamlPath := writeTestYAML(t, tmpDir, validTestYAML)
	logger := &testLogger{}

	cfg := defaultConfig()
	cm := newTestConfigManager(t, cfg, yamlPath, logger)

	assert.NotNil(t, cm)
	assert.Equal(t, cfg, cm.Get())
	assert.Equal(t, uint64(0), cm.Version())
	assert.False(t, cm.LastReloadAt().IsZero())
}

func TestNewConfigManager_NilConfig(t *testing.T) {
	t.Parallel()

	_, err := NewConfigManager(nil, "", nil)
	require.Error(t, err)
	assert.ErrorIs(t, err, ErrConfigNil)
}

func TestNewConfigManager_NilLogger(t *testing.T) {
	t.Parallel()

	cfg := defaultConfig()
	cm, err := NewConfigManager(cfg, "", nil)
	require.NoError(t, err)

	t.Cleanup(cm.Stop)

	// Should use NopLogger internally — no panic.
	assert.NotNil(t, cm)
}

func TestNewConfigManager_NoFile(t *testing.T) {
	t.Parallel()

	cfg := defaultConfig()
	cm := newTestConfigManager(t, cfg, "", &testLogger{})

	// Manager still works in env-only mode.
	assert.Equal(t, cfg, cm.Get())
}

func TestNewConfigManager_MissingFile(t *testing.T) {
	t.Parallel()

	cfg := defaultConfig()
	cm, err := NewConfigManager(cfg, "/nonexistent/matcher.yaml", &testLogger{})
	require.NoError(t, err)

	t.Cleanup(cm.Stop)

	// File-not-found is graceful — manager works with defaults.
	assert.NotNil(t, cm)
}

func TestNewConfigManager_MalformedYAML(t *testing.T) {
	t.Parallel()

	tmpDir := t.TempDir()
	yamlPath := writeTestYAML(t, tmpDir, `this is: [[[invalid yaml`)

	cfg := defaultConfig()
	_, err := NewConfigManager(cfg, yamlPath, &testLogger{})
	require.Error(t, err)
	assert.Contains(t, err.Error(), "read initial YAML")
}

func TestConfigManager_Get_ReturnsCurrentConfig(t *testing.T) {
	t.Parallel()

	cfg := defaultConfig()
	cfg.App.LogLevel = "debug"

	cm := newTestConfigManager(t, cfg, "", &testLogger{})

	got := cm.Get()
	assert.Equal(t, "debug", got.App.LogLevel)
	assert.Same(t, cfg, got)
}

func TestConfigManager_Get_Concurrent(t *testing.T) {
	t.Parallel()

	cfg := defaultConfig()

	cm := newTestConfigManager(t, cfg, "", &testLogger{})

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

func TestConfigManager_Reload_ValidYAML(t *testing.T) {
	// Not parallel: clearConfigEnvVars uses t.Setenv.
	clearConfigEnvVars(t)

	tmpDir := t.TempDir()
	yamlPath := writeTestYAML(t, tmpDir, validTestYAML)
	logger := &testLogger{}

	cfg := defaultConfig()
	cm := newTestConfigManager(t, cfg, yamlPath, logger)

	// Verify initial state.
	assert.Equal(t, "info", cm.Get().App.LogLevel)

	// Write new YAML with changed log level.
	updatedYAML := `
app:
  env_name: "development"
  log_level: "debug"
server:
  address: ":4018"
  body_limit_bytes: 104857600
tenancy:
  default_tenant_id: "11111111-1111-1111-1111-111111111111"
  default_tenant_slug: "default"
infrastructure:
  connect_timeout_sec: 30
rate_limit:
  enabled: true
  max: 200
  expiry_sec: 60
  export_max: 10
  export_expiry_sec: 60
  dispatch_max: 50
  dispatch_expiry_sec: 60
`
	require.NoError(t, os.WriteFile(yamlPath, []byte(updatedYAML), 0o600))

	result, err := cm.Reload()
	require.NoError(t, err)

	assert.Equal(t, uint64(1), result.Version)
	assert.False(t, result.ReloadedAt.IsZero())
	assert.Greater(t, result.ChangesDetected, 0)

	// Verify the config was actually updated.
	newCfg := cm.Get()
	assert.Equal(t, "debug", newCfg.App.LogLevel)
	assert.Equal(t, 200, newCfg.RateLimit.Max)
}

func TestConfigManager_Reload_InvalidYAML(t *testing.T) {
	t.Parallel()

	tmpDir := t.TempDir()
	yamlPath := writeTestYAML(t, tmpDir, validTestYAML)
	logger := &testLogger{}

	cfg := defaultConfig()
	cm := newTestConfigManager(t, cfg, yamlPath, logger)

	// Corrupt the YAML file.
	require.NoError(t, os.WriteFile(yamlPath, []byte(`invalid: [[[yaml`), 0o600))

	_, reloadErr := cm.Reload()
	require.Error(t, reloadErr)

	// Original config preserved.
	assert.Equal(t, "info", cm.Get().App.LogLevel)
	assert.Equal(t, uint64(0), cm.Version())
}

func TestConfigManager_Reload_ValidationFailure(t *testing.T) {
	// Not parallel: clearConfigEnvVars uses t.Setenv.
	clearConfigEnvVars(t)

	tmpDir := t.TempDir()
	yamlPath := writeTestYAML(t, tmpDir, validTestYAML)
	logger := &testLogger{}

	cfg := defaultConfig()
	cm := newTestConfigManager(t, cfg, yamlPath, logger)

	// Write YAML that will fail validation (invalid log_level).
	invalidYAML := `
app:
  env_name: "development"
  log_level: "banana"
server:
  address: ":4018"
  body_limit_bytes: 104857600
tenancy:
  default_tenant_id: "11111111-1111-1111-1111-111111111111"
  default_tenant_slug: "default"
infrastructure:
  connect_timeout_sec: 30
rate_limit:
  enabled: true
  max: 100
  expiry_sec: 60
  export_max: 10
  export_expiry_sec: 60
  dispatch_max: 50
  dispatch_expiry_sec: 60
`
	require.NoError(t, os.WriteFile(yamlPath, []byte(invalidYAML), 0o600))

	_, reloadErr := cm.Reload()
	require.Error(t, reloadErr)
	assert.Contains(t, reloadErr.Error(), "validation")

	// Original config preserved.
	assert.Equal(t, "info", cm.Get().App.LogLevel)
	assert.Equal(t, uint64(0), cm.Version())
}

func TestConfigManager_Reload_PreservesLoggerAndGracePeriod(t *testing.T) {
	t.Parallel()

	tmpDir := t.TempDir()
	yamlPath := writeTestYAML(t, tmpDir, validTestYAML)
	logger := &testLogger{}

	cfg := defaultConfig()
	cfg.Logger = logger
	cfg.ShutdownGracePeriod = 10 * time.Second

	cm := newTestConfigManager(t, cfg, yamlPath, logger)

	result, err := cm.Reload()
	require.NoError(t, err)
	assert.Equal(t, uint64(1), result.Version)

	// Logger and ShutdownGracePeriod must carry forward.
	newCfg := cm.Get()
	assert.Equal(t, logger, newCfg.Logger)
	assert.Equal(t, 10*time.Second, newCfg.ShutdownGracePeriod)
}

func TestConfigManager_Subscribe_CalledOnReload(t *testing.T) {
	t.Parallel()

	tmpDir := t.TempDir()
	yamlPath := writeTestYAML(t, tmpDir, validTestYAML)
	logger := &testLogger{}

	cfg := defaultConfig()
	cm := newTestConfigManager(t, cfg, yamlPath, logger)

	var called atomic.Bool

	var receivedCfg atomic.Pointer[Config]

	cm.Subscribe(func(c *Config) {
		called.Store(true)
		receivedCfg.Store(c)
	})

	_, err := cm.Reload()
	require.NoError(t, err)

	assert.True(t, called.Load(), "subscriber should have been called")
	assert.NotNil(t, receivedCfg.Load(), "subscriber should receive the new config")
	assert.Equal(t, cm.Get(), receivedCfg.Load(), "subscriber config should match current config")
}

func TestConfigManager_Subscribe_NotCalledOnFailedReload(t *testing.T) {
	t.Parallel()

	tmpDir := t.TempDir()
	yamlPath := writeTestYAML(t, tmpDir, validTestYAML)
	logger := &testLogger{}

	cfg := defaultConfig()
	cm := newTestConfigManager(t, cfg, yamlPath, logger)

	var called atomic.Bool

	cm.Subscribe(func(_ *Config) {
		called.Store(true)
	})

	// Corrupt the file to cause reload failure.
	require.NoError(t, os.WriteFile(yamlPath, []byte(`broken: [[[yaml`), 0o600))

	_, reloadErr := cm.Reload()
	require.Error(t, reloadErr)

	assert.False(t, called.Load(), "subscriber should NOT be called on failed reload")
}

func TestConfigManager_Subscribe_MultipleSubscribers(t *testing.T) {
	t.Parallel()

	tmpDir := t.TempDir()
	yamlPath := writeTestYAML(t, tmpDir, validTestYAML)

	cfg := defaultConfig()
	cm := newTestConfigManager(t, cfg, yamlPath, &testLogger{})

	var count atomic.Int32

	for range 5 {
		cm.Subscribe(func(_ *Config) {
			count.Add(1)
		})
	}

	_, err := cm.Reload()
	require.NoError(t, err)

	assert.Equal(t, int32(5), count.Load(), "all 5 subscribers should have been called")
}

func TestConfigManager_Subscribe_PanicRecovery(t *testing.T) {
	t.Parallel()

	tmpDir := t.TempDir()
	yamlPath := writeTestYAML(t, tmpDir, validTestYAML)
	logger := &testLogger{}

	cfg := defaultConfig()
	cm := newTestConfigManager(t, cfg, yamlPath, logger)

	var secondCalled atomic.Bool

	// First subscriber panics.
	cm.Subscribe(func(_ *Config) {
		panic("test panic in subscriber")
	})

	// Second subscriber should still run.
	cm.Subscribe(func(_ *Config) {
		secondCalled.Store(true)
	})

	_, err := cm.Reload()
	require.NoError(t, err)

	assert.True(t, secondCalled.Load(), "second subscriber should run even after first panics")

	// Logger should record the panic.
	msgs := logger.getMessages()
	foundPanicMsg := false

	for _, msg := range msgs {
		if strings.Contains(msg, "panicked") {
			foundPanicMsg = true

			break
		}
	}

	assert.True(t, foundPanicMsg, "logger should record subscriber panic, got: %v", msgs)
}

func TestConfigManager_Version_Increments(t *testing.T) {
	t.Parallel()

	tmpDir := t.TempDir()
	yamlPath := writeTestYAML(t, tmpDir, validTestYAML)

	cfg := defaultConfig()
	cm := newTestConfigManager(t, cfg, yamlPath, &testLogger{})

	assert.Equal(t, uint64(0), cm.Version())

	_, err := cm.Reload()
	require.NoError(t, err)
	assert.Equal(t, uint64(1), cm.Version())

	_, err = cm.Reload()
	require.NoError(t, err)
	assert.Equal(t, uint64(2), cm.Version())

	_, err = cm.Reload()
	require.NoError(t, err)
	assert.Equal(t, uint64(3), cm.Version())
}

func TestConfigManager_Version_DoesNotIncrementOnFailure(t *testing.T) {
	t.Parallel()

	tmpDir := t.TempDir()
	yamlPath := writeTestYAML(t, tmpDir, validTestYAML)

	cfg := defaultConfig()
	cm := newTestConfigManager(t, cfg, yamlPath, &testLogger{})

	// Successful reload.
	_, err := cm.Reload()
	require.NoError(t, err)
	assert.Equal(t, uint64(1), cm.Version())

	// Corrupt file.
	require.NoError(t, os.WriteFile(yamlPath, []byte(`bad: [[[`), 0o600))

	_, err = cm.Reload()
	require.Error(t, err)
	assert.Equal(t, uint64(1), cm.Version(), "version should NOT increment on failure")
}

func TestConfigManager_LastReloadAt_Updates(t *testing.T) {
	t.Parallel()

	tmpDir := t.TempDir()
	yamlPath := writeTestYAML(t, tmpDir, validTestYAML)

	cfg := defaultConfig()
	cm := newTestConfigManager(t, cfg, yamlPath, &testLogger{})

	initialTime := cm.LastReloadAt()
	assert.False(t, initialTime.IsZero())

	// Small sleep to ensure time difference.
	time.Sleep(10 * time.Millisecond)

	_, err := cm.Reload()
	require.NoError(t, err)

	afterReload := cm.LastReloadAt()
	assert.True(t, afterReload.After(initialTime), "LastReloadAt should update after reload")
}

func TestConfigManager_Stop_Idempotent(t *testing.T) {
	t.Parallel()

	cfg := defaultConfig()
	cm, err := NewConfigManager(cfg, "", &testLogger{})
	require.NoError(t, err)

	// Call Stop() multiple times — should not panic.
	cm.Stop()
	cm.Stop()
	cm.Stop()

	// Get() still works after Stop().
	assert.NotNil(t, cm.Get())
}

func TestConfigManager_Stop_StopsDebounceTimer(t *testing.T) {
	t.Parallel()

	tmpDir := t.TempDir()
	yamlPath := writeTestYAML(t, tmpDir, validTestYAML)

	cfg := defaultConfig()
	cm := newTestConfigManager(t, cfg, yamlPath, &testLogger{})

	// Trigger a debounced reload.
	cm.reloadDebounced()

	// Stop should clean up the timer.
	cm.Stop()

	// Give the debounce timer enough time to fire (if it wasn't stopped).
	time.Sleep(debounceDuration + 100*time.Millisecond)

	// Version should be 0 — the debounced reload was cancelled by Stop().
	assert.Equal(t, uint64(0), cm.Version())
}

func TestConfigManager_Debounce_CoalescesEvents(t *testing.T) {
	t.Parallel()

	tmpDir := t.TempDir()
	yamlPath := writeTestYAML(t, tmpDir, validTestYAML)

	cfg := defaultConfig()
	cm := newTestConfigManager(t, cfg, yamlPath, &testLogger{})

	// NOTE: This test depends on real-time timing. The 50ms sleep is well within
	// the 500ms debounce window (10x margin). On heavily loaded CI machines,
	// individual iterations could occasionally exceed 50ms, but all 10 iterations
	// (500ms total) should still fall within a single debounce window.

	// Simulate rapid file events — each resets the debounce timer.
	for range 10 {
		cm.reloadDebounced()
		time.Sleep(50 * time.Millisecond) // Well within the 500ms debounce window.
	}

	// Wait for the debounce to fire.
	time.Sleep(debounceDuration + 200*time.Millisecond)

	// Should have resulted in exactly 1 reload, not 10.
	assert.Equal(t, uint64(1), cm.Version(), "debounce should coalesce 10 events into 1 reload")
}

func TestConfigManager_Update_MutableKeys(t *testing.T) {
	// Not parallel: clearConfigEnvVars uses t.Setenv.
	clearConfigEnvVars(t)

	tmpDir := t.TempDir()
	yamlPath := writeTestYAML(t, tmpDir, validTestYAML)

	cfg := defaultConfig()
	cm := newTestConfigManager(t, cfg, yamlPath, &testLogger{})

	result, err := cm.Update(map[string]any{
		"rate_limit.max": 500,
	})

	require.NoError(t, err)
	assert.Len(t, result.Applied, 1)
	assert.Empty(t, result.Rejected)

	// Verify config updated.
	newCfg := cm.Get()
	assert.Equal(t, 500, newCfg.RateLimit.Max)
}

func TestConfigManager_Update_ImmutableKeysRejected(t *testing.T) {
	t.Parallel()

	tmpDir := t.TempDir()
	yamlPath := writeTestYAML(t, tmpDir, validTestYAML)

	cfg := defaultConfig()
	cm := newTestConfigManager(t, cfg, yamlPath, &testLogger{})

	result, err := cm.Update(map[string]any{
		"postgres.primary_host": "evil-host",
		"auth.token_secret":     "stolen-secret",
	})

	require.NoError(t, err)
	assert.Empty(t, result.Applied)
	assert.Len(t, result.Rejected, 2)

	for _, rejected := range result.Rejected {
		assert.Contains(t, rejected.Reason, "not mutable")
	}
}

func TestConfigManager_Update_MixedApplyAndReject(t *testing.T) {
	t.Parallel()

	tmpDir := t.TempDir()
	yamlPath := writeTestYAML(t, tmpDir, validTestYAML)

	cfg := defaultConfig()
	cm := newTestConfigManager(t, cfg, yamlPath, &testLogger{})

	result, err := cm.Update(map[string]any{
		"rate_limit.max":        250,   // mutable — accepted
		"postgres.primary_host": "bad", // immutable — rejected
	})

	require.NoError(t, err)
	assert.Len(t, result.Applied, 1)
	assert.Len(t, result.Rejected, 1)
	assert.Equal(t, "rate_limit.max", result.Applied[0].Key)
	assert.Equal(t, "postgres.primary_host", result.Rejected[0].Key)
}

func TestConfigManager_Update_EmptyChanges(t *testing.T) {
	t.Parallel()

	cfg := defaultConfig()
	cm := newTestConfigManager(t, cfg, "", &testLogger{})

	result, err := cm.Update(map[string]any{})
	require.NoError(t, err)
	assert.Empty(t, result.Applied)
	assert.Empty(t, result.Rejected)
}

func TestConfigManager_Update_ValidationFailureRollsBack(t *testing.T) {
	// Not parallel: clearConfigEnvVars uses t.Setenv.
	clearConfigEnvVars(t)

	tmpDir := t.TempDir()
	yamlPath := writeTestYAML(t, tmpDir, validTestYAML)

	cfg := defaultConfig()
	cm := newTestConfigManager(t, cfg, yamlPath, &testLogger{})

	originalMax := cm.Get().RateLimit.Max

	// Try to set an invalid log level — will fail validation.
	_, updateErr := cm.Update(map[string]any{
		"app.log_level": "banana",
	})

	require.Error(t, updateErr)
	assert.Contains(t, updateErr.Error(), "validation")

	// Original config preserved.
	assert.Equal(t, originalMax, cm.Get().RateLimit.Max)
}

func TestConfigManager_Update_IncrementsVersion(t *testing.T) {
	t.Parallel()

	tmpDir := t.TempDir()
	yamlPath := writeTestYAML(t, tmpDir, validTestYAML)

	cfg := defaultConfig()
	cm := newTestConfigManager(t, cfg, yamlPath, &testLogger{})

	assert.Equal(t, uint64(0), cm.Version())

	_, err := cm.Update(map[string]any{
		"rate_limit.max": 200,
	})
	require.NoError(t, err)
	assert.Equal(t, uint64(1), cm.Version())
}

func TestConfigManager_Update_NotifiesSubscribers(t *testing.T) {
	t.Parallel()

	tmpDir := t.TempDir()
	yamlPath := writeTestYAML(t, tmpDir, validTestYAML)

	cfg := defaultConfig()
	cm := newTestConfigManager(t, cfg, yamlPath, &testLogger{})

	var called atomic.Bool

	cm.Subscribe(func(_ *Config) {
		called.Store(true)
	})

	_, err := cm.Update(map[string]any{
		"rate_limit.max": 300,
	})
	require.NoError(t, err)

	assert.True(t, called.Load(), "subscriber should be called after Update")
}

func TestConfigManager_Update_WritesYAMLFile(t *testing.T) {
	t.Parallel()

	tmpDir := t.TempDir()
	yamlPath := writeTestYAML(t, tmpDir, validTestYAML)

	cfg := defaultConfig()
	cm := newTestConfigManager(t, cfg, yamlPath, &testLogger{})

	_, err := cm.Update(map[string]any{
		"rate_limit.max": 999,
	})
	require.NoError(t, err)

	// Verify the file was written.
	content, readErr := os.ReadFile(yamlPath)
	require.NoError(t, readErr)
	assert.NotEmpty(t, content)
	assert.Contains(t, string(content), "rate_limit", "written YAML should contain rate_limit section")
}

func TestConfigManager_Reload_ConcurrentSafety(t *testing.T) {
	t.Parallel()

	tmpDir := t.TempDir()
	yamlPath := writeTestYAML(t, tmpDir, validTestYAML)

	cfg := defaultConfig()
	cm := newTestConfigManager(t, cfg, yamlPath, &testLogger{})

	const goroutines = 20

	var wg sync.WaitGroup

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

				// Reload may fail under concurrent access — errors are expected
				// and intentionally discarded. This test verifies absence of
				// panics and data races, not error-free operation.
				_, _ = cm.Reload()
			}()
		}
	}

	wg.Wait()

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

	// Should detect changes in "app" and "rate_limit" sections.
	keys := make(map[string]bool, len(changes))
	for _, c := range changes {
		keys[c.Key] = true
	}

	assert.True(t, keys["app"], "should detect app config change")
	assert.True(t, keys["rate_limit"], "should detect rate_limit config change")
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
	cm := newTestConfigManager(t, cfg, "", &testLogger{})

	// Reload without a file should still work (env-only mode).
	result, err := cm.Reload()
	require.NoError(t, err)
	assert.Equal(t, uint64(1), result.Version)
}

func TestConfigManager_StartWatcher_NoFile(t *testing.T) {
	t.Parallel()

	cfg := defaultConfig()
	cm := newTestConfigManager(t, cfg, "", &testLogger{})

	// StartWatcher with empty path is a no-op — should not panic.
	cm.StartWatcher()
}

func TestConfigManager_StartWatcher_AfterStop(t *testing.T) {
	t.Parallel()

	tmpDir := t.TempDir()
	yamlPath := writeTestYAML(t, tmpDir, validTestYAML)

	cfg := defaultConfig()
	cm, err := NewConfigManager(cfg, yamlPath, &testLogger{})
	require.NoError(t, err)

	cm.Stop()

	// StartWatcher after Stop is a no-op — should not panic.
	cm.StartWatcher()
}

func TestConfigManager_BootstrapWiring(t *testing.T) {
	t.Parallel()

	t.Run("ConfigManager.Get returns the same snapshot used at init", func(t *testing.T) {
		t.Parallel()

		cfg := defaultConfig()
		cfg.App.LogLevel = "warn"

		cm := newTestConfigManager(t, cfg, "", &testLogger{})

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

		tmpDir := t.TempDir()
		yamlPath := writeTestYAML(t, tmpDir, validTestYAML)

		cfg := defaultConfig()
		cm := newTestConfigManager(t, cfg, yamlPath, &testLogger{})

		svc := &Service{
			Config:        cfg,
			ConfigManager: cm,
		}

		// Simulate a config reload (e.g., from file watcher).
		_, err := svc.ConfigManager.Reload()
		require.NoError(t, err)

		// After reload, ConfigManager.Get() may return a NEW pointer...
		newCfg := svc.ConfigManager.Get()
		assert.NotNil(t, newCfg)

		// ...but the static snapshot is unchanged.
		assert.Same(t, cfg, svc.Config,
			"static Config snapshot must not change after ConfigManager reload")
	})

	t.Run("cleanup stops ConfigManager file watcher", func(t *testing.T) {
		t.Parallel()

		cfg := defaultConfig()
		cm, err := NewConfigManager(cfg, "", &testLogger{})
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
		{name: "lowercase password", key: "password", expected: true},
		{name: "uppercase PASSWORD", key: "PASSWORD", expected: true},
		{name: "mixed case Password", key: "Password", expected: true},
		{name: "contains password", key: "primary_password", expected: true},
		{name: "token", key: "token", expected: true},
		{name: "contains token", key: "token_secret", expected: true},
		{name: "TOKEN uppercase", key: "TOKEN", expected: true},
		{name: "secret", key: "secret", expected: true},
		{name: "contains secret", key: "hmac_secret", expected: true},
		{name: "SECRET uppercase", key: "SECRET", expected: true},
		{name: "normal key", key: "rate_limit.max", expected: false},
		{name: "host key", key: "postgres.primary_host", expected: false},
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
	t.Parallel()

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
	t.Parallel()

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
	t.Parallel()

	dst := defaultConfig()
	snapshot := defaultConfig()

	snapshot.Auth.Enabled = true
	dst.Auth.Enabled = false // zeroed

	restoreZeroedFields(dst, snapshot)

	assert.True(t, dst.Auth.Enabled,
		"zeroed bool should be restored from snapshot")
}
