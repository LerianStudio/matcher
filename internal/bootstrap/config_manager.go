// Copyright 2025 Lerian Studio. All rights reserved.
// Use of this source code is governed by an Elastic License 2.0
// that can be found in the LICENSE.md file.

package bootstrap

import (
	"errors"
	"fmt"
	"sync"
	"sync/atomic"
	"time"

	libLog "github.com/LerianStudio/lib-commons/v4/commons/log"

	"github.com/LerianStudio/matcher/pkg/systemplane/domain"
)

// errConfigNilAtomicLoad is returned when the atomic config pointer unexpectedly
// holds nil. This should never happen in practice (the constructor validates
// non-nil, and both reload and update only store validated configs).
var errConfigNilAtomicLoad = errors.New("current config is unexpectedly nil")

// errNotInSeedMode is returned when UpdateFromSystemplane is called before the
// ConfigManager has been transitioned to seed mode by the systemplane supervisor.
var errNotInSeedMode = errors.New("config manager is not in seed mode")

// ConfigManager manages configuration for the Matcher service.
//
// Thread-safety model:
//   - Readers call Get() which uses atomic.Pointer — lock-free, zero contention.
//   - Writers (UpdateFromSystemplane) are serialized via mu to prevent concurrent
//     writes. The atomic swap happens inside the critical section, so readers
//     never block on the mutex.
//
// Lifecycle: NewConfigManager() → Get() → Stop().
//
// After systemplane initialization, the ConfigManager enters seed mode. In seed
// mode, Reload() returns a skipped result and all runtime configuration changes
// flow through the systemplane supervisor via UpdateFromSystemplane(). The Get()
// method continues to work in seed mode.
type ConfigManager struct {
	config     atomic.Pointer[Config]
	mu         sync.Mutex // serializes writes (update)
	logger     libLog.Logger
	version    atomic.Uint64
	lastReload atomic.Value // stores time.Time
	stopOnce   sync.Once
	stopCh     chan struct{}

	// seedMode is set to true when the systemplane Supervisor has assumed
	// runtime authority. In seed mode, Reload() is a no-op and callers
	// should use the systemplane API for runtime configuration changes.
	// Get() still works.
	seedMode atomic.Bool
}

// ReloadResult describes the outcome of a configuration reload.
type ReloadResult struct {
	Version         uint64         `json:"version"`
	ReloadedAt      time.Time      `json:"reloadedAt"`
	ChangesDetected int            `json:"changesDetected"`
	Changes         []ConfigChange `json:"changes,omitempty"`
	Skipped         bool           `json:"skipped,omitempty"`
	Reason          string         `json:"reason,omitempty"`
}

// ConfigChange captures a single key that changed between reloads.
type ConfigChange struct {
	Key      string `json:"key"`
	OldValue any    `json:"oldValue"`
	NewValue any    `json:"newValue"`
}

// NewConfigManager creates a ConfigManager that wraps the given initial config.
// After systemplane initialization, all runtime configuration changes flow
// through the systemplane supervisor.
func NewConfigManager(cfg *Config, logger libLog.Logger) (*ConfigManager, error) {
	if cfg == nil {
		return nil, ErrConfigNil
	}

	if isNilInterface(logger) {
		logger = &libLog.NopLogger{}
	}

	cm := &ConfigManager{
		logger: logger,
		stopCh: make(chan struct{}),
	}

	cm.config.Store(cfg)
	cm.lastReload.Store(time.Now().UTC())

	return cm, nil
}

// Get returns the current configuration. This is the hot path — it uses an
// atomic load with zero locking overhead. Safe to call from any goroutine.
func (cm *ConfigManager) Get() *Config {
	return cm.config.Load()
}

// Reload returns a ReloadResult describing the current state. In seed mode,
// it returns immediately — the systemplane supervisor owns all runtime
// configuration changes.
func (cm *ConfigManager) Reload() (*ReloadResult, error) {
	return cm.reload()
}

// Version returns the current config version. Starts at 0 and increments on
// each successful Reload(). Useful for cache invalidation and
// change detection by consumers.
func (cm *ConfigManager) Version() uint64 {
	return cm.version.Load()
}

// LastReloadAt returns the timestamp of the last successful config reload.
func (cm *ConfigManager) LastReloadAt() time.Time {
	if t, ok := cm.lastReload.Load().(time.Time); ok {
		return t
	}

	return time.Time{}
}

// Stop cleans up resources. Idempotent — safe to call multiple times.
func (cm *ConfigManager) Stop() {
	cm.stopOnce.Do(func() {
		close(cm.stopCh)
	})
}

// InSeedMode reports whether the ConfigManager has been superseded by the
// systemplane Supervisor. In seed mode, hot-reload is disabled and callers
// should use the systemplane API for runtime configuration changes.
func (cm *ConfigManager) InSeedMode() bool {
	return cm.seedMode.Load()
}

// enterSeedMode transitions the ConfigManager to seed mode. In seed mode,
// Reload() returns early and all runtime changes flow through the systemplane
// supervisor. Safe to call multiple times.
func (cm *ConfigManager) enterSeedMode() {
	cm.seedMode.Store(true)
}

// UpdateFromSystemplane converts a systemplane snapshot into a *Config and
// atomically updates the config pointer. This is the bridge that allows all
// existing per-request consumers (rate limiters, health checks) to read
// systemplane-backed values through the existing configManager.Get() path.
//
// This method only works in seed mode. It does NOT notify subscribers because
// the systemplane supervisor handles change propagation through reconcilers.
func (cm *ConfigManager) UpdateFromSystemplane(snap domain.Snapshot) error {
	if !cm.InSeedMode() {
		return fmt.Errorf("update from systemplane: %w", errNotInSeedMode)
	}

	cm.mu.Lock()
	defer cm.mu.Unlock()

	oldCfg := cm.config.Load()
	if oldCfg == nil {
		return fmt.Errorf("update from systemplane: %w", errConfigNilAtomicLoad)
	}

	newCfg := snapshotToFullConfig(snap, oldCfg)

	if err := newCfg.Validate(); err != nil {
		return fmt.Errorf("update from systemplane: validation: %w", err)
	}

	cm.config.Store(newCfg)
	cm.version.Add(1)
	cm.lastReload.Store(time.Now().UTC())

	return nil
}
