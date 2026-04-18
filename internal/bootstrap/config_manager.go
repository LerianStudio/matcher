// Copyright 2025 Lerian Studio. All rights reserved.
// Use of this source code is governed by an Elastic License 2.0
// that can be found in the LICENSE.md file.

package bootstrap

import (
	"sync/atomic"

	libLog "github.com/LerianStudio/lib-commons/v5/commons/log"

	sharedPorts "github.com/LerianStudio/matcher/internal/shared/ports"
)

// ConfigManager manages configuration for the Matcher service.
//
// Thread-safety model:
//   - Readers call Get() which uses atomic.Pointer -- lock-free, zero contention.
//   - Writers call Update() which atomically swaps the config pointer.
//
// Lifecycle: NewConfigManager() -> Get() -> Stop().
//
// After systemplane initialization, runtime config changes flow through the
// systemplane Client's OnChange callbacks, which call Update() to refresh
// the config pointer.
type ConfigManager struct {
	config atomic.Pointer[Config]
	logger libLog.Logger
}

// NewConfigManager creates a ConfigManager that wraps the given initial config.
func NewConfigManager(cfg *Config, logger libLog.Logger) (*ConfigManager, error) {
	if cfg == nil {
		return nil, ErrConfigNil
	}

	if sharedPorts.IsNilValue(logger) {
		logger = &libLog.NopLogger{}
	}

	cm := &ConfigManager{
		logger: logger,
	}

	cm.config.Store(cfg)

	return cm, nil
}

// Get returns the current configuration. This is the hot path -- it uses an
// atomic load with zero locking overhead. Safe to call from any goroutine.
func (cm *ConfigManager) Get() *Config {
	if cm == nil {
		return nil
	}

	return cm.config.Load()
}

// Update atomically replaces the current configuration with a new one.
// Validates the new config before storing. Returns an error if cfg is nil
// or fails validation.
func (cm *ConfigManager) Update(cfg *Config) error {
	if cm == nil || cfg == nil {
		return ErrConfigNil
	}

	if err := cfg.Validate(); err != nil {
		return err
	}

	cm.config.Store(cfg)

	return nil
}

// Stop is a no-op retained for the shutdown ordering contract.
// ConfigManager has no background goroutines.
func (cm *ConfigManager) Stop() {}
