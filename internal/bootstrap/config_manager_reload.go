// Copyright 2025 Lerian Studio. All rights reserved.
// Use of this source code is governed by an Elastic License 2.0
// that can be found in the LICENSE.md file.

package bootstrap

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"time"

	libLog "github.com/LerianStudio/lib-commons/v4/commons/log"
)

// errConfigManagerStopped is returned when a reload is attempted after the manager has stopped.
var (
	errConfigManagerStopped       = errors.New("config manager stopped")
	errImmutableReloadKeysChanged = errors.New("immutable keys changed via file reload")
)

var blockedFileReloadKeys = map[string]bool{
	"fetcher.enabled": true,
}

func (cm *ConfigManager) reload(source string) (*ReloadResult, error) {
	var (
		notifyCfg *Config
		callbacks []func(*Config) error
		oldCfg    *Config
	)

	cm.mu.Lock()
	locked := true

	defer func() {
		if locked {
			cm.mu.Unlock()
		}
	}()

	oldCfg = cm.config.Load()
	if oldCfg == nil {
		return nil, fmt.Errorf("config reload: %w", errConfigNilAtomicLoad)
	}

	oldVersion := cm.version.Load()
	oldReloadAt, _ := cm.lastReload.Load().(time.Time)
	oldChanges, _ := cm.lastChanges.Load().([]ConfigChange)
	oldSource, _ := cm.lastUpdateSource.Load().(string)

	result, err := cm.reloadLocked(source)
	if err != nil {
		return nil, err
	}

	notifyCfg = cm.config.Load()
	callbacks = cm.snapshotSubscribersLocked()
	cm.mu.Unlock()

	locked = false

	if notifyErr := cm.notifySubscribers(notifyCfg, callbacks); notifyErr != nil {
		cm.mu.Lock()
		cm.rollbackViperToConfigLocked(oldCfg, result.Changes)
		cm.config.Store(oldCfg)
		cm.version.Store(oldVersion)
		cm.lastReload.Store(oldReloadAt)
		cm.lastChanges.Store(oldChanges)
		cm.lastUpdateSource.Store(oldSource)
		cm.mu.Unlock()

		return result, fmt.Errorf("config reload: %w", notifyErr)
	}

	return result, nil
}

// reloadLocked performs the actual reload. Caller MUST hold cm.mu.
func (cm *ConfigManager) reloadLocked(source string) (*ReloadResult, error) {
	select {
	case <-cm.stopCh:
		return nil, errConfigManagerStopped
	default:
	}

	ctx := context.Background()

	if source == "" {
		source = configUpdateSourceReload
	}

	// Re-read the file into viper's store.
	if cm.filePath != "" {
		if err := cm.viper.ReadInConfig(); err != nil && !isConfigFileNotFound(err) {
			cm.logger.Log(ctx, libLog.LevelError, "config reload: failed to read YAML",
				libLog.String("path", cm.filePath), libLog.Err(err))

			return nil, fmt.Errorf("config reload: read YAML: %w", err)
		}
	}

	// Unmarshal into a fresh Config struct.
	newCfg := defaultConfig()
	if err := cm.viper.Unmarshal(newCfg); err != nil {
		cm.logger.Log(ctx, libLog.LevelError, "config reload: unmarshal failed", libLog.Err(err))

		return nil, fmt.Errorf("config reload: unmarshal: %w", err)
	}

	// Env-var overlay: preserves backward compat with direct env vars
	// (e.g., POSTGRES_HOST without MATCHER_ prefix).
	// loadConfigFromEnv uses SetConfigFromEnvVars which zeroes fields when
	// the corresponding env var is absent. We snapshot the viper-based config
	// BEFORE the overlay and restore any fields that got blanked.
	viperSnapshot := *newCfg
	if err := loadConfigFromEnv(newCfg); err != nil {
		cm.logger.Log(ctx, libLog.LevelError, "config reload: env overlay failed", libLog.Err(err))

		return nil, fmt.Errorf("config reload: env overlay: %w", err)
	}

	restoreZeroedFields(newCfg, &viperSnapshot)
	newCfg.normalizeTenancyConfig()

	// Carry forward the logger from the current config (it's not YAML-managed).
	oldCfg := cm.config.Load()
	if oldCfg == nil {
		return nil, fmt.Errorf("config reload: %w", errConfigNilAtomicLoad)
	}

	preserveStartupOnlyRuntimeSettings(newCfg, oldCfg)

	newCfg.Logger = oldCfg.Logger
	newCfg.ShutdownGracePeriod = oldCfg.ShutdownGracePeriod

	// Enforce body limit default before production security defaults.
	if newCfg.Server.BodyLimitBytes <= 0 {
		newCfg.Server.BodyLimitBytes = defaultHTTPBodyLimitBytes
	}

	// Production security enforcement (silent corrections with warnings).
	newCfg.enforceProductionSecurityDefaults(cm.logger)

	// Validate — reject bad config before swapping.
	if err := newCfg.Validate(); err != nil {
		cm.logger.Log(ctx, libLog.LevelError, "config reload: validation failed", libLog.Err(err))

		return nil, fmt.Errorf("config reload: validation: %w: %w", ErrConfigValidationFailure, err)
	}

	// Compute diff before swap.
	changes := diffConfigs(oldCfg, newCfg)
	if err := rejectImmutableReloadChanges(changes); err != nil {
		cm.logger.Log(ctx, libLog.LevelWarn, "config reload rejected: immutable keys changed", libLog.Err(err))

		return nil, fmt.Errorf("config reload: %w", err)
	}

	// Atomic swap — readers see the new config immediately.
	cm.config.Store(newCfg)

	now := time.Now().UTC()
	newVersion := cm.version.Add(1)
	cm.lastReload.Store(now)

	result := &ReloadResult{
		Version:         newVersion,
		ReloadedAt:      now,
		ChangesDetected: len(changes),
		Changes:         changes,
	}

	cm.logger.Log(ctx, libLog.LevelInfo, "config reloaded successfully",
		libLog.Int("version", safeUint64ToInt(newVersion)),
		libLog.Int("changes", len(changes)))

	// Store changes and source BEFORE notifying so subscribers can access them.
	cm.lastChanges.Store(changes)
	cm.lastUpdateSource.Store(source)

	return result, nil
}

func rejectImmutableReloadChanges(changes []ConfigChange) error {
	immutableKeys := make([]string, 0)

	for _, change := range changes {
		if !blockedFileReloadKeys[change.Key] {
			continue
		}

		immutableKeys = append(immutableKeys, change.Key)
	}

	if len(immutableKeys) == 0 {
		return nil
	}

	return fmt.Errorf("%w: %s", errImmutableReloadKeysChanged, strings.Join(immutableKeys, ", "))
}

// buildCandidateConfig unmarshals the current viper state into a new Config,
// applies env overlay + production security defaults, and validates. Returns
// the validated config or an error (caller should roll back viper keys on error).
func (cm *ConfigManager) buildCandidateConfig(oldCfg *Config) (*Config, error) {
	if oldCfg == nil {
		return nil, fmt.Errorf("buildCandidateConfig: %w", errConfigNilAtomicLoad)
	}

	candidateCfg := defaultConfig()
	if err := cm.viper.Unmarshal(candidateCfg); err != nil {
		return nil, fmt.Errorf("unmarshal: %w", err)
	}

	candidateCfg.Logger = oldCfg.Logger
	candidateCfg.ShutdownGracePeriod = oldCfg.ShutdownGracePeriod

	if candidateCfg.Server.BodyLimitBytes <= 0 {
		candidateCfg.Server.BodyLimitBytes = defaultHTTPBodyLimitBytes
	}

	// Apply env overlay for backward compatibility. See reloadLocked comment
	// for why we snapshot and restore.
	candidateSnapshot := *candidateCfg
	if err := loadConfigFromEnv(candidateCfg); err != nil {
		return nil, fmt.Errorf("env overlay: %w", err)
	}

	restoreZeroedFields(candidateCfg, &candidateSnapshot)
	candidateCfg.normalizeTenancyConfig()
	preserveStartupOnlyRuntimeSettings(candidateCfg, oldCfg)

	candidateCfg.enforceProductionSecurityDefaults(cm.logger)

	if err := candidateCfg.Validate(); err != nil {
		return nil, fmt.Errorf("validation failed: %w: %w", ErrConfigValidationFailure, err)
	}

	return candidateCfg, nil
}

// preserveStartupOnlyRuntimeSettings keeps settings that still require bootstrap-time
// construction stable across reload/update cycles. These values can change in YAML
// and take effect on the next process start, but they must not change the active
// runtime config for the running process.
func preserveStartupOnlyRuntimeSettings(candidateCfg, oldCfg *Config) {
	if candidateCfg == nil || oldCfg == nil {
		return
	}

	candidateCfg.ExportWorker.Enabled = oldCfg.ExportWorker.Enabled
	candidateCfg.CleanupWorker.Enabled = oldCfg.CleanupWorker.Enabled
	candidateCfg.Archival.Enabled = oldCfg.Archival.Enabled
	candidateCfg.Auth = oldCfg.Auth
	candidateCfg.Tenancy.DefaultTenantID = oldCfg.Tenancy.DefaultTenantID
	candidateCfg.Tenancy.DefaultTenantSlug = oldCfg.Tenancy.DefaultTenantSlug
	candidateCfg.Tenancy.MultiTenantEnabled = oldCfg.Tenancy.MultiTenantEnabled
	candidateCfg.Tenancy.MultiTenantURL = oldCfg.Tenancy.MultiTenantURL
	candidateCfg.Tenancy.MultiTenantEnvironment = oldCfg.Tenancy.MultiTenantEnvironment
	candidateCfg.Tenancy.MultiTenantMaxTenantPools = oldCfg.Tenancy.MultiTenantMaxTenantPools
	candidateCfg.Tenancy.MultiTenantIdleTimeoutSec = oldCfg.Tenancy.MultiTenantIdleTimeoutSec
	candidateCfg.Tenancy.MultiTenantCircuitBreakerThreshold = oldCfg.Tenancy.MultiTenantCircuitBreakerThreshold
	candidateCfg.Tenancy.MultiTenantCircuitBreakerTimeoutSec = oldCfg.Tenancy.MultiTenantCircuitBreakerTimeoutSec
	candidateCfg.Tenancy.MultiTenantServiceAPIKey = oldCfg.Tenancy.MultiTenantServiceAPIKey
	candidateCfg.Tenancy.MultiTenantInfraEnabled = oldCfg.Tenancy.MultiTenantInfraEnabled
	candidateCfg.ObjectStorage = oldCfg.ObjectStorage
	candidateCfg.Archival.StorageBucket = oldCfg.Archival.StorageBucket
	candidateCfg.Archival.StoragePrefix = oldCfg.Archival.StoragePrefix
	candidateCfg.Archival.StorageClass = oldCfg.Archival.StorageClass
	candidateCfg.Dedupe = oldCfg.Dedupe
}
