package bootstrap

import (
	"context"
	"errors"
	"fmt"
	"time"

	libLog "github.com/LerianStudio/lib-commons/v4/commons/log"
)

func (cm *ConfigManager) reload(source string) (*ReloadResult, error) {
	var (
		notifyCfg *Config
		callbacks []func(*Config)
	)

	cm.mu.Lock()
	defer func() {
		cm.mu.Unlock()

		if notifyCfg != nil {
			cm.notifySubscribers(notifyCfg, callbacks)
		}
	}()

	result, err := cm.reloadLocked(source)
	if err != nil {
		return nil, err
	}

	notifyCfg = cm.config.Load()
	callbacks = cm.snapshotSubscribersLocked()

	return result, nil
}

// reloadLocked performs the actual reload. Caller MUST hold cm.mu.
func (cm *ConfigManager) reloadLocked(source string) (*ReloadResult, error) {
	select {
	case <-cm.stopCh:
		return nil, errors.New("config manager stopped")
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

	// Carry forward the logger from the current config (it's not YAML-managed).
	oldCfg := cm.config.Load()
	if oldCfg == nil {
		return nil, fmt.Errorf("config reload: %w", errConfigNilAtomicLoad)
	}

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

		return nil, fmt.Errorf("config reload: validation: %w", err)
	}

	// Compute diff before swap.
	changes := diffConfigs(oldCfg, newCfg)

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

	candidateCfg.enforceProductionSecurityDefaults(cm.logger)

	if err := candidateCfg.Validate(); err != nil {
		return nil, fmt.Errorf("validation failed: %w", err)
	}

	return candidateCfg, nil
}
