// Copyright 2025 Lerian Studio. All rights reserved.
// Use of this source code is governed by an Elastic License 2.0
// that can be found in the LICENSE.md file.

package bootstrap

import (
	"context"
	"errors"
	"fmt"
	"math"
	"sort"
	"time"

	"github.com/spf13/viper"

	libLog "github.com/LerianStudio/lib-commons/v4/commons/log"
)

type configUpdateState struct {
	config     *Config
	version    uint64
	reloadedAt time.Time
	changes    []ConfigChange
	source     string
	oldValues  map[string]any
}

// Update applies programmatic changes to the config. Each key is validated:
//   - Must be a known mutable key (in mutableConfigKeys)
//   - Must pass overall config validation after applying
//
// Changes are written to the YAML file via atomic rename, then Reload() is
// triggered to pick them up through the normal pipeline.
func (cm *ConfigManager) Update(changes map[string]any) (*UpdateResult, error) {
	var (
		notifyCfg         *Config
		callbacks         []func(*Config) error
		persistedSnapshot *viper.Viper
	)

	cm.mu.Lock()
	locked := true

	defer func() {
		if locked {
			cm.mu.Unlock()
		}
	}()

	ctx := context.Background()
	result := &UpdateResult{}

	if len(changes) == 0 {
		return result, nil
	}

	applicableChanges := prepareApplicableChanges(changes, result)
	if len(applicableChanges) == 0 {
		return result, nil
	}

	// Phase 2: apply to viper and compute old values.
	updateState, err := cm.snapshotUpdateState(applicableChanges)
	if err != nil {
		return nil, fmt.Errorf("config update: %w", err)
	}

	for _, key := range sortedChangeKeys(applicableChanges) {
		value := applicableChanges[key]
		updateState.oldValues[key] = cm.viper.Get(key)
		cm.viper.Set(key, value)
	}

	// Phase 3: build, overlay, and validate the candidate config.
	candidateCfg, err := cm.buildCandidateConfig(updateState.config)
	if err != nil {
		cm.rollbackViperKeysLocked(applicableChanges, updateState.oldValues)

		return nil, fmt.Errorf("config update: %w", err)
	}

	// Phase 4: write YAML via atomic rename (temp file + rename).
	if cm.filePath != "" {
		persistedSnapshot, err = cm.snapshotPersistedConfig()
		if err != nil {
			cm.rollbackViperKeysLocked(applicableChanges, updateState.oldValues)

			return nil, fmt.Errorf("config update: snapshot persisted config: %w", err)
		}

		if err := cm.writePersistedConfigAtomically(applicableChanges); err != nil {
			cm.rollbackViperKeysLocked(applicableChanges, updateState.oldValues)

			cm.logger.Log(ctx, libLog.LevelError, "config update: YAML write failed", libLog.Err(err))

			return nil, fmt.Errorf("config update: write YAML: %w", err)
		}
	}

	// Phase 5: do the swap (same as reload but we already have the validated config).
	cm.config.Store(candidateCfg)

	now := time.Now().UTC()
	newVersion := cm.version.Add(1)
	cm.lastReload.Store(now)

	// Build applied/rejected results and derive config changes for subscribers.
	configChanges := buildUpdateResults(applicableChanges, updateState.oldValues, updateState.config, candidateCfg, result)

	cm.logger.Log(ctx, libLog.LevelInfo, "config updated via API",
		libLog.Int("version", safeUint64ToInt(newVersion)),
		libLog.Int("applied", len(result.Applied)),
		libLog.Int("rejected", len(result.Rejected)))

	cm.lastChanges.Store(configChanges)

	// Mark source as API so the audit subscriber skips (API handler publishes its own event).
	cm.lastUpdateSource.Store(configUpdateSourceAPI)

	notifyCfg = candidateCfg
	callbacks = cm.snapshotSubscribersLocked()
	cm.mu.Unlock()

	locked = false

	if notifyErr := cm.notifySubscribers(notifyCfg, callbacks); notifyErr != nil {
		rollbackErr := cm.rollbackAfterNotifyFailure(applicableChanges, updateState, persistedSnapshot)
		if rollbackErr != nil {
			return result, fmt.Errorf("config update: %w", errors.Join(notifyErr, fmt.Errorf("rollback persisted config: %w", rollbackErr)))
		}

		return result, fmt.Errorf("config update: %w", notifyErr)
	}

	return result, nil
}

func (cm *ConfigManager) snapshotUpdateState(applicableChanges map[string]any) (*configUpdateState, error) {
	oldCfg := cm.config.Load()
	if oldCfg == nil {
		return nil, errConfigNilAtomicLoad
	}

	oldReloadAt, _ := cm.lastReload.Load().(time.Time)
	oldChanges, _ := cm.lastChanges.Load().([]ConfigChange)
	oldSource, _ := cm.lastUpdateSource.Load().(string)

	return &configUpdateState{
		config:     oldCfg,
		version:    cm.version.Load(),
		reloadedAt: oldReloadAt,
		changes:    oldChanges,
		source:     oldSource,
		oldValues:  make(map[string]any, len(applicableChanges)),
	}, nil
}

func (cm *ConfigManager) rollbackAfterNotifyFailure(
	applicableChanges map[string]any,
	state *configUpdateState,
	persistedSnapshot *viper.Viper,
) error {
	cm.mu.Lock()
	cm.rollbackViperKeysLocked(applicableChanges, state.oldValues)
	cm.config.Store(state.config)
	cm.version.Store(state.version)
	cm.lastReload.Store(state.reloadedAt)
	cm.lastChanges.Store(state.changes)
	cm.lastUpdateSource.Store(state.source)

	var rollbackErr error
	if persistedSnapshot != nil {
		rollbackErr = writeViperConfigAtomically(persistedSnapshot, cm.filePath)
	}
	cm.mu.Unlock()

	return rollbackErr
}

func prepareApplicableChanges(changes map[string]any, result *UpdateResult) map[string]any {
	applicableChanges := classifyApplicableChanges(changes, result)
	if len(applicableChanges) == 0 {
		return applicableChanges
	}

	rejectTypeErrors(applicableChanges, result)

	return applicableChanges
}

// rejectTypeErrors validates value types against the config schema and rejects
// mismatched entries. Mutates applicableChanges (deletes rejected keys) and
// appends rejections to result.
func rejectTypeErrors(applicableChanges map[string]any, result *UpdateResult) {
	schema := buildConfigSchema()
	schemaByKey := make(map[string]configFieldDef, len(schema))

	for _, def := range schema {
		schemaByKey[def.Key] = def
	}

	for _, key := range sortedChangeKeys(applicableChanges) {
		value := applicableChanges[key]

		def, ok := schemaByKey[key]
		if !ok {
			continue // key not in schema — let viper handle it
		}

		if !isValueTypeCompatible(value, def.Type) {
			result.Rejected = append(result.Rejected, ConfigChangeRejection{
				Key:    key,
				Value:  redactIfSensitive(key, value),
				Reason: "type mismatch: expected " + def.Type,
			})

			delete(applicableChanges, key)
		}
	}
}

func classifyApplicableChanges(changes map[string]any, result *UpdateResult) map[string]any {
	applicableChanges := make(map[string]any, len(changes))

	for _, key := range sortedChangeKeys(changes) {
		value := changes[key]
		if !mutableConfigKeys[key] {
			result.Rejected = append(result.Rejected, ConfigChangeRejection{
				Key:    key,
				Value:  redactIfSensitive(key, value),
				Reason: "key is not mutable via API (env-only or infrastructure-bound)",
			})

			continue
		}

		applicableChanges[key] = value
	}

	return applicableChanges
}

// buildUpdateResults classifies each applicable change as applied or rejected
// (env-overridden) and returns the derived ConfigChange slice for subscriber notification.
func buildUpdateResults(
	applicableChanges map[string]any,
	oldValues map[string]any,
	oldCfg, candidateCfg *Config,
	result *UpdateResult,
) []ConfigChange {
	for _, key := range sortedChangeKeys(applicableChanges) {
		requested := applicableChanges[key]
		effectiveOld, _ := resolveConfigValue(oldCfg, key)
		effectiveNew, _ := resolveConfigValue(candidateCfg, key)
		hotReloaded := !valuesEquivalent(effectiveOld, effectiveNew)

		if !hotReloaded && !valuesEquivalent(requested, effectiveNew) {
			oldPersisted := oldValues[key]
			result.Applied = append(result.Applied, ConfigChangeResult{
				Key:         key,
				OldValue:    redactIfSensitive(key, oldPersisted),
				NewValue:    redactIfSensitive(key, requested),
				HotReloaded: false,
			})

			continue
		}

		result.Applied = append(result.Applied, ConfigChangeResult{
			Key:         key,
			OldValue:    redactIfSensitive(key, effectiveOld),
			NewValue:    redactIfSensitive(key, effectiveNew),
			HotReloaded: hotReloaded,
		})
	}

	configChanges := make([]ConfigChange, 0, len(result.Applied))

	for _, appliedChange := range result.Applied {
		configChanges = append(configChanges, ConfigChange{
			Key:      appliedChange.Key,
			OldValue: appliedChange.OldValue,
			NewValue: appliedChange.NewValue,
		})
	}

	return configChanges
}

func sortedChangeKeys(values map[string]any) []string {
	keys := make([]string, 0, len(values))
	for key := range values {
		keys = append(keys, key)
	}

	sort.Strings(keys)

	return keys
}

// rollbackViperKeysLocked restores the given viper keys to their previous values.
// Used by Update() to undo partial changes when validation fails.
// Caller MUST hold cm.mu.
func (cm *ConfigManager) rollbackViperKeysLocked(keys, oldValues map[string]any) {
	for key := range keys {
		cm.viper.Set(key, oldValues[key])
	}
}

func (cm *ConfigManager) rollbackViperToConfigLocked(cfg *Config, changes []ConfigChange) {
	if cfg == nil {
		return
	}

	for _, change := range changes {
		if change.Key == "" {
			continue
		}

		value, ok := resolveConfigValue(cfg, change.Key)
		if !ok {
			cm.viper.Set(change.Key, nil)
			continue
		}

		cm.viper.Set(change.Key, value)
	}
}

// isValueTypeCompatible checks if a JSON-deserialized value is compatible with
// the expected schema type. JSON numbers can be float64 or json.Number; both
// are accepted for int fields since viper handles the conversion.
func isValueTypeCompatible(value any, expectedType string) bool {
	if value == nil {
		return false
	}

	switch expectedType {
	case "string":
		_, ok := value.(string)
		return ok
	case "int":
		switch typedValue := value.(type) {
		case int, int64:
			return true
		case float64:
			return !math.IsNaN(typedValue) && !math.IsInf(typedValue, 0) && math.Trunc(typedValue) == typedValue
		default:
			return false
		}
	case "bool":
		_, ok := value.(bool)
		return ok
	default:
		return true // unknown type — let viper handle it
	}
}
