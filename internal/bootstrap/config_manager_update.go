package bootstrap

import (
	"context"
	"fmt"
	"sort"
	"time"

	libLog "github.com/LerianStudio/lib-commons/v4/commons/log"
)

// Update applies programmatic changes to the config. Each key is validated:
//   - Must be a known mutable key (in mutableConfigKeys)
//   - Must pass overall config validation after applying
//
// Changes are written to the YAML file via atomic rename, then Reload() is
// triggered to pick them up through the normal pipeline.
func (cm *ConfigManager) Update(changes map[string]any) (*UpdateResult, error) {
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

	ctx := context.Background()
	result := &UpdateResult{}

	if len(changes) == 0 {
		return result, nil
	}

	// Phase 1: classify changes as applicable or rejected.
	applicableChanges := classifyApplicableChanges(changes, result)

	if len(applicableChanges) == 0 {
		return result, nil
	}

	// Phase 1.5: validate value types against schema expectations.
	rejectTypeErrors(applicableChanges, result)

	if len(applicableChanges) == 0 {
		return result, nil
	}

	// Phase 2: apply to viper and compute old values.
	oldCfg := cm.config.Load()
	if oldCfg == nil {
		return nil, fmt.Errorf("config update: %w", errConfigNilAtomicLoad)
	}

	oldValues := make(map[string]any, len(applicableChanges))

	for _, key := range sortedChangeKeys(applicableChanges) {
		value := applicableChanges[key]
		oldValues[key] = cm.viper.Get(key)
		cm.viper.Set(key, value)
	}

	// Phase 3: build, overlay, and validate the candidate config.
	candidateCfg, err := cm.buildCandidateConfig(oldCfg)
	if err != nil {
		cm.rollbackViperKeys(applicableChanges, oldValues)

		return nil, fmt.Errorf("config update: %w", err)
	}

	// Phase 4: write YAML via atomic rename (temp file + rename).
	if cm.filePath != "" {
		if err := cm.writeConfigAtomically(); err != nil {
			cm.rollbackViperKeys(applicableChanges, oldValues)

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
	configChanges := buildUpdateResults(applicableChanges, oldCfg, candidateCfg, result)

	cm.logger.Log(ctx, libLog.LevelInfo, "config updated via API",
		libLog.Int("version", safeUint64ToInt(newVersion)),
		libLog.Int("applied", len(result.Applied)),
		libLog.Int("rejected", len(result.Rejected)))

	cm.lastChanges.Store(configChanges)

	// Mark source as API so the audit subscriber skips (API handler publishes its own event).
	cm.lastUpdateSource.Store(configUpdateSourceAPI)

	notifyCfg = candidateCfg
	callbacks = cm.snapshotSubscribersLocked()

	return result, nil
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
				Value:  value,
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
				Value:  value,
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
	oldCfg, candidateCfg *Config,
	result *UpdateResult,
) []ConfigChange {
	for _, key := range sortedChangeKeys(applicableChanges) {
		requested := applicableChanges[key]
		effectiveOld, _ := resolveConfigValue(oldCfg, key)
		effectiveNew, _ := resolveConfigValue(candidateCfg, key)
		hotReloaded := !valuesEquivalent(effectiveOld, effectiveNew)

		if !hotReloaded && !valuesEquivalent(requested, effectiveNew) {
			result.Rejected = append(result.Rejected, ConfigChangeRejection{
				Key:    key,
				Value:  redactIfSensitive(key, requested),
				Reason: "overridden by environment variable; value persisted but not effective at runtime",
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

// rollbackViperKeys restores the given viper keys to their previous values.
// Used by Update() to undo partial changes when validation fails.
func (cm *ConfigManager) rollbackViperKeys(keys, oldValues map[string]any) {
	for key := range keys {
		cm.viper.Set(key, oldValues[key])
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
		switch value.(type) {
		case int, int64, float64: // JSON numbers deserialize as float64
			return true
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
