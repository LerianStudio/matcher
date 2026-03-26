package bootstrap

import (
	"context"
	"fmt"

	spdomain "github.com/LerianStudio/lib-commons/v4/commons/systemplane/domain"
	spports "github.com/LerianStudio/lib-commons/v4/commons/systemplane/ports"
	spservice "github.com/LerianStudio/lib-commons/v4/commons/systemplane/service"
)

type aliasAwareSystemplaneManager struct {
	delegate spservice.Manager
}

func newAliasAwareSystemplaneManager(delegate spservice.Manager) spservice.Manager {
	if delegate == nil {
		return nil
	}

	return &aliasAwareSystemplaneManager{delegate: delegate}
}

// GetConfigs returns resolved runtime configs plus legacy key aliases.
func (manager *aliasAwareSystemplaneManager) GetConfigs(ctx context.Context) (spservice.ResolvedSet, error) {
	resolved, err := manager.delegate.GetConfigs(ctx)
	if err != nil {
		return spservice.ResolvedSet{}, fmt.Errorf("get configs: %w", err)
	}

	resolved.Values = appendLegacyAliasesToResolvedSet(resolved.Values)

	return resolved, nil
}

// GetSettings delegates settings reads without alias translation.
func (manager *aliasAwareSystemplaneManager) GetSettings(ctx context.Context, subject spservice.Subject) (spservice.ResolvedSet, error) {
	resolved, err := manager.delegate.GetSettings(ctx, subject)
	if err != nil {
		return spservice.ResolvedSet{}, fmt.Errorf("get settings: %w", err)
	}

	return resolved, nil
}

// PatchConfigs normalizes legacy key aliases before writing config updates.
func (manager *aliasAwareSystemplaneManager) PatchConfigs(ctx context.Context, req spservice.PatchRequest) (spservice.WriteResult, error) {
	resolved, err := manager.delegate.GetConfigs(ctx)
	if err != nil {
		return spservice.WriteResult{}, fmt.Errorf("get configs before patch: %w", err)
	}

	req.Ops = normalizePatchOps(req.Ops, resolved.Values)

	result, patchErr := manager.delegate.PatchConfigs(ctx, req)
	if patchErr != nil {
		return spservice.WriteResult{}, fmt.Errorf("patch configs: %w", patchErr)
	}

	return result, nil
}

// PatchSettings delegates settings writes without alias translation.
func (manager *aliasAwareSystemplaneManager) PatchSettings(ctx context.Context, subject spservice.Subject, req spservice.PatchRequest) (spservice.WriteResult, error) {
	result, err := manager.delegate.PatchSettings(ctx, subject, req)
	if err != nil {
		return spservice.WriteResult{}, fmt.Errorf("patch settings: %w", err)
	}

	return result, nil
}

// GetConfigSchema returns the config schema plus legacy alias entries.
func (manager *aliasAwareSystemplaneManager) GetConfigSchema(ctx context.Context) ([]spservice.SchemaEntry, error) {
	entries, err := manager.delegate.GetConfigSchema(ctx)
	if err != nil {
		return nil, fmt.Errorf("get config schema: %w", err)
	}

	return appendLegacyAliasesToSchema(entries), nil
}

// GetSettingSchema delegates setting schema reads without alias translation.
func (manager *aliasAwareSystemplaneManager) GetSettingSchema(ctx context.Context) ([]spservice.SchemaEntry, error) {
	entries, err := manager.delegate.GetSettingSchema(ctx)
	if err != nil {
		return nil, fmt.Errorf("get setting schema: %w", err)
	}

	return entries, nil
}

// GetConfigHistory reads config history and remaps canonical keys back to requested legacy aliases.
func (manager *aliasAwareSystemplaneManager) GetConfigHistory(ctx context.Context, filter spports.HistoryFilter) ([]spports.HistoryEntry, error) {
	requestedLegacyKey := filter.Key
	if canonicalKey, ok := canonicalConfigKey(filter.Key); ok {
		filter.Key = canonicalKey
	}

	history, err := manager.delegate.GetConfigHistory(ctx, filter)
	if err != nil {
		return nil, fmt.Errorf("get config history: %w", err)
	}

	if _, ok := canonicalConfigKey(requestedLegacyKey); ok {
		for idx := range history {
			history[idx].Key = requestedLegacyKey
		}
	}

	return history, nil
}

// GetSettingHistory delegates settings history reads without alias translation.
func (manager *aliasAwareSystemplaneManager) GetSettingHistory(ctx context.Context, filter spports.HistoryFilter) ([]spports.HistoryEntry, error) {
	history, err := manager.delegate.GetSettingHistory(ctx, filter)
	if err != nil {
		return nil, fmt.Errorf("get setting history: %w", err)
	}

	return history, nil
}

// ApplyChangeSignal forwards change-feed signals to the delegate manager.
func (manager *aliasAwareSystemplaneManager) ApplyChangeSignal(ctx context.Context, signal spports.ChangeSignal) error {
	if err := manager.delegate.ApplyChangeSignal(ctx, signal); err != nil {
		return fmt.Errorf("apply change signal: %w", err)
	}

	return nil
}

// Resync forces the delegate manager to rebuild and apply the latest snapshot.
func (manager *aliasAwareSystemplaneManager) Resync(ctx context.Context) error {
	if err := manager.delegate.Resync(ctx); err != nil {
		return fmt.Errorf("resync manager: %w", err)
	}

	return nil
}

func appendLegacyAliasesToResolvedSet(values map[string]spdomain.EffectiveValue) map[string]spdomain.EffectiveValue {
	if values == nil {
		return nil
	}

	result := make(map[string]spdomain.EffectiveValue, len(values)+len(configKeyAliases))
	for key, value := range values {
		result[key] = value
	}

	for canonicalKey, legacyKey := range configKeyAliases {
		if _, exists := result[legacyKey]; exists {
			continue
		}

		if value, exists := result[canonicalKey]; exists {
			aliased := value
			aliased.Key = legacyKey
			result[legacyKey] = aliased
		}
	}

	return result
}

func appendLegacyAliasesToSchema(entries []spservice.SchemaEntry) []spservice.SchemaEntry {
	if len(entries) == 0 {
		return entries
	}

	result := make([]spservice.SchemaEntry, 0, len(entries)+len(configKeyAliases))

	seen := make(map[string]struct{}, len(entries)+len(configKeyAliases))
	for _, entry := range entries {
		result = append(result, entry)
		seen[entry.Key] = struct{}{}
	}

	for _, entry := range entries {
		legacyKey, ok := legacyConfigKey(entry.Key)
		if !ok {
			continue
		}

		if _, exists := seen[legacyKey]; exists {
			continue
		}

		aliased := entry
		aliased.Key = legacyKey
		result = append(result, aliased)
		seen[legacyKey] = struct{}{}
	}

	return result
}

func normalizePatchOps(ops []spports.WriteOp, currentValues map[string]spdomain.EffectiveValue) []spports.WriteOp {
	if len(ops) == 0 {
		return ops
	}

	normalized := make([]spports.WriteOp, len(ops))
	for idx, op := range ops {
		normalized[idx] = op

		canonicalKey, legacyRequest := canonicalConfigKey(op.Key)
		if !legacyRequest {
			canonicalKey = op.Key
		}

		legacyKey, hasLegacyAlias := legacyConfigKey(canonicalKey)
		if hasLegacyAlias {
			_, hasCanonicalValue := currentValues[canonicalKey]

			_, hasLegacyValue := currentValues[legacyKey]
			if hasLegacyValue && !hasCanonicalValue {
				normalized[idx].Key = legacyKey
				continue
			}
		}

		normalized[idx].Key = canonicalKey
	}

	return normalized
}

var _ spservice.Manager = (*aliasAwareSystemplaneManager)(nil)
