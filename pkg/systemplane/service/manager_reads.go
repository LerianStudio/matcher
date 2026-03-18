// Copyright 2025 Lerian Studio.

package service

import (
	"context"
	"fmt"

	"github.com/LerianStudio/matcher/pkg/systemplane/domain"
	"github.com/LerianStudio/matcher/pkg/systemplane/ports"
)

// GetConfigs returns resolved config values.
func (manager *defaultManager) GetConfigs(ctx context.Context) (ResolvedSet, error) {
	snap := manager.supervisor.Snapshot()
	if snap.BuiltAt.IsZero() || snap.Configs == nil {
		values, revision, err := manager.builder.BuildConfigs(ctx)
		if err != nil {
			return ResolvedSet{}, fmt.Errorf("get configs: %w", err)
		}

		return ResolvedSet{
			Values:   redactEffectiveValues(manager.registry, values),
			Revision: revision,
		}, nil
	}

	values := redactEffectiveValues(manager.registry, cloneEffectiveValues(snap.Configs))

	return ResolvedSet{
		Values:   values,
		Revision: revisionFromValues(values),
	}, nil
}

// GetSettings returns resolved setting values for the requested subject.
func (manager *defaultManager) GetSettings(ctx context.Context, subject Subject) (ResolvedSet, error) {
	snap := manager.supervisor.Snapshot()

	if !snap.BuiltAt.IsZero() {
		resolved, ok := manager.cachedSettingsFromSnapshot(snap, subject)
		if ok {
			return resolved, nil
		}
	}

	values, revision, err := manager.builder.BuildSettings(ctx, subject)
	if err != nil {
		return ResolvedSet{}, fmt.Errorf("get settings: %w", err)
	}

	return ResolvedSet{
		Values:   redactEffectiveValues(manager.registry, values),
		Revision: revision,
	}, nil
}

func (manager *defaultManager) cachedSettingsFromSnapshot(snapshot domain.Snapshot, subject Subject) (ResolvedSet, bool) {
	switch subject.Scope {
	case domain.ScopeGlobal:
		if snapshot.GlobalSettings == nil {
			return ResolvedSet{}, false
		}

		values := redactEffectiveValues(manager.registry, cloneEffectiveValues(snapshot.GlobalSettings))

		return ResolvedSet{Values: values, Revision: revisionFromValues(values)}, true
	case domain.ScopeTenant:
		if snapshot.TenantSettings == nil {
			return ResolvedSet{}, false
		}

		tenantValues, ok := snapshot.TenantSettings[subject.SubjectID]
		if !ok {
			return ResolvedSet{}, false
		}

		values := redactEffectiveValues(manager.registry, cloneEffectiveValues(tenantValues))

		return ResolvedSet{Values: values, Revision: revisionFromValues(values)}, true
	default:
		return ResolvedSet{}, false
	}
}

// GetConfigSchema returns metadata for all registered config keys.
func (manager *defaultManager) GetConfigSchema(_ context.Context) ([]SchemaEntry, error) {
	return buildSchema(manager.registry, domain.KindConfig), nil
}

// GetSettingSchema returns metadata for all registered setting keys.
func (manager *defaultManager) GetSettingSchema(_ context.Context) ([]SchemaEntry, error) {
	return buildSchema(manager.registry, domain.KindSetting), nil
}

// GetConfigHistory retrieves redacted change history for configs.
func (manager *defaultManager) GetConfigHistory(ctx context.Context, filter ports.HistoryFilter) ([]ports.HistoryEntry, error) {
	filter.Kind = domain.KindConfig

	entries, err := manager.history.ListHistory(ctx, filter)
	if err != nil {
		return nil, fmt.Errorf("get config history: %w", err)
	}

	return redactHistoryEntries(manager.registry, entries), nil
}

// GetSettingHistory retrieves redacted change history for settings.
func (manager *defaultManager) GetSettingHistory(ctx context.Context, filter ports.HistoryFilter) ([]ports.HistoryEntry, error) {
	filter.Kind = domain.KindSetting

	entries, err := manager.history.ListHistory(ctx, filter)
	if err != nil {
		return nil, fmt.Errorf("get setting history: %w", err)
	}

	return redactHistoryEntries(manager.registry, entries), nil
}

// Resync triggers a full reload of the supervisor.
func (manager *defaultManager) Resync(ctx context.Context) error {
	if err := manager.supervisor.Reload(ctx, "resync"); err != nil {
		return fmt.Errorf("resync: %w", err)
	}

	return nil
}
