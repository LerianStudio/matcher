// Copyright 2025 Lerian Studio.

package service

import (
	"context"
	"fmt"

	libCommons "github.com/LerianStudio/lib-commons/v4/commons"
	libOpentelemetry "github.com/LerianStudio/lib-commons/v4/commons/opentelemetry"

	"github.com/LerianStudio/matcher/pkg/systemplane/domain"
	"github.com/LerianStudio/matcher/pkg/systemplane/ports"
)

// GetConfigs returns resolved config values.
func (manager *defaultManager) GetConfigs(ctx context.Context) (ResolvedSet, error) {
	_, tracer, _, _ := libCommons.NewTrackingFromContext(ctx) //nolint:dogsled

	ctx, span := tracer.Start(ctx, "systemplane.manager.get_configs")
	defer span.End()

	snap := manager.supervisor.Snapshot()
	if snap.BuiltAt.IsZero() || snap.Configs == nil {
		values, revision, err := manager.builder.BuildConfigs(ctx)
		if err != nil {
			libOpentelemetry.HandleSpanError(span, "build configs", err)
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
	_, tracer, _, _ := libCommons.NewTrackingFromContext(ctx) //nolint:dogsled

	ctx, span := tracer.Start(ctx, "systemplane.manager.get_settings")
	defer span.End()

	snap := manager.supervisor.Snapshot()

	if !snap.BuiltAt.IsZero() {
		resolved, ok := manager.cachedSettingsFromSnapshot(snap, subject)
		if ok {
			return resolved, nil
		}
	}

	values, revision, err := manager.builder.BuildSettings(ctx, subject)
	if err != nil {
		libOpentelemetry.HandleSpanError(span, "build settings", err)
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
func (manager *defaultManager) GetConfigSchema(ctx context.Context) ([]SchemaEntry, error) {
	_, tracer, _, _ := libCommons.NewTrackingFromContext(ctx) //nolint:dogsled

	_, span := tracer.Start(ctx, "systemplane.manager.get_config_schema")
	defer span.End()

	return buildSchema(manager.registry, domain.KindConfig), nil
}

// GetSettingSchema returns metadata for all registered setting keys.
func (manager *defaultManager) GetSettingSchema(ctx context.Context) ([]SchemaEntry, error) {
	_, tracer, _, _ := libCommons.NewTrackingFromContext(ctx) //nolint:dogsled

	_, span := tracer.Start(ctx, "systemplane.manager.get_setting_schema")
	defer span.End()

	return buildSchema(manager.registry, domain.KindSetting), nil
}

// GetConfigHistory retrieves redacted change history for configs.
func (manager *defaultManager) GetConfigHistory(ctx context.Context, filter ports.HistoryFilter) ([]ports.HistoryEntry, error) {
	_, tracer, _, _ := libCommons.NewTrackingFromContext(ctx) //nolint:dogsled

	ctx, span := tracer.Start(ctx, "systemplane.manager.get_config_history")
	defer span.End()

	filter.Kind = domain.KindConfig

	entries, err := manager.history.ListHistory(ctx, filter)
	if err != nil {
		libOpentelemetry.HandleSpanError(span, "list config history", err)
		return nil, fmt.Errorf("get config history: %w", err)
	}

	return redactHistoryEntries(manager.registry, entries), nil
}

// GetSettingHistory retrieves redacted change history for settings.
func (manager *defaultManager) GetSettingHistory(ctx context.Context, filter ports.HistoryFilter) ([]ports.HistoryEntry, error) {
	_, tracer, _, _ := libCommons.NewTrackingFromContext(ctx) //nolint:dogsled

	ctx, span := tracer.Start(ctx, "systemplane.manager.get_setting_history")
	defer span.End()

	filter.Kind = domain.KindSetting

	entries, err := manager.history.ListHistory(ctx, filter)
	if err != nil {
		libOpentelemetry.HandleSpanError(span, "list setting history", err)
		return nil, fmt.Errorf("get setting history: %w", err)
	}

	return redactHistoryEntries(manager.registry, entries), nil
}

// Resync triggers a full reload of the supervisor.
func (manager *defaultManager) Resync(ctx context.Context) error {
	_, tracer, _, _ := libCommons.NewTrackingFromContext(ctx) //nolint:dogsled

	ctx, span := tracer.Start(ctx, "systemplane.manager.resync")
	defer span.End()

	if err := manager.supervisor.Reload(ctx, "resync"); err != nil {
		libOpentelemetry.HandleSpanError(span, "reload supervisor", err)
		return fmt.Errorf("resync: %w", err)
	}

	return nil
}
