// Copyright 2025 Lerian Studio.

package service

import (
	"context"
	"errors"
	"fmt"
	"sort"
	"time"

	libCommons "github.com/LerianStudio/lib-commons/v4/commons"
	libOpentelemetry "github.com/LerianStudio/lib-commons/v4/commons/opentelemetry"

	"github.com/LerianStudio/matcher/pkg/systemplane/domain"
	"github.com/LerianStudio/matcher/pkg/systemplane/ports"
	"github.com/LerianStudio/matcher/pkg/systemplane/registry"
)

var (
	errBuilderRegistryRequired = errors.New("new snapshot builder: registry is required")
	errBuilderStoreRequired    = errors.New("new snapshot builder: store is required")
)

// SnapshotBuilder merges registry defaults with store overrides to produce
// immutable snapshot values.
type SnapshotBuilder struct {
	registry registry.Registry
	store    ports.Store
}

// NewSnapshotBuilder creates a new SnapshotBuilder with the given registry and
// store dependencies. Both are required; a nil dependency causes a
// construction-time error rather than a runtime panic on first use.
func NewSnapshotBuilder(reg registry.Registry, store ports.Store) (*SnapshotBuilder, error) {
	if domain.IsNilValue(reg) {
		return nil, errBuilderRegistryRequired
	}

	if domain.IsNilValue(store) {
		return nil, errBuilderStoreRequired
	}

	return &SnapshotBuilder{registry: reg, store: store}, nil
}

// BuildConfigs builds the config portion of a snapshot by starting with all
// KindConfig defaults from the registry and then overlaying global overrides
// from the store.
func (builder *SnapshotBuilder) BuildConfigs(ctx context.Context) (map[string]domain.EffectiveValue, domain.Revision, error) {
	_, tracer, _, _ := libCommons.NewTrackingFromContext(ctx) //nolint:dogsled

	ctx, span := tracer.Start(ctx, "systemplane.snapshot_builder.build_configs")
	defer span.End()

	defs := builder.registry.List(domain.KindConfig)
	effective := initDefaults(defs)

	target, err := domain.NewTarget(domain.KindConfig, domain.ScopeGlobal, "")
	if err != nil {
		libOpentelemetry.HandleSpanError(span, "build config target", err)
		return nil, domain.RevisionZero, fmt.Errorf("build config target: %w", err)
	}

	result, err := builder.store.Get(ctx, target)
	if err != nil {
		libOpentelemetry.HandleSpanError(span, "load config overrides", err)
		return nil, domain.RevisionZero, fmt.Errorf("get config overrides: %w", err)
	}

	applyOverrides(effective, result.Entries, "global-override")
	setRevision(effective, result.Revision)

	return effective, result.Revision, nil
}

// BuildGlobalSettings builds global settings using defaults plus global
// overrides only.
func (builder *SnapshotBuilder) BuildGlobalSettings(ctx context.Context) (map[string]domain.EffectiveValue, domain.Revision, error) {
	_, tracer, _, _ := libCommons.NewTrackingFromContext(ctx) //nolint:dogsled

	ctx, span := tracer.Start(ctx, "systemplane.snapshot_builder.build_global_settings")
	defer span.End()

	defs := filterDefsByScope(builder.registry.List(domain.KindSetting), domain.ScopeGlobal)
	effective := initDefaults(defs)

	target, err := domain.NewTarget(domain.KindSetting, domain.ScopeGlobal, "")
	if err != nil {
		libOpentelemetry.HandleSpanError(span, "build global settings target", err)
		return nil, domain.RevisionZero, fmt.Errorf("build global setting target: %w", err)
	}

	result, err := builder.store.Get(ctx, target)
	if err != nil {
		libOpentelemetry.HandleSpanError(span, "load global setting overrides", err)
		return nil, domain.RevisionZero, fmt.Errorf("get global setting overrides: %w", err)
	}

	applyOverrides(effective, result.Entries, "global-override")
	setRevision(effective, result.Revision)

	return effective, result.Revision, nil
}

// BuildSettings builds effective settings for the requested subject.
func (builder *SnapshotBuilder) BuildSettings(ctx context.Context, subject Subject) (map[string]domain.EffectiveValue, domain.Revision, error) {
	_, tracer, _, _ := libCommons.NewTrackingFromContext(ctx) //nolint:dogsled

	ctx, span := tracer.Start(ctx, "systemplane.snapshot_builder.build_settings")
	defer span.End()

	switch subject.Scope {
	case domain.ScopeGlobal:
		return builder.BuildGlobalSettings(ctx)
	case domain.ScopeTenant:
		return builder.buildTenantSettings(ctx, subject.SubjectID)
	default:
		libOpentelemetry.HandleSpanError(span, "build settings scope", domain.ErrScopeInvalid)
		return nil, domain.RevisionZero, fmt.Errorf("build settings scope %q: %w", subject.Scope, domain.ErrScopeInvalid)
	}
}

func (builder *SnapshotBuilder) buildTenantSettings(ctx context.Context, tenantID string) (map[string]domain.EffectiveValue, domain.Revision, error) {
	defs := filterDefsByScope(builder.registry.List(domain.KindSetting), domain.ScopeTenant)
	effective := initDefaults(defs)

	globalTarget, err := domain.NewTarget(domain.KindSetting, domain.ScopeGlobal, "")
	if err != nil {
		return nil, domain.RevisionZero, fmt.Errorf("build global setting target: %w", err)
	}

	globalResult, err := builder.store.Get(ctx, globalTarget)
	if err != nil {
		return nil, domain.RevisionZero, fmt.Errorf("get global setting overrides: %w", err)
	}

	applyOverrides(effective, globalResult.Entries, "global-override")

	tenantTarget, err := domain.NewTarget(domain.KindSetting, domain.ScopeTenant, tenantID)
	if err != nil {
		return nil, domain.RevisionZero, fmt.Errorf("build tenant setting target: %w", err)
	}

	tenantResult, err := builder.store.Get(ctx, tenantTarget)
	if err != nil {
		return nil, domain.RevisionZero, fmt.Errorf("get tenant setting overrides: %w", err)
	}

	applyOverrides(effective, tenantResult.Entries, "tenant-override")
	setRevision(effective, tenantResult.Revision)

	return effective, tenantResult.Revision, nil
}

// BuildFull builds a complete snapshot with configs, global settings, and any
// requested tenant settings.
func (builder *SnapshotBuilder) BuildFull(ctx context.Context, tenantIDs ...string) (domain.Snapshot, error) {
	_, tracer, _, _ := libCommons.NewTrackingFromContext(ctx) //nolint:dogsled

	ctx, span := tracer.Start(ctx, "systemplane.snapshot_builder.build_full")
	defer span.End()

	configs, configRev, err := builder.BuildConfigs(ctx)
	if err != nil {
		libOpentelemetry.HandleSpanError(span, "build configs", err)
		return domain.Snapshot{}, fmt.Errorf("build configs: %w", err)
	}

	globalSettings, globalRev, err := builder.BuildGlobalSettings(ctx)
	if err != nil {
		libOpentelemetry.HandleSpanError(span, "build global settings", err)
		return domain.Snapshot{}, fmt.Errorf("build global settings: %w", err)
	}

	tenantSettings := make(map[string]map[string]domain.EffectiveValue)
	maxRevision := maxRevisions(configRev, globalRev)

	for _, tenantID := range uniqueTenantIDs(tenantIDs) {
		settings, rev, buildErr := builder.BuildSettings(ctx, Subject{Scope: domain.ScopeTenant, SubjectID: tenantID})
		if buildErr != nil {
			libOpentelemetry.HandleSpanError(span, "build tenant settings", buildErr)
			return domain.Snapshot{}, fmt.Errorf("build tenant settings %q: %w", tenantID, buildErr)
		}

		tenantSettings[tenantID] = settings
		maxRevision = maxRevisions(maxRevision, rev)
	}

	return domain.Snapshot{
		Configs:        configs,
		GlobalSettings: globalSettings,
		TenantSettings: tenantSettings,
		Revision:       maxRevision,
		BuiltAt:        time.Now().UTC(),
	}, nil
}

// initDefaults initializes an EffectiveValue map from a list of KeyDefs,
// populating each entry with its default value and redaction metadata.
func initDefaults(defs []domain.KeyDef) map[string]domain.EffectiveValue {
	valuesByKey := make(map[string]domain.EffectiveValue, len(defs))

	for _, def := range defs {
		valuesByKey[def.Key] = domain.EffectiveValue{
			Key:      def.Key,
			Value:    def.DefaultValue,
			Default:  def.DefaultValue,
			Override: nil,
			Source:   "default",
			Revision: domain.RevisionZero,
			Redacted: def.RedactPolicy != domain.RedactNone,
		}
	}

	return valuesByKey
}

// applyOverrides merges store entries into the effective map. Only entries
// whose keys already exist in the map are applied.
func applyOverrides(effective map[string]domain.EffectiveValue, entries []domain.Entry, source string) {
	for _, entry := range entries {
		ev, ok := effective[entry.Key]
		if !ok {
			continue
		}

		if entry.Value == nil {
			continue
		}

		ev.Value = entry.Value
		ev.Override = entry.Value
		ev.Source = source
		effective[entry.Key] = ev
	}
}

func filterDefsByScope(defs []domain.KeyDef, scope domain.Scope) []domain.KeyDef {
	filtered := make([]domain.KeyDef, 0, len(defs))
	for _, def := range defs {
		if scopeAllowed(def.AllowedScopes, scope) {
			filtered = append(filtered, def)
		}
	}

	return filtered
}

func setRevision(effective map[string]domain.EffectiveValue, revision domain.Revision) {
	for key, value := range effective {
		value.Revision = revision
		effective[key] = value
	}
}

func uniqueTenantIDs(tenantIDs []string) []string {
	if len(tenantIDs) == 0 {
		return nil
	}

	seen := make(map[string]struct{}, len(tenantIDs))
	unique := make([]string, 0, len(tenantIDs))

	for _, tenantID := range tenantIDs {
		if tenantID == "" {
			continue
		}

		if _, ok := seen[tenantID]; ok {
			continue
		}

		seen[tenantID] = struct{}{}
		unique = append(unique, tenantID)
	}

	sort.Strings(unique)

	return unique
}

func maxRevisions(values ...domain.Revision) domain.Revision {
	maxRevision := domain.RevisionZero

	for _, value := range values {
		if value > maxRevision {
			maxRevision = value
		}
	}

	return maxRevision
}
