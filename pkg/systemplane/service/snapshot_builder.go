// Copyright 2025 Lerian Studio.

package service

import (
	"context"
	"fmt"
	"sort"
	"time"

	"github.com/LerianStudio/matcher/pkg/systemplane/domain"
	"github.com/LerianStudio/matcher/pkg/systemplane/ports"
	"github.com/LerianStudio/matcher/pkg/systemplane/registry"
)

// SnapshotBuilder merges registry defaults with store overrides to produce
// immutable snapshot values.
type SnapshotBuilder struct {
	registry registry.Registry
	store    ports.Store
}

// NewSnapshotBuilder creates a new SnapshotBuilder with the given registry and
// store dependencies.
func NewSnapshotBuilder(reg registry.Registry, store ports.Store) *SnapshotBuilder {
	return &SnapshotBuilder{registry: reg, store: store}
}

// BuildConfigs builds the config portion of a snapshot by starting with all
// KindConfig defaults from the registry and then overlaying global overrides
// from the store.
func (b *SnapshotBuilder) BuildConfigs(ctx context.Context) (map[string]domain.EffectiveValue, domain.Revision, error) {
	defs := b.registry.List(domain.KindConfig)
	effective := initDefaults(defs)

	target, err := domain.NewTarget(domain.KindConfig, domain.ScopeGlobal, "")
	if err != nil {
		return nil, domain.RevisionZero, fmt.Errorf("build config target: %w", err)
	}

	result, err := b.store.Get(ctx, target)
	if err != nil {
		return nil, domain.RevisionZero, fmt.Errorf("get config overrides: %w", err)
	}

	applyOverrides(effective, result.Entries, "global-override")
	setRevision(effective, result.Revision)

	return effective, result.Revision, nil
}

// BuildGlobalSettings builds global settings using defaults plus global
// overrides only.
func (b *SnapshotBuilder) BuildGlobalSettings(ctx context.Context) (map[string]domain.EffectiveValue, domain.Revision, error) {
	defs := filterDefsByScope(b.registry.List(domain.KindSetting), domain.ScopeGlobal)
	effective := initDefaults(defs)

	target, err := domain.NewTarget(domain.KindSetting, domain.ScopeGlobal, "")
	if err != nil {
		return nil, domain.RevisionZero, fmt.Errorf("build global setting target: %w", err)
	}

	result, err := b.store.Get(ctx, target)
	if err != nil {
		return nil, domain.RevisionZero, fmt.Errorf("get global setting overrides: %w", err)
	}

	applyOverrides(effective, result.Entries, "global-override")
	setRevision(effective, result.Revision)

	return effective, result.Revision, nil
}

// BuildSettings builds effective settings for the requested subject.
func (b *SnapshotBuilder) BuildSettings(ctx context.Context, subject Subject) (map[string]domain.EffectiveValue, domain.Revision, error) {
	switch subject.Scope {
	case domain.ScopeGlobal:
		return b.BuildGlobalSettings(ctx)
	case domain.ScopeTenant:
		return b.buildTenantSettings(ctx, subject.SubjectID)
	default:
		return nil, domain.RevisionZero, fmt.Errorf("build settings scope %q: %w", subject.Scope, domain.ErrScopeInvalid)
	}
}

func (b *SnapshotBuilder) buildTenantSettings(ctx context.Context, tenantID string) (map[string]domain.EffectiveValue, domain.Revision, error) {
	defs := filterDefsByScope(b.registry.List(domain.KindSetting), domain.ScopeTenant)
	effective := initDefaults(defs)

	globalTarget, err := domain.NewTarget(domain.KindSetting, domain.ScopeGlobal, "")
	if err != nil {
		return nil, domain.RevisionZero, fmt.Errorf("build global setting target: %w", err)
	}

	globalResult, err := b.store.Get(ctx, globalTarget)
	if err != nil {
		return nil, domain.RevisionZero, fmt.Errorf("get global setting overrides: %w", err)
	}

	applyOverrides(effective, globalResult.Entries, "global-override")

	tenantTarget, err := domain.NewTarget(domain.KindSetting, domain.ScopeTenant, tenantID)
	if err != nil {
		return nil, domain.RevisionZero, fmt.Errorf("build tenant setting target: %w", err)
	}

	tenantResult, err := b.store.Get(ctx, tenantTarget)
	if err != nil {
		return nil, domain.RevisionZero, fmt.Errorf("get tenant setting overrides: %w", err)
	}

	applyOverrides(effective, tenantResult.Entries, "tenant-override")
	setRevision(effective, tenantResult.Revision)

	return effective, tenantResult.Revision, nil
}

// BuildFull builds a complete snapshot with configs, global settings, and any
// requested tenant settings.
func (b *SnapshotBuilder) BuildFull(ctx context.Context, tenantIDs ...string) (domain.Snapshot, error) {
	configs, configRev, err := b.BuildConfigs(ctx)
	if err != nil {
		return domain.Snapshot{}, fmt.Errorf("build configs: %w", err)
	}

	globalSettings, globalRev, err := b.BuildGlobalSettings(ctx)
	if err != nil {
		return domain.Snapshot{}, fmt.Errorf("build global settings: %w", err)
	}

	tenantSettings := make(map[string]map[string]domain.EffectiveValue)
	maxRevision := maxRevisions(configRev, globalRev)

	for _, tenantID := range uniqueTenantIDs(tenantIDs) {
		settings, rev, buildErr := b.BuildSettings(ctx, Subject{Scope: domain.ScopeTenant, SubjectID: tenantID})
		if buildErr != nil {
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
	m := make(map[string]domain.EffectiveValue, len(defs))

	for _, def := range defs {
		m[def.Key] = domain.EffectiveValue{
			Key:      def.Key,
			Value:    def.DefaultValue,
			Default:  def.DefaultValue,
			Override: nil,
			Source:   "default",
			Revision: domain.RevisionZero,
			Redacted: def.RedactPolicy != domain.RedactNone,
		}
	}

	return m
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
	max := domain.RevisionZero
	for _, value := range values {
		if value > max {
			max = value
		}
	}

	return max
}
