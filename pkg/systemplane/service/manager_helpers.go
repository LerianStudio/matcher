// Copyright 2025 Lerian Studio.

package service

import (
	"github.com/LerianStudio/matcher/pkg/systemplane/domain"
	"github.com/LerianStudio/matcher/pkg/systemplane/ports"
	"github.com/LerianStudio/matcher/pkg/systemplane/registry"
)

func buildSchema(reg registry.Registry, kind domain.Kind) []SchemaEntry {
	defs := reg.List(kind)

	entries := make([]SchemaEntry, len(defs))

	for i, def := range defs {
		entries[i] = SchemaEntry{
			Key:              def.Key,
			Kind:             def.Kind,
			AllowedScopes:    append([]domain.Scope(nil), def.AllowedScopes...),
			ValueType:        def.ValueType,
			DefaultValue:     redactValue(def, def.DefaultValue),
			MutableAtRuntime: def.MutableAtRuntime,
			ApplyBehavior:    def.ApplyBehavior,
			Secret:           def.Secret,
			RedactPolicy:     def.RedactPolicy,
			Description:      def.Description,
			Group:            def.Group,
		}
	}

	return entries
}

func cloneSnapshot(snapshot domain.Snapshot) domain.Snapshot {
	cloned := domain.Snapshot{
		Configs:        cloneEffectiveValues(snapshot.Configs),
		GlobalSettings: cloneEffectiveValues(snapshot.GlobalSettings),
		TenantSettings: make(map[string]map[string]domain.EffectiveValue, len(snapshot.TenantSettings)),
		Revision:       snapshot.Revision,
		BuiltAt:        snapshot.BuiltAt,
	}
	for tenantID, values := range snapshot.TenantSettings {
		cloned.TenantSettings[tenantID] = cloneEffectiveValues(values)
	}

	return cloned
}

func cloneEffectiveValues(values map[string]domain.EffectiveValue) map[string]domain.EffectiveValue {
	if values == nil {
		return nil
	}

	cloned := make(map[string]domain.EffectiveValue, len(values))

	for key, value := range values {
		cloned[key] = value
	}

	return cloned
}

// revisionFromValues returns the highest revision across all entries in the
// map. In practice all entries share the same revision (set by setRevision
// after each build), but using max() is a safety net against future callers
// that may not uphold that invariant.
func revisionFromValues(values map[string]domain.EffectiveValue) domain.Revision {
	maxRev := domain.RevisionZero
	for _, value := range values {
		if value.Revision > maxRev {
			maxRev = value.Revision
		}
	}

	return maxRev
}

func snapshotRevision(snapshot domain.Snapshot) domain.Revision {
	revision := revisionFromValues(snapshot.Configs)

	revision = maxRevisions(revision, revisionFromValues(snapshot.GlobalSettings))

	for _, values := range snapshot.TenantSettings {
		revision = maxRevisions(revision, revisionFromValues(values))
	}

	return revision
}

func redactEffectiveValues(reg registry.Registry, values map[string]domain.EffectiveValue) map[string]domain.EffectiveValue {
	for key, value := range values {
		def, ok := reg.Get(key)
		if !ok {
			continue
		}

		value.Value = redactValue(def, value.Value)
		value.Default = redactValue(def, value.Default)
		value.Override = redactValue(def, value.Override)
		values[key] = value
	}

	return values
}

func redactHistoryEntries(reg registry.Registry, entries []ports.HistoryEntry) []ports.HistoryEntry {
	redacted := make([]ports.HistoryEntry, len(entries))
	for i, entry := range entries {
		redacted[i] = entry

		def, ok := reg.Get(entry.Key)
		if !ok {
			continue
		}

		redacted[i].OldValue = redactValue(def, entry.OldValue)
		redacted[i].NewValue = redactValue(def, entry.NewValue)
	}

	return redacted
}

func redactValue(def domain.KeyDef, value any) any {
	if value == nil {
		return nil
	}

	// Secret keys are always redacted regardless of RedactPolicy setting.
	// This prevents accidental secret leaks when a developer sets Secret=true
	// but forgets to set RedactPolicy explicitly.
	if def.Secret {
		return "****"
	}

	if def.RedactPolicy == "" || def.RedactPolicy == domain.RedactNone {
		return value
	}

	// Non-secret keys with an explicit redact policy (e.g. RedactMask,
	// RedactFull) are redacted.
	return "****"
}

func scopeAllowed(allowed []domain.Scope, target domain.Scope) bool {
	for _, scope := range allowed {
		if scope == target {
			return true
		}
	}

	return false
}
