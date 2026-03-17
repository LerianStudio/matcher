// Copyright 2025 Lerian Studio.

package service

import (
	"context"
	"fmt"
	"time"

	"github.com/LerianStudio/matcher/pkg/systemplane/domain"
	"github.com/LerianStudio/matcher/pkg/systemplane/ports"
	"github.com/LerianStudio/matcher/pkg/systemplane/registry"
)

// Subject identifies the scope and subject for a settings operation.
type Subject struct {
	Scope     domain.Scope
	SubjectID string
}

// PatchRequest contains the parameters for a config/settings patch operation.
type PatchRequest struct {
	Ops              []ports.WriteOp
	ExpectedRevision domain.Revision
	Actor            domain.Actor
	Source           string
}

// WriteResult contains the outcome of a patch operation.
type WriteResult struct {
	Revision domain.Revision
}

// ResolvedSet contains the resolved effective values and target revision.
type ResolvedSet struct {
	Values   map[string]domain.EffectiveValue
	Revision domain.Revision
}

// SchemaEntry describes a single key's metadata for the schema endpoint.
type SchemaEntry struct {
	Key              string
	Kind             domain.Kind
	AllowedScopes    []domain.Scope
	ValueType        domain.ValueType
	DefaultValue     any
	MutableAtRuntime bool
	ApplyBehavior    domain.ApplyBehavior
	Secret           bool
	RedactPolicy     domain.RedactPolicy
	Description      string
	Group            string
}

// Manager provides the application-level API for reading and writing
// configuration.
type Manager interface {
	GetConfigs(ctx context.Context) (ResolvedSet, error)
	GetSettings(ctx context.Context, subject Subject) (ResolvedSet, error)
	PatchConfigs(ctx context.Context, req PatchRequest) (WriteResult, error)
	PatchSettings(ctx context.Context, subject Subject, req PatchRequest) (WriteResult, error)
	GetConfigSchema(ctx context.Context) ([]SchemaEntry, error)
	GetSettingSchema(ctx context.Context) ([]SchemaEntry, error)
	GetConfigHistory(ctx context.Context, filter ports.HistoryFilter) ([]ports.HistoryEntry, error)
	GetSettingHistory(ctx context.Context, filter ports.HistoryFilter) ([]ports.HistoryEntry, error)
	Resync(ctx context.Context) error
}

// ManagerConfig holds the dependencies for constructing a Manager.
type ManagerConfig struct {
	Registry   registry.Registry
	Store      ports.Store
	History    ports.HistoryStore
	Supervisor Supervisor
	Builder    *SnapshotBuilder
}

// NewManager creates a new Manager with the supplied dependencies.
func NewManager(cfg ManagerConfig) Manager {
	return &defaultManager{
		registry:   cfg.Registry,
		store:      cfg.Store,
		history:    cfg.History,
		supervisor: cfg.Supervisor,
		builder:    cfg.Builder,
	}
}

type defaultManager struct {
	registry   registry.Registry
	store      ports.Store
	history    ports.HistoryStore
	supervisor Supervisor
	builder    *SnapshotBuilder
}

// GetConfigs returns resolved config values.
func (m *defaultManager) GetConfigs(ctx context.Context) (ResolvedSet, error) {
	snap := m.supervisor.Snapshot()
	if snap.BuiltAt.IsZero() || snap.Configs == nil {
		values, revision, err := m.builder.BuildConfigs(ctx)
		if err != nil {
			return ResolvedSet{}, fmt.Errorf("get configs: %w", err)
		}

		return ResolvedSet{
			Values:   redactEffectiveValues(m.registry, values),
			Revision: revision,
		}, nil
	}

	values := redactEffectiveValues(m.registry, cloneEffectiveValues(snap.Configs))

	return ResolvedSet{
		Values:   values,
		Revision: revisionFromValues(values),
	}, nil
}

// GetSettings returns resolved setting values for the requested subject.
func (m *defaultManager) GetSettings(ctx context.Context, subject Subject) (ResolvedSet, error) {
	snap := m.supervisor.Snapshot()
	if !snap.BuiltAt.IsZero() {
		switch subject.Scope {
		case domain.ScopeGlobal:
			if snap.GlobalSettings != nil {
				values := redactEffectiveValues(m.registry, cloneEffectiveValues(snap.GlobalSettings))
				return ResolvedSet{Values: values, Revision: revisionFromValues(values)}, nil
			}
		case domain.ScopeTenant:
			if snap.TenantSettings != nil {
				if tenantValues, ok := snap.TenantSettings[subject.SubjectID]; ok {
					values := redactEffectiveValues(m.registry, cloneEffectiveValues(tenantValues))
					return ResolvedSet{Values: values, Revision: revisionFromValues(values)}, nil
				}
			}
		}
	}

	values, revision, err := m.builder.BuildSettings(ctx, subject)
	if err != nil {
		return ResolvedSet{}, fmt.Errorf("get settings: %w", err)
	}

	return ResolvedSet{
		Values:   redactEffectiveValues(m.registry, values),
		Revision: revision,
	}, nil
}

// PatchConfigs validates the mutations, persists them, and applies the
// escalation behavior.
func (m *defaultManager) PatchConfigs(ctx context.Context, req PatchRequest) (WriteResult, error) {
	for _, op := range req.Ops {
		if err := m.validateConfigOp(op); err != nil {
			return WriteResult{}, err
		}
	}

	escalation, _, err := Escalate(m.registry, req.Ops)
	if err != nil {
		return WriteResult{}, fmt.Errorf("patch configs escalation: %w", err)
	}

	target, err := domain.NewTarget(domain.KindConfig, domain.ScopeGlobal, "")
	if err != nil {
		return WriteResult{}, fmt.Errorf("patch configs target: %w", err)
	}

	revision, err := m.store.Put(ctx, target, req.Ops, req.ExpectedRevision, req.Actor, req.Source)
	if err != nil {
		return WriteResult{}, fmt.Errorf("patch configs put: %w", err)
	}

	if err := m.applyEscalation(ctx, target, escalation); err != nil {
		return WriteResult{}, fmt.Errorf("patch configs apply: %w", err)
	}

	return WriteResult{Revision: revision}, nil
}

// PatchSettings validates the mutations, persists them, and applies the
// escalation behavior.
func (m *defaultManager) PatchSettings(ctx context.Context, subject Subject, req PatchRequest) (WriteResult, error) {
	for _, op := range req.Ops {
		if err := m.validateSettingOp(op, subject.Scope); err != nil {
			return WriteResult{}, err
		}
	}

	escalation, _, err := Escalate(m.registry, req.Ops)
	if err != nil {
		return WriteResult{}, fmt.Errorf("patch settings escalation: %w", err)
	}

	target, err := domain.NewTarget(domain.KindSetting, subject.Scope, subject.SubjectID)
	if err != nil {
		return WriteResult{}, fmt.Errorf("patch settings target: %w", err)
	}

	revision, err := m.store.Put(ctx, target, req.Ops, req.ExpectedRevision, req.Actor, req.Source)
	if err != nil {
		return WriteResult{}, fmt.Errorf("patch settings put: %w", err)
	}

	if err := m.applyEscalation(ctx, target, escalation); err != nil {
		return WriteResult{}, fmt.Errorf("patch settings apply: %w", err)
	}

	return WriteResult{Revision: revision}, nil
}

// GetConfigSchema returns metadata for all registered config keys.
func (m *defaultManager) GetConfigSchema(_ context.Context) ([]SchemaEntry, error) {
	return buildSchema(m.registry, domain.KindConfig), nil
}

// GetSettingSchema returns metadata for all registered setting keys.
func (m *defaultManager) GetSettingSchema(_ context.Context) ([]SchemaEntry, error) {
	return buildSchema(m.registry, domain.KindSetting), nil
}

// GetConfigHistory retrieves redacted change history for configs.
func (m *defaultManager) GetConfigHistory(ctx context.Context, filter ports.HistoryFilter) ([]ports.HistoryEntry, error) {
	filter.Kind = domain.KindConfig
	entries, err := m.history.ListHistory(ctx, filter)
	if err != nil {
		return nil, fmt.Errorf("get config history: %w", err)
	}

	return redactHistoryEntries(m.registry, entries), nil
}

// GetSettingHistory retrieves redacted change history for settings.
func (m *defaultManager) GetSettingHistory(ctx context.Context, filter ports.HistoryFilter) ([]ports.HistoryEntry, error) {
	filter.Kind = domain.KindSetting
	entries, err := m.history.ListHistory(ctx, filter)
	if err != nil {
		return nil, fmt.Errorf("get setting history: %w", err)
	}

	return redactHistoryEntries(m.registry, entries), nil
}

// Resync triggers a full reload of the supervisor.
func (m *defaultManager) Resync(ctx context.Context) error {
	if err := m.supervisor.Reload(ctx, "resync"); err != nil {
		return fmt.Errorf("resync: %w", err)
	}

	return nil
}

func (m *defaultManager) validateConfigOp(op ports.WriteOp) error {
	def, ok := m.registry.Get(op.Key)
	if !ok {
		return fmt.Errorf("key %q: %w", op.Key, domain.ErrKeyUnknown)
	}

	if def.Kind != domain.KindConfig {
		return fmt.Errorf("key %q is kind %q, not config: %w", op.Key, def.Kind, domain.ErrKeyUnknown)
	}

	if !def.MutableAtRuntime {
		return fmt.Errorf("key %q: %w", op.Key, domain.ErrKeyNotMutable)
	}

	if !op.Reset && op.Value != nil {
		if err := m.registry.Validate(op.Key, op.Value); err != nil {
			return fmt.Errorf("key %q: %w", op.Key, err)
		}
	}

	return nil
}

func (m *defaultManager) validateSettingOp(op ports.WriteOp, scope domain.Scope) error {
	def, ok := m.registry.Get(op.Key)
	if !ok {
		return fmt.Errorf("key %q: %w", op.Key, domain.ErrKeyUnknown)
	}

	if def.Kind != domain.KindSetting {
		return fmt.Errorf("key %q is kind %q, not setting: %w", op.Key, def.Kind, domain.ErrKeyUnknown)
	}

	if !def.MutableAtRuntime {
		return fmt.Errorf("key %q: %w", op.Key, domain.ErrKeyNotMutable)
	}

	if !scopeAllowed(def.AllowedScopes, scope) {
		return fmt.Errorf("key %q does not allow scope %q: %w", op.Key, scope, domain.ErrScopeInvalid)
	}

	if !op.Reset && op.Value != nil {
		if err := m.registry.Validate(op.Key, op.Value); err != nil {
			return fmt.Errorf("key %q: %w", op.Key, err)
		}
	}

	return nil
}

func (m *defaultManager) applyEscalation(ctx context.Context, target domain.Target, escalation domain.ApplyBehavior) error {
	switch escalation {
	case domain.ApplyLiveRead:
		snap, err := m.buildActiveSnapshot(ctx, target)
		if err != nil {
			return fmt.Errorf("build snapshot for live-read: %w", err)
		}

		return m.supervisor.PublishSnapshot(ctx, snap, "live-read")
	case domain.ApplyWorkerReconcile:
		snap, err := m.buildActiveSnapshot(ctx, target)
		if err != nil {
			return fmt.Errorf("build snapshot for worker-reconcile: %w", err)
		}

		return m.supervisor.ReconcileCurrent(ctx, snap, "worker-reconcile")
	case domain.ApplyBundleRebuild, domain.ApplyBundleRebuildAndReconcile:
		return m.supervisor.Reload(ctx, string(escalation))
	case domain.ApplyBootstrapOnly:
		return nil
	default:
		return nil
	}
}

func (m *defaultManager) buildActiveSnapshot(ctx context.Context, target domain.Target) (domain.Snapshot, error) {
	current := cloneSnapshot(m.supervisor.Snapshot())
	if current.Configs == nil {
		current.Configs = make(map[string]domain.EffectiveValue)
	}
	if current.GlobalSettings == nil {
		current.GlobalSettings = make(map[string]domain.EffectiveValue)
	}
	if current.TenantSettings == nil {
		current.TenantSettings = make(map[string]map[string]domain.EffectiveValue)
	}

	switch target.Kind {
	case domain.KindConfig:
		configs, _, err := m.builder.BuildConfigs(ctx)
		if err != nil {
			return domain.Snapshot{}, err
		}
		current.Configs = configs
	case domain.KindSetting:
		switch target.Scope {
		case domain.ScopeGlobal:
			globalSettings, _, err := m.builder.BuildSettings(ctx, Subject{Scope: domain.ScopeGlobal})
			if err != nil {
				return domain.Snapshot{}, err
			}
			current.GlobalSettings = globalSettings

			for tenantID := range current.TenantSettings {
				settings, _, err := m.builder.BuildSettings(ctx, Subject{Scope: domain.ScopeTenant, SubjectID: tenantID})
				if err != nil {
					return domain.Snapshot{}, err
				}
				current.TenantSettings[tenantID] = settings
			}
		case domain.ScopeTenant:
			settings, _, err := m.builder.BuildSettings(ctx, Subject{Scope: domain.ScopeTenant, SubjectID: target.SubjectID})
			if err != nil {
				return domain.Snapshot{}, err
			}
			current.TenantSettings[target.SubjectID] = settings
		}
	}

	current.Revision = snapshotRevision(current)
	current.BuiltAt = time.Now().UTC()

	return current, nil
}

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

func revisionFromValues(values map[string]domain.EffectiveValue) domain.Revision {
	for _, value := range values {
		return value.Revision
	}

	return domain.RevisionZero
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
	if value == nil || def.RedactPolicy == "" || def.RedactPolicy == domain.RedactNone {
		return value
	}

	if !def.Secret && def.RedactPolicy == domain.RedactNone {
		return value
	}

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
