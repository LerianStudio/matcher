// Copyright 2025 Lerian Studio.

package service

import (
	"context"
	"fmt"
	"time"

	"github.com/LerianStudio/matcher/pkg/systemplane/domain"
	"github.com/LerianStudio/matcher/pkg/systemplane/ports"
)

func (manager *defaultManager) previewConfigSnapshot(ctx context.Context, ops []ports.WriteOp) (domain.Snapshot, error) {
	current := cloneSnapshot(manager.supervisor.Snapshot())
	if current.BuiltAt.IsZero() || current.Configs == nil {
		fresh, err := manager.builder.BuildFull(ctx)
		if err != nil {
			return domain.Snapshot{}, fmt.Errorf("build fresh snapshot: %w", err)
		}

		current = cloneSnapshot(fresh)
	}

	if current.Configs == nil {
		current.Configs = make(map[string]domain.EffectiveValue)
	}

	for _, op := range ops {
		def, ok := manager.registry.Get(op.Key)
		if !ok {
			return domain.Snapshot{}, fmt.Errorf("preview config key %q: %w", op.Key, domain.ErrKeyUnknown)
		}

		ev := current.Configs[op.Key]
		ev.Key = def.Key
		ev.Default = def.DefaultValue
		ev.Redacted = def.RedactPolicy != domain.RedactNone

		if op.Reset || domain.IsNilValue(op.Value) {
			ev.Value = def.DefaultValue
			ev.Override = nil
			ev.Source = "default"
		} else {
			ev.Value = op.Value
			ev.Override = op.Value
			ev.Source = "preview-override"
		}

		current.Configs[op.Key] = ev
	}

	current.Revision = snapshotRevision(current)
	current.BuiltAt = time.Now().UTC()

	return current, nil
}

// PatchSettings validates the mutations, persists them, and applies the
// escalation behavior.

func (manager *defaultManager) validateConfigOp(op ports.WriteOp) error {
	def, ok := manager.registry.Get(op.Key)
	if !ok {
		return fmt.Errorf("key %q: %w", op.Key, domain.ErrKeyUnknown)
	}

	if def.Kind != domain.KindConfig {
		return fmt.Errorf("key %q is kind %q, not config: %w", op.Key, def.Kind, domain.ErrKeyUnknown)
	}

	if !def.MutableAtRuntime {
		return fmt.Errorf("key %q: %w", op.Key, domain.ErrKeyNotMutable)
	}

	if !op.Reset && !domain.IsNilValue(op.Value) {
		if err := manager.registry.Validate(op.Key, op.Value); err != nil {
			return fmt.Errorf("key %q: %w", op.Key, err)
		}
	}

	return nil
}

func (manager *defaultManager) validateSettingOp(op ports.WriteOp, scope domain.Scope) error {
	def, ok := manager.registry.Get(op.Key)
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

	if !op.Reset && !domain.IsNilValue(op.Value) {
		if err := manager.registry.Validate(op.Key, op.Value); err != nil {
			return fmt.Errorf("key %q: %w", op.Key, err)
		}
	}

	return nil
}

func (manager *defaultManager) applyEscalation(ctx context.Context, target domain.Target, escalation domain.ApplyBehavior) error {
	switch escalation {
	case domain.ApplyBootstrapOnly:
		return nil
	case domain.ApplyLiveRead:
		snap, err := manager.buildActiveSnapshot(ctx, target)
		if err != nil {
			return fmt.Errorf("build snapshot for live-read: %w", err)
		}

		if err := manager.supervisor.PublishSnapshot(ctx, snap, "live-read"); err != nil {
			return fmt.Errorf("publish snapshot for live-read: %w", err)
		}

		if manager.stateSync != nil {
			manager.stateSync(ctx, snap)
		}

		return nil
	case domain.ApplyWorkerReconcile:
		snap, err := manager.buildActiveSnapshot(ctx, target)
		if err != nil {
			return fmt.Errorf("build snapshot for worker-reconcile: %w", err)
		}

		if err := manager.supervisor.ReconcileCurrent(ctx, snap, "worker-reconcile"); err != nil {
			return fmt.Errorf("reconcile current for worker-reconcile: %w", err)
		}

		if manager.stateSync != nil {
			manager.stateSync(ctx, snap)
		}

		return nil
	case domain.ApplyBundleRebuild, domain.ApplyBundleRebuildAndReconcile:
		var extraTenants []string
		if target.Scope == domain.ScopeTenant && target.SubjectID != "" {
			extraTenants = []string{target.SubjectID}
		}

		if err := manager.supervisor.Reload(ctx, string(escalation), extraTenants...); err != nil {
			return fmt.Errorf("reload for %s: %w", escalation, err)
		}

		return nil
	default:
		return fmt.Errorf("%w %q", errUnexpectedApplyBehavior, escalation)
	}
}

// ApplyChangeSignal applies an externally produced change signal using the
// signal's escalation behavior or a safe rebuild fallback.

func (manager *defaultManager) buildActiveSnapshot(ctx context.Context, target domain.Target) (domain.Snapshot, error) {
	current := cloneSnapshot(manager.supervisor.Snapshot())

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
		configs, _, err := manager.builder.BuildConfigs(ctx)
		if err != nil {
			return domain.Snapshot{}, err
		}

		current.Configs = configs
	case domain.KindSetting:
		switch target.Scope {
		case domain.ScopeGlobal:
			globalSettings, _, err := manager.builder.BuildSettings(ctx, Subject{Scope: domain.ScopeGlobal})
			if err != nil {
				return domain.Snapshot{}, err
			}

			current.GlobalSettings = globalSettings

			for tenantID := range current.TenantSettings {
				settings, _, err := manager.builder.BuildSettings(ctx, Subject{Scope: domain.ScopeTenant, SubjectID: tenantID})
				if err != nil {
					return domain.Snapshot{}, err
				}

				current.TenantSettings[tenantID] = settings
			}
		case domain.ScopeTenant:
			settings, _, err := manager.builder.BuildSettings(ctx, Subject{Scope: domain.ScopeTenant, SubjectID: target.SubjectID})
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
