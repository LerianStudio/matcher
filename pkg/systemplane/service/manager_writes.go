// Copyright 2025 Lerian Studio.

package service

import (
	"context"
	"errors"
	"fmt"

	libCommons "github.com/LerianStudio/lib-commons/v4/commons"
	libOpentelemetry "github.com/LerianStudio/lib-commons/v4/commons/opentelemetry"

	"github.com/LerianStudio/matcher/pkg/systemplane/domain"
	"github.com/LerianStudio/matcher/pkg/systemplane/ports"
)

var errUnexpectedApplyBehavior = errors.New("systemplane manager: unexpected apply behavior")

// PatchConfigs validates the mutations, persists them, and applies the
// escalation behavior.
func (manager *defaultManager) PatchConfigs(ctx context.Context, req PatchRequest) (WriteResult, error) {
	_, tracer, _, _ := libCommons.NewTrackingFromContext(ctx) //nolint:dogsled

	ctx, span := tracer.Start(ctx, "systemplane.manager.patch_configs")
	defer span.End()

	if len(req.Ops) == 0 {
		return WriteResult{}, nil
	}

	for _, op := range req.Ops {
		if err := manager.validateConfigOp(op); err != nil {
			return WriteResult{}, err
		}
	}

	if manager.configWriteValidator != nil {
		candidate, err := manager.previewConfigSnapshot(ctx, req.Ops)
		if err != nil {
			libOpentelemetry.HandleSpanError(span, "preview config snapshot", err)
			return WriteResult{}, fmt.Errorf("patch configs preview: %w", err)
		}

		if err := manager.configWriteValidator(ctx, candidate); err != nil {
			libOpentelemetry.HandleSpanError(span, "validate config snapshot", err)
			return WriteResult{}, fmt.Errorf("patch configs validation: %w", err)
		}
	}

	escalation, _, err := Escalate(manager.registry, req.Ops)
	if err != nil {
		libOpentelemetry.HandleSpanError(span, "escalate config patch", err)
		return WriteResult{}, fmt.Errorf("patch configs escalation: %w", err)
	}

	target, err := domain.NewTarget(domain.KindConfig, domain.ScopeGlobal, "")
	if err != nil {
		libOpentelemetry.HandleSpanError(span, "build config target", err)
		return WriteResult{}, fmt.Errorf("patch configs target: %w", err)
	}

	revision, err := manager.store.Put(ctx, target, req.Ops, req.ExpectedRevision, req.Actor, req.Source)
	if err != nil {
		libOpentelemetry.HandleSpanError(span, "persist config patch", err)
		return WriteResult{}, fmt.Errorf("patch configs put: %w", err)
	}

	if err := manager.applyEscalation(ctx, target, escalation); err != nil {
		libOpentelemetry.HandleSpanError(span, "apply config escalation", err)
		return WriteResult{}, fmt.Errorf("patch configs apply: %w", err)
	}

	return WriteResult{Revision: revision}, nil
}

// PatchSettings validates and persists setting mutations for the provided subject.
func (manager *defaultManager) PatchSettings(ctx context.Context, subject Subject, req PatchRequest) (WriteResult, error) {
	_, tracer, _, _ := libCommons.NewTrackingFromContext(ctx) //nolint:dogsled

	ctx, span := tracer.Start(ctx, "systemplane.manager.patch_settings")
	defer span.End()

	if len(req.Ops) == 0 {
		return WriteResult{}, nil
	}

	for _, op := range req.Ops {
		if err := manager.validateSettingOp(op, subject.Scope); err != nil {
			return WriteResult{}, err
		}
	}

	escalation, _, err := Escalate(manager.registry, req.Ops)
	if err != nil {
		libOpentelemetry.HandleSpanError(span, "escalate settings patch", err)
		return WriteResult{}, fmt.Errorf("patch settings escalation: %w", err)
	}

	target, err := domain.NewTarget(domain.KindSetting, subject.Scope, subject.SubjectID)
	if err != nil {
		libOpentelemetry.HandleSpanError(span, "build settings target", err)
		return WriteResult{}, fmt.Errorf("patch settings target: %w", err)
	}

	revision, err := manager.store.Put(ctx, target, req.Ops, req.ExpectedRevision, req.Actor, req.Source)
	if err != nil {
		libOpentelemetry.HandleSpanError(span, "persist settings patch", err)
		return WriteResult{}, fmt.Errorf("patch settings put: %w", err)
	}

	if err := manager.applyEscalation(ctx, target, escalation); err != nil {
		libOpentelemetry.HandleSpanError(span, "apply settings escalation", err)
		return WriteResult{}, fmt.Errorf("patch settings apply: %w", err)
	}

	return WriteResult{Revision: revision}, nil
}

// ApplyChangeSignal applies a precomputed runtime escalation from an external source.
func (manager *defaultManager) ApplyChangeSignal(ctx context.Context, signal ports.ChangeSignal) error {
	_, tracer, _, _ := libCommons.NewTrackingFromContext(ctx) //nolint:dogsled

	ctx, span := tracer.Start(ctx, "systemplane.manager.apply_change_signal")
	defer span.End()

	behavior := signal.ApplyBehavior
	if !behavior.IsValid() {
		behavior = domain.ApplyBundleRebuild
	}

	if err := manager.applyEscalation(ctx, signal.Target, behavior); err != nil {
		libOpentelemetry.HandleSpanError(span, "apply change signal", err)
		return err
	}

	return nil
}
