package command

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"time"

	"github.com/google/uuid"

	libCommons "github.com/LerianStudio/lib-uncommons/v2/uncommons"
	libLog "github.com/LerianStudio/lib-uncommons/v2/uncommons/log"
	libOpentelemetry "github.com/LerianStudio/lib-uncommons/v2/uncommons/opentelemetry"

	"github.com/LerianStudio/matcher/internal/configuration/domain/entities"
	"github.com/LerianStudio/matcher/internal/configuration/ports"
)

// publishAudit publishes an audit event if the publisher is configured.
//
// Design Decision: Audit publish is intentionally non-blocking and best-effort.
// Per governance requirements (SOX compliance), audit failures must NOT block
// business operations. This follows the principle that observability/compliance
// infrastructure should be resilient to failures without impacting core workflows.
//
// Failures are observable via:
// - Structured logging with correlation IDs (from tracking context)
// - OpenTelemetry span events (for tracing integration)
//
// If durable audit guarantees are required in the future, consider implementing
// the outbox pattern: persist audit events in the same DB transaction, then
// publish asynchronously via a background worker.
func (uc *UseCase) publishAudit(
	ctx context.Context,
	entityType string,
	entityID uuid.UUID,
	action string,
	changes map[string]any,
) {
	if uc.auditPublisher == nil {
		return
	}

	logger, tracer, _, headerID := libCommons.NewTrackingFromContext(ctx)

	_, span := tracer.Start(ctx, "audit.publish")
	defer span.End()

	event := ports.AuditEvent{
		EntityType: entityType,
		EntityID:   entityID,
		Action:     action,
		OccurredAt: time.Now().UTC(),
		Changes:    changes,
	}

	if err := uc.auditPublisher.Publish(ctx, event); err != nil {
		libOpentelemetry.HandleSpanError(span, "audit_publish_failed", err)

		logger.With(
			libLog.Any("audit.entity_type", entityType),
			libLog.Any("audit.entity_id", entityID.String()),
			libLog.Any("audit.action", action),
			libLog.Any("correlation.header_id", headerID),
			libLog.Any("error.message", err.Error()),
		).Log(ctx, libLog.LevelError, "failed to publish audit event")
	}
}

// CreateContext creates a new reconciliation context.
func (uc *UseCase) CreateContext(
	ctx context.Context,
	tenantID uuid.UUID,
	input entities.CreateReconciliationContextInput,
) (*entities.ReconciliationContext, error) {
	if uc == nil || uc.contextRepo == nil {
		return nil, ErrNilContextRepository
	}

	logger, tracer, _, _ := libCommons.NewTrackingFromContext(ctx)

	ctx, span := tracer.Start(ctx, "command.create_reconciliation_context")
	defer span.End()

	// Enforce unique context name within tenant scope.
	// FindByName returns (nil, nil) when no context exists with the given name.
	// A non-nil error (other than sql.ErrNoRows) indicates a transient failure
	// that must not be silently swallowed, as that could allow duplicate names.
	existing, err := uc.contextRepo.FindByName(ctx, input.Name)
	if err != nil && !errors.Is(err, sql.ErrNoRows) {
		return nil, fmt.Errorf("checking context name uniqueness: %w", err)
	}

	if existing != nil {
		return nil, ErrContextNameAlreadyExists
	}

	entity, err := entities.NewReconciliationContext(ctx, tenantID, input)
	if err != nil {
		libOpentelemetry.HandleSpanError(span, "invalid reconciliation context input", err)
		return nil, err
	}

	created, err := uc.contextRepo.Create(ctx, entity)
	if err != nil {
		libOpentelemetry.HandleSpanError(span, "failed to create reconciliation context", err)

		logger.With(libLog.Any("error", err.Error())).Log(ctx, libLog.LevelError, "failed to create reconciliation context")

		return nil, fmt.Errorf("creating reconciliation context: %w", err)
	}

	uc.publishAudit(ctx, "context", created.ID, "create", map[string]any{
		"name":     created.Name,
		"type":     created.Type,
		"interval": created.Interval,
	})

	return created, nil
}

// UpdateContext modifies an existing reconciliation context.
func (uc *UseCase) UpdateContext(
	ctx context.Context,
	contextID uuid.UUID,
	input entities.UpdateReconciliationContextInput,
) (*entities.ReconciliationContext, error) {
	if uc == nil || uc.contextRepo == nil {
		return nil, ErrNilContextRepository
	}

	logger, tracer, _, _ := libCommons.NewTrackingFromContext(ctx)

	ctx, span := tracer.Start(ctx, "command.update_reconciliation_context")
	defer span.End()

	entity, err := uc.contextRepo.FindByID(ctx, contextID)
	if err != nil {
		libOpentelemetry.HandleSpanError(span, "failed to load reconciliation context", err)

		logger.With(libLog.Any("error", err.Error())).Log(ctx, libLog.LevelError, "failed to load reconciliation context")

		return nil, fmt.Errorf("finding reconciliation context: %w", err)
	}

	// Enforce unique context name when name is being changed.
	if err := uc.checkContextNameUniqueness(ctx, input.Name, entity.Name, contextID); err != nil {
		return nil, err
	}

	if err := entity.Update(ctx, input); err != nil {
		libOpentelemetry.HandleSpanError(span, "invalid reconciliation context update", err)
		return nil, err
	}

	updated, err := uc.contextRepo.Update(ctx, entity)
	if err != nil {
		libOpentelemetry.HandleSpanError(span, "failed to update reconciliation context", err)

		logger.With(libLog.Any("error", err.Error())).Log(ctx, libLog.LevelError, "failed to update reconciliation context")

		return nil, fmt.Errorf("updating reconciliation context: %w", err)
	}

	uc.publishAudit(ctx, "context", updated.ID, "update", map[string]any{
		"name":     updated.Name,
		"type":     updated.Type,
		"interval": updated.Interval,
	})

	return updated, nil
}

// checkContextNameUniqueness verifies that the new name (if changed) does not
// conflict with an existing context. Returns nil when no rename is requested
// or the name is available. FindByName returns (nil, nil) when no context
// exists with the given name. A non-nil error (other than sql.ErrNoRows)
// indicates a transient failure that must not be silently swallowed.
func (uc *UseCase) checkContextNameUniqueness(
	ctx context.Context,
	newName *string,
	currentName string,
	contextID uuid.UUID,
) error {
	if newName == nil || *newName == currentName {
		return nil
	}

	existing, err := uc.contextRepo.FindByName(ctx, *newName)
	if err != nil && !errors.Is(err, sql.ErrNoRows) {
		return fmt.Errorf("checking context name uniqueness: %w", err)
	}

	if existing != nil && existing.ID != contextID {
		return ErrContextNameAlreadyExists
	}

	return nil
}

// DeleteContext removes a reconciliation context.
//
// Before deleting, this method checks for child entities (sources, match rules,
// and schedules) that reference this context. If any exist, the deletion is
// rejected with ErrContextHasChildEntities to prevent orphan data and
// referential integrity violations.
func (uc *UseCase) DeleteContext(ctx context.Context, contextID uuid.UUID) error {
	if uc == nil || uc.contextRepo == nil {
		return ErrNilContextRepository
	}

	logger, tracer, _, _ := libCommons.NewTrackingFromContext(ctx)

	ctx, span := tracer.Start(ctx, "command.delete_reconciliation_context")
	defer span.End()

	if _, err := uc.contextRepo.FindByID(ctx, contextID); err != nil {
		libOpentelemetry.HandleSpanError(span, "failed to load reconciliation context", err)

		logger.With(libLog.Any("error", err.Error())).Log(ctx, libLog.LevelError, "failed to load reconciliation context")

		return fmt.Errorf("finding reconciliation context: %w", err)
	}

	if err := uc.checkContextChildren(ctx, contextID); err != nil {
		libOpentelemetry.HandleSpanError(span, "context has child entities", err)

		logger.With(libLog.String("context.id", contextID.String())).Log(ctx, libLog.LevelError, "cannot delete context: has child entities")

		return err
	}

	if err := uc.contextRepo.Delete(ctx, contextID); err != nil {
		libOpentelemetry.HandleSpanError(span, "failed to delete reconciliation context", err)

		logger.With(libLog.Any("error", err.Error())).Log(ctx, libLog.LevelError, "failed to delete reconciliation context")

		return fmt.Errorf("deleting reconciliation context: %w", err)
	}

	uc.publishAudit(ctx, "context", contextID, "delete", nil)

	return nil
}

// checkContextChildren verifies that no child entities (sources, match rules,
// or schedules) reference the given context. Returns ErrContextHasChildEntities
// if any children exist. This is a guard against accidental data loss.
func (uc *UseCase) checkContextChildren(ctx context.Context, contextID uuid.UUID) error {
	// Check for associated sources.
	sources, _, err := uc.sourceRepo.FindByContextID(ctx, contextID, "", 1)
	if err != nil {
		return fmt.Errorf("checking context sources: %w", err)
	}

	if len(sources) > 0 {
		return ErrContextHasChildEntities
	}

	// Check for associated match rules.
	rules, _, err := uc.matchRuleRepo.FindByContextID(ctx, contextID, "", 1)
	if err != nil {
		return fmt.Errorf("checking context match rules: %w", err)
	}

	if len(rules) > 0 {
		return ErrContextHasChildEntities
	}

	// Check for associated schedules (optional dependency).
	if uc.scheduleRepo != nil {
		schedules, err := uc.scheduleRepo.FindByContextID(ctx, contextID)
		if err != nil {
			return fmt.Errorf("checking context schedules: %w", err)
		}

		if len(schedules) > 0 {
			return ErrContextHasChildEntities
		}
	}

	return nil
}
