package command

import (
	"context"
	"fmt"
	"strings"
	"time"

	"github.com/google/uuid"
	"go.opentelemetry.io/otel/trace"

	libCommons "github.com/LerianStudio/lib-commons/v4/commons"
	libLog "github.com/LerianStudio/lib-commons/v4/commons/log"
	libOpentelemetry "github.com/LerianStudio/lib-commons/v4/commons/opentelemetry"

	"github.com/LerianStudio/matcher/internal/exception/domain/entities"
	"github.com/LerianStudio/matcher/internal/exception/domain/value_objects"
	"github.com/LerianStudio/matcher/internal/exception/ports"
)

// ForceMatchCommand contains parameters for the force match operation.
type ForceMatchCommand struct {
	ExceptionID    uuid.UUID
	OverrideReason string
	Notes          string
}

type forceMatchParams struct {
	actor          string
	notes          string
	overrideReason value_objects.OverrideReason
}

func (uc *UseCase) validateForceMatch(
	ctx context.Context,
	cmd ForceMatchCommand,
) (*forceMatchParams, error) {
	if err := uc.validateDependencies(); err != nil {
		return nil, err
	}

	if cmd.ExceptionID == uuid.Nil {
		return nil, ErrExceptionIDRequired
	}

	actor := strings.TrimSpace(uc.actorExtractor.GetActor(ctx))
	if actor == "" {
		return nil, ErrActorRequired
	}

	notes := strings.TrimSpace(cmd.Notes)
	if notes == "" {
		return nil, entities.ErrResolutionNotesRequired
	}

	overrideReason, err := value_objects.ParseOverrideReason(cmd.OverrideReason)
	if err != nil {
		return nil, fmt.Errorf("parse override reason: %w", err)
	}

	return &forceMatchParams{actor: actor, notes: notes, overrideReason: overrideReason}, nil
}

// ForceMatch resolves an exception by forcing a match with an override reason.
func (uc *UseCase) ForceMatch(
	ctx context.Context,
	cmd ForceMatchCommand,
) (*entities.Exception, error) {
	params, err := uc.validateForceMatch(ctx, cmd)
	if err != nil {
		return nil, err
	}

	logger, tracer, _, _ := libCommons.NewTrackingFromContext(ctx)

	ctx, span := tracer.Start(ctx, "command.force_match_exception")
	defer span.End()

	return uc.processForceMatch(ctx, cmd, params, logger, span)
}

func (uc *UseCase) processForceMatch(
	ctx context.Context,
	cmd ForceMatchCommand,
	params *forceMatchParams,
	logger libLog.Logger,
	span trace.Span,
) (*entities.Exception, error) {
	exception, err := uc.exceptionRepo.FindByID(ctx, cmd.ExceptionID)
	if err != nil {
		libOpentelemetry.HandleSpanError(span, "failed to load exception", err)

		logger.Log(ctx, libLog.LevelError, fmt.Sprintf("failed to load exception: %v", err))

		return nil, fmt.Errorf("find exception: %w", err)
	}

	if exception == nil {
		return nil, fmt.Errorf("find exception: %w", entities.ErrExceptionNotFound)
	}

	if err := value_objects.ValidateResolutionTransition(exception.Status, value_objects.ExceptionStatusResolved); err != nil {
		libOpentelemetry.HandleSpanBusinessErrorEvent(span, "invalid resolution transition", err)

		return nil, fmt.Errorf("validate transition: %w", err)
	}

	// Guard: set PENDING_RESOLUTION before gateway call to prevent re-processing.
	previousStatus := exception.Status

	if err := exception.StartResolution(ctx); err != nil {
		libOpentelemetry.HandleSpanBusinessErrorEvent(span, "start resolution failed", err)

		return nil, fmt.Errorf("start resolution: %w", err)
	}

	exception, err = uc.exceptionRepo.Update(ctx, exception)
	if err != nil {
		libOpentelemetry.HandleSpanError(span, "failed to persist pending resolution", err)

		return nil, fmt.Errorf("persist pending resolution: %w", err)
	}

	if exception == nil {
		return nil, fmt.Errorf("persist pending resolution: %w", ErrUnexpectedNilResult)
	}

	// Phase 1: Execute gateway call with automatic revert on failure.
	if err := uc.executeWithRevert(
		ctx,
		exception,
		previousStatus,
		func() error {
			return uc.resolutionExecutor.ForceMatch(ctx, exception.ID, params.notes, params.overrideReason)
		},
		logger,
	); err != nil {
		libOpentelemetry.HandleSpanError(span, "force match executor failed", err)

		return nil, fmt.Errorf("force match executor: %w", err)
	}

	// Phase 2: Resolve exception and persist atomically with audit.
	if err := exception.Resolve(ctx, params.notes,
		entities.WithResolutionType("FORCE_MATCH"),
		entities.WithResolutionReason(string(params.overrideReason)),
	); err != nil {
		libOpentelemetry.HandleSpanBusinessErrorEvent(span, "resolve exception failed", err)

		return nil, fmt.Errorf("resolve exception: %w", err)
	}

	// Atomic transaction: update exception state AND create audit log in same transaction.
	// This ensures SOX compliance - if either fails, both are rolled back.
	txLease, err := uc.infraProvider.BeginTx(ctx)
	if err != nil {
		libOpentelemetry.HandleSpanError(span, "failed to begin transaction", err)

		return nil, fmt.Errorf("begin transaction: %w", err)
	}

	defer func() {
		_ = txLease.Rollback() // No-op if already committed
	}()

	updated, err := uc.exceptionRepo.UpdateWithTx(ctx, txLease.SQLTx(), exception)
	if err != nil {
		libOpentelemetry.HandleSpanError(span, "failed to update exception", err)

		return nil, fmt.Errorf("update exception: %w", err)
	}

	reasonValue := string(params.overrideReason)
	if err := uc.auditPublisher.PublishExceptionEventWithTx(ctx, txLease.SQLTx(), ports.AuditEvent{
		ExceptionID: updated.ID,
		Action:      "FORCE_MATCH",
		Actor:       params.actor,
		Notes:       params.notes,
		ReasonCode:  &reasonValue,
		OccurredAt:  time.Now().UTC(),
		Metadata: map[string]string{
			"previous_status": string(previousStatus),
			"resolution_type": "FORCE_MATCH",
			"override_reason": string(params.overrideReason),
			"transaction_id":  updated.TransactionID.String(),
		},
	}); err != nil {
		libOpentelemetry.HandleSpanError(span, "audit publish failed", err)

		return nil, fmt.Errorf("publish audit: %w", err)
	}

	if err := txLease.Commit(); err != nil {
		libOpentelemetry.HandleSpanError(span, "failed to commit transaction", err)

		return nil, fmt.Errorf("commit transaction: %w", err)
	}

	return updated, nil
}
