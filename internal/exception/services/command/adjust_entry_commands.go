// Copyright 2025 Lerian Studio. All rights reserved.
// Use of this source code is governed by an Elastic License 2.0
// that can be found in the LICENSE.md file.

package command

import (
	"context"
	"fmt"
	"strings"
	"time"

	"github.com/google/uuid"
	"github.com/shopspring/decimal"
	"go.opentelemetry.io/otel/trace"

	libCommons "github.com/LerianStudio/lib-commons/v5/commons"
	libLog "github.com/LerianStudio/lib-commons/v5/commons/log"
	libOpentelemetry "github.com/LerianStudio/lib-commons/v5/commons/opentelemetry"
	"github.com/LerianStudio/lib-commons/v5/commons/runtime"

	"github.com/LerianStudio/matcher/internal/exception/domain/entities"
	"github.com/LerianStudio/matcher/internal/exception/domain/value_objects"
	"github.com/LerianStudio/matcher/internal/exception/ports"
)

// TODO(telemetry): exception/adapters/http/handlers.go — logSpanError uses HandleSpanError for
// business outcomes (badRequest, notFound, unprocessable, forbidden). Add logSpanBusinessEvent using
// HandleSpanBusinessErrorEvent and create business-aware variants for 400/404/409/422 responses.
// See reporting/adapters/http/handlers_export_job.go for the reference implementation.

// AdjustEntryCommand contains parameters for the adjust entry operation.
type AdjustEntryCommand struct {
	ExceptionID uuid.UUID
	ReasonCode  string
	Notes       string
	Amount      decimal.Decimal
	Currency    string
	EffectiveAt time.Time
}

type adjustEntryParams struct {
	actor    string
	notes    string
	currency value_objects.CurrencyCode
	reason   value_objects.AdjustmentReasonCode
}

func (uc *ExceptionUseCase) validateAdjustEntry(
	ctx context.Context,
	cmd AdjustEntryCommand,
) (*adjustEntryParams, error) {
	if err := uc.validateResolutionDeps(); err != nil {
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

	if cmd.Amount.IsZero() {
		return nil, ErrZeroAdjustmentAmount
	}

	if cmd.Amount.IsNegative() {
		return nil, ErrNegativeAdjustmentAmount
	}

	currency, err := value_objects.ParseCurrencyCode(cmd.Currency)
	if err != nil {
		return nil, fmt.Errorf("parse currency: %w", err)
	}

	reason, err := value_objects.ParseAdjustmentReason(cmd.ReasonCode)
	if err != nil {
		return nil, fmt.Errorf("parse adjustment reason: %w", err)
	}

	return &adjustEntryParams{actor: actor, notes: notes, currency: currency, reason: reason}, nil
}

// validateResolutionDeps checks all dependencies required by the resolution
// operations (ForceMatch, AdjustEntry). Returns the matching ErrNil*
// sentinel when a dependency is missing. Safe on a nil receiver — returns
// ErrNilExceptionRepository so nil-UseCase callers get a deterministic
// error rather than a panic.
func (uc *ExceptionUseCase) validateResolutionDeps() error {
	if uc == nil || uc.exceptionRepo == nil {
		return ErrNilExceptionRepository
	}

	if uc.resolutionExecutor == nil {
		return ErrNilResolutionExecutor
	}

	if uc.auditPublisher == nil {
		return ErrNilAuditPublisher
	}

	if uc.actorExtractor == nil {
		return ErrNilActorExtractor
	}

	return nil
}

// AdjustEntry resolves an exception by adjusting the related entry.
func (uc *ExceptionUseCase) AdjustEntry(
	ctx context.Context,
	cmd AdjustEntryCommand,
) (*entities.Exception, error) {
	params, err := uc.validateAdjustEntry(ctx, cmd)
	if err != nil {
		return nil, err
	}

	logger, tracer, _, _ := libCommons.NewTrackingFromContext(ctx)

	ctx, span := tracer.Start(ctx, "command.adjust_entry_exception")
	defer span.End()

	return uc.processAdjustEntry(ctx, cmd, params, logger, span)
}

func (uc *ExceptionUseCase) processAdjustEntry(
	ctx context.Context,
	cmd AdjustEntryCommand,
	params *adjustEntryParams,
	logger libLog.Logger,
	span trace.Span,
) (*entities.Exception, error) {
	exception, err := uc.exceptionRepo.FindByID(ctx, cmd.ExceptionID)
	if err != nil {
		libOpentelemetry.HandleSpanError(span, "failed to load exception", err)

		libLog.SafeError(logger, ctx, "failed to load exception", err, runtime.IsProductionMode())

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

	adjustment := ports.AdjustmentInput{
		Amount:      cmd.Amount,
		Currency:    params.currency.String(),
		EffectiveAt: cmd.EffectiveAt,
		Reason:      params.reason,
		Notes:       params.notes,
	}

	// Phase 1: Execute gateway call with automatic revert on failure.
	if err := uc.executeWithRevert(
		ctx,
		exception,
		previousStatus,
		func() error {
			return uc.resolutionExecutor.AdjustEntry(ctx, exception.ID, adjustment)
		},
		logger,
	); err != nil {
		libOpentelemetry.HandleSpanError(span, "adjust entry executor failed", err)

		return nil, fmt.Errorf("adjust entry executor: %w", err)
	}

	// Phase 2: Resolve exception and persist atomically with audit.
	if err := exception.Resolve(ctx, params.notes,
		entities.WithResolutionType("ADJUST_ENTRY"),
		entities.WithResolutionReason(string(params.reason)),
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

	reasonValue := string(params.reason)
	if err := uc.auditPublisher.PublishExceptionEventWithTx(ctx, txLease.SQLTx(), ports.AuditEvent{
		ExceptionID: updated.ID,
		Action:      "ADJUST_ENTRY",
		Actor:       params.actor,
		Notes:       params.notes,
		ReasonCode:  &reasonValue,
		OccurredAt:  time.Now().UTC(),
		Metadata:    map[string]string{"currency": params.currency.String()},
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
