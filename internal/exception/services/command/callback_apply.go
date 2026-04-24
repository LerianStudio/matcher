// Copyright 2025 Lerian Studio. All rights reserved.
// Use of this source code is governed by an Elastic License 2.0
// that can be found in the LICENSE.md file.

package command

import (
	"context"
	"database/sql"
	"fmt"
	"strings"
	"time"

	"go.opentelemetry.io/otel/trace"

	libLog "github.com/LerianStudio/lib-commons/v5/commons/log"
	libOpentelemetry "github.com/LerianStudio/lib-commons/v5/commons/opentelemetry"
	"github.com/LerianStudio/lib-commons/v5/commons/pointers"
	"github.com/LerianStudio/lib-commons/v5/commons/runtime"

	"github.com/LerianStudio/matcher/internal/exception/domain/entities"
	"github.com/LerianStudio/matcher/internal/exception/domain/value_objects"
	"github.com/LerianStudio/matcher/internal/exception/ports"
)

func (uc *ExceptionUseCase) applyCallback(
	ctx context.Context,
	exception *entities.Exception,
	params *callbackParams,
) error {
	if exception == nil {
		return entities.ErrExceptionNil
	}

	// External tracking fields are set before the status check so callbacks always
	// update the external reference, even when the status has not changed.
	exception.ExternalSystem = pointers.String(params.externalSystem)
	exception.ExternalIssueID = pointers.String(params.externalIssueID)

	if exception.Status == params.status {
		applyCallbackMetadataUpdate(exception, params)
		return nil
	}

	if err := value_objects.ValidateResolutionTransition(exception.Status, params.status); err != nil {
		return fmt.Errorf("validate resolution transition: %w", err)
	}

	// OPEN is not a valid callback target — exceptions are created in OPEN status
	// and the transition table only allows OPEN → ASSIGNED or OPEN → RESOLVED.
	// The transition table allows PENDING_RESOLUTION → OPEN (for revert/abort),
	// but callbacks should only drive forward to ASSIGNED or RESOLVED.
	if params.status == value_objects.ExceptionStatusOpen {
		return ErrCallbackOpenNotValidTarget
	}

	return applyCallbackStatusTransition(ctx, exception, params)
}

// applyCallbackMetadataUpdate applies assignment metadata changes (assignee, due date)
// when the callback status matches the current exception status.
func applyCallbackMetadataUpdate(exception *entities.Exception, params *callbackParams) {
	updated := false

	if trimmed := strings.TrimSpace(params.assignee); trimmed != "" {
		exception.AssignedTo = &trimmed
		updated = true
	}

	if params.dueAt != nil {
		exception.DueAt = params.dueAt
		updated = true
	}

	if updated {
		exception.UpdatedAt = time.Now().UTC()
	}
}

// applyCallbackStatusTransition dispatches the status transition to the appropriate
// exception method (Assign or Resolve) based on the target status.
func applyCallbackStatusTransition(
	ctx context.Context,
	exception *entities.Exception,
	params *callbackParams,
) error {
	switch params.status {
	case value_objects.ExceptionStatusAssigned:
		if strings.TrimSpace(params.assignee) == "" {
			return ErrCallbackAssigneeRequired
		}

		if err := exception.Assign(ctx, params.assignee, params.dueAt); err != nil {
			return fmt.Errorf("assign exception: %w", err)
		}

		return nil
	case value_objects.ExceptionStatusResolved:
		notes := params.resolutionNotes
		if notes == nil || strings.TrimSpace(*notes) == "" {
			notes = pointers.String(fmt.Sprintf("Resolved via %s callback", params.externalSystem))
		}

		if err := exception.Resolve(ctx, *notes); err != nil {
			return fmt.Errorf("resolve exception: %w", err)
		}

		return nil
	default:
		return fmt.Errorf("%w: %s", ErrCallbackStatusUnsupported, params.status)
	}
}

func (uc *ExceptionUseCase) processCallback(
	ctx context.Context,
	cmd ProcessCallbackCommand,
	params *callbackParams,
	logger libLog.Logger,
	span trace.Span,
) error {
	exception, err := uc.exceptionRepo.FindByID(ctx, cmd.ExceptionID)
	if err != nil {
		libOpentelemetry.HandleSpanError(span, "failed to find exception", err)

		libLog.SafeError(logger, ctx, "failed to find exception", err, runtime.IsProductionMode())

		uc.markIdempotencyFailed(ctx, params.dedupeKey)

		return fmt.Errorf("find exception: %w", err)
	}

	if err := uc.applyCallback(ctx, exception, params); err != nil {
		libOpentelemetry.HandleSpanBusinessErrorEvent(span, "failed to apply callback", err)
		uc.markIdempotencyFailed(ctx, params.dedupeKey)

		return fmt.Errorf("apply callback: %w", err)
	}

	// Atomic transaction: update exception state AND create audit log in same transaction.
	// This ensures SOX compliance - if either fails, both are rolled back.
	txLease, err := uc.infraProvider.BeginTx(ctx)
	if err != nil {
		libOpentelemetry.HandleSpanError(span, "failed to begin transaction", err)
		uc.markIdempotencyFailed(ctx, params.dedupeKey)

		return fmt.Errorf("begin transaction: %w", err)
	}

	defer func() {
		_ = txLease.Rollback() // No-op if already committed
	}()

	updated, err := uc.exceptionRepo.UpdateWithTx(ctx, txLease.SQLTx(), exception)
	if err != nil {
		libOpentelemetry.HandleSpanError(span, "failed to update exception", err)
		uc.markIdempotencyFailed(ctx, params.dedupeKey)

		return fmt.Errorf("update exception: %w", err)
	}

	if updated == nil {
		uc.markIdempotencyFailed(ctx, params.dedupeKey)

		return fmt.Errorf("update exception: %w", ErrUnexpectedNilResult)
	}

	if err := uc.publishCallbackAudit(ctx, txLease.SQLTx(), cmd, params, updated, span); err != nil {
		return err
	}

	if err := txLease.Commit(); err != nil {
		libOpentelemetry.HandleSpanError(span, "failed to commit transaction", err)
		uc.markIdempotencyFailed(ctx, params.dedupeKey)

		return fmt.Errorf("commit transaction: %w", err)
	}

	if err := uc.idempotencyRepo.MarkComplete(ctx, params.dedupeKey, nil, 0); err != nil {
		libOpentelemetry.HandleSpanError(span, "failed to mark idempotency complete", err)

		logger.Log(ctx, libLog.LevelWarn, fmt.Sprintf("failed to mark idempotency complete after successful processing: %v", err))
	}

	return nil
}

func (uc *ExceptionUseCase) publishCallbackAudit(
	ctx context.Context,
	tx *sql.Tx,
	cmd ProcessCallbackCommand,
	params *callbackParams,
	updated *entities.Exception,
	span trace.Span,
) error {
	auditNotes := ""
	if params.resolutionNotes != nil {
		auditNotes = *params.resolutionNotes
	}

	callbackType := normalizeCallbackString(cmd.CallbackType)
	if callbackType == "" {
		callbackType = params.externalSystem
	}

	metadata := map[string]string{
		"idempotency_key_hash": idempotencyKeyHash(params.idempotencyKey),
		"callback_type":        callbackType,
		"external_system":      params.externalSystem,
		"external_issue_id":    params.externalIssueID,
		"status":               params.status.String(),
	}

	if params.assignee != "" {
		metadata["assignee"] = params.assignee
	}

	if params.dueAt != nil {
		metadata["due_at"] = params.dueAt.UTC().Format(time.RFC3339)
	}

	if params.updatedAt != nil {
		metadata["updated_at"] = params.updatedAt.UTC().Format(time.RFC3339)
	}

	if err := uc.auditPublisher.PublishExceptionEventWithTx(ctx, tx, ports.AuditEvent{
		ExceptionID: updated.ID,
		Action:      "CALLBACK_PROCESSED",
		Actor:       "system",
		Notes:       auditNotes,
		OccurredAt:  time.Now().UTC(),
		Metadata:    metadata,
	}); err != nil {
		libOpentelemetry.HandleSpanError(span, "audit publish failed", err)
		uc.markIdempotencyFailed(ctx, params.dedupeKey)

		return fmt.Errorf("publish audit: %w", err)
	}

	return nil
}
