// Copyright 2026 Lerian Studio. All rights reserved.
// Use of this source code is governed by an Elastic License 2.0
// that can be found in the LICENSE.md file.

package command

import (
	"context"
	"database/sql"
	"fmt"
	"time"

	"github.com/google/uuid"
	"go.opentelemetry.io/otel/trace"

	libOpentelemetry "github.com/LerianStudio/lib-commons/v5/commons/opentelemetry"

	disputeDomain "github.com/LerianStudio/matcher/internal/exception/domain/dispute"
	"github.com/LerianStudio/matcher/internal/exception/domain/entities"
	"github.com/LerianStudio/matcher/internal/streaming/emission"
)

func (uc *ExceptionUseCase) emitExceptionImportant(ctx context.Context, span trace.Span, definitionKey string, exception *entities.Exception, extra map[string]any) {
	if err := uc.emitExceptionPayload(ctx, span, nil, false, definitionKey, exception.ID.String(), exceptionPayload(exception, extra)); err != nil {
		libOpentelemetry.HandleSpanError(span, "failed to emit important exception streaming event "+definitionKey, err)
	}
}

func (uc *ExceptionUseCase) emitExceptionCritical(ctx context.Context, span trace.Span, tx *sql.Tx, definitionKey string, exception *entities.Exception, extra map[string]any) error {
	return uc.emitExceptionPayload(ctx, span, tx, true, definitionKey, exception.ID.String(), exceptionPayload(exception, extra))
}

func (uc *ExceptionUseCase) emitExceptionPayload(ctx context.Context, span trace.Span, tx *sql.Tx, requireTx bool, definitionKey, subject string, payload map[string]any) error {
	options := []emission.Option{}
	if requireTx {
		options = append(options, emission.RequireOutboxTx())
	}

	if tx != nil {
		options = append(options, emission.WithOutboxTx(tx))
	}

	if err := emission.Emit(ctx, uc.streamEmitter, definitionKey, subject, payload, options...); err != nil {
		if span != nil {
			libOpentelemetry.HandleSpanError(span, "failed to emit streaming event "+definitionKey, err)
		}

		return fmt.Errorf("emit exception streaming event %s: %w", definitionKey, err)
	}

	return nil
}

func exceptionPayload(exception *entities.Exception, extra map[string]any) map[string]any {
	payload := map[string]any{
		"exception_id": exception.ID.String(),
		"status":       string(exception.Status),
		"version":      exception.Version,
	}

	if exception.ResolutionType != nil {
		payload["resolution_type"] = *exception.ResolutionType
	}

	if exception.TransactionID != uuid.Nil {
		payload["transaction_id"] = exception.TransactionID.String()
	}

	for key, value := range extra {
		payload[key] = value
	}

	return payload
}

func (uc *ExceptionUseCase) emitCommentAdded(ctx context.Context, span trace.Span, comment *entities.ExceptionComment) {
	if comment == nil {
		return
	}

	if err := uc.emitExceptionPayload(ctx, span, nil, false, "exception_comment.added", comment.ID.String(), map[string]any{
		"comment_id":   comment.ID.String(),
		"exception_id": comment.ExceptionID.String(),
		"created_at":   formatExceptionTime(comment.CreatedAt),
	}); err != nil {
		libOpentelemetry.HandleSpanError(span, "failed to emit exception comment added", err)
	}
}

func (uc *ExceptionUseCase) emitCommentDeleted(ctx context.Context, span trace.Span, exceptionID, commentID uuid.UUID, actor string) {
	if err := uc.emitExceptionPayload(ctx, span, nil, false, "exception_comment.deleted", commentID.String(), map[string]any{
		"comment_id":   commentID.String(),
		"exception_id": exceptionID.String(),
		"actor":        actor,
		"deleted_at":   formatExceptionTime(time.Now().UTC()),
	}); err != nil {
		libOpentelemetry.HandleSpanError(span, "failed to emit exception comment deleted", err)
	}
}

func (uc *ExceptionUseCase) emitDisputeCritical(ctx context.Context, span trace.Span, tx *sql.Tx, definitionKey string, dispute *disputeDomain.Dispute, extra map[string]any) error {
	if dispute == nil {
		return nil
	}

	// TODO(streaming): re-add version once Dispute entity gains optimistic concurrency.
	// The previous "version": 0 hardcode was structurally meaningless and risked
	// consumer misinterpretation; the field is intentionally absent until the
	// Dispute aggregate models a real optimistic-concurrency version.
	payload := map[string]any{
		"dispute_id":   dispute.ID.String(),
		"exception_id": dispute.ExceptionID.String(),
		"state":        string(dispute.State),
	}
	if dispute.Resolution != nil {
		payload["resolution"] = *dispute.Resolution
	}

	for key, value := range extra {
		payload[key] = value
	}

	return uc.emitExceptionPayload(ctx, span, tx, true, definitionKey, dispute.ID.String(), payload)
}

// formatExceptionTime delegates to emission.FormatTime; preserved as a thin
// wrapper for backward compatibility with existing unit tests.
func formatExceptionTime(value time.Time) string {
	return emission.FormatTime(value)
}
