// Copyright 2025 Lerian Studio. All rights reserved.
// Use of this source code is governed by an Elastic License 2.0
// that can be found in the LICENSE.md file.

package command

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"strings"
	"time"

	"github.com/google/uuid"
	"go.opentelemetry.io/otel/trace"

	libCommons "github.com/LerianStudio/lib-commons/v5/commons"
	libLog "github.com/LerianStudio/lib-commons/v5/commons/log"
	libOpentelemetry "github.com/LerianStudio/lib-commons/v5/commons/opentelemetry"

	"github.com/LerianStudio/matcher/internal/exception/ports"
	shared "github.com/LerianStudio/matcher/internal/shared/domain"
)

func (uc *ExceptionUseCase) handleExistingCallback(
	ctx context.Context,
	cmd ProcessCallbackCommand,
	params *callbackParams,
	logger libLog.Logger,
	span trace.Span,
	cachedResult *shared.IdempotencyResult,
) error {
	if cachedResult == nil {
		libOpentelemetry.HandleSpanBusinessErrorEvent(span, "idempotency state missing", ErrCallbackRetryable)
		return ErrCallbackRetryable
	}

	switch cachedResult.Status {
	case shared.IdempotencyStatusComplete:
		return uc.handleDuplicateCallback(ctx, cmd, params, logger, span)
	case shared.IdempotencyStatusPending:
		libOpentelemetry.HandleSpanBusinessErrorEvent(span, "callback is still processing", ErrCallbackInProgress)
		return ErrCallbackInProgress
	case shared.IdempotencyStatusFailed:
		reacquired, err := uc.idempotencyRepo.TryReacquireFromFailed(ctx, params.dedupeKey)
		if err != nil {
			libOpentelemetry.HandleSpanError(span, "failed to reacquire failed idempotency key", err)
			return fmt.Errorf("reacquire failed callback: %w", err)
		}

		if reacquired {
			return uc.processCallback(ctx, cmd, params, logger, span)
		}

		libOpentelemetry.HandleSpanBusinessErrorEvent(span, "callback is still processing", ErrCallbackInProgress)

		return ErrCallbackInProgress
	case shared.IdempotencyStatusUnknown:
		libOpentelemetry.HandleSpanBusinessErrorEvent(span, "callback requires retry", ErrCallbackRetryable)
		return ErrCallbackRetryable
	default:
		libOpentelemetry.HandleSpanBusinessErrorEvent(span, "invalid idempotency status", ErrCallbackRetryable)
		return ErrCallbackRetryable
	}
}

func scopeCallbackIdempotencyKey(
	clientKey shared.IdempotencyKey,
	exceptionID uuid.UUID,
	externalSystem string,
) shared.IdempotencyKey {
	base := strings.ToUpper(strings.TrimSpace(externalSystem)) + ":" + exceptionID.String() + ":" + clientKey.String()
	hash := sha256.Sum256([]byte(base))

	return shared.IdempotencyKey("cb_" + hex.EncodeToString(hash[:]))
}

func (uc *ExceptionUseCase) markIdempotencyFailed(
	ctx context.Context,
	key shared.IdempotencyKey,
) {
	if err := uc.idempotencyRepo.MarkFailed(ctx, key); err != nil {
		logger, _, _, _ := libCommons.NewTrackingFromContext(ctx)
		logger.Log(ctx, libLog.LevelWarn, fmt.Sprintf("failed to mark idempotency failed: %v", err))
	}
}

func (uc *ExceptionUseCase) handleDuplicateCallback(
	ctx context.Context,
	cmd ProcessCallbackCommand,
	params *callbackParams,
	logger libLog.Logger,
	span trace.Span,
) error {
	logger.Log(ctx, libLog.LevelInfo, fmt.Sprintf("duplicate callback ignored (idempotency_key_hash=%s)", idempotencyKeyHash(params.idempotencyKey)))

	if err := uc.auditPublisher.PublishExceptionEvent(ctx, ports.AuditEvent{
		ExceptionID: cmd.ExceptionID,
		Action:      "CALLBACK_DUPLICATE_IGNORED",
		Actor:       "system",
		OccurredAt:  time.Now().UTC(),
		Metadata: map[string]string{
			"idempotency_key_hash": idempotencyKeyHash(params.idempotencyKey),
			"callback_type":        normalizeCallbackString(cmd.CallbackType),
			"external_system":      params.externalSystem,
			"external_issue_id":    params.externalIssueID,
			"status":               params.status.String(),
		},
	}); err != nil {
		libOpentelemetry.HandleSpanError(span, "audit publish failed for duplicate", err)
		return fmt.Errorf("publish duplicate audit: %w", err)
	}

	return nil
}
