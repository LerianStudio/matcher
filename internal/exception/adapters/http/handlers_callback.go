// Copyright 2025 Lerian Studio. All rights reserved.
// Use of this source code is governed by an Elastic License 2.0
// that can be found in the LICENSE.md file.

package http

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"time"

	"github.com/gofiber/fiber/v2"
	"github.com/google/uuid"
	"go.opentelemetry.io/otel/trace"

	libLog "github.com/LerianStudio/lib-commons/v5/commons/log"
	libHTTP "github.com/LerianStudio/lib-commons/v5/commons/net/http"

	"github.com/LerianStudio/matcher/internal/exception/adapters/http/dto"
	"github.com/LerianStudio/matcher/internal/exception/domain/entities"
	"github.com/LerianStudio/matcher/internal/exception/services/command"
	sharedhttp "github.com/LerianStudio/matcher/internal/shared/adapters/http"
	shared "github.com/LerianStudio/matcher/internal/shared/domain"
)

var _ = sharedhttp.ErrorResponse{}

// ProcessCallback handles webhook callbacks from external systems.
// It processes status updates from external ticketing/dispatch systems.
//
// @ID processCallback
// @Summary Process external system callback
// @Description Receives webhook callbacks from external systems to update exception status
// @Tags Exception
// @Accept json
// @Produce json
// @Security BearerAuth
// @Param X-Idempotency-Key header string true "Idempotency key for deduplication"
// @Param exceptionId path string true "Exception ID" format(uuid)
// @Param request body dto.ProcessCallbackRequest true "Callback payload"
// @Success 200 {object} dto.ProcessCallbackResponse "Callback processed successfully"
// @Failure 400 {object} sharedhttp.ErrorResponse "Invalid request"
// @Failure 401 {object} sharedhttp.ErrorResponse "Unauthorized - Bearer token missing or invalid"
// @Failure 403 {object} sharedhttp.ErrorResponse "Forbidden - insufficient permissions"
// @Failure 404 {object} sharedhttp.ErrorResponse "Exception not found"
// @Failure 409 {object} sharedhttp.ErrorResponse "Conflict or idempotency"
// @Failure 429 {object} sharedhttp.ErrorResponse "Rate limit exceeded"
// @Failure 500 {object} sharedhttp.ErrorResponse "Internal server error"
// @Router /v1/exceptions/{exceptionId}/callback [post]
//
// Design Decision: This handler intentionally does NOT use ParseAndVerifyResourceScopedID
// for tenant ownership verification. Callbacks from external systems (e.g., JIRA, ServiceNow)
// arrive via authenticated webhook endpoints and identify exceptions by ID only.
// Tenant isolation is ensured at the database layer via schema-per-tenant (SET LOCAL search_path)
// which is applied automatically by pgcommon.WithTenantTxProvider in the CallbackUseCase.
// The JWT authentication middleware on the route already validates the caller's identity.
func (handler *Handlers) ProcessCallback(fiberCtx *fiber.Ctx) error {
	ctx, span, logger := startHandlerSpan(fiberCtx, "handler.exception.process_callback")
	defer span.End()

	// Parse exception ID from path
	exceptionIDStr := fiberCtx.Params("exceptionId")
	if exceptionIDStr == "" {
		return handler.badRequest(ctx, fiberCtx, span, logger, "exception id is required", ErrMissingParameter)
	}

	exceptionID, err := uuid.Parse(exceptionIDStr)
	if err != nil {
		return handler.badRequest(ctx, fiberCtx, span, logger, "invalid exception id", ErrInvalidExceptionID)
	}

	// Parse idempotency key from header
	idempotencyKey := fiberCtx.Get("X-Idempotency-Key")
	if idempotencyKey == "" {
		return handler.badRequest(ctx, fiberCtx, span, logger, "X-Idempotency-Key header is required", ErrMissingParameter)
	}

	// Parse request body
	var req dto.ProcessCallbackRequest
	if err := libHTTP.ParseBodyAndValidate(fiberCtx, &req); err != nil {
		return handler.badRequest(ctx, fiberCtx, span, logger, "invalid request body", err)
	}

	// Build command from DTO
	cmd := command.ProcessCallbackCommand{
		IdempotencyKey:  idempotencyKey,
		ExceptionID:     exceptionID,
		CallbackType:    req.CallbackType,
		ExternalSystem:  req.ExternalSystem,
		ExternalIssueID: req.ExternalIssueID,
		Status:          req.Status,
		ResolutionNotes: req.ResolutionNotes,
		Assignee:        req.Assignee,
		Payload:         req.Payload,
	}

	// Parse optional time fields and normalize to UTC.
	if req.DueAt != nil {
		parsed, parseErr := time.Parse(time.RFC3339, *req.DueAt)
		if parseErr != nil {
			return handler.badRequest(ctx, fiberCtx, span, logger, "invalid dueAt format, expected RFC3339", parseErr)
		}

		utc := parsed.UTC()
		cmd.DueAt = &utc
	}

	if req.UpdatedAt != nil {
		parsed, parseErr := time.Parse(time.RFC3339, *req.UpdatedAt)
		if parseErr != nil {
			return handler.badRequest(ctx, fiberCtx, span, logger, "invalid updatedAt format, expected RFC3339", parseErr)
		}

		utc := parsed.UTC()
		cmd.UpdatedAt = &utc
	}

	// Process the callback
	if err := handler.commandUC.ProcessCallback(ctx, cmd); err != nil {
		return handler.handleCallbackError(ctx, fiberCtx, span, logger, err)
	}

	if err := libHTTP.Respond(fiberCtx, fiber.StatusOK, dto.ProcessCallbackResponse{Status: "accepted"}); err != nil {
		return fmt.Errorf("respond process callback: %w", err)
	}

	return nil
}

// callbackValidationErrors lists all sentinel errors that map to HTTP 400 Bad Request
// for callback processing. Extracted to reduce cyclomatic complexity of handleCallbackError.
var callbackValidationErrors = []error{
	command.ErrCallbackExternalSystem,
	command.ErrCallbackExternalIssueID,
	command.ErrCallbackStatusRequired,
	command.ErrCallbackAssigneeRequired,
	shared.ErrEmptyIdempotencyKey,
	shared.ErrInvalidIdempotencyKey,
	command.ErrCallbackOpenNotValidTarget,
	command.ErrCallbackStatusUnsupported,
	command.ErrExceptionIDRequired,
}

// isCallbackValidationError checks whether err matches any known callback validation sentinel.
func isCallbackValidationError(err error) bool {
	for _, sentinel := range callbackValidationErrors {
		if errors.Is(err, sentinel) {
			return true
		}
	}

	return false
}

// handleCallbackError maps callback use case errors to HTTP responses.
func (handler *Handlers) handleCallbackError(
	ctx context.Context,
	fiberCtx *fiber.Ctx,
	span trace.Span,
	logger libLog.Logger,
	err error,
) error {
	// Rate limit exceeded -> 429
	if errors.Is(err, command.ErrCallbackRateLimitExceeded) {
		handler.logSpanError(ctx, span, logger, "callback rate limit exceeded", err)

		return respondError(fiberCtx, fiber.StatusTooManyRequests, "rate_limit_exceeded", "callback rate limit exceeded")
	}

	if errors.Is(err, command.ErrCallbackInProgress) {
		handler.logSpanError(ctx, span, logger, "callback already in progress", err)

		return respondError(fiberCtx, fiber.StatusConflict, "callback_in_progress", "callback is already being processed")
	}

	if errors.Is(err, command.ErrCallbackRetryable) {
		handler.logSpanError(ctx, span, logger, "callback retry required", err)

		return respondError(fiberCtx, fiber.StatusConflict, "callback_retryable", "callback can be retried")
	}

	// Validation errors -> 400
	if isCallbackValidationError(err) {
		return handler.badRequest(ctx, fiberCtx, span, logger, err.Error(), err)
	}

	// Exception not found -> 404
	if errors.Is(err, sql.ErrNoRows) || errors.Is(err, entities.ErrExceptionNotFound) {
		return handler.notFoundWithSlug(ctx, fiberCtx, span, logger, "exception_not_found", "exception not found", err)
	}

	// Everything else -> 500
	return handler.internalError(ctx, fiberCtx, span, logger, "failed to process callback", err)
}
