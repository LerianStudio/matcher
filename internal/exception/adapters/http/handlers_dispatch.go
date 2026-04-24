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
	"go.opentelemetry.io/otel/trace"

	libLog "github.com/LerianStudio/lib-commons/v5/commons/log"
	libHTTP "github.com/LerianStudio/lib-commons/v5/commons/net/http"

	"github.com/LerianStudio/matcher/internal/auth"
	"github.com/LerianStudio/matcher/internal/exception/adapters/http/dto"
	"github.com/LerianStudio/matcher/internal/exception/domain/entities"
	"github.com/LerianStudio/matcher/internal/exception/services/command"
)

// DispatchToExternal dispatches an exception to an external system.
// @Summary Dispatch exception to external system
// @Description Dispatches an exception to an external ticketing system such as JIRA or ServiceNow for tracking and resolution.
// @ID dispatchException
// @Tags Exception
// @Accept json
// @Produce json
// @Security BearerAuth
// @Param X-Request-Id header string false "Request ID for tracing"
// @Param X-Idempotency-Key header string false "Idempotency key for safe retries"
// @Param exceptionId path string true "Exception ID" format(uuid)
// @Param request body dto.DispatchRequest true "Dispatch payload"
// @Success 200 {object} dto.DispatchResponse
// @Failure 400 {object} sharedhttp.ErrorResponse "Invalid request payload"
// @Failure 401 {object} sharedhttp.ErrorResponse "Unauthorized"
// @Failure 403 {object} sharedhttp.ErrorResponse "Forbidden"
// @Failure 404 {object} sharedhttp.ErrorResponse "Exception not found"
// @Failure 422 {object} sharedhttp.ErrorResponse "Unprocessable entity: invalid state transition or connector not configured"
// @Failure 500 {object} sharedhttp.ErrorResponse "Internal server error"
// @Router /v1/exceptions/{exceptionId}/dispatch [post]
func (handler *Handlers) DispatchToExternal(fiberCtx *fiber.Ctx) error {
	ctx, span, logger := startHandlerSpan(fiberCtx, "handler.exception.dispatch")
	defer span.End()

	exceptionID, tenantID, err := libHTTP.ParseAndVerifyResourceScopedID(
		fiberCtx,
		"exceptionId",
		libHTTP.IDLocationParam,
		handler.exceptionVerifier,
		auth.GetTenantID,
		ErrMissingExceptionID,
		ErrInvalidExceptionID,
		ErrExceptionAccessDenied,
		"exception",
	)
	if err != nil {
		return handler.handleExceptionVerificationError(ctx, fiberCtx, span, logger, err)
	}

	libHTTP.SetExceptionSpanAttributes(span, tenantID, exceptionID)

	var req dto.DispatchRequest

	if err := libHTTP.ParseBodyAndValidate(fiberCtx, &req); err != nil {
		return handler.badRequest(ctx, fiberCtx, span, logger, "invalid request body", err)
	}

	result, err := handler.commandUC.Dispatch(ctx, command.DispatchCommand{
		ExceptionID:  exceptionID,
		TargetSystem: req.TargetSystem,
		Queue:        req.Queue,
	})
	if err != nil {
		return handler.handleDispatchError(ctx, fiberCtx, span, logger, err)
	}

	if err := libHTTP.Respond(fiberCtx, fiber.StatusOK, dto.DispatchResponse{
		ExceptionID:       result.ExceptionID.String(),
		Target:            result.Target,
		ExternalReference: result.ExternalReference,
		Acknowledged:      result.Acknowledged,
		DispatchedAt:      result.DispatchedAt.Format(time.RFC3339),
	}); err != nil {
		return fmt.Errorf("respond dispatch exception: %w", err)
	}

	return nil
}

// handleDispatchError maps dispatch use case errors to HTTP responses.
func (handler *Handlers) handleDispatchError(
	ctx context.Context,
	fiberCtx *fiber.Ctx,
	span trace.Span,
	logger libLog.Logger,
	err error,
) error {
	// Not-found: raw sql.ErrNoRows is a defensive fallback for edge cases where
	// the repository does not convert to the domain error.
	if errors.Is(err, sql.ErrNoRows) || errors.Is(err, entities.ErrExceptionNotFound) {
		return handler.notFoundWithSlug(ctx, fiberCtx, span, logger, "exception_not_found", "exception not found", err)
	}

	if errors.Is(err, command.ErrExceptionIDRequired) ||
		errors.Is(err, command.ErrTargetSystemRequired) ||
		errors.Is(err, command.ErrActorRequired) {
		return handler.badRequest(ctx, fiberCtx, span, logger, err.Error(), err)
	}

	if errors.Is(err, command.ErrUnsupportedTargetSystem) {
		return handler.unprocessableWithSlug(ctx, fiberCtx, span, logger, "dispatch_target_unsupported", err.Error(), err)
	}

	if errors.Is(err, command.ErrDispatchConnectorNotConfigured) {
		return handler.unprocessableWithSlug(ctx, fiberCtx, span, logger, "dispatch_connector_not_configured", "connector not configured for target system", err)
	}

	return handler.internalError(ctx, fiberCtx, span, logger, "failed to dispatch exception", err)
}
