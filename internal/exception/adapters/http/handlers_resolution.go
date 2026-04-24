// Copyright 2025 Lerian Studio. All rights reserved.
// Use of this source code is governed by an Elastic License 2.0
// that can be found in the LICENSE.md file.

package http

import (
	"context"
	"database/sql"
	"errors"
	"fmt"

	"github.com/gofiber/fiber/v2"
	"go.opentelemetry.io/otel/trace"

	libLog "github.com/LerianStudio/lib-commons/v5/commons/log"
	libHTTP "github.com/LerianStudio/lib-commons/v5/commons/net/http"

	"github.com/LerianStudio/matcher/internal/auth"
	"github.com/LerianStudio/matcher/internal/exception/adapters/http/dto"
	"github.com/LerianStudio/matcher/internal/exception/domain/entities"
	"github.com/LerianStudio/matcher/internal/exception/domain/value_objects"
	"github.com/LerianStudio/matcher/internal/exception/services/command"
	crossAdapters "github.com/LerianStudio/matcher/internal/shared/adapters/cross"
	sharedhttp "github.com/LerianStudio/matcher/internal/shared/adapters/http"
)

var _ = sharedhttp.ErrorResponse{}

type errorResponseHandler func(*Handlers, context.Context, *fiber.Ctx, trace.Span, libLog.Logger, string, error) error

// errorMapping associates a domain error with an HTTP response handler and message.
// When message is empty, the error's own message (err.Error()) is used.
type errorMapping struct {
	target  error
	handler errorResponseHandler
	slug    string
	message string
}

// exceptionErrorMappings defines the table-driven mapping from domain errors
// to HTTP responses for exception operations. Handlers are method
// expressions so each entry adapts to whichever *Handlers instance is
// invoking handleExceptionError.
//
//nolint:gochecknoglobals // package-level table used by handleExceptionError
var exceptionErrorMappings = []errorMapping{
	// Exception not found -> 404
	{sql.ErrNoRows, (*Handlers).exceptionNotFound, "", "exception not found"},
	{entities.ErrExceptionNotFound, (*Handlers).exceptionNotFound, "", "exception not found"},

	// Validation errors -> 400 (message derived from error)
	{command.ErrExceptionIDRequired, (*Handlers).badRequest, "", ""},
	{command.ErrActorRequired, (*Handlers).badRequest, "", ""},
	{command.ErrZeroAdjustmentAmount, (*Handlers).badRequest, "", ""},
	{command.ErrNegativeAdjustmentAmount, (*Handlers).badRequest, "", ""},
	{command.ErrInvalidCurrency, (*Handlers).badRequest, "", ""},
	{value_objects.ErrInvalidCurrencyCode, (*Handlers).badRequest, "", ""},
	{value_objects.ErrInvalidAdjustmentReason, (*Handlers).badRequest, "", ""},
	{value_objects.ErrInvalidOverrideReason, (*Handlers).badRequest, "", ""},
	{entities.ErrResolutionNotesRequired, (*Handlers).badRequest, "", ""},

	// State transition errors -> 422
	{entities.ErrExceptionMustBeOpenOrAssignedToResolve, (*Handlers).unprocessable, "exception_invalid_state", "exception cannot be resolved in current state"},
	{value_objects.ErrInvalidResolutionTransition, (*Handlers).unprocessable, "exception_invalid_state", "exception cannot be resolved in current state"},

	// Cross-context lookup failures: the exception references data that
	// cannot be resolved (transaction, ingestion job, or source not found).
	// These indicate a data integrity issue rather than a system error.
	{crossAdapters.ErrTransactionNotFound, (*Handlers).notFound, "", "transaction referenced by exception not found"},
	{crossAdapters.ErrIngestionJobNotFound, (*Handlers).unprocessable, "", "unable to resolve reconciliation context for exception"},
	{crossAdapters.ErrSourceNotFound, (*Handlers).unprocessable, "", "unable to resolve reconciliation context for exception"},
	{crossAdapters.ErrContextNotFound, (*Handlers).unprocessable, "", "unable to resolve reconciliation context for exception"},

	// Infrastructure errors -> 500
	{crossAdapters.ErrContextLookupNotInitialized, (*Handlers).internalError, "", "context lookup service not configured"},
}

// handleExceptionError maps exception use case errors to HTTP responses.
func (handler *Handlers) handleExceptionError(
	ctx context.Context,
	fiberCtx *fiber.Ctx,
	span trace.Span,
	logger libLog.Logger,
	err error,
) error {
	for _, mapping := range exceptionErrorMappings {
		if errors.Is(err, mapping.target) {
			msg := mapping.message
			if msg == "" {
				msg = err.Error()
			}

			if mapping.slug != "" {
				return handler.unprocessableWithSlug(ctx, fiberCtx, span, logger, mapping.slug, msg, err)
			}

			return mapping.handler(handler, ctx, fiberCtx, span, logger, msg, err)
		}
	}

	return handler.internalError(ctx, fiberCtx, span, logger, "failed to process exception", err)
}

// ForceMatch resolves an exception by forcing a match.
// @Summary Force match an exception
// @Description Resolves an exception by forcing a match with an override reason. Used when manual intervention determines the transaction should be considered matched despite discrepancies.
// @ID forceMatchException
// @Tags Exception
// @Accept json
// @Produce json
// @Security BearerAuth
// @Param X-Request-Id header string false "Request ID for tracing"
// @Param X-Idempotency-Key header string false "Idempotency key for safe retries"
// @Param exceptionId path string true "Exception ID" format(uuid)
// @Param request body dto.ForceMatchRequest true "Force match payload"
// @Success 200 {object} dto.ExceptionResponse
// @Failure 400 {object} sharedhttp.ErrorResponse "Invalid request payload"
// @Failure 401 {object} sharedhttp.ErrorResponse "Unauthorized"
// @Failure 403 {object} sharedhttp.ErrorResponse "Forbidden"
// @Failure 404 {object} sharedhttp.ErrorResponse "Exception not found"
// @Failure 422 {object} sharedhttp.ErrorResponse "Unprocessable entity: invalid state transition"
// @Failure 500 {object} sharedhttp.ErrorResponse "Internal server error"
// @Router /v1/exceptions/{exceptionId}/force-match [post]
func (handler *Handlers) ForceMatch(fiberCtx *fiber.Ctx) error {
	ctx, span, logger := startHandlerSpan(fiberCtx, "handler.exception.force_match")
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

	var req dto.ForceMatchRequest

	if err := libHTTP.ParseBodyAndValidate(fiberCtx, &req); err != nil {
		return handler.badRequest(ctx, fiberCtx, span, logger, "invalid request body", err)
	}

	result, err := handler.commandUC.ForceMatch(ctx, command.ForceMatchCommand{
		ExceptionID:    exceptionID,
		OverrideReason: req.OverrideReason,
		Notes:          req.Notes,
	})
	if err != nil {
		return handler.handleExceptionError(ctx, fiberCtx, span, logger, err)
	}

	if err := libHTTP.Respond(fiberCtx, fiber.StatusOK, dto.ExceptionToResponse(result)); err != nil {
		return fmt.Errorf("respond resolve exception: %w", err)
	}

	return nil
}

// AdjustEntry resolves an exception by adjusting the related entry.
// @Summary Adjust entry for an exception
// @Description Resolves an exception by creating an adjustment entry. Used when a monetary correction is needed to reconcile the transaction.
// @ID adjustEntryException
// @Tags Exception
// @Accept json
// @Produce json
// @Security BearerAuth
// @Param X-Request-Id header string false "Request ID for tracing"
// @Param X-Idempotency-Key header string false "Idempotency key for safe retries"
// @Param exceptionId path string true "Exception ID" format(uuid)
// @Param request body dto.AdjustEntryRequest true "Adjust entry payload"
// @Success 200 {object} dto.ExceptionResponse
// @Failure 400 {object} sharedhttp.ErrorResponse "Invalid request payload"
// @Failure 401 {object} sharedhttp.ErrorResponse "Unauthorized"
// @Failure 403 {object} sharedhttp.ErrorResponse "Forbidden"
// @Failure 404 {object} sharedhttp.ErrorResponse "Exception not found"
// @Failure 422 {object} sharedhttp.ErrorResponse "Unprocessable entity: invalid state transition"
// @Failure 500 {object} sharedhttp.ErrorResponse "Internal server error"
// @Router /v1/exceptions/{exceptionId}/adjust-entry [post]
func (handler *Handlers) AdjustEntry(fiberCtx *fiber.Ctx) error {
	ctx, span, logger := startHandlerSpan(fiberCtx, "handler.exception.adjust_entry")
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

	var req dto.AdjustEntryRequest

	if err := libHTTP.ParseBodyAndValidate(fiberCtx, &req); err != nil {
		return handler.badRequest(ctx, fiberCtx, span, logger, "invalid request body", err)
	}

	result, err := handler.commandUC.AdjustEntry(ctx, command.AdjustEntryCommand{
		ExceptionID: exceptionID,
		ReasonCode:  req.ReasonCode,
		Notes:       req.Notes,
		Amount:      req.Amount,
		Currency:    req.Currency,
		EffectiveAt: req.EffectiveAt,
	})
	if err != nil {
		return handler.handleExceptionError(ctx, fiberCtx, span, logger, err)
	}

	if err := libHTTP.Respond(fiberCtx, fiber.StatusOK, dto.ExceptionToResponse(result)); err != nil {
		return fmt.Errorf("respond adjust entry: %w", err)
	}

	return nil
}
