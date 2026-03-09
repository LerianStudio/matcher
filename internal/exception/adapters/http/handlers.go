// Package http provides HTTP handlers for exception operations.
//
// TODO(LOW) REVIEW_EXCEPTION L3: Extract repetitive HTTP error handling logic into
// shared helper functions to reduce duplication across handlers.
//
// TODO(LOW) REVIEW_EXCEPTION L7: Add edge case tests for HTTP handlers including
// malformed JSON, boundary values, and concurrent requests.
package http

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"sync/atomic"
	"time"

	"github.com/gofiber/fiber/v2"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/trace"

	libCommons "github.com/LerianStudio/lib-commons/v4/commons"
	libLog "github.com/LerianStudio/lib-commons/v4/commons/log"
	libHTTP "github.com/LerianStudio/lib-commons/v4/commons/net/http"
	libOpentelemetry "github.com/LerianStudio/lib-commons/v4/commons/opentelemetry"

	"github.com/LerianStudio/matcher/internal/auth"
	"github.com/LerianStudio/matcher/internal/exception/adapters/http/dto"
	"github.com/LerianStudio/matcher/internal/exception/domain/dispute"
	"github.com/LerianStudio/matcher/internal/exception/domain/entities"
	"github.com/LerianStudio/matcher/internal/exception/domain/repositories"
	"github.com/LerianStudio/matcher/internal/exception/domain/value_objects"
	"github.com/LerianStudio/matcher/internal/exception/services/command"
	"github.com/LerianStudio/matcher/internal/exception/services/query"
	crossAdapters "github.com/LerianStudio/matcher/internal/shared/adapters/cross"
	"github.com/LerianStudio/matcher/internal/shared/constants"
)

// productionMode indicates whether the application is running in production.
// Set once during handler construction via NewHandler; governs SafeError behavior
// (suppresses internal error details in client responses when true).
// Uses atomic.Bool because parallel tests construct handlers concurrently.
var productionMode atomic.Bool

// Handlers provides HTTP handlers for exception operations.
type Handlers struct {
	exceptionUC       *command.UseCase
	disputeUC         *command.DisputeUseCase
	queryUC           *query.UseCase
	dispatchUC        *command.DispatchUseCase
	commentUC         *command.CommentUseCase
	commentQueryUC    *query.CommentQueryUseCase
	callbackUC        *command.CallbackUseCase
	exceptionVerifier libHTTP.ResourceOwnershipVerifier
	disputeVerifier   libHTTP.ResourceOwnershipVerifier
}

// NewHandlers creates a new Handlers instance with the given use cases and verifiers.
func NewHandlers(
	exceptionUC *command.UseCase,
	disputeUC *command.DisputeUseCase,
	queryUC *query.UseCase,
	dispatchUC *command.DispatchUseCase,
	commentUC *command.CommentUseCase,
	commentQueryUC *query.CommentQueryUseCase,
	callbackUC *command.CallbackUseCase,
	exceptionProvider exceptionProvider,
	disputeProvider disputeProvider,
	production bool,
) (*Handlers, error) {
	if exceptionUC == nil {
		return nil, ErrNilExceptionUseCase
	}

	if disputeUC == nil {
		return nil, ErrNilDisputeUseCase
	}

	if queryUC == nil {
		return nil, ErrNilQueryUseCase
	}

	if dispatchUC == nil {
		return nil, ErrNilDispatchUseCase
	}

	if commentUC == nil {
		return nil, ErrNilCommentUseCase
	}

	if commentQueryUC == nil {
		return nil, ErrNilCommentQueryUseCase
	}

	if callbackUC == nil {
		return nil, ErrNilCallbackUseCase
	}

	if exceptionProvider == nil {
		return nil, ErrNilExceptionProvider
	}

	if disputeProvider == nil {
		return nil, ErrNilDisputeProvider
	}

	productionMode.Store(production)

	return &Handlers{
		exceptionUC:       exceptionUC,
		disputeUC:         disputeUC,
		queryUC:           queryUC,
		dispatchUC:        dispatchUC,
		commentUC:         commentUC,
		commentQueryUC:    commentQueryUC,
		callbackUC:        callbackUC,
		exceptionVerifier: NewExceptionOwnershipVerifier(exceptionProvider),
		disputeVerifier:   NewDisputeOwnershipVerifier(disputeProvider),
	}, nil
}

func startHandlerSpan(c *fiber.Ctx, name string) (context.Context, trace.Span, libLog.Logger) {
	ctx := c.UserContext()
	logger, tracer, _, _ := libCommons.NewTrackingFromContext(ctx)

	if tracer == nil {
		tracer = otel.Tracer("commons.default")
	}

	ctx, span := tracer.Start(ctx, name)

	return ctx, span, logger
}

func logSpanError(ctx context.Context, span trace.Span, logger libLog.Logger, message string, err error) {
	libOpentelemetry.HandleSpanError(span, message, err)
	libLog.SafeError(logger, ctx, message, err, productionMode.Load())
}

func badRequest(
	ctx context.Context,
	fiberCtx *fiber.Ctx,
	span trace.Span,
	logger libLog.Logger,
	message string,
	err error,
) error {
	logSpanError(ctx, span, logger, message, err)

	return libHTTP.RespondError(fiberCtx, fiber.StatusBadRequest, "invalid_request", message)
}

func notFound(
	ctx context.Context,
	fiberCtx *fiber.Ctx,
	span trace.Span,
	logger libLog.Logger,
	message string,
	err error,
) error {
	logSpanError(ctx, span, logger, message, err)

	return libHTTP.RespondError(fiberCtx, fiber.StatusNotFound, "not_found", message)
}

func unprocessable(
	ctx context.Context,
	fiberCtx *fiber.Ctx,
	span trace.Span,
	logger libLog.Logger,
	message string,
	err error,
) error {
	logSpanError(ctx, span, logger, message, err)

	return libHTTP.RespondError(fiberCtx, fiber.StatusUnprocessableEntity, "unprocessable_entity", message)
}

func internalError(
	ctx context.Context,
	fiberCtx *fiber.Ctx,
	span trace.Span,
	logger libLog.Logger,
	message string,
	err error,
) error {
	logSpanError(ctx, span, logger, message, err)

	return libHTTP.RespondError(fiberCtx, fiber.StatusInternalServerError, "internal_server_error", "an unexpected error occurred")
}

func forbidden(ctx context.Context, fiberCtx *fiber.Ctx, span trace.Span, logger libLog.Logger, err error) error {
	const message = "access denied"

	if err == nil {
		err = fmt.Errorf("%w: %s", errForbidden, message)
	}

	libOpentelemetry.HandleSpanError(span, message, err)

	logger.Log(ctx, libLog.LevelWarn, "access denied: "+message)

	return libHTTP.RespondError(fiberCtx, fiber.StatusForbidden, "forbidden", message)
}

// handleExceptionVerificationError maps errors from ParseAndVerifyResourceScopedID to HTTP responses.
func handleExceptionVerificationError(
	ctx context.Context,
	fiberCtx *fiber.Ctx,
	span trace.Span,
	logger libLog.Logger,
	err error,
) error {
	// Invalid or missing exception ID -> bad request
	if errors.Is(err, ErrMissingExceptionID) ||
		errors.Is(err, ErrInvalidExceptionID) {
		return badRequest(ctx, fiberCtx, span, logger, "invalid exception_id", err)
	}

	// Missing or invalid tenant ID -> unauthorized
	if errors.Is(err, libHTTP.ErrTenantIDNotFound) ||
		errors.Is(err, libHTTP.ErrInvalidTenantID) {
		logSpanError(ctx, span, logger, "invalid tenant id", err)

		return libHTTP.RespondError(fiberCtx, fiber.StatusUnauthorized, "unauthorized", "unauthorized")
	}

	// Exception not found -> 404
	if errors.Is(err, ErrExceptionNotFound) {
		return notFound(ctx, fiberCtx, span, logger, "exception not found", err)
	}

	// Infrastructure lookup failures (e.g. database errors during ownership check) -> 500
	if errors.Is(err, libHTTP.ErrLookupFailed) {
		return internalError(ctx, fiberCtx, span, logger, "failed to verify exception access", err)
	}

	// Tenant or ownership issues -> forbidden
	return forbidden(ctx, fiberCtx, span, logger, err)
}

// handleDisputeVerificationError maps errors from ParseAndVerifyResourceScopedID to HTTP responses.
func handleDisputeVerificationError(
	ctx context.Context,
	fiberCtx *fiber.Ctx,
	span trace.Span,
	logger libLog.Logger,
	err error,
) error {
	// Invalid or missing dispute ID -> bad request
	if errors.Is(err, ErrMissingDisputeID) ||
		errors.Is(err, ErrInvalidDisputeID) {
		return badRequest(ctx, fiberCtx, span, logger, "invalid dispute_id", err)
	}

	// Missing or invalid tenant ID -> unauthorized
	if errors.Is(err, libHTTP.ErrTenantIDNotFound) ||
		errors.Is(err, libHTTP.ErrInvalidTenantID) {
		logSpanError(ctx, span, logger, "invalid tenant id", err)

		return libHTTP.RespondError(fiberCtx, fiber.StatusUnauthorized, "unauthorized", "unauthorized")
	}

	// Dispute not found -> 404
	if errors.Is(err, ErrDisputeNotFound) {
		return notFound(ctx, fiberCtx, span, logger, "dispute not found", err)
	}

	// Infrastructure lookup failures (e.g. database errors during ownership check) -> 500
	if errors.Is(err, libHTTP.ErrLookupFailed) {
		return internalError(ctx, fiberCtx, span, logger, "failed to verify dispute access", err)
	}

	// Tenant or ownership issues -> forbidden
	return forbidden(ctx, fiberCtx, span, logger, err)
}

// errorResponseHandler is the function signature for HTTP error response helpers.
type errorResponseHandler func(context.Context, *fiber.Ctx, trace.Span, libLog.Logger, string, error) error

// errorMapping associates a domain error with an HTTP response handler and message.
// When message is empty, the error's own message (err.Error()) is used.
type errorMapping struct {
	target  error
	handler errorResponseHandler
	message string
}

// exceptionErrorMappings defines the table-driven mapping from domain errors
// to HTTP responses for exception operations.
//
//nolint:gochecknoglobals // package-level table used by handleExceptionError
var exceptionErrorMappings = []errorMapping{
	// Exception not found -> 404
	{sql.ErrNoRows, notFound, "exception not found"},
	{entities.ErrExceptionNotFound, notFound, "exception not found"},

	// Validation errors -> 400 (message derived from error)
	{command.ErrExceptionIDRequired, badRequest, ""},
	{command.ErrActorRequired, badRequest, ""},
	{command.ErrZeroAdjustmentAmount, badRequest, ""},
	{command.ErrInvalidCurrency, badRequest, ""},
	{value_objects.ErrInvalidCurrencyCode, badRequest, ""},
	{value_objects.ErrInvalidAdjustmentReason, badRequest, ""},
	{entities.ErrResolutionNotesRequired, badRequest, ""},

	// State transition errors -> 422
	{entities.ErrExceptionMustBeOpenOrAssignedToResolve, unprocessable, "exception cannot be resolved in current state"},
	{value_objects.ErrInvalidResolutionTransition, unprocessable, "exception cannot be resolved in current state"},

	// Cross-context lookup failures: the exception references data that
	// cannot be resolved (transaction, ingestion job, or source not found).
	// These indicate a data integrity issue rather than a system error.
	{crossAdapters.ErrTransactionNotFound, notFound, "transaction referenced by exception not found"},
	{crossAdapters.ErrIngestionJobNotFound, unprocessable, "unable to resolve reconciliation context for exception"},
	{crossAdapters.ErrSourceNotFound, unprocessable, "unable to resolve reconciliation context for exception"},
	{crossAdapters.ErrContextNotFound, unprocessable, "unable to resolve reconciliation context for exception"},

	// Infrastructure errors -> 500
	{crossAdapters.ErrContextLookupNotInitialized, internalError, "context lookup service not configured"},
}

// handleExceptionError maps exception use case errors to HTTP responses.
func handleExceptionError(
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

			return mapping.handler(ctx, fiberCtx, span, logger, msg, err)
		}
	}

	return internalError(ctx, fiberCtx, span, logger, "failed to process exception", err)
}

// handleDisputeError maps dispute use case errors to HTTP responses.
func handleDisputeError(
	ctx context.Context,
	fiberCtx *fiber.Ctx,
	span trace.Span,
	logger libLog.Logger,
	err error,
) error {
	if errors.Is(err, sql.ErrNoRows) {
		return notFound(ctx, fiberCtx, span, logger, "dispute not found", err)
	}

	if errors.Is(err, command.ErrDisputeIDRequired) ||
		errors.Is(err, command.ErrDisputeCategoryRequired) ||
		errors.Is(err, dispute.ErrInvalidDisputeCategory) ||
		errors.Is(err, command.ErrDisputeDescriptionRequired) ||
		errors.Is(err, command.ErrDisputeCommentRequired) ||
		errors.Is(err, command.ErrDisputeResolutionRequired) ||
		errors.Is(err, command.ErrActorRequired) {
		return badRequest(ctx, fiberCtx, span, logger, err.Error(), err)
	}

	if errors.Is(err, dispute.ErrCannotAddEvidenceInCurrentState) ||
		errors.Is(err, dispute.ErrInvalidDisputeTransition) {
		return unprocessable(ctx, fiberCtx, span, logger, err.Error(), err)
	}

	return internalError(ctx, fiberCtx, span, logger, "failed to process dispute", err)
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
// @Failure 400 {object} ErrorResponse "Invalid request payload"
// @Failure 401 {object} ErrorResponse "Unauthorized"
// @Failure 403 {object} ErrorResponse "Forbidden"
// @Failure 404 {object} ErrorResponse "Exception not found"
// @Failure 422 {object} ErrorResponse "Unprocessable entity: invalid state transition"
// @Failure 500 {object} ErrorResponse "Internal server error"
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
		return handleExceptionVerificationError(ctx, fiberCtx, span, logger, err)
	}

	libHTTP.SetExceptionSpanAttributes(span, tenantID, exceptionID)

	var req dto.ForceMatchRequest

	if err := libHTTP.ParseBodyAndValidate(fiberCtx, &req); err != nil {
		return badRequest(ctx, fiberCtx, span, logger, "invalid request body", err)
	}

	result, err := handler.exceptionUC.ForceMatch(ctx, command.ForceMatchCommand{
		ExceptionID:    exceptionID,
		OverrideReason: req.OverrideReason,
		Notes:          req.Notes,
	})
	if err != nil {
		return handleExceptionError(ctx, fiberCtx, span, logger, err)
	}

	return libHTTP.Respond(fiberCtx, fiber.StatusOK, dto.ExceptionToResponse(result))
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
// @Failure 400 {object} ErrorResponse "Invalid request payload"
// @Failure 401 {object} ErrorResponse "Unauthorized"
// @Failure 403 {object} ErrorResponse "Forbidden"
// @Failure 404 {object} ErrorResponse "Exception not found"
// @Failure 422 {object} ErrorResponse "Unprocessable entity: invalid state transition"
// @Failure 500 {object} ErrorResponse "Internal server error"
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
		return handleExceptionVerificationError(ctx, fiberCtx, span, logger, err)
	}

	libHTTP.SetExceptionSpanAttributes(span, tenantID, exceptionID)

	var req dto.AdjustEntryRequest

	if err := libHTTP.ParseBodyAndValidate(fiberCtx, &req); err != nil {
		return badRequest(ctx, fiberCtx, span, logger, "invalid request body", err)
	}

	result, err := handler.exceptionUC.AdjustEntry(ctx, command.AdjustEntryCommand{
		ExceptionID: exceptionID,
		ReasonCode:  req.ReasonCode,
		Notes:       req.Notes,
		Amount:      req.Amount,
		Currency:    req.Currency,
		EffectiveAt: req.EffectiveAt,
	})
	if err != nil {
		return handleExceptionError(ctx, fiberCtx, span, logger, err)
	}

	return libHTTP.Respond(fiberCtx, fiber.StatusOK, dto.ExceptionToResponse(result))
}

// OpenDispute opens a new dispute for an exception.
// @Summary Open a dispute
// @Description Opens a new dispute for an exception. Disputes are used to formally challenge or investigate discrepancies with external parties.
// @ID openDispute
// @Tags Exception
// @Accept json
// @Produce json
// @Security BearerAuth
// @Param X-Request-Id header string false "Request ID for tracing"
// @Param X-Idempotency-Key header string false "Idempotency key for safe retries"
// @Param exceptionId path string true "Exception ID" format(uuid)
// @Param request body dto.OpenDisputeRequest true "Open dispute payload"
// @Success 201 {object} dto.DisputeResponse
// @Failure 400 {object} ErrorResponse "Invalid request payload"
// @Failure 401 {object} ErrorResponse "Unauthorized"
// @Failure 403 {object} ErrorResponse "Forbidden"
// @Failure 404 {object} ErrorResponse "Exception not found"
// @Failure 500 {object} ErrorResponse "Internal server error"
// @Router /v1/exceptions/{exceptionId}/disputes [post]
func (handler *Handlers) OpenDispute(fiberCtx *fiber.Ctx) error {
	ctx, span, logger := startHandlerSpan(fiberCtx, "handler.exception.open_dispute")
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
		return handleExceptionVerificationError(ctx, fiberCtx, span, logger, err)
	}

	libHTTP.SetExceptionSpanAttributes(span, tenantID, exceptionID)

	var req dto.OpenDisputeRequest

	if err := libHTTP.ParseBodyAndValidate(fiberCtx, &req); err != nil {
		return badRequest(ctx, fiberCtx, span, logger, "invalid request body", err)
	}

	result, err := handler.disputeUC.OpenDispute(ctx, command.OpenDisputeCommand{
		ExceptionID: exceptionID,
		Category:    req.Category,
		Description: req.Description,
	})
	if err != nil {
		return handleDisputeError(ctx, fiberCtx, span, logger, err)
	}

	return libHTTP.Respond(fiberCtx, fiber.StatusCreated, dto.DisputeToResponse(result))
}

// CloseDispute closes an existing dispute as won or lost.
// @Summary Close a dispute
// @Description Closes a dispute with a resolution. The dispute can be marked as won or lost based on the outcome.
// @ID closeDispute
// @Tags Exception
// @Accept json
// @Produce json
// @Security BearerAuth
// @Param X-Request-Id header string false "Request ID for tracing"
// @Param X-Idempotency-Key header string false "Idempotency key for safe retries"
// @Param disputeId path string true "Dispute ID" format(uuid)
// @Param request body dto.CloseDisputeRequest true "Close dispute payload"
// @Success 200 {object} dto.DisputeResponse
// @Failure 400 {object} ErrorResponse "Invalid request payload"
// @Failure 401 {object} ErrorResponse "Unauthorized"
// @Failure 403 {object} ErrorResponse "Forbidden"
// @Failure 404 {object} ErrorResponse "Dispute not found"
// @Failure 422 {object} ErrorResponse "Unprocessable entity: invalid state transition"
// @Failure 500 {object} ErrorResponse "Internal server error"
// @Router /v1/disputes/{disputeId}/close [post]
func (handler *Handlers) CloseDispute(fiberCtx *fiber.Ctx) error {
	ctx, span, logger := startHandlerSpan(fiberCtx, "handler.exception.close_dispute")
	defer span.End()

	disputeID, tenantID, err := libHTTP.ParseAndVerifyResourceScopedID(
		fiberCtx,
		"disputeId",
		libHTTP.IDLocationParam,
		handler.disputeVerifier,
		auth.GetTenantID,
		ErrMissingDisputeID,
		ErrInvalidDisputeID,
		ErrDisputeAccessDenied,
		"dispute",
	)
	if err != nil {
		return handleDisputeVerificationError(ctx, fiberCtx, span, logger, err)
	}

	libHTTP.SetDisputeSpanAttributes(span, tenantID, disputeID)

	var req dto.CloseDisputeRequest

	if err := libHTTP.ParseBodyAndValidate(fiberCtx, &req); err != nil {
		return badRequest(ctx, fiberCtx, span, logger, "invalid request body", err)
	}

	result, err := handler.disputeUC.CloseDispute(ctx, command.CloseDisputeCommand{
		DisputeID:  disputeID,
		Resolution: req.Resolution,
		Won:        req.Won,
	})
	if err != nil {
		return handleDisputeError(ctx, fiberCtx, span, logger, err)
	}

	return libHTTP.Respond(fiberCtx, fiber.StatusOK, dto.DisputeToResponse(result))
}

// SubmitEvidence adds evidence to an existing dispute.
// @Summary Submit evidence to a dispute
// @Description Adds evidence to a dispute. Evidence can include comments and optional file attachments to support the dispute case.
// @ID submitEvidence
// @Tags Exception
// @Accept json
// @Produce json
// @Security BearerAuth
// @Param X-Request-Id header string false "Request ID for tracing"
// @Param X-Idempotency-Key header string false "Idempotency key for safe retries"
// @Param disputeId path string true "Dispute ID" format(uuid)
// @Param request body dto.SubmitEvidenceRequest true "Submit evidence payload"
// @Success 200 {object} dto.DisputeResponse
// @Failure 400 {object} ErrorResponse "Invalid request payload"
// @Failure 401 {object} ErrorResponse "Unauthorized"
// @Failure 403 {object} ErrorResponse "Forbidden"
// @Failure 404 {object} ErrorResponse "Dispute not found"
// @Failure 422 {object} ErrorResponse "Unprocessable entity: invalid state transition"
// @Failure 500 {object} ErrorResponse "Internal server error"
// @Router /v1/disputes/{disputeId}/evidence [post]
func (handler *Handlers) SubmitEvidence(fiberCtx *fiber.Ctx) error {
	ctx, span, logger := startHandlerSpan(fiberCtx, "handler.exception.submit_evidence")
	defer span.End()

	disputeID, tenantID, err := libHTTP.ParseAndVerifyResourceScopedID(
		fiberCtx,
		"disputeId",
		libHTTP.IDLocationParam,
		handler.disputeVerifier,
		auth.GetTenantID,
		ErrMissingDisputeID,
		ErrInvalidDisputeID,
		ErrDisputeAccessDenied,
		"dispute",
	)
	if err != nil {
		return handleDisputeVerificationError(ctx, fiberCtx, span, logger, err)
	}

	libHTTP.SetDisputeSpanAttributes(span, tenantID, disputeID)

	var req dto.SubmitEvidenceRequest

	if err := libHTTP.ParseBodyAndValidate(fiberCtx, &req); err != nil {
		return badRequest(ctx, fiberCtx, span, logger, "invalid request body", err)
	}

	result, err := handler.disputeUC.SubmitEvidence(ctx, command.SubmitEvidenceCommand{
		DisputeID: disputeID,
		Comment:   req.Comment,
		FileURL:   req.FileURL,
	})
	if err != nil {
		return handleDisputeError(ctx, fiberCtx, span, logger, err)
	}

	return libHTTP.Respond(fiberCtx, fiber.StatusOK, dto.DisputeToResponse(result))
}

// ListExceptions lists exceptions with optional filters and pagination.
// @Summary List exceptions
// @Description Lists all exceptions with optional filters for status, severity, assigned user, external system, and date range. Supports cursor-based pagination.
// @ID listExceptions
// @Tags Exception
// @Produce json
// @Security BearerAuth
// @Param X-Request-Id header string false "Request ID for tracing"
// @Param status query string false "Filter by status" Enums(OPEN,ASSIGNED,RESOLVED)
// @Param severity query string false "Filter by severity" Enums(LOW,MEDIUM,HIGH,CRITICAL)
// @Param assigned_to query string false "Filter by assigned user"
// @Param external_system query string false "Filter by external system" Enums(JIRA,SERVICENOW,WEBHOOK)
// @Param date_from query string false "Filter from date (RFC3339)"
// @Param date_to query string false "Filter to date (RFC3339)"
// @Param cursor query string false "Cursor for pagination (opaque)"
// @Param limit query int false "Maximum number of records to return" default(20) minimum(1) maximum(200)
// @Param sort_by query string false "Sort by field" Enums(id,created_at,updated_at,severity,status) default(id)
// @Param sort_order query string false "Sort order" Enums(asc,desc) default(desc)
// @Success 200 {object} dto.ListExceptionsResponse
// @Failure 400 {object} ErrorResponse "Invalid request payload"
// @Failure 401 {object} ErrorResponse "Unauthorized"
// @Failure 403 {object} ErrorResponse "Forbidden"
// @Failure 500 {object} ErrorResponse "Internal server error"
// @Router /v1/exceptions [get]
func (handler *Handlers) ListExceptions(fiberCtx *fiber.Ctx) error {
	ctx, span, logger := startHandlerSpan(fiberCtx, "handler.exception.list")
	defer span.End()

	filter, cursorFilter, err := parseListFilters(fiberCtx)
	if err != nil {
		return badRequest(ctx, fiberCtx, span, logger, "invalid filter parameters", err)
	}

	exceptions, pagination, err := handler.queryUC.ListExceptions(ctx, query.ListQuery{
		Filter: filter,
		Cursor: cursorFilter,
	})
	if err != nil {
		if errors.Is(err, libHTTP.ErrInvalidCursor) {
			return badRequest(ctx, fiberCtx, span, logger, "invalid pagination parameters", err)
		}

		return internalError(ctx, fiberCtx, span, logger, "failed to list exceptions", err)
	}

	items := dto.ExceptionsToResponse(exceptions)

	response := dto.ListExceptionsResponse{
		Items: items,
		CursorResponse: dto.CursorResponse{
			NextCursor: pagination.Next,
			PrevCursor: pagination.Prev,
			Limit:      cursorFilter.Limit,
			HasMore:    pagination.Next != "",
		},
	}

	return libHTTP.Respond(fiberCtx, fiber.StatusOK, response)
}

// ListDisputes lists disputes with optional filters and pagination.
// @Summary List disputes
// @Description Lists all disputes with optional filters for state, category, and date range. Supports cursor-based pagination.
// @ID listDisputes
// @Tags Exception
// @Produce json
// @Security BearerAuth
// @Param X-Request-Id header string false "Request ID for tracing"
// @Param state query string false "Filter by state" Enums(DRAFT,OPEN,PENDING_EVIDENCE,WON,LOST)
// @Param category query string false "Filter by category" Enums(BANK_FEE_ERROR,UNRECOGNIZED_CHARGE,DUPLICATE_TRANSACTION,OTHER)
// @Param date_from query string false "Filter from date (RFC3339)"
// @Param date_to query string false "Filter to date (RFC3339)"
// @Param cursor query string false "Cursor for pagination (opaque)"
// @Param limit query int false "Maximum number of records to return" default(20) minimum(1) maximum(200)
// @Param sort_by query string false "Sort by field" Enums(id,created_at,updated_at,state,category) default(id)
// @Param sort_order query string false "Sort order" Enums(asc,desc) default(desc)
// @Success 200 {object} dto.ListDisputesResponse
// @Failure 400 {object} ErrorResponse "Invalid request payload"
// @Failure 401 {object} ErrorResponse "Unauthorized"
// @Failure 403 {object} ErrorResponse "Forbidden"
// @Failure 500 {object} ErrorResponse "Internal server error"
// @Router /v1/disputes [get]
func (handler *Handlers) ListDisputes(fiberCtx *fiber.Ctx) error {
	ctx, span, logger := startHandlerSpan(fiberCtx, "handler.dispute.list")
	defer span.End()

	filter, cursorFilter, err := parseDisputeListFilters(fiberCtx)
	if err != nil {
		return badRequest(ctx, fiberCtx, span, logger, "invalid filter parameters", err)
	}

	disputes, pagination, err := handler.queryUC.ListDisputes(ctx, query.DisputeListQuery{
		Filter: filter,
		Cursor: cursorFilter,
	})
	if err != nil {
		if errors.Is(err, libHTTP.ErrInvalidCursor) {
			return badRequest(ctx, fiberCtx, span, logger, "invalid pagination parameters", err)
		}

		return internalError(ctx, fiberCtx, span, logger, "failed to list disputes", err)
	}

	items := dto.DisputesToResponse(disputes)

	response := dto.ListDisputesResponse{
		Items: items,
		CursorResponse: dto.CursorResponse{
			NextCursor: pagination.Next,
			PrevCursor: pagination.Prev,
			Limit:      cursorFilter.Limit,
			HasMore:    pagination.Next != "",
		},
	}

	return libHTTP.Respond(fiberCtx, fiber.StatusOK, response)
}

// GetDispute retrieves a single dispute by ID.
// @Summary Get dispute
// @Description Retrieves a single dispute by its ID.
// @ID getDispute
// @Tags Exception
// @Produce json
// @Security BearerAuth
// @Param X-Request-Id header string false "Request ID for tracing"
// @Param disputeId path string true "Dispute ID" format(uuid)
// @Success 200 {object} dto.DisputeResponse
// @Failure 400 {object} ErrorResponse "Invalid request payload"
// @Failure 401 {object} ErrorResponse "Unauthorized"
// @Failure 403 {object} ErrorResponse "Forbidden"
// @Failure 404 {object} ErrorResponse "Dispute not found"
// @Failure 500 {object} ErrorResponse "Internal server error"
// @Router /v1/disputes/{disputeId} [get]
func (handler *Handlers) GetDispute(fiberCtx *fiber.Ctx) error {
	ctx, span, logger := startHandlerSpan(fiberCtx, "handler.dispute.get")
	defer span.End()

	disputeID, tenantID, err := libHTTP.ParseAndVerifyResourceScopedID(
		fiberCtx,
		"disputeId",
		libHTTP.IDLocationParam,
		handler.disputeVerifier,
		auth.GetTenantID,
		ErrMissingDisputeID,
		ErrInvalidDisputeID,
		ErrDisputeAccessDenied,
		"dispute",
	)
	if err != nil {
		return handleDisputeVerificationError(ctx, fiberCtx, span, logger, err)
	}

	libHTTP.SetDisputeSpanAttributes(span, tenantID, disputeID)

	result, err := handler.queryUC.GetDispute(ctx, disputeID)
	if err != nil {
		if errors.Is(err, query.ErrDisputeNotFound) {
			return notFound(ctx, fiberCtx, span, logger, "dispute not found", err)
		}

		return internalError(ctx, fiberCtx, span, logger, "failed to get dispute", err)
	}

	return libHTTP.Respond(fiberCtx, fiber.StatusOK, dto.DisputeToResponse(result))
}

func parseDisputeListFilters(
	fiberCtx *fiber.Ctx,
) (repositories.DisputeFilter, repositories.CursorFilter, error) {
	filter, err := parseDisputeFilter(fiberCtx)
	if err != nil {
		return repositories.DisputeFilter{}, repositories.CursorFilter{}, err
	}

	cursorFilter, err := parseCursorFilter(fiberCtx)
	if err != nil {
		return repositories.DisputeFilter{}, repositories.CursorFilter{}, err
	}

	return filter, cursorFilter, nil
}

func parseDisputeFilter(fiberCtx *fiber.Ctx) (repositories.DisputeFilter, error) {
	var filter repositories.DisputeFilter

	if state := fiberCtx.Query("state"); state != "" {
		parsed, err := dispute.ParseDisputeState(state)
		if err != nil {
			return filter, fmt.Errorf("invalid state: %w", err)
		}

		filter.State = &parsed
	}

	if category := fiberCtx.Query("category"); category != "" {
		parsed, err := dispute.ParseDisputeCategory(category)
		if err != nil {
			return filter, fmt.Errorf("invalid category: %w", err)
		}

		filter.Category = &parsed
	}

	if dateFrom := fiberCtx.Query("date_from"); dateFrom != "" {
		parsed, err := time.Parse(time.RFC3339, dateFrom)
		if err != nil {
			return filter, fmt.Errorf("invalid date_from: %w", err)
		}

		filter.DateFrom = &parsed
	}

	if dateTo := fiberCtx.Query("date_to"); dateTo != "" {
		parsed, err := time.Parse(time.RFC3339, dateTo)
		if err != nil {
			return filter, fmt.Errorf("invalid date_to: %w", err)
		}

		filter.DateTo = &parsed
	}

	return filter, nil
}

func parseListFilters(
	fiberCtx *fiber.Ctx,
) (repositories.ExceptionFilter, repositories.CursorFilter, error) {
	filter, err := parseExceptionFilter(fiberCtx)
	if err != nil {
		return repositories.ExceptionFilter{}, repositories.CursorFilter{}, err
	}

	cursorFilter, err := parseCursorFilter(fiberCtx)
	if err != nil {
		return repositories.ExceptionFilter{}, repositories.CursorFilter{}, err
	}

	return filter, cursorFilter, nil
}

func parseExceptionFilter(fiberCtx *fiber.Ctx) (repositories.ExceptionFilter, error) {
	var filter repositories.ExceptionFilter

	if status := fiberCtx.Query("status"); status != "" {
		parsed, err := value_objects.ParseExceptionStatus(status)
		if err != nil {
			return filter, fmt.Errorf("invalid status: %w", err)
		}

		filter.Status = &parsed
	}

	if severity := fiberCtx.Query("severity"); severity != "" {
		parsed, err := value_objects.ParseExceptionSeverity(severity)
		if err != nil {
			return filter, fmt.Errorf("invalid severity: %w", err)
		}

		filter.Severity = &parsed
	}

	if assignedTo := fiberCtx.Query("assigned_to"); assignedTo != "" {
		if err := libHTTP.ValidateQueryParamLength(assignedTo, "assigned_to", libHTTP.MaxQueryParamLengthLong); err != nil {
			return filter, fmt.Errorf("invalid assigned_to: %w", err)
		}

		filter.AssignedTo = &assignedTo
	}

	if externalSystem := fiberCtx.Query("external_system"); externalSystem != "" {
		if err := libHTTP.ValidateQueryParamLength(externalSystem, "external_system", libHTTP.MaxQueryParamLengthShort); err != nil {
			return filter, fmt.Errorf("invalid external_system: %w", err)
		}

		filter.ExternalSystem = &externalSystem
	}

	if dateFrom := fiberCtx.Query("date_from"); dateFrom != "" {
		parsed, err := time.Parse(time.RFC3339, dateFrom)
		if err != nil {
			return filter, fmt.Errorf("invalid date_from: %w", err)
		}

		filter.DateFrom = &parsed
	}

	if dateTo := fiberCtx.Query("date_to"); dateTo != "" {
		parsed, err := time.Parse(time.RFC3339, dateTo)
		if err != nil {
			return filter, fmt.Errorf("invalid date_to: %w", err)
		}

		filter.DateTo = &parsed
	}

	return filter, nil
}

// allowedSortColumns defines the whitelist of columns allowed for sorting.
var allowedSortColumns = []string{"id", "created_at", "updated_at", "severity", "status"}

// allowedSortOrders defines the whitelist of sort orders.
var allowedSortOrders = map[string]bool{
	"asc":  true,
	"desc": true,
}

func parseCursorFilter(fiberCtx *fiber.Ctx) (repositories.CursorFilter, error) {
	cursor, limit, err := libHTTP.ParseOpaqueCursorPagination(fiberCtx)
	if err != nil {
		return repositories.CursorFilter{}, fmt.Errorf("parse pagination: %w", err)
	}

	sortBy := libHTTP.ValidateSortColumn(fiberCtx.Query("sort_by"), allowedSortColumns, "id")

	sortOrder := fiberCtx.Query("sort_order")
	if sortOrder != "" && !allowedSortOrders[sortOrder] {
		return repositories.CursorFilter{}, ErrInvalidSortOrder
	}

	return repositories.CursorFilter{
		Limit:     limit,
		Cursor:    cursor,
		SortBy:    sortBy,
		SortOrder: sortOrder,
	}, nil
}

// GetException retrieves a single exception by ID.
// @Summary Get exception
// @Description Retrieves a single exception by its ID.
// @ID getException
// @Tags Exception
// @Produce json
// @Security BearerAuth
// @Param X-Request-Id header string false "Request ID for tracing"
// @Param exceptionId path string true "Exception ID" format(uuid)
// @Success 200 {object} dto.ExceptionResponse
// @Failure 400 {object} ErrorResponse "Invalid request payload"
// @Failure 401 {object} ErrorResponse "Unauthorized"
// @Failure 403 {object} ErrorResponse "Forbidden"
// @Failure 404 {object} ErrorResponse "Exception not found"
// @Failure 500 {object} ErrorResponse "Internal server error"
// @Router /v1/exceptions/{exceptionId} [get]
func (handler *Handlers) GetException(fiberCtx *fiber.Ctx) error {
	ctx, span, logger := startHandlerSpan(fiberCtx, "handler.exception.get")
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
		return handleExceptionVerificationError(ctx, fiberCtx, span, logger, err)
	}

	libHTTP.SetExceptionSpanAttributes(span, tenantID, exceptionID)

	exception, err := handler.queryUC.GetException(ctx, exceptionID)
	if err != nil {
		if errors.Is(err, entities.ErrExceptionNotFound) {
			return notFound(ctx, fiberCtx, span, logger, "exception not found", err)
		}

		return internalError(ctx, fiberCtx, span, logger, "failed to get exception", err)
	}

	return libHTTP.Respond(fiberCtx, fiber.StatusOK, dto.ExceptionToResponse(exception))
}

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
// @Failure 400 {object} ErrorResponse "Invalid request payload"
// @Failure 401 {object} ErrorResponse "Unauthorized"
// @Failure 403 {object} ErrorResponse "Forbidden"
// @Failure 404 {object} ErrorResponse "Exception not found"
// @Failure 422 {object} ErrorResponse "Unprocessable entity: invalid state transition"
// @Failure 500 {object} ErrorResponse "Internal server error"
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
		return handleExceptionVerificationError(ctx, fiberCtx, span, logger, err)
	}

	libHTTP.SetExceptionSpanAttributes(span, tenantID, exceptionID)

	var req dto.DispatchRequest

	if err := libHTTP.ParseBodyAndValidate(fiberCtx, &req); err != nil {
		return badRequest(ctx, fiberCtx, span, logger, "invalid request body", err)
	}

	result, err := handler.dispatchUC.Dispatch(ctx, command.DispatchCommand{
		ExceptionID:  exceptionID,
		TargetSystem: req.TargetSystem,
		Queue:        req.Queue,
	})
	if err != nil {
		return handleDispatchError(ctx, fiberCtx, span, logger, err)
	}

	return libHTTP.Respond(fiberCtx, fiber.StatusOK, dto.DispatchResponse{
		ExceptionID:       result.ExceptionID.String(),
		Target:            result.Target,
		ExternalReference: result.ExternalReference,
		Acknowledged:      result.Acknowledged,
		DispatchedAt:      result.DispatchedAt.Format(time.RFC3339),
	})
}

// handleDispatchError maps dispatch use case errors to HTTP responses.
func handleDispatchError(
	ctx context.Context,
	fiberCtx *fiber.Ctx,
	span trace.Span,
	logger libLog.Logger,
	err error,
) error {
	if errors.Is(err, sql.ErrNoRows) {
		return notFound(ctx, fiberCtx, span, logger, "exception not found", err)
	}

	if errors.Is(err, command.ErrExceptionIDRequired) ||
		errors.Is(err, command.ErrTargetSystemRequired) ||
		errors.Is(err, command.ErrActorRequired) {
		return badRequest(ctx, fiberCtx, span, logger, err.Error(), err)
	}

	if errors.Is(err, command.ErrUnsupportedTargetSystem) {
		return unprocessable(ctx, fiberCtx, span, logger, err.Error(), err)
	}

	return internalError(ctx, fiberCtx, span, logger, "failed to dispatch exception", err)
}

// GetHistory retrieves the audit history for an exception.
// @Summary Get exception history
// @Description Retrieves the audit history for an exception, showing all actions taken on it. Pagination is forward-only (no prevCursor); use the nextCursor value to fetch subsequent pages.
// @ID getExceptionHistory
// @Tags Exception
// @Produce json
// @Security BearerAuth
// @Param X-Request-Id header string false "Request ID for tracing"
// @Param exceptionId path string true "Exception ID" format(uuid)
// @Param cursor query string false "Cursor for pagination (opaque)"
// @Param limit query int false "Maximum number of records to return" default(20) minimum(1) maximum(200)
// @Success 200 {object} dto.HistoryResponse
// @Failure 400 {object} ErrorResponse "Invalid request payload"
// @Failure 401 {object} ErrorResponse "Unauthorized"
// @Failure 403 {object} ErrorResponse "Forbidden"
// @Failure 404 {object} ErrorResponse "Exception not found"
// @Failure 500 {object} ErrorResponse "Internal server error"
// @Router /v1/exceptions/{exceptionId}/history [get]
func (handler *Handlers) GetHistory(fiberCtx *fiber.Ctx) error {
	ctx, span, logger := startHandlerSpan(fiberCtx, "handler.exception.get_history")
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
		return handleExceptionVerificationError(ctx, fiberCtx, span, logger, err)
	}

	libHTTP.SetExceptionSpanAttributes(span, tenantID, exceptionID)

	cursorParam := fiberCtx.Query("cursor")

	// cursorPtr validates/parses the timestamp cursor; the raw cursorParam is forwarded as-is.
	cursorPtr, limit, err := parseTimestampCursorPagination(fiberCtx)
	if err != nil {
		return badRequest(ctx, fiberCtx, span, logger, "invalid pagination parameters", err)
	}

	cursor := ""
	if cursorPtr != nil {
		cursor = cursorParam
	}

	entries, nextCursor, err := handler.queryUC.GetHistory(ctx, exceptionID, cursor, limit)
	if err != nil {
		if errors.Is(err, query.ErrTenantIDRequired) {
			return badRequest(ctx, fiberCtx, span, logger, "tenant context required", err)
		}

		if errors.Is(err, libHTTP.ErrInvalidCursor) {
			return badRequest(ctx, fiberCtx, span, logger, "invalid pagination parameters", err)
		}

		return internalError(ctx, fiberCtx, span, logger, "failed to get history", err)
	}

	items := make([]dto.HistoryEntryResponse, len(entries))

	for i, entry := range entries {
		var changes any

		if len(entry.Changes) > 0 {
			if err := json.Unmarshal(entry.Changes, &changes); err != nil {
				logger.Log(ctx, libLog.LevelWarn, fmt.Sprintf(
					"failed to unmarshal history entry changes for entry %s: %v",
					entry.ID.String(),
					err,
				))
			}
		}

		items[i] = dto.HistoryEntryResponse{
			ID:        entry.ID.String(),
			Action:    entry.Action,
			ActorID:   entry.ActorID,
			Changes:   changes,
			CreatedAt: entry.CreatedAt,
		}
	}

	return libHTTP.Respond(fiberCtx, fiber.StatusOK, dto.HistoryResponse{
		Items: items,
		CursorResponse: dto.CursorResponse{
			NextCursor: nextCursor,
			Limit:      limit,
			HasMore:    nextCursor != "",
		},
	})
}

func parseTimestampCursorPagination(fiberCtx *fiber.Ctx) (*libHTTP.TimestampCursor, int, error) {
	cursor, limit, err := libHTTP.ParseTimestampCursorPagination(fiberCtx)
	if err != nil {
		return nil, 0, fmt.Errorf("parse timestamp cursor pagination: %w", err)
	}

	limit = libHTTP.ValidateLimit(limit, constants.DefaultPaginationLimit, constants.MaximumPaginationLimit)

	return cursor, limit, nil
}
