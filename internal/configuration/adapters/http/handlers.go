// Package http provides HTTP handlers for configuration management.
package http

import (
	"context"
	"database/sql"
	"errors"
	"sync/atomic"

	"github.com/gofiber/fiber/v2"
	"github.com/google/uuid"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/trace"

	libCommons "github.com/LerianStudio/lib-commons/v4/commons"
	libLog "github.com/LerianStudio/lib-commons/v4/commons/log"
	libHTTP "github.com/LerianStudio/lib-commons/v4/commons/net/http"
	libOpentelemetry "github.com/LerianStudio/lib-commons/v4/commons/opentelemetry"

	"github.com/LerianStudio/matcher/internal/configuration/domain/entities"
	"github.com/LerianStudio/matcher/internal/configuration/services/command"
	"github.com/LerianStudio/matcher/internal/configuration/services/query"
)

// Configuration handler errors.
var (
	// ErrNilCommandUseCase is returned when the command use case is nil.
	ErrNilCommandUseCase = errors.New("command use case is required")
	// ErrNilQueryUseCase is returned when the query use case is nil.
	ErrNilQueryUseCase = errors.New("query use case is required")
	// ErrRuleIDsRequired is returned when rule IDs are not provided.
	ErrRuleIDsRequired = errors.New("rule IDs are required")
)

// productionMode indicates whether the application is running in production.
// Set once during handler construction via NewHandler; governs SafeError behavior
// (suppresses internal error details in client responses when true).
// Uses atomic.Bool because parallel tests construct handlers concurrently.
var productionMode atomic.Bool

// Handler handles HTTP requests for configuration operations.
type Handler struct {
	command         *command.UseCase
	query           *query.UseCase
	contextVerifier libHTTP.TenantOwnershipVerifier
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

	return libHTTP.RespondError(fiberCtx, fiber.StatusBadRequest, "invalid_request", safeClientMessage(message, err))
}

func unauthorized(ctx context.Context, c *fiber.Ctx, span trace.Span, logger libLog.Logger, err error) error {
	logSpanError(ctx, span, logger, "invalid tenant id", err)
	return libHTTP.RespondError(c, fiber.StatusUnauthorized, "unauthorized", "unauthorized")
}

func writeNotFound(c *fiber.Ctx, message string) error {
	return libHTTP.RespondError(c, fiber.StatusNotFound, "not_found", message)
}

func safeClientMessage(defaultMsg string, err error) string {
	if err == nil {
		return defaultMsg
	}

	if isClientSafeError(err) {
		return err.Error()
	}

	return defaultMsg
}

func (handler *Handler) ensureSourceAccess(
	ctx context.Context,
	fiberCtx *fiber.Ctx,
	span trace.Span,
	logger libLog.Logger,
	contextID, sourceID uuid.UUID,
) error {
	_, err := handler.query.GetSource(ctx, contextID, sourceID)
	if err != nil {
		logSpanError(ctx, span, logger, "failed to load source", err)

		if errors.Is(err, sql.ErrNoRows) {
			return writeNotFound(fiberCtx, "source not found")
		}

		return writeServiceError(fiberCtx, err)
	}

	return nil
}

func handleContextVerificationError(
	ctx context.Context,
	fiberCtx *fiber.Ctx,
	span trace.Span,
	logger libLog.Logger,
	err error,
) error {
	logSpanError(ctx, span, logger, "context verification failed", err)

	if errors.Is(err, libHTTP.ErrMissingContextID) ||
		errors.Is(err, libHTTP.ErrInvalidContextID) {
		return libHTTP.RespondError(fiberCtx, fiber.StatusBadRequest, "invalid_request", "invalid context id")
	}

	if errors.Is(err, libHTTP.ErrTenantIDNotFound) ||
		errors.Is(err, libHTTP.ErrInvalidTenantID) {
		return libHTTP.RespondError(fiberCtx, fiber.StatusUnauthorized, "unauthorized", "unauthorized")
	}

	if errors.Is(err, libHTTP.ErrContextNotFound) {
		return writeNotFound(fiberCtx, "context not found")
	}

	if errors.Is(err, libHTTP.ErrContextNotActive) {
		return libHTTP.RespondError(fiberCtx, fiber.StatusForbidden, "context_not_active", "context is not active")
	}

	if errors.Is(err, libHTTP.ErrContextNotOwned) ||
		errors.Is(err, libHTTP.ErrContextAccessDenied) {
		return libHTTP.RespondError(fiberCtx, fiber.StatusForbidden, "forbidden", "access denied")
	}

	return libHTTP.RespondError(fiberCtx, fiber.StatusInternalServerError, "internal_server_error", "an unexpected error occurred")
}

func handleOwnershipVerificationError(
	ctx context.Context,
	fiberCtx *fiber.Ctx,
	span trace.Span,
	logger libLog.Logger,
	err error,
) error {
	logSpanError(ctx, span, logger, "context ownership verification failed", err)

	if errors.Is(err, libHTTP.ErrContextNotFound) ||
		errors.Is(err, libHTTP.ErrContextNotOwned) {
		return writeNotFound(fiberCtx, "resource not found")
	}

	return writeServiceError(fiberCtx, err)
}

// NewHandler creates a new configuration handler.
func NewHandler(commandUseCase *command.UseCase, queryUseCase *query.UseCase, production bool) (*Handler, error) {
	if commandUseCase == nil {
		return nil, ErrNilCommandUseCase
	}

	if queryUseCase == nil {
		return nil, ErrNilQueryUseCase
	}

	productionMode.Store(production)

	return &Handler{
		command:         commandUseCase,
		query:           queryUseCase,
		contextVerifier: NewTenantOwnershipVerifier(queryUseCase),
	}, nil
}

// boolDefault returns the value of b if non-nil, or the default value otherwise.
func boolDefault(b *bool, defaultVal bool) bool {
	if b == nil {
		return defaultVal
	}

	return *b
}

func mapUpdateContextError(fiberCtx *fiber.Ctx, err error) error {
	switch {
	case errors.Is(err, sql.ErrNoRows):
		return writeNotFound(fiberCtx, "context not found")
	case errors.Is(err, command.ErrContextNameAlreadyExists):
		return libHTTP.RespondError(fiberCtx, fiber.StatusConflict, "duplicate_name", err.Error())
	case errors.Is(err, entities.ErrInvalidStateTransition):
		return libHTTP.RespondError(fiberCtx, fiber.StatusConflict, "invalid_state_transition", err.Error())
	case errors.Is(err, entities.ErrArchivedContextCannotBeModified):
		return libHTTP.RespondError(fiberCtx, fiber.StatusConflict, "archived_context", err.Error())
	default:
		return writeServiceError(fiberCtx, err)
	}
}

// ErrorResponse is a placeholder for Swagger documentation.
// The actual error response type is defined in lib-commons.
type ErrorResponse struct {
	Code    int    `json:"code"`
	Title   string `json:"title"`
	Message string `json:"message"`
}

func writeServiceError(fiberCtx *fiber.Ctx, err error) error {
	message := clientErrorMessage(err)
	if isClientSafeError(err) {
		return libHTTP.RespondError(fiberCtx, fiber.StatusBadRequest, "invalid_request", message)
	}

	return libHTTP.RespondError(fiberCtx, fiber.StatusInternalServerError, "internal_server_error", "an unexpected error occurred")
}

func clientErrorMessage(err error) string {
	return safeClientMessage("request_failed", err)
}

func isClientSafeError(err error) bool {
	safeErrors := []error{
		entities.ErrNilReconciliationContext,
		entities.ErrContextNameRequired,
		entities.ErrContextNameTooLong,
		entities.ErrContextTypeInvalid,
		entities.ErrContextStatusInvalid,
		entities.ErrContextIntervalRequired,
		entities.ErrContextTenantRequired,
		entities.ErrSourceNameRequired,
		entities.ErrSourceNameTooLong,
		entities.ErrSourceTypeInvalid,
		entities.ErrSourceContextRequired,
		entities.ErrSourceSideRequired,
		entities.ErrSourceSideInvalid,
		entities.ErrFieldMapNil,
		entities.ErrFieldMapContextRequired,
		entities.ErrFieldMapSourceRequired,
		entities.ErrFieldMapMappingRequired,
		entities.ErrFieldMapMappingValueEmpty,
		entities.ErrMatchRuleNil,
		entities.ErrRuleContextRequired,
		entities.ErrRulePriorityInvalid,
		entities.ErrRuleTypeInvalid,
		entities.ErrRuleConfigRequired,
		entities.ErrRuleConfigMissingRequiredKeys,
		entities.ErrRulePriorityConflict,
	}
	for _, safeErr := range safeErrors {
		if errors.Is(err, safeErr) {
			return true
		}
	}

	return false
}
