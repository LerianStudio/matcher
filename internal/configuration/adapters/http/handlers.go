// Package http provides HTTP handlers for configuration management.
package http

import (
	"context"
	"database/sql"
	"errors"
	"strings"
	"sync/atomic"

	"github.com/gofiber/fiber/v2"
	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgconn"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/trace"

	libCommons "github.com/LerianStudio/lib-commons/v4/commons"
	libLog "github.com/LerianStudio/lib-commons/v4/commons/log"
	libHTTP "github.com/LerianStudio/lib-commons/v4/commons/net/http"
	libOpentelemetry "github.com/LerianStudio/lib-commons/v4/commons/opentelemetry"

	"github.com/LerianStudio/matcher/internal/auth"
	"github.com/LerianStudio/matcher/internal/configuration/adapters/http/dto"
	"github.com/LerianStudio/matcher/internal/configuration/domain/entities"
	"github.com/LerianStudio/matcher/internal/configuration/domain/value_objects"
	"github.com/LerianStudio/matcher/internal/configuration/services/command"
	"github.com/LerianStudio/matcher/internal/configuration/services/query"
	sharedpagination "github.com/LerianStudio/matcher/internal/shared/adapters/http"
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

// CreateContext creates a reconciliation context.
//
// @ID createContext
// @Summary Create a reconciliation context
// @Description Creates a new reconciliation context used to scope matching rules.
// @Tags Configuration Contexts
// @Accept json
// @Produce json
// @Security BearerAuth
// @Param X-Request-Id header string false "Request ID for tracing"
// @Param context body dto.CreateContextRequest true "Context creation payload"
// @Success 201 {object} dto.ReconciliationContextResponse "Successfully created context"
// @Failure 400 {object} ErrorResponse "Invalid request payload"
// @Failure 401 {object} ErrorResponse "Unauthorized"
// @Failure 403 {object} ErrorResponse "Forbidden"
// @Failure 409 {object} ErrorResponse "Conflict: duplicate resource or idempotency key in progress"
// @Failure 500 {object} ErrorResponse "Internal server error"
// @Router /v1/contexts [post]
func (handler *Handler) CreateContext(fiberCtx *fiber.Ctx) error {
	ctx, span, logger := startHandlerSpan(fiberCtx, "handler.context.create")
	defer span.End()

	var req dto.CreateContextRequest
	if err := libHTTP.ParseBodyAndValidate(fiberCtx, &req); err != nil {
		return badRequest(ctx, fiberCtx, span, logger, "invalid context payload", err)
	}

	tenantID, err := tenantIDFromContext(ctx)
	if err != nil {
		return unauthorized(ctx, fiberCtx, span, logger, err)
	}

	libHTTP.SetTenantSpanAttribute(span, tenantID)

	domainInput, err := req.ToDomainInput()
	if err != nil {
		return badRequest(ctx, fiberCtx, span, logger, "invalid context payload", err)
	}

	result, err := handler.command.CreateContext(ctx, tenantID, domainInput)
	if err != nil {
		logSpanError(ctx, span, logger, "failed to create context", err)

		if errors.Is(err, command.ErrContextNameAlreadyExists) {
			return libHTTP.RespondError(fiberCtx, fiber.StatusConflict, "duplicate_name", err.Error())
		}

		if errors.Is(err, entities.ErrRulePriorityConflict) {
			return libHTTP.RespondError(fiberCtx, fiber.StatusConflict, "priority_conflict", err.Error())
		}

		return writeServiceError(fiberCtx, err)
	}

	return libHTTP.Respond(fiberCtx, fiber.StatusCreated, dto.ReconciliationContextToResponse(result))
}

// ListContexts lists reconciliation contexts.
//
// @ID listContexts
// @Summary List reconciliation contexts
// @Description Returns a cursor-paginated list of reconciliation contexts, optionally filtered by type and status.
// @Tags Configuration Contexts
// @Produce json
// @Security BearerAuth
// @Param X-Request-Id header string false "Request ID for tracing"
// @Param limit query int false "Maximum number of records to return" default(20) minimum(1) maximum(200)
// @Param cursor query string false "Cursor for pagination (opaque)"
// @Param type query string false "Filter by context type" Enums(1:1,1:N,N:M)
// @Param status query string false "Filter by context status" Enums(DRAFT,ACTIVE,PAUSED,ARCHIVED)
// @Success 200 {object} ListContextsResponse "List of contexts with cursor pagination"
// @Failure 400 {object} ErrorResponse "Invalid query parameters"
// @Failure 401 {object} ErrorResponse "Unauthorized"
// @Failure 403 {object} ErrorResponse "Forbidden"
// @Failure 500 {object} ErrorResponse "Internal server error"
// @Router /v1/contexts [get]
func (handler *Handler) ListContexts(fiberCtx *fiber.Ctx) error {
	ctx, span, logger := startHandlerSpan(fiberCtx, "handler.context.list")
	defer span.End()

	tenantID, err := tenantIDFromContext(ctx)
	if err != nil {
		return unauthorized(ctx, fiberCtx, span, logger, err)
	}

	libHTTP.SetTenantSpanAttribute(span, tenantID)

	cursor, limit, err := libHTTP.ParseOpaqueCursorPagination(fiberCtx)
	if err != nil {
		return badRequest(ctx, fiberCtx, span, logger, "invalid pagination", err)
	}

	var contextType *value_objects.ContextType

	if typeParam := strings.TrimSpace(fiberCtx.Query("type")); typeParam != "" {
		parsed, err := value_objects.ParseContextType(strings.ToUpper(typeParam))
		if err != nil {
			return badRequest(ctx, fiberCtx, span, logger, "invalid context type", err)
		}

		contextType = &parsed
	}

	var status *value_objects.ContextStatus

	if statusParam := strings.TrimSpace(fiberCtx.Query("status")); statusParam != "" {
		parsed, err := value_objects.ParseContextStatus(strings.ToUpper(statusParam))
		if err != nil {
			return badRequest(ctx, fiberCtx, span, logger, "invalid context status", err)
		}

		status = &parsed
	}

	result, pagination, err := handler.query.ListContexts(ctx, cursor, limit, contextType, status)
	if err != nil {
		if errors.Is(err, libHTTP.ErrInvalidCursor) {
			return badRequest(ctx, fiberCtx, span, logger, "invalid pagination", err)
		}

		logSpanError(ctx, span, logger, "failed to list contexts", err)

		return writeServiceError(fiberCtx, err)
	}

	if result == nil {
		result = []*entities.ReconciliationContext{}
	}

	response := ListContextsResponse{
		Items: toContextValues(result),
		CursorResponse: sharedpagination.CursorResponse{
			NextCursor: pagination.Next,
			PrevCursor: pagination.Prev,
			Limit:      limit,
			HasMore:    pagination.Next != "",
		},
	}

	return libHTTP.Respond(fiberCtx, fiber.StatusOK, response)
}

// GetContext retrieves a reconciliation context.
//
// @ID getContext
// @Summary Get a reconciliation context
// @Description Returns a reconciliation context by ID.
// @Tags Configuration Contexts
// @Produce json
// @Security BearerAuth
// @Param X-Request-Id header string false "Request ID for tracing"
// @Param contextId path string true "Context ID" format(uuid)
// @Success 200 {object} dto.ReconciliationContextResponse "Successfully retrieved context"
// @Failure 400 {object} ErrorResponse "Invalid context ID format"
// @Failure 401 {object} ErrorResponse "Unauthorized"
// @Failure 403 {object} ErrorResponse "Forbidden"
// @Failure 404 {object} ErrorResponse "Context not found"
// @Failure 500 {object} ErrorResponse "Internal server error"
// @Router /v1/contexts/{contextId} [get]
func (handler *Handler) GetContext(fiberCtx *fiber.Ctx) error {
	ctx, span, logger := startHandlerSpan(fiberCtx, "handler.context.get")
	defer span.End()

	contextID, tenantID, err := libHTTP.ParseAndVerifyTenantScopedID(
		fiberCtx,
		"contextId",
		libHTTP.IDLocationParam,
		handler.contextVerifier,
		auth.GetTenantID,
		libHTTP.ErrMissingContextID,
		libHTTP.ErrInvalidContextID,
		libHTTP.ErrContextAccessDenied,
	)
	if err != nil {
		return handleContextVerificationError(ctx, fiberCtx, span, logger, err)
	}

	libHTTP.SetHandlerSpanAttributes(span, tenantID, contextID)

	result, err := handler.query.GetContext(ctx, contextID)
	if err != nil {
		logSpanError(ctx, span, logger, "failed to get context", err)

		if errors.Is(err, sql.ErrNoRows) {
			return writeNotFound(fiberCtx, "context not found")
		}

		return writeServiceError(fiberCtx, err)
	}

	return libHTTP.Respond(fiberCtx, fiber.StatusOK, dto.ReconciliationContextToResponse(result))
}

// UpdateContext updates a reconciliation context.
//
// @ID updateContext
// @Summary Update a reconciliation context
// @Description Updates fields on a reconciliation context by ID.
// @Tags Configuration Contexts
// @Accept json
// @Produce json
// @Security BearerAuth
// @Param X-Request-Id header string false "Request ID for tracing"
// @Param contextId path string true "Context ID" format(uuid)
// @Param context body dto.UpdateContextRequest true "Context updates"
// @Success 200 {object} dto.ReconciliationContextResponse "Successfully updated context"
// @Failure 400 {object} ErrorResponse "Invalid request payload"
// @Failure 401 {object} ErrorResponse "Unauthorized"
// @Failure 403 {object} ErrorResponse "Forbidden"
// @Failure 404 {object} ErrorResponse "Context not found"
// @Failure 409 {object} ErrorResponse "Conflict: duplicate resource or idempotency key in progress"
// @Failure 500 {object} ErrorResponse "Internal server error"
// @Router /v1/contexts/{contextId} [patch]
func (handler *Handler) UpdateContext(fiberCtx *fiber.Ctx) error {
	ctx, span, logger := startHandlerSpan(fiberCtx, "handler.context.update")
	defer span.End()

	contextID, tenantID, err := libHTTP.ParseAndVerifyTenantScopedID(
		fiberCtx,
		"contextId",
		libHTTP.IDLocationParam,
		handler.contextVerifier,
		auth.GetTenantID,
		libHTTP.ErrMissingContextID,
		libHTTP.ErrInvalidContextID,
		libHTTP.ErrContextAccessDenied,
	)
	if err != nil {
		return handleContextVerificationError(ctx, fiberCtx, span, logger, err)
	}

	libHTTP.SetHandlerSpanAttributes(span, tenantID, contextID)

	var req dto.UpdateContextRequest
	if err := libHTTP.ParseBodyAndValidate(fiberCtx, &req); err != nil {
		return badRequest(ctx, fiberCtx, span, logger, "invalid context payload", err)
	}

	domainInput, err := req.ToDomainInput()
	if err != nil {
		return badRequest(ctx, fiberCtx, span, logger, "invalid context payload", err)
	}

	result, err := handler.command.UpdateContext(ctx, contextID, domainInput)
	if err != nil {
		logSpanError(ctx, span, logger, "failed to update context", err)

		return mapUpdateContextError(fiberCtx, err)
	}

	return libHTTP.Respond(fiberCtx, fiber.StatusOK, dto.ReconciliationContextToResponse(result))
}

// DeleteContext deletes a reconciliation context.
//
// @ID deleteContext
// @Summary Delete a reconciliation context
// @Description Removes a reconciliation context by ID.
// @Tags Configuration Contexts
// @Security BearerAuth
// @Param X-Request-Id header string false "Request ID for tracing"
// @Param contextId path string true "Context ID" format(uuid)
// @Success 204 "Context successfully deleted"
// @Failure 400 {object} ErrorResponse "Invalid context ID format"
// @Failure 401 {object} ErrorResponse "Unauthorized"
// @Failure 403 {object} ErrorResponse "Forbidden"
// @Failure 404 {object} ErrorResponse "Context not found"
// @Failure 500 {object} ErrorResponse "Internal server error"
// @Router /v1/contexts/{contextId} [delete]
func (handler *Handler) DeleteContext(fiberCtx *fiber.Ctx) error {
	ctx, span, logger := startHandlerSpan(fiberCtx, "handler.context.delete")
	defer span.End()

	contextID, tenantID, err := libHTTP.ParseAndVerifyTenantScopedID(
		fiberCtx,
		"contextId",
		libHTTP.IDLocationParam,
		handler.contextVerifier,
		auth.GetTenantID,
		libHTTP.ErrMissingContextID,
		libHTTP.ErrInvalidContextID,
		libHTTP.ErrContextAccessDenied,
	)
	if err != nil {
		return handleContextVerificationError(ctx, fiberCtx, span, logger, err)
	}

	libHTTP.SetHandlerSpanAttributes(span, tenantID, contextID)

	if err := handler.command.DeleteContext(ctx, contextID); err != nil {
		logSpanError(ctx, span, logger, "failed to delete context", err)

		if errors.Is(err, sql.ErrNoRows) {
			return writeNotFound(fiberCtx, "context not found")
		}

		if errors.Is(err, command.ErrContextHasChildEntities) {
			return libHTTP.RespondError(fiberCtx, fiber.StatusConflict, "has_children", err.Error())
		}

		return writeServiceError(fiberCtx, err)
	}

	return libHTTP.RespondStatus(fiberCtx, fiber.StatusNoContent)
}

// CloneContext creates a deep copy of a reconciliation context with all its configuration.
//
// @ID cloneContext
// @Summary Clone a reconciliation context
// @Description Creates a deep copy of a reconciliation context including its sources, field maps, match rules, and optionally fee schedules.
// @Tags Configuration Contexts
// @Accept json
// @Produce json
// @Security BearerAuth
// @Param X-Request-Id header string false "Request ID for tracing"
// @Param contextId path string true "Context ID" format(uuid)
// @Param request body dto.CloneContextRequest true "Clone parameters"
// @Success 201 {object} dto.CloneContextResponse "Context successfully cloned"
// @Failure 400 {object} ErrorResponse "Invalid request payload"
// @Failure 401 {object} ErrorResponse "Unauthorized"
// @Failure 403 {object} ErrorResponse "Forbidden"
// @Failure 404 {object} ErrorResponse "Context not found"
// @Failure 409 {object} ErrorResponse "Conflict: duplicate resource or idempotency key in progress"
// @Failure 500 {object} ErrorResponse "Internal server error"
// @Router /v1/contexts/{contextId}/clone [post]
func (handler *Handler) CloneContext(fiberCtx *fiber.Ctx) error {
	ctx, span, logger := startHandlerSpan(fiberCtx, "handler.context.clone")
	defer span.End()

	contextID, tenantID, err := libHTTP.ParseAndVerifyTenantScopedID(
		fiberCtx,
		"contextId",
		libHTTP.IDLocationParam,
		handler.contextVerifier,
		auth.GetTenantID,
		libHTTP.ErrMissingContextID,
		libHTTP.ErrInvalidContextID,
		libHTTP.ErrContextAccessDenied,
	)
	if err != nil {
		return handleContextVerificationError(ctx, fiberCtx, span, logger, err)
	}

	libHTTP.SetHandlerSpanAttributes(span, tenantID, contextID)

	var payload dto.CloneContextRequest
	if err := libHTTP.ParseBodyAndValidate(fiberCtx, &payload); err != nil {
		return badRequest(ctx, fiberCtx, span, logger, "invalid clone payload", err)
	}

	input := command.CloneContextInput{
		SourceContextID: contextID,
		NewName:         payload.Name,
		IncludeSources:  boolDefault(payload.IncludeSources, true),
		IncludeRules:    boolDefault(payload.IncludeRules, true),
	}

	result, err := handler.command.CloneContext(ctx, input)
	if err != nil {
		logSpanError(ctx, span, logger, "failed to clone context", err)

		if errors.Is(err, command.ErrCloneNameRequired) {
			return libHTTP.RespondError(fiberCtx, fiber.StatusBadRequest, "invalid_request", err.Error())
		}

		if errors.Is(err, sql.ErrNoRows) {
			return writeNotFound(fiberCtx, "source context not found")
		}

		var pgErr *pgconn.PgError
		if errors.As(err, &pgErr) && pgErr.Code == "23505" {
			return libHTTP.RespondError(fiberCtx, fiber.StatusConflict, "duplicate_name", "a context with this name already exists")
		}

		return writeServiceError(fiberCtx, err)
	}

	return libHTTP.Respond(fiberCtx, fiber.StatusCreated, dto.CloneResultToResponse(result))
}

// boolDefault returns the value of b if non-nil, or the default value otherwise.
func boolDefault(b *bool, defaultVal bool) bool {
	if b == nil {
		return defaultVal
	}

	return *b
}

// CreateSource creates a reconciliation source.
//
// @ID createSource
// @Summary Create a reconciliation source
// @Description Creates a new reconciliation source under a context.
// @Tags Configuration Sources
// @Accept json
// @Produce json
// @Security BearerAuth
// @Param X-Request-Id header string false "Request ID for tracing"
// @Param contextId path string true "Context ID" format(uuid)
// @Param source body dto.CreateSourceRequest true "Source creation payload"
// @Success 201 {object} dto.ReconciliationSourceResponse "Successfully created source"
// @Failure 400 {object} ErrorResponse "Invalid request payload"
// @Failure 401 {object} ErrorResponse "Unauthorized"
// @Failure 403 {object} ErrorResponse "Forbidden"
// @Failure 404 {object} ErrorResponse "Context not found"
// @Failure 409 {object} ErrorResponse "Conflict: duplicate resource or idempotency key in progress"
// @Failure 500 {object} ErrorResponse "Internal server error"
// @Router /v1/contexts/{contextId}/sources [post]
func (handler *Handler) CreateSource(fiberCtx *fiber.Ctx) error {
	ctx, span, logger := startHandlerSpan(fiberCtx, "handler.source.create")
	defer span.End()

	contextID, tenantID, err := libHTTP.ParseAndVerifyTenantScopedID(
		fiberCtx,
		"contextId",
		libHTTP.IDLocationParam,
		handler.contextVerifier,
		auth.GetTenantID,
		libHTTP.ErrMissingContextID,
		libHTTP.ErrInvalidContextID,
		libHTTP.ErrContextAccessDenied,
	)
	if err != nil {
		return handleContextVerificationError(ctx, fiberCtx, span, logger, err)
	}

	libHTTP.SetHandlerSpanAttributes(span, tenantID, contextID)

	var req dto.CreateSourceRequest
	if err := libHTTP.ParseBodyAndValidate(fiberCtx, &req); err != nil {
		return badRequest(ctx, fiberCtx, span, logger, "invalid source payload", err)
	}

	domainInput, err := req.ToDomainInput()
	if err != nil {
		return badRequest(ctx, fiberCtx, span, logger, "invalid source payload", err)
	}

	result, err := handler.command.CreateSource(ctx, contextID, domainInput)
	if err != nil {
		logSpanError(ctx, span, logger, "failed to create source", err)
		return writeServiceError(fiberCtx, err)
	}

	return libHTTP.Respond(fiberCtx, fiber.StatusCreated, dto.ReconciliationSourceToResponse(result))
}

// ListSources lists reconciliation sources.
//
// @ID listSources
// @Summary List reconciliation sources
// @Description Returns a cursor-paginated list of reconciliation sources under a context, optionally filtered by type.
// @Tags Configuration Sources
// @Produce json
// @Security BearerAuth
// @Param X-Request-Id header string false "Request ID for tracing"
// @Param contextId path string true "Context ID" format(uuid)
// @Param limit query int false "Maximum number of records to return" default(20) minimum(1) maximum(200)
// @Param cursor query string false "Cursor for pagination (opaque)"
// @Param type query string false "Filter by source type" Enums(LEDGER,BANK,GATEWAY,CUSTOM,FETCHER)
// @Success 200 {object} ListSourcesResponse "List of sources with cursor pagination"
// @Failure 400 {object} ErrorResponse "Invalid query parameters"
// @Failure 401 {object} ErrorResponse "Unauthorized"
// @Failure 403 {object} ErrorResponse "Forbidden"
// @Failure 404 {object} ErrorResponse "Context not found"
// @Failure 500 {object} ErrorResponse "Internal server error"
// @Router /v1/contexts/{contextId}/sources [get]
func (handler *Handler) ListSources(fiberCtx *fiber.Ctx) error {
	ctx, span, logger := startHandlerSpan(fiberCtx, "handler.source.list")
	defer span.End()

	contextID, tenantID, err := libHTTP.ParseAndVerifyTenantScopedID(
		fiberCtx,
		"contextId",
		libHTTP.IDLocationParam,
		handler.contextVerifier,
		auth.GetTenantID,
		libHTTP.ErrMissingContextID,
		libHTTP.ErrInvalidContextID,
		libHTTP.ErrContextAccessDenied,
	)
	if err != nil {
		return handleContextVerificationError(ctx, fiberCtx, span, logger, err)
	}

	libHTTP.SetHandlerSpanAttributes(span, tenantID, contextID)

	cursor, limit, err := libHTTP.ParseOpaqueCursorPagination(fiberCtx)
	if err != nil {
		return badRequest(ctx, fiberCtx, span, logger, "invalid pagination", err)
	}

	var sourceType *value_objects.SourceType

	if typeParam := strings.TrimSpace(fiberCtx.Query("type")); typeParam != "" {
		parsed, err := value_objects.ParseSourceType(strings.ToUpper(typeParam))
		if err != nil {
			return badRequest(ctx, fiberCtx, span, logger, "invalid source type", err)
		}

		sourceType = &parsed
	}

	result, pagination, err := handler.query.ListSources(ctx, contextID, cursor, limit, sourceType)
	if err != nil {
		if errors.Is(err, libHTTP.ErrInvalidCursor) {
			return badRequest(ctx, fiberCtx, span, logger, "invalid pagination", err)
		}

		logSpanError(ctx, span, logger, "failed to list sources", err)

		return writeServiceError(fiberCtx, err)
	}

	if result == nil {
		result = []*entities.ReconciliationSource{}
	}

	// Check which sources have field maps
	sourceIDs := make([]uuid.UUID, len(result))
	for i, src := range result {
		sourceIDs[i] = src.ID
	}

	fieldMapsExist, err := handler.query.CheckFieldMapsExistence(ctx, sourceIDs)
	if err != nil {
		logSpanError(ctx, span, logger, "failed to check field maps existence", err)
		return writeServiceError(fiberCtx, err)
	}

	response := ListSourcesResponse{
		Items: toSourceValuesWithFieldMaps(result, fieldMapsExist),
		CursorResponse: sharedpagination.CursorResponse{
			NextCursor: pagination.Next,
			PrevCursor: pagination.Prev,
			Limit:      limit,
			HasMore:    pagination.Next != "",
		},
	}

	return libHTTP.Respond(fiberCtx, fiber.StatusOK, response)
}

// GetSource retrieves a reconciliation source.
//
// @ID getSource
// @Summary Get a reconciliation source
// @Description Returns a reconciliation source by ID.
// @Tags Configuration Sources
// @Produce json
// @Security BearerAuth
// @Param X-Request-Id header string false "Request ID for tracing"
// @Param contextId path string true "Context ID" format(uuid)
// @Param sourceId path string true "Source ID" format(uuid)
// @Success 200 {object} dto.ReconciliationSourceResponse "Successfully retrieved source"
// @Failure 400 {object} ErrorResponse "Invalid source ID format"
// @Failure 401 {object} ErrorResponse "Unauthorized"
// @Failure 403 {object} ErrorResponse "Forbidden"
// @Failure 404 {object} ErrorResponse "Source not found"
// @Failure 500 {object} ErrorResponse "Internal server error"
// @Router /v1/contexts/{contextId}/sources/{sourceId} [get]
func (handler *Handler) GetSource(fiberCtx *fiber.Ctx) error {
	ctx, span, logger := startHandlerSpan(fiberCtx, "handler.source.get")
	defer span.End()

	contextID, tenantID, err := libHTTP.ParseAndVerifyTenantScopedID(
		fiberCtx,
		"contextId",
		libHTTP.IDLocationParam,
		handler.contextVerifier,
		auth.GetTenantID,
		libHTTP.ErrMissingContextID,
		libHTTP.ErrInvalidContextID,
		libHTTP.ErrContextAccessDenied,
	)
	if err != nil {
		return handleContextVerificationError(ctx, fiberCtx, span, logger, err)
	}

	libHTTP.SetHandlerSpanAttributes(span, tenantID, contextID)

	sourceID, err := parseUUIDParam(fiberCtx, "sourceId")
	if err != nil {
		return badRequest(ctx, fiberCtx, span, logger, "invalid source id", err)
	}

	result, err := handler.query.GetSource(ctx, contextID, sourceID)
	if err != nil {
		logSpanError(ctx, span, logger, "failed to get source", err)

		if errors.Is(err, sql.ErrNoRows) {
			return writeNotFound(fiberCtx, "source not found")
		}

		return writeServiceError(fiberCtx, err)
	}

	return libHTTP.Respond(fiberCtx, fiber.StatusOK, dto.ReconciliationSourceToResponse(result))
}

// UpdateSource updates a reconciliation source.
//
// @ID updateSource
// @Summary Update a reconciliation source
// @Description Updates fields on a reconciliation source by ID.
// @Tags Configuration Sources
// @Accept json
// @Produce json
// @Security BearerAuth
// @Param X-Request-Id header string false "Request ID for tracing"
// @Param contextId path string true "Context ID" format(uuid)
// @Param sourceId path string true "Source ID" format(uuid)
// @Param source body dto.UpdateSourceRequest true "Source updates"
// @Success 200 {object} dto.ReconciliationSourceResponse "Successfully updated source"
// @Failure 400 {object} ErrorResponse "Invalid request payload"
// @Failure 401 {object} ErrorResponse "Unauthorized"
// @Failure 403 {object} ErrorResponse "Forbidden"
// @Failure 404 {object} ErrorResponse "Source not found"
// @Failure 409 {object} ErrorResponse "Conflict: duplicate resource or idempotency key in progress"
// @Failure 500 {object} ErrorResponse "Internal server error"
// @Router /v1/contexts/{contextId}/sources/{sourceId} [patch]
func (handler *Handler) UpdateSource(fiberCtx *fiber.Ctx) error {
	ctx, span, logger := startHandlerSpan(fiberCtx, "handler.source.update")
	defer span.End()

	contextID, tenantID, err := libHTTP.ParseAndVerifyTenantScopedID(
		fiberCtx,
		"contextId",
		libHTTP.IDLocationParam,
		handler.contextVerifier,
		auth.GetTenantID,
		libHTTP.ErrMissingContextID,
		libHTTP.ErrInvalidContextID,
		libHTTP.ErrContextAccessDenied,
	)
	if err != nil {
		return handleContextVerificationError(ctx, fiberCtx, span, logger, err)
	}

	libHTTP.SetHandlerSpanAttributes(span, tenantID, contextID)

	sourceID, err := parseUUIDParam(fiberCtx, "sourceId")
	if err != nil {
		return badRequest(ctx, fiberCtx, span, logger, "invalid source id", err)
	}

	var req dto.UpdateSourceRequest
	if err := libHTTP.ParseBodyAndValidate(fiberCtx, &req); err != nil {
		return badRequest(ctx, fiberCtx, span, logger, "invalid source payload", err)
	}

	domainInput, err := req.ToDomainInput()
	if err != nil {
		return badRequest(ctx, fiberCtx, span, logger, "invalid source payload", err)
	}

	result, err := handler.command.UpdateSource(ctx, contextID, sourceID, domainInput)
	if err != nil {
		logSpanError(ctx, span, logger, "failed to update source", err)

		if errors.Is(err, sql.ErrNoRows) {
			return writeNotFound(fiberCtx, "source not found")
		}

		return writeServiceError(fiberCtx, err)
	}

	return libHTTP.Respond(fiberCtx, fiber.StatusOK, dto.ReconciliationSourceToResponse(result))
}

// DeleteSource deletes a reconciliation source.
//
// @ID deleteSource
// @Summary Delete a reconciliation source
// @Description Removes a reconciliation source by ID.
// @Tags Configuration Sources
// @Security BearerAuth
// @Param X-Request-Id header string false "Request ID for tracing"
// @Param contextId path string true "Context ID" format(uuid)
// @Param sourceId path string true "Source ID" format(uuid)
// @Success 204 "Source successfully deleted"
// @Failure 400 {object} ErrorResponse "Invalid source ID format"
// @Failure 401 {object} ErrorResponse "Unauthorized"
// @Failure 403 {object} ErrorResponse "Forbidden"
// @Failure 404 {object} ErrorResponse "Source not found"
// @Failure 500 {object} ErrorResponse "Internal server error"
// @Router /v1/contexts/{contextId}/sources/{sourceId} [delete]
func (handler *Handler) DeleteSource(fiberCtx *fiber.Ctx) error {
	ctx, span, logger := startHandlerSpan(fiberCtx, "handler.source.delete")
	defer span.End()

	contextID, tenantID, err := libHTTP.ParseAndVerifyTenantScopedID(
		fiberCtx,
		"contextId",
		libHTTP.IDLocationParam,
		handler.contextVerifier,
		auth.GetTenantID,
		libHTTP.ErrMissingContextID,
		libHTTP.ErrInvalidContextID,
		libHTTP.ErrContextAccessDenied,
	)
	if err != nil {
		return handleContextVerificationError(ctx, fiberCtx, span, logger, err)
	}

	libHTTP.SetHandlerSpanAttributes(span, tenantID, contextID)

	sourceID, err := parseUUIDParam(fiberCtx, "sourceId")
	if err != nil {
		return badRequest(ctx, fiberCtx, span, logger, "invalid source id", err)
	}

	if err := handler.command.DeleteSource(ctx, contextID, sourceID); err != nil {
		logSpanError(ctx, span, logger, "failed to delete source", err)

		if errors.Is(err, sql.ErrNoRows) {
			return writeNotFound(fiberCtx, "source not found")
		}

		if errors.Is(err, command.ErrSourceHasFieldMap) {
			return libHTTP.RespondError(fiberCtx, fiber.StatusConflict, "has_field_map", err.Error())
		}

		return writeServiceError(fiberCtx, err)
	}

	return libHTTP.RespondStatus(fiberCtx, fiber.StatusNoContent)
}

// CreateFieldMap creates a field map.
//
// @ID createFieldMap
// @Summary Create a field map
// @Description Creates a field map for a source within a context.
// @Tags Configuration Field Maps
// @Accept json
// @Produce json
// @Security BearerAuth
// @Param X-Request-Id header string false "Request ID for tracing"
// @Param contextId path string true "Context ID" format(uuid)
// @Param sourceId path string true "Source ID" format(uuid)
// @Param fieldMap body dto.CreateFieldMapRequest true "Field map creation payload"
// @Success 201 {object} dto.FieldMapResponse "Successfully created field map"
// @Failure 400 {object} ErrorResponse "Invalid request payload"
// @Failure 401 {object} ErrorResponse "Unauthorized"
// @Failure 403 {object} ErrorResponse "Forbidden"
// @Failure 404 {object} ErrorResponse "Context or source not found"
// @Failure 409 {object} ErrorResponse "Conflict: duplicate resource or idempotency key in progress"
// @Failure 500 {object} ErrorResponse "Internal server error"
// @Router /v1/contexts/{contextId}/sources/{sourceId}/field-maps [post]
func (handler *Handler) CreateFieldMap(fiberCtx *fiber.Ctx) error {
	ctx, span, logger := startHandlerSpan(fiberCtx, "handler.fieldmap.create")
	defer span.End()

	contextID, tenantID, err := libHTTP.ParseAndVerifyTenantScopedID(
		fiberCtx,
		"contextId",
		libHTTP.IDLocationParam,
		handler.contextVerifier,
		auth.GetTenantID,
		libHTTP.ErrMissingContextID,
		libHTTP.ErrInvalidContextID,
		libHTTP.ErrContextAccessDenied,
	)
	if err != nil {
		return handleContextVerificationError(ctx, fiberCtx, span, logger, err)
	}

	libHTTP.SetHandlerSpanAttributes(span, tenantID, contextID)

	sourceID, err := parseUUIDParam(fiberCtx, "sourceId")
	if err != nil {
		return badRequest(ctx, fiberCtx, span, logger, "invalid source id", err)
	}

	var req dto.CreateFieldMapRequest
	if err := libHTTP.ParseBodyAndValidate(fiberCtx, &req); err != nil {
		return badRequest(ctx, fiberCtx, span, logger, "invalid field map payload", err)
	}

	if err := handler.ensureSourceAccess(ctx, fiberCtx, span, logger, contextID, sourceID); err != nil {
		return err
	}

	result, err := handler.command.CreateFieldMap(ctx, contextID, sourceID, req.ToDomainInput())
	if err != nil {
		logSpanError(ctx, span, logger, "failed to create field map", err)
		return writeServiceError(fiberCtx, err)
	}

	return libHTTP.Respond(fiberCtx, fiber.StatusCreated, dto.FieldMapToResponse(result))
}

// GetFieldMapBySource retrieves a field map by source.
//
// @ID getFieldMapBySource
// @Summary Get a field map by source
// @Description Returns the field map for a source within a context.
// @Tags Configuration Field Maps
// @Produce json
// @Security BearerAuth
// @Param X-Request-Id header string false "Request ID for tracing"
// @Param contextId path string true "Context ID" format(uuid)
// @Param sourceId path string true "Source ID" format(uuid)
// @Success 200 {object} dto.FieldMapResponse "Successfully retrieved field map"
// @Failure 400 {object} ErrorResponse "Invalid source ID format"
// @Failure 401 {object} ErrorResponse "Unauthorized"
// @Failure 403 {object} ErrorResponse "Forbidden"
// @Failure 404 {object} ErrorResponse "Field map not found"
// @Failure 500 {object} ErrorResponse "Internal server error"
// @Router /v1/contexts/{contextId}/sources/{sourceId}/field-maps [get]
func (handler *Handler) GetFieldMapBySource(fiberCtx *fiber.Ctx) error {
	ctx, span, logger := startHandlerSpan(fiberCtx, "handler.fieldmap.get_by_source")
	defer span.End()

	contextID, tenantID, err := libHTTP.ParseAndVerifyTenantScopedID(
		fiberCtx,
		"contextId",
		libHTTP.IDLocationParam,
		handler.contextVerifier,
		auth.GetTenantID,
		libHTTP.ErrMissingContextID,
		libHTTP.ErrInvalidContextID,
		libHTTP.ErrContextAccessDenied,
	)
	if err != nil {
		return handleContextVerificationError(ctx, fiberCtx, span, logger, err)
	}

	libHTTP.SetHandlerSpanAttributes(span, tenantID, contextID)

	sourceID, err := parseUUIDParam(fiberCtx, "sourceId")
	if err != nil {
		return badRequest(ctx, fiberCtx, span, logger, "invalid source id", err)
	}

	if err := handler.ensureSourceAccess(ctx, fiberCtx, span, logger, contextID, sourceID); err != nil {
		return err
	}

	result, err := handler.query.GetFieldMapBySource(ctx, sourceID)
	if err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return writeNotFound(fiberCtx, "field map not found")
		}

		logSpanError(ctx, span, logger, "failed to get field map", err)

		return writeServiceError(fiberCtx, err)
	}

	return libHTTP.Respond(fiberCtx, fiber.StatusOK, dto.FieldMapToResponse(result))
}

// UpdateFieldMap updates a field map.
//
// @ID updateFieldMap
// @Summary Update a field map
// @Description Updates fields on a field map by ID.
// @Tags Configuration Field Maps
// @Accept json
// @Produce json
// @Security BearerAuth
// @Param X-Request-Id header string false "Request ID for tracing"
// @Param fieldMapId path string true "Field map ID" format(uuid)
// @Param fieldMap body dto.UpdateFieldMapRequest true "Field map updates"
// @Success 200 {object} dto.FieldMapResponse "Successfully updated field map"
// @Failure 400 {object} ErrorResponse "Invalid request payload"
// @Failure 401 {object} ErrorResponse "Unauthorized"
// @Failure 403 {object} ErrorResponse "Forbidden"
// @Failure 404 {object} ErrorResponse "Field map not found"
// @Failure 409 {object} ErrorResponse "Conflict: duplicate resource or idempotency key in progress"
// @Failure 500 {object} ErrorResponse "Internal server error"
// @Router /v1/field-maps/{fieldMapId} [patch]
func (handler *Handler) UpdateFieldMap(fiberCtx *fiber.Ctx) error {
	ctx, span, logger := startHandlerSpan(fiberCtx, "handler.fieldmap.update")
	defer span.End()

	fieldMapID, err := parseUUIDParam(fiberCtx, "fieldMapId")
	if err != nil {
		return badRequest(ctx, fiberCtx, span, logger, "invalid field map id", err)
	}

	var req dto.UpdateFieldMapRequest
	if err := libHTTP.ParseBodyAndValidate(fiberCtx, &req); err != nil {
		return badRequest(ctx, fiberCtx, span, logger, "invalid field map payload", err)
	}

	tenantID, err := tenantIDFromContext(ctx)
	if err != nil {
		return unauthorized(ctx, fiberCtx, span, logger, err)
	}

	fieldMap, err := handler.query.GetFieldMap(ctx, fieldMapID)
	if err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return writeNotFound(fiberCtx, "resource not found")
		}

		logSpanError(ctx, span, logger, "failed to load field map", err)

		return writeServiceError(fiberCtx, err)
	}

	if err := handler.contextVerifier(ctx, tenantID, fieldMap.ContextID); err != nil {
		return handleOwnershipVerificationError(ctx, fiberCtx, span, logger, err)
	}

	libHTTP.SetHandlerSpanAttributes(span, tenantID, fieldMap.ContextID)

	result, err := handler.command.UpdateFieldMap(ctx, fieldMapID, req.ToDomainInput())
	if err != nil {
		logSpanError(ctx, span, logger, "failed to update field map", err)

		if errors.Is(err, sql.ErrNoRows) {
			return writeNotFound(fiberCtx, "resource not found")
		}

		return writeServiceError(fiberCtx, err)
	}

	return libHTTP.Respond(fiberCtx, fiber.StatusOK, dto.FieldMapToResponse(result))
}

// DeleteFieldMap deletes a field map.
//
// @ID deleteFieldMap
// @Summary Delete a field map
// @Description Removes a field map by ID.
// @Tags Configuration Field Maps
// @Security BearerAuth
// @Param X-Request-Id header string false "Request ID for tracing"
// @Param fieldMapId path string true "Field map ID" format(uuid)
// @Success 204 "Field map successfully deleted"
// @Failure 400 {object} ErrorResponse "Invalid field map ID format"
// @Failure 401 {object} ErrorResponse "Unauthorized"
// @Failure 403 {object} ErrorResponse "Forbidden"
// @Failure 404 {object} ErrorResponse "Field map not found"
// @Failure 500 {object} ErrorResponse "Internal server error"
// @Router /v1/field-maps/{fieldMapId} [delete]
func (handler *Handler) DeleteFieldMap(fiberCtx *fiber.Ctx) error {
	ctx, span, logger := startHandlerSpan(fiberCtx, "handler.fieldmap.delete")
	defer span.End()

	fieldMapID, err := parseUUIDParam(fiberCtx, "fieldMapId")
	if err != nil {
		return badRequest(ctx, fiberCtx, span, logger, "invalid field map id", err)
	}

	tenantID, err := tenantIDFromContext(ctx)
	if err != nil {
		return unauthorized(ctx, fiberCtx, span, logger, err)
	}

	fieldMap, err := handler.query.GetFieldMap(ctx, fieldMapID)
	if err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return writeNotFound(fiberCtx, "resource not found")
		}

		logSpanError(ctx, span, logger, "failed to load field map", err)

		return writeServiceError(fiberCtx, err)
	}

	if err := handler.contextVerifier(ctx, tenantID, fieldMap.ContextID); err != nil {
		return handleOwnershipVerificationError(ctx, fiberCtx, span, logger, err)
	}

	libHTTP.SetHandlerSpanAttributes(span, tenantID, fieldMap.ContextID)

	if err := handler.command.DeleteFieldMap(ctx, fieldMapID); err != nil {
		logSpanError(ctx, span, logger, "failed to delete field map", err)

		if errors.Is(err, sql.ErrNoRows) {
			return writeNotFound(fiberCtx, "resource not found")
		}

		return writeServiceError(fiberCtx, err)
	}

	return libHTTP.RespondStatus(fiberCtx, fiber.StatusNoContent)
}

// CreateMatchRule creates a match rule.
//
// @ID createMatchRule
// @Summary Create a match rule
// @Description Creates a new match rule within a context.
// @Tags Configuration Match Rules
// @Accept json
// @Produce json
// @Security BearerAuth
// @Param X-Request-Id header string false "Request ID for tracing"
// @Param contextId path string true "Context ID" format(uuid)
// @Param rule body dto.CreateMatchRuleRequest true "Match rule creation payload"
// @Success 201 {object} dto.MatchRuleResponse "Successfully created match rule"
// @Failure 400 {object} ErrorResponse "Invalid request payload"
// @Failure 401 {object} ErrorResponse "Unauthorized"
// @Failure 403 {object} ErrorResponse "Forbidden"
// @Failure 404 {object} ErrorResponse "Context not found"
// @Failure 409 {object} ErrorResponse "Conflict: duplicate resource or idempotency key in progress"
// @Failure 500 {object} ErrorResponse "Internal server error"
// @Router /v1/contexts/{contextId}/rules [post]
func (handler *Handler) CreateMatchRule(fiberCtx *fiber.Ctx) error {
	ctx, span, logger := startHandlerSpan(fiberCtx, "handler.matchrule.create")
	defer span.End()

	contextID, tenantID, err := libHTTP.ParseAndVerifyTenantScopedID(
		fiberCtx,
		"contextId",
		libHTTP.IDLocationParam,
		handler.contextVerifier,
		auth.GetTenantID,
		libHTTP.ErrMissingContextID,
		libHTTP.ErrInvalidContextID,
		libHTTP.ErrContextAccessDenied,
	)
	if err != nil {
		return handleContextVerificationError(ctx, fiberCtx, span, logger, err)
	}

	libHTTP.SetHandlerSpanAttributes(span, tenantID, contextID)

	var req dto.CreateMatchRuleRequest
	if err := libHTTP.ParseBodyAndValidate(fiberCtx, &req); err != nil {
		return badRequest(ctx, fiberCtx, span, logger, "invalid match rule payload", err)
	}

	domainInput, err := req.ToDomainInput()
	if err != nil {
		return badRequest(ctx, fiberCtx, span, logger, "invalid match rule payload", err)
	}

	result, err := handler.command.CreateMatchRule(ctx, contextID, domainInput)
	if err != nil {
		logSpanError(ctx, span, logger, "failed to create match rule", err)

		if errors.Is(err, entities.ErrRulePriorityConflict) {
			return libHTTP.RespondError(fiberCtx, fiber.StatusConflict, "priority_conflict", err.Error())
		}

		return writeServiceError(fiberCtx, err)
	}

	return libHTTP.Respond(fiberCtx, fiber.StatusCreated, dto.MatchRuleToResponse(result))
}

// ListMatchRules lists match rules.
//
// @ID listMatchRules
// @Summary List match rules
// @Description Returns a cursor-paginated list of match rules under a context, optionally filtered by type.
// @Tags Configuration Match Rules
// @Produce json
// @Security BearerAuth
// @Param X-Request-Id header string false "Request ID for tracing"
// @Param contextId path string true "Context ID" format(uuid)
// @Param limit query int false "Maximum number of records to return" default(20) minimum(1) maximum(200)
// @Param cursor query string false "Cursor for pagination (opaque)"
// @Param type query string false "Filter by rule type" Enums(EXACT,TOLERANCE,DATE_LAG)
// @Success 200 {object} ListMatchRulesResponse "List of match rules with cursor pagination"
// @Failure 400 {object} ErrorResponse "Invalid query parameters"
// @Failure 401 {object} ErrorResponse "Unauthorized"
// @Failure 403 {object} ErrorResponse "Forbidden"
// @Failure 404 {object} ErrorResponse "Context not found"
// @Failure 500 {object} ErrorResponse "Internal server error"
// @Router /v1/contexts/{contextId}/rules [get]
func (handler *Handler) ListMatchRules(fiberCtx *fiber.Ctx) error {
	ctx, span, logger := startHandlerSpan(fiberCtx, "handler.matchrule.list")
	defer span.End()

	contextID, tenantID, err := libHTTP.ParseAndVerifyTenantScopedID(
		fiberCtx,
		"contextId",
		libHTTP.IDLocationParam,
		handler.contextVerifier,
		auth.GetTenantID,
		libHTTP.ErrMissingContextID,
		libHTTP.ErrInvalidContextID,
		libHTTP.ErrContextAccessDenied,
	)
	if err != nil {
		return handleContextVerificationError(ctx, fiberCtx, span, logger, err)
	}

	libHTTP.SetHandlerSpanAttributes(span, tenantID, contextID)

	cursor, limit, err := libHTTP.ParseOpaqueCursorPagination(fiberCtx)
	if err != nil {
		return badRequest(ctx, fiberCtx, span, logger, "invalid pagination", err)
	}

	var ruleType *value_objects.RuleType

	if typeParam := strings.TrimSpace(fiberCtx.Query("type")); typeParam != "" {
		parsed, err := value_objects.ParseRuleType(strings.ToUpper(typeParam))
		if err != nil {
			return badRequest(ctx, fiberCtx, span, logger, "invalid rule type", err)
		}

		ruleType = &parsed
	}

	result, pagination, err := handler.query.ListMatchRules(ctx, contextID, cursor, limit, ruleType)
	if err != nil {
		if errors.Is(err, libHTTP.ErrInvalidCursor) {
			return badRequest(ctx, fiberCtx, span, logger, "invalid pagination", err)
		}

		logSpanError(ctx, span, logger, "failed to list match rules", err)

		return writeServiceError(fiberCtx, err)
	}

	if result == nil {
		result = []*entities.MatchRule{}
	}

	response := ListMatchRulesResponse{
		Items: toMatchRuleValues(result),
		CursorResponse: sharedpagination.CursorResponse{
			NextCursor: pagination.Next,
			PrevCursor: pagination.Prev,
			Limit:      limit,
			HasMore:    pagination.Next != "",
		},
	}

	return libHTTP.Respond(fiberCtx, fiber.StatusOK, response)
}

// GetMatchRule retrieves a match rule.
//
// @ID getMatchRule
// @Summary Get a match rule
// @Description Returns a match rule by ID.
// @Tags Configuration Match Rules
// @Produce json
// @Security BearerAuth
// @Param X-Request-Id header string false "Request ID for tracing"
// @Param contextId path string true "Context ID" format(uuid)
// @Param ruleId path string true "Rule ID" format(uuid)
// @Success 200 {object} dto.MatchRuleResponse "Successfully retrieved match rule"
// @Failure 400 {object} ErrorResponse "Invalid rule ID format"
// @Failure 401 {object} ErrorResponse "Unauthorized"
// @Failure 403 {object} ErrorResponse "Forbidden"
// @Failure 404 {object} ErrorResponse "Match rule not found"
// @Failure 500 {object} ErrorResponse "Internal server error"
// @Router /v1/contexts/{contextId}/rules/{ruleId} [get]
func (handler *Handler) GetMatchRule(fiberCtx *fiber.Ctx) error {
	ctx, span, logger := startHandlerSpan(fiberCtx, "handler.matchrule.get")
	defer span.End()

	contextID, tenantID, err := libHTTP.ParseAndVerifyTenantScopedID(
		fiberCtx,
		"contextId",
		libHTTP.IDLocationParam,
		handler.contextVerifier,
		auth.GetTenantID,
		libHTTP.ErrMissingContextID,
		libHTTP.ErrInvalidContextID,
		libHTTP.ErrContextAccessDenied,
	)
	if err != nil {
		return handleContextVerificationError(ctx, fiberCtx, span, logger, err)
	}

	libHTTP.SetHandlerSpanAttributes(span, tenantID, contextID)

	ruleID, err := parseUUIDParam(fiberCtx, "ruleId")
	if err != nil {
		return badRequest(ctx, fiberCtx, span, logger, "invalid rule id", err)
	}

	result, err := handler.query.GetMatchRule(ctx, contextID, ruleID)
	if err != nil {
		logSpanError(ctx, span, logger, "failed to get match rule", err)

		if errors.Is(err, sql.ErrNoRows) {
			return writeNotFound(fiberCtx, "match rule not found")
		}

		return writeServiceError(fiberCtx, err)
	}

	return libHTTP.Respond(fiberCtx, fiber.StatusOK, dto.MatchRuleToResponse(result))
}

// UpdateMatchRule updates a match rule.
//
// @ID updateMatchRule
// @Summary Update a match rule
// @Description Updates fields on a match rule by ID.
// @Tags Configuration Match Rules
// @Accept json
// @Produce json
// @Security BearerAuth
// @Param X-Request-Id header string false "Request ID for tracing"
// @Param contextId path string true "Context ID" format(uuid)
// @Param ruleId path string true "Rule ID" format(uuid)
// @Param rule body dto.UpdateMatchRuleRequest true "Match rule updates"
// @Success 200 {object} dto.MatchRuleResponse "Successfully updated match rule"
// @Failure 400 {object} ErrorResponse "Invalid request payload"
// @Failure 401 {object} ErrorResponse "Unauthorized"
// @Failure 403 {object} ErrorResponse "Forbidden"
// @Failure 404 {object} ErrorResponse "Match rule not found"
// @Failure 409 {object} ErrorResponse "Conflict: duplicate resource or idempotency key in progress"
// @Failure 500 {object} ErrorResponse "Internal server error"
// @Router /v1/contexts/{contextId}/rules/{ruleId} [patch]
func (handler *Handler) UpdateMatchRule(fiberCtx *fiber.Ctx) error {
	ctx, span, logger := startHandlerSpan(fiberCtx, "handler.matchrule.update")
	defer span.End()

	contextID, tenantID, err := libHTTP.ParseAndVerifyTenantScopedID(
		fiberCtx,
		"contextId",
		libHTTP.IDLocationParam,
		handler.contextVerifier,
		auth.GetTenantID,
		libHTTP.ErrMissingContextID,
		libHTTP.ErrInvalidContextID,
		libHTTP.ErrContextAccessDenied,
	)
	if err != nil {
		return handleContextVerificationError(ctx, fiberCtx, span, logger, err)
	}

	libHTTP.SetHandlerSpanAttributes(span, tenantID, contextID)

	ruleID, err := parseUUIDParam(fiberCtx, "ruleId")
	if err != nil {
		return badRequest(ctx, fiberCtx, span, logger, "invalid rule id", err)
	}

	var req dto.UpdateMatchRuleRequest
	if err := libHTTP.ParseBodyAndValidate(fiberCtx, &req); err != nil {
		return badRequest(ctx, fiberCtx, span, logger, "invalid match rule payload", err)
	}

	result, err := handler.command.UpdateMatchRule(ctx, contextID, ruleID, req.ToDomainInput())
	if err != nil {
		logSpanError(ctx, span, logger, "failed to update match rule", err)

		if errors.Is(err, entities.ErrRulePriorityConflict) {
			return libHTTP.RespondError(fiberCtx, fiber.StatusConflict, "priority_conflict", err.Error())
		}

		if errors.Is(err, sql.ErrNoRows) {
			return writeNotFound(fiberCtx, "match rule not found")
		}

		return writeServiceError(fiberCtx, err)
	}

	return libHTTP.Respond(fiberCtx, fiber.StatusOK, dto.MatchRuleToResponse(result))
}

// DeleteMatchRule deletes a match rule.
//
// @ID deleteMatchRule
// @Summary Delete a match rule
// @Description Removes a match rule by ID.
// @Tags Configuration Match Rules
// @Security BearerAuth
// @Param X-Request-Id header string false "Request ID for tracing"
// @Param contextId path string true "Context ID" format(uuid)
// @Param ruleId path string true "Rule ID" format(uuid)
// @Success 204 "Match rule successfully deleted"
// @Failure 400 {object} ErrorResponse "Invalid rule ID format"
// @Failure 401 {object} ErrorResponse "Unauthorized"
// @Failure 403 {object} ErrorResponse "Forbidden"
// @Failure 404 {object} ErrorResponse "Match rule not found"
// @Failure 500 {object} ErrorResponse "Internal server error"
// @Router /v1/contexts/{contextId}/rules/{ruleId} [delete]
func (handler *Handler) DeleteMatchRule(fiberCtx *fiber.Ctx) error {
	ctx, span, logger := startHandlerSpan(fiberCtx, "handler.matchrule.delete")
	defer span.End()

	contextID, tenantID, err := libHTTP.ParseAndVerifyTenantScopedID(
		fiberCtx,
		"contextId",
		libHTTP.IDLocationParam,
		handler.contextVerifier,
		auth.GetTenantID,
		libHTTP.ErrMissingContextID,
		libHTTP.ErrInvalidContextID,
		libHTTP.ErrContextAccessDenied,
	)
	if err != nil {
		return handleContextVerificationError(ctx, fiberCtx, span, logger, err)
	}

	libHTTP.SetHandlerSpanAttributes(span, tenantID, contextID)

	ruleID, err := parseUUIDParam(fiberCtx, "ruleId")
	if err != nil {
		return badRequest(ctx, fiberCtx, span, logger, "invalid rule id", err)
	}

	if err := handler.command.DeleteMatchRule(ctx, contextID, ruleID); err != nil {
		logSpanError(ctx, span, logger, "failed to delete match rule", err)

		if errors.Is(err, sql.ErrNoRows) {
			return writeNotFound(fiberCtx, "match rule not found")
		}

		return writeServiceError(fiberCtx, err)
	}

	return libHTTP.RespondStatus(fiberCtx, fiber.StatusNoContent)
}

// ReorderRequest defines the rule ID ordering payload.
type ReorderRequest struct {
	RuleIDs []uuid.UUID `json:"ruleIds" validate:"required,min=1,max=100,dive,required" minItems:"1" maxItems:"100"`
}

// ReorderMatchRules reorders match rule priorities.
//
// @ID reorderMatchRules
// @Summary Reorder match rules
// @Description Reorders match rule priorities within a context.
// @Tags Configuration Match Rules
// @Accept json
// @Produce json
// @Security BearerAuth
// @Param X-Request-Id header string false "Request ID for tracing"
// @Param contextId path string true "Context ID" format(uuid)
// @Param reorder body ReorderRequest true "Ordered list of rule IDs"
// @Success 204 "Match rules reordered"
// @Failure 400 {object} ErrorResponse "Invalid request payload"
// @Failure 401 {object} ErrorResponse "Unauthorized"
// @Failure 403 {object} ErrorResponse "Forbidden"
// @Failure 404 {object} ErrorResponse "Context not found"
// @Failure 500 {object} ErrorResponse "Internal server error"
// @Router /v1/contexts/{contextId}/rules/reorder [post]
func (handler *Handler) ReorderMatchRules(fiberCtx *fiber.Ctx) error {
	ctx, span, logger := startHandlerSpan(fiberCtx, "handler.matchrule.reorder")
	defer span.End()

	contextID, tenantID, err := libHTTP.ParseAndVerifyTenantScopedID(
		fiberCtx,
		"contextId",
		libHTTP.IDLocationParam,
		handler.contextVerifier,
		auth.GetTenantID,
		libHTTP.ErrMissingContextID,
		libHTTP.ErrInvalidContextID,
		libHTTP.ErrContextAccessDenied,
	)
	if err != nil {
		return handleContextVerificationError(ctx, fiberCtx, span, logger, err)
	}

	libHTTP.SetHandlerSpanAttributes(span, tenantID, contextID)

	var payload ReorderRequest
	if err := libHTTP.ParseBodyAndValidate(fiberCtx, &payload); err != nil {
		return badRequest(ctx, fiberCtx, span, logger, "invalid reorder payload", err)
	}

	if len(payload.RuleIDs) == 0 {
		return badRequest(ctx, fiberCtx, span, logger, "missing rule IDs", ErrRuleIDsRequired)
	}

	if err := handler.command.ReorderMatchRulePriorities(ctx, contextID, payload.RuleIDs); err != nil {
		logSpanError(ctx, span, logger, "failed to reorder match rules", err)

		if errors.Is(err, sql.ErrNoRows) {
			return writeNotFound(fiberCtx, "match rule not found")
		}

		return writeServiceError(fiberCtx, err)
	}

	return libHTTP.RespondStatus(fiberCtx, fiber.StatusNoContent)
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
