package http

import (
	"database/sql"
	"errors"
	"fmt"
	"strings"

	"github.com/gofiber/fiber/v2"
	"github.com/jackc/pgx/v5/pgconn"

	libHTTP "github.com/LerianStudio/lib-commons/v5/commons/net/http"

	"github.com/LerianStudio/matcher/internal/auth"
	"github.com/LerianStudio/matcher/internal/configuration/adapters/http/dto"
	"github.com/LerianStudio/matcher/internal/configuration/domain/entities"
	"github.com/LerianStudio/matcher/internal/configuration/domain/value_objects"
	"github.com/LerianStudio/matcher/internal/configuration/services/command"
	sharedhttp "github.com/LerianStudio/matcher/internal/shared/adapters/http"
)

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
// @Failure 400 {object} sharedhttp.ErrorResponse "Invalid request payload"
// @Failure 401 {object} sharedhttp.ErrorResponse "Unauthorized"
// @Failure 403 {object} sharedhttp.ErrorResponse "Forbidden"
// @Failure 409 {object} sharedhttp.ErrorResponse "Conflict: duplicate resource or idempotency key in progress"
// @Failure 500 {object} sharedhttp.ErrorResponse "Internal server error"
// @Router /v1/config/contexts [post]
func (handler *Handler) CreateContext(fiberCtx *fiber.Ctx) error {
	ctx, span, logger := startHandlerSpan(fiberCtx, "handler.context.create")
	defer span.End()

	var req dto.CreateContextRequest
	if err := libHTTP.ParseBodyAndValidate(fiberCtx, &req); err != nil {
		return handler.badRequest(ctx, fiberCtx, span, logger, "invalid context payload", err)
	}

	tenantID, err := tenantIDFromContext(ctx)
	if err != nil {
		return handler.unauthorized(ctx, fiberCtx, span, logger, err)
	}

	libHTTP.SetTenantSpanAttribute(span, tenantID)

	domainInput, err := req.ToDomainInput()
	if err != nil {
		return handler.badRequest(ctx, fiberCtx, span, logger, "invalid context payload", err)
	}

	result, err := handler.command.CreateContext(ctx, tenantID, domainInput)
	if err != nil {
		handler.logSpanError(ctx, span, logger, "failed to create context", err)

		if errors.Is(err, command.ErrContextNameAlreadyExists) {
			return respondError(fiberCtx, fiber.StatusConflict, "duplicate_name", err.Error())
		}

		if errors.Is(err, entities.ErrRulePriorityConflict) {
			return respondError(fiberCtx, fiber.StatusConflict, "priority_conflict", err.Error())
		}

		return writeServiceError(fiberCtx, err)
	}

	if err := libHTTP.Respond(fiberCtx, fiber.StatusCreated, dto.ReconciliationContextToResponse(result)); err != nil {
		return fmt.Errorf("respond create context: %w", err)
	}

	return nil
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
// @Failure 400 {object} sharedhttp.ErrorResponse "Invalid query parameters"
// @Failure 401 {object} sharedhttp.ErrorResponse "Unauthorized"
// @Failure 403 {object} sharedhttp.ErrorResponse "Forbidden"
// @Failure 500 {object} sharedhttp.ErrorResponse "Internal server error"
// @Router /v1/config/contexts [get]
func (handler *Handler) ListContexts(fiberCtx *fiber.Ctx) error {
	ctx, span, logger := startHandlerSpan(fiberCtx, "handler.context.list")
	defer span.End()

	tenantID, err := tenantIDFromContext(ctx)
	if err != nil {
		return handler.unauthorized(ctx, fiberCtx, span, logger, err)
	}

	libHTTP.SetTenantSpanAttribute(span, tenantID)

	cursor, limit, err := libHTTP.ParseOpaqueCursorPagination(fiberCtx)
	if err != nil {
		return handler.badRequest(ctx, fiberCtx, span, logger, "invalid pagination", err)
	}

	var contextType *value_objects.ContextType

	if typeParam := strings.TrimSpace(fiberCtx.Query("type")); typeParam != "" {
		parsed, err := value_objects.ParseContextType(strings.ToUpper(typeParam))
		if err != nil {
			return handler.badRequest(ctx, fiberCtx, span, logger, "invalid context type", err)
		}

		contextType = &parsed
	}

	var status *value_objects.ContextStatus

	if statusParam := strings.TrimSpace(fiberCtx.Query("status")); statusParam != "" {
		parsed, err := value_objects.ParseContextStatus(strings.ToUpper(statusParam))
		if err != nil {
			return handler.badRequest(ctx, fiberCtx, span, logger, "invalid context status", err)
		}

		status = &parsed
	}

	result, pagination, err := handler.query.ListContexts(ctx, cursor, limit, contextType, status)
	if err != nil {
		if errors.Is(err, libHTTP.ErrInvalidCursor) {
			return handler.badRequest(ctx, fiberCtx, span, logger, "invalid pagination", err)
		}

		handler.logSpanError(ctx, span, logger, "failed to list contexts", err)

		return writeServiceError(fiberCtx, err)
	}

	if result == nil {
		result = []*entities.ReconciliationContext{}
	}

	response := ListContextsResponse{
		Items: toContextValues(result),
		CursorResponse: sharedhttp.CursorResponse{
			NextCursor: pagination.Next,
			PrevCursor: pagination.Prev,
			Limit:      limit,
			HasMore:    pagination.Next != "",
		},
	}

	if err := libHTTP.Respond(fiberCtx, fiber.StatusOK, response); err != nil {
		return fmt.Errorf("respond list contexts: %w", err)
	}

	return nil
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
// @Failure 400 {object} sharedhttp.ErrorResponse "Invalid context ID format"
// @Failure 401 {object} sharedhttp.ErrorResponse "Unauthorized"
// @Failure 403 {object} sharedhttp.ErrorResponse "Forbidden"
// @Failure 404 {object} sharedhttp.ErrorResponse "Context not found"
// @Failure 500 {object} sharedhttp.ErrorResponse "Internal server error"
// @Router /v1/config/contexts/{contextId} [get]
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
		return handler.handleContextVerificationError(ctx, fiberCtx, span, logger, err)
	}

	libHTTP.SetHandlerSpanAttributes(span, tenantID, contextID)

	result, err := handler.query.GetContext(ctx, contextID)
	if err != nil {
		handler.logSpanError(ctx, span, logger, "failed to get context", err)

		if errors.Is(err, sql.ErrNoRows) {
			return writeNotFound(fiberCtx, "configuration_context_not_found", "context not found")
		}

		return writeServiceError(fiberCtx, err)
	}

	if err := libHTTP.Respond(fiberCtx, fiber.StatusOK, dto.ReconciliationContextToResponse(result)); err != nil {
		return fmt.Errorf("respond get context: %w", err)
	}

	return nil
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
// @Failure 400 {object} sharedhttp.ErrorResponse "Invalid request payload"
// @Failure 401 {object} sharedhttp.ErrorResponse "Unauthorized"
// @Failure 403 {object} sharedhttp.ErrorResponse "Forbidden"
// @Failure 404 {object} sharedhttp.ErrorResponse "Context not found"
// @Failure 409 {object} sharedhttp.ErrorResponse "Conflict: duplicate resource or idempotency key in progress"
// @Failure 500 {object} sharedhttp.ErrorResponse "Internal server error"
// @Router /v1/config/contexts/{contextId} [patch]
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
		return handler.handleContextVerificationError(ctx, fiberCtx, span, logger, err)
	}

	libHTTP.SetHandlerSpanAttributes(span, tenantID, contextID)

	var req dto.UpdateContextRequest
	if err := libHTTP.ParseBodyAndValidate(fiberCtx, &req); err != nil {
		return handler.badRequest(ctx, fiberCtx, span, logger, "invalid context payload", err)
	}

	domainInput, err := req.ToDomainInput()
	if err != nil {
		return handler.badRequest(ctx, fiberCtx, span, logger, "invalid context payload", err)
	}

	result, err := handler.command.UpdateContext(ctx, contextID, domainInput)
	if err != nil {
		handler.logSpanError(ctx, span, logger, "failed to update context", err)

		return mapUpdateContextError(fiberCtx, err)
	}

	if err := libHTTP.Respond(fiberCtx, fiber.StatusOK, dto.ReconciliationContextToResponse(result)); err != nil {
		return fmt.Errorf("respond update context: %w", err)
	}

	return nil
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
// @Failure 400 {object} sharedhttp.ErrorResponse "Invalid context ID format"
// @Failure 401 {object} sharedhttp.ErrorResponse "Unauthorized"
// @Failure 403 {object} sharedhttp.ErrorResponse "Forbidden"
// @Failure 404 {object} sharedhttp.ErrorResponse "Context not found"
// @Failure 500 {object} sharedhttp.ErrorResponse "Internal server error"
// @Router /v1/config/contexts/{contextId} [delete]
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
		return handler.handleContextVerificationError(ctx, fiberCtx, span, logger, err)
	}

	libHTTP.SetHandlerSpanAttributes(span, tenantID, contextID)

	if err := handler.command.DeleteContext(ctx, contextID); err != nil {
		handler.logSpanError(ctx, span, logger, "failed to delete context", err)

		if errors.Is(err, sql.ErrNoRows) {
			return writeNotFound(fiberCtx, "configuration_context_not_found", "context not found")
		}

		if errors.Is(err, command.ErrContextHasChildEntities) {
			return respondError(fiberCtx, fiber.StatusConflict, "has_children", err.Error())
		}

		return writeServiceError(fiberCtx, err)
	}

	if err := libHTTP.RespondStatus(fiberCtx, fiber.StatusNoContent); err != nil {
		return fmt.Errorf("respond delete context: %w", err)
	}

	return nil
}

// CloneContext creates a deep copy of a reconciliation context with all its configuration.
//
// @ID cloneContext
// @Summary Clone a reconciliation context
// @Description Creates a deep copy of a reconciliation context including its sources, field maps, match rules, and fee rules. Referenced fee schedules are reused by cloned fee rules.
// @Tags Configuration Contexts
// @Accept json
// @Produce json
// @Security BearerAuth
// @Param X-Request-Id header string false "Request ID for tracing"
// @Param contextId path string true "Context ID" format(uuid)
// @Param request body dto.CloneContextRequest true "Clone parameters"
// @Success 201 {object} dto.CloneContextResponse "Context successfully cloned"
// @Failure 400 {object} sharedhttp.ErrorResponse "Invalid request payload"
// @Failure 401 {object} sharedhttp.ErrorResponse "Unauthorized"
// @Failure 403 {object} sharedhttp.ErrorResponse "Forbidden"
// @Failure 404 {object} sharedhttp.ErrorResponse "Context not found"
// @Failure 409 {object} sharedhttp.ErrorResponse "Conflict: duplicate resource or idempotency key in progress"
// @Failure 500 {object} sharedhttp.ErrorResponse "Internal server error"
// @Router /v1/config/contexts/{contextId}/clone [post]
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
		return handler.handleContextVerificationError(ctx, fiberCtx, span, logger, err)
	}

	libHTTP.SetHandlerSpanAttributes(span, tenantID, contextID)

	var payload dto.CloneContextRequest
	if err := libHTTP.ParseBodyAndValidate(fiberCtx, &payload); err != nil {
		return handler.badRequest(ctx, fiberCtx, span, logger, "invalid clone payload", err)
	}

	input := command.CloneContextInput{
		SourceContextID: contextID,
		NewName:         payload.Name,
		IncludeSources:  boolDefault(payload.IncludeSources, true),
		IncludeRules:    boolDefault(payload.IncludeRules, true),
	}

	result, err := handler.command.CloneContext(ctx, input)
	if err != nil {
		handler.logSpanError(ctx, span, logger, "failed to clone context", err)

		if errors.Is(err, command.ErrCloneNameRequired) {
			return respondError(fiberCtx, fiber.StatusBadRequest, "configuration_context_name_required", err.Error())
		}

		if errors.Is(err, sql.ErrNoRows) {
			return writeNotFound(fiberCtx, "configuration_context_not_found", "source context not found")
		}

		var pgErr *pgconn.PgError
		if errors.As(err, &pgErr) && pgErr.Code == "23505" {
			return respondError(fiberCtx, fiber.StatusConflict, "duplicate_name", "a context with this name already exists")
		}

		return writeServiceError(fiberCtx, err)
	}

	if err := libHTTP.Respond(fiberCtx, fiber.StatusCreated, dto.CloneResultToResponse(result)); err != nil {
		return fmt.Errorf("respond clone context: %w", err)
	}

	return nil
}
