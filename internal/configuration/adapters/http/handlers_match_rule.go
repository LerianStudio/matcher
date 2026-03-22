package http

import (
	"database/sql"
	"errors"
	"strings"

	"github.com/gofiber/fiber/v2"
	"github.com/google/uuid"

	libHTTP "github.com/LerianStudio/lib-commons/v4/commons/net/http"

	"github.com/LerianStudio/matcher/internal/auth"
	"github.com/LerianStudio/matcher/internal/configuration/adapters/http/dto"
	"github.com/LerianStudio/matcher/internal/configuration/domain/entities"
	"github.com/LerianStudio/matcher/internal/configuration/domain/value_objects"
	sharedpagination "github.com/LerianStudio/matcher/internal/shared/adapters/http"
)

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
// @Router /v1/config/contexts/{contextId}/rules [post]
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
// @Router /v1/config/contexts/{contextId}/rules [get]
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
// @Router /v1/config/contexts/{contextId}/rules/{ruleId} [get]
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
// @Router /v1/config/contexts/{contextId}/rules/{ruleId} [patch]
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
// @Router /v1/config/contexts/{contextId}/rules/{ruleId} [delete]
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
// @Router /v1/config/contexts/{contextId}/rules/reorder [post]
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
