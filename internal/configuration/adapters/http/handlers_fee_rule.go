package http

import (
	"errors"

	"github.com/gofiber/fiber/v2"
	"github.com/google/uuid"

	libHTTP "github.com/LerianStudio/lib-commons/v4/commons/net/http"

	"github.com/LerianStudio/matcher/internal/auth"
	"github.com/LerianStudio/matcher/internal/configuration/adapters/http/dto"
	"github.com/LerianStudio/matcher/internal/configuration/services/command"
	"github.com/LerianStudio/matcher/internal/shared/domain/fee"
)

// CreateFeeRule creates a fee rule.
//
// @ID createFeeRule
// @Summary Create a fee rule
// @Description Creates a new fee rule that maps transaction metadata to a fee schedule within a context. Priority must be unique within a context across all sides (LEFT, RIGHT, and ANY rules share the same priority space).
// @Tags Configuration Fee Rules
// @Accept json
// @Produce json
// @Security BearerAuth
// @Param X-Request-Id header string false "Request ID for tracing"
// @Param X-Idempotency-Key header string false "Idempotency key for safe retries"
// @Param contextId path string true "Context ID" format(uuid)
// @Param feeRule body dto.CreateFeeRuleRequest true "Fee rule creation payload"
// @Success 201 {object} dto.FeeRuleResponse "Successfully created fee rule"
// @Failure 400 {object} ErrorResponse "Invalid request payload"
// @Failure 401 {object} ErrorResponse "Unauthorized"
// @Failure 403 {object} ErrorResponse "Forbidden"
// @Failure 404 {object} ErrorResponse "Context not found"
// @Failure 409 {object} ErrorResponse "Conflict: duplicate priority or name"
// @Failure 500 {object} ErrorResponse "Internal server error"
// @Router /v1/config/contexts/{contextId}/fee-rules [post]
func (handler *Handler) CreateFeeRule(fiberCtx *fiber.Ctx) error {
	ctx, span, logger := startHandlerSpan(fiberCtx, "handler.fee_rule.create")
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

	var payload dto.CreateFeeRuleRequest
	if err := libHTTP.ParseBodyAndValidate(fiberCtx, &payload); err != nil {
		return badRequest(ctx, fiberCtx, span, logger, "invalid fee rule payload", err)
	}

	feeScheduleID, err := uuid.Parse(payload.FeeScheduleID)
	if err != nil {
		return badRequest(ctx, fiberCtx, span, logger, "invalid fee schedule id", err)
	}

	predicates := dto.ToPredicates(payload.Predicates)

	result, err := handler.command.CreateFeeRule(
		ctx,
		contextID,
		payload.Side,
		feeScheduleID,
		payload.Name,
		payload.Priority,
		predicates,
	)
	if err != nil {
		logSpanError(ctx, span, logger, "failed to create fee rule", err)

		return mapFeeRuleError(fiberCtx, err)
	}

	return libHTTP.Respond(fiberCtx, fiber.StatusCreated, dto.FeeRuleToResponse(result))
}

// ListFeeRules lists fee rules for a context.
//
// @ID listFeeRules
// @Summary List fee rules
// @Description Returns all fee rules for a reconciliation context, ordered by priority.
// @Tags Configuration Fee Rules
// @Produce json
// @Security BearerAuth
// @Param X-Request-Id header string false "Request ID for tracing"
// @Param contextId path string true "Context ID" format(uuid)
// @Success 200 {array} dto.FeeRuleResponse "List of fee rules"
// @Failure 400 {object} ErrorResponse "Invalid context ID format"
// @Failure 401 {object} ErrorResponse "Unauthorized"
// @Failure 403 {object} ErrorResponse "Forbidden"
// @Failure 404 {object} ErrorResponse "Context not found"
// @Failure 500 {object} ErrorResponse "Internal server error"
// @Router /v1/config/contexts/{contextId}/fee-rules [get]
func (handler *Handler) ListFeeRules(fiberCtx *fiber.Ctx) error {
	ctx, span, logger := startHandlerSpan(fiberCtx, "handler.fee_rule.list")
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

	result, err := handler.query.ListFeeRules(ctx, contextID)
	if err != nil {
		logSpanError(ctx, span, logger, "failed to list fee rules", err)
		return writeServiceError(fiberCtx, err)
	}

	if result == nil {
		result = []*fee.FeeRule{}
	}

	return libHTTP.Respond(fiberCtx, fiber.StatusOK, dto.FeeRulesToResponse(result))
}

// GetFeeRule retrieves a fee rule.
//
// @ID getFeeRule
// @Summary Get a fee rule
// @Description Returns a fee rule by ID.
// @Tags Configuration Fee Rules
// @Produce json
// @Security BearerAuth
// @Param X-Request-Id header string false "Request ID for tracing"
// @Param feeRuleId path string true "Fee Rule ID" format(uuid)
// @Success 200 {object} dto.FeeRuleResponse "Successfully retrieved fee rule"
// @Failure 400 {object} ErrorResponse "Invalid fee rule ID format"
// @Failure 401 {object} ErrorResponse "Unauthorized"
// @Failure 403 {object} ErrorResponse "Forbidden"
// @Failure 404 {object} ErrorResponse "Fee rule not found"
// @Failure 500 {object} ErrorResponse "Internal server error"
// @Router /v1/config/fee-rules/{feeRuleId} [get]
func (handler *Handler) GetFeeRule(fiberCtx *fiber.Ctx) error {
	ctx, span, logger := startHandlerSpan(fiberCtx, "handler.fee_rule.get")
	defer span.End()

	tenantID, err := tenantIDFromContext(ctx)
	if err != nil {
		return unauthorized(ctx, fiberCtx, span, logger, err)
	}

	libHTTP.SetTenantSpanAttribute(span, tenantID)

	feeRuleID, err := parseUUIDParam(fiberCtx, "feeRuleId")
	if err != nil {
		return badRequest(ctx, fiberCtx, span, logger, "invalid fee rule id", err)
	}

	result, err := handler.query.GetFeeRule(ctx, feeRuleID)
	if err != nil {
		logSpanError(ctx, span, logger, "failed to get fee rule", err)

		if errors.Is(err, fee.ErrFeeRuleNotFound) {
			return writeNotFound(fiberCtx, "fee rule not found")
		}

		return writeServiceError(fiberCtx, err)
	}

	if result == nil {
		return writeNotFound(fiberCtx, "fee rule not found")
	}

	if err := handler.contextVerifier(ctx, tenantID, result.ContextID); err != nil {
		return handleOwnershipVerificationError(ctx, fiberCtx, span, logger, err)
	}

	libHTTP.SetHandlerSpanAttributes(span, tenantID, result.ContextID)

	return libHTTP.Respond(fiberCtx, fiber.StatusOK, dto.FeeRuleToResponse(result))
}

// UpdateFeeRule updates a fee rule.
//
// @ID updateFeeRule
// @Summary Update a fee rule
// @Description Updates fields on a fee rule by ID.
// @Tags Configuration Fee Rules
// @Accept json
// @Produce json
// @Security BearerAuth
// @Param X-Request-Id header string false "Request ID for tracing"
// @Param X-Idempotency-Key header string false "Idempotency key for safe retries"
// @Param feeRuleId path string true "Fee Rule ID" format(uuid)
// @Param feeRule body dto.UpdateFeeRuleRequest true "Fee rule updates"
// @Success 200 {object} dto.FeeRuleResponse "Successfully updated fee rule"
// @Failure 400 {object} ErrorResponse "Invalid request payload"
// @Failure 401 {object} ErrorResponse "Unauthorized"
// @Failure 403 {object} ErrorResponse "Forbidden"
// @Failure 404 {object} ErrorResponse "Fee rule not found"
// @Failure 409 {object} ErrorResponse "Conflict: duplicate priority or name"
// @Failure 500 {object} ErrorResponse "Internal server error"
// @Router /v1/config/fee-rules/{feeRuleId} [patch]
func (handler *Handler) UpdateFeeRule(fiberCtx *fiber.Ctx) error {
	ctx, span, logger := startHandlerSpan(fiberCtx, "handler.fee_rule.update")
	defer span.End()

	tenantID, err := tenantIDFromContext(ctx)
	if err != nil {
		return unauthorized(ctx, fiberCtx, span, logger, err)
	}

	libHTTP.SetTenantSpanAttribute(span, tenantID)

	feeRuleID, err := parseUUIDParam(fiberCtx, "feeRuleId")
	if err != nil {
		return badRequest(ctx, fiberCtx, span, logger, "invalid fee rule id", err)
	}

	existing, err := handler.query.GetFeeRule(ctx, feeRuleID)
	if err != nil {
		logSpanError(ctx, span, logger, "failed to get fee rule", err)

		if errors.Is(err, fee.ErrFeeRuleNotFound) {
			return writeNotFound(fiberCtx, "fee rule not found")
		}

		return writeServiceError(fiberCtx, err)
	}

	if existing == nil {
		return writeNotFound(fiberCtx, "fee rule not found")
	}

	if err := handler.contextVerifier(ctx, tenantID, existing.ContextID); err != nil {
		return handleOwnershipVerificationError(ctx, fiberCtx, span, logger, err)
	}

	libHTTP.SetHandlerSpanAttributes(span, tenantID, existing.ContextID)

	var payload dto.UpdateFeeRuleRequest
	if err := libHTTP.ParseBodyAndValidate(fiberCtx, &payload); err != nil {
		return badRequest(ctx, fiberCtx, span, logger, "invalid fee rule payload", err)
	}

	var predicates *[]fee.FieldPredicate

	if payload.Predicates != nil {
		converted := dto.ToPredicates(*payload.Predicates)
		predicates = &converted
	}

	result, err := handler.command.UpdateFeeRule(
		ctx,
		feeRuleID,
		payload.Side,
		payload.FeeScheduleID,
		payload.Name,
		payload.Priority,
		predicates,
	)
	if err != nil {
		logSpanError(ctx, span, logger, "failed to update fee rule", err)

		return mapFeeRuleError(fiberCtx, err)
	}

	return libHTTP.Respond(fiberCtx, fiber.StatusOK, dto.FeeRuleToResponse(result))
}

// DeleteFeeRule deletes a fee rule.
//
// @ID deleteFeeRule
// @Summary Delete a fee rule
// @Description Removes a fee rule by ID.
// @Tags Configuration Fee Rules
// @Security BearerAuth
// @Param X-Request-Id header string false "Request ID for tracing"
// @Param feeRuleId path string true "Fee Rule ID" format(uuid)
// @Success 204 "Fee rule successfully deleted"
// @Failure 400 {object} ErrorResponse "Invalid fee rule ID format"
// @Failure 401 {object} ErrorResponse "Unauthorized"
// @Failure 403 {object} ErrorResponse "Forbidden"
// @Failure 404 {object} ErrorResponse "Fee rule not found"
// @Failure 500 {object} ErrorResponse "Internal server error"
// @Router /v1/config/fee-rules/{feeRuleId} [delete]
func (handler *Handler) DeleteFeeRule(fiberCtx *fiber.Ctx) error {
	ctx, span, logger := startHandlerSpan(fiberCtx, "handler.fee_rule.delete")
	defer span.End()

	tenantID, err := tenantIDFromContext(ctx)
	if err != nil {
		return unauthorized(ctx, fiberCtx, span, logger, err)
	}

	libHTTP.SetTenantSpanAttribute(span, tenantID)

	feeRuleID, err := parseUUIDParam(fiberCtx, "feeRuleId")
	if err != nil {
		return badRequest(ctx, fiberCtx, span, logger, "invalid fee rule id", err)
	}

	existing, err := handler.query.GetFeeRule(ctx, feeRuleID)
	if err != nil {
		logSpanError(ctx, span, logger, "failed to get fee rule", err)

		if errors.Is(err, fee.ErrFeeRuleNotFound) {
			return writeNotFound(fiberCtx, "fee rule not found")
		}

		return writeServiceError(fiberCtx, err)
	}

	if existing == nil {
		return writeNotFound(fiberCtx, "fee rule not found")
	}

	if err := handler.contextVerifier(ctx, tenantID, existing.ContextID); err != nil {
		return handleOwnershipVerificationError(ctx, fiberCtx, span, logger, err)
	}

	libHTTP.SetHandlerSpanAttributes(span, tenantID, existing.ContextID)

	if err := handler.command.DeleteFeeRule(ctx, feeRuleID); err != nil {
		logSpanError(ctx, span, logger, "failed to delete fee rule", err)

		if errors.Is(err, fee.ErrFeeRuleNotFound) {
			return writeNotFound(fiberCtx, "fee rule not found")
		}

		return writeServiceError(fiberCtx, err)
	}

	return libHTTP.RespondStatus(fiberCtx, fiber.StatusNoContent)
}

// mapFeeRuleError maps fee rule domain and constraint errors to HTTP responses.
func mapFeeRuleError(fiberCtx *fiber.Ctx, err error) error {
	switch {
	case errors.Is(err, fee.ErrFeeRuleNotFound):
		return writeNotFound(fiberCtx, "fee rule not found")
	case errors.Is(err, command.ErrDuplicateFeeRulePriority):
		return libHTTP.RespondError(fiberCtx, fiber.StatusConflict, "duplicate_priority", err.Error())
	case errors.Is(err, command.ErrDuplicateFeeRuleName):
		return libHTTP.RespondError(fiberCtx, fiber.StatusConflict, "duplicate_name", err.Error())
	case errors.Is(err, fee.ErrFeeScheduleNotFound):
		return writeNotFound(fiberCtx, "fee schedule not found")
	case isFeeRuleClientError(err):
		return libHTTP.RespondError(fiberCtx, fiber.StatusBadRequest, "invalid_request", err.Error())
	default:
		return writeServiceError(fiberCtx, err)
	}
}

// isFeeRuleClientError returns true if the error is a client-side validation error.
func isFeeRuleClientError(err error) bool {
	clientErrors := []error{
		fee.ErrFeeRuleNameRequired,
		fee.ErrFeeRuleNameTooLong,
		fee.ErrFeeRuleScheduleIDRequired,
		fee.ErrFeeRuleContextIDRequired,
		fee.ErrFeeRulePriorityNegative,
		fee.ErrFeeRuleTooManyPredicates,
		fee.ErrInvalidMatchingSide,
		fee.ErrInvalidPredicateOperator,
		fee.ErrPredicateFieldRequired,
		fee.ErrPredicateValueRequired,
		fee.ErrPredicateValuesRequired,
	}
	for _, safeErr := range clientErrors {
		if errors.Is(err, safeErr) {
			return true
		}
	}

	return false
}
