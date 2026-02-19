package http

import (
	"database/sql"
	"errors"

	"github.com/gofiber/fiber/v2"

	libHTTP "github.com/LerianStudio/lib-uncommons/v2/uncommons/net/http"

	"github.com/LerianStudio/matcher/internal/auth"
	"github.com/LerianStudio/matcher/internal/configuration/adapters/http/dto"
	"github.com/LerianStudio/matcher/internal/configuration/domain/entities"
	"github.com/LerianStudio/matcher/internal/configuration/services/command"
	"github.com/LerianStudio/matcher/internal/configuration/services/query"
)

// CreateSchedule creates a reconciliation schedule for a context.
//
// @ID createSchedule
// @Summary Create a reconciliation schedule
// @Description Creates a new cron-based schedule for automated matching.
// @Tags Configuration Schedules
// @Accept json
// @Produce json
// @Security BearerAuth
// @Param X-Request-Id header string false "Request ID for tracing"
// @Param contextId path string true "Context ID" format(uuid)
// @Param schedule body dto.CreateScheduleRequest true "Schedule creation payload"
// @Success 201 {object} dto.ScheduleResponse "Successfully created schedule"
// @Failure 400 {object} ErrorResponse "Invalid request payload"
// @Failure 401 {object} ErrorResponse "Unauthorized"
// @Failure 403 {object} ErrorResponse "Forbidden"
// @Failure 404 {object} ErrorResponse "Context not found"
// @Failure 409 {object} ErrorResponse "Conflict: duplicate resource or idempotency key in progress"
// @Failure 500 {object} ErrorResponse "Internal server error"
// @Router /v1/config/contexts/{contextId}/schedules [post]
func (handler *Handler) CreateSchedule(fiberCtx *fiber.Ctx) error {
	ctx, span, logger := startHandlerSpan(fiberCtx, "handler.schedule.create")
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

	var req dto.CreateScheduleRequest
	if err := libHTTP.ParseBodyAndValidate(fiberCtx, &req); err != nil {
		return badRequest(ctx, fiberCtx, span, logger, "invalid schedule payload", err)
	}

	result, err := handler.command.CreateSchedule(ctx, contextID, req.ToDomainInput())
	if err != nil {
		logSpanError(ctx, span, logger, "failed to create schedule", err)

		if isScheduleClientError(err) {
			return libHTTP.RespondError(fiberCtx, fiber.StatusBadRequest, "invalid_request", err.Error())
		}

		if errors.Is(err, sql.ErrNoRows) {
			return writeNotFound(fiberCtx, "context not found")
		}

		return writeServiceError(fiberCtx, err)
	}

	return libHTTP.Respond(fiberCtx, fiber.StatusCreated, dto.ScheduleToResponse(result))
}

// ListSchedules lists reconciliation schedules for a context.
//
// @ID listSchedules
// @Summary List reconciliation schedules
// @Description Returns all schedules for a reconciliation context.
// @Tags Configuration Schedules
// @Produce json
// @Security BearerAuth
// @Param X-Request-Id header string false "Request ID for tracing"
// @Param contextId path string true "Context ID" format(uuid)
// @Success 200 {array} dto.ScheduleResponse "List of schedules"
// @Failure 400 {object} ErrorResponse "Invalid context ID format"
// @Failure 401 {object} ErrorResponse "Unauthorized"
// @Failure 403 {object} ErrorResponse "Forbidden"
// @Failure 404 {object} ErrorResponse "Context not found"
// @Failure 500 {object} ErrorResponse "Internal server error"
// @Router /v1/config/contexts/{contextId}/schedules [get]
func (handler *Handler) ListSchedules(fiberCtx *fiber.Ctx) error {
	ctx, span, logger := startHandlerSpan(fiberCtx, "handler.schedule.list")
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

	result, err := handler.query.ListSchedules(ctx, contextID)
	if err != nil {
		logSpanError(ctx, span, logger, "failed to list schedules", err)
		return writeServiceError(fiberCtx, err)
	}

	return libHTTP.Respond(fiberCtx, fiber.StatusOK, dto.SchedulesToResponse(result))
}

// GetSchedule retrieves a reconciliation schedule.
//
// @ID getSchedule
// @Summary Get a reconciliation schedule
// @Description Returns a reconciliation schedule by ID.
// @Tags Configuration Schedules
// @Produce json
// @Security BearerAuth
// @Param X-Request-Id header string false "Request ID for tracing"
// @Param contextId path string true "Context ID" format(uuid)
// @Param scheduleId path string true "Schedule ID" format(uuid)
// @Success 200 {object} dto.ScheduleResponse "Successfully retrieved schedule"
// @Failure 400 {object} ErrorResponse "Invalid schedule ID format"
// @Failure 401 {object} ErrorResponse "Unauthorized"
// @Failure 403 {object} ErrorResponse "Forbidden"
// @Failure 404 {object} ErrorResponse "Schedule not found"
// @Failure 500 {object} ErrorResponse "Internal server error"
// @Router /v1/config/contexts/{contextId}/schedules/{scheduleId} [get]
func (handler *Handler) GetSchedule(fiberCtx *fiber.Ctx) error {
	ctx, span, logger := startHandlerSpan(fiberCtx, "handler.schedule.get")
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

	scheduleID, err := parseUUIDParam(fiberCtx, "scheduleId")
	if err != nil {
		return badRequest(ctx, fiberCtx, span, logger, "invalid schedule id", err)
	}

	result, err := handler.query.GetSchedule(ctx, scheduleID)
	if err != nil {
		logSpanError(ctx, span, logger, "failed to get schedule", err)

		if errors.Is(err, query.ErrScheduleNotFound) || errors.Is(err, sql.ErrNoRows) {
			return writeNotFound(fiberCtx, "schedule not found")
		}

		return writeServiceError(fiberCtx, err)
	}

	// Verify schedule belongs to this context
	if result.ContextID != contextID {
		return writeNotFound(fiberCtx, "schedule not found")
	}

	return libHTTP.Respond(fiberCtx, fiber.StatusOK, dto.ScheduleToResponse(result))
}

// UpdateSchedule updates a reconciliation schedule.
//
// @ID updateSchedule
// @Summary Update a reconciliation schedule
// @Description Updates fields on a reconciliation schedule.
// @Tags Configuration Schedules
// @Accept json
// @Produce json
// @Security BearerAuth
// @Param X-Request-Id header string false "Request ID for tracing"
// @Param contextId path string true "Context ID" format(uuid)
// @Param scheduleId path string true "Schedule ID" format(uuid)
// @Param schedule body dto.UpdateScheduleRequest true "Schedule updates"
// @Success 200 {object} dto.ScheduleResponse "Successfully updated schedule"
// @Failure 400 {object} ErrorResponse "Invalid request payload"
// @Failure 401 {object} ErrorResponse "Unauthorized"
// @Failure 403 {object} ErrorResponse "Forbidden"
// @Failure 404 {object} ErrorResponse "Schedule not found"
// @Failure 409 {object} ErrorResponse "Conflict: duplicate resource or idempotency key in progress"
// @Failure 500 {object} ErrorResponse "Internal server error"
// @Router /v1/config/contexts/{contextId}/schedules/{scheduleId} [patch]
func (handler *Handler) UpdateSchedule(fiberCtx *fiber.Ctx) error {
	ctx, span, logger := startHandlerSpan(fiberCtx, "handler.schedule.update")
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

	scheduleID, err := parseUUIDParam(fiberCtx, "scheduleId")
	if err != nil {
		return badRequest(ctx, fiberCtx, span, logger, "invalid schedule id", err)
	}

	var req dto.UpdateScheduleRequest
	if err := libHTTP.ParseBodyAndValidate(fiberCtx, &req); err != nil {
		return badRequest(ctx, fiberCtx, span, logger, "invalid schedule payload", err)
	}

	result, err := handler.command.UpdateSchedule(ctx, contextID, scheduleID, req.ToDomainInput())
	if err != nil {
		logSpanError(ctx, span, logger, "failed to update schedule", err)

		if isScheduleClientError(err) {
			return libHTTP.RespondError(fiberCtx, fiber.StatusBadRequest, "invalid_request", err.Error())
		}

		if errors.Is(err, command.ErrScheduleNotFound) ||
			errors.Is(err, command.ErrScheduleContextMismatch) ||
			errors.Is(err, sql.ErrNoRows) {
			return writeNotFound(fiberCtx, "schedule not found")
		}

		return writeServiceError(fiberCtx, err)
	}

	return libHTTP.Respond(fiberCtx, fiber.StatusOK, dto.ScheduleToResponse(result))
}

// DeleteSchedule deletes a reconciliation schedule.
//
// @ID deleteSchedule
// @Summary Delete a reconciliation schedule
// @Description Removes a reconciliation schedule by ID.
// @Tags Configuration Schedules
// @Security BearerAuth
// @Param X-Request-Id header string false "Request ID for tracing"
// @Param contextId path string true "Context ID" format(uuid)
// @Param scheduleId path string true "Schedule ID" format(uuid)
// @Success 204 "Schedule successfully deleted"
// @Failure 400 {object} ErrorResponse "Invalid schedule ID format"
// @Failure 401 {object} ErrorResponse "Unauthorized"
// @Failure 403 {object} ErrorResponse "Forbidden"
// @Failure 404 {object} ErrorResponse "Schedule not found"
// @Failure 500 {object} ErrorResponse "Internal server error"
// @Router /v1/config/contexts/{contextId}/schedules/{scheduleId} [delete]
func (handler *Handler) DeleteSchedule(fiberCtx *fiber.Ctx) error {
	ctx, span, logger := startHandlerSpan(fiberCtx, "handler.schedule.delete")
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

	scheduleID, err := parseUUIDParam(fiberCtx, "scheduleId")
	if err != nil {
		return badRequest(ctx, fiberCtx, span, logger, "invalid schedule id", err)
	}

	if err := handler.command.DeleteSchedule(ctx, contextID, scheduleID); err != nil {
		logSpanError(ctx, span, logger, "failed to delete schedule", err)

		if errors.Is(err, command.ErrScheduleNotFound) ||
			errors.Is(err, command.ErrScheduleContextMismatch) ||
			errors.Is(err, sql.ErrNoRows) {
			return writeNotFound(fiberCtx, "schedule not found")
		}

		return writeServiceError(fiberCtx, err)
	}

	return libHTTP.RespondStatus(fiberCtx, fiber.StatusNoContent)
}

// isScheduleClientError returns true for schedule-related client errors.
func isScheduleClientError(err error) bool {
	return errors.Is(err, entities.ErrScheduleContextIDRequired) ||
		errors.Is(err, entities.ErrScheduleCronExpressionRequired) ||
		errors.Is(err, entities.ErrScheduleCronExpressionInvalid)
}
