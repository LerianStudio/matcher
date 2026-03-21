package http

import (
	"errors"
	"fmt"
	"strconv"

	"github.com/gofiber/fiber/v2"
	"github.com/shopspring/decimal"

	libHTTP "github.com/LerianStudio/lib-commons/v4/commons/net/http"

	"github.com/LerianStudio/matcher/internal/configuration/adapters/http/dto"
	"github.com/LerianStudio/matcher/internal/configuration/services/command"
	"github.com/LerianStudio/matcher/internal/shared/constants"
	"github.com/LerianStudio/matcher/internal/shared/domain/fee"
)

var errParseItemStructure = errors.New("item structure parse error")

// CreateFeeSchedule creates a fee schedule.
//
// @ID createFeeSchedule
// @Summary Create a fee schedule
// @Description Creates a new fee schedule for transaction fee calculation.
// @Tags Configuration Fee Schedules
// @Accept json
// @Produce json
// @Security BearerAuth
// @Param X-Request-Id header string false "Request ID for tracing"
// @Param X-Idempotency-Key header string false "Idempotency key for safe retries"
// @Param feeSchedule body dto.CreateFeeScheduleRequest true "Fee schedule creation payload"
// @Success 201 {object} dto.FeeScheduleResponse "Successfully created fee schedule"
// @Failure 400 {object} ErrorResponse "Invalid request payload"
// @Failure 401 {object} ErrorResponse "Unauthorized"
// @Failure 403 {object} ErrorResponse "Forbidden"
// @Failure 409 {object} ErrorResponse "Conflict: duplicate resource or idempotency key in progress"
// @Failure 500 {object} ErrorResponse "Internal server error"
// @Router /v1/fee-schedules [post]
func (handler *Handler) CreateFeeSchedule(fiberCtx *fiber.Ctx) error {
	ctx, span, logger := startHandlerSpan(fiberCtx, "handler.fee_schedule.create")
	defer span.End()

	var payload dto.CreateFeeScheduleRequest
	if err := libHTTP.ParseBodyAndValidate(fiberCtx, &payload); err != nil {
		return badRequest(ctx, fiberCtx, span, logger, "invalid fee schedule payload", err)
	}

	tenantID, err := tenantIDFromContext(ctx)
	if err != nil {
		return unauthorized(ctx, fiberCtx, span, logger, err)
	}

	libHTTP.SetTenantSpanAttribute(span, tenantID)

	items, err := parseFeeScheduleItems(payload.Items)
	if err != nil {
		return badRequest(ctx, fiberCtx, span, logger, "invalid fee schedule items", err)
	}

	result, err := handler.command.CreateFeeSchedule(
		ctx,
		tenantID,
		payload.Name,
		payload.Currency,
		payload.ApplicationOrder,
		payload.RoundingScale,
		payload.RoundingMode,
		items,
	)
	if err != nil {
		logSpanError(ctx, span, logger, "failed to create fee schedule", err)

		if isFeeScheduleClientError(err) {
			return libHTTP.RespondError(fiberCtx, fiber.StatusBadRequest, "invalid_request", err.Error())
		}

		return writeServiceError(fiberCtx, err)
	}

	return libHTTP.Respond(fiberCtx, fiber.StatusCreated, dto.FeeScheduleToResponse(result))
}

// ListFeeSchedules lists fee schedules.
//
// @ID listFeeSchedules
// @Summary List fee schedules
// @Description Returns fee schedules for the tenant. Results may be capped by the "limit" query parameter (default 20, max 200).
// @Tags Configuration Fee Schedules
// @Produce json
// @Security BearerAuth
// @Param X-Request-Id header string false "Request ID for tracing"
// @Param limit query int false "Maximum number of records to return" default(20) minimum(1) maximum(200)
// @Success 200 {array} dto.FeeScheduleResponse "List of fee schedules"
// @Failure 401 {object} ErrorResponse "Unauthorized"
// @Failure 403 {object} ErrorResponse "Forbidden"
// @Failure 500 {object} ErrorResponse "Internal server error"
// @Router /v1/fee-schedules [get]
func (handler *Handler) ListFeeSchedules(fiberCtx *fiber.Ctx) error {
	ctx, span, logger := startHandlerSpan(fiberCtx, "handler.fee_schedule.list")
	defer span.End()

	tenantID, err := tenantIDFromContext(ctx)
	if err != nil {
		return unauthorized(ctx, fiberCtx, span, logger, err)
	}

	libHTTP.SetTenantSpanAttribute(span, tenantID)

	limitStr := fiberCtx.Query("limit", strconv.Itoa(constants.DefaultPaginationLimit))

	limit, parseErr := strconv.Atoi(limitStr)
	if parseErr != nil || limit < 1 {
		limit = constants.DefaultPaginationLimit
	}

	limit = libHTTP.ValidateLimit(limit, constants.DefaultPaginationLimit, constants.MaximumPaginationLimit)

	result, err := handler.query.ListFeeSchedules(ctx, limit)
	if err != nil {
		logSpanError(ctx, span, logger, "failed to list fee schedules", err)
		return writeServiceError(fiberCtx, err)
	}

	if result == nil {
		result = []*fee.FeeSchedule{}
	}

	return libHTTP.Respond(fiberCtx, fiber.StatusOK, dto.FeeSchedulesToResponse(result))
}

// GetFeeSchedule retrieves a fee schedule.
//
// @ID getFeeSchedule
// @Summary Get a fee schedule
// @Description Returns a fee schedule by ID.
// @Tags Configuration Fee Schedules
// @Produce json
// @Security BearerAuth
// @Param X-Request-Id header string false "Request ID for tracing"
// @Param scheduleId path string true "Schedule ID" format(uuid)
// @Success 200 {object} dto.FeeScheduleResponse "Successfully retrieved fee schedule"
// @Failure 400 {object} ErrorResponse "Invalid schedule ID format"
// @Failure 401 {object} ErrorResponse "Unauthorized"
// @Failure 403 {object} ErrorResponse "Forbidden"
// @Failure 404 {object} ErrorResponse "Fee schedule not found"
// @Failure 500 {object} ErrorResponse "Internal server error"
// @Router /v1/fee-schedules/{scheduleId} [get]
func (handler *Handler) GetFeeSchedule(fiberCtx *fiber.Ctx) error {
	ctx, span, logger := startHandlerSpan(fiberCtx, "handler.fee_schedule.get")
	defer span.End()

	tenantID, err := tenantIDFromContext(ctx)
	if err != nil {
		return unauthorized(ctx, fiberCtx, span, logger, err)
	}

	libHTTP.SetTenantSpanAttribute(span, tenantID)

	scheduleID, err := parseUUIDParam(fiberCtx, "scheduleId")
	if err != nil {
		return badRequest(ctx, fiberCtx, span, logger, "invalid schedule id", err)
	}

	result, err := handler.query.GetFeeSchedule(ctx, scheduleID)
	if err != nil {
		logSpanError(ctx, span, logger, "failed to get fee schedule", err)

		if errors.Is(err, fee.ErrFeeScheduleNotFound) {
			return writeNotFound(fiberCtx, "fee schedule not found")
		}

		return writeServiceError(fiberCtx, err)
	}

	return libHTTP.Respond(fiberCtx, fiber.StatusOK, dto.FeeScheduleToResponse(result))
}

// UpdateFeeSchedule updates a fee schedule.
//
// @ID updateFeeSchedule
// @Summary Update a fee schedule
// @Description Updates fields on a fee schedule by ID.
// @Tags Configuration Fee Schedules
// @Accept json
// @Produce json
// @Security BearerAuth
// @Param X-Request-Id header string false "Request ID for tracing"
// @Param X-Idempotency-Key header string false "Idempotency key for safe retries"
// @Param scheduleId path string true "Schedule ID" format(uuid)
// @Param feeSchedule body dto.UpdateFeeScheduleRequest true "Fee schedule updates"
// @Success 200 {object} dto.FeeScheduleResponse "Successfully updated fee schedule"
// @Failure 400 {object} ErrorResponse "Invalid request payload"
// @Failure 401 {object} ErrorResponse "Unauthorized"
// @Failure 403 {object} ErrorResponse "Forbidden"
// @Failure 404 {object} ErrorResponse "Fee schedule not found"
// @Failure 409 {object} ErrorResponse "Conflict: duplicate resource or idempotency key in progress"
// @Failure 500 {object} ErrorResponse "Internal server error"
// @Router /v1/fee-schedules/{scheduleId} [patch]
func (handler *Handler) UpdateFeeSchedule(fiberCtx *fiber.Ctx) error {
	ctx, span, logger := startHandlerSpan(fiberCtx, "handler.fee_schedule.update")
	defer span.End()

	tenantID, err := tenantIDFromContext(ctx)
	if err != nil {
		return unauthorized(ctx, fiberCtx, span, logger, err)
	}

	libHTTP.SetTenantSpanAttribute(span, tenantID)

	scheduleID, err := parseUUIDParam(fiberCtx, "scheduleId")
	if err != nil {
		return badRequest(ctx, fiberCtx, span, logger, "invalid schedule id", err)
	}

	var payload dto.UpdateFeeScheduleRequest
	if err := libHTTP.ParseBodyAndValidate(fiberCtx, &payload); err != nil {
		return badRequest(ctx, fiberCtx, span, logger, "invalid fee schedule payload", err)
	}

	result, err := handler.command.UpdateFeeSchedule(
		ctx,
		scheduleID,
		payload.Name,
		payload.ApplicationOrder,
		payload.RoundingScale,
		payload.RoundingMode,
	)
	if err != nil {
		logSpanError(ctx, span, logger, "failed to update fee schedule", err)

		if errors.Is(err, fee.ErrFeeScheduleNotFound) {
			return writeNotFound(fiberCtx, "fee schedule not found")
		}

		if isFeeScheduleClientError(err) {
			return libHTTP.RespondError(fiberCtx, fiber.StatusBadRequest, "invalid_request", err.Error())
		}

		return writeServiceError(fiberCtx, err)
	}

	return libHTTP.Respond(fiberCtx, fiber.StatusOK, dto.FeeScheduleToResponse(result))
}

// DeleteFeeSchedule deletes a fee schedule.
//
// @ID deleteFeeSchedule
// @Summary Delete a fee schedule
// @Description Removes a fee schedule by ID.
// @Tags Configuration Fee Schedules
// @Security BearerAuth
// @Param X-Request-Id header string false "Request ID for tracing"
// @Param scheduleId path string true "Schedule ID" format(uuid)
// @Success 204 "Fee schedule successfully deleted"
// @Failure 400 {object} ErrorResponse "Invalid schedule ID format"
// @Failure 401 {object} ErrorResponse "Unauthorized"
// @Failure 403 {object} ErrorResponse "Forbidden"
// @Failure 404 {object} ErrorResponse "Fee schedule not found"
// @Failure 500 {object} ErrorResponse "Internal server error"
// @Router /v1/fee-schedules/{scheduleId} [delete]
func (handler *Handler) DeleteFeeSchedule(fiberCtx *fiber.Ctx) error {
	ctx, span, logger := startHandlerSpan(fiberCtx, "handler.fee_schedule.delete")
	defer span.End()

	tenantID, err := tenantIDFromContext(ctx)
	if err != nil {
		return unauthorized(ctx, fiberCtx, span, logger, err)
	}

	libHTTP.SetTenantSpanAttribute(span, tenantID)

	scheduleID, err := parseUUIDParam(fiberCtx, "scheduleId")
	if err != nil {
		return badRequest(ctx, fiberCtx, span, logger, "invalid schedule id", err)
	}

	if err := handler.command.DeleteFeeSchedule(ctx, scheduleID); err != nil {
		logSpanError(ctx, span, logger, "failed to delete fee schedule", err)

		if errors.Is(err, fee.ErrFeeScheduleNotFound) {
			return writeNotFound(fiberCtx, "fee schedule not found")
		}

		return writeServiceError(fiberCtx, err)
	}

	return libHTTP.RespondStatus(fiberCtx, fiber.StatusNoContent)
}

// SimulateFeeSchedule simulates fee calculation for a schedule.
//
// @ID simulateFeeSchedule
// @Summary Simulate fee calculation
// @Description Calculates fees for a given gross amount using a specific fee schedule.
// @Tags Configuration Fee Schedules
// @Accept json
// @Produce json
// @Security BearerAuth
// @Param X-Request-Id header string false "Request ID for tracing"
// @Param scheduleId path string true "Schedule ID" format(uuid)
// @Param simulate body dto.SimulateFeeRequest true "Simulation parameters"
// @Success 200 {object} dto.SimulateFeeResponse "Simulation result"
// @Failure 400 {object} ErrorResponse "Invalid request payload"
// @Failure 401 {object} ErrorResponse "Unauthorized"
// @Failure 403 {object} ErrorResponse "Forbidden"
// @Failure 404 {object} ErrorResponse "Fee schedule not found"
// @Failure 500 {object} ErrorResponse "Internal server error"
// @Router /v1/fee-schedules/{scheduleId}/simulate [post]
func (handler *Handler) SimulateFeeSchedule(fiberCtx *fiber.Ctx) error {
	ctx, span, logger := startHandlerSpan(fiberCtx, "handler.fee_schedule.simulate")
	defer span.End()

	tenantID, err := tenantIDFromContext(ctx)
	if err != nil {
		return unauthorized(ctx, fiberCtx, span, logger, err)
	}

	libHTTP.SetTenantSpanAttribute(span, tenantID)

	scheduleID, err := parseUUIDParam(fiberCtx, "scheduleId")
	if err != nil {
		return badRequest(ctx, fiberCtx, span, logger, "invalid schedule id", err)
	}

	var payload dto.SimulateFeeRequest
	if err := libHTTP.ParseBodyAndValidate(fiberCtx, &payload); err != nil {
		return badRequest(ctx, fiberCtx, span, logger, "invalid simulation payload", err)
	}

	grossAmount, err := decimal.NewFromString(payload.GrossAmount)
	if err != nil {
		return badRequest(ctx, fiberCtx, span, logger, "invalid gross amount", err)
	}

	schedule, err := handler.query.GetFeeSchedule(ctx, scheduleID)
	if err != nil {
		logSpanError(ctx, span, logger, "failed to get fee schedule for simulation", err)

		if errors.Is(err, fee.ErrFeeScheduleNotFound) {
			return writeNotFound(fiberCtx, "fee schedule not found")
		}

		return writeServiceError(fiberCtx, err)
	}

	gross := fee.Money{Amount: grossAmount, Currency: payload.Currency}

	breakdown, err := fee.CalculateSchedule(ctx, gross, schedule)
	if err != nil {
		logSpanError(ctx, span, logger, "failed to simulate fee schedule", err)

		if isFeeScheduleClientError(err) {
			return libHTTP.RespondError(fiberCtx, fiber.StatusBadRequest, "invalid_request", err.Error())
		}

		return writeServiceError(fiberCtx, err)
	}

	return libHTTP.Respond(fiberCtx, fiber.StatusOK, dto.FeeBreakdownToSimulateResponse(grossAmount, payload.Currency, breakdown))
}

func parseFeeScheduleItems(items []dto.CreateFeeScheduleItemRequest) ([]fee.FeeScheduleItemInput, error) {
	result := make([]fee.FeeScheduleItemInput, 0, len(items))

	for i, item := range items {
		structure, err := command.ParseFeeStructureFromRequest(item.StructureType, item.Structure)
		if err != nil {
			wrappedErr := fmt.Errorf("item[%d]: %w", i, err)
			return nil, errors.Join(wrappedErr, errParseItemStructure)
		}

		result = append(result, fee.FeeScheduleItemInput{
			Name:      item.Name,
			Priority:  item.Priority,
			Structure: structure,
		})
	}

	return result, nil
}

func isFeeScheduleClientError(err error) bool {
	clientErrors := []error{
		fee.ErrScheduleNameRequired,
		fee.ErrScheduleNameTooLong,
		fee.ErrScheduleItemsRequired,
		fee.ErrDuplicateItemPriority,
		fee.ErrInvalidApplicationOrder,
		fee.ErrInvalidRoundingScale,
		fee.ErrInvalidRoundingMode,
		fee.ErrItemNameRequired,
		fee.ErrNilFeeStructure,
		fee.ErrInvalidCurrency,
		fee.ErrCurrencyMismatch,
		fee.ErrNegativeAmount,
		fee.ErrInvalidPercentageRate,
		fee.ErrInvalidTieredDefinition,
	}
	for _, safeErr := range clientErrors {
		if errors.Is(err, safeErr) {
			return true
		}
	}

	return false
}
