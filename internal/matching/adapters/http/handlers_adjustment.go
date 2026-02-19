// Package http provides HTTP handlers for the matching domain.
package http

import (
	"context"
	"errors"
	"fmt"

	"github.com/gofiber/fiber/v2"
	"github.com/shopspring/decimal"
	"go.opentelemetry.io/otel/trace"

	libLog "github.com/LerianStudio/lib-uncommons/v2/uncommons/log"
	libHTTP "github.com/LerianStudio/lib-uncommons/v2/uncommons/net/http"
	libOpentelemetry "github.com/LerianStudio/lib-uncommons/v2/uncommons/opentelemetry"

	"github.com/LerianStudio/matcher/internal/auth"
	"github.com/LerianStudio/matcher/internal/matching/adapters/http/dto"
	"github.com/LerianStudio/matcher/internal/matching/services/command"
)

// CreateAdjustment creates a balancing journal entry to resolve variance.
// @Summary Create an adjustment
// @Description Creates a balancing journal entry (e.g., bank fee, FX difference) to resolve variance between matched transactions or on a single transaction.
// @ID createAdjustment
// @Tags Matching
// @Accept json
// @Produce json
// @Security BearerAuth
// @Param X-Request-Id header string false "Request ID for tracing"
// @Param contextId query string true "Context ID" format(uuid)
// @Param request body CreateAdjustmentRequest true "Adjustment payload"
// @Success 201 {object} AdjustmentResponse
// @Failure 400 {object} libHTTP.ErrorResponse "Invalid request payload"
// @Failure 401 {object} libHTTP.ErrorResponse "Unauthorized"
// @Failure 403 {object} libHTTP.ErrorResponse "Forbidden"
// @Failure 404 {object} libHTTP.ErrorResponse "Context not found"
// @Failure 500 {object} libHTTP.ErrorResponse "Internal server error"
// @Router /v1/matching/adjustments [post]
func (handler *Handler) CreateAdjustment(fiberCtx *fiber.Ctx) error {
	ctx, span, logger := startHandlerSpan(fiberCtx, "handler.matching.create_adjustment")
	defer span.End()

	contextID, tenantID, err := libHTTP.ParseAndVerifyResourceScopedID(
		fiberCtx,
		"contextId",
		libHTTP.IDLocationQuery,
		handler.resourceContextVerifier,
		auth.GetTenantID,
		libHTTP.ErrMissingContextID,
		libHTTP.ErrInvalidContextID,
		libHTTP.ErrContextAccessDenied,
		"context",
	)
	if shouldReturn, returnErr := handleContextQueryVerificationError(ctx, fiberCtx, span, logger, err); shouldReturn {
		return returnErr
	}

	libHTTP.SetHandlerSpanAttributes(span, tenantID, contextID)

	var payload CreateAdjustmentRequest
	if err := libHTTP.ParseBodyAndValidate(fiberCtx, &payload); err != nil {
		return badRequest(ctx, fiberCtx, span, logger, "invalid adjustment payload", err)
	}

	amount := decimal.Zero

	if payload.Amount != "" {
		parsedAmount, err := decimal.NewFromString(payload.Amount)
		if err != nil {
			return badRequest(ctx, fiberCtx, span, logger, "invalid amount format", err)
		}

		amount = parsedAmount
	}

	matchGroupID, err := parseOptionalUUID(payload.MatchGroupID)
	if err != nil {
		return badRequest(ctx, fiberCtx, span, logger, "invalid match_group_id", err)
	}

	transactionID, err := parseOptionalUUID(payload.TransactionID)
	if err != nil {
		return badRequest(ctx, fiberCtx, span, logger, "invalid transaction_id", err)
	}

	createdBy := getUserFromRequest(fiberCtx)
	if createdBy == "" {
		createdBy = "system"
	}

	adjustment, err := handler.command.CreateAdjustment(ctx, command.CreateAdjustmentInput{
		TenantID:      tenantID,
		ContextID:     contextID,
		MatchGroupID:  matchGroupID,
		TransactionID: transactionID,
		Type:          payload.Type,
		Direction:     payload.Direction,
		Amount:        amount,
		Currency:      payload.Currency,
		Description:   payload.Description,
		Reason:        payload.Reason,
		CreatedBy:     createdBy,
	})
	if err != nil {
		return handleAdjustmentError(ctx, fiberCtx, span, logger, err)
	}

	adjResp := dto.AdjustmentToResponse(adjustment)
	if writeErr := libHTTP.Respond(fiberCtx, fiber.StatusCreated, AdjustmentResponse{Adjustment: &adjResp}); writeErr != nil {
		return fmt.Errorf("write created response: %w", writeErr)
	}

	return nil
}

func getUserFromRequest(fiberCtx *fiber.Ctx) string {
	return auth.GetUserID(fiberCtx.UserContext())
}

// adjustmentNotFoundErrors maps not-found errors to their messages.
var adjustmentNotFoundErrors = map[error]string{
	command.ErrAdjustmentContextNotFound:     "context not found",
	command.ErrAdjustmentMatchGroupNotFound:  "match group not found",
	command.ErrAdjustmentTransactionNotFound: "transaction not found",
}

// adjustmentBadRequestErrors maps bad-request errors to their messages.
var adjustmentBadRequestErrors = map[error]string{
	command.ErrAdjustmentTypeInvalid:         "invalid adjustment type",
	command.ErrAdjustmentDirectionInvalid:    "invalid adjustment direction",
	command.ErrAdjustmentAmountNotPositive:   "amount must be positive",
	command.ErrAdjustmentTenantIDRequired:    "tenant id is required",
	command.ErrAdjustmentContextIDRequired:   "context id is required",
	command.ErrAdjustmentTargetRequired:      "match_group_id or transaction_id is required",
	command.ErrAdjustmentTypeRequired:        "type is required",
	command.ErrAdjustmentDirectionRequired:   "direction is required",
	command.ErrAdjustmentCurrencyRequired:    "currency is required",
	command.ErrAdjustmentDescriptionRequired: "description is required",
	command.ErrAdjustmentReasonRequired:      "reason is required",
	command.ErrAdjustmentCreatedByRequired:   "created_by is required",
}

func handleAdjustmentError(
	ctx context.Context,
	fiberCtx *fiber.Ctx,
	span trace.Span,
	logger libLog.Logger,
	err error,
) error {
	// Check not-found errors
	for sentinel, msg := range adjustmentNotFoundErrors {
		if errors.Is(err, sentinel) {
			libOpentelemetry.HandleSpanError(span, msg, err)

			return writeNotFound(fiberCtx, msg)
		}
	}

	// Check bad-request errors
	for sentinel, msg := range adjustmentBadRequestErrors {
		if errors.Is(err, sentinel) {
			return badRequest(ctx, fiberCtx, span, logger, msg, err)
		}
	}

	// Check forbidden error
	if errors.Is(err, command.ErrAdjustmentContextNotActive) {
		return libHTTP.RespondError(fiberCtx, fiber.StatusForbidden, "context_not_active", "context is not active")
	}

	return writeServiceError(ctx, fiberCtx, span, logger, "failed to create adjustment", err)
}
