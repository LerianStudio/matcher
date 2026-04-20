// Package http provides HTTP handlers for the matching domain.
package http

import (
	"context"
	"errors"
	"fmt"

	"github.com/gofiber/fiber/v2"
	"github.com/shopspring/decimal"
	"go.opentelemetry.io/otel/trace"

	libLog "github.com/LerianStudio/lib-commons/v5/commons/log"
	libHTTP "github.com/LerianStudio/lib-commons/v5/commons/net/http"
	libOpentelemetry "github.com/LerianStudio/lib-commons/v5/commons/opentelemetry"

	"github.com/LerianStudio/matcher/internal/auth"
	"github.com/LerianStudio/matcher/internal/matching/adapters/http/dto"
	"github.com/LerianStudio/matcher/internal/matching/services/command"
	sharedhttp "github.com/LerianStudio/matcher/internal/shared/adapters/http"
)

var _ = sharedhttp.ErrorResponse{}

// errAdjustmentUnauthenticated is returned when CreateAdjustment is invoked
// without an authenticated user in the request context. The HTTP surface
// fails closed rather than silently attributing the write to "system".
var errAdjustmentUnauthenticated = errors.New("unauthenticated adjustment request")

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
// @Failure 400 {object} sharedhttp.ErrorResponse "Invalid request payload"
// @Failure 401 {object} sharedhttp.ErrorResponse "Unauthorized"
// @Failure 403 {object} sharedhttp.ErrorResponse "Forbidden"
// @Failure 404 {object} sharedhttp.ErrorResponse "Context not found"
// @Failure 500 {object} sharedhttp.ErrorResponse "Internal server error"
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
	if shouldReturn, returnErr := handler.handleContextQueryVerificationError(ctx, fiberCtx, span, logger, err); shouldReturn {
		return returnErr
	}

	libHTTP.SetHandlerSpanAttributes(span, tenantID, contextID)

	var payload CreateAdjustmentRequest
	if err := libHTTP.ParseBodyAndValidate(fiberCtx, &payload); err != nil {
		return handler.badRequest(ctx, fiberCtx, span, logger, "invalid adjustment payload", err)
	}

	amount := decimal.Zero

	if payload.Amount != "" {
		parsedAmount, err := decimal.NewFromString(payload.Amount)
		if err != nil {
			return handler.badRequest(ctx, fiberCtx, span, logger, "invalid amount format", err)
		}

		amount = parsedAmount
	}

	matchGroupID, err := parseOptionalUUID(payload.MatchGroupID)
	if err != nil {
		return handler.badRequest(ctx, fiberCtx, span, logger, "invalid match_group_id", err)
	}

	transactionID, err := parseOptionalUUID(payload.TransactionID)
	if err != nil {
		return handler.badRequest(ctx, fiberCtx, span, logger, "invalid transaction_id", err)
	}

	createdBy := getUserFromRequest(fiberCtx)
	if createdBy == "" {
		// Adjustments reach the HTTP surface only through authenticated
		// tenant traffic; a missing user ID means auth middleware is
		// misconfigured or bypassed. Fail closed — never attribute the
		// write to a generic "system" principal. Any genuinely
		// background-initiated adjustment must go through the cross-context
		// gateway (internal/shared/adapters/cross/exception_matching_gateway.go),
		// which carries its own actor.
		handler.logSpanError(ctx, span, logger, "adjustment rejected: missing authenticated user", errAdjustmentUnauthenticated)

		return respondError(fiberCtx, fiber.StatusUnauthorized, "unauthorized", "authenticated user required")
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
		return handler.handleAdjustmentError(ctx, fiberCtx, span, logger, err)
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

func (handler *Handler) handleAdjustmentError(
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
			return handler.badRequest(ctx, fiberCtx, span, logger, msg, err)
		}
	}

	// Check forbidden error
	if errors.Is(err, command.ErrAdjustmentContextNotActive) {
		return respondError(fiberCtx, fiber.StatusForbidden, "context_not_active", "context is not active")
	}

	return handler.writeServiceError(ctx, fiberCtx, span, logger, "failed to create adjustment", err)
}
