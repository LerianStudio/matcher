// Package http provides HTTP handlers for the matching domain.
package http

import (
	"context"
	"errors"
	"fmt"

	"github.com/gofiber/fiber/v2"
	"github.com/google/uuid"
	"go.opentelemetry.io/otel/trace"

	libLog "github.com/LerianStudio/lib-commons/v4/commons/log"
	libHTTP "github.com/LerianStudio/lib-commons/v4/commons/net/http"

	"github.com/LerianStudio/matcher/internal/auth"
	"github.com/LerianStudio/matcher/internal/matching/adapters/http/dto"
	"github.com/LerianStudio/matcher/internal/matching/services/command"
)

// CreateManualMatch creates a manual match group for selected transactions.
// @Summary Create a manual match
// @Description Manually links multiple transactions (at least 2) into a match group with 100% confidence. Transactions must be UNMATCHED and belong to the specified context.
// @ID createManualMatch
// @Tags Matching
// @Accept json
// @Produce json
// @Security BearerAuth
// @Param X-Request-Id header string false "Request ID for tracing"
// @Param contextId query string true "Context ID" format(uuid)
// @Param request body CreateManualMatchRequest true "Manual match payload"
// @Success 201 {object} ManualMatchResponse
// @Failure 400 {object} libHTTP.ErrorResponse "Invalid request payload"
// @Failure 401 {object} libHTTP.ErrorResponse "Unauthorized"
// @Failure 403 {object} libHTTP.ErrorResponse "Forbidden"
// @Failure 404 {object} libHTTP.ErrorResponse "Transaction not found"
// @Failure 500 {object} libHTTP.ErrorResponse "Internal server error"
// @Router /v1/matching/manual [post]
func (handler *Handler) CreateManualMatch(fiberCtx *fiber.Ctx) error {
	ctx, span, logger := startHandlerSpan(fiberCtx, "handler.matching.create_manual_match")
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

	var payload CreateManualMatchRequest
	if err := libHTTP.ParseBodyAndValidate(fiberCtx, &payload); err != nil {
		return badRequest(ctx, fiberCtx, span, logger, "invalid manual match payload", err)
	}

	transactionIDs := make([]uuid.UUID, 0, len(payload.TransactionIDs))
	for _, idStr := range payload.TransactionIDs {
		id, parseErr := uuid.Parse(idStr)
		if parseErr != nil {
			return badRequest(ctx, fiberCtx, span, logger, "invalid transaction id: "+idStr, parseErr)
		}

		transactionIDs = append(transactionIDs, id)
	}

	group, err := handler.command.ManualMatch(ctx, command.ManualMatchInput{
		TenantID:       tenantID,
		ContextID:      contextID,
		TransactionIDs: transactionIDs,
		Notes:          payload.Notes,
	})
	if err != nil {
		return mapManualMatchErrorToResponse(ctx, fiberCtx, span, logger, err)
	}

	groupResp := dto.MatchGroupToResponse(group)
	if writeErr := libHTTP.Respond(fiberCtx, fiber.StatusCreated, ManualMatchResponse{MatchGroup: &groupResp}); writeErr != nil {
		return fmt.Errorf("write created response: %w", writeErr)
	}

	return nil
}

// mapManualMatchErrorToResponse maps domain errors to HTTP responses for manual match operations.
func mapManualMatchErrorToResponse(
	ctx context.Context,
	fiberCtx *fiber.Ctx,
	span trace.Span,
	logger libLog.Logger,
	err error,
) error {
	switch {
	case errors.Is(err, command.ErrContextNotFound):
		logSpanError(ctx, span, logger, "context not found", err)

		return writeNotFound(fiberCtx, "context not found")
	case errors.Is(err, command.ErrContextNotActive):
		logSpanError(ctx, span, logger, "context not active", err)

		return libHTTP.RespondError(fiberCtx, fiber.StatusForbidden, "context_not_active", "context is not active")
	case errors.Is(err, command.ErrTransactionNotFound):
		logSpanError(ctx, span, logger, "transaction not found", err)

		return writeNotFound(fiberCtx, "one or more transactions not found")
	case errors.Is(err, command.ErrTransactionNotUnmatched):
		return badRequest(ctx, fiberCtx, span, logger, "one or more transactions are not unmatched", err)
	case errors.Is(err, command.ErrMinimumTransactionsRequired):
		return badRequest(ctx, fiberCtx, span, logger, "at least two transactions are required", err)
	case errors.Is(err, command.ErrDuplicateTransactionIDs):
		return badRequest(ctx, fiberCtx, span, logger, "duplicate transaction IDs provided", err)
	case errors.Is(err, command.ErrManualMatchSourcesNotDiverse):
		return badRequest(ctx, fiberCtx, span, logger, "transactions must come from at least two different sources", err)
	default:
		return writeServiceError(ctx, fiberCtx, span, logger, "failed to create manual match", err)
	}
}
