// Package http provides HTTP handlers for the matching domain.
package http

import (
	"errors"
	"fmt"

	"github.com/gofiber/fiber/v2"

	libHTTP "github.com/LerianStudio/lib-commons/v5/commons/net/http"

	"github.com/LerianStudio/matcher/internal/auth"
	"github.com/LerianStudio/matcher/internal/matching/services/command"
	sharedhttp "github.com/LerianStudio/matcher/internal/shared/adapters/http"
)

var _ = sharedhttp.ErrorResponse{}

// Unmatch breaks an existing match group and reverts transaction statuses.
// @Summary Break/Unmatch a match group
// @Description Breaks an incorrect match group, rejecting it with a reason and reverting all associated transactions to UNMATCHED status.
// @ID unmatch
// @Tags Matching
// @Accept json
// @Produce json
// @Security BearerAuth
// @Param X-Request-Id header string false "Request ID for tracing"
// @Param matchGroupId path string true "Match Group ID" format(uuid)
// @Param contextId query string true "Context ID" format(uuid)
// @Param request body UnmatchRequest true "Unmatch payload with rejection reason"
// @Success 204 "No Content"
// @Failure 400 {object} sharedhttp.ErrorResponse "Invalid request payload"
// @Failure 401 {object} sharedhttp.ErrorResponse "Unauthorized"
// @Failure 403 {object} sharedhttp.ErrorResponse "Forbidden"
// @Failure 404 {object} sharedhttp.ErrorResponse "Match group not found"
// @Failure 500 {object} sharedhttp.ErrorResponse "Internal server error"
// @Router /v1/matching/groups/{matchGroupId} [delete]
func (handler *Handler) Unmatch(fiberCtx *fiber.Ctx) error {
	ctx, span, logger := startHandlerSpan(fiberCtx, "handler.matching.unmatch")
	defer span.End()

	matchGroupID, err := parseUUIDParam(fiberCtx, "matchGroupId")
	if err != nil {
		return handler.badRequest(ctx, fiberCtx, span, logger, "invalid match group id", err)
	}

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

	var payload UnmatchRequest
	if err := libHTTP.ParseBodyAndValidate(fiberCtx, &payload); err != nil {
		return handler.badRequest(ctx, fiberCtx, span, logger, "invalid unmatch payload", err)
	}

	if payload.Reason == "" {
		return handler.badRequest(ctx, fiberCtx, span, logger, "reason is required", ErrReasonRequired)
	}

	if err := handler.command.Unmatch(ctx, command.UnmatchInput{
		TenantID:     tenantID,
		ContextID:    contextID,
		MatchGroupID: matchGroupID,
		Reason:       payload.Reason,
	}); err != nil {
		if errors.Is(err, command.ErrUnmatchMatchGroupIDRequired) || errors.Is(err, command.ErrUnmatchContextIDRequired) {
			return handler.badRequest(ctx, fiberCtx, span, logger, "invalid unmatch parameters", err)
		}

		if errors.Is(err, command.ErrMatchGroupNotFound) {
			return writeNotFound(fiberCtx, "match group not found")
		}

		return handler.writeServiceError(ctx, fiberCtx, span, logger, "failed to unmatch", err)
	}

	if writeErr := libHTTP.RespondStatus(fiberCtx, fiber.StatusNoContent); writeErr != nil {
		return fmt.Errorf("write no content response: %w", writeErr)
	}

	return nil
}
