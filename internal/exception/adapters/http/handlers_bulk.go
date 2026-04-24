// Copyright 2025 Lerian Studio. All rights reserved.
// Use of this source code is governed by an Elastic License 2.0
// that can be found in the LICENSE.md file.

package http

import (
	"context"
	"errors"
	"fmt"
	"strings"

	"github.com/gofiber/fiber/v2"
	"github.com/google/uuid"
	"go.opentelemetry.io/otel/trace"

	libLog "github.com/LerianStudio/lib-commons/v5/commons/log"
	libHTTP "github.com/LerianStudio/lib-commons/v5/commons/net/http"

	"github.com/LerianStudio/matcher/internal/exception/adapters/http/dto"
	"github.com/LerianStudio/matcher/internal/exception/services/command"
	sharedhttp "github.com/LerianStudio/matcher/internal/shared/adapters/http"
)

var _ = sharedhttp.ErrorResponse{}

// BulkAssign assigns multiple exceptions to the specified assignee.
// @Summary Bulk assign exceptions
// @Description Assigns multiple exceptions to a single assignee. Partial success is supported -- each exception is processed independently.
// @ID bulkAssignExceptions
// @Tags Exception
// @Accept json
// @Produce json
// @Security BearerAuth
// @Param X-Request-Id header string false "Request ID for tracing"
// @Param request body dto.BulkAssignRequest true "Bulk assign payload"
// @Success 200 {object} dto.BulkActionResponse
// @Failure 400 {object} sharedhttp.ErrorResponse "Invalid request payload"
// @Failure 401 {object} sharedhttp.ErrorResponse "Unauthorized"
// @Failure 403 {object} sharedhttp.ErrorResponse "Forbidden"
// @Failure 500 {object} sharedhttp.ErrorResponse "Internal server error"
// @Router /v1/exceptions/bulk/assign [post]
func (handler *Handlers) BulkAssign(fiberCtx *fiber.Ctx) error {
	ctx, span, logger := startHandlerSpan(fiberCtx, "handler.exception.bulk_assign")
	defer span.End()

	var req dto.BulkAssignRequest

	if err := libHTTP.ParseBodyAndValidate(fiberCtx, &req); err != nil {
		return handler.badRequest(ctx, fiberCtx, span, logger, "invalid request body", err)
	}

	exceptionIDs, err := parseUUIDs(req.ExceptionIDs)
	if err != nil {
		return handler.badRequest(ctx, fiberCtx, span, logger, "invalid exception id", err)
	}

	result, err := handler.commandUC.BulkAssign(ctx, command.BulkAssignInput{
		ExceptionIDs: exceptionIDs,
		Assignee:     req.Assignee,
	})
	if err != nil {
		return handler.handleBulkError(ctx, fiberCtx, span, logger, "bulk assign failed", err)
	}

	if err := libHTTP.Respond(fiberCtx, fiber.StatusOK, toBulkActionResponse(result)); err != nil {
		return fmt.Errorf("respond bulk assign: %w", err)
	}

	return nil
}

// BulkResolve resolves multiple exceptions with the specified resolution.
// @Summary Bulk resolve exceptions
// @Description Resolves multiple exceptions with a shared resolution message. Partial success is supported -- each exception is processed independently.
// @ID bulkResolveExceptions
// @Tags Exception
// @Accept json
// @Produce json
// @Security BearerAuth
// @Param X-Request-Id header string false "Request ID for tracing"
// @Param request body dto.BulkResolveRequest true "Bulk resolve payload"
// @Success 200 {object} dto.BulkActionResponse
// @Failure 400 {object} sharedhttp.ErrorResponse "Invalid request payload"
// @Failure 401 {object} sharedhttp.ErrorResponse "Unauthorized"
// @Failure 403 {object} sharedhttp.ErrorResponse "Forbidden"
// @Failure 500 {object} sharedhttp.ErrorResponse "Internal server error"
// @Router /v1/exceptions/bulk/resolve [post]
func (handler *Handlers) BulkResolve(fiberCtx *fiber.Ctx) error {
	ctx, span, logger := startHandlerSpan(fiberCtx, "handler.exception.bulk_resolve")
	defer span.End()

	var req dto.BulkResolveRequest

	if err := libHTTP.ParseBodyAndValidate(fiberCtx, &req); err != nil {
		return handler.badRequest(ctx, fiberCtx, span, logger, "invalid request body", err)
	}

	exceptionIDs, err := parseUUIDs(req.ExceptionIDs)
	if err != nil {
		return handler.badRequest(ctx, fiberCtx, span, logger, "invalid exception id", err)
	}

	result, err := handler.commandUC.BulkResolve(ctx, command.BulkResolveInput{
		ExceptionIDs: exceptionIDs,
		Resolution:   req.Resolution,
		Reason:       req.Reason,
	})
	if err != nil {
		return handler.handleBulkError(ctx, fiberCtx, span, logger, "bulk resolve failed", err)
	}

	if err := libHTTP.Respond(fiberCtx, fiber.StatusOK, toBulkActionResponse(result)); err != nil {
		return fmt.Errorf("respond bulk resolve: %w", err)
	}

	return nil
}

// BulkDispatch dispatches multiple exceptions to an external system.
// @Summary Bulk dispatch exceptions
// @Description Dispatches multiple exceptions to a target external system. Partial success is supported -- each exception is processed independently.
// @ID bulkDispatchExceptions
// @Tags Exception
// @Accept json
// @Produce json
// @Security BearerAuth
// @Param X-Request-Id header string false "Request ID for tracing"
// @Param request body dto.BulkDispatchRequest true "Bulk dispatch payload"
// @Success 200 {object} dto.BulkActionResponse
// @Failure 400 {object} sharedhttp.ErrorResponse "Invalid request payload"
// @Failure 401 {object} sharedhttp.ErrorResponse "Unauthorized"
// @Failure 403 {object} sharedhttp.ErrorResponse "Forbidden"
// @Failure 500 {object} sharedhttp.ErrorResponse "Internal server error"
// @Router /v1/exceptions/bulk/dispatch [post]
func (handler *Handlers) BulkDispatch(fiberCtx *fiber.Ctx) error {
	ctx, span, logger := startHandlerSpan(fiberCtx, "handler.exception.bulk_dispatch")
	defer span.End()

	var req dto.BulkDispatchRequest

	if err := libHTTP.ParseBodyAndValidate(fiberCtx, &req); err != nil {
		return handler.badRequest(ctx, fiberCtx, span, logger, "invalid request body", err)
	}

	exceptionIDs, err := parseUUIDs(req.ExceptionIDs)
	if err != nil {
		return handler.badRequest(ctx, fiberCtx, span, logger, "invalid exception id", err)
	}

	result, err := handler.commandUC.BulkDispatch(ctx, command.BulkDispatchInput{
		ExceptionIDs: exceptionIDs,
		TargetSystem: req.TargetSystem,
		Queue:        req.Queue,
	})
	if err != nil {
		return handler.handleBulkError(ctx, fiberCtx, span, logger, "bulk dispatch failed", err)
	}

	if err := libHTTP.Respond(fiberCtx, fiber.StatusOK, toBulkActionResponse(result)); err != nil {
		return fmt.Errorf("respond bulk dispatch: %w", err)
	}

	return nil
}

// handleBulkError maps bulk command errors to HTTP responses.
func (handler *Handlers) handleBulkError(
	ctx context.Context,
	fiberCtx *fiber.Ctx,
	span trace.Span,
	logger libLog.Logger,
	message string,
	err error,
) error {
	handler.logSpanError(ctx, span, logger, message, err)

	if errors.Is(err, command.ErrBulkEmptyIDs) ||
		errors.Is(err, command.ErrBulkTooManyIDs) ||
		errors.Is(err, command.ErrBulkAssigneeEmpty) ||
		errors.Is(err, command.ErrBulkResolutionEmpty) ||
		errors.Is(err, command.ErrBulkTargetSystemEmpty) ||
		errors.Is(err, command.ErrActorRequired) {
		return respondError(fiberCtx, fiber.StatusBadRequest, "invalid_request", err.Error())
	}

	return respondError(fiberCtx, fiber.StatusInternalServerError, "internal_server_error", "an unexpected error occurred")
}

// ErrNilUUIDNotAllowed is returned when a parsed UUID is the nil UUID.
var ErrNilUUIDNotAllowed = errors.New("nil uuid not allowed")

func parseUUIDs(ids []string) ([]uuid.UUID, error) {
	result := make([]uuid.UUID, 0, len(ids))

	for _, idStr := range ids {
		parsed, err := uuid.Parse(strings.TrimSpace(idStr))
		if err != nil {
			return nil, fmt.Errorf("invalid uuid %q: %w", idStr, err)
		}

		if parsed == uuid.Nil {
			return nil, fmt.Errorf("%w: %s", ErrNilUUIDNotAllowed, idStr)
		}

		result = append(result, parsed)
	}

	return result, nil
}

func toBulkActionResponse(result *command.BulkActionResult) dto.BulkActionResponse {
	succeeded := make([]string, 0, len(result.Succeeded))
	for _, id := range result.Succeeded {
		succeeded = append(succeeded, id.String())
	}

	failed := make([]dto.BulkFailure, 0, len(result.Failed))
	for _, f := range result.Failed {
		failed = append(failed, dto.BulkFailure{
			ExceptionID: f.ExceptionID.String(),
			Error:       f.Error,
		})
	}

	return dto.BulkActionResponse{
		Succeeded: succeeded,
		Failed:    failed,
		Total:     len(succeeded) + len(failed),
	}
}
