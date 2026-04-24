// Copyright 2025 Lerian Studio. All rights reserved.
// Use of this source code is governed by an Elastic License 2.0
// that can be found in the LICENSE.md file.

package http

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"time"

	"github.com/gofiber/fiber/v2"
	"go.opentelemetry.io/otel/trace"

	libLog "github.com/LerianStudio/lib-commons/v5/commons/log"
	libHTTP "github.com/LerianStudio/lib-commons/v5/commons/net/http"

	"github.com/LerianStudio/matcher/internal/auth"
	"github.com/LerianStudio/matcher/internal/exception/adapters/http/dto"
	"github.com/LerianStudio/matcher/internal/exception/domain/dispute"
	"github.com/LerianStudio/matcher/internal/exception/domain/repositories"
	"github.com/LerianStudio/matcher/internal/exception/services/command"
	"github.com/LerianStudio/matcher/internal/exception/services/query"
	sharedhttp "github.com/LerianStudio/matcher/internal/shared/adapters/http"
)

func (handler *Handlers) handleDisputeError(
	ctx context.Context,
	fiberCtx *fiber.Ctx,
	span trace.Span,
	logger libLog.Logger,
	err error,
) error {
	if errors.Is(err, sql.ErrNoRows) {
		return handler.disputeNotFound(ctx, fiberCtx, span, logger, "dispute not found", err)
	}

	if errors.Is(err, query.ErrDisputeNotFound) {
		return handler.disputeNotFound(ctx, fiberCtx, span, logger, "dispute not found", err)
	}

	if errors.Is(err, command.ErrDisputeIDRequired) ||
		errors.Is(err, command.ErrDisputeCategoryRequired) ||
		errors.Is(err, dispute.ErrInvalidDisputeCategory) ||
		errors.Is(err, command.ErrDisputeDescriptionRequired) ||
		errors.Is(err, command.ErrDisputeCommentRequired) ||
		errors.Is(err, command.ErrDisputeResolutionRequired) ||
		errors.Is(err, command.ErrActorRequired) {
		return handler.badRequest(ctx, fiberCtx, span, logger, err.Error(), err)
	}

	if errors.Is(err, dispute.ErrCannotAddEvidenceInCurrentState) ||
		errors.Is(err, dispute.ErrInvalidDisputeTransition) {
		return handler.unprocessable(ctx, fiberCtx, span, logger, err.Error(), err)
	}

	return handler.internalError(ctx, fiberCtx, span, logger, "failed to process dispute", err)
}

// OpenDispute opens a new dispute for an exception.
// @Summary Open a dispute
// @Description Opens a new dispute for an exception. Disputes are used to formally challenge or investigate discrepancies with external parties.
// @ID openDispute
// @Tags Exception
// @Accept json
// @Produce json
// @Security BearerAuth
// @Param X-Request-Id header string false "Request ID for tracing"
// @Param X-Idempotency-Key header string false "Idempotency key for safe retries"
// @Param exceptionId path string true "Exception ID" format(uuid)
// @Param request body dto.OpenDisputeRequest true "Open dispute payload"
// @Success 201 {object} dto.DisputeResponse
// @Failure 400 {object} sharedhttp.ErrorResponse "Invalid request payload"
// @Failure 401 {object} sharedhttp.ErrorResponse "Unauthorized"
// @Failure 403 {object} sharedhttp.ErrorResponse "Forbidden"
// @Failure 404 {object} sharedhttp.ErrorResponse "Exception not found"
// @Failure 500 {object} sharedhttp.ErrorResponse "Internal server error"
// @Router /v1/exceptions/{exceptionId}/disputes [post]
func (handler *Handlers) OpenDispute(fiberCtx *fiber.Ctx) error {
	ctx, span, logger := startHandlerSpan(fiberCtx, "handler.exception.open_dispute")
	defer span.End()

	exceptionID, tenantID, err := libHTTP.ParseAndVerifyResourceScopedID(
		fiberCtx,
		"exceptionId",
		libHTTP.IDLocationParam,
		handler.exceptionVerifier,
		auth.GetTenantID,
		ErrMissingExceptionID,
		ErrInvalidExceptionID,
		ErrExceptionAccessDenied,
		"exception",
	)
	if err != nil {
		return handler.handleExceptionVerificationError(ctx, fiberCtx, span, logger, err)
	}

	libHTTP.SetExceptionSpanAttributes(span, tenantID, exceptionID)

	var req dto.OpenDisputeRequest

	if err := libHTTP.ParseBodyAndValidate(fiberCtx, &req); err != nil {
		return handler.badRequest(ctx, fiberCtx, span, logger, "invalid request body", err)
	}

	result, err := handler.commandUC.OpenDispute(ctx, command.OpenDisputeCommand{
		ExceptionID: exceptionID,
		Category:    req.Category,
		Description: req.Description,
	})
	if err != nil {
		return handler.handleDisputeError(ctx, fiberCtx, span, logger, err)
	}

	if err := libHTTP.Respond(fiberCtx, fiber.StatusCreated, dto.DisputeToResponse(result)); err != nil {
		return fmt.Errorf("respond open dispute: %w", err)
	}

	return nil
}

// CloseDispute closes an existing dispute as won or lost.
// @Summary Close a dispute
// @Description Closes a dispute with a resolution. The dispute can be marked as won or lost based on the outcome.
// @ID closeDispute
// @Tags Exception
// @Accept json
// @Produce json
// @Security BearerAuth
// @Param X-Request-Id header string false "Request ID for tracing"
// @Param X-Idempotency-Key header string false "Idempotency key for safe retries"
// @Param disputeId path string true "Dispute ID" format(uuid)
// @Param request body dto.CloseDisputeRequest true "Close dispute payload"
// @Success 200 {object} dto.DisputeResponse
// @Failure 400 {object} sharedhttp.ErrorResponse "Invalid request payload"
// @Failure 401 {object} sharedhttp.ErrorResponse "Unauthorized"
// @Failure 403 {object} sharedhttp.ErrorResponse "Forbidden"
// @Failure 404 {object} sharedhttp.ErrorResponse "Dispute not found"
// @Failure 422 {object} sharedhttp.ErrorResponse "Unprocessable entity: invalid state transition"
// @Failure 500 {object} sharedhttp.ErrorResponse "Internal server error"
// @Router /v1/disputes/{disputeId}/close [post]
func (handler *Handlers) CloseDispute(fiberCtx *fiber.Ctx) error {
	ctx, span, logger := startHandlerSpan(fiberCtx, "handler.exception.close_dispute")
	defer span.End()

	disputeID, tenantID, err := libHTTP.ParseAndVerifyResourceScopedID(
		fiberCtx,
		"disputeId",
		libHTTP.IDLocationParam,
		handler.disputeVerifier,
		auth.GetTenantID,
		ErrMissingDisputeID,
		ErrInvalidDisputeID,
		ErrDisputeAccessDenied,
		"dispute",
	)
	if err != nil {
		return handler.handleDisputeVerificationError(ctx, fiberCtx, span, logger, err)
	}

	libHTTP.SetDisputeSpanAttributes(span, tenantID, disputeID)

	var req dto.CloseDisputeRequest

	if err := libHTTP.ParseBodyAndValidate(fiberCtx, &req); err != nil {
		return handler.badRequest(ctx, fiberCtx, span, logger, "invalid request body", err)
	}

	result, err := handler.commandUC.CloseDispute(ctx, command.CloseDisputeCommand{
		DisputeID:  disputeID,
		Resolution: req.Resolution,
		Won:        req.Won,
	})
	if err != nil {
		return handler.handleDisputeError(ctx, fiberCtx, span, logger, err)
	}

	if err := libHTTP.Respond(fiberCtx, fiber.StatusOK, dto.DisputeToResponse(result)); err != nil {
		return fmt.Errorf("respond close dispute: %w", err)
	}

	return nil
}

// SubmitEvidence adds evidence to an existing dispute.
// @Summary Submit evidence to a dispute
// @Description Adds evidence to a dispute. Evidence can include comments and optional file attachments to support the dispute case.
// @ID submitEvidence
// @Tags Exception
// @Accept json
// @Produce json
// @Security BearerAuth
// @Param X-Request-Id header string false "Request ID for tracing"
// @Param X-Idempotency-Key header string false "Idempotency key for safe retries"
// @Param disputeId path string true "Dispute ID" format(uuid)
// @Param request body dto.SubmitEvidenceRequest true "Submit evidence payload"
// @Success 200 {object} dto.DisputeResponse
// @Failure 400 {object} sharedhttp.ErrorResponse "Invalid request payload"
// @Failure 401 {object} sharedhttp.ErrorResponse "Unauthorized"
// @Failure 403 {object} sharedhttp.ErrorResponse "Forbidden"
// @Failure 404 {object} sharedhttp.ErrorResponse "Dispute not found"
// @Failure 422 {object} sharedhttp.ErrorResponse "Unprocessable entity: invalid state transition"
// @Failure 500 {object} sharedhttp.ErrorResponse "Internal server error"
// @Router /v1/disputes/{disputeId}/evidence [post]
func (handler *Handlers) SubmitEvidence(fiberCtx *fiber.Ctx) error {
	ctx, span, logger := startHandlerSpan(fiberCtx, "handler.exception.submit_evidence")
	defer span.End()

	disputeID, tenantID, err := libHTTP.ParseAndVerifyResourceScopedID(
		fiberCtx,
		"disputeId",
		libHTTP.IDLocationParam,
		handler.disputeVerifier,
		auth.GetTenantID,
		ErrMissingDisputeID,
		ErrInvalidDisputeID,
		ErrDisputeAccessDenied,
		"dispute",
	)
	if err != nil {
		return handler.handleDisputeVerificationError(ctx, fiberCtx, span, logger, err)
	}

	libHTTP.SetDisputeSpanAttributes(span, tenantID, disputeID)

	var req dto.SubmitEvidenceRequest

	if err := libHTTP.ParseBodyAndValidate(fiberCtx, &req); err != nil {
		return handler.badRequest(ctx, fiberCtx, span, logger, "invalid request body", err)
	}

	result, err := handler.commandUC.SubmitEvidence(ctx, command.SubmitEvidenceCommand{
		DisputeID: disputeID,
		Comment:   req.Comment,
		FileURL:   req.FileURL,
	})
	if err != nil {
		return handler.handleDisputeError(ctx, fiberCtx, span, logger, err)
	}

	if err := libHTTP.Respond(fiberCtx, fiber.StatusOK, dto.DisputeToResponse(result)); err != nil {
		return fmt.Errorf("respond submit evidence: %w", err)
	}

	return nil
}

// ListDisputes lists disputes with optional filters and pagination.
// @Summary List disputes
// @Description Lists all disputes with optional filters for state, category, and date range. Supports cursor-based pagination.
// @ID listDisputes
// @Tags Exception
// @Produce json
// @Security BearerAuth
// @Param X-Request-Id header string false "Request ID for tracing"
// @Param state query string false "Filter by state" Enums(DRAFT,OPEN,PENDING_EVIDENCE,WON,LOST)
// @Param category query string false "Filter by category" Enums(BANK_FEE_ERROR,UNRECOGNIZED_CHARGE,DUPLICATE_TRANSACTION,OTHER)
// @Param date_from query string false "Filter from date (RFC3339)"
// @Param date_to query string false "Filter to date (RFC3339)"
// @Param cursor query string false "Cursor for pagination (opaque)"
// @Param limit query int false "Maximum number of records to return" default(20) minimum(1) maximum(200)
// @Param sort_by query string false "Sort by field" Enums(id,created_at,updated_at,state,category) default(id)
// @Param sort_order query string false "Sort order" Enums(asc,desc) default(desc)
// @Success 200 {object} dto.ListDisputesResponse
// @Failure 400 {object} sharedhttp.ErrorResponse "Invalid request payload"
// @Failure 401 {object} sharedhttp.ErrorResponse "Unauthorized"
// @Failure 403 {object} sharedhttp.ErrorResponse "Forbidden"
// @Failure 500 {object} sharedhttp.ErrorResponse "Internal server error"
// @Router /v1/disputes [get]
func (handler *Handlers) ListDisputes(fiberCtx *fiber.Ctx) error {
	ctx, span, logger := startHandlerSpan(fiberCtx, "handler.dispute.list")
	defer span.End()

	filter, cursorFilter, err := parseDisputeListFilters(fiberCtx)
	if err != nil {
		return handler.badRequest(ctx, fiberCtx, span, logger, "invalid filter parameters", err)
	}

	disputes, pagination, err := handler.queryUC.ListDisputes(ctx, query.DisputeListQuery{
		Filter: filter,
		Cursor: cursorFilter,
	})
	if err != nil {
		if errors.Is(err, libHTTP.ErrInvalidCursor) {
			return handler.badRequest(ctx, fiberCtx, span, logger, "invalid pagination parameters", err)
		}

		return handler.internalError(ctx, fiberCtx, span, logger, "failed to list disputes", err)
	}

	items := dto.DisputesToResponse(disputes)

	response := dto.ListDisputesResponse{
		Items: items,
		CursorResponse: sharedhttp.CursorResponse{
			NextCursor: pagination.Next,
			PrevCursor: pagination.Prev,
			Limit:      cursorFilter.Limit,
			HasMore:    pagination.Next != "",
		},
	}

	if err := libHTTP.Respond(fiberCtx, fiber.StatusOK, response); err != nil {
		return fmt.Errorf("respond list disputes: %w", err)
	}

	return nil
}

// GetDispute retrieves a single dispute by ID.
// @Summary Get dispute
// @Description Retrieves a single dispute by its ID.
// @ID getDispute
// @Tags Exception
// @Produce json
// @Security BearerAuth
// @Param X-Request-Id header string false "Request ID for tracing"
// @Param disputeId path string true "Dispute ID" format(uuid)
// @Success 200 {object} dto.DisputeResponse
// @Failure 400 {object} sharedhttp.ErrorResponse "Invalid request payload"
// @Failure 401 {object} sharedhttp.ErrorResponse "Unauthorized"
// @Failure 403 {object} sharedhttp.ErrorResponse "Forbidden"
// @Failure 404 {object} sharedhttp.ErrorResponse "Dispute not found"
// @Failure 500 {object} sharedhttp.ErrorResponse "Internal server error"
// @Router /v1/disputes/{disputeId} [get]
func (handler *Handlers) GetDispute(fiberCtx *fiber.Ctx) error {
	ctx, span, logger := startHandlerSpan(fiberCtx, "handler.dispute.get")
	defer span.End()

	disputeID, tenantID, err := libHTTP.ParseAndVerifyResourceScopedID(
		fiberCtx,
		"disputeId",
		libHTTP.IDLocationParam,
		handler.disputeVerifier,
		auth.GetTenantID,
		ErrMissingDisputeID,
		ErrInvalidDisputeID,
		ErrDisputeAccessDenied,
		"dispute",
	)
	if err != nil {
		return handler.handleDisputeVerificationError(ctx, fiberCtx, span, logger, err)
	}

	libHTTP.SetDisputeSpanAttributes(span, tenantID, disputeID)

	result, err := handler.queryUC.GetDispute(ctx, disputeID)
	if err != nil {
		if errors.Is(err, query.ErrDisputeNotFound) {
			return handler.disputeNotFound(ctx, fiberCtx, span, logger, "dispute not found", err)
		}

		return handler.internalError(ctx, fiberCtx, span, logger, "failed to get dispute", err)
	}

	if err := libHTTP.Respond(fiberCtx, fiber.StatusOK, dto.DisputeToResponse(result)); err != nil {
		return fmt.Errorf("respond get dispute: %w", err)
	}

	return nil
}

func parseDisputeListFilters(
	fiberCtx *fiber.Ctx,
) (repositories.DisputeFilter, repositories.CursorFilter, error) {
	filter, err := parseDisputeFilter(fiberCtx)
	if err != nil {
		return repositories.DisputeFilter{}, repositories.CursorFilter{}, err
	}

	cursorFilter, err := parseCursorFilter(fiberCtx)
	if err != nil {
		return repositories.DisputeFilter{}, repositories.CursorFilter{}, err
	}

	return filter, cursorFilter, nil
}

func parseDisputeFilter(fiberCtx *fiber.Ctx) (repositories.DisputeFilter, error) {
	var filter repositories.DisputeFilter

	if state := fiberCtx.Query("state"); state != "" {
		parsed, err := dispute.ParseDisputeState(state)
		if err != nil {
			return filter, fmt.Errorf("invalid state: %w", err)
		}

		filter.State = &parsed
	}

	if category := fiberCtx.Query("category"); category != "" {
		parsed, err := dispute.ParseDisputeCategory(category)
		if err != nil {
			return filter, fmt.Errorf("invalid category: %w", err)
		}

		filter.Category = &parsed
	}

	if dateFrom := fiberCtx.Query("date_from"); dateFrom != "" {
		parsed, err := time.Parse(time.RFC3339, dateFrom)
		if err != nil {
			return filter, fmt.Errorf("invalid date_from: %w", err)
		}

		filter.DateFrom = &parsed
	}

	if dateTo := fiberCtx.Query("date_to"); dateTo != "" {
		parsed, err := time.Parse(time.RFC3339, dateTo)
		if err != nil {
			return filter, fmt.Errorf("invalid date_to: %w", err)
		}

		filter.DateTo = &parsed
	}

	return filter, nil
}
