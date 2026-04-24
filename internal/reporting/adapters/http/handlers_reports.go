// Copyright 2025 Lerian Studio. All rights reserved.
// Use of this source code is governed by an Elastic License 2.0
// that can be found in the LICENSE.md file.

package http

import (
	"errors"
	"fmt"

	"github.com/gofiber/fiber/v2"

	libHTTP "github.com/LerianStudio/lib-commons/v5/commons/net/http"

	"github.com/LerianStudio/matcher/internal/auth"
	"github.com/LerianStudio/matcher/internal/reporting/adapters/http/dto"
)

// GetMatchedReport handles GET /v1/reports/contexts/:contextId/matched
// @ID getMatchedReport
// @Summary Get matched transactions report
// @Description Returns paginated matched transactions for a reconciliation context within the specified date range.
// @Tags Reporting
// @Produce json
// @Security BearerAuth
// @Param X-Request-Id header string false "Request ID for tracing"
// @Param contextId path string true "Context ID" format(uuid)
// @Param date_from query string true "Start date (YYYY-MM-DD)" format(date)
// @Param date_to query string true "End date (YYYY-MM-DD)" format(date)
// @Param source_id query string false "Source ID filter"
// @Param cursor query string false "Cursor for pagination (opaque)"
// @Param limit query int false "Maximum number of records to return" default(20) minimum(1) maximum(200)
// @Param sort_order query string false "Sort order for created_at (asc or desc; defaults to desc)" default(desc) Enums(asc, desc)
// @Success 200 {object} dto.ListMatchedReportResponse
// @Failure 400 {object} sharedhttp.ErrorResponse "Invalid query parameters"
// @Failure 401 {object} sharedhttp.ErrorResponse "Unauthorized"
// @Failure 403 {object} sharedhttp.ErrorResponse "Forbidden"
// @Failure 404 {object} sharedhttp.ErrorResponse "Context not found"
// @Failure 500 {object} sharedhttp.ErrorResponse "Internal server error"
// @Router /v1/reports/contexts/{contextId}/matched [get]
func (handler *Handlers) GetMatchedReport(fiberCtx *fiber.Ctx) error {
	ctx, span, logger := startHandlerSpan(fiberCtx, "handler.reporting.get_matched_report")
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
		return handler.handleContextVerificationError(ctx, fiberCtx, span, logger, err)
	}

	libHTTP.SetHandlerSpanAttributes(span, tenantID, contextID)

	filter, err := parseReportFilter(fiberCtx, contextID)
	if err != nil {
		return handler.badRequest(ctx, fiberCtx, span, logger, err.Error(), err)
	}

	items, pagination, err := handler.reportRepo.ListMatched(ctx, filter)
	if err != nil {
		if errors.Is(err, libHTTP.ErrInvalidCursor) {
			return handler.badRequest(ctx, fiberCtx, span, logger, "invalid pagination parameters", err)
		}

		handler.logSpanError(ctx, span, logger, "failed to get matched report", err)

		return respondError(fiberCtx, fiber.StatusInternalServerError, "internal_server_error", "an unexpected error occurred")
	}

	if err := libHTTP.Respond(fiberCtx, fiber.StatusOK, dto.ListMatchedReportResponse{
		Items:      dto.MatchedItemsToResponse(items),
		Pagination: pagination,
	}); err != nil {
		return fmt.Errorf("respond list matched: %w", err)
	}

	return nil
}

// GetUnmatchedReport handles GET /v1/reports/contexts/:contextId/unmatched
// @ID getUnmatchedReport
// @Summary Get unmatched transactions report
// @Description Returns paginated unmatched transactions for a reconciliation context within the specified date range.
// @Tags Reporting
// @Produce json
// @Security BearerAuth
// @Param X-Request-Id header string false "Request ID for tracing"
// @Param contextId path string true "Context ID" format(uuid)
// @Param date_from query string true "Start date (YYYY-MM-DD)" format(date)
// @Param date_to query string true "End date (YYYY-MM-DD)" format(date)
// @Param source_id query string false "Source ID filter"
// @Param cursor query string false "Cursor for pagination (opaque)"
// @Param limit query int false "Maximum number of records to return" default(20) minimum(1) maximum(200)
// @Param sort_order query string false "Sort order for created_at (asc or desc; defaults to desc)" default(desc) Enums(asc, desc)
// @Success 200 {object} dto.ListUnmatchedReportResponse
// @Failure 400 {object} sharedhttp.ErrorResponse "Invalid query parameters"
// @Failure 401 {object} sharedhttp.ErrorResponse "Unauthorized"
// @Failure 403 {object} sharedhttp.ErrorResponse "Forbidden"
// @Failure 404 {object} sharedhttp.ErrorResponse "Context not found"
// @Failure 500 {object} sharedhttp.ErrorResponse "Internal server error"
// @Router /v1/reports/contexts/{contextId}/unmatched [get]
func (handler *Handlers) GetUnmatchedReport(fiberCtx *fiber.Ctx) error {
	ctx, span, logger := startHandlerSpan(fiberCtx, "handler.reporting.get_unmatched_report")
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
		return handler.handleContextVerificationError(ctx, fiberCtx, span, logger, err)
	}

	libHTTP.SetHandlerSpanAttributes(span, tenantID, contextID)

	filter, err := parseReportFilter(fiberCtx, contextID)
	if err != nil {
		return handler.badRequest(ctx, fiberCtx, span, logger, err.Error(), err)
	}

	items, pagination, err := handler.reportRepo.ListUnmatched(ctx, filter)
	if err != nil {
		if errors.Is(err, libHTTP.ErrInvalidCursor) {
			return handler.badRequest(ctx, fiberCtx, span, logger, "invalid pagination parameters", err)
		}

		handler.logSpanError(ctx, span, logger, "failed to get unmatched report", err)

		return respondError(fiberCtx, fiber.StatusInternalServerError, "internal_server_error", "an unexpected error occurred")
	}

	if err := libHTTP.Respond(fiberCtx, fiber.StatusOK, dto.ListUnmatchedReportResponse{
		Items:      dto.UnmatchedItemsToResponse(items),
		Pagination: pagination,
	}); err != nil {
		return fmt.Errorf("respond list unmatched: %w", err)
	}

	return nil
}

// GetSummaryReport handles GET /v1/reports/contexts/:contextId/summary
// @ID getSummaryReport
// @Summary Get reconciliation summary report
// @Description Returns aggregated summary statistics for a reconciliation context within the specified date range.
// @Tags Reporting
// @Produce json
// @Security BearerAuth
// @Param X-Request-Id header string false "Request ID for tracing"
// @Param contextId path string true "Context ID" format(uuid)
// @Param date_from query string true "Start date (YYYY-MM-DD)" format(date)
// @Param date_to query string true "End date (YYYY-MM-DD)" format(date)
// @Param source_id query string false "Source ID filter"
// @Success 200 {object} dto.SummaryReportResponse
// @Failure 400 {object} sharedhttp.ErrorResponse "Invalid query parameters"
// @Failure 401 {object} sharedhttp.ErrorResponse "Unauthorized"
// @Failure 403 {object} sharedhttp.ErrorResponse "Forbidden"
// @Failure 404 {object} sharedhttp.ErrorResponse "Context not found"
// @Failure 500 {object} sharedhttp.ErrorResponse "Internal server error"
// @Router /v1/reports/contexts/{contextId}/summary [get]
func (handler *Handlers) GetSummaryReport(fiberCtx *fiber.Ctx) error {
	ctx, span, logger := startHandlerSpan(fiberCtx, "handler.reporting.get_summary_report")
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
		return handler.handleContextVerificationError(ctx, fiberCtx, span, logger, err)
	}

	libHTTP.SetHandlerSpanAttributes(span, tenantID, contextID)

	filter, err := parseReportFilter(fiberCtx, contextID)
	if err != nil {
		return handler.badRequest(ctx, fiberCtx, span, logger, err.Error(), err)
	}

	summary, err := handler.reportRepo.GetSummary(ctx, filter)
	if err != nil {
		handler.logSpanError(ctx, span, logger, "failed to get summary report", err)

		return respondError(fiberCtx, fiber.StatusInternalServerError, "internal_server_error", "an unexpected error occurred")
	}

	if err := libHTTP.Respond(fiberCtx, fiber.StatusOK, dto.SummaryReportToResponse(summary)); err != nil {
		return fmt.Errorf("respond summary report: %w", err)
	}

	return nil
}

// GetVarianceReport handles GET /v1/reports/contexts/:contextId/variance
// @ID getVarianceReport
// @Summary Get variance report
// @Description Returns paginated variance report rows for a reconciliation context within the specified date range.
// @Tags Reporting
// @Produce json
// @Security BearerAuth
// @Param X-Request-Id header string false "Request ID for tracing"
// @Param contextId path string true "Context ID" format(uuid)
// @Param date_from query string true "Start date (YYYY-MM-DD)" format(date)
// @Param date_to query string true "End date (YYYY-MM-DD)" format(date)
// @Param source_id query string false "Source ID filter"
// @Param cursor query string false "Cursor for pagination (opaque)"
// @Param limit query int false "Maximum number of records to return" default(20) minimum(1) maximum(200)
// @Param sort_order query string false "Sort order for created_at (asc or desc; defaults to desc)" default(desc) Enums(asc, desc)
// @Success 200 {object} dto.ListVarianceReportResponse
// @Failure 400 {object} sharedhttp.ErrorResponse "Invalid query parameters"
// @Failure 401 {object} sharedhttp.ErrorResponse "Unauthorized"
// @Failure 403 {object} sharedhttp.ErrorResponse "Forbidden"
// @Failure 404 {object} sharedhttp.ErrorResponse "Context not found"
// @Failure 500 {object} sharedhttp.ErrorResponse "Internal server error"
// @Router /v1/reports/contexts/{contextId}/variance [get]
func (handler *Handlers) GetVarianceReport(fiberCtx *fiber.Ctx) error {
	ctx, span, logger := startHandlerSpan(fiberCtx, "handler.reporting.get_variance_report")
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
		return handler.handleContextVerificationError(ctx, fiberCtx, span, logger, err)
	}

	libHTTP.SetHandlerSpanAttributes(span, tenantID, contextID)

	filter, err := parseVarianceReportFilter(fiberCtx, contextID)
	if err != nil {
		return handler.badRequest(ctx, fiberCtx, span, logger, err.Error(), err)
	}

	rows, pagination, err := handler.reportRepo.GetVarianceReport(ctx, filter)
	if err != nil {
		if errors.Is(err, libHTTP.ErrInvalidCursor) {
			return handler.badRequest(ctx, fiberCtx, span, logger, "invalid pagination parameters", err)
		}

		handler.logSpanError(ctx, span, logger, "failed to get variance report", err)

		return respondError(fiberCtx, fiber.StatusInternalServerError, "internal_server_error", "an unexpected error occurred")
	}

	if err := libHTTP.Respond(fiberCtx, fiber.StatusOK, dto.ListVarianceReportResponse{
		Items:      dto.VarianceRowsToResponse(rows),
		Pagination: pagination,
	}); err != nil {
		return fmt.Errorf("respond variance report: %w", err)
	}

	return nil
}
