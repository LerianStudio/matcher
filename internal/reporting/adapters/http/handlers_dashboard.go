// Copyright 2025 Lerian Studio. All rights reserved.
// Use of this source code is governed by an Elastic License 2.0
// that can be found in the LICENSE.md file.

package http

import (
	"fmt"

	"github.com/gofiber/fiber/v2"

	libHTTP "github.com/LerianStudio/lib-commons/v5/commons/net/http"

	"github.com/LerianStudio/matcher/internal/auth"
	"github.com/LerianStudio/matcher/internal/reporting/adapters/http/dto"
	sharedhttp "github.com/LerianStudio/matcher/internal/shared/adapters/http"
)

var _ = sharedhttp.ErrorResponse{}

// GetVolumeStats handles GET /v1/reports/contexts/:contextId/dashboard/volume
// @ID getVolumeStats
// @Summary Get volume statistics
// @Description Returns transaction volume statistics for a reconciliation context within the specified date range.
// @Tags Reporting
// @Produce json
// @Security BearerAuth
// @Param X-Request-Id header string false "Request ID for tracing"
// @Param contextId path string true "Context ID" format(uuid)
// @Param date_from query string true "Start date (YYYY-MM-DD)" format(date)
// @Param date_to query string true "End date (YYYY-MM-DD)" format(date)
// @Param source_id query string false "Source ID filter"
// @Success 200 {object} dto.VolumeStatsResponse
// @Failure 400 {object} sharedhttp.ErrorResponse "Invalid query parameters"
// @Failure 401 {object} sharedhttp.ErrorResponse "Unauthorized"
// @Failure 403 {object} sharedhttp.ErrorResponse "Forbidden"
// @Failure 404 {object} sharedhttp.ErrorResponse "Context not found"
// @Failure 500 {object} sharedhttp.ErrorResponse "Internal server error"
// @Router /v1/reports/contexts/{contextId}/dashboard/volume [get]
func (handler *Handlers) GetVolumeStats(fiberCtx *fiber.Ctx) error {
	ctx, span, logger := startHandlerSpan(fiberCtx, "handler.reporting.get_volume_stats")
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

	filter, err := parseDashboardFilter(fiberCtx, contextID)
	if err != nil {
		return handler.badRequest(ctx, fiberCtx, span, logger, err.Error(), err)
	}

	stats, err := handler.dashboardUC.GetVolumeStats(ctx, filter)
	if err != nil {
		handler.logSpanError(ctx, span, logger, "failed to get volume stats", err)

		return respondError(fiberCtx, fiber.StatusInternalServerError, "internal_server_error", "an unexpected error occurred")
	}

	if err := libHTTP.Respond(fiberCtx, fiber.StatusOK, dto.VolumeStatsToResponse(stats)); err != nil {
		return fmt.Errorf("respond volume stats: %w", err)
	}

	return nil
}

// GetMatchRateStats handles GET /v1/reports/contexts/:contextId/dashboard/match-rate
// @ID getMatchRateStats
// @Summary Get match rate statistics
// @Description Returns match rate percentage and trend data for a reconciliation context within the specified date range.
// @Tags Reporting
// @Produce json
// @Security BearerAuth
// @Param X-Request-Id header string false "Request ID for tracing"
// @Param contextId path string true "Context ID" format(uuid)
// @Param date_from query string true "Start date (YYYY-MM-DD)" format(date)
// @Param date_to query string true "End date (YYYY-MM-DD)" format(date)
// @Param source_id query string false "Source ID filter"
// @Success 200 {object} dto.MatchRateStatsResponse
// @Failure 400 {object} sharedhttp.ErrorResponse "Invalid query parameters"
// @Failure 401 {object} sharedhttp.ErrorResponse "Unauthorized"
// @Failure 403 {object} sharedhttp.ErrorResponse "Forbidden"
// @Failure 404 {object} sharedhttp.ErrorResponse "Context not found"
// @Failure 500 {object} sharedhttp.ErrorResponse "Internal server error"
// @Router /v1/reports/contexts/{contextId}/dashboard/match-rate [get]
func (handler *Handlers) GetMatchRateStats(fiberCtx *fiber.Ctx) error {
	ctx, span, logger := startHandlerSpan(fiberCtx, "handler.reporting.get_match_rate_stats")
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

	filter, err := parseDashboardFilter(fiberCtx, contextID)
	if err != nil {
		return handler.badRequest(ctx, fiberCtx, span, logger, err.Error(), err)
	}

	stats, err := handler.dashboardUC.GetMatchRateStats(ctx, filter)
	if err != nil {
		handler.logSpanError(ctx, span, logger, "failed to get match rate stats", err)

		return respondError(fiberCtx, fiber.StatusInternalServerError, "internal_server_error", "an unexpected error occurred")
	}

	if err := libHTTP.Respond(fiberCtx, fiber.StatusOK, dto.MatchRateStatsToResponse(stats)); err != nil {
		return fmt.Errorf("respond match rate stats: %w", err)
	}

	return nil
}

// GetSLAStats handles GET /v1/reports/contexts/:contextId/dashboard/sla
// @ID getSLAStats
// @Summary Get SLA statistics
// @Description Returns SLA compliance statistics for a reconciliation context within the specified date range.
// @Tags Reporting
// @Produce json
// @Security BearerAuth
// @Param X-Request-Id header string false "Request ID for tracing"
// @Param contextId path string true "Context ID" format(uuid)
// @Param date_from query string true "Start date (YYYY-MM-DD)" format(date)
// @Param date_to query string true "End date (YYYY-MM-DD)" format(date)
// @Param source_id query string false "Source ID filter"
// @Success 200 {object} dto.SLAStatsResponse
// @Failure 400 {object} sharedhttp.ErrorResponse "Invalid query parameters"
// @Failure 401 {object} sharedhttp.ErrorResponse "Unauthorized"
// @Failure 403 {object} sharedhttp.ErrorResponse "Forbidden"
// @Failure 404 {object} sharedhttp.ErrorResponse "Context not found"
// @Failure 500 {object} sharedhttp.ErrorResponse "Internal server error"
// @Router /v1/reports/contexts/{contextId}/dashboard/sla [get]
func (handler *Handlers) GetSLAStats(fiberCtx *fiber.Ctx) error {
	ctx, span, logger := startHandlerSpan(fiberCtx, "handler.reporting.get_sla_stats")
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

	filter, err := parseDashboardFilter(fiberCtx, contextID)
	if err != nil {
		return handler.badRequest(ctx, fiberCtx, span, logger, err.Error(), err)
	}

	stats, err := handler.dashboardUC.GetSLAStats(ctx, filter)
	if err != nil {
		handler.logSpanError(ctx, span, logger, "failed to get sla stats", err)

		return respondError(fiberCtx, fiber.StatusInternalServerError, "internal_server_error", "an unexpected error occurred")
	}

	if err := libHTTP.Respond(fiberCtx, fiber.StatusOK, dto.SLAStatsToResponse(stats)); err != nil {
		return fmt.Errorf("respond sla stats: %w", err)
	}

	return nil
}

// GetDashboardAggregates handles GET /v1/reports/contexts/:contextId/dashboard
// @ID getDashboardAggregates
// @Summary Get all dashboard aggregates
// @Description Returns combined dashboard aggregates including volume, match rate, and SLA statistics.
// @Tags Reporting
// @Produce json
// @Security BearerAuth
// @Param X-Request-Id header string false "Request ID for tracing"
// @Param contextId path string true "Context ID" format(uuid)
// @Param date_from query string true "Start date (YYYY-MM-DD)" format(date)
// @Param date_to query string true "End date (YYYY-MM-DD)" format(date)
// @Param source_id query string false "Source ID filter"
// @Success 200 {object} dto.DashboardAggregatesResponse
// @Failure 400 {object} sharedhttp.ErrorResponse "Invalid query parameters"
// @Failure 401 {object} sharedhttp.ErrorResponse "Unauthorized"
// @Failure 403 {object} sharedhttp.ErrorResponse "Forbidden"
// @Failure 404 {object} sharedhttp.ErrorResponse "Context not found"
// @Failure 500 {object} sharedhttp.ErrorResponse "Internal server error"
// @Router /v1/reports/contexts/{contextId}/dashboard [get]
func (handler *Handlers) GetDashboardAggregates(fiberCtx *fiber.Ctx) error {
	ctx, span, logger := startHandlerSpan(fiberCtx, "handler.reporting.get_dashboard_aggregates")
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

	filter, err := parseDashboardFilter(fiberCtx, contextID)
	if err != nil {
		return handler.badRequest(ctx, fiberCtx, span, logger, err.Error(), err)
	}

	aggregates, err := handler.dashboardUC.GetDashboardAggregates(ctx, filter)
	if err != nil {
		handler.logSpanError(ctx, span, logger, "failed to get dashboard aggregates", err)

		return respondError(fiberCtx, fiber.StatusInternalServerError, "internal_server_error", "an unexpected error occurred")
	}

	if err := libHTTP.Respond(fiberCtx, fiber.StatusOK, dto.DashboardAggregatesToResponse(aggregates)); err != nil {
		return fmt.Errorf("respond dashboard aggregates: %w", err)
	}

	return nil
}

// GetMatcherDashboardMetrics handles GET /v1/reports/contexts/:contextId/dashboard/metrics
// @ID getMatcherDashboardMetrics
// @Summary Get comprehensive dashboard metrics
// @Description Returns complete dashboard metrics including summary, trends, and breakdowns for the Command Center.
// @Tags Reporting
// @Produce json
// @Security BearerAuth
// @Param X-Request-Id header string false "Request ID for tracing"
// @Param contextId path string true "Context ID" format(uuid)
// @Param date_from query string true "Start date (YYYY-MM-DD)" format(date)
// @Param date_to query string true "End date (YYYY-MM-DD)" format(date)
// @Param source_id query string false "Source ID filter"
// @Success 200 {object} dto.MatcherDashboardMetricsResponse
// @Failure 400 {object} sharedhttp.ErrorResponse "Invalid query parameters"
// @Failure 401 {object} sharedhttp.ErrorResponse "Unauthorized"
// @Failure 403 {object} sharedhttp.ErrorResponse "Forbidden"
// @Failure 404 {object} sharedhttp.ErrorResponse "Context not found"
// @Failure 500 {object} sharedhttp.ErrorResponse "Internal server error"
// @Router /v1/reports/contexts/{contextId}/dashboard/metrics [get]
func (handler *Handlers) GetMatcherDashboardMetrics(fiberCtx *fiber.Ctx) error {
	ctx, span, logger := startHandlerSpan(
		fiberCtx,
		"handler.reporting.get_matcher_dashboard_metrics",
	)
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

	filter, err := parseDashboardFilter(fiberCtx, contextID)
	if err != nil {
		return handler.badRequest(ctx, fiberCtx, span, logger, err.Error(), err)
	}

	metrics, err := handler.dashboardUC.GetMatcherDashboardMetrics(ctx, filter)
	if err != nil {
		handler.logSpanError(ctx, span, logger, "failed to get matcher dashboard metrics", err)

		return respondError(fiberCtx, fiber.StatusInternalServerError, "internal_server_error", "an unexpected error occurred")
	}

	if err := libHTTP.Respond(fiberCtx, fiber.StatusOK, dto.MatcherDashboardMetricsToResponse(metrics)); err != nil {
		return fmt.Errorf("respond dashboard metrics: %w", err)
	}

	return nil
}

// GetSourceBreakdown handles GET /v1/reports/contexts/:contextId/dashboard/source-breakdown
// @ID getSourceBreakdown
// @Summary Get per-source reconciliation breakdown
// @Description Returns reconciliation statistics broken down by source for a context within the specified date range.
// @Tags Reporting
// @Produce json
// @Security BearerAuth
// @Param X-Request-Id header string false "Request ID for tracing"
// @Param contextId path string true "Context ID" format(uuid)
// @Param date_from query string true "Start date (YYYY-MM-DD)" format(date)
// @Param date_to query string true "End date (YYYY-MM-DD)" format(date)
// @Param source_id query string false "Source ID filter"
// @Success 200 {object} dto.SourceBreakdownListResponse
// @Failure 400 {object} sharedhttp.ErrorResponse "Invalid query parameters"
// @Failure 401 {object} sharedhttp.ErrorResponse "Unauthorized"
// @Failure 403 {object} sharedhttp.ErrorResponse "Forbidden"
// @Failure 404 {object} sharedhttp.ErrorResponse "Context not found"
// @Failure 500 {object} sharedhttp.ErrorResponse "Internal server error"
// @Router /v1/reports/contexts/{contextId}/dashboard/source-breakdown [get]
func (handler *Handlers) GetSourceBreakdown(fiberCtx *fiber.Ctx) error {
	ctx, span, logger := startHandlerSpan(
		fiberCtx,
		"handler.reporting.get_source_breakdown",
	)
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

	filter, err := parseDashboardFilter(fiberCtx, contextID)
	if err != nil {
		return handler.badRequest(ctx, fiberCtx, span, logger, err.Error(), err)
	}

	breakdowns, err := handler.dashboardUC.GetSourceBreakdown(ctx, filter)
	if err != nil {
		handler.logSpanError(ctx, span, logger, "failed to get source breakdown", err)

		return respondError(fiberCtx, fiber.StatusInternalServerError, "internal_server_error", "an unexpected error occurred")
	}

	if err := libHTTP.Respond(fiberCtx, fiber.StatusOK, dto.SourceBreakdownToResponse(breakdowns)); err != nil {
		return fmt.Errorf("respond source breakdown: %w", err)
	}

	return nil
}

// GetCashImpactSummary handles GET /v1/reports/contexts/:contextId/dashboard/cash-impact
// @ID getCashImpactSummary
// @Summary Get cash impact summary
// @Description Returns unreconciled financial exposure summary with currency and age breakdowns.
// @Tags Reporting
// @Produce json
// @Security BearerAuth
// @Param X-Request-Id header string false "Request ID for tracing"
// @Param contextId path string true "Context ID" format(uuid)
// @Param date_from query string true "Start date (YYYY-MM-DD)" format(date)
// @Param date_to query string true "End date (YYYY-MM-DD)" format(date)
// @Param source_id query string false "Source ID filter"
// @Success 200 {object} dto.CashImpactSummaryResponse
// @Failure 400 {object} sharedhttp.ErrorResponse "Invalid query parameters"
// @Failure 401 {object} sharedhttp.ErrorResponse "Unauthorized"
// @Failure 403 {object} sharedhttp.ErrorResponse "Forbidden"
// @Failure 404 {object} sharedhttp.ErrorResponse "Context not found"
// @Failure 500 {object} sharedhttp.ErrorResponse "Internal server error"
// @Router /v1/reports/contexts/{contextId}/dashboard/cash-impact [get]
func (handler *Handlers) GetCashImpactSummary(fiberCtx *fiber.Ctx) error {
	ctx, span, logger := startHandlerSpan(
		fiberCtx,
		"handler.reporting.get_cash_impact_summary",
	)
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

	filter, err := parseDashboardFilter(fiberCtx, contextID)
	if err != nil {
		return handler.badRequest(ctx, fiberCtx, span, logger, err.Error(), err)
	}

	summary, err := handler.dashboardUC.GetCashImpactSummary(ctx, filter)
	if err != nil {
		handler.logSpanError(ctx, span, logger, "failed to get cash impact summary", err)

		return respondError(fiberCtx, fiber.StatusInternalServerError, "internal_server_error", "an unexpected error occurred")
	}

	if err := libHTTP.Respond(fiberCtx, fiber.StatusOK, dto.CashImpactSummaryToResponse(summary)); err != nil {
		return fmt.Errorf("respond cash impact summary: %w", err)
	}

	return nil
}
