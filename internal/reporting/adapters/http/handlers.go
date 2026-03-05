// Package http provides HTTP handlers for the reporting context.
package http

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/gofiber/fiber/v2"
	"github.com/google/uuid"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/trace"

	libCommons "github.com/LerianStudio/lib-uncommons/v2/uncommons"
	libLog "github.com/LerianStudio/lib-uncommons/v2/uncommons/log"
	libHTTP "github.com/LerianStudio/lib-uncommons/v2/uncommons/net/http"
	libOpentelemetry "github.com/LerianStudio/lib-uncommons/v2/uncommons/opentelemetry"

	"github.com/LerianStudio/matcher/internal/auth"
	"github.com/LerianStudio/matcher/internal/reporting/adapters/http/dto"
	"github.com/LerianStudio/matcher/internal/reporting/domain/entities"
	"github.com/LerianStudio/matcher/internal/reporting/services/query"
)

const (
	maxDateRangeDays = 90
	hoursPerDay      = 24

	formatCSV       = "csv"
	formatPDF       = "pdf"
	contentTypeCSV  = "text/csv"
	contentTypePDF  = "application/pdf"
	contentDispoFmt = "attachment; filename=\""
)

var (
	// ErrNilDashboardUseCase indicates dashboard use case is nil.
	ErrNilDashboardUseCase = errors.New("dashboard use case is required")
	// ErrNilContextProvider indicates context provider is nil.
	ErrNilContextProvider = errors.New("context provider is required")
	// ErrInvalidDateRange indicates invalid date range.
	ErrInvalidDateRange = errors.New("invalid date range")
	// ErrDateFromRequired indicates date_from parameter is missing.
	ErrDateFromRequired = errors.New("date_from is required")
	// ErrDateToRequired indicates date_to parameter is missing.
	ErrDateToRequired = errors.New("date_to is required")
	// ErrInvalidSourceID indicates source_id parameter is invalid.
	ErrInvalidSourceID = errors.New("source_id must be a valid UUID")
	// ErrDateRangeExceeded indicates the date range exceeds the maximum allowed.
	ErrDateRangeExceeded = errors.New("date range cannot exceed 90 days")
	// ErrInvalidExportFormat indicates export format is invalid.
	ErrInvalidExportFormat = errors.New("format must be csv or pdf")
	// ErrInvalidSortOrder indicates sort_order parameter is invalid.
	ErrInvalidSortOrder = errors.New("sort_order must be asc or desc")
	// ErrNilExportUseCase indicates export use case is nil.
	ErrNilExportUseCase = errors.New("export use case is required")
	// errForbidden indicates access denied.
	errForbidden = errors.New("forbidden")
)

// ReconciliationContextInfo contains the context information needed by reporting.
type ReconciliationContextInfo struct {
	ID     uuid.UUID
	Active bool
}

type contextProvider interface {
	FindByID(ctx context.Context, tenantID, contextID uuid.UUID) (*ReconciliationContextInfo, error)
}

// Handlers provides HTTP handlers for reporting operations.
type Handlers struct {
	dashboardUC     *query.DashboardUseCase
	exportUC        *query.UseCase
	contextProvider contextProvider
	contextVerifier libHTTP.TenantOwnershipVerifier
}

// NewHandlers creates a new Handlers instance with the given use cases.
func NewHandlers(
	dashboardUC *query.DashboardUseCase,
	ctxProvider contextProvider,
	exportUC *query.UseCase,
) (*Handlers, error) {
	if dashboardUC == nil {
		return nil, ErrNilDashboardUseCase
	}

	if ctxProvider == nil {
		return nil, ErrNilContextProvider
	}

	if exportUC == nil {
		return nil, ErrNilExportUseCase
	}

	verifier := NewTenantOwnershipVerifier(ctxProvider)

	return &Handlers{
		dashboardUC:     dashboardUC,
		exportUC:        exportUC,
		contextProvider: ctxProvider,
		contextVerifier: verifier,
	}, nil
}

func startHandlerSpan(c *fiber.Ctx, name string) (context.Context, trace.Span, libLog.Logger) {
	ctx := c.UserContext()
	logger, tracer, _, _ := libCommons.NewTrackingFromContext(ctx)

	if tracer == nil {
		tracer = otel.Tracer("commons.default")
	}

	ctx, span := tracer.Start(ctx, name)

	return ctx, span, logger
}

func logSpanError(ctx context.Context, span trace.Span, logger libLog.Logger, message string, err error) {
	libOpentelemetry.HandleSpanError(span, message, err)
	libLog.SafeError(logger, ctx, message, err, false)
}

func badRequest(
	ctx context.Context,
	fiberCtx *fiber.Ctx,
	span trace.Span,
	logger libLog.Logger,
	message string,
	err error,
) error {
	logSpanError(ctx, span, logger, message, err)

	return libHTTP.RespondError(fiberCtx, fiber.StatusBadRequest, "invalid_request", message)
}

func notFound(
	ctx context.Context,
	fiberCtx *fiber.Ctx,
	span trace.Span,
	logger libLog.Logger,
	message string,
	err error,
) error {
	logSpanError(ctx, span, logger, message, err)

	return libHTTP.RespondError(fiberCtx, fiber.StatusNotFound, "not_found", message)
}

func forbidden(ctx context.Context, fiberCtx *fiber.Ctx, span trace.Span, logger libLog.Logger, err error) error {
	const message = "access denied"

	if err == nil {
		err = fmt.Errorf("%w: %s", errForbidden, message)
	}

	libOpentelemetry.HandleSpanError(span, message, err)

	logger.Log(ctx, libLog.LevelWarn, "access denied: "+message)

	return libHTTP.RespondError(fiberCtx, fiber.StatusForbidden, "forbidden", message)
}

// handleContextVerificationError maps errors from ParseAndVerifyTenantScopedID to HTTP responses.
func handleContextVerificationError(
	ctx context.Context,
	fiberCtx *fiber.Ctx,
	span trace.Span,
	logger libLog.Logger,
	err error,
) error {
	if errors.Is(err, libHTTP.ErrMissingContextID) ||
		errors.Is(err, libHTTP.ErrInvalidContextID) {
		return badRequest(ctx, fiberCtx, span, logger, "invalid context_id", err)
	}

	// Missing or invalid tenant ID → unauthorized
	if errors.Is(err, libHTTP.ErrTenantIDNotFound) ||
		errors.Is(err, libHTTP.ErrInvalidTenantID) {
		logSpanError(ctx, span, logger, "invalid tenant id", err)

		return libHTTP.RespondError(fiberCtx, fiber.StatusUnauthorized, "unauthorized", "unauthorized")
	}

	if errors.Is(err, libHTTP.ErrContextNotFound) {
		return notFound(ctx, fiberCtx, span, logger, "context not found", err)
	}

	if errors.Is(err, libHTTP.ErrContextNotActive) {
		logSpanError(ctx, span, logger, "context not active", err)

		return libHTTP.RespondError(fiberCtx, fiber.StatusForbidden, "context_not_active", "context is not active")
	}

	if errors.Is(err, libHTTP.ErrContextLookupFailed) {
		logSpanError(ctx, span, logger, "context lookup failed", err)

		return libHTTP.RespondError(fiberCtx, fiber.StatusInternalServerError, "internal_server_error", "an unexpected error occurred")
	}

	return forbidden(ctx, fiberCtx, span, logger, err)
}

func parseDashboardFilter(
	fiberCtx *fiber.Ctx,
	contextID uuid.UUID,
) (entities.DashboardFilter, error) {
	dateFrom, dateTo, err := parseDateFilter(fiberCtx)
	if err != nil {
		return entities.DashboardFilter{}, err
	}

	sourceID, err := parseSourceIDFilter(fiberCtx)
	if err != nil {
		return entities.DashboardFilter{}, err
	}

	return entities.DashboardFilter{
		ContextID: contextID,
		DateFrom:  dateFrom,
		DateTo:    dateTo,
		SourceID:  sourceID,
	}, nil
}

func parseDateFilter(fiberCtx *fiber.Ctx) (time.Time, time.Time, error) {
	dateFromStr := fiberCtx.Query("date_from")
	if dateFromStr == "" {
		return time.Time{}, time.Time{}, ErrDateFromRequired
	}

	dateToStr := fiberCtx.Query("date_to")
	if dateToStr == "" {
		return time.Time{}, time.Time{}, ErrDateToRequired
	}

	dateFrom, err := time.Parse(time.DateOnly, dateFromStr)
	if err != nil {
		return time.Time{}, time.Time{}, ErrInvalidDateRange
	}

	dateTo, err := time.Parse(time.DateOnly, dateToStr)
	if err != nil {
		return time.Time{}, time.Time{}, ErrInvalidDateRange
	}

	dateTo = dateTo.Add(hoursPerDay*time.Hour - time.Nanosecond)

	if dateFrom.After(dateTo) {
		return time.Time{}, time.Time{}, ErrInvalidDateRange
	}

	if dateTo.Sub(dateFrom).Hours() > float64(maxDateRangeDays*hoursPerDay) {
		return time.Time{}, time.Time{}, ErrDateRangeExceeded
	}

	return dateFrom, dateTo, nil
}

func parseSourceIDFilter(fiberCtx *fiber.Ctx) (*uuid.UUID, error) {
	sourceIDStr := fiberCtx.Query("source_id")
	if sourceIDStr == "" {
		return nil, nil
	}

	sourceID, err := uuid.Parse(sourceIDStr)
	if err != nil {
		return nil, ErrInvalidSourceID
	}

	return &sourceID, nil
}

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
// @Failure 400 {object} libHTTP.ErrorResponse "Invalid query parameters"
// @Failure 401 {object} libHTTP.ErrorResponse "Unauthorized"
// @Failure 403 {object} libHTTP.ErrorResponse "Forbidden"
// @Failure 404 {object} libHTTP.ErrorResponse "Context not found"
// @Failure 500 {object} libHTTP.ErrorResponse "Internal server error"
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
		return handleContextVerificationError(ctx, fiberCtx, span, logger, err)
	}

	libHTTP.SetHandlerSpanAttributes(span, tenantID, contextID)

	filter, err := parseDashboardFilter(fiberCtx, contextID)
	if err != nil {
		return badRequest(ctx, fiberCtx, span, logger, err.Error(), err)
	}

	stats, err := handler.dashboardUC.GetVolumeStats(ctx, filter)
	if err != nil {
		logSpanError(ctx, span, logger, "failed to get volume stats", err)

		return libHTTP.RespondError(fiberCtx, fiber.StatusInternalServerError, "internal_server_error", "an unexpected error occurred")
	}

	return libHTTP.Respond(fiberCtx, fiber.StatusOK, dto.VolumeStatsToResponse(stats))
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
// @Failure 400 {object} libHTTP.ErrorResponse "Invalid query parameters"
// @Failure 401 {object} libHTTP.ErrorResponse "Unauthorized"
// @Failure 403 {object} libHTTP.ErrorResponse "Forbidden"
// @Failure 404 {object} libHTTP.ErrorResponse "Context not found"
// @Failure 500 {object} libHTTP.ErrorResponse "Internal server error"
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
		return handleContextVerificationError(ctx, fiberCtx, span, logger, err)
	}

	libHTTP.SetHandlerSpanAttributes(span, tenantID, contextID)

	filter, err := parseDashboardFilter(fiberCtx, contextID)
	if err != nil {
		return badRequest(ctx, fiberCtx, span, logger, err.Error(), err)
	}

	stats, err := handler.dashboardUC.GetMatchRateStats(ctx, filter)
	if err != nil {
		logSpanError(ctx, span, logger, "failed to get match rate stats", err)

		return libHTTP.RespondError(fiberCtx, fiber.StatusInternalServerError, "internal_server_error", "an unexpected error occurred")
	}

	return libHTTP.Respond(fiberCtx, fiber.StatusOK, dto.MatchRateStatsToResponse(stats))
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
// @Failure 400 {object} libHTTP.ErrorResponse "Invalid query parameters"
// @Failure 401 {object} libHTTP.ErrorResponse "Unauthorized"
// @Failure 403 {object} libHTTP.ErrorResponse "Forbidden"
// @Failure 404 {object} libHTTP.ErrorResponse "Context not found"
// @Failure 500 {object} libHTTP.ErrorResponse "Internal server error"
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
		return handleContextVerificationError(ctx, fiberCtx, span, logger, err)
	}

	libHTTP.SetHandlerSpanAttributes(span, tenantID, contextID)

	filter, err := parseDashboardFilter(fiberCtx, contextID)
	if err != nil {
		return badRequest(ctx, fiberCtx, span, logger, err.Error(), err)
	}

	stats, err := handler.dashboardUC.GetSLAStats(ctx, filter)
	if err != nil {
		logSpanError(ctx, span, logger, "failed to get sla stats", err)

		return libHTTP.RespondError(fiberCtx, fiber.StatusInternalServerError, "internal_server_error", "an unexpected error occurred")
	}

	return libHTTP.Respond(fiberCtx, fiber.StatusOK, dto.SLAStatsToResponse(stats))
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
// @Failure 400 {object} libHTTP.ErrorResponse "Invalid query parameters"
// @Failure 401 {object} libHTTP.ErrorResponse "Unauthorized"
// @Failure 403 {object} libHTTP.ErrorResponse "Forbidden"
// @Failure 404 {object} libHTTP.ErrorResponse "Context not found"
// @Failure 500 {object} libHTTP.ErrorResponse "Internal server error"
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
		return handleContextVerificationError(ctx, fiberCtx, span, logger, err)
	}

	libHTTP.SetHandlerSpanAttributes(span, tenantID, contextID)

	filter, err := parseDashboardFilter(fiberCtx, contextID)
	if err != nil {
		return badRequest(ctx, fiberCtx, span, logger, err.Error(), err)
	}

	aggregates, err := handler.dashboardUC.GetDashboardAggregates(ctx, filter)
	if err != nil {
		logSpanError(ctx, span, logger, "failed to get dashboard aggregates", err)

		return libHTTP.RespondError(fiberCtx, fiber.StatusInternalServerError, "internal_server_error", "an unexpected error occurred")
	}

	return libHTTP.Respond(fiberCtx, fiber.StatusOK, dto.DashboardAggregatesToResponse(aggregates))
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
// @Failure 400 {object} libHTTP.ErrorResponse "Invalid query parameters"
// @Failure 401 {object} libHTTP.ErrorResponse "Unauthorized"
// @Failure 403 {object} libHTTP.ErrorResponse "Forbidden"
// @Failure 404 {object} libHTTP.ErrorResponse "Context not found"
// @Failure 500 {object} libHTTP.ErrorResponse "Internal server error"
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
		return handleContextVerificationError(ctx, fiberCtx, span, logger, err)
	}

	libHTTP.SetHandlerSpanAttributes(span, tenantID, contextID)

	filter, err := parseDashboardFilter(fiberCtx, contextID)
	if err != nil {
		return badRequest(ctx, fiberCtx, span, logger, err.Error(), err)
	}

	metrics, err := handler.dashboardUC.GetMatcherDashboardMetrics(ctx, filter)
	if err != nil {
		logSpanError(ctx, span, logger, "failed to get matcher dashboard metrics", err)

		return libHTTP.RespondError(fiberCtx, fiber.StatusInternalServerError, "internal_server_error", "an unexpected error occurred")
	}

	return libHTTP.Respond(fiberCtx, fiber.StatusOK, dto.MatcherDashboardMetricsToResponse(metrics))
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
// @Failure 400 {object} libHTTP.ErrorResponse "Invalid query parameters"
// @Failure 401 {object} libHTTP.ErrorResponse "Unauthorized"
// @Failure 403 {object} libHTTP.ErrorResponse "Forbidden"
// @Failure 404 {object} libHTTP.ErrorResponse "Context not found"
// @Failure 500 {object} libHTTP.ErrorResponse "Internal server error"
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
		return handleContextVerificationError(ctx, fiberCtx, span, logger, err)
	}

	libHTTP.SetHandlerSpanAttributes(span, tenantID, contextID)

	filter, err := parseDashboardFilter(fiberCtx, contextID)
	if err != nil {
		return badRequest(ctx, fiberCtx, span, logger, err.Error(), err)
	}

	breakdowns, err := handler.dashboardUC.GetSourceBreakdown(ctx, filter)
	if err != nil {
		logSpanError(ctx, span, logger, "failed to get source breakdown", err)

		return libHTTP.RespondError(fiberCtx, fiber.StatusInternalServerError, "internal_server_error", "an unexpected error occurred")
	}

	return libHTTP.Respond(fiberCtx, fiber.StatusOK, dto.SourceBreakdownToResponse(breakdowns))
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
// @Failure 400 {object} libHTTP.ErrorResponse "Invalid query parameters"
// @Failure 401 {object} libHTTP.ErrorResponse "Unauthorized"
// @Failure 403 {object} libHTTP.ErrorResponse "Forbidden"
// @Failure 404 {object} libHTTP.ErrorResponse "Context not found"
// @Failure 500 {object} libHTTP.ErrorResponse "Internal server error"
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
		return handleContextVerificationError(ctx, fiberCtx, span, logger, err)
	}

	libHTTP.SetHandlerSpanAttributes(span, tenantID, contextID)

	filter, err := parseDashboardFilter(fiberCtx, contextID)
	if err != nil {
		return badRequest(ctx, fiberCtx, span, logger, err.Error(), err)
	}

	summary, err := handler.dashboardUC.GetCashImpactSummary(ctx, filter)
	if err != nil {
		logSpanError(ctx, span, logger, "failed to get cash impact summary", err)

		return libHTTP.RespondError(fiberCtx, fiber.StatusInternalServerError, "internal_server_error", "an unexpected error occurred")
	}

	return libHTTP.Respond(fiberCtx, fiber.StatusOK, dto.CashImpactSummaryToResponse(summary))
}

// countFn produces a count for a given report filter.
type countFn func(ctx context.Context, filter entities.ReportFilter) (int64, error)

func (handler *Handlers) handleCount(
	fiberCtx *fiber.Ctx,
	spanName string,
	fn countFn,
) error {
	ctx, span, logger := startHandlerSpan(fiberCtx, spanName)
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

	filter, err := parseReportFilter(fiberCtx, contextID)
	if err != nil {
		return badRequest(ctx, fiberCtx, span, logger, err.Error(), err)
	}

	count, err := fn(ctx, filter)
	if err != nil {
		logSpanError(ctx, span, logger, "failed to count records", err)

		return libHTTP.RespondError(fiberCtx, fiber.StatusInternalServerError, "internal_server_error", "an unexpected error occurred")
	}

	return libHTTP.Respond(fiberCtx, fiber.StatusOK, dto.ExportCountResponse{Count: count})
}

// CountMatched handles GET /v1/reports/contexts/:contextId/matches/count
// @ID countMatched
// @Summary Count matched records
// @Description Returns the total count of matched records for the specified filters.
// @Description Used to decide between sync download (<1000 rows) and async export job.
// @Tags Reporting
// @Produce json
// @Security BearerAuth
// @Param X-Request-Id header string false "Request ID for tracing"
// @Param contextId path string true "Context ID" format(uuid)
// @Param date_from query string true "Start date (YYYY-MM-DD)" format(date)
// @Param date_to query string true "End date (YYYY-MM-DD)" format(date)
// @Param source_id query string false "Source ID filter"
// @Success 200 {object} dto.ExportCountResponse
// @Failure 400 {object} libHTTP.ErrorResponse "Invalid query parameters"
// @Failure 401 {object} libHTTP.ErrorResponse "Unauthorized"
// @Failure 403 {object} libHTTP.ErrorResponse "Forbidden"
// @Failure 404 {object} libHTTP.ErrorResponse "Context not found"
// @Failure 500 {object} libHTTP.ErrorResponse "Internal server error"
// @Router /v1/reports/contexts/{contextId}/matches/count [get]
func (handler *Handlers) CountMatched(fiberCtx *fiber.Ctx) error {
	return handler.handleCount(
		fiberCtx,
		"handler.reporting.count_matched",
		handler.exportUC.CountMatched,
	)
}

// CountTransactions handles GET /v1/reports/contexts/:contextId/transactions/count
// @ID countTransactions
// @Summary Count all transactions
// @Description Returns the total count of all transactions for the specified filters.
// @Description Used to decide between sync download (<1000 rows) and async export job.
// @Tags Reporting
// @Produce json
// @Security BearerAuth
// @Param X-Request-Id header string false "Request ID for tracing"
// @Param contextId path string true "Context ID" format(uuid)
// @Param date_from query string true "Start date (YYYY-MM-DD)" format(date)
// @Param date_to query string true "End date (YYYY-MM-DD)" format(date)
// @Param source_id query string false "Source ID filter"
// @Success 200 {object} dto.ExportCountResponse
// @Failure 400 {object} libHTTP.ErrorResponse "Invalid query parameters"
// @Failure 401 {object} libHTTP.ErrorResponse "Unauthorized"
// @Failure 403 {object} libHTTP.ErrorResponse "Forbidden"
// @Failure 404 {object} libHTTP.ErrorResponse "Context not found"
// @Failure 500 {object} libHTTP.ErrorResponse "Internal server error"
// @Router /v1/reports/contexts/{contextId}/transactions/count [get]
func (handler *Handlers) CountTransactions(fiberCtx *fiber.Ctx) error {
	return handler.handleCount(
		fiberCtx,
		"handler.reporting.count_transactions",
		handler.exportUC.CountTransactions,
	)
}

// CountExceptions handles GET /v1/reports/contexts/:contextId/exceptions/count
// @ID countExceptions
// @Summary Count exceptions
// @Description Returns the total count of exceptions for the specified filters.
// @Description Used to decide between sync download (<1000 rows) and async export job.
// @Tags Reporting
// @Produce json
// @Security BearerAuth
// @Param X-Request-Id header string false "Request ID for tracing"
// @Param contextId path string true "Context ID" format(uuid)
// @Param date_from query string true "Start date (YYYY-MM-DD)" format(date)
// @Param date_to query string true "End date (YYYY-MM-DD)" format(date)
// @Param source_id query string false "Source ID filter"
// @Success 200 {object} dto.ExportCountResponse
// @Failure 400 {object} libHTTP.ErrorResponse "Invalid query parameters"
// @Failure 401 {object} libHTTP.ErrorResponse "Unauthorized"
// @Failure 403 {object} libHTTP.ErrorResponse "Forbidden"
// @Failure 404 {object} libHTTP.ErrorResponse "Context not found"
// @Failure 500 {object} libHTTP.ErrorResponse "Internal server error"
// @Router /v1/reports/contexts/{contextId}/exceptions/count [get]
func (handler *Handlers) CountExceptions(fiberCtx *fiber.Ctx) error {
	return handler.handleCount(
		fiberCtx,
		"handler.reporting.count_exceptions",
		handler.exportUC.CountExceptions,
	)
}

func parseReportFilter(fiberCtx *fiber.Ctx, contextID uuid.UUID) (entities.ReportFilter, error) {
	dateFrom, dateTo, err := parseDateFilter(fiberCtx)
	if err != nil {
		return entities.ReportFilter{}, err
	}

	sourceID, err := parseSourceIDFilter(fiberCtx)
	if err != nil {
		return entities.ReportFilter{}, err
	}

	cursor, limit, err := libHTTP.ParseOpaqueCursorPagination(fiberCtx)
	if err != nil {
		return entities.ReportFilter{}, fmt.Errorf("invalid pagination: %w", err)
	}

	sortOrder, err := parseSortOrder(fiberCtx)
	if err != nil {
		return entities.ReportFilter{}, err
	}

	return entities.ReportFilter{
		ContextID: contextID,
		DateFrom:  dateFrom,
		DateTo:    dateTo,
		SourceID:  sourceID,
		Limit:     limit,
		Cursor:    cursor,
		SortOrder: sortOrder,
	}, nil
}

func parseVarianceReportFilter(
	fiberCtx *fiber.Ctx,
	contextID uuid.UUID,
) (entities.VarianceReportFilter, error) {
	dateFrom, dateTo, err := parseDateFilter(fiberCtx)
	if err != nil {
		return entities.VarianceReportFilter{}, err
	}

	sourceID, err := parseSourceIDFilter(fiberCtx)
	if err != nil {
		return entities.VarianceReportFilter{}, err
	}

	cursor, limit, err := libHTTP.ParseOpaqueCursorPagination(fiberCtx)
	if err != nil {
		return entities.VarianceReportFilter{}, fmt.Errorf("invalid pagination: %w", err)
	}

	sortOrder, err := parseSortOrder(fiberCtx)
	if err != nil {
		return entities.VarianceReportFilter{}, err
	}

	return entities.VarianceReportFilter{
		ContextID: contextID,
		DateFrom:  dateFrom,
		DateTo:    dateTo,
		SourceID:  sourceID,
		Limit:     limit,
		Cursor:    cursor,
		SortOrder: sortOrder,
	}, nil
}

// parseSortOrder validates and normalizes the sort_order query parameter.
// Accepts "asc" or "desc" (case-insensitive), defaults to "desc" when empty.
func parseSortOrder(fiberCtx *fiber.Ctx) (string, error) {
	raw := fiberCtx.Query("sort_order", "desc")
	normalized := strings.ToLower(raw)

	if normalized != "asc" && normalized != "desc" {
		return "", ErrInvalidSortOrder
	}

	return normalized, nil
}

// exportFn produces export data for a given format.
// Return ErrInvalidExportFormat for unsupported formats.
type exportFn func(ctx context.Context, filter entities.ReportFilter, format string) ([]byte, string, string, error)

func (handler *Handlers) handleExport(
	fiberCtx *fiber.Ctx,
	spanName string,
	fn exportFn,
) error {
	ctx, span, logger := startHandlerSpan(fiberCtx, spanName)
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

	filter, err := parseReportFilter(fiberCtx, contextID)
	if err != nil {
		return badRequest(ctx, fiberCtx, span, logger, err.Error(), err)
	}

	format := fiberCtx.Query("format", formatCSV)

	data, contentType, filename, err := fn(ctx, filter, format)
	if err != nil {
		if errors.Is(err, ErrInvalidExportFormat) {
			return badRequest(ctx, fiberCtx, span, logger, "invalid format", err)
		}

		logSpanError(ctx, span, logger, "failed to export report", err)

		return libHTTP.RespondError(fiberCtx, fiber.StatusInternalServerError, "internal_server_error", "an unexpected error occurred")
	}

	fiberCtx.Set("Content-Type", contentType)
	fiberCtx.Set("Content-Disposition", contentDispoFmt+filename+"\"")

	return fiberCtx.Send(data)
}

// ExportMatchedReport handles GET /v1/reports/contexts/:contextId/matched/export
// @ID exportMatchedReport
// @Summary Export matched transactions report
// @Description Exports matched transactions report in CSV or PDF format for the specified date range.
// @Tags Reporting
// @Produce text/csv,application/pdf,application/json
// @Security BearerAuth
// @Param X-Request-Id header string false "Request ID for tracing"
// @Param contextId path string true "Context ID" format(uuid)
// @Param date_from query string true "Start date (YYYY-MM-DD)" format(date)
// @Param date_to query string true "End date (YYYY-MM-DD)" format(date)
// @Param source_id query string false "Source ID filter"
// @Param format query string false "Export format (csv or pdf)" default(csv)
// @Success 200 {file} file
// @Failure 400 {object} libHTTP.ErrorResponse "Invalid query parameters"
// @Failure 401 {object} libHTTP.ErrorResponse "Unauthorized"
// @Failure 403 {object} libHTTP.ErrorResponse "Forbidden"
// @Failure 404 {object} libHTTP.ErrorResponse "Context not found"
// @Failure 500 {object} libHTTP.ErrorResponse "Internal server error"
// @Router /v1/reports/contexts/{contextId}/matched/export [get]
func (handler *Handlers) ExportMatchedReport(fiberCtx *fiber.Ctx) error {
	return handler.handleExport(
		fiberCtx,
		"handler.reporting.export_matched_report",
		func(ctx context.Context, filter entities.ReportFilter, format string) ([]byte, string, string, error) {
			switch format {
			case formatCSV:
				data, err := handler.exportUC.ExportMatchedCSV(ctx, filter)
				if err != nil {
					return nil, "", "", fmt.Errorf("export matched csv: %w", err)
				}

				return data, contentTypeCSV, "matched_report.csv", nil
			case formatPDF:
				data, err := handler.exportUC.ExportMatchedPDF(ctx, filter)
				if err != nil {
					return nil, "", "", fmt.Errorf("export matched pdf: %w", err)
				}

				return data, contentTypePDF, "matched_report.pdf", nil
			default:
				return nil, "", "", ErrInvalidExportFormat
			}
		},
	)
}

// ExportUnmatchedReport handles GET /v1/reports/contexts/:contextId/unmatched/export
// @ID exportUnmatchedReport
// @Summary Export unmatched transactions report
// @Description Exports unmatched transactions report in CSV or PDF format for the specified date range.
// @Tags Reporting
// @Produce text/csv,application/pdf,application/json
// @Security BearerAuth
// @Param X-Request-Id header string false "Request ID for tracing"
// @Param contextId path string true "Context ID" format(uuid)
// @Param date_from query string true "Start date (YYYY-MM-DD)" format(date)
// @Param date_to query string true "End date (YYYY-MM-DD)" format(date)
// @Param source_id query string false "Source ID filter"
// @Param format query string false "Export format (csv or pdf)" default(csv)
// @Success 200 {file} file
// @Failure 400 {object} libHTTP.ErrorResponse "Invalid query parameters"
// @Failure 401 {object} libHTTP.ErrorResponse "Unauthorized"
// @Failure 403 {object} libHTTP.ErrorResponse "Forbidden"
// @Failure 404 {object} libHTTP.ErrorResponse "Context not found"
// @Failure 500 {object} libHTTP.ErrorResponse "Internal server error"
// @Router /v1/reports/contexts/{contextId}/unmatched/export [get]
func (handler *Handlers) ExportUnmatchedReport(fiberCtx *fiber.Ctx) error {
	return handler.handleExport(
		fiberCtx,
		"handler.reporting.export_unmatched_report",
		func(ctx context.Context, filter entities.ReportFilter, format string) ([]byte, string, string, error) {
			switch format {
			case formatCSV:
				data, err := handler.exportUC.ExportUnmatchedCSV(ctx, filter)
				if err != nil {
					return nil, "", "", fmt.Errorf("export unmatched csv: %w", err)
				}

				return data, contentTypeCSV, "unmatched_report.csv", nil
			case formatPDF:
				data, err := handler.exportUC.ExportUnmatchedPDF(ctx, filter)
				if err != nil {
					return nil, "", "", fmt.Errorf("export unmatched pdf: %w", err)
				}

				return data, contentTypePDF, "unmatched_report.pdf", nil
			default:
				return nil, "", "", ErrInvalidExportFormat
			}
		},
	)
}

// ExportSummaryReport handles GET /v1/reports/contexts/:contextId/summary/export
// @ID exportSummaryReport
// @Summary Export summary report
// @Description Exports reconciliation summary report in CSV or PDF format for the specified date range.
// @Tags Reporting
// @Produce text/csv,application/pdf,application/json
// @Security BearerAuth
// @Param X-Request-Id header string false "Request ID for tracing"
// @Param contextId path string true "Context ID" format(uuid)
// @Param date_from query string true "Start date (YYYY-MM-DD)" format(date)
// @Param date_to query string true "End date (YYYY-MM-DD)" format(date)
// @Param source_id query string false "Source ID filter"
// @Param format query string false "Export format (csv or pdf)" default(csv)
// @Success 200 {file} file
// @Failure 400 {object} libHTTP.ErrorResponse "Invalid query parameters"
// @Failure 401 {object} libHTTP.ErrorResponse "Unauthorized"
// @Failure 403 {object} libHTTP.ErrorResponse "Forbidden"
// @Failure 404 {object} libHTTP.ErrorResponse "Context not found"
// @Failure 500 {object} libHTTP.ErrorResponse "Internal server error"
// @Router /v1/reports/contexts/{contextId}/summary/export [get]
func (handler *Handlers) ExportSummaryReport(fiberCtx *fiber.Ctx) error {
	return handler.handleExport(
		fiberCtx,
		"handler.reporting.export_summary_report",
		func(ctx context.Context, filter entities.ReportFilter, format string) ([]byte, string, string, error) {
			switch format {
			case formatCSV:
				data, err := handler.exportUC.ExportSummaryCSV(ctx, filter)
				if err != nil {
					return nil, "", "", fmt.Errorf("export summary csv: %w", err)
				}

				return data, contentTypeCSV, "summary_report.csv", nil
			case formatPDF:
				data, err := handler.exportUC.ExportSummaryPDF(ctx, filter)
				if err != nil {
					return nil, "", "", fmt.Errorf("export summary pdf: %w", err)
				}

				return data, contentTypePDF, "summary_report.pdf", nil
			default:
				return nil, "", "", ErrInvalidExportFormat
			}
		},
	)
}

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
// @Failure 400 {object} libHTTP.ErrorResponse "Invalid query parameters"
// @Failure 401 {object} libHTTP.ErrorResponse "Unauthorized"
// @Failure 403 {object} libHTTP.ErrorResponse "Forbidden"
// @Failure 404 {object} libHTTP.ErrorResponse "Context not found"
// @Failure 500 {object} libHTTP.ErrorResponse "Internal server error"
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
		return handleContextVerificationError(ctx, fiberCtx, span, logger, err)
	}

	libHTTP.SetHandlerSpanAttributes(span, tenantID, contextID)

	filter, err := parseReportFilter(fiberCtx, contextID)
	if err != nil {
		return badRequest(ctx, fiberCtx, span, logger, err.Error(), err)
	}

	items, pagination, err := handler.exportUC.GetMatchedReport(ctx, filter)
	if err != nil {
		logSpanError(ctx, span, logger, "failed to get matched report", err)

		return libHTTP.RespondError(fiberCtx, fiber.StatusInternalServerError, "internal_server_error", "an unexpected error occurred")
	}

	return libHTTP.Respond(fiberCtx, fiber.StatusOK, dto.ListMatchedReportResponse{
		Items:      dto.MatchedItemsToResponse(items),
		Pagination: pagination,
	})
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
// @Failure 400 {object} libHTTP.ErrorResponse "Invalid query parameters"
// @Failure 401 {object} libHTTP.ErrorResponse "Unauthorized"
// @Failure 403 {object} libHTTP.ErrorResponse "Forbidden"
// @Failure 404 {object} libHTTP.ErrorResponse "Context not found"
// @Failure 500 {object} libHTTP.ErrorResponse "Internal server error"
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
		return handleContextVerificationError(ctx, fiberCtx, span, logger, err)
	}

	libHTTP.SetHandlerSpanAttributes(span, tenantID, contextID)

	filter, err := parseReportFilter(fiberCtx, contextID)
	if err != nil {
		return badRequest(ctx, fiberCtx, span, logger, err.Error(), err)
	}

	items, pagination, err := handler.exportUC.GetUnmatchedReport(ctx, filter)
	if err != nil {
		logSpanError(ctx, span, logger, "failed to get unmatched report", err)

		return libHTTP.RespondError(fiberCtx, fiber.StatusInternalServerError, "internal_server_error", "an unexpected error occurred")
	}

	return libHTTP.Respond(fiberCtx, fiber.StatusOK, dto.ListUnmatchedReportResponse{
		Items:      dto.UnmatchedItemsToResponse(items),
		Pagination: pagination,
	})
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
// @Failure 400 {object} libHTTP.ErrorResponse "Invalid query parameters"
// @Failure 401 {object} libHTTP.ErrorResponse "Unauthorized"
// @Failure 403 {object} libHTTP.ErrorResponse "Forbidden"
// @Failure 404 {object} libHTTP.ErrorResponse "Context not found"
// @Failure 500 {object} libHTTP.ErrorResponse "Internal server error"
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
		return handleContextVerificationError(ctx, fiberCtx, span, logger, err)
	}

	libHTTP.SetHandlerSpanAttributes(span, tenantID, contextID)

	filter, err := parseReportFilter(fiberCtx, contextID)
	if err != nil {
		return badRequest(ctx, fiberCtx, span, logger, err.Error(), err)
	}

	summary, err := handler.exportUC.GetSummaryReport(ctx, filter)
	if err != nil {
		logSpanError(ctx, span, logger, "failed to get summary report", err)

		return libHTTP.RespondError(fiberCtx, fiber.StatusInternalServerError, "internal_server_error", "an unexpected error occurred")
	}

	return libHTTP.Respond(fiberCtx, fiber.StatusOK, dto.SummaryReportToResponse(summary))
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
// @Failure 400 {object} libHTTP.ErrorResponse "Invalid query parameters"
// @Failure 401 {object} libHTTP.ErrorResponse "Unauthorized"
// @Failure 403 {object} libHTTP.ErrorResponse "Forbidden"
// @Failure 404 {object} libHTTP.ErrorResponse "Context not found"
// @Failure 500 {object} libHTTP.ErrorResponse "Internal server error"
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
		return handleContextVerificationError(ctx, fiberCtx, span, logger, err)
	}

	libHTTP.SetHandlerSpanAttributes(span, tenantID, contextID)

	filter, err := parseVarianceReportFilter(fiberCtx, contextID)
	if err != nil {
		return badRequest(ctx, fiberCtx, span, logger, err.Error(), err)
	}

	rows, pagination, err := handler.exportUC.GetVarianceReport(ctx, filter)
	if err != nil {
		logSpanError(ctx, span, logger, "failed to get variance report", err)

		return libHTTP.RespondError(fiberCtx, fiber.StatusInternalServerError, "internal_server_error", "an unexpected error occurred")
	}

	return libHTTP.Respond(fiberCtx, fiber.StatusOK, dto.ListVarianceReportResponse{
		Items:      dto.VarianceRowsToResponse(rows),
		Pagination: pagination,
	})
}

// CountUnmatched handles GET /v1/reports/contexts/:contextId/unmatched/count
// @ID countUnmatched
// @Summary Count unmatched records
// @Description Returns the total count of unmatched records for the specified filters.
// @Description Used to decide between sync download (<1000 rows) and async export job.
// @Tags Reporting
// @Produce json
// @Security BearerAuth
// @Param X-Request-Id header string false "Request ID for tracing"
// @Param contextId path string true "Context ID" format(uuid)
// @Param date_from query string true "Start date (YYYY-MM-DD)" format(date)
// @Param date_to query string true "End date (YYYY-MM-DD)" format(date)
// @Param source_id query string false "Source ID filter"
// @Success 200 {object} dto.ExportCountResponse
// @Failure 400 {object} libHTTP.ErrorResponse "Invalid query parameters"
// @Failure 401 {object} libHTTP.ErrorResponse "Unauthorized"
// @Failure 403 {object} libHTTP.ErrorResponse "Forbidden"
// @Failure 404 {object} libHTTP.ErrorResponse "Context not found"
// @Failure 500 {object} libHTTP.ErrorResponse "Internal server error"
// @Router /v1/reports/contexts/{contextId}/unmatched/count [get]
func (handler *Handlers) CountUnmatched(fiberCtx *fiber.Ctx) error {
	return handler.handleCount(
		fiberCtx,
		"handler.reporting.count_unmatched",
		handler.exportUC.CountUnmatched,
	)
}

// ExportVarianceReport handles GET /v1/reports/contexts/:contextId/variance/export
// @ID exportVarianceReport
// @Summary Export variance report
// @Description Exports variance analysis report in CSV or PDF format for the specified date range.
// @Tags Reporting
// @Produce text/csv,application/pdf,application/json
// @Security BearerAuth
// @Param X-Request-Id header string false "Request ID for tracing"
// @Param contextId path string true "Context ID" format(uuid)
// @Param date_from query string true "Start date (YYYY-MM-DD)" format(date)
// @Param date_to query string true "End date (YYYY-MM-DD)" format(date)
// @Param source_id query string false "Source ID filter"
// @Param format query string false "Export format (csv or pdf)" default(csv)
// @Success 200 {file} file
// @Failure 400 {object} libHTTP.ErrorResponse "Invalid query parameters"
// @Failure 401 {object} libHTTP.ErrorResponse "Unauthorized"
// @Failure 403 {object} libHTTP.ErrorResponse "Forbidden"
// @Failure 404 {object} libHTTP.ErrorResponse "Context not found"
// @Failure 500 {object} libHTTP.ErrorResponse "Internal server error"
// @Router /v1/reports/contexts/{contextId}/variance/export [get]
func (handler *Handlers) ExportVarianceReport(fiberCtx *fiber.Ctx) error {
	ctx, span, logger := startHandlerSpan(fiberCtx, "handler.reporting.export_variance_report")
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

	filter, err := parseVarianceReportFilter(fiberCtx, contextID)
	if err != nil {
		return badRequest(ctx, fiberCtx, span, logger, err.Error(), err)
	}

	format := fiberCtx.Query("format", formatCSV)

	switch format {
	case formatCSV:
		fiberCtx.Set("Content-Type", contentTypeCSV)
		fiberCtx.Set("Content-Disposition", contentDispoFmt+"variance_report.csv\"")

		data, err := handler.exportUC.ExportVarianceCSV(ctx, filter)
		if err != nil {
			logSpanError(ctx, span, logger, "failed to export variance CSV", err)

			return libHTTP.RespondError(fiberCtx, fiber.StatusInternalServerError, "internal_server_error", "an unexpected error occurred")
		}

		return fiberCtx.Send(data)

	case formatPDF:
		data, err := handler.exportUC.ExportVariancePDF(ctx, filter)
		if err != nil {
			logSpanError(ctx, span, logger, "failed to export variance PDF", err)

			return libHTTP.RespondError(fiberCtx, fiber.StatusInternalServerError, "internal_server_error", "an unexpected error occurred")
		}

		fiberCtx.Set("Content-Type", contentTypePDF)
		fiberCtx.Set("Content-Disposition", contentDispoFmt+"variance_report.pdf\"")

		return fiberCtx.Send(data)

	default:
		return badRequest(ctx, fiberCtx, span, logger, "invalid format", ErrInvalidExportFormat)
	}
}
