package http

import (
	"errors"

	"github.com/gofiber/fiber/v2"

	"github.com/LerianStudio/matcher/internal/auth"
)

var (
	// ErrProtectedRouteHelperRequired indicates protected route helper is nil.
	ErrProtectedRouteHelperRequired = errors.New("protected route helper is required")
	// ErrHandlersRequired indicates handlers are nil.
	ErrHandlersRequired = errors.New("reporting handlers are required")
	// ErrExportLimiterRequired indicates export rate limiter is nil.
	ErrExportLimiterRequired = errors.New("export rate limiter is required")
	// ErrExportJobHandlersRequired indicates export job handlers are nil.
	ErrExportJobHandlersRequired = errors.New("export job handlers are required")
)

// RegisterRoutes registers the reporting HTTP routes with export rate limiting.
func RegisterRoutes(
	protected func(resource string, actions ...string) fiber.Router,
	handlers *Handlers,
	exportLimiter fiber.Handler,
) error {
	if protected == nil {
		return ErrProtectedRouteHelperRequired
	}

	if handlers == nil {
		return ErrHandlersRequired
	}

	if exportLimiter == nil {
		return ErrExportLimiterRequired
	}

	protected(
		auth.ResourceReporting,
		auth.ActionDashboardRead,
	).Get("/v1/reports/contexts/:contextId/dashboard", handlers.GetDashboardAggregates)
	protected(
		auth.ResourceReporting,
		auth.ActionDashboardRead,
	).Get("/v1/reports/contexts/:contextId/dashboard/metrics", handlers.GetMatcherDashboardMetrics)
	protected(
		auth.ResourceReporting,
		auth.ActionDashboardRead,
	).Get("/v1/reports/contexts/:contextId/dashboard/volume", handlers.GetVolumeStats)
	protected(
		auth.ResourceReporting,
		auth.ActionDashboardRead,
	).Get("/v1/reports/contexts/:contextId/dashboard/match-rate", handlers.GetMatchRateStats)
	protected(
		auth.ResourceReporting,
		auth.ActionDashboardRead,
	).Get("/v1/reports/contexts/:contextId/dashboard/sla", handlers.GetSLAStats)
	protected(
		auth.ResourceReporting,
		auth.ActionDashboardRead,
	).Get("/v1/reports/contexts/:contextId/dashboard/source-breakdown", handlers.GetSourceBreakdown)
	protected(
		auth.ResourceReporting,
		auth.ActionDashboardRead,
	).Get("/v1/reports/contexts/:contextId/dashboard/cash-impact", handlers.GetCashImpactSummary)

	// Report list routes (paginated browsing)
	protected(
		auth.ResourceReporting,
		auth.ActionExportRead,
	).Get("/v1/reports/contexts/:contextId/matched", handlers.GetMatchedReport)
	protected(
		auth.ResourceReporting,
		auth.ActionExportRead,
	).Get("/v1/reports/contexts/:contextId/unmatched", handlers.GetUnmatchedReport)
	protected(
		auth.ResourceReporting,
		auth.ActionExportRead,
	).Get("/v1/reports/contexts/:contextId/summary", handlers.GetSummaryReport)
	protected(
		auth.ResourceReporting,
		auth.ActionExportRead,
	).Get("/v1/reports/contexts/:contextId/variance", handlers.GetVarianceReport)

	// Count routes
	protected(
		auth.ResourceReporting,
		auth.ActionExportRead,
	).Get("/v1/reports/contexts/:contextId/matches/count", handlers.CountMatched)
	protected(
		auth.ResourceReporting,
		auth.ActionExportRead,
	).Get("/v1/reports/contexts/:contextId/transactions/count", handlers.CountTransactions)
	protected(
		auth.ResourceReporting,
		auth.ActionExportRead,
	).Get("/v1/reports/contexts/:contextId/exceptions/count", handlers.CountExceptions)
	protected(
		auth.ResourceReporting,
		auth.ActionExportRead,
	).Get("/v1/reports/contexts/:contextId/unmatched/count", handlers.CountUnmatched)

	protected(
		auth.ResourceReporting,
		auth.ActionExportRead,
	).Get("/v1/reports/contexts/:contextId/matched/export", exportLimiter, handlers.ExportMatchedReport)
	protected(
		auth.ResourceReporting,
		auth.ActionExportRead,
	).Get("/v1/reports/contexts/:contextId/unmatched/export", exportLimiter, handlers.ExportUnmatchedReport)
	protected(
		auth.ResourceReporting,
		auth.ActionExportRead,
	).Get("/v1/reports/contexts/:contextId/summary/export", exportLimiter, handlers.ExportSummaryReport)
	protected(
		auth.ResourceReporting,
		auth.ActionExportRead,
	).Get("/v1/reports/contexts/:contextId/variance/export", exportLimiter, handlers.ExportVarianceReport)

	return nil
}

// RegisterExportJobRoutes registers the export job HTTP routes.
func RegisterExportJobRoutes(
	protected func(resource string, actions ...string) fiber.Router,
	handlers *ExportJobHandlers,
	exportLimiter fiber.Handler,
) error {
	if protected == nil {
		return ErrProtectedRouteHelperRequired
	}

	if handlers == nil {
		return ErrExportJobHandlersRequired
	}

	if exportLimiter == nil {
		return ErrExportLimiterRequired
	}

	protected(
		auth.ResourceReporting,
		auth.ActionExportJobWrite,
	).Post("/v1/contexts/:contextId/export-jobs", exportLimiter, handlers.CreateExportJob)
	protected(
		auth.ResourceReporting,
		auth.ActionExportJobRead,
	).Get("/v1/contexts/:contextId/export-jobs", handlers.ListExportJobsByContext)
	protected(
		auth.ResourceReporting,
		auth.ActionExportJobRead,
	).Get("/v1/export-jobs", handlers.ListExportJobs)
	protected(
		auth.ResourceReporting,
		auth.ActionExportJobRead,
	).Get("/v1/export-jobs/:jobId", handlers.GetExportJob)
	protected(
		auth.ResourceReporting,
		auth.ActionExportJobWrite,
	).Post("/v1/export-jobs/:jobId/cancel", handlers.CancelExportJob)
	protected(
		auth.ResourceReporting,
		auth.ActionExportJobRead,
	).Get("/v1/export-jobs/:jobId/download", handlers.DownloadExportJob)

	return nil
}
