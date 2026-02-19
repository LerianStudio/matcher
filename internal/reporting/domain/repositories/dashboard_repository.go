// Package repositories provides reporting persistence contracts.
package repositories

//go:generate mockgen -destination=mocks/dashboard_repository_mock.go -package=mocks . DashboardRepository

import (
	"context"

	"github.com/LerianStudio/matcher/internal/reporting/domain/entities"
)

// DashboardRepository defines read-only operations for dashboard data.
type DashboardRepository interface {
	// GetVolumeStats retrieves transaction volume statistics.
	GetVolumeStats(
		ctx context.Context,
		filter entities.DashboardFilter,
	) (*entities.VolumeStats, error)

	// GetSLAStats retrieves SLA compliance statistics.
	GetSLAStats(ctx context.Context, filter entities.DashboardFilter) (*entities.SLAStats, error)

	// GetSummaryMetrics retrieves executive summary metrics for the dashboard.
	GetSummaryMetrics(
		ctx context.Context,
		filter entities.DashboardFilter,
	) (*entities.SummaryMetrics, error)

	// GetTrendMetrics retrieves time-series trend data for charts.
	GetTrendMetrics(
		ctx context.Context,
		filter entities.DashboardFilter,
	) (*entities.TrendMetrics, error)

	// GetBreakdownMetrics retrieves categorical aggregations for charts.
	GetBreakdownMetrics(
		ctx context.Context,
		filter entities.DashboardFilter,
	) (*entities.BreakdownMetrics, error)

	// GetSourceBreakdown retrieves per-source reconciliation statistics.
	GetSourceBreakdown(
		ctx context.Context,
		filter entities.DashboardFilter,
	) ([]entities.SourceBreakdown, error)

	// GetCashImpactSummary retrieves unreconciled financial exposure.
	GetCashImpactSummary(
		ctx context.Context,
		filter entities.DashboardFilter,
	) (*entities.CashImpactSummary, error)
}
