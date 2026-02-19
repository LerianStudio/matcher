// Package ports defines interfaces for reporting infrastructure.
package ports

//go:generate mockgen -source=cache.go -destination=mocks/dashboard_cache_service_mock.go -package=mocks

import (
	"context"

	"github.com/google/uuid"

	"github.com/LerianStudio/matcher/internal/reporting/domain/entities"
)

// DashboardCacheService provides caching operations for dashboard data.
type DashboardCacheService interface {
	// GetVolumeStats retrieves cached volume stats.
	GetVolumeStats(ctx context.Context, filter entities.DashboardFilter) (*entities.VolumeStats, error)

	// SetVolumeStats caches volume stats.
	SetVolumeStats(ctx context.Context, filter entities.DashboardFilter, stats *entities.VolumeStats) error

	// GetSLAStats retrieves cached SLA stats.
	GetSLAStats(ctx context.Context, filter entities.DashboardFilter) (*entities.SLAStats, error)

	// SetSLAStats caches SLA stats.
	SetSLAStats(ctx context.Context, filter entities.DashboardFilter, stats *entities.SLAStats) error

	// GetMatchRateStats retrieves cached match rate stats.
	GetMatchRateStats(ctx context.Context, filter entities.DashboardFilter) (*entities.MatchRateStats, error)

	// SetMatchRateStats caches match rate stats.
	SetMatchRateStats(ctx context.Context, filter entities.DashboardFilter, stats *entities.MatchRateStats) error

	// GetDashboardAggregates retrieves cached dashboard aggregates.
	GetDashboardAggregates(ctx context.Context, filter entities.DashboardFilter) (*entities.DashboardAggregates, error)

	// SetDashboardAggregates caches dashboard aggregates.
	SetDashboardAggregates(ctx context.Context, filter entities.DashboardFilter, aggregates *entities.DashboardAggregates) error

	// GetMatcherDashboardMetrics retrieves cached matcher dashboard metrics.
	GetMatcherDashboardMetrics(ctx context.Context, filter entities.DashboardFilter) (*entities.MatcherDashboardMetrics, error)

	// SetMatcherDashboardMetrics caches matcher dashboard metrics.
	SetMatcherDashboardMetrics(ctx context.Context, filter entities.DashboardFilter, metrics *entities.MatcherDashboardMetrics) error

	// InvalidateContext invalidates all cached data for a context.
	InvalidateContext(ctx context.Context, contextID uuid.UUID) error
}
