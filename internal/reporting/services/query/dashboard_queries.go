// Package query provides read operations for reporting.
package query

import (
	"context"
	"errors"
	"fmt"
	"time"

	"go.opentelemetry.io/otel/trace"

	libCommons "github.com/LerianStudio/lib-commons/v5/commons"
	"github.com/LerianStudio/lib-commons/v5/commons/errgroup"
	libLog "github.com/LerianStudio/lib-commons/v5/commons/log"
	libOpentelemetry "github.com/LerianStudio/lib-commons/v5/commons/opentelemetry"
	"github.com/LerianStudio/lib-commons/v5/commons/runtime"

	"github.com/LerianStudio/matcher/internal/reporting/domain/entities"
	"github.com/LerianStudio/matcher/internal/reporting/domain/repositories"
	"github.com/LerianStudio/matcher/internal/reporting/ports"
)

// ErrNilDashboardRepository is returned when a nil dashboard repository is provided.
var ErrNilDashboardRepository = errors.New("dashboard repository is required")

// ErrIncompleteDashboardMetrics is returned when one or more dashboard metric queries return nil.
var ErrIncompleteDashboardMetrics = errors.New("incomplete dashboard metrics: one or more queries returned nil")

// DashboardUseCase orchestrates dashboard queries with caching.
type DashboardUseCase struct {
	repo  repositories.DashboardRepository
	cache ports.DashboardCacheService
}

// NewDashboardUseCase creates a new dashboard use case with the required repository.
func NewDashboardUseCase(
	repo repositories.DashboardRepository,
	cache ports.DashboardCacheService,
) (*DashboardUseCase, error) {
	if repo == nil {
		return nil, ErrNilDashboardRepository
	}

	return &DashboardUseCase{
		repo:  repo,
		cache: cache,
	}, nil
}

// GetVolumeStats retrieves volume statistics with cache support.
func (uc *DashboardUseCase) GetVolumeStats(
	ctx context.Context,
	filter entities.DashboardFilter,
) (*entities.VolumeStats, error) {
	logger, tracer, _, _ := libCommons.NewTrackingFromContext(ctx)

	ctx, span := tracer.Start(ctx, "dashboard.query.get_volume_stats")
	defer span.End()

	if uc.cache != nil {
		cached, err := uc.cache.GetVolumeStats(ctx, filter)
		if err == nil && cached != nil {
			return cached, nil
		}
	}

	result, err := uc.repo.GetVolumeStats(ctx, filter)
	if err != nil {
		libOpentelemetry.HandleSpanError(span, "failed to get volume stats", err)

		libLog.SafeError(logger, ctx, "failed to get volume stats", err, runtime.IsProductionMode())

		return nil, fmt.Errorf("getting volume stats: %w", err)
	}

	if uc.cache != nil {
		_ = uc.cache.SetVolumeStats(ctx, filter, result)
	}

	return result, nil
}

// GetSLAStats retrieves SLA statistics with cache support.
func (uc *DashboardUseCase) GetSLAStats(
	ctx context.Context,
	filter entities.DashboardFilter,
) (*entities.SLAStats, error) {
	logger, tracer, _, _ := libCommons.NewTrackingFromContext(ctx)

	ctx, span := tracer.Start(ctx, "dashboard.query.get_sla_stats")
	defer span.End()

	if uc.cache != nil {
		cached, err := uc.cache.GetSLAStats(ctx, filter)
		if err == nil && cached != nil {
			return cached, nil
		}
	}

	result, err := uc.repo.GetSLAStats(ctx, filter)
	if err != nil {
		libOpentelemetry.HandleSpanError(span, "failed to get sla stats", err)

		libLog.SafeError(logger, ctx, "failed to get sla stats", err, runtime.IsProductionMode())

		return nil, fmt.Errorf("getting sla stats: %w", err)
	}

	if uc.cache != nil {
		_ = uc.cache.SetSLAStats(ctx, filter, result)
	}

	return result, nil
}

// GetMatchRateStats retrieves match rate statistics.
func (uc *DashboardUseCase) GetMatchRateStats(
	ctx context.Context,
	filter entities.DashboardFilter,
) (*entities.MatchRateStats, error) {
	logger, tracer, _, _ := libCommons.NewTrackingFromContext(ctx)

	ctx, span := tracer.Start(ctx, "dashboard.query.get_match_rate_stats")
	defer span.End()

	volume, err := uc.GetVolumeStats(ctx, filter)
	if err != nil {
		libOpentelemetry.HandleSpanError(span, "failed to get volume for match rate", err)

		libLog.SafeError(logger, ctx, "failed to get volume for match rate", err, runtime.IsProductionMode())

		return nil, fmt.Errorf("getting volume for match rate: %w", err)
	}

	return entities.CalculateMatchRate(volume), nil
}

// GetDashboardAggregates retrieves all dashboard aggregates with cache support.
func (uc *DashboardUseCase) GetDashboardAggregates(
	ctx context.Context,
	filter entities.DashboardFilter,
) (*entities.DashboardAggregates, error) {
	logger, tracer, _, _ := libCommons.NewTrackingFromContext(ctx)

	ctx, span := tracer.Start(ctx, "dashboard.query.get_dashboard_aggregates")
	defer span.End()

	if uc.cache != nil {
		cached, err := uc.cache.GetDashboardAggregates(ctx, filter)
		if err == nil && cached != nil {
			return cached, nil
		}
	}

	volume, err := uc.GetVolumeStats(ctx, filter)
	if err != nil {
		libOpentelemetry.HandleSpanError(span, "failed to get volume stats", err)

		libLog.SafeError(logger, ctx, "failed to get volume stats", err, runtime.IsProductionMode())

		return nil, fmt.Errorf("getting volume stats: %w", err)
	}

	sla, err := uc.GetSLAStats(ctx, filter)
	if err != nil {
		libOpentelemetry.HandleSpanError(span, "failed to get sla stats", err)

		libLog.SafeError(logger, ctx, "failed to get sla stats", err, runtime.IsProductionMode())

		return nil, fmt.Errorf("getting sla stats: %w", err)
	}

	matchRate := entities.CalculateMatchRate(volume)

	aggregates := &entities.DashboardAggregates{
		Volume:    volume,
		MatchRate: matchRate,
		SLA:       sla,
		UpdatedAt: time.Now().UTC(),
	}

	if uc.cache != nil {
		_ = uc.cache.SetDashboardAggregates(ctx, filter, aggregates)
	}

	return aggregates, nil
}

// GetSourceBreakdown retrieves per-source reconciliation statistics.
// Note: intentionally not cached — these queries aggregate real-time financial data
// where staleness could lead to incorrect operational decisions.
func (uc *DashboardUseCase) GetSourceBreakdown(
	ctx context.Context,
	filter entities.DashboardFilter,
) ([]entities.SourceBreakdown, error) {
	logger, tracer, _, _ := libCommons.NewTrackingFromContext(ctx)

	ctx, span := tracer.Start(ctx, "dashboard.query.get_source_breakdown")
	defer span.End()

	result, err := uc.repo.GetSourceBreakdown(ctx, filter)
	if err != nil {
		libOpentelemetry.HandleSpanError(span, "failed to get source breakdown", err)

		libLog.SafeError(logger, ctx, "failed to get source breakdown", err, runtime.IsProductionMode())

		return nil, fmt.Errorf("getting source breakdown: %w", err)
	}

	return result, nil
}

// GetCashImpactSummary retrieves unreconciled financial exposure.
// Note: intentionally not cached — these queries aggregate real-time financial data
// where staleness could lead to incorrect operational decisions.
func (uc *DashboardUseCase) GetCashImpactSummary(
	ctx context.Context,
	filter entities.DashboardFilter,
) (*entities.CashImpactSummary, error) {
	logger, tracer, _, _ := libCommons.NewTrackingFromContext(ctx)

	ctx, span := tracer.Start(ctx, "dashboard.query.get_cash_impact_summary")
	defer span.End()

	result, err := uc.repo.GetCashImpactSummary(ctx, filter)
	if err != nil {
		libOpentelemetry.HandleSpanError(span, "failed to get cash impact summary", err)

		libLog.SafeError(logger, ctx, "failed to get cash impact summary", err, runtime.IsProductionMode())

		return nil, fmt.Errorf("getting cash impact summary: %w", err)
	}

	return result, nil
}

// GetMatcherDashboardMetrics retrieves comprehensive dashboard metrics for the Command Center.
func (uc *DashboardUseCase) GetMatcherDashboardMetrics(
	ctx context.Context,
	filter entities.DashboardFilter,
) (*entities.MatcherDashboardMetrics, error) {
	logger, tracer, _, _ := libCommons.NewTrackingFromContext(ctx)

	ctx, span := tracer.Start(ctx, "dashboard.query.get_matcher_dashboard_metrics")
	defer span.End()

	if uc.cache != nil {
		cached, err := uc.cache.GetMatcherDashboardMetrics(ctx, filter)
		if err == nil && cached != nil {
			return cached, nil
		}
	}

	var summary *entities.SummaryMetrics

	var trends *entities.TrendMetrics

	var breakdowns *entities.BreakdownMetrics

	group, groupCtx := errgroup.WithContext(ctx)
	group.SetLogger(logger)

	group.Go(func() error {
		var err error

		summary, err = uc.repo.GetSummaryMetrics(groupCtx, filter)
		if err != nil {
			return reportQueryError(groupCtx, span, logger, "summary metrics", err)
		}

		return nil
	})

	group.Go(func() error {
		var err error

		trends, err = uc.repo.GetTrendMetrics(groupCtx, filter)
		if err != nil {
			return reportQueryError(groupCtx, span, logger, "trend metrics", err)
		}

		return nil
	})

	group.Go(func() error {
		var err error

		breakdowns, err = uc.repo.GetBreakdownMetrics(groupCtx, filter)
		if err != nil {
			return reportQueryError(groupCtx, span, logger, "breakdown metrics", err)
		}

		return nil
	})

	if err := group.Wait(); err != nil {
		return nil, fmt.Errorf("fetching dashboard metrics: %w", err)
	}

	// Defensive: ensure all metrics are populated (repos must not return (nil, nil))
	if summary == nil || trends == nil || breakdowns == nil {
		return nil, ErrIncompleteDashboardMetrics
	}

	metrics := &entities.MatcherDashboardMetrics{
		Summary:    summary,
		Trends:     trends,
		Breakdowns: breakdowns,
		UpdatedAt:  time.Now().UTC(),
	}

	if uc.cache != nil {
		_ = uc.cache.SetMatcherDashboardMetrics(ctx, filter, metrics)
	}

	return metrics, nil
}

// reportQueryError handles span error tracking and logging for failed dashboard queries.
func reportQueryError(ctx context.Context, span trace.Span, logger libLog.Logger, msg string, err error) error {
	libOpentelemetry.HandleSpanError(span, "failed to get "+msg, err)

	libLog.SafeError(logger, ctx, "failed to get "+msg, err, runtime.IsProductionMode())

	return fmt.Errorf("getting %s: %w", msg, err)
}
