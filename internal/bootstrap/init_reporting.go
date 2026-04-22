// Copyright 2025 Lerian Studio. All rights reserved.
// Use of this source code is governed by an Elastic License 2.0
// that can be found in the LICENSE.md file.

package bootstrap

import (
	"context"
	"fmt"

	"github.com/gofiber/fiber/v2"

	libLog "github.com/LerianStudio/lib-commons/v5/commons/log"
	"github.com/LerianStudio/lib-commons/v5/commons/net/http/ratelimit"

	reportingHTTP "github.com/LerianStudio/matcher/internal/reporting/adapters/http"
	reportDashboard "github.com/LerianStudio/matcher/internal/reporting/adapters/postgres/dashboard"
	reportExportJob "github.com/LerianStudio/matcher/internal/reporting/adapters/postgres/export_job"
	reportRepo "github.com/LerianStudio/matcher/internal/reporting/adapters/postgres/report"
	reportingRedis "github.com/LerianStudio/matcher/internal/reporting/adapters/redis"
	reportingCommand "github.com/LerianStudio/matcher/internal/reporting/services/command"
	reportingQuery "github.com/LerianStudio/matcher/internal/reporting/services/query"
	reportingWorker "github.com/LerianStudio/matcher/internal/reporting/services/worker"
	crossAdapters "github.com/LerianStudio/matcher/internal/shared/adapters/cross"
	"github.com/LerianStudio/matcher/internal/shared/objectstorage"
	sharedPorts "github.com/LerianStudio/matcher/internal/shared/ports"
)

func initReportingModule(
	routes *Routes,
	cfg *Config,
	configGetter func() *Config,
	settingsResolver *runtimeSettingsResolver,
	provider sharedPorts.InfrastructureProvider,
	storage *objectstorage.Client,
	rateLimiterGetter func() *ratelimit.RateLimiter,
	logger libLog.Logger,
	repos *sharedRepositories,
	production bool,
) (*reportingWorker.ExportWorker, *reportingWorker.CleanupWorker, error) {
	contextAdapter := crossAdapters.NewContextAccessProviderAdapter(repos.configContext)

	dashboardRepository := reportDashboard.NewRepository(provider)
	reportRepository := reportRepo.NewRepository(provider)
	exportJobRepository := reportExportJob.NewRepository(provider)
	cacheService := reportingRedis.NewCacheService(provider, 0)

	dashboardUseCase, err := reportingQuery.NewDashboardUseCase(dashboardRepository, cacheService)
	if err != nil {
		return nil, nil, fmt.Errorf("create dashboard use case: %w", err)
	}

	exportUseCase, err := reportingQuery.NewUseCase(reportRepository)
	if err != nil {
		return nil, nil, fmt.Errorf("create export use case: %w", err)
	}

	reportingHandler, err := reportingHTTP.NewHandlers(
		dashboardUseCase,
		contextAdapter,
		exportUseCase,
		reportRepository,
		production,
	)
	if err != nil {
		return nil, nil, fmt.Errorf("create reporting handler: %w", err)
	}

	exportLimiter := NewExportRateLimit(rateLimiterGetter, cfg, configGetter, settingsResolver)

	if err := reportingHTTP.RegisterRoutes(routes.Protected, reportingHandler, exportLimiter); err != nil {
		return nil, nil, fmt.Errorf("register reporting routes: %w", err)
	}

	return initExportWorkers(
		routes,
		cfg,
		configGetter,
		settingsResolver,
		exportJobRepository,
		reportRepository,
		storage,
		contextAdapter,
		exportLimiter,
		logger,
	)
}

func initExportWorkers(
	routes *Routes,
	cfg *Config,
	configGetter func() *Config,
	settingsResolver *runtimeSettingsResolver,
	exportJobRepository *reportExportJob.Repository,
	reportRepository *reportRepo.Repository,
	storage *objectstorage.Client,
	contextAdapter sharedPorts.ContextAccessProvider,
	exportLimiter fiber.Handler,
	logger libLog.Logger,
) (*reportingWorker.ExportWorker, *reportingWorker.CleanupWorker, error) {
	exportJobUseCase, err := reportingCommand.NewExportJobUseCase(exportJobRepository)
	if err != nil {
		return nil, nil, fmt.Errorf("create export job use case: %w", err)
	}

	exportJobQuerySvc, err := reportingQuery.NewExportJobQueryService(exportJobRepository)
	if err != nil {
		return nil, nil, fmt.Errorf("create export job query service: %w", err)
	}

	exportJobHandler, err := reportingHTTP.NewExportJobHandlers(
		exportJobUseCase,
		exportJobQuerySvc,
		exportJobRepository,
		storage,
		contextAdapter,
		configuredExportPresignExpiry(context.Background(), cfg),
		IsProductionEnvironment(cfg.App.EnvName),
	)
	if err != nil {
		return nil, nil, fmt.Errorf("create export job handler: %w", err)
	}

	exportJobHandler.SetRuntimeEnabled(cfg.ExportWorker.Enabled)

	if configGetter != nil || settingsResolver != nil {
		exportJobHandler.SetRuntimeConfigResolver(func(ctx context.Context) reportingHTTP.ExportJobRuntimeConfig {
			runtimeCfg := runtimeConfigOrFallback(cfg, configGetter)

			enabled := cfg.ExportWorker.Enabled
			if runtimeCfg != nil {
				enabled = runtimeCfg.ExportWorker.Enabled
			}

			return reportingHTTP.ExportJobRuntimeConfig{
				Enabled:       &enabled,
				PresignExpiry: resolveExportPresignExpiry(ctx, cfg, configGetter, settingsResolver),
			}
		})
	}

	if err := reportingHTTP.RegisterExportJobRoutes(routes.Protected, exportJobHandler, exportLimiter); err != nil {
		return nil, nil, fmt.Errorf("register export job routes: %w", err)
	}

	workerCfg := reportingWorker.ExportWorkerConfig{
		PollInterval: cfg.ExportWorkerPollInterval(),
		PageSize:     cfg.ExportWorker.PageSize,
	}

	exportWorker, err := reportingWorker.NewExportWorker(
		exportJobRepository,
		reportRepository,
		storage,
		workerCfg,
		logger,
	)
	if err != nil {
		return nil, nil, fmt.Errorf("create export worker: %w", err)
	}

	cleanupCfg := reportingWorker.CleanupWorkerConfig{
		Interval:              cfg.CleanupWorkerInterval(),
		BatchSize:             cfg.CleanupWorkerBatchSize(),
		FileDeleteGracePeriod: cfg.CleanupWorkerGracePeriod(),
	}

	cleanupWorker, err := reportingWorker.NewCleanupWorker(
		exportJobRepository,
		storage,
		cleanupCfg,
		logger,
	)
	if err != nil {
		return nil, nil, fmt.Errorf("create cleanup worker: %w", err)
	}

	return exportWorker, cleanupWorker, nil
}
