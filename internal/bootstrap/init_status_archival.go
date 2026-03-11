// Copyright 2025 Lerian Studio. All rights reserved.
// Use of this source code is governed by an Elastic License 2.0
// that can be found in the LICENSE.md file.

package bootstrap

import (
	"context"
	"database/sql"
	"fmt"
	"time"

	"go.opentelemetry.io/otel"

	libLog "github.com/LerianStudio/lib-commons/v4/commons/log"

	governanceHTTP "github.com/LerianStudio/matcher/internal/governance/adapters/http"
	archiveMetadataRepo "github.com/LerianStudio/matcher/internal/governance/adapters/postgres/archive_metadata"
	governanceCommand "github.com/LerianStudio/matcher/internal/governance/services/command"
	governanceWorker "github.com/LerianStudio/matcher/internal/governance/services/worker"
	reportingStorage "github.com/LerianStudio/matcher/internal/reporting/adapters/storage"
	reportingPorts "github.com/LerianStudio/matcher/internal/reporting/ports"
	"github.com/LerianStudio/matcher/internal/shared/constants"
	sharedPorts "github.com/LerianStudio/matcher/internal/shared/ports"
)

// InfraStatus tracks the status of infrastructure components for consolidated logging.
type InfraStatus struct {
	PostgresConnected      bool
	RedisConnected         bool
	RedisMode              string
	RabbitMQConnected      bool
	ObjectStorageEnabled   bool
	HasReplica             bool
	ExportWorkerEnabled    bool
	CleanupWorkerEnabled   bool
	ArchivalWorkerEnabled  bool
	SchedulerWorkerEnabled bool
	DiscoveryWorkerEnabled bool
	TelemetryConfigured    bool
	TelemetryActive        bool
	TelemetryDegraded      bool
}

func shouldRedactInfraDetails(envName string) bool {
	return IsProductionEnvironment(envName)
}

func safeInfraTarget(envName, value string) string {
	if shouldRedactInfraDetails(envName) {
		return "configured"
	}

	return value
}

func logStartupInfo(logger libLog.Logger, cfg *Config, status *InfraStatus) {
	ctx := context.Background()

	banner := `
                    __       __             
   _________ ______/ /______/ /_  ___  _____
  / __  __ /  __ / __/  ___/ __ \/ _ \/ ___/
 / / / / / / /_/ / /_/ /__/ / / /  __/ /    
/_/ /_/ /_/\__,_/\__/\___/_/ /_/\___/_/     
                       by lerian studio         
`
	logger.Log(ctx, libLog.LevelInfo, banner)

	logger.Log(ctx, libLog.LevelInfo, "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━")
	logger.Log(ctx, libLog.LevelInfo, "  🚀 SERVICE CONFIGURATION")
	logger.Log(ctx, libLog.LevelInfo, "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━")
	logger.Log(ctx, libLog.LevelInfo, "  Environment     : "+cfg.App.EnvName)
	logger.Log(ctx, libLog.LevelInfo, "  Server Address  : "+cfg.Server.Address)
	logger.Log(ctx, libLog.LevelInfo, "  Log Level       : "+cfg.App.LogLevel)
	logger.Log(ctx, libLog.LevelInfo, "")

	logger.Log(ctx, libLog.LevelInfo, "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━")
	logger.Log(ctx, libLog.LevelInfo, "  📦 INFRASTRUCTURE")
	logger.Log(ctx, libLog.LevelInfo, "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━")

	pgStatus := formatConnStatus(status.PostgresConnected)
	pgDisplay := safeInfraTarget(cfg.App.EnvName, cfg.Postgres.PrimaryHost+"/"+cfg.Postgres.PrimaryDB)

	if status.HasReplica {
		pgDisplay += " (+replica)"
	}

	logger.Log(ctx, libLog.LevelInfo, "  PostgreSQL      : "+pgDisplay+" "+pgStatus)

	redisDisplay := safeInfraTarget(cfg.App.EnvName, cfg.Redis.Host)

	if status.RedisMode != "" {
		redisDisplay = redisDisplay + " (" + status.RedisMode + ")"
	}

	logger.Log(ctx, libLog.LevelInfo, "  Redis           : "+redisDisplay+" "+formatConnStatus(status.RedisConnected))
	logger.Log(ctx, libLog.LevelInfo, "  RabbitMQ        : "+safeInfraTarget(cfg.App.EnvName, cfg.RabbitMQ.Host)+" "+formatConnStatus(status.RabbitMQConnected))

	if cfg.ObjectStorage.Endpoint != "" && cfg.ObjectStorage.Bucket != "" {
		objStatus := formatConnStatus(status.ObjectStorageEnabled)
		logger.Log(ctx, libLog.LevelInfo, "  Object Storage  : "+safeInfraTarget(cfg.App.EnvName, cfg.ObjectStorage.Endpoint+"/"+cfg.ObjectStorage.Bucket)+" "+objStatus)
	}

	telemetryStatus := formatFeatureStatus(status.TelemetryConfigured)

	if status.TelemetryDegraded {
		telemetryStatus = "degraded ⚠ (collector unavailable at startup)"
	}

	logger.Log(ctx, libLog.LevelInfo, "  Telemetry       : "+telemetryStatus)
	logger.Log(ctx, libLog.LevelInfo, "")

	logger.Log(ctx, libLog.LevelInfo, "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━")
	logger.Log(ctx, libLog.LevelInfo, "  🔧 FEATURES")
	logger.Log(ctx, libLog.LevelInfo, "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━")
	logger.Log(ctx, libLog.LevelInfo, "  Authentication  : "+formatFeatureStatus(cfg.Auth.Enabled))
	logger.Log(ctx, libLog.LevelInfo, "  Multi-Tenant    : "+formatFeatureStatus(cfg.Tenancy.MultiTenantInfraEnabled))
	logger.Log(ctx, libLog.LevelInfo, "")

	logger.Log(ctx, libLog.LevelInfo, "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━")
	logger.Log(ctx, libLog.LevelInfo, "  ⚙️  WORKERS")
	logger.Log(ctx, libLog.LevelInfo, "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━")
	logger.Log(ctx, libLog.LevelInfo, "  Export Worker   : "+formatWorkerStatus(status.ExportWorkerEnabled, cfg.ExportWorkerPollInterval()))
	logger.Log(ctx, libLog.LevelInfo, "  Cleanup Worker  : "+formatWorkerStatus(status.CleanupWorkerEnabled, time.Hour))
	logger.Log(ctx, libLog.LevelInfo, "  Archival Worker : "+formatWorkerStatus(status.ArchivalWorkerEnabled, cfg.ArchivalInterval()))
	logger.Log(ctx, libLog.LevelInfo, "  Scheduler Worker: "+formatWorkerStatus(status.SchedulerWorkerEnabled, time.Minute))
	logger.Log(ctx, libLog.LevelInfo, "  Discovery Worker: "+formatWorkerStatus(status.DiscoveryWorkerEnabled, cfg.FetcherDiscoveryInterval()))
	logger.Log(ctx, libLog.LevelInfo, "")

	logger.Log(ctx, libLog.LevelInfo, "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━")
	logger.Log(ctx, libLog.LevelInfo, "  ✅ Matcher service ready to accept connections")
	logger.Log(ctx, libLog.LevelInfo, "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━")
}

const statusDisabled = "disabled ✗"

func formatConnStatus(connected bool) string {
	if connected {
		return "✅"
	}

	return "❌"
}

func formatFeatureStatus(enabled bool) string {
	if enabled {
		return "enabled ✓"
	}

	return statusDisabled
}

func formatWorkerStatus(enabled bool, interval time.Duration) string {
	if enabled {
		return fmt.Sprintf("enabled (interval: %v)", interval)
	}

	return statusDisabled
}

// initArchivalComponents initializes the archival worker and archive retrieval routes.
func initArchivalComponents(
	routes *Routes,
	cfg *Config,
	provider sharedPorts.InfrastructureProvider,
	logger libLog.Logger,
	cleanups *[]func(),
) (*governanceWorker.ArchivalWorker, error) {
	ctx := context.Background()

	archiveRepo := archiveMetadataRepo.NewRepository(provider)

	archivalStorage, err := createArchivalStorage(cfg, logger)
	if err != nil {
		return nil, fmt.Errorf("create archival storage: %w", err)
	}

	if archivalStorage != nil {
		archiveHandler, handlerErr := governanceHTTP.NewArchiveHandler(archiveRepo, archivalStorage, cfg.ArchivalPresignExpiry())
		if handlerErr != nil {
			return nil, fmt.Errorf("create archive handler: %w", handlerErr)
		}

		if routeErr := governanceHTTP.RegisterArchiveRoutes(routes.Protected, archiveHandler); routeErr != nil {
			return nil, fmt.Errorf("register archive routes: %w", routeErr)
		}
	}

	if !cfg.Archival.Enabled {
		return nil, nil
	}

	if archivalStorage == nil {
		return nil, ErrArchivalStorageRequired
	}

	archivalDB, dbErr := sql.Open("pgx", cfg.PrimaryDSN())
	if dbErr != nil {
		return nil, fmt.Errorf("open database for archival worker: %w", dbErr)
	}

	success := false

	defer func() {
		if !success {
			if closeErr := archivalDB.Close(); closeErr != nil {
				logger.Log(ctx, libLog.LevelWarn, fmt.Sprintf("failed to close archival DB on error path: %v", closeErr))
			}
		}
	}()

	archivalDB.SetMaxOpenConns(archivalMaxOpenConns)
	archivalDB.SetMaxIdleConns(archivalMaxIdleConns)
	archivalDB.SetConnMaxLifetime(cfg.ConnMaxLifetime())
	archivalDB.SetConnMaxIdleTime(cfg.ConnMaxIdleTime())

	tracer := otel.Tracer(constants.ApplicationName)

	partitionMgr, pmErr := governanceCommand.NewPartitionManager(archivalDB, logger, tracer)
	if pmErr != nil {
		return nil, fmt.Errorf("create partition manager: %w", pmErr)
	}

	archivalWorkerCfg := governanceWorker.ArchivalWorkerConfig{
		Interval:            cfg.ArchivalInterval(),
		HotRetentionDays:    cfg.Archival.HotRetentionDays,
		WarmRetentionMonths: cfg.Archival.WarmRetentionMonths,
		ColdRetentionMonths: cfg.Archival.ColdRetentionMonths,
		BatchSize:           cfg.Archival.BatchSize,
		StorageBucket:       cfg.Archival.StorageBucket,
		StoragePrefix:       cfg.Archival.StoragePrefix,
		StorageClass:        cfg.Archival.StorageClass,
		PartitionLookahead:  cfg.Archival.PartitionLookahead,
	}

	worker, workerErr := governanceWorker.NewArchivalWorker(
		archiveRepo,
		partitionMgr,
		archivalStorage,
		archivalDB,
		provider,
		archivalWorkerCfg,
		logger,
	)
	if workerErr != nil {
		return nil, fmt.Errorf("create archival worker: %w", workerErr)
	}

	success = true

	*cleanups = append(*cleanups, func() {
		if err := archivalDB.Close(); err != nil {
			logger.Log(ctx, libLog.LevelError, fmt.Sprintf("failed to close archival database: %v", err))
		}
	})

	return worker, nil
}

// createArchivalStorage creates an S3-compatible object storage client for the archival bucket.
func createArchivalStorage(cfg *Config, _ libLog.Logger) (reportingPorts.ObjectStorageClient, error) {
	if cfg.Archival.StorageBucket == "" || cfg.ObjectStorage.Endpoint == "" {
		return nil, nil
	}

	s3Cfg := reportingStorage.S3Config{
		Endpoint:        cfg.ObjectStorage.Endpoint,
		Region:          cfg.ObjectStorage.Region,
		Bucket:          cfg.Archival.StorageBucket,
		AccessKeyID:     cfg.ObjectStorage.AccessKeyID,
		SecretAccessKey: cfg.ObjectStorage.SecretAccessKey,
		UsePathStyle:    cfg.ObjectStorage.UsePathStyle,
	}

	client, err := newS3ClientFn(context.Background(), s3Cfg)
	if err != nil {
		return nil, fmt.Errorf("create archival S3 client: %w", err)
	}

	return client, nil
}
