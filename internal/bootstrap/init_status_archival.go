// Copyright 2025 Lerian Studio. All rights reserved.
// Use of this source code is governed by an Elastic License 2.0
// that can be found in the LICENSE.md file.

package bootstrap

import (
	"context"
	"crypto/sha256"
	"database/sql"
	"fmt"
	"sync"
	"time"

	"go.opentelemetry.io/otel"

	libLog "github.com/LerianStudio/lib-commons/v4/commons/log"

	governanceHTTP "github.com/LerianStudio/matcher/internal/governance/adapters/http"
	archiveMetadataRepo "github.com/LerianStudio/matcher/internal/governance/adapters/postgres/archive_metadata"
	governanceWorker "github.com/LerianStudio/matcher/internal/governance/services/worker"
	reportingStorage "github.com/LerianStudio/matcher/internal/reporting/adapters/storage"
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

	logger.Log(ctx, libLog.LevelInfo, "")
	logger.Log(ctx, libLog.LevelInfo, `                     __       __`)
	logger.Log(ctx, libLog.LevelInfo, `    _________ ______/ /______/ /_  ___  _____`)
	logger.Log(ctx, libLog.LevelInfo, `   / __  __ /  __ / __/  ___/ __ \/ _ \/ ___/`)
	logger.Log(ctx, libLog.LevelInfo, `  / / / / / / /_/ / /_/ /__/ / / /  __/ /`)
	logger.Log(ctx, libLog.LevelInfo, ` /_/ /_/ /_/\__,_/\__/\___/_/ /_/\___/_/`)
	logger.Log(ctx, libLog.LevelInfo, `                        by lerian studio`)
	logger.Log(ctx, libLog.LevelInfo, "")

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
	logger.Log(ctx, libLog.LevelInfo, "  Multi-Tenant    : "+formatFeatureStatus(cfg.Tenancy.MultiTenantEnabled))
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

func newArchivalPresignExpiryGetter(cfg *Config, configGetter func() *Config) func() time.Duration {
	if configGetter == nil {
		return nil
	}

	return func() time.Duration {
		runtimeCfg := configGetter()
		if runtimeCfg == nil {
			return cfg.ArchivalPresignExpiry()
		}

		return runtimeCfg.ArchivalPresignExpiry()
	}
}

func registerArchiveRoutesIfAvailable(
	routes *Routes,
	cfg *Config,
	archiveRepo *archiveMetadataRepo.Repository,
	archivalStorage sharedPorts.ObjectStorageClient,
	configGetter func() *Config,
) error {
	if archivalStorage == nil {
		return nil
	}

	archiveHandler, err := governanceHTTP.NewArchiveHandler(archiveRepo, archivalStorage, cfg.ArchivalPresignExpiry())
	if err != nil {
		return fmt.Errorf("create archive handler: %w", err)
	}

	if expiryGetter := newArchivalPresignExpiryGetter(cfg, configGetter); expiryGetter != nil {
		archiveHandler.SetRuntimePresignExpiryGetter(expiryGetter)
	}

	if err := governanceHTTP.RegisterArchiveRoutes(routes.Protected, archiveHandler); err != nil {
		return fmt.Errorf("register archive routes: %w", err)
	}

	return nil
}

func shouldSkipArchivalWorker(cfg *Config) bool {
	return cfg == nil || !cfg.Archival.Enabled
}

func openArchivalDatabase(cfg *Config) (*sql.DB, error) {
	archivalDB, err := sql.Open("pgx", cfg.PrimaryDSN())
	if err != nil {
		return nil, fmt.Errorf("open database for archival worker: %w", err)
	}

	archivalDB.SetMaxOpenConns(archivalMaxOpenConns)
	archivalDB.SetMaxIdleConns(archivalMaxIdleConns)
	archivalDB.SetConnMaxLifetime(cfg.ConnMaxLifetime())
	archivalDB.SetConnMaxIdleTime(cfg.ConnMaxIdleTime())

	return archivalDB, nil
}

// initArchivalComponents initializes the archival worker and archive retrieval routes.
func initArchivalComponents(
	routes *Routes,
	cfg *Config,
	configGetter func() *Config,
	provider sharedPorts.InfrastructureProvider,
	logger libLog.Logger,
	cleanups *[]func(),
) (*governanceWorker.ArchivalWorker, error) {
	archiveRepo := archiveMetadataRepo.NewRepository(provider)

	archivalStorage, err := createArchivalStorage(context.TODO(), cfg)
	if err != nil {
		return nil, fmt.Errorf("create archival storage: %w", err)
	}

	// Create runtime wrapper BEFORE passing to routes so handler and worker share the same dynamic client.
	if configGetter != nil && archivalStorage != nil {
		archivalStorage = newRuntimeArchivalStorageClient(cfg, configGetter, archivalStorage)
	}

	if err := registerArchiveRoutesIfAvailable(routes, cfg, archiveRepo, archivalStorage, configGetter); err != nil {
		return nil, err
	}

	if shouldSkipArchivalWorker(cfg) {
		return nil, nil
	}

	if cfg.Archival.Enabled && !createArchivalStorageAvailable(cfg) {
		return nil, ErrArchivalStorageRequired
	}

	archivalDB, err := openArchivalDatabase(cfg)
	if err != nil {
		return nil, err
	}

	tracer := otel.Tracer(constants.ApplicationName)

	partitionMgr, err := newDynamicPartitionManager(provider, logger, tracer)
	if err != nil {
		return nil, fmt.Errorf("create partition manager: %w", err)
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
		_ = archivalDB.Close()
		return nil, fmt.Errorf("create archival worker: %w", workerErr)
	}

	*cleanups = append(*cleanups, func() {
		_ = archivalDB.Close()
	})

	return worker, nil
}

func createArchivalStorageAvailable(cfg *Config) bool {
	return cfg != nil && cfg.Archival.StorageBucket != "" && cfg.ObjectStorage.Endpoint != ""
}

// createArchivalStorage creates an S3-compatible object storage client for the archival bucket.
func createArchivalStorage(ctx context.Context, cfg *Config) (sharedPorts.ObjectStorageClient, error) {
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
		AllowInsecure:   allowInsecureObjectStorageEndpoint(cfg),
	}

	client, err := newS3ClientFn(detachedContext(ctx), s3Cfg)
	if err != nil {
		return nil, fmt.Errorf("create archival S3 client: %w", err)
	}

	return client, nil
}

func newRuntimeArchivalStorageClient(
	initialCfg *Config,
	configGetter func() *Config,
	fallback sharedPorts.ObjectStorageClient,
) sharedPorts.ObjectStorageClient {
	var (
		mu           sync.Mutex
		activeClient sharedPorts.ObjectStorageClient
		activeKey    string
	)

	activeClient = fallback
	activeKey = archivalStorageCacheKey(initialCfg)

	return newDynamicObjectStorageClient(func() sharedPorts.ObjectStorageClient {
		cfg := initialCfg

		if configGetter != nil {
			if runtimeCfg := configGetter(); runtimeCfg != nil {
				cfg = runtimeCfg
			}
		}

		cacheKey := archivalStorageCacheKey(cfg)

		mu.Lock()
		defer mu.Unlock()

		if activeClient != nil && cacheKey == activeKey {
			return activeClient
		}

		client, err := createArchivalStorage(context.TODO(), cfg)
		if err != nil || client == nil {
			return nil
		}

		activeClient = client
		activeKey = cacheKey

		return activeClient
	}, fallback)
}

func archivalStorageCacheKey(cfg *Config) string {
	if cfg == nil {
		return ""
	}

	secretHash := sha256.Sum256([]byte(cfg.ObjectStorage.SecretAccessKey))

	return fmt.Sprintf("%s|%s|%s|%s|%x|%t|%t", cfg.ObjectStorage.Endpoint, cfg.ObjectStorage.Region, cfg.Archival.StorageBucket, cfg.ObjectStorage.AccessKeyID, secretHash[:8], cfg.ObjectStorage.UsePathStyle, allowInsecureObjectStorageEndpoint(cfg))
}
