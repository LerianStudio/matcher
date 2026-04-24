// Copyright 2025 Lerian Studio. All rights reserved.
// Use of this source code is governed by an Elastic License 2.0
// that can be found in the LICENSE.md file.

// interface-only:skip-check-tests

package bootstrap

import (
	"context"
	"crypto/sha256"
	"fmt"
	"time"

	libLog "github.com/LerianStudio/lib-commons/v5/commons/log"
	"github.com/LerianStudio/lib-commons/v5/commons/runtime"

	configScheduleRepo "github.com/LerianStudio/matcher/internal/configuration/adapters/postgres/schedule"
	configWorker "github.com/LerianStudio/matcher/internal/configuration/services/worker"
	exceptionRedis "github.com/LerianStudio/matcher/internal/exception/adapters/redis"
	matchingCommand "github.com/LerianStudio/matcher/internal/matching/services/command"
	reportingStorage "github.com/LerianStudio/matcher/internal/reporting/adapters/storage"
	"github.com/LerianStudio/matcher/internal/shared/objectstorage"
	sharedPorts "github.com/LerianStudio/matcher/internal/shared/ports"
)

// createSchedulerWorker creates the scheduler worker for cron-based matching.
// Returns nil if any dependency fails to initialize (logged as warnings).
func createSchedulerWorker(
	ctx context.Context,
	cfg *Config,
	provider sharedPorts.InfrastructureProvider,
	matchingUseCase *matchingCommand.UseCase,
	logger libLog.Logger,
) *configWorker.SchedulerWorker {
	ctx = detachedContext(ctx)

	configScheduleRepository := configScheduleRepo.NewRepository(provider)

	if matchingUseCase == nil {
		logger.Log(ctx, libLog.LevelWarn, "scheduler worker not started: matching use case unavailable")

		return nil
	}

	// Use a provider-backed lock manager that resolves Redis lazily per lock
	// attempt. This ensures the scheduler survives transient Redis outages at
	// boot and benefits from runtime infrastructure bundle swaps.
	lockManager := newProviderBackedLockManager(provider)

	workerCfg := configWorker.SchedulerWorkerConfig{
		Interval: schedulerInterval(cfg),
	}

	// T-004 (K-06a): matchingUseCase satisfies sharedPorts.MatchTrigger
	// directly — no adapter layer. The ceremony wrapper was removed when
	// TriggerMatchForContext moved onto the UseCase itself.
	sw, err := configWorker.NewSchedulerWorker(
		configScheduleRepository,
		matchingUseCase,
		lockManager,
		workerCfg,
		logger,
	)
	if err != nil {
		logger.Log(ctx, libLog.LevelWarn, fmt.Sprintf("scheduler worker not available: %v", err))

		return nil
	}

	return sw
}

func createIdempotencyRepository(
	cfg *Config,
	configGetter func() *Config,
	settingsResolver *runtimeSettingsResolver,
	provider sharedPorts.InfrastructureProvider,
	logger libLog.Logger,
) sharedPorts.IdempotencyRepository {
	ctx := context.Background()

	if provider == nil {
		logger.Log(ctx, libLog.LevelWarn, "idempotency repository: infrastructure provider is nil, idempotency disabled")

		return nil
	}

	exceptionIdempotencyRepo, err := exceptionRedis.NewIdempotencyRepositoryWithConfig(
		provider,
		cfg.IdempotencyRetryWindow(),
		cfg.IdempotencySuccessTTL(),
		cfg.Idempotency.HMACSecret,
	)
	if err != nil {
		libLog.SafeError(
			logger,
			ctx,
			fmt.Sprintf("failed to create idempotency repository (retryWindow=%v, successTTL=%v)",
				cfg.IdempotencyRetryWindow(), cfg.IdempotencySuccessTTL()),
			err,
			runtime.IsProductionMode(),
		)

		return nil
	}

	if configGetter != nil || settingsResolver != nil {
		exceptionIdempotencyRepo.SetRuntimeConfigResolvers(
			func(ctx context.Context) time.Duration {
				return resolveIdempotencyRetryWindow(ctx, cfg, configGetter, settingsResolver)
			},
			func(ctx context.Context) time.Duration {
				return resolveIdempotencySuccessTTL(ctx, cfg, configGetter, settingsResolver)
			},
			func(ctx context.Context) string {
				return resolveIdempotencyHMACSecret(ctx, cfg, configGetter, settingsResolver)
			},
		)
	}

	return exceptionIdempotencyRepo
}

// createObjectStorage initialises the S3/MinIO client only when the reporting
// background workers actually need it at startup.
func createObjectStorage(
	ctx context.Context,
	cfg *Config,
	connector InfraConnector,
) (objectstorage.Backend, error) {
	if !reportingStorageRequired(cfg) {
		return nil, nil
	}

	if cfg.ObjectStorage.Bucket == "" {
		return nil, ErrObjectStorageBucketRequired
	}

	if connector == nil {
		connector = DefaultInfraConnector()
	}

	s3Cfg := reportingStorage.S3Config{
		Endpoint:        cfg.ObjectStorage.Endpoint,
		Region:          cfg.ObjectStorage.Region,
		Bucket:          cfg.ObjectStorage.Bucket,
		AccessKeyID:     cfg.ObjectStorage.AccessKeyID,
		SecretAccessKey: cfg.ObjectStorage.SecretAccessKey,
		UsePathStyle:    cfg.ObjectStorage.UsePathStyle,
		AllowInsecure:   allowInsecureObjectStorageEndpoint(cfg),
	}

	client, err := connector.NewS3Client(detachedContext(ctx), s3Cfg)
	if err != nil {
		return nil, fmt.Errorf("create S3 client: %w", err)
	}

	return client, nil
}

func reportingStorageRequired(cfg *Config) bool {
	if cfg == nil {
		return false
	}

	return cfg.ExportWorker.Enabled || cfg.CleanupWorker.Enabled
}

// newRuntimeReportingStorageClient wraps the startup-time reporting storage
// client in a dynamic delegate that resolves the concrete client from the
// current runtime config on every call. When object_storage.* changes via
// /system, subsequent reporting operations pick up the new credentials and
// endpoint without requiring a restart. When no storage is configured, calls
// fail at invocation time with ErrObjectStorageUnavailable rather than
// preventing startup.
//
// The concrete client is cached keyed on the current config snapshot; it is
// rebuilt only when the snapshot changes, so routine resolutions incur no S3
// client reconstruction cost.
//
// This mirrors newRuntimeArchivalStorageClient and allows the reporting module
// to register its routes and workers unconditionally, even when the export and
// cleanup workers are disabled or the object-storage endpoint is empty.
func newRuntimeReportingStorageClient(
	initialCfg *Config,
	configGetter func() *Config,
	fallback objectstorage.Backend,
	connector InfraConnector,
) *objectstorage.Client {
	if connector == nil {
		connector = DefaultInfraConnector()
	}

	resolver := func(ctx context.Context) (objectstorage.Backend, string, error) {
		cfg := initialCfg

		if configGetter != nil {
			if runtimeCfg := configGetter(); runtimeCfg != nil {
				cfg = runtimeCfg
			}
		}

		backend, err := createObjectStorage(ctx, cfg, connector)
		if err != nil {
			return nil, "", err
		}

		if backend == nil {
			return nil, reportingStorageCacheKey(cfg), nil
		}

		return backend, reportingStorageCacheKey(cfg), nil
	}

	return objectstorage.NewClientWithResolver(fallback, resolver)
}

func reportingStorageCacheKey(cfg *Config) string {
	if cfg == nil {
		return ""
	}

	secretHash := sha256.Sum256([]byte(cfg.ObjectStorage.SecretAccessKey))

	return fmt.Sprintf("%s|%s|%s|%s|%x|%t|%t", cfg.ObjectStorage.Endpoint, cfg.ObjectStorage.Region, cfg.ObjectStorage.Bucket, cfg.ObjectStorage.AccessKeyID, secretHash[:8], cfg.ObjectStorage.UsePathStyle, allowInsecureObjectStorageEndpoint(cfg))
}
