// Copyright 2025 Lerian Studio. All rights reserved.
// Use of this source code is governed by an Elastic License 2.0
// that can be found in the LICENSE.md file.

// interface-only:skip-check-tests

package bootstrap

import (
	"context"
	"strings"

	libOpentelemetry "github.com/LerianStudio/lib-commons/v5/commons/opentelemetry"
	libPostgres "github.com/LerianStudio/lib-commons/v5/commons/postgres"
	libRabbitmq "github.com/LerianStudio/lib-commons/v5/commons/rabbitmq"
	libRedis "github.com/LerianStudio/lib-commons/v5/commons/redis"
)

// checkClientConnected returns the connected state of a client that exposes IsConnected.
// Returns false when the client is nil or IsConnected returns an error.
func checkClientConnected[T interface{ IsConnected() (bool, error) }](client T) bool {
	connected, err := client.IsConnected()
	if err != nil {
		return false
	}

	return connected
}

func buildWorkerStatus(cfg *Config, modules *modulesResult) (export, cleanup, archival, scheduler, discovery bool) {
	if modules == nil {
		return false, false, false, false, false
	}

	return modules.exportWorker != nil && cfg.ExportWorker.Enabled,
		modules.cleanupWorker != nil && cfg.CleanupWorker.Enabled,
		modules.archivalWorker != nil && cfg.Archival.Enabled,
		modules.schedulerWorker != nil,
		modules.discoveryWorker != nil && cfg.Fetcher.Enabled
}

func buildInfraStatus(
	cfg *Config,
	postgres *libPostgres.Client,
	redis *libRedis.Client,
	rabbitmq *libRabbitmq.RabbitMQConnection,
	modules *modulesResult,
	healthDeps *HealthDependencies,
	telemetry *libOpentelemetry.Telemetry,
) *InfraStatus {
	pgConnected := postgres != nil && checkClientConnected(postgres)
	redisConnected := redis != nil && checkClientConnected(redis)
	exportEnabled, cleanupEnabled, archivalEnabled, schedulerEnabled, discoveryEnabled := buildWorkerStatus(cfg, modules)

	status := &InfraStatus{
		PostgresConnected:      pgConnected,
		RedisConnected:         redisConnected,
		RabbitMQConnected:      rabbitmq != nil && rabbitmq.Channel != nil,
		HasReplica:             cfg.Postgres.ReplicaHost != "" && cfg.Postgres.ReplicaHost != cfg.Postgres.PrimaryHost,
		ObjectStorageEnabled:   healthDeps != nil && healthDeps.ObjectStorage != nil,
		ExportWorkerEnabled:    exportEnabled,
		CleanupWorkerEnabled:   cleanupEnabled,
		ArchivalWorkerEnabled:  archivalEnabled,
		SchedulerWorkerEnabled: schedulerEnabled,
		DiscoveryWorkerEnabled: discoveryEnabled,
		TelemetryConfigured:    cfg.Telemetry.Enabled,
		TelemetryActive:        telemetry != nil && telemetry.EnableTelemetry,
	}

	status.TelemetryDegraded = status.TelemetryConfigured && !status.TelemetryActive

	if redis != nil {
		status.RedisMode = detectRedisMode(cfg)
	}

	return status
}

func detectRedisMode(cfg *Config) string {
	if cfg.Redis.MasterName != "" {
		return "sentinel"
	}

	if strings.Contains(cfg.Redis.Host, ",") {
		return "cluster"
	}

	return "standalone"
}

func detachedContext(ctx context.Context) context.Context {
	if ctx == nil {
		return context.TODO()
	}

	return context.WithoutCancel(ctx)
}

func appendCleanup(cleanups *[]func(), cleanup func()) {
	if cleanups == nil || cleanup == nil {
		return
	}

	*cleanups = append(*cleanups, cleanup)
}
