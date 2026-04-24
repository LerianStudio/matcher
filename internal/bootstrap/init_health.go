// Copyright 2025 Lerian Studio. All rights reserved.
// Use of this source code is governed by an Elastic License 2.0
// that can be found in the LICENSE.md file.

// interface-only:skip-check-tests

package bootstrap

import (
	"context"
	"database/sql"
	"fmt"
	"sync"

	libLog "github.com/LerianStudio/lib-commons/v5/commons/log"
	libPostgres "github.com/LerianStudio/lib-commons/v5/commons/postgres"
	libRabbitmq "github.com/LerianStudio/lib-commons/v5/commons/rabbitmq"
	libRedis "github.com/LerianStudio/lib-commons/v5/commons/redis"
	"github.com/LerianStudio/lib-commons/v5/commons/runtime"

	reportingStorage "github.com/LerianStudio/matcher/internal/reporting/adapters/storage"
)

func assignReplicaHealthCheck(
	ctx context.Context,
	cfg *Config,
	logger libLog.Logger,
	deps *HealthDependencies,
	cleanups *[]func(),
) {
	if cfg.Postgres.ReplicaHost == "" || cfg.Postgres.ReplicaHost == cfg.Postgres.PrimaryHost {
		return
	}

	check, cleanup := createPostgresReplicaHealthCheck(ctx, cfg, logger)
	deps.PostgresReplicaCheck = check

	appendCleanup(cleanups, cleanup)
}

func resolvePrimaryDB(checkCtx context.Context, postgres *libPostgres.Client) (*sql.DB, error) {
	resolver, err := postgres.Resolver(checkCtx)
	if err != nil {
		return nil, fmt.Errorf("postgres health check: get primary db failed: %w", err)
	}

	primaryDBs := resolver.PrimaryDBs()
	if len(primaryDBs) == 0 || primaryDBs[0] == nil {
		return nil, errPostgresPrimaryNil
	}

	return primaryDBs[0], nil
}

func resolveReplicaDB(checkCtx context.Context, postgres *libPostgres.Client) (*sql.DB, error) {
	resolver, err := postgres.Resolver(checkCtx)
	if err != nil {
		return nil, fmt.Errorf("postgres replica health check: get db failed: %w", err)
	}

	replicaDBs := resolver.ReplicaDBs()
	if len(replicaDBs) == 0 || replicaDBs[0] == nil {
		return nil, errNoReplicasConfigured
	}

	return replicaDBs[0], nil
}

func pingSQLDB(checkCtx context.Context, db *sql.DB, operation string) error {
	if err := db.PingContext(checkCtx); err != nil {
		return fmt.Errorf("%s: %w", operation, err)
	}

	return nil
}

func newPostgresHealthCheck(
	postgres *libPostgres.Client,
) HealthCheckFunc {
	return func(checkCtx context.Context) error {
		if postgres == nil {
			return errPostgresPrimaryNil
		}

		primaryDB, err := resolvePrimaryDB(checkCtx, postgres)
		if err != nil {
			return err
		}

		return pingSQLDB(checkCtx, primaryDB, "postgres health check: ping primary db")
	}
}

func newRedisHealthCheck(
	redis *libRedis.Client,
) HealthCheckFunc {
	return func(checkCtx context.Context) error {
		if redis == nil {
			return errRedisClientNil
		}

		client, err := redis.GetClient(checkCtx)
		if err != nil {
			return fmt.Errorf("redis health check: get client failed: %w", err)
		}

		if client == nil {
			return errRedisClientNil
		}

		if err := client.Ping(checkCtx).Err(); err != nil {
			return fmt.Errorf("redis health check: ping failed: %w", err)
		}

		return nil
	}
}

func newPostgresReplicaHealthCheck(
	postgres *libPostgres.Client,
) HealthCheckFunc {
	return func(checkCtx context.Context) error {
		if postgres == nil {
			return errNoReplicasConfigured
		}

		replicaDB, err := resolveReplicaDB(checkCtx, postgres)
		if err != nil {
			return err
		}

		return pingSQLDB(checkCtx, replicaDB, "postgres replica health check: ping replica db")
	}
}

func newRabbitMQHealthCheck(
	rabbitmq *libRabbitmq.RabbitMQConnection,
) HealthCheckFunc {
	return func(checkCtx context.Context) error {
		conn := rabbitmq
		if conn == nil {
			return errRabbitMQConnectionNil
		}

		if conn.HealthCheckURL != "" &&
			(conn.AllowInsecureHealthCheck || !isInsecureHTTPHealthCheckURL(conn.HealthCheckURL)) {
			if err := checkRabbitMQHTTPHealth(checkCtx, conn.HealthCheckURL); err == nil {
				return nil
			}
		}

		if err := conn.EnsureChannel(); err != nil {
			return fmt.Errorf("rabbitmq health check: ensure channel: %w", err)
		}

		return nil
	}
}

func attachBundleHealthChecks(
	cfg *Config,
	deps *HealthDependencies,
	postgres *libPostgres.Client,
	redis *libRedis.Client,
	rabbitmq *libRabbitmq.RabbitMQConnection,
) {
	deps.PostgresCheck = newPostgresHealthCheck(postgres)
	deps.RedisCheck = newRedisHealthCheck(redis)
	// Preserve a DSN-based replica probe installed by assignReplicaHealthCheck.
	// Overwriting would neutralise its dedicated connection + cleanup pair.
	//
	// Only install a primary-backed fallback when a DISTINCT replica is
	// configured. Otherwise the fallback calls resolveReplicaDB on the primary
	// client, which returns errNoReplicasConfigured → status=down on every
	// /readyz hit — noisy dashboards for a legitimately absent dep. The
	// evaluator's applyReadinessCheckResult treats an unresolved optional dep
	// as "skipped, reason=postgres_replica not configured", which is the
	// accurate story.
	if deps.PostgresReplicaCheck == nil && cfg != nil &&
		cfg.Postgres.ReplicaHost != "" && cfg.Postgres.ReplicaHost != cfg.Postgres.PrimaryHost {
		deps.PostgresReplicaCheck = newPostgresReplicaHealthCheck(postgres)
	}

	deps.RabbitMQCheck = newRabbitMQHealthCheck(rabbitmq)
}

func configureObjectStorageHealthChecks(
	ctx context.Context,
	cfg *Config,
	deps *HealthDependencies,
	logger libLog.Logger,
	connector InfraConnector,
) error {
	objectStorage, err := createObjectStorageForHealth(ctx, cfg, connector)
	if err != nil {
		if cfg.ExportWorker.Enabled {
			return fmt.Errorf("object storage required when EXPORT_WORKER_ENABLED=true: %w", err)
		}

		logger.Log(ctx, libLog.LevelDebug, fmt.Sprintf("Object storage health check disabled: %v", err))
	} else if objectStorage != nil {
		deps.ObjectStorage = objectStorage
	}

	deps.ObjectStorageCheck = func(checkCtx context.Context) error {
		if deps.ObjectStorage == nil {
			return nil
		}

		_, existsErr := deps.ObjectStorage.Exists(checkCtx, ".health-check")
		if existsErr != nil {
			return fmt.Errorf("object storage health check: exists: %w", existsErr)
		}

		return nil
	}

	return nil
}

func createHealthDependencies(
	ctx context.Context,
	cfg *Config,
	logger libLog.Logger,
	postgres *libPostgres.Client,
	redis *libRedis.Client,
	rabbitmq *libRabbitmq.RabbitMQConnection,
	cleanups *[]func(),
	connector InfraConnector,
) (*HealthDependencies, error) {
	deps := NewHealthDependencies(postgres, nil, redis, rabbitmq, nil)

	// Redis is required for readiness.
	// Multiple critical paths depend on Redis (idempotency middleware,
	// matching locks, and rate limiting), so reporting ready while Redis is down
	// can route write traffic to an instance that cannot safely process it.
	deps.RedisOptional = false

	assignReplicaHealthCheck(ctx, cfg, logger, deps, cleanups)
	attachBundleHealthChecks(cfg, deps, postgres, redis, rabbitmq)

	if err := configureObjectStorageHealthChecks(ctx, cfg, deps, logger, connector); err != nil {
		return nil, err
	}

	return deps, nil
}

func createPostgresReplicaHealthCheck(
	ctx context.Context,
	cfg *Config,
	logger libLog.Logger,
) (HealthCheckFunc, func()) {
	replicaDSN := cfg.ReplicaDSN()
	logCtx := detachedContext(ctx)

	// Create a single connection for health checks to avoid connection leak.
	// The connection is lazily initialized on first health check.
	var (
		healthDB *sql.DB
		initOnce sync.Once
		initErr  error
	)

	check := func(ctx context.Context) error {
		initOnce.Do(func() {
			healthDB, initErr = sql.Open("pgx", replicaDSN)
			if initErr != nil {
				return
			}

			healthDB.SetMaxOpenConns(1)
			healthDB.SetMaxIdleConns(1)
			healthDB.SetConnMaxLifetime(healthConnMaxLifetime)
		})

		if initErr != nil {
			return fmt.Errorf("postgres replica health check: open failed: %w", initErr)
		}

		if err := healthDB.PingContext(ctx); err != nil {
			return fmt.Errorf("postgres replica health check: ping failed: %w", err)
		}

		return nil
	}

	cleanup := func() {
		if healthDB != nil {
			if err := healthDB.Close(); err != nil {
				libLog.SafeError(logger, logCtx, "failed to close postgres replica health check connection", err, runtime.IsProductionMode())
			}
		}
	}

	return check, cleanup
}

func createObjectStorageForHealth(
	ctx context.Context,
	cfg *Config,
	connector InfraConnector,
) (ObjectStorageHealthChecker, error) {
	if cfg.ObjectStorage.Endpoint == "" {
		return nil, nil
	}

	if cfg.ObjectStorage.Bucket == "" {
		return nil, nil
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
		return nil, fmt.Errorf("create S3 client for health check: %w", err)
	}

	return client, nil
}
