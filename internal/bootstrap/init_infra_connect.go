// Copyright 2025 Lerian Studio. All rights reserved.
// Use of this source code is governed by an Elastic License 2.0
// that can be found in the LICENSE.md file.

// interface-only:skip-check-tests

package bootstrap

import (
	"context"
	"database/sql"
	"fmt"
	"strings"
	"time"

	"github.com/LerianStudio/lib-commons/v5/commons/assert"
	"github.com/LerianStudio/lib-commons/v5/commons/errgroup"
	libLog "github.com/LerianStudio/lib-commons/v5/commons/log"
	libPostgres "github.com/LerianStudio/lib-commons/v5/commons/postgres"
	libRabbitmq "github.com/LerianStudio/lib-commons/v5/commons/rabbitmq"
	libRedis "github.com/LerianStudio/lib-commons/v5/commons/redis"
	"github.com/LerianStudio/lib-commons/v5/commons/runtime"
)

func cleanupConnections(
	ctx context.Context,
	postgres *libPostgres.Client,
	redis *libRedis.Client,
	rabbitmq *libRabbitmq.RabbitMQConnection,
	logger libLog.Logger,
) {
	cleanupPostgres(ctx, postgres, logger)
	cleanupRedis(ctx, redis, logger)
	cleanupRabbitMQ(ctx, rabbitmq, logger)
}

func cleanupPostgres(ctx context.Context, postgres *libPostgres.Client, logger libLog.Logger) {
	if postgres == nil {
		return
	}

	start := time.Now()
	err := postgres.Close()
	duration := time.Since(start)

	if err != nil {
		libLog.SafeError(logger, ctx, "failed to close postgres connection", err, runtime.IsProductionMode())
		recordCleanup(ctx, "postgres", false, duration)

		return
	}

	recordCleanup(ctx, "postgres", true, duration)
}

func cleanupRedis(ctx context.Context, redis *libRedis.Client, logger libLog.Logger) {
	if redis == nil {
		return
	}

	start := time.Now()
	err := redis.Close()
	duration := time.Since(start)

	if err != nil {
		libLog.SafeError(logger, ctx, "failed to close redis connection", err, runtime.IsProductionMode())
		recordCleanup(ctx, "redis", false, duration)

		return
	}

	recordCleanup(ctx, "redis", true, duration)
}

func cleanupRabbitMQ(ctx context.Context, rabbitmq *libRabbitmq.RabbitMQConnection, logger libLog.Logger) {
	if rabbitmq == nil {
		return
	}

	start := time.Now()
	hasError := false

	if rabbitmq.Channel != nil {
		if err := rabbitmq.Channel.Close(); err != nil {
			libLog.SafeError(logger, ctx, "failed to close rabbitmq channel", err, runtime.IsProductionMode())

			hasError = true
		}
	}

	if rabbitmq.Connection != nil {
		if err := rabbitmq.Connection.Close(); err != nil {
			libLog.SafeError(logger, ctx, "failed to close rabbitmq connection", err, runtime.IsProductionMode())

			hasError = true
		}
	}

	recordCleanup(ctx, "rabbitmq", !hasError, time.Since(start))
}

// connectInfrastructure runs database migrations, then connects to PostgreSQL and
// RabbitMQ in parallel.
//
// Redis verification is intentionally omitted: createRedisConnection() already calls
// libRedis.New() which performs Connect() + Ping(). A second verification here would
// be redundant work.
//
// Dependency graph:
//
//	sequential:
//	  1) Migrations
//
//	errgroup (parallel after migrations):
//	  goroutine 1: Postgres Connect → Pool Settings
//	  goroutine 2: RabbitMQ Connect
//
// Running migrations before the errgroup prevents unrelated dependency failures
// (e.g., RabbitMQ outage) from canceling in-flight migrations and leaving dirty
// migration state.
func connectInfrastructure(
	ctx context.Context,
	asserter *assert.Asserter,
	cfg *Config,
	postgres *libPostgres.Client,
	rabbitmq *libRabbitmq.RabbitMQConnection,
	logger libLog.Logger,
	connector InfraConnector,
) error {
	if postgres == nil {
		return errPostgresClientRequired
	}

	if rabbitmq == nil {
		return errRabbitMQClientRequired
	}

	if connector == nil {
		connector = DefaultInfraConnector()
	}

	// Each infrastructure service gets its own timeout budget to prevent
	// a single slow dependency from starving the others. The parent context
	// still enforces the overall deadline.
	perServiceTimeout := cfg.InfraConnectTimeout() / infraConnectTimeoutDivisor //nolint:contextcheck // 1/2 of total budget per service; InfraConnectTimeout is a pure config getter
	if perServiceTimeout < minPerServiceTimeout {
		perServiceTimeout = minPerServiceTimeout
	}

	allowDirtyRecovery := shouldAllowDirtyMigrationRecovery(cfg.App.EnvName)
	if err := connector.RunMigrations(
		ctx,
		cfg.PrimaryDSN(),
		cfg.Postgres.PrimaryDB,
		cfg.Postgres.MigrationsPath,
		logger,
		allowDirtyRecovery,
	); err != nil {
		return fmt.Errorf("run migrations: %w", err)
	}

	infraGroup, groupCtx := errgroup.WithContext(ctx)
	infraGroup.SetLogger(logger)

	infraGroup.Go(func() error {
		pgCtx, pgCancel := context.WithTimeout(groupCtx, perServiceTimeout)
		defer pgCancel()

		if err := asserter.NoError(pgCtx, connector.ConnectPostgres(pgCtx, postgres), "failed to connect postgres"); err != nil {
			return fmt.Errorf("connect postgres: %w", err)
		}

		return configurePostgresPoolSettings(groupCtx, cfg, postgres)
	})

	// Goroutine 2: RabbitMQ Connect (independent of migrations/postgres).
	infraGroup.Go(func() error {
		rabbitCtx, rabbitCancel := context.WithTimeout(groupCtx, perServiceTimeout)
		defer rabbitCancel()

		if err := asserter.NoError(rabbitCtx, connector.EnsureRabbitChannel(rabbitmq), "failed to connect rabbitmq"); err != nil {
			return fmt.Errorf("connect rabbitmq: %w", err)
		}

		return nil
	})

	if err := infraGroup.Wait(); err != nil {
		return fmt.Errorf("connect infrastructure: %w", err)
	}

	return nil
}

func shouldAllowDirtyMigrationRecovery(env string) bool {
	switch strings.ToLower(strings.TrimSpace(env)) {
	case defaultEnvName, envLocalName, envTestName:
		return true
	default:
		return false
	}
}

func configurePostgresPoolSettings(ctx context.Context, cfg *Config, postgres *libPostgres.Client) error {
	connected, connErr := postgres.IsConnected()
	if connErr != nil {
		return fmt.Errorf("check postgres connectivity for pool settings: %w", connErr)
	}

	if !connected {
		return nil
	}

	resolver, resolverErr := postgres.Resolver(ctx)
	if resolverErr != nil {
		return fmt.Errorf("resolve postgres for pool settings: %w", resolverErr)
	}

	if resolver == nil {
		return errPostgresResolverRequired
	}

	applySQLPoolSettings(resolver.PrimaryDBs(), cfg.ConnMaxLifetime(), cfg.ConnMaxIdleTime())
	applySQLPoolSettings(resolver.ReplicaDBs(), cfg.ConnMaxLifetime(), cfg.ConnMaxIdleTime())

	return nil
}

func applySQLPoolSettings(dbs []*sql.DB, maxLifetime, maxIdle time.Duration) {
	for _, db := range dbs {
		if db == nil {
			continue
		}

		db.SetConnMaxLifetime(maxLifetime)
		db.SetConnMaxIdleTime(maxIdle)
	}
}
