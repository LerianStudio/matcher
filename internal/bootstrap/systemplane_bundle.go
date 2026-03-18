// Copyright 2025 Lerian Studio. All rights reserved.
// Use of this source code is governed by an Elastic License 2.0
// that can be found in the LICENSE.md file.

package bootstrap

import (
	"context"
	"errors"
	"fmt"
	"io"

	libLog "github.com/LerianStudio/lib-commons/v4/commons/log"
	libPostgres "github.com/LerianStudio/lib-commons/v4/commons/postgres"
	libRabbitmq "github.com/LerianStudio/lib-commons/v4/commons/rabbitmq"
	libRedis "github.com/LerianStudio/lib-commons/v4/commons/redis"
)

// MatcherBundle is the runtime dependency container for the Matcher service.
// It implements domain.RuntimeBundle and groups related clients into sub-bundles
// that are rebuilt atomically when bundle-rebuild config changes occur.
//
// The sub-bundles partition concerns:
//   - Infra: infrastructure clients that require connection management (Postgres, Redis, RabbitMQ, object storage).
//   - HTTP: HTTP-layer policy values that can change at runtime without rebuilding the Fiber app.
//   - Logger: the rebuilt structured logger instance.
//
// Close tears down all held resources in reverse dependency order, collecting
// all errors so that a single failure does not prevent remaining resources
// from being released.
type MatcherBundle struct {
	Infra  *InfraBundle
	HTTP   *HTTPPolicyBundle
	Logger *LoggerBundle
}

// InfraBundle groups infrastructure clients that require connection management.
// All fields are pointer types or interfaces so that nil means "not configured"
// or "construction was skipped".
type InfraBundle struct {
	// Postgres is the primary/replica database client managed by lib-commons.
	Postgres *libPostgres.Client
	// Redis is the Redis universal client managed by lib-commons.
	Redis *libRedis.Client
	// RabbitMQ is the RabbitMQ connection (includes Channel + Connection).
	RabbitMQ *libRabbitmq.RabbitMQConnection
	// ObjectStorage is the S3-compatible object storage client.
	// Uses io.Closer to avoid a circular import with reporting ports.
	ObjectStorage io.Closer
}

// HTTPPolicyBundle holds HTTP-layer configuration that can change at runtime
// without rebuilding the Fiber app itself. These values are read by middleware
// and route handlers from the active bundle.
type HTTPPolicyBundle struct {
	BodyLimitBytes     int
	CORSAllowedOrigins string
	CORSAllowedMethods string
	CORSAllowedHeaders string
	SwaggerEnabled     bool
	SwaggerHost        string
	SwaggerSchemes     string
}

// LoggerBundle holds the rebuilt logger and its configured level.
type LoggerBundle struct {
	Logger libLog.Logger
	Level  string
}

// Close releases all resources held by the bundle in reverse dependency order:
// Logger (sync) -> ObjectStorage -> RabbitMQ -> Redis -> Postgres.
//
// Each close is attempted regardless of earlier failures. All errors are
// collected and returned as a joined error via errors.Join.
func (bundle *MatcherBundle) Close(ctx context.Context) error {
	if bundle == nil {
		return nil
	}

	var errs []error

	// 1. Logger sync (least dependent — only observability).
	// The lib-commons Logger interface includes Sync(ctx) so we call it
	// directly rather than using a type assertion.
	if bundle.Logger != nil && bundle.Logger.Logger != nil {
		if err := bundle.Logger.Logger.Sync(ctx); err != nil {
			errs = append(errs, fmt.Errorf("sync logger: %w", err))
		}
	}

	// 2. Infrastructure clients in reverse dependency order.
	if infraErr := bundle.closeInfra(); infraErr != nil {
		errs = append(errs, infraErr)
	}

	return errors.Join(errs...)
}

// closeInfra closes all infrastructure clients in reverse dependency order.
// Each close is attempted regardless of earlier failures.
func (bundle *MatcherBundle) closeInfra() error {
	if bundle.Infra == nil {
		return nil
	}

	var errs []error

	if bundle.Infra.ObjectStorage != nil {
		if err := bundle.Infra.ObjectStorage.Close(); err != nil {
			errs = append(errs, fmt.Errorf("close object storage: %w", err))
		}
	}

	if bundle.Infra.RabbitMQ != nil {
		if err := closeRabbitMQ(bundle.Infra.RabbitMQ); err != nil {
			errs = append(errs, fmt.Errorf("close rabbitmq: %w", err))
		}
	}

	if bundle.Infra.Redis != nil {
		if err := bundle.Infra.Redis.Close(); err != nil {
			errs = append(errs, fmt.Errorf("close redis: %w", err))
		}
	}

	if bundle.Infra.Postgres != nil {
		if err := bundle.Infra.Postgres.Close(); err != nil {
			errs = append(errs, fmt.Errorf("close postgres: %w", err))
		}
	}

	return errors.Join(errs...)
}

// closeRabbitMQ closes the RabbitMQ channel and connection in order.
// Both are attempted even if the channel close fails, matching the
// existing cleanupRabbitMQ pattern in init.go.
func closeRabbitMQ(conn *libRabbitmq.RabbitMQConnection) error {
	var errs []error

	if conn.Channel != nil {
		if err := conn.Channel.Close(); err != nil {
			errs = append(errs, fmt.Errorf("channel: %w", err))
		}
	}

	if conn.Connection != nil {
		if err := conn.Connection.Close(); err != nil {
			errs = append(errs, fmt.Errorf("connection: %w", err))
		}
	}

	return errors.Join(errs...)
}

// DB returns the primary PostgreSQL client, or nil if the infrastructure
// bundle is not available.
func (bundle *MatcherBundle) DB() *libPostgres.Client {
	if bundle == nil || bundle.Infra == nil {
		return nil
	}

	return bundle.Infra.Postgres
}

// RedisClient returns the Redis client, or nil if the infrastructure
// bundle is not available.
func (bundle *MatcherBundle) RedisClient() *libRedis.Client {
	if bundle == nil || bundle.Infra == nil {
		return nil
	}

	return bundle.Infra.Redis
}

// RabbitMQConn returns the RabbitMQ connection, or nil if the infrastructure
// bundle is not available.
func (bundle *MatcherBundle) RabbitMQConn() *libRabbitmq.RabbitMQConnection {
	if bundle == nil || bundle.Infra == nil {
		return nil
	}

	return bundle.Infra.RabbitMQ
}

// Log returns the current logger, or nil if the logger bundle is not available.
func (bundle *MatcherBundle) Log() libLog.Logger {
	if bundle == nil || bundle.Logger == nil {
		return nil
	}

	return bundle.Logger.Logger
}
