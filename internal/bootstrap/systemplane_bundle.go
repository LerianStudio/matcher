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

	ingestionRabbitmq "github.com/LerianStudio/matcher/internal/ingestion/adapters/rabbitmq"
	matchingRabbitmq "github.com/LerianStudio/matcher/internal/matching/adapters/rabbitmq"
	"github.com/LerianStudio/matcher/pkg/systemplane/domain"
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
	Infra                    *InfraBundle
	HTTP                     *HTTPPolicyBundle
	Logger                   *LoggerBundle
	StagedMatchingPublisher  *matchingRabbitmq.EventPublisher
	StagedIngestionPublisher *ingestionRabbitmq.EventPublisher

	ownershipTracked  bool
	ownsLogger        bool
	ownsPostgres      bool
	ownsRedis         bool
	ownsRabbitMQ      bool
	ownsObjectStorage bool
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
	if bundle.shouldCloseLogger() && bundle.Logger != nil && bundle.Logger.Logger != nil {
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

	if bundle.shouldCloseObjectStorage() && bundle.Infra.ObjectStorage != nil {
		if err := bundle.Infra.ObjectStorage.Close(); err != nil {
			errs = append(errs, fmt.Errorf("close object storage: %w", err))
		}
	}

	if bundle.shouldCloseRabbitMQ() && bundle.Infra.RabbitMQ != nil {
		if err := closeRabbitMQ(bundle.Infra.RabbitMQ); err != nil {
			errs = append(errs, fmt.Errorf("close rabbitmq: %w", err))
		}
	}

	if bundle.shouldCloseRedis() && bundle.Infra.Redis != nil {
		if err := bundle.Infra.Redis.Close(); err != nil {
			errs = append(errs, fmt.Errorf("close redis: %w", err))
		}
	}

	if bundle.shouldClosePostgres() && bundle.Infra.Postgres != nil {
		if err := bundle.Infra.Postgres.Close(); err != nil {
			errs = append(errs, fmt.Errorf("close postgres: %w", err))
		}
	}

	if bundle.StagedMatchingPublisher != nil {
		if err := loadCloseMatchingEventPublisherFn()(bundle.StagedMatchingPublisher); err != nil {
			errs = append(errs, fmt.Errorf("close staged matching publisher: %w", err))
		}
	}

	if bundle.StagedIngestionPublisher != nil {
		if err := loadCloseIngestionEventPublisherFn()(bundle.StagedIngestionPublisher); err != nil {
			errs = append(errs, fmt.Errorf("close staged ingestion publisher: %w", err))
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

// AdoptResourcesFrom transfers ownership of reused resources from the previous
// bundle after the new bundle has been committed as active.
func (bundle *MatcherBundle) AdoptResourcesFrom(previous domain.RuntimeBundle) {
	prev, ok := previous.(*MatcherBundle)
	if !ok || bundle == nil || prev == nil || !bundle.ownershipTracked {
		return
	}

	if !bundle.ownsLogger && bundle.Logger == prev.Logger {
		bundle.ownsLogger = true
		prev.Logger = nil
		prev.ownsLogger = false
	}

	if bundle.Infra == nil || prev.Infra == nil {
		return
	}

	if !bundle.ownsPostgres && bundle.Infra.Postgres == prev.Infra.Postgres {
		bundle.ownsPostgres = true
		prev.Infra.Postgres = nil
		prev.ownsPostgres = false
	}

	if !bundle.ownsRedis && bundle.Infra.Redis == prev.Infra.Redis {
		bundle.ownsRedis = true
		prev.Infra.Redis = nil
		prev.ownsRedis = false
	}

	if !bundle.ownsRabbitMQ && bundle.Infra.RabbitMQ == prev.Infra.RabbitMQ {
		bundle.ownsRabbitMQ = true
		prev.Infra.RabbitMQ = nil
		prev.ownsRabbitMQ = false
	}

	if !bundle.ownsObjectStorage && bundle.Infra.ObjectStorage == prev.Infra.ObjectStorage {
		bundle.ownsObjectStorage = true
		prev.Infra.ObjectStorage = nil
		prev.ownsObjectStorage = false
	}

	if bundle.HTTP == prev.HTTP {
		prev.HTTP = nil
	}
}

func (bundle *MatcherBundle) shouldCloseLogger() bool {
	return bundle != nil && (!bundle.ownershipTracked || bundle.ownsLogger)
}

func (bundle *MatcherBundle) shouldClosePostgres() bool {
	return bundle != nil && (!bundle.ownershipTracked || bundle.ownsPostgres)
}

func (bundle *MatcherBundle) shouldCloseRedis() bool {
	return bundle != nil && (!bundle.ownershipTracked || bundle.ownsRedis)
}

func (bundle *MatcherBundle) shouldCloseRabbitMQ() bool {
	return bundle != nil && (!bundle.ownershipTracked || bundle.ownsRabbitMQ)
}

func (bundle *MatcherBundle) shouldCloseObjectStorage() bool {
	return bundle != nil && (!bundle.ownershipTracked || bundle.ownsObjectStorage)
}
