// Copyright 2025 Lerian Studio. All rights reserved.
// Use of this source code is governed by an Elastic License 2.0
// that can be found in the LICENSE.md file.

package bootstrap

import (
	"context"
	"fmt"
	"time"

	"github.com/gofiber/fiber/v2"

	libLog "github.com/LerianStudio/lib-commons/v4/commons/log"
	sharedhttp "github.com/LerianStudio/lib-commons/v4/commons/net/http"
	libPostgres "github.com/LerianStudio/lib-commons/v4/commons/postgres"
	libRabbitmq "github.com/LerianStudio/lib-commons/v4/commons/rabbitmq"
	libRedis "github.com/LerianStudio/lib-commons/v4/commons/redis"
)

// HealthCheckFunc is a function type for performing health checks on dependencies.
type HealthCheckFunc func(ctx context.Context) error

// ReadinessResponse represents the readiness check response.
// @Description Service readiness status with optional dependency checks
type ReadinessResponse struct {
	// Status is "ok" when all required dependencies are available, "degraded" otherwise
	Status string `json:"status"           example:"ok" enums:"ok,degraded"`
	// Checks contains individual dependency status (only in non-production environments)
	Checks *DependencyChecks `json:"checks,omitempty"`
}

// DependencyChecks contains the status of each infrastructure dependency.
// @Description Individual dependency health status
type DependencyChecks struct {
	// Database check status: ok, down, or unknown
	Database string `json:"database"        example:"ok" enums:"ok,down,unknown"`
	// DatabaseReplica check status: ok, down, or unknown
	DatabaseReplica string `json:"databaseReplica" example:"ok" enums:"ok,down,unknown"`
	// Redis check status: ok, down, or unknown
	Redis string `json:"redis"           example:"ok" enums:"ok,down,unknown"`
	// RabbitMQ check status: ok, down, or unknown
	RabbitMQ string `json:"rabbitmq"        example:"ok" enums:"ok,down,unknown"`
	// ObjectStorage check status: ok, down, or unknown
	ObjectStorage string `json:"objectStorage"   example:"ok" enums:"ok,down,unknown"`
}

// HealthDependencies holds references to infrastructure components for health checks.
type HealthDependencies struct {
	Postgres        *libPostgres.Client
	PostgresReplica *libPostgres.Client
	Redis           *libRedis.Client
	RabbitMQ        *libRabbitmq.RabbitMQConnection
	ObjectStorage   ObjectStorageHealthChecker

	PostgresCheck        HealthCheckFunc
	PostgresReplicaCheck HealthCheckFunc
	RedisCheck           HealthCheckFunc
	RabbitMQCheck        HealthCheckFunc
	ObjectStorageCheck   HealthCheckFunc

	// Optional dependencies do not impact overall readiness status when unavailable
	// or failing their readiness checks.
	PostgresOptional        bool
	PostgresReplicaOptional bool
	RedisOptional           bool
	RabbitMQOptional        bool
	ObjectStorageOptional   bool
}

// ObjectStorageHealthChecker is an interface for checking object storage health.
type ObjectStorageHealthChecker interface {
	// Exists checks if an object exists at the given key (used for health check).
	Exists(ctx context.Context, key string) (bool, error)
}

// NewHealthDependencies creates a new HealthDependencies with default settings.
func NewHealthDependencies(
	postgres *libPostgres.Client,
	postgresReplica *libPostgres.Client,
	redis *libRedis.Client,
	rabbitmq *libRabbitmq.RabbitMQConnection,
	objectStorage ObjectStorageHealthChecker,
) *HealthDependencies {
	return &HealthDependencies{
		Postgres:        postgres,
		PostgresReplica: postgresReplica,
		Redis:           redis,
		RabbitMQ:        rabbitmq,
		ObjectStorage:   objectStorage,

		// Redis, replica, and object storage are treated as optional dependencies by default.
		RedisOptional:           true,
		PostgresReplicaOptional: true,
		ObjectStorageOptional:   true,
	}
}

// healthHandler responds to liveness probe requests.
//
//	@Summary		Liveness check
//	@Description	Returns a simple health check response to indicate the service is alive.
//	@Description	Used by Kubernetes liveness probes to detect if the service needs to be restarted.
//	@Tags			Health
//	@Produce		plain
//	@Success		200	{string}	string	"healthy"
//	@Router			/health [get]
//	@ID				getHealth
func healthHandler(c *fiber.Ctx) error {
	c.Type("txt")

	return c.SendString("healthy")
}

// readinessHandler creates a handler that responds to readiness probe requests.
//
//	@Summary		Readiness check
//	@Description	Checks if the service is ready to accept traffic by verifying all required dependencies.
//	@Description	Used by Kubernetes readiness probes to control traffic routing.
//	@Description	Returns 200 OK when all required dependencies are healthy, 503 Service Unavailable otherwise.
//	@Description	Dependency check details are only included in non-production environments.
//	@Tags			Health
//	@Produce		json
//	@Success		200	{object}	ReadinessResponse	"Service is ready to accept traffic"
//	@Failure		503	{object}	ReadinessResponse	"Service is not ready (degraded state)"
//	@Router			/ready [get]
//	@ID				getReady
func readinessHandler(initialCfg *Config, configGetter func() *Config, deps *HealthDependencies, logger libLog.Logger) fiber.Handler {
	return func(fiberCtx *fiber.Ctx) error {
		ctx := fiberCtx.UserContext()
		if ctx == nil {
			ctx = context.Background()
		}

		cfg := initialCfg

		if configGetter != nil {
			if runtimeCfg := configGetter(); runtimeCfg != nil {
				cfg = runtimeCfg
			}
		}

		// Derive per-check timeout from the current effective config; fall back to
		// the default in applyReadinessCheck when zero/non-positive.
		var healthCheckTimeout time.Duration
		if cfg != nil && cfg.Infrastructure.HealthCheckTimeoutSec > 0 {
			healthCheckTimeout = time.Duration(cfg.Infrastructure.HealthCheckTimeoutSec) * time.Second
		}

		status, readyStatus, checks := evaluateReadinessChecksWithTimeout(ctx, deps, logger, healthCheckTimeout)

		response := ReadinessResponse{
			Status: readyStatus,
		}

		if shouldIncludeReadinessDetails(cfg) {
			response.Checks = &DependencyChecks{
				Database:        checksToString(checks, "database", logger),
				DatabaseReplica: checksToString(checks, "databaseReplica", logger),
				Redis:           checksToString(checks, "redis", logger),
				RabbitMQ:        checksToString(checks, "rabbitmq", logger),
				ObjectStorage:   checksToString(checks, "objectStorage", logger),
			}
		}

		return sharedhttp.Respond(fiberCtx, status, response)
	}
}

func checksToString(checks fiber.Map, key string, logger libLog.Logger) string {
	if checks == nil {
		return statusUnknown
	}

	val, ok := checks[key]
	if !ok {
		return statusUnknown
	}

	stringVal, ok := val.(string)
	if !ok {
		if logger != nil {
			logger.Log(context.Background(), libLog.LevelDebug, "Readiness check value not a string: "+key)
		}

		return statusUnknown
	}

	return stringVal
}

func evaluateReadinessChecksWithTimeout(
	ctx context.Context,
	deps *HealthDependencies,
	logger libLog.Logger,
	timeout time.Duration,
) (int, string, fiber.Map) {
	checks := fiber.Map{}
	allOk := true

	postgresOptional := deps != nil && deps.PostgresOptional
	postgresCheck, postgresAvailable := resolvePostgresCheck(deps)
	postgresOk := applyReadinessCheck(
		ctx,
		"database",
		checks,
		postgresCheck,
		postgresAvailable,
		postgresOptional,
		logger,
		timeout,
	)
	allOk = allOk && postgresOk

	replicaOptional := deps != nil && deps.PostgresReplicaOptional
	replicaCheck, replicaAvailable := resolvePostgresReplicaCheck(deps)
	replicaOk := applyReadinessCheck(
		ctx,
		"databaseReplica",
		checks,
		replicaCheck,
		replicaAvailable,
		replicaOptional,
		logger,
		timeout,
	)
	allOk = allOk && replicaOk

	redisOptional := deps != nil && deps.RedisOptional
	redisCheck, redisAvailable := resolveRedisCheck(deps)
	redisOk := applyReadinessCheck(
		ctx,
		"redis",
		checks,
		redisCheck,
		redisAvailable,
		redisOptional,
		logger,
		timeout,
	)
	allOk = allOk && redisOk

	rabbitOptional := deps != nil && deps.RabbitMQOptional
	rabbitCheck, rabbitAvailable := resolveRabbitMQCheck(deps)
	rabbitOk := applyReadinessCheck(
		ctx,
		"rabbitmq",
		checks,
		rabbitCheck,
		rabbitAvailable,
		rabbitOptional,
		logger,
		timeout,
	)
	allOk = allOk && rabbitOk

	objectStorageOptional := deps != nil && deps.ObjectStorageOptional
	objectStorageCheck, objectStorageAvailable := resolveObjectStorageCheck(deps)
	objectStorageOk := applyReadinessCheck(
		ctx,
		"objectStorage",
		checks,
		objectStorageCheck,
		objectStorageAvailable,
		objectStorageOptional,
		logger,
		timeout,
	)
	allOk = allOk && objectStorageOk

	status := fiber.StatusOK
	readyStatus := "ok"

	if !allOk {
		status = fiber.StatusServiceUnavailable
		readyStatus = "degraded"
	}

	return status, readyStatus, checks
}

// applyReadinessCheck performs a readiness check and returns true if the check passed
// or the dependency is optional (allowing the service to remain ready despite optional failures).
// The timeout parameter controls the per-check deadline; when zero, the default of 5s is used.
func applyReadinessCheck(
	ctx context.Context,
	name string,
	checks fiber.Map,
	checkFunc HealthCheckFunc,
	available, optional bool,
	logger libLog.Logger,
	timeout time.Duration,
) bool {
	if !available || checkFunc == nil {
		checks[name] = statusUnknown

		return optional
	}

	// Apply a per-check timeout to prevent a single hung dependency
	// from blocking the entire readiness probe.
	const perCheckTimeout = 5 * time.Second

	effectiveTimeout := perCheckTimeout
	if timeout > 0 {
		effectiveTimeout = timeout
	}

	checkCtx, checkCancel := context.WithTimeout(ctx, effectiveTimeout)
	defer checkCancel()

	if err := checkFunc(checkCtx); err != nil {
		checks[name] = "down"
		if logger != nil {
			logger.Log(ctx, libLog.LevelWarn, fmt.Sprintf("Readiness check failed: %s %v", name, err))
		}

		return optional
	}

	checks[name] = "ok"

	return true
}

func shouldIncludeReadinessDetails(cfg *Config) bool {
	if cfg == nil {
		return false
	}

	return !IsProductionEnvironment(cfg.App.EnvName)
}
