// Copyright 2025 Lerian Studio. All rights reserved.
// Use of this source code is governed by an Elastic License 2.0
// that can be found in the LICENSE.md file.

package bootstrap

// Direct OTel imports are required for infrastructure-level meter/tracer setup.
// otel.Meter() and otel.Tracer() create named instruments for cleanup metrics
// and outbox/archival tracers. attribute/metric types are needed for metric
// recording. lib-commons does not abstract global provider accessors.
import (
	"context"
	"errors"
	"fmt"
	"os"
	"time"

	"github.com/google/uuid"

	libLog "github.com/LerianStudio/lib-commons/v5/commons/log"

	"github.com/LerianStudio/matcher/internal/auth"
)

const (
	healthConnMaxLifetime = 5 * time.Minute
	minPerServiceTimeout  = 5 * time.Second

	// archivalMaxOpenConns is the max open connections for the dedicated archival DB pool.
	// Low count because archival runs sequentially with long-lived transactions.
	archivalMaxOpenConns = 3

	// archivalMaxIdleConns is the max idle connections for the dedicated archival DB pool.
	archivalMaxIdleConns = 1

	// infraConnectTimeoutDivisor splits the total infra connect timeout evenly between
	// the two parallel infrastructure goroutines (Postgres and RabbitMQ).
	infraConnectTimeoutDivisor = 2

	// statusSuccess and statusError are metric attribute values for cleanup recording.
	statusSuccess = "success"
	statusError   = "error"
)

var (
	// ErrObjectStorageBucketRequired is returned when export worker is enabled but bucket is not configured.
	ErrObjectStorageBucketRequired = errors.New(
		"OBJECT_STORAGE_BUCKET is required when EXPORT_WORKER_ENABLED=true",
	)

	// ErrArchivalStorageRequired is returned when archival worker is enabled but storage is not configured.
	ErrArchivalStorageRequired = errors.New("archival storage is required when ARCHIVAL_WORKER_ENABLED=true")

	// ErrAuditPublisherRequired is returned when the system starts without audit publishing capability.
	// Audit events are compliance-critical (SOX) and must never be silently dropped.
	ErrAuditPublisherRequired = errors.New("audit publisher is required: compliance-critical audit events must not be dropped")

	errPostgresClientRequired   = errors.New("postgres client is required")
	errRabbitMQClientRequired   = errors.New("rabbitmq connection is required")
	errPostgresResolverRequired = errors.New("postgres resolver is nil")
	errAuthBoundaryLoggerNil    = errors.New("auth boundary logger is nil")

	// errSystemplanePrimaryUnavailable indicates the postgres primary handle
	// returned nil without a concrete error, blocking systemplane init in
	// production environments where runtime config is compliance-critical.
	errSystemplanePrimaryUnavailable = errors.New("systemplane init: postgres primary unavailable")
)

// tenantExtractorAdapter adapts auth.GetTenantID to the TenantExtractor interface.
type tenantExtractorAdapter struct{}

// GetTenantID extracts the tenant ID from context using the auth package.
func (t *tenantExtractorAdapter) GetTenantID(ctx context.Context) uuid.UUID {
	tenantIDStr := auth.GetTenantID(ctx)

	id, err := uuid.Parse(tenantIDStr)
	if err != nil {
		return uuid.Nil
	}

	return id
}

func buildTenantExtractor(cfg *Config) (*auth.TenantExtractor, error) {
	if err := auth.SetDefaultTenantID(cfg.Tenancy.DefaultTenantID); err != nil {
		return nil, fmt.Errorf("set default tenant id: %w", err)
	}

	if err := auth.SetDefaultTenantSlug(cfg.Tenancy.DefaultTenantSlug); err != nil {
		return nil, fmt.Errorf("set default tenant slug: %w", err)
	}

	extractor, err := auth.NewTenantExtractor(
		cfg.Auth.Enabled,
		cfg.Tenancy.MultiTenantEnabled,
		cfg.Tenancy.DefaultTenantID,
		cfg.Tenancy.DefaultTenantSlug,
		cfg.Auth.TokenSecret,
		cfg.App.EnvName,
	)
	if err != nil {
		return nil, fmt.Errorf("create tenant extractor: %w", err)
	}

	return extractor, nil
}

// InitServersWithOptions initializes and returns the complete Matcher service with custom options.
//
// The body is intentionally linear: each phase function writes into the shared
// bootstrapState and returns an error. The orchestrator's only responsibility
// is the cleanup chain — it arms a deferred tear-down that runs on any failure
// until success is flipped at the end.
func InitServersWithOptions(opts *Options) (*Service, error) {
	state, err := initConfigStage(opts)
	if err != nil {
		return nil, err
	}

	// Track success to skip cleanup on successful startup.
	success := false

	// Deferred tear-down armed before any fallible connection opens.
	// Runs cleanups in LIFO order, closes the connection manager (if created),
	// and tears down the raw infra connections. On happy-path success the flag
	// is flipped and this defer returns early.
	defer func() {
		if success {
			return
		}

		for i := len(state.cleanups) - 1; i >= 0; i-- {
			state.cleanups[i]()
		}

		if state.infraConnectionManager != nil {
			logCloseErr(state.ctx, state.logger, "failed to close connection manager", state.infraConnectionManager.Close)
		}

		cleanupConnections(state.ctx, state.postgresConnection, state.redisConnection, state.rabbitMQConnection, state.logger)
	}()

	if err := initInfrastructure(state); err != nil {
		return nil, err
	}

	if err := initServers(state); err != nil {
		return nil, err
	}

	svc := assembleService(state)

	success = true

	return svc, nil
}

// configGetterFuncFromManager returns the ConfigManager's Get function for use as
// a dynamic config getter, or nil if the manager is unavailable.
func configGetterFuncFromManager(configManager *ConfigManager) func() *Config {
	if configManager == nil {
		return nil
	}

	return configManager.Get
}

func initLogger(opts *Options) (libLog.Logger, error) {
	if opts != nil && opts.Logger != nil {
		return opts.Logger, nil
	}

	loggerBundle, err := buildLoggerBundle(os.Getenv("ENV_NAME"), os.Getenv("LOG_LEVEL"))
	if err != nil {
		return nil, fmt.Errorf("initialize logger: %w", err)
	}

	return loggerBundle.Logger, nil
}
