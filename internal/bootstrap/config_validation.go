// Copyright 2025 Lerian Studio. All rights reserved.
// Use of this source code is governed by an Elastic License 2.0
// that can be found in the LICENSE.md file.

package bootstrap

import (
	"context"
	"fmt"
	"strings"

	libCommons "github.com/LerianStudio/lib-commons/v4/commons"
	"github.com/LerianStudio/lib-commons/v4/commons/assert"

	"github.com/LerianStudio/matcher/internal/shared/constants"
)

const (
	maxRateLimitRequestsPerWindow = 1_000_000
	maxRateLimitWindowSeconds     = 86_400
)

// Validate checks the configuration for required fields and production constraints.
func (cfg *Config) Validate() error {
	ctx := context.Background()
	asserter := newConfigAsserter(ctx, "config.validate")

	if err := asserter.NotNil(ctx, cfg, "config must be provided"); err != nil {
		return fmt.Errorf("config validation: %w", err)
	}

	if IsProductionEnvironment(cfg.App.EnvName) {
		if err := cfg.validateProductionConfig(asserter); err != nil {
			return err
		}
	}

	if err := cfg.validateServerConfig(asserter); err != nil {
		return err
	}

	if err := cfg.validateRateLimitConfig(asserter); err != nil {
		return err
	}

	if err := cfg.validateArchivalConfig(asserter); err != nil {
		return err
	}

	if cfg.Fetcher.Enabled {
		if err := cfg.validateFetcherConfig(asserter); err != nil {
			return err
		}
	}

	return nil
}

// validateServerConfig validates server and middleware configuration.
func (cfg *Config) validateServerConfig(asserter *assert.Asserter) error {
	ctx := context.Background()

	if err := asserter.That(ctx, (strings.TrimSpace(cfg.Server.TLSCertFile) == "") == (strings.TrimSpace(cfg.Server.TLSKeyFile) == ""), "SERVER_TLS_CERT_FILE and SERVER_TLS_KEY_FILE must be set together"); err != nil {
		return fmt.Errorf("config validation: %w", err)
	}

	if err := cfg.validateAuthConfig(asserter); err != nil {
		return err
	}

	if err := asserter.That(ctx, libCommons.IsUUID(cfg.Tenancy.DefaultTenantID), "DEFAULT_TENANT_ID must be a valid UUID", "tenant_id", cfg.Tenancy.DefaultTenantID); err != nil {
		return fmt.Errorf("config validation: %w", err)
	}

	if err := asserter.That(ctx, cfg.Server.BodyLimitBytes > 0, "HTTP_BODY_LIMIT_BYTES must be positive", "body_limit", cfg.Server.BodyLimitBytes); err != nil {
		return fmt.Errorf("config validation: %w", err)
	}

	if err := asserter.That(ctx, cfg.Postgres.ConnectTimeoutSec >= 0, "PostgresConnectTimeoutSec must be non-negative", "postgres_connect_timeout_sec", cfg.Postgres.ConnectTimeoutSec); err != nil {
		return fmt.Errorf("config validation: %w", err)
	}

	if err := asserter.That(ctx, cfg.Postgres.QueryTimeoutSec >= 0, "PostgresQueryTimeoutSec must be non-negative", "postgres_query_timeout_sec", cfg.Postgres.QueryTimeoutSec); err != nil {
		return fmt.Errorf("config validation: %w", err)
	}

	if err := asserter.That(ctx, cfg.Webhook.TimeoutSec >= 0, "WEBHOOK_TIMEOUT_SEC must be non-negative (see Config.WebhookTimeout() for runtime defaulting/capping)", "webhook_timeout_sec", cfg.Webhook.TimeoutSec); err != nil {
		return fmt.Errorf("config validation: %w", err)
	}

	if err := asserter.That(ctx, cfg.Infrastructure.ConnectTimeoutSec > 0, "InfraConnectTimeoutSec must be positive", "infra_connect_timeout_sec", cfg.Infrastructure.ConnectTimeoutSec); err != nil {
		return fmt.Errorf("config validation: %w", err)
	}

	validLogLevels := map[string]bool{
		"debug": true,
		"info":  true,
		"warn":  true,
		"error": true,
		"fatal": true,
	}

	logLevel := strings.ToLower(strings.TrimSpace(cfg.App.LogLevel))
	_, validLogLevel := validLogLevels[logLevel]

	if err := asserter.That(ctx, validLogLevel, "LOG_LEVEL must be one of: debug, info, warn, error, fatal", "log_level", cfg.App.LogLevel); err != nil {
		return fmt.Errorf("config validation: %w", err)
	}

	if cfg.Telemetry.Enabled {
		validOtelEnvs := map[string]bool{"development": true, "staging": true, "production": true}

		otelEnv := strings.ToLower(strings.TrimSpace(cfg.Telemetry.DeploymentEnv))
		_, validOtelEnv := validOtelEnvs[otelEnv]

		if err := asserter.That(ctx, validOtelEnv, "OTEL_RESOURCE_DEPLOYMENT_ENVIRONMENT must be one of: development, staging, production", "otel_env", cfg.Telemetry.DeploymentEnv); err != nil {
			return fmt.Errorf("config validation: %w", err)
		}
	}

	return nil
}

// validateAuthConfig validates authentication configuration when auth is enabled.
func (cfg *Config) validateAuthConfig(asserter *assert.Asserter) error {
	if !cfg.Auth.Enabled {
		return nil
	}

	ctx := context.Background()

	if err := asserter.NotEmpty(ctx, strings.TrimSpace(cfg.Auth.Host), "AUTH_SERVICE_ADDRESS is required when AUTH_ENABLED=true"); err != nil {
		return fmt.Errorf("config validation: %w", err)
	}

	if err := asserter.NotEmpty(ctx, strings.TrimSpace(cfg.Auth.TokenSecret), "AUTH_JWT_SECRET is required when AUTH_ENABLED=true"); err != nil {
		return fmt.Errorf("config validation: %w", err)
	}

	return nil
}

// validateRateLimitConfig validates rate limiting configuration.
// Export limits are always validated; global and dispatch limits only when rate limiting is enabled.
func (cfg *Config) validateRateLimitConfig(asserter *assert.Asserter) error {
	if err := cfg.validateExportRateLimitConfig(asserter); err != nil {
		return err
	}

	if !cfg.RateLimit.Enabled {
		return nil
	}

	return cfg.validateActiveRateLimitConfig(asserter)
}

// validateExportRateLimitConfig validates export-specific rate limit bounds.
func (cfg *Config) validateExportRateLimitConfig(asserter *assert.Asserter) error {
	ctx := context.Background()

	if err := asserter.That(ctx, cfg.RateLimit.ExportMax > 0, "EXPORT_RATE_LIMIT_MAX must be positive", "export_rate_limit_max", cfg.RateLimit.ExportMax); err != nil {
		return fmt.Errorf("config validation: %w", err)
	}

	if err := asserter.That(ctx, cfg.RateLimit.ExportMax <= maxRateLimitRequestsPerWindow,
		"EXPORT_RATE_LIMIT_MAX must not exceed 1000000",
		"export_rate_limit_max", cfg.RateLimit.ExportMax); err != nil {
		return fmt.Errorf("config validation: %w", err)
	}

	if err := asserter.That(ctx, cfg.RateLimit.ExportExpirySec > 0, "EXPORT_RATE_LIMIT_EXPIRY_SEC must be positive", "export_rate_limit_expiry", cfg.RateLimit.ExportExpirySec); err != nil {
		return fmt.Errorf("config validation: %w", err)
	}

	if err := asserter.That(ctx, cfg.RateLimit.ExportExpirySec <= maxRateLimitWindowSeconds,
		"EXPORT_RATE_LIMIT_EXPIRY_SEC must not exceed 86400",
		"export_rate_limit_expiry", cfg.RateLimit.ExportExpirySec); err != nil {
		return fmt.Errorf("config validation: %w", err)
	}

	return nil
}

// validateActiveRateLimitConfig validates global and dispatch rate limit bounds
// when rate limiting is enabled.
func (cfg *Config) validateActiveRateLimitConfig(asserter *assert.Asserter) error {
	ctx := context.Background()

	if err := asserter.That(ctx, cfg.RateLimit.Max > 0, "RATE_LIMIT_MAX must be positive", "rate_limit_max", cfg.RateLimit.Max); err != nil {
		return fmt.Errorf("config validation: %w", err)
	}

	if err := asserter.That(ctx, cfg.RateLimit.Max <= maxRateLimitRequestsPerWindow,
		"RATE_LIMIT_MAX must not exceed 1000000",
		"rate_limit_max", cfg.RateLimit.Max); err != nil {
		return fmt.Errorf("config validation: %w", err)
	}

	if err := asserter.That(ctx, cfg.RateLimit.ExpirySec > 0, "RATE_LIMIT_EXPIRY_SEC must be positive", "rate_limit_expiry", cfg.RateLimit.ExpirySec); err != nil {
		return fmt.Errorf("config validation: %w", err)
	}

	if err := asserter.That(ctx, cfg.RateLimit.ExpirySec <= maxRateLimitWindowSeconds,
		"RATE_LIMIT_EXPIRY_SEC must not exceed 86400",
		"rate_limit_expiry", cfg.RateLimit.ExpirySec); err != nil {
		return fmt.Errorf("config validation: %w", err)
	}

	if err := asserter.That(ctx, cfg.RateLimit.DispatchMax > 0, "DISPATCH_RATE_LIMIT_MAX must be positive", "dispatch_rate_limit_max", cfg.RateLimit.DispatchMax); err != nil {
		return fmt.Errorf("config validation: %w", err)
	}

	if err := asserter.That(ctx, cfg.RateLimit.DispatchMax <= maxRateLimitRequestsPerWindow,
		"DISPATCH_RATE_LIMIT_MAX must not exceed 1000000",
		"dispatch_rate_limit_max", cfg.RateLimit.DispatchMax); err != nil {
		return fmt.Errorf("config validation: %w", err)
	}

	if err := asserter.That(ctx, cfg.RateLimit.DispatchExpirySec > 0, "DISPATCH_RATE_LIMIT_EXPIRY_SEC must be positive", "dispatch_rate_limit_expiry", cfg.RateLimit.DispatchExpirySec); err != nil {
		return fmt.Errorf("config validation: %w", err)
	}

	if err := asserter.That(ctx, cfg.RateLimit.DispatchExpirySec <= maxRateLimitWindowSeconds,
		"DISPATCH_RATE_LIMIT_EXPIRY_SEC must not exceed 86400",
		"dispatch_rate_limit_expiry", cfg.RateLimit.DispatchExpirySec); err != nil {
		return fmt.Errorf("config validation: %w", err)
	}

	return nil
}

// validateProductionConfig validates configuration constraints specific to production environments.
func (cfg *Config) validateProductionConfig(asserter *assert.Asserter) error {
	if err := cfg.validateProductionCoreConfig(asserter); err != nil {
		return err
	}

	if err := cfg.validateProductionSecurityConfig(asserter); err != nil {
		return err
	}

	if err := cfg.validateProductionOptionalConfig(asserter); err != nil {
		return err
	}

	return nil
}

func (cfg *Config) validateProductionCoreConfig(asserter *assert.Asserter) error {
	ctx := context.Background()

	if err := asserter.NotEmpty(ctx, strings.TrimSpace(cfg.Postgres.PrimaryPassword), "POSTGRES_PASSWORD is required in production"); err != nil {
		return fmt.Errorf("production config validation: %w", err)
	}

	if err := asserter.That(ctx, strings.TrimSpace(cfg.Server.CORSAllowedOrigins) != "" && !strings.Contains(cfg.Server.CORSAllowedOrigins, "*"), "CORS_ALLOWED_ORIGINS must be restricted in production", "cors_origins", cfg.Server.CORSAllowedOrigins); err != nil {
		return fmt.Errorf("production config validation: %w", err)
	}

	return nil
}

func (cfg *Config) validateProductionSecurityConfig(asserter *assert.Asserter) error {
	ctx := context.Background()

	if err := asserter.That(ctx, !strings.EqualFold(strings.TrimSpace(cfg.RabbitMQ.User), "guest") && !strings.EqualFold(strings.TrimSpace(cfg.RabbitMQ.Password), "guest"), "RABBITMQ credentials must be set to non-default values in production"); err != nil {
		return fmt.Errorf("production config validation: %w", err)
	}

	if err := asserter.That(ctx, !cfg.RabbitMQ.AllowInsecureHealthCheck, "RABBITMQ_ALLOW_INSECURE_HEALTH_CHECK must be false in production"); err != nil {
		return fmt.Errorf("production config validation: %w", err)
	}

	return nil
}

func (cfg *Config) validateProductionOptionalConfig(asserter *assert.Asserter) error {
	ctx := context.Background()

	if err := asserter.NotEmpty(ctx, strings.TrimSpace(cfg.Redis.Password), "REDIS_PASSWORD is required in production"); err != nil {
		return fmt.Errorf("production config validation: %w", err)
	}

	return nil
}

// validateArchivalConfig validates archival worker configuration.
// Retention and batch validations only run when archival is enabled because
// lib-commons.SetConfigFromEnvVars does not apply envDefault tags -- fields
// default to Go zero values when env vars are absent.
func (cfg *Config) validateArchivalConfig(asserter *assert.Asserter) error {
	if !cfg.Archival.Enabled {
		return nil
	}

	ctx := context.Background()

	if err := asserter.NotEmpty(ctx, strings.TrimSpace(cfg.Archival.StorageBucket), "ARCHIVAL_STORAGE_BUCKET is required when ARCHIVAL_WORKER_ENABLED=true"); err != nil {
		return fmt.Errorf("config validation: %w", err)
	}

	if err := asserter.That(ctx, cfg.Archival.HotRetentionDays > 0, "ARCHIVAL_HOT_RETENTION_DAYS must be positive", "hot_retention_days", cfg.Archival.HotRetentionDays); err != nil {
		return fmt.Errorf("config validation: %w", err)
	}

	if err := asserter.That(ctx, cfg.Archival.BatchSize > 0, "ARCHIVAL_BATCH_SIZE must be positive", "batch_size", cfg.Archival.BatchSize); err != nil {
		return fmt.Errorf("config validation: %w", err)
	}

	if err := asserter.That(ctx, cfg.Archival.PartitionLookahead > 0, "ARCHIVAL_PARTITION_LOOKAHEAD must be positive", "partition_lookahead", cfg.Archival.PartitionLookahead); err != nil {
		return fmt.Errorf("config validation: %w", err)
	}

	hotAsMonths := cfg.Archival.HotRetentionDays / 30 //nolint:mnd // 30 days per month approximation

	if err := asserter.That(ctx, cfg.Archival.WarmRetentionMonths > hotAsMonths, "ARCHIVAL_WARM_RETENTION_MONTHS must be greater than ARCHIVAL_HOT_RETENTION_DAYS / 30", "warm_months", cfg.Archival.WarmRetentionMonths, "hot_as_months", hotAsMonths); err != nil {
		return fmt.Errorf("config validation: %w", err)
	}

	if err := asserter.That(ctx, cfg.Archival.ColdRetentionMonths >= cfg.Archival.WarmRetentionMonths, "ARCHIVAL_COLD_RETENTION_MONTHS must be >= ARCHIVAL_WARM_RETENTION_MONTHS", "cold_months", cfg.Archival.ColdRetentionMonths, "warm_months", cfg.Archival.WarmRetentionMonths); err != nil {
		return fmt.Errorf("config validation: %w", err)
	}

	return nil
}

// validateFetcherConfig validates fetcher-related configuration.
// Validation is skipped when the fetcher is disabled.
func (cfg *Config) validateFetcherConfig(asserter *assert.Asserter) error {
	if !cfg.Fetcher.Enabled {
		return nil
	}

	ctx := context.Background()

	if err := asserter.NotEmpty(ctx, strings.TrimSpace(cfg.Fetcher.URL), "FETCHER_URL is required when FETCHER_ENABLED=true"); err != nil {
		return fmt.Errorf("config validation: %w", err)
	}

	return nil
}

func newConfigAsserter(ctx context.Context, operation string) *assert.Asserter {
	return assert.New(ctx, nil, constants.ApplicationName, operation)
}
