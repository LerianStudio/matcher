// Copyright 2025 Lerian Studio. All rights reserved.
// Use of this source code is governed by an Elastic License 2.0
// that can be found in the LICENSE.md file.

package bootstrap

import (
	"context"
	"errors"
	"fmt"
	"net/url"
	"strings"

	libCommons "github.com/LerianStudio/lib-commons/v4/commons"
	"github.com/LerianStudio/lib-commons/v4/commons/assert"

	"github.com/LerianStudio/matcher/internal/shared/constants"
)

// errURLParsedToNil is returned when url.Parse succeeds but yields a nil *url.URL.
var errURLParsedToNil = errors.New("production config validation: URL parsed to nil")

const (
	maxRateLimitRequestsPerWindow = 1_000_000
	maxRateLimitWindowSeconds     = 86_400
)

// wellKnownDevCredentials lists passwords that are known development defaults.
// These pass the not-empty and not-guest checks but must NEVER be used in production.
// This list is intentionally kept in source code as a safety net — if a credential
// appears here, production validation will reject it.
var wellKnownDevCredentials = []string{
	"matcher_dev_password",
	"password",
	"changeme",
	"secret",
}

// isWellKnownDevCredential returns true if the given value matches a known development default.
func isWellKnownDevCredential(value string) bool {
	lower := strings.ToLower(strings.TrimSpace(value))
	for _, blocked := range wellKnownDevCredentials {
		if lower == blocked {
			return true
		}
	}

	return false
}

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

	if err := cfg.validateReportingStorageConfig(asserter); err != nil {
		return err
	}

	if err := cfg.validateInsecureObjectStoragePolicy(asserter); err != nil {
		return err
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

	if err := cfg.validateTenancyConfig(asserter); err != nil {
		return err
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

	if err := validateTrustedProxies(ctx, asserter, cfg.Server.TrustedProxies); err != nil {
		return err
	}

	if err := asserter.That(ctx, cfg.Infrastructure.ConnectTimeoutSec > 0, "InfraConnectTimeoutSec must be positive", "infra_connect_timeout_sec", cfg.Infrastructure.ConnectTimeoutSec); err != nil {
		return fmt.Errorf("config validation: %w", err)
	}

	if err := cfg.validateLogLevel(asserter); err != nil {
		return err
	}

	if err := cfg.validateTelemetryConfig(asserter); err != nil {
		return err
	}

	return nil
}

func (cfg *Config) validateLogLevel(asserter *assert.Asserter) error {
	ctx := context.Background()

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

	return nil
}

func (cfg *Config) validateTelemetryConfig(asserter *assert.Asserter) error {
	if !cfg.Telemetry.Enabled {
		return nil
	}

	ctx := context.Background()
	validOtelEnvs := map[string]bool{"development": true, "staging": true, "production": true}

	otelEnv := strings.ToLower(strings.TrimSpace(cfg.Telemetry.DeploymentEnv))
	_, validOtelEnv := validOtelEnvs[otelEnv]

	if err := asserter.That(ctx, validOtelEnv, "OTEL_RESOURCE_DEPLOYMENT_ENVIRONMENT must be one of: development, staging, production", "otel_env", cfg.Telemetry.DeploymentEnv); err != nil {
		return fmt.Errorf("config validation: %w", err)
	}

	return nil
}

func (cfg *Config) validateTenancyConfig(asserter *assert.Asserter) error {
	ctx := context.Background()

	if !multiTenantModeEnabled(cfg) {
		return nil
	}

	if err := validateMultiTenantURL(ctx, asserter, cfg.Tenancy.MultiTenantURL); err != nil {
		return err
	}

	if err := asserter.NotEmpty(ctx, strings.TrimSpace(cfg.Tenancy.MultiTenantServiceAPIKey), "MULTI_TENANT_SERVICE_API_KEY is required when multi-tenant mode is enabled"); err != nil {
		return fmt.Errorf("config validation: %w", err)
	}

	if err := asserter.That(ctx, cfg.Tenancy.MultiTenantMaxTenantPools > 0, "MULTI_TENANT_MAX_TENANT_POOLS must be positive", "multi_tenant_max_tenant_pools", cfg.Tenancy.MultiTenantMaxTenantPools); err != nil {
		return fmt.Errorf("config validation: %w", err)
	}

	if err := asserter.That(ctx, cfg.Tenancy.MultiTenantIdleTimeoutSec > 0, "MULTI_TENANT_IDLE_TIMEOUT_SEC must be positive", "multi_tenant_idle_timeout_sec", cfg.Tenancy.MultiTenantIdleTimeoutSec); err != nil {
		return fmt.Errorf("config validation: %w", err)
	}

	if err := asserter.That(ctx, cfg.Tenancy.MultiTenantTimeout > 0, "MULTI_TENANT_TIMEOUT must be positive", "multi_tenant_timeout", cfg.Tenancy.MultiTenantTimeout); err != nil {
		return fmt.Errorf("config validation: %w", err)
	}

	if err := asserter.That(ctx, cfg.Tenancy.MultiTenantCircuitBreakerThreshold > 0, "MULTI_TENANT_CIRCUIT_BREAKER_THRESHOLD must be positive", "multi_tenant_circuit_breaker_threshold", cfg.Tenancy.MultiTenantCircuitBreakerThreshold); err != nil {
		return fmt.Errorf("config validation: %w", err)
	}

	if err := asserter.That(ctx, cfg.Tenancy.MultiTenantCircuitBreakerTimeoutSec > 0, "MULTI_TENANT_CIRCUIT_BREAKER_TIMEOUT_SEC must be positive", "multi_tenant_circuit_breaker_timeout_sec", cfg.Tenancy.MultiTenantCircuitBreakerTimeoutSec); err != nil {
		return fmt.Errorf("config validation: %w", err)
	}

	if err := asserter.That(ctx, cfg.Tenancy.MultiTenantCacheTTLSec > 0, "MULTI_TENANT_CACHE_TTL_SEC must be positive", "multi_tenant_cache_ttl_sec", cfg.Tenancy.MultiTenantCacheTTLSec); err != nil {
		return fmt.Errorf("config validation: %w", err)
	}

	if err := asserter.That(ctx, cfg.Tenancy.MultiTenantConnectionsCheckIntervalSec > 0, "MULTI_TENANT_CONNECTIONS_CHECK_INTERVAL_SEC must be positive", "multi_tenant_connections_check_interval_sec", cfg.Tenancy.MultiTenantConnectionsCheckIntervalSec); err != nil {
		return fmt.Errorf("config validation: %w", err)
	}

	if err := asserter.NotEmpty(ctx, cfg.effectiveMultiTenantEnvironment(), "MULTI_TENANT_ENVIRONMENT is required when multi-tenant mode is enabled"); err != nil {
		return fmt.Errorf("config validation: %w", err)
	}

	return nil
}

func validateMultiTenantURL(ctx context.Context, asserter *assert.Asserter, rawURL string) error {
	if err := asserter.NotEmpty(ctx, strings.TrimSpace(rawURL), "MULTI_TENANT_URL is required when multi-tenant mode is enabled"); err != nil {
		return fmt.Errorf("config validation: %w", err)
	}

	parsedTenantURL, parseErr := url.Parse(strings.TrimSpace(rawURL))
	if err := asserter.NoError(ctx, parseErr, "MULTI_TENANT_URL must be a valid absolute URL"); err != nil {
		return fmt.Errorf("config validation: %w", err)
	}

	if err := asserter.That(
		ctx,
		parsedTenantURL != nil && parsedTenantURL.IsAbs() && parsedTenantURL.Host != "",
		"MULTI_TENANT_URL must be an absolute URL with scheme and host",
		"multi_tenant_url", rawURL,
	); err != nil {
		return fmt.Errorf("config validation: %w", err)
	}

	if err := asserter.That(
		ctx,
		strings.EqualFold(parsedTenantURL.Scheme, "http") || strings.EqualFold(parsedTenantURL.Scheme, "https"),
		"MULTI_TENANT_URL must use http or https",
		"multi_tenant_url", rawURL,
	); err != nil {
		return fmt.Errorf("config validation: %w", err)
	}

	return nil
}

func validateTrustedProxies(ctx context.Context, asserter *assert.Asserter, trustedProxies string) error {
	for _, candidate := range strings.Split(trustedProxies, ",") {
		proxy := strings.TrimSpace(candidate)
		if proxy == "" {
			continue
		}

		if err := asserter.That(
			ctx,
			proxy != "*" && proxy != "0.0.0.0/0" && proxy != "::/0",
			"TRUSTED_PROXIES must not trust all addresses",
			"trusted_proxy", proxy,
		); err != nil {
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

	if err := asserter.NotEmpty(ctx, strings.TrimSpace(cfg.Auth.Host), "PLUGIN_AUTH_ADDRESS is required when PLUGIN_AUTH_ENABLED=true"); err != nil {
		return fmt.Errorf("config validation: %w", err)
	}

	if err := asserter.NotEmpty(ctx, strings.TrimSpace(cfg.Auth.TokenSecret), "AUTH_JWT_SECRET is required when PLUGIN_AUTH_ENABLED=true"); err != nil {
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

	if err := asserter.That(ctx, !isWellKnownDevCredential(cfg.Postgres.PrimaryPassword),
		"POSTGRES_PASSWORD must not use a well-known development default in production",
		"password_hint", "set a strong, unique password via POSTGRES_PASSWORD env var"); err != nil {
		return fmt.Errorf("production config validation: %w", err)
	}

	if err := asserter.That(ctx, strings.TrimSpace(cfg.Server.CORSAllowedOrigins) != "" && !corsContainsWildcard(cfg.Server.CORSAllowedOrigins), "CORS_ALLOWED_ORIGINS must be restricted in production (exact \"*\" not allowed)", "cors_origins", cfg.Server.CORSAllowedOrigins); err != nil {
		return fmt.Errorf("production config validation: %w", err)
	}

	return nil
}

func (cfg *Config) validateProductionSecurityConfig(asserter *assert.Asserter) error {
	ctx := context.Background()

	if err := asserter.That(ctx, !strings.EqualFold(strings.TrimSpace(cfg.RabbitMQ.User), "guest") && !strings.EqualFold(strings.TrimSpace(cfg.RabbitMQ.Password), "guest"), "RABBITMQ credentials must be set to non-default values in production"); err != nil {
		return fmt.Errorf("production config validation: %w", err)
	}

	if err := asserter.That(ctx, !isWellKnownDevCredential(cfg.RabbitMQ.Password),
		"RABBITMQ_PASSWORD must not use a well-known development default in production",
		"password_hint", "set a strong, unique password via RABBITMQ_PASSWORD env var"); err != nil {
		return fmt.Errorf("production config validation: %w", err)
	}

	if err := asserter.That(ctx, !cfg.RabbitMQ.AllowInsecureHealthCheck, "RABBITMQ_ALLOW_INSECURE_HEALTH_CHECK must be false in production"); err != nil {
		return fmt.Errorf("production config validation: %w", err)
	}

	if err := asserter.That(ctx, !cfg.ObjectStorage.AllowInsecure, "OBJECT_STORAGE_ALLOW_INSECURE_ENDPOINT must be false in production"); err != nil {
		return fmt.Errorf("production config validation: %w", err)
	}

	return nil
}

func (cfg *Config) validateInsecureObjectStoragePolicy(asserter *assert.Asserter) error {
	if cfg == nil || !cfg.ObjectStorage.AllowInsecure {
		return nil
	}

	ctx := context.Background()

	if err := asserter.That(
		ctx,
		isAllowedInsecureObjectStorageEnvironment(cfg.App.EnvName),
		"OBJECT_STORAGE_ALLOW_INSECURE_ENDPOINT is restricted to local/development/test environments",
		"env_name",
		cfg.App.EnvName,
	); err != nil {
		return fmt.Errorf("config validation: %w", err)
	}

	return nil
}

func (cfg *Config) validateProductionOptionalConfig(asserter *assert.Asserter) error {
	ctx := context.Background()

	if err := asserter.NotEmpty(ctx, strings.TrimSpace(cfg.Redis.Password), "REDIS_PASSWORD is required in production"); err != nil {
		return fmt.Errorf("production config validation: %w", err)
	}

	if err := cfg.validateProductionEndpoints(asserter); err != nil {
		return err
	}

	return nil
}

// validateProductionEndpoints enforces HTTPS for service URLs in production environments.
func (cfg *Config) validateProductionEndpoints(asserter *assert.Asserter) error {
	ctx := context.Background()

	// Multi-tenant URL must use HTTPS in production — it carries tenant
	// provisioning data and service API keys.
	if multiTenantModeEnabled(cfg) && strings.TrimSpace(cfg.Tenancy.MultiTenantURL) != "" {
		if err := requireProductionHTTPS(ctx, asserter, cfg.Tenancy.MultiTenantURL, "MULTI_TENANT_URL"); err != nil {
			return err
		}
	}

	// Fetcher URL must use HTTPS in production when fetcher is enabled.
	if cfg.Fetcher.Enabled && strings.TrimSpace(cfg.Fetcher.URL) != "" {
		if err := requireProductionHTTPS(ctx, asserter, cfg.Fetcher.URL, "FETCHER_URL"); err != nil {
			return err
		}
	}

	// Object storage endpoint must use HTTPS in production when configured.
	if strings.TrimSpace(cfg.ObjectStorage.Endpoint) != "" {
		if err := requireProductionHTTPS(ctx, asserter, cfg.ObjectStorage.Endpoint, "OBJECT_STORAGE_ENDPOINT"); err != nil {
			return err
		}
	}

	return nil
}

// requireProductionHTTPS validates that rawURL uses HTTPS. The envVar name is
// used in both the assertion message and the structured key-value pair.
func requireProductionHTTPS(ctx context.Context, asserter *assert.Asserter, rawURL, envVar string) error {
	parsed, parseErr := url.Parse(strings.TrimSpace(rawURL))
	if parseErr != nil {
		return fmt.Errorf("production config validation: invalid URL %q for %s: %w", rawURL, envVar, parseErr)
	}

	if parsed == nil {
		return fmt.Errorf("%s: %w", envVar, errURLParsedToNil)
	}

	if err := asserter.That(ctx,
		strings.EqualFold(parsed.Scheme, "https"),
		envVar+" must use HTTPS in production",
		strings.ToLower(envVar), rawURL); err != nil {
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

func (cfg *Config) validateReportingStorageConfig(asserter *assert.Asserter) error {
	if cfg == nil || !reportingStorageRequired(cfg) {
		return nil
	}

	ctx := context.Background()

	if err := asserter.NotEmpty(ctx, strings.TrimSpace(cfg.ObjectStorage.Bucket), "OBJECT_STORAGE_BUCKET is required when export or cleanup workers are enabled"); err != nil {
		return fmt.Errorf("config validation: %w", err)
	}

	if err := asserter.NotEmpty(ctx, strings.TrimSpace(cfg.ObjectStorage.Endpoint), "OBJECT_STORAGE_ENDPOINT is required when export or cleanup workers are enabled"); err != nil {
		return fmt.Errorf("config validation: %w", err)
	}

	return nil
}

// corsContainsWildcard returns true if the comma-separated origin list contains
// an exact "*" entry. Subdomain wildcards like "https://*.example.com" are allowed.
func corsContainsWildcard(origins string) bool {
	for _, entry := range strings.Split(origins, ",") {
		if strings.TrimSpace(entry) == "*" {
			return true
		}
	}

	return false
}

func newConfigAsserter(ctx context.Context, operation string) *assert.Asserter {
	return assert.New(ctx, nil, constants.ApplicationName, operation)
}
