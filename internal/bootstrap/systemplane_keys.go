// Copyright 2025 Lerian Studio. All rights reserved.
// Use of this source code is governed by an Elastic License 2.0
// that can be found in the LICENSE.md file.

package bootstrap

import (
	"fmt"
	"strings"

	"github.com/LerianStudio/matcher/pkg/systemplane/domain"
	"github.com/LerianStudio/matcher/pkg/systemplane/registry"
)

// Default values for Matcher configuration keys. These constants match the
// envDefault tag values in the Config struct hierarchy and serve as the
// canonical source of truth for the systemplane registry.
const (
	// App defaults.
	defaultEnvName  = "development"
	defaultLogLevel = "info"

	// Server defaults.
	defaultServerAddress         = ":4018"
	defaultCORSAllowedOrigins    = "http://localhost:3000"
	defaultCORSAllowedMethods    = "GET,POST,PUT,PATCH,DELETE,OPTIONS"
	defaultCORSAllowedHeaders    = "Origin,Content-Type,Accept,Authorization,X-Request-ID"
	defaultKeyBodyLimitBytes     = 104857600 // 100 MB
	defaultTLSTerminatedUpstream = false
	defaultServerTrustedProxies  = ""
	defaultServerTLSCertFile     = ""
	defaultServerTLSKeyFile      = ""

	// Tenancy defaults.
	defaultTenantID                        = "11111111-1111-1111-1111-111111111111"
	defaultTenantSlug                      = "default"
	defaultMultiTenantEnabled              = false
	defaultMultiTenantMaxTenantPools       = 100
	defaultMultiTenantIdleTimeoutSec       = 300
	defaultMultiTenantCircuitBreakerThresh = 5
	defaultMultiTenantCircuitBreakerSec    = 30
	defaultMultiTenantInfraEnabled         = false

	// PostgreSQL defaults.
	defaultPGHost            = "localhost"
	defaultPGPort            = "5432"
	defaultPGUser            = "matcher"
	defaultPGPassword        = "matcher_dev_password" //nolint:gosec // G101: Dev-mode default; rejected by validateProductionConfig in production.
	defaultPGDB              = "matcher"
	defaultPGSSLMode         = "disable"
	defaultPGMaxOpenConns    = 25
	defaultPGMaxIdleConns    = 5
	defaultPGConnMaxLifeMins = 30
	defaultPGConnMaxIdleMins = 5
	defaultPGConnectTimeout  = 10
	defaultPGQueryTimeout    = 30
	defaultPGMigrationsPath  = "migrations"

	// Redis defaults.
	defaultRedisHost         = "localhost:6379"
	defaultRedisDB           = 0
	defaultRedisProtocol     = 3
	defaultRedisTLS          = false
	defaultRedisPoolSize     = 10
	defaultRedisMinIdleConn  = 2
	defaultRedisReadTimeout  = 3000
	defaultRedisWriteTimeout = 3000
	defaultRedisDialTimeout  = 5000

	// RabbitMQ defaults.
	defaultRabbitURI                 = "amqp"
	defaultRabbitHost                = "localhost"
	defaultRabbitPort                = "5672"
	defaultRabbitUser                = "matcher_admin"
	defaultRabbitPassword            = "matcher_dev_password" //nolint:gosec // G101: Dev-mode default; rejected by validateProductionConfig in production.
	defaultRabbitVHost               = "/"
	defaultRabbitHealthURL           = "http://localhost:15672"
	defaultRabbitAllowInsecureHealth = false

	// Auth defaults.
	defaultAuthEnabled = false

	// Swagger defaults.
	defaultSwaggerEnabled = false
	defaultSwaggerSchemes = "https"

	// Telemetry defaults.
	defaultTelemetryEnabled         = false
	defaultTelemetryServiceName     = "matcher"
	defaultTelemetryLibraryName     = "github.com/LerianStudio/matcher"
	defaultTelemetryServiceVersion  = "1.0.0"
	defaultTelemetryDeploymentEnv   = "development"
	defaultTelemetryCollectorEP     = "localhost:4317"
	defaultTelemetryDBMetricsIntSec = 15

	// Rate limit defaults.
	defaultRateLimitEnabled      = true
	defaultRateLimitMax          = 100
	defaultRateLimitExpirySec    = 60
	defaultRateLimitExportMax    = 10
	defaultRateLimitExportExpiry = 60
	defaultRateLimitDispatchMax  = 50
	defaultRateLimitDispatchExp  = 60

	// Infrastructure defaults.
	defaultInfraConnectTimeout     = 30
	defaultInfraHealthCheckTimeout = 5

	// Idempotency defaults.
	defaultIdempotencyRetryWindow = 300
	defaultIdempotencySuccessTTL  = 168

	// Callback rate limit defaults.
	defaultCallbackPerMinute = 60

	// Fetcher defaults.
	defaultFetcherEnabled           = false
	defaultFetcherURL               = "http://localhost:4006"
	defaultFetcherAllowPrivateIPs   = false
	defaultKeyFetcherHealthTimeout  = 5
	defaultKeyFetcherRequestTimeout = 30
	defaultFetcherDiscoveryInt      = 60
	defaultKeyFetcherSchemaCacheTTL = 300
	defaultFetcherExtractionPoll    = 5
	defaultFetcherExtractionTO      = 600

	// Deduplication defaults.
	defaultDedupeTTLSec = 3600

	// Object storage defaults.
	defaultObjStorageEndpoint  = "http://localhost:8333"
	defaultObjStorageRegion    = "us-east-1"
	defaultObjStorageBucket    = "matcher-exports"
	defaultObjStoragePathStyle = true

	// Export worker defaults.
	defaultExportEnabled    = true
	defaultExportPollInt    = 5
	defaultExportPageSize   = 1000
	defaultExportPresignExp = 3600

	// Webhook defaults.
	defaultWebhookTimeout = 30

	// Cleanup worker defaults.
	defaultCleanupEnabled     = true
	defaultCleanupInterval    = 3600
	defaultCleanupBatchSize   = 100
	defaultCleanupGracePeriod = 3600

	// Scheduler defaults.
	defaultSchedulerInterval = 60

	// Archival defaults.
	defaultArchivalEnabled       = false
	defaultArchivalInterval      = 24
	defaultArchivalHotDays       = 90
	defaultArchivalWarmMonths    = 24
	defaultArchivalColdMonths    = 84
	defaultArchivalBatchSize     = 5000
	defaultArchivalStoragePrefix = "archives/audit-logs"
	defaultArchivalStorageClass  = "GLACIER"
	defaultArchivalPartitionLA   = 3
	defaultArchivalPresignExpiry = 3600
)

// RegisterMatcherKeys registers all Matcher configuration keys in the
// systemplane registry. Each key corresponds to a field in the Config struct
// and its sub-structs, using dotted mapstructure tag paths as key names.
func RegisterMatcherKeys(reg registry.Registry) error {
	for _, def := range matcherKeyDefs() {
		if err := reg.Register(def); err != nil {
			return fmt.Errorf("register matcher key %q: %w", def.Key, err)
		}
	}

	return nil
}

// matcherKeyDefs returns all Matcher configuration key definitions. The order
// follows the Config struct field order for auditability. Every field with a
// mapstructure tag gets a corresponding KeyDef entry.
//
//nolint:funlen // Key registration is inherently large; splitting would hurt auditability.
func matcherKeyDefs() []domain.KeyDef {
	return []domain.KeyDef{
		// --- App. ---
		{
			Key:              "app.env_name",
			Kind:             domain.KindConfig,
			AllowedScopes:    []domain.Scope{domain.ScopeGlobal},
			DefaultValue:     defaultEnvName,
			ValueType:        domain.ValueTypeString,
			ApplyBehavior:    domain.ApplyBootstrapOnly,
			MutableAtRuntime: false,
			Description:      "Application environment name (e.g., development, staging, production)",
			Group:            "app",
			RedactPolicy:     domain.RedactNone,
		},
		{
			Key:              "app.log_level",
			Kind:             domain.KindConfig,
			AllowedScopes:    []domain.Scope{domain.ScopeGlobal},
			DefaultValue:     defaultLogLevel,
			ValueType:        domain.ValueTypeString,
			Validator:        validateLogLevel,
			ApplyBehavior:    domain.ApplyBundleRebuild,
			MutableAtRuntime: true,
			Description:      "Application log level (debug, info, warn, error)",
			Group:            "app",
			RedactPolicy:     domain.RedactNone,
		},

		// --- Server. ---
		{
			Key:              "server.address",
			Kind:             domain.KindConfig,
			AllowedScopes:    []domain.Scope{domain.ScopeGlobal},
			DefaultValue:     defaultServerAddress,
			ValueType:        domain.ValueTypeString,
			ApplyBehavior:    domain.ApplyBootstrapOnly,
			MutableAtRuntime: false,
			Description:      "HTTP server listen address (e.g., :4018)",
			Group:            "server",
			RedactPolicy:     domain.RedactNone,
		},
		{
			Key:              "server.body_limit_bytes",
			Kind:             domain.KindConfig,
			AllowedScopes:    []domain.Scope{domain.ScopeGlobal},
			DefaultValue:     defaultKeyBodyLimitBytes,
			ValueType:        domain.ValueTypeInt,
			Validator:        validatePositiveInt,
			ApplyBehavior:    domain.ApplyBundleRebuild,
			MutableAtRuntime: true,
			Description:      "Maximum HTTP request body size in bytes",
			Group:            "server",
			RedactPolicy:     domain.RedactNone,
		},
		{
			Key:              "server.cors_allowed_origins",
			Kind:             domain.KindConfig,
			AllowedScopes:    []domain.Scope{domain.ScopeGlobal},
			DefaultValue:     defaultCORSAllowedOrigins,
			ValueType:        domain.ValueTypeString,
			ApplyBehavior:    domain.ApplyBundleRebuild,
			MutableAtRuntime: true,
			Description:      "Comma-separated list of allowed CORS origins",
			Group:            "server",
			RedactPolicy:     domain.RedactNone,
		},
		{
			Key:              "server.cors_allowed_methods",
			Kind:             domain.KindConfig,
			AllowedScopes:    []domain.Scope{domain.ScopeGlobal},
			DefaultValue:     defaultCORSAllowedMethods,
			ValueType:        domain.ValueTypeString,
			ApplyBehavior:    domain.ApplyBundleRebuild,
			MutableAtRuntime: true,
			Description:      "Comma-separated list of allowed CORS methods",
			Group:            "server",
			RedactPolicy:     domain.RedactNone,
		},
		{
			Key:              "server.cors_allowed_headers",
			Kind:             domain.KindConfig,
			AllowedScopes:    []domain.Scope{domain.ScopeGlobal},
			DefaultValue:     defaultCORSAllowedHeaders,
			ValueType:        domain.ValueTypeString,
			ApplyBehavior:    domain.ApplyBundleRebuild,
			MutableAtRuntime: true,
			Description:      "Comma-separated list of allowed CORS headers",
			Group:            "server",
			RedactPolicy:     domain.RedactNone,
		},
		{
			Key:              "server.tls_cert_file",
			Kind:             domain.KindConfig,
			AllowedScopes:    []domain.Scope{domain.ScopeGlobal},
			DefaultValue:     defaultServerTLSCertFile,
			ValueType:        domain.ValueTypeString,
			ApplyBehavior:    domain.ApplyBootstrapOnly,
			MutableAtRuntime: false,
			Description:      "Path to TLS certificate file",
			Group:            "server",
			RedactPolicy:     domain.RedactNone,
		},
		{
			Key:              "server.tls_key_file",
			Kind:             domain.KindConfig,
			AllowedScopes:    []domain.Scope{domain.ScopeGlobal},
			DefaultValue:     defaultServerTLSKeyFile,
			ValueType:        domain.ValueTypeString,
			ApplyBehavior:    domain.ApplyBootstrapOnly,
			MutableAtRuntime: false,
			Description:      "Path to TLS private key file",
			Group:            "server",
			RedactPolicy:     domain.RedactNone,
		},
		{
			Key:              "server.tls_terminated_upstream",
			Kind:             domain.KindConfig,
			AllowedScopes:    []domain.Scope{domain.ScopeGlobal},
			DefaultValue:     defaultTLSTerminatedUpstream,
			ValueType:        domain.ValueTypeBool,
			ApplyBehavior:    domain.ApplyBootstrapOnly,
			MutableAtRuntime: false,
			Description:      "Whether TLS is terminated by an upstream proxy",
			Group:            "server",
			RedactPolicy:     domain.RedactNone,
		},
		{
			Key:              "server.trusted_proxies",
			Kind:             domain.KindConfig,
			AllowedScopes:    []domain.Scope{domain.ScopeGlobal},
			DefaultValue:     defaultServerTrustedProxies,
			ValueType:        domain.ValueTypeString,
			ApplyBehavior:    domain.ApplyBootstrapOnly,
			MutableAtRuntime: false,
			Description:      "Comma-separated list of trusted proxy CIDRs",
			Group:            "server",
			RedactPolicy:     domain.RedactNone,
		},

		// --- Tenancy. ---
		{
			Key:              "tenancy.default_tenant_id",
			Kind:             domain.KindConfig,
			AllowedScopes:    []domain.Scope{domain.ScopeGlobal},
			DefaultValue:     defaultTenantID,
			ValueType:        domain.ValueTypeString,
			ApplyBehavior:    domain.ApplyBundleRebuild,
			MutableAtRuntime: true,
			Description:      "Default tenant UUID for single-tenant mode",
			Group:            "tenancy",
			RedactPolicy:     domain.RedactNone,
		},
		{
			Key:              "tenancy.default_tenant_slug",
			Kind:             domain.KindConfig,
			AllowedScopes:    []domain.Scope{domain.ScopeGlobal},
			DefaultValue:     defaultTenantSlug,
			ValueType:        domain.ValueTypeString,
			ApplyBehavior:    domain.ApplyBundleRebuild,
			MutableAtRuntime: true,
			Description:      "Default tenant slug for single-tenant mode",
			Group:            "tenancy",
			RedactPolicy:     domain.RedactNone,
		},
		{
			Key:              "tenancy.multi_tenant_enabled",
			Kind:             domain.KindConfig,
			AllowedScopes:    []domain.Scope{domain.ScopeGlobal},
			DefaultValue:     defaultMultiTenantEnabled,
			ValueType:        domain.ValueTypeBool,
			ApplyBehavior:    domain.ApplyBundleRebuild,
			MutableAtRuntime: true,
			Description:      "Enable multi-tenant infrastructure (tenant manager, per-tenant pools)",
			Group:            "tenancy",
			RedactPolicy:     domain.RedactNone,
		},
		{
			Key:              "tenancy.multi_tenant_url",
			Kind:             domain.KindConfig,
			AllowedScopes:    []domain.Scope{domain.ScopeGlobal},
			DefaultValue:     "",
			ValueType:        domain.ValueTypeString,
			ApplyBehavior:    domain.ApplyBundleRebuild,
			MutableAtRuntime: true,
			Description:      "Tenant management service URL",
			Group:            "tenancy",
			RedactPolicy:     domain.RedactNone,
		},
		{
			Key:              "tenancy.multi_tenant_environment",
			Kind:             domain.KindConfig,
			AllowedScopes:    []domain.Scope{domain.ScopeGlobal},
			DefaultValue:     "",
			ValueType:        domain.ValueTypeString,
			ApplyBehavior:    domain.ApplyBundleRebuild,
			MutableAtRuntime: true,
			Description:      "Tenant management environment identifier",
			Group:            "tenancy",
			RedactPolicy:     domain.RedactNone,
		},
		{
			Key:              "tenancy.multi_tenant_max_tenant_pools",
			Kind:             domain.KindConfig,
			AllowedScopes:    []domain.Scope{domain.ScopeGlobal},
			DefaultValue:     defaultMultiTenantMaxTenantPools,
			ValueType:        domain.ValueTypeInt,
			Validator:        validatePositiveInt,
			ApplyBehavior:    domain.ApplyBundleRebuild,
			MutableAtRuntime: true,
			Description:      "Maximum number of concurrent per-tenant connection pools",
			Group:            "tenancy",
			RedactPolicy:     domain.RedactNone,
		},
		{
			Key:              "tenancy.multi_tenant_idle_timeout_sec",
			Kind:             domain.KindConfig,
			AllowedScopes:    []domain.Scope{domain.ScopeGlobal},
			DefaultValue:     defaultMultiTenantIdleTimeoutSec,
			ValueType:        domain.ValueTypeInt,
			Validator:        validatePositiveInt,
			ApplyBehavior:    domain.ApplyBundleRebuild,
			MutableAtRuntime: true,
			Description:      "Idle timeout (seconds) before evicting a tenant connection pool",
			Group:            "tenancy",
			RedactPolicy:     domain.RedactNone,
		},
		{
			Key:              "tenancy.multi_tenant_circuit_breaker_threshold",
			Kind:             domain.KindConfig,
			AllowedScopes:    []domain.Scope{domain.ScopeGlobal},
			DefaultValue:     defaultMultiTenantCircuitBreakerThresh,
			ValueType:        domain.ValueTypeInt,
			Validator:        validatePositiveInt,
			ApplyBehavior:    domain.ApplyBundleRebuild,
			MutableAtRuntime: true,
			Description:      "Failure threshold before circuit breaker opens for a tenant pool",
			Group:            "tenancy",
			RedactPolicy:     domain.RedactNone,
		},
		{
			Key:              "tenancy.multi_tenant_circuit_breaker_timeout_sec",
			Kind:             domain.KindConfig,
			AllowedScopes:    []domain.Scope{domain.ScopeGlobal},
			DefaultValue:     defaultMultiTenantCircuitBreakerSec,
			ValueType:        domain.ValueTypeInt,
			Validator:        validatePositiveInt,
			ApplyBehavior:    domain.ApplyBundleRebuild,
			MutableAtRuntime: true,
			Description:      "Circuit breaker open duration (seconds) before half-open retry",
			Group:            "tenancy",
			RedactPolicy:     domain.RedactNone,
		},
		{
			Key:              "tenancy.multi_tenant_service_api_key",
			Kind:             domain.KindConfig,
			AllowedScopes:    []domain.Scope{domain.ScopeGlobal},
			DefaultValue:     "",
			ValueType:        domain.ValueTypeString,
			ApplyBehavior:    domain.ApplyBundleRebuild,
			MutableAtRuntime: true,
			Secret:           true,
			Description:      "API key for authenticating with the tenant management service",
			Group:            "tenancy",
			RedactPolicy:     domain.RedactFull,
		},
		{
			Key:              "tenancy.multi_tenant_infra_enabled",
			Kind:             domain.KindConfig,
			AllowedScopes:    []domain.Scope{domain.ScopeGlobal},
			DefaultValue:     defaultMultiTenantInfraEnabled,
			ValueType:        domain.ValueTypeBool,
			ApplyBehavior:    domain.ApplyBundleRebuild,
			MutableAtRuntime: true,
			Description:      "Deprecated: backward-compatible alias for multi_tenant_enabled",
			Group:            "tenancy",
			RedactPolicy:     domain.RedactNone,
		},

		// --- PostgreSQL. ---
		{
			Key:              "postgres.primary_host",
			Kind:             domain.KindConfig,
			AllowedScopes:    []domain.Scope{domain.ScopeGlobal},
			DefaultValue:     defaultPGHost,
			ValueType:        domain.ValueTypeString,
			Validator:        validateNonEmptyString,
			ApplyBehavior:    domain.ApplyBundleRebuild,
			MutableAtRuntime: true,
			Description:      "PostgreSQL primary host address",
			Group:            "postgres",
			RedactPolicy:     domain.RedactNone,
		},
		{
			Key:              "postgres.primary_port",
			Kind:             domain.KindConfig,
			AllowedScopes:    []domain.Scope{domain.ScopeGlobal},
			DefaultValue:     defaultPGPort,
			ValueType:        domain.ValueTypeString,
			ApplyBehavior:    domain.ApplyBundleRebuild,
			MutableAtRuntime: true,
			Description:      "PostgreSQL primary port",
			Group:            "postgres",
			RedactPolicy:     domain.RedactNone,
		},
		{
			Key:              "postgres.primary_user",
			Kind:             domain.KindConfig,
			AllowedScopes:    []domain.Scope{domain.ScopeGlobal},
			DefaultValue:     defaultPGUser,
			ValueType:        domain.ValueTypeString,
			Validator:        validateNonEmptyString,
			ApplyBehavior:    domain.ApplyBundleRebuild,
			MutableAtRuntime: true,
			Description:      "PostgreSQL primary connection user",
			Group:            "postgres",
			RedactPolicy:     domain.RedactNone,
		},
		{
			Key:              "postgres.primary_password",
			Kind:             domain.KindConfig,
			AllowedScopes:    []domain.Scope{domain.ScopeGlobal},
			DefaultValue:     defaultPGPassword,
			ValueType:        domain.ValueTypeString,
			ApplyBehavior:    domain.ApplyBundleRebuild,
			MutableAtRuntime: true,
			Secret:           true,
			Description:      "PostgreSQL primary connection password",
			Group:            "postgres",
			RedactPolicy:     domain.RedactFull,
		},
		{
			Key:              "postgres.primary_db",
			Kind:             domain.KindConfig,
			AllowedScopes:    []domain.Scope{domain.ScopeGlobal},
			DefaultValue:     defaultPGDB,
			ValueType:        domain.ValueTypeString,
			Validator:        validateNonEmptyString,
			ApplyBehavior:    domain.ApplyBundleRebuild,
			MutableAtRuntime: true,
			Description:      "PostgreSQL primary database name",
			Group:            "postgres",
			RedactPolicy:     domain.RedactNone,
		},
		{
			Key:              "postgres.primary_ssl_mode",
			Kind:             domain.KindConfig,
			AllowedScopes:    []domain.Scope{domain.ScopeGlobal},
			DefaultValue:     defaultPGSSLMode,
			ValueType:        domain.ValueTypeString,
			Validator:        validateSSLMode,
			ApplyBehavior:    domain.ApplyBundleRebuild,
			MutableAtRuntime: true,
			Description:      "PostgreSQL primary SSL mode (disable, require, verify-ca, verify-full)",
			Group:            "postgres",
			RedactPolicy:     domain.RedactNone,
		},
		{
			Key:              "postgres.replica_host",
			Kind:             domain.KindConfig,
			AllowedScopes:    []domain.Scope{domain.ScopeGlobal},
			DefaultValue:     "",
			ValueType:        domain.ValueTypeString,
			ApplyBehavior:    domain.ApplyBundleRebuild,
			MutableAtRuntime: true,
			Description:      "PostgreSQL replica host address",
			Group:            "postgres",
			RedactPolicy:     domain.RedactNone,
		},
		{
			Key:              "postgres.replica_port",
			Kind:             domain.KindConfig,
			AllowedScopes:    []domain.Scope{domain.ScopeGlobal},
			DefaultValue:     "",
			ValueType:        domain.ValueTypeString,
			ApplyBehavior:    domain.ApplyBundleRebuild,
			MutableAtRuntime: true,
			Description:      "PostgreSQL replica port",
			Group:            "postgres",
			RedactPolicy:     domain.RedactNone,
		},
		{
			Key:              "postgres.replica_user",
			Kind:             domain.KindConfig,
			AllowedScopes:    []domain.Scope{domain.ScopeGlobal},
			DefaultValue:     "",
			ValueType:        domain.ValueTypeString,
			ApplyBehavior:    domain.ApplyBundleRebuild,
			MutableAtRuntime: true,
			Description:      "PostgreSQL replica connection user",
			Group:            "postgres",
			RedactPolicy:     domain.RedactNone,
		},
		{
			Key:              "postgres.replica_password",
			Kind:             domain.KindConfig,
			AllowedScopes:    []domain.Scope{domain.ScopeGlobal},
			DefaultValue:     "",
			ValueType:        domain.ValueTypeString,
			ApplyBehavior:    domain.ApplyBundleRebuild,
			MutableAtRuntime: true,
			Secret:           true,
			Description:      "PostgreSQL replica connection password",
			Group:            "postgres",
			RedactPolicy:     domain.RedactFull,
		},
		{
			Key:              "postgres.replica_db",
			Kind:             domain.KindConfig,
			AllowedScopes:    []domain.Scope{domain.ScopeGlobal},
			DefaultValue:     "",
			ValueType:        domain.ValueTypeString,
			ApplyBehavior:    domain.ApplyBundleRebuild,
			MutableAtRuntime: true,
			Description:      "PostgreSQL replica database name",
			Group:            "postgres",
			RedactPolicy:     domain.RedactNone,
		},
		{
			Key:              "postgres.replica_ssl_mode",
			Kind:             domain.KindConfig,
			AllowedScopes:    []domain.Scope{domain.ScopeGlobal},
			DefaultValue:     "",
			ValueType:        domain.ValueTypeString,
			Validator:        validateOptionalSSLMode,
			ApplyBehavior:    domain.ApplyBundleRebuild,
			MutableAtRuntime: true,
			Description:      "PostgreSQL replica SSL mode (disable, require, verify-ca, verify-full)",
			Group:            "postgres",
			RedactPolicy:     domain.RedactNone,
		},
		{
			Key:              "postgres.max_open_connections",
			Kind:             domain.KindConfig,
			AllowedScopes:    []domain.Scope{domain.ScopeGlobal},
			DefaultValue:     defaultPGMaxOpenConns,
			ValueType:        domain.ValueTypeInt,
			Validator:        validatePositiveInt,
			ApplyBehavior:    domain.ApplyBundleRebuild,
			MutableAtRuntime: true,
			Description:      "Maximum number of open PostgreSQL connections",
			Group:            "postgres",
			RedactPolicy:     domain.RedactNone,
		},
		{
			Key:              "postgres.max_idle_connections",
			Kind:             domain.KindConfig,
			AllowedScopes:    []domain.Scope{domain.ScopeGlobal},
			DefaultValue:     defaultPGMaxIdleConns,
			ValueType:        domain.ValueTypeInt,
			Validator:        validatePositiveInt,
			ApplyBehavior:    domain.ApplyBundleRebuild,
			MutableAtRuntime: true,
			Description:      "Maximum number of idle PostgreSQL connections",
			Group:            "postgres",
			RedactPolicy:     domain.RedactNone,
		},
		{
			Key:              "postgres.conn_max_lifetime_mins",
			Kind:             domain.KindConfig,
			AllowedScopes:    []domain.Scope{domain.ScopeGlobal},
			DefaultValue:     defaultPGConnMaxLifeMins,
			ValueType:        domain.ValueTypeInt,
			Validator:        validatePositiveInt,
			ApplyBehavior:    domain.ApplyBundleRebuild,
			MutableAtRuntime: true,
			Description:      "Maximum connection lifetime in minutes",
			Group:            "postgres",
			RedactPolicy:     domain.RedactNone,
		},
		{
			Key:              "postgres.conn_max_idle_time_mins",
			Kind:             domain.KindConfig,
			AllowedScopes:    []domain.Scope{domain.ScopeGlobal},
			DefaultValue:     defaultPGConnMaxIdleMins,
			ValueType:        domain.ValueTypeInt,
			Validator:        validatePositiveInt,
			ApplyBehavior:    domain.ApplyBundleRebuild,
			MutableAtRuntime: true,
			Description:      "Maximum idle connection time in minutes",
			Group:            "postgres",
			RedactPolicy:     domain.RedactNone,
		},
		{
			Key:              "postgres.connect_timeout_sec",
			Kind:             domain.KindConfig,
			AllowedScopes:    []domain.Scope{domain.ScopeGlobal},
			DefaultValue:     defaultPGConnectTimeout,
			ValueType:        domain.ValueTypeInt,
			Validator:        validatePositiveInt,
			ApplyBehavior:    domain.ApplyBundleRebuild,
			MutableAtRuntime: true,
			Description:      "PostgreSQL connection timeout in seconds",
			Group:            "postgres",
			RedactPolicy:     domain.RedactNone,
		},
		{
			Key:              "postgres.query_timeout_sec",
			Kind:             domain.KindConfig,
			AllowedScopes:    []domain.Scope{domain.ScopeGlobal},
			DefaultValue:     defaultPGQueryTimeout,
			ValueType:        domain.ValueTypeInt,
			Validator:        validatePositiveInt,
			ApplyBehavior:    domain.ApplyBundleRebuild,
			MutableAtRuntime: true,
			Description:      "PostgreSQL query timeout in seconds",
			Group:            "postgres",
			RedactPolicy:     domain.RedactNone,
		},
		{
			Key:              "postgres.migrations_path",
			Kind:             domain.KindConfig,
			AllowedScopes:    []domain.Scope{domain.ScopeGlobal},
			DefaultValue:     defaultPGMigrationsPath,
			ValueType:        domain.ValueTypeString,
			ApplyBehavior:    domain.ApplyBundleRebuild,
			MutableAtRuntime: true,
			Description:      "Path to database migration files",
			Group:            "postgres",
			RedactPolicy:     domain.RedactNone,
		},

		// --- Redis. ---
		{
			Key:              "redis.host",
			Kind:             domain.KindConfig,
			AllowedScopes:    []domain.Scope{domain.ScopeGlobal},
			DefaultValue:     defaultRedisHost,
			ValueType:        domain.ValueTypeString,
			ApplyBehavior:    domain.ApplyBundleRebuild,
			MutableAtRuntime: true,
			Description:      "Redis host address(es), comma-separated for sentinel mode",
			Group:            "redis",
			RedactPolicy:     domain.RedactNone,
		},
		{
			Key:              "redis.master_name",
			Kind:             domain.KindConfig,
			AllowedScopes:    []domain.Scope{domain.ScopeGlobal},
			DefaultValue:     "",
			ValueType:        domain.ValueTypeString,
			ApplyBehavior:    domain.ApplyBundleRebuild,
			MutableAtRuntime: true,
			Description:      "Redis sentinel master name (enables sentinel mode when set)",
			Group:            "redis",
			RedactPolicy:     domain.RedactNone,
		},
		{
			Key:              "redis.password",
			Kind:             domain.KindConfig,
			AllowedScopes:    []domain.Scope{domain.ScopeGlobal},
			DefaultValue:     "",
			ValueType:        domain.ValueTypeString,
			ApplyBehavior:    domain.ApplyBundleRebuild,
			MutableAtRuntime: true,
			Secret:           true,
			Description:      "Redis connection password",
			Group:            "redis",
			RedactPolicy:     domain.RedactFull,
		},
		{
			Key:              "redis.db",
			Kind:             domain.KindConfig,
			AllowedScopes:    []domain.Scope{domain.ScopeGlobal},
			DefaultValue:     defaultRedisDB,
			ValueType:        domain.ValueTypeInt,
			Validator:        validateNonNegativeInt,
			ApplyBehavior:    domain.ApplyBundleRebuild,
			MutableAtRuntime: true,
			Description:      "Redis database number",
			Group:            "redis",
			RedactPolicy:     domain.RedactNone,
		},
		{
			Key:              "redis.protocol",
			Kind:             domain.KindConfig,
			AllowedScopes:    []domain.Scope{domain.ScopeGlobal},
			DefaultValue:     defaultRedisProtocol,
			ValueType:        domain.ValueTypeInt,
			ApplyBehavior:    domain.ApplyBundleRebuild,
			MutableAtRuntime: true,
			Description:      "Redis protocol version (2 or 3)",
			Group:            "redis",
			RedactPolicy:     domain.RedactNone,
		},
		{
			Key:              "redis.tls",
			Kind:             domain.KindConfig,
			AllowedScopes:    []domain.Scope{domain.ScopeGlobal},
			DefaultValue:     defaultRedisTLS,
			ValueType:        domain.ValueTypeBool,
			ApplyBehavior:    domain.ApplyBundleRebuild,
			MutableAtRuntime: true,
			Description:      "Enable TLS for Redis connections",
			Group:            "redis",
			RedactPolicy:     domain.RedactNone,
		},
		{
			Key:              "redis.ca_cert",
			Kind:             domain.KindConfig,
			AllowedScopes:    []domain.Scope{domain.ScopeGlobal},
			DefaultValue:     "",
			ValueType:        domain.ValueTypeString,
			ApplyBehavior:    domain.ApplyBundleRebuild,
			MutableAtRuntime: true,
			Secret:           true,
			Description:      "Redis TLS CA certificate content",
			Group:            "redis",
			RedactPolicy:     domain.RedactFull,
		},
		{
			Key:              "redis.pool_size",
			Kind:             domain.KindConfig,
			AllowedScopes:    []domain.Scope{domain.ScopeGlobal},
			DefaultValue:     defaultRedisPoolSize,
			ValueType:        domain.ValueTypeInt,
			Validator:        validatePositiveInt,
			ApplyBehavior:    domain.ApplyBundleRebuild,
			MutableAtRuntime: true,
			Description:      "Redis connection pool size",
			Group:            "redis",
			RedactPolicy:     domain.RedactNone,
		},
		{
			Key:              "redis.min_idle_conn",
			Kind:             domain.KindConfig,
			AllowedScopes:    []domain.Scope{domain.ScopeGlobal},
			DefaultValue:     defaultRedisMinIdleConn,
			ValueType:        domain.ValueTypeInt,
			Validator:        validateNonNegativeInt,
			ApplyBehavior:    domain.ApplyBundleRebuild,
			MutableAtRuntime: true,
			Description:      "Minimum number of idle Redis connections",
			Group:            "redis",
			RedactPolicy:     domain.RedactNone,
		},
		{
			Key:              "redis.read_timeout_ms",
			Kind:             domain.KindConfig,
			AllowedScopes:    []domain.Scope{domain.ScopeGlobal},
			DefaultValue:     defaultRedisReadTimeout,
			ValueType:        domain.ValueTypeInt,
			Validator:        validatePositiveInt,
			ApplyBehavior:    domain.ApplyBundleRebuild,
			MutableAtRuntime: true,
			Description:      "Redis read timeout in milliseconds",
			Group:            "redis",
			RedactPolicy:     domain.RedactNone,
		},
		{
			Key:              "redis.write_timeout_ms",
			Kind:             domain.KindConfig,
			AllowedScopes:    []domain.Scope{domain.ScopeGlobal},
			DefaultValue:     defaultRedisWriteTimeout,
			ValueType:        domain.ValueTypeInt,
			Validator:        validatePositiveInt,
			ApplyBehavior:    domain.ApplyBundleRebuild,
			MutableAtRuntime: true,
			Description:      "Redis write timeout in milliseconds",
			Group:            "redis",
			RedactPolicy:     domain.RedactNone,
		},
		{
			Key:              "redis.dial_timeout_ms",
			Kind:             domain.KindConfig,
			AllowedScopes:    []domain.Scope{domain.ScopeGlobal},
			DefaultValue:     defaultRedisDialTimeout,
			ValueType:        domain.ValueTypeInt,
			Validator:        validatePositiveInt,
			ApplyBehavior:    domain.ApplyBundleRebuild,
			MutableAtRuntime: true,
			Description:      "Redis dial timeout in milliseconds",
			Group:            "redis",
			RedactPolicy:     domain.RedactNone,
		},

		// --- RabbitMQ. ---
		{
			Key:              "rabbitmq.uri",
			Kind:             domain.KindConfig,
			AllowedScopes:    []domain.Scope{domain.ScopeGlobal},
			DefaultValue:     defaultRabbitURI,
			ValueType:        domain.ValueTypeString,
			ApplyBehavior:    domain.ApplyBundleRebuild,
			MutableAtRuntime: true,
			Description:      "RabbitMQ connection URI scheme",
			Group:            "rabbitmq",
			RedactPolicy:     domain.RedactNone,
		},
		{
			Key:              "rabbitmq.host",
			Kind:             domain.KindConfig,
			AllowedScopes:    []domain.Scope{domain.ScopeGlobal},
			DefaultValue:     defaultRabbitHost,
			ValueType:        domain.ValueTypeString,
			ApplyBehavior:    domain.ApplyBundleRebuild,
			MutableAtRuntime: true,
			Description:      "RabbitMQ host address",
			Group:            "rabbitmq",
			RedactPolicy:     domain.RedactNone,
		},
		{
			Key:              "rabbitmq.port",
			Kind:             domain.KindConfig,
			AllowedScopes:    []domain.Scope{domain.ScopeGlobal},
			DefaultValue:     defaultRabbitPort,
			ValueType:        domain.ValueTypeString,
			ApplyBehavior:    domain.ApplyBundleRebuild,
			MutableAtRuntime: true,
			Description:      "RabbitMQ port number",
			Group:            "rabbitmq",
			RedactPolicy:     domain.RedactNone,
		},
		{
			Key:              "rabbitmq.user",
			Kind:             domain.KindConfig,
			AllowedScopes:    []domain.Scope{domain.ScopeGlobal},
			DefaultValue:     defaultRabbitUser,
			ValueType:        domain.ValueTypeString,
			ApplyBehavior:    domain.ApplyBundleRebuild,
			MutableAtRuntime: true,
			Description:      "RabbitMQ connection user",
			Group:            "rabbitmq",
			RedactPolicy:     domain.RedactNone,
		},
		{
			Key:              "rabbitmq.password",
			Kind:             domain.KindConfig,
			AllowedScopes:    []domain.Scope{domain.ScopeGlobal},
			DefaultValue:     defaultRabbitPassword,
			ValueType:        domain.ValueTypeString,
			ApplyBehavior:    domain.ApplyBundleRebuild,
			MutableAtRuntime: true,
			Secret:           true,
			Description:      "RabbitMQ connection password",
			Group:            "rabbitmq",
			RedactPolicy:     domain.RedactFull,
		},
		{
			Key:              "rabbitmq.vhost",
			Kind:             domain.KindConfig,
			AllowedScopes:    []domain.Scope{domain.ScopeGlobal},
			DefaultValue:     defaultRabbitVHost,
			ValueType:        domain.ValueTypeString,
			ApplyBehavior:    domain.ApplyBundleRebuild,
			MutableAtRuntime: true,
			Description:      "RabbitMQ virtual host",
			Group:            "rabbitmq",
			RedactPolicy:     domain.RedactNone,
		},
		{
			Key:              "rabbitmq.health_url",
			Kind:             domain.KindConfig,
			AllowedScopes:    []domain.Scope{domain.ScopeGlobal},
			DefaultValue:     defaultRabbitHealthURL,
			ValueType:        domain.ValueTypeString,
			ApplyBehavior:    domain.ApplyBundleRebuild,
			MutableAtRuntime: true,
			Description:      "RabbitMQ management API health endpoint URL",
			Group:            "rabbitmq",
			RedactPolicy:     domain.RedactNone,
		},
		{
			Key:              "rabbitmq.allow_insecure_health_check",
			Kind:             domain.KindConfig,
			AllowedScopes:    []domain.Scope{domain.ScopeGlobal},
			DefaultValue:     defaultRabbitAllowInsecureHealth,
			ValueType:        domain.ValueTypeBool,
			ApplyBehavior:    domain.ApplyBundleRebuild,
			MutableAtRuntime: true,
			Description:      "Allow insecure TLS for RabbitMQ health checks",
			Group:            "rabbitmq",
			RedactPolicy:     domain.RedactNone,
		},

		// --- Auth. ---
		{
			Key:              "auth.enabled",
			Kind:             domain.KindConfig,
			AllowedScopes:    []domain.Scope{domain.ScopeGlobal},
			DefaultValue:     defaultAuthEnabled,
			ValueType:        domain.ValueTypeBool,
			ApplyBehavior:    domain.ApplyBootstrapOnly,
			MutableAtRuntime: false,
			Description:      "Enable authentication and authorization middleware",
			Group:            "auth",
			RedactPolicy:     domain.RedactNone,
		},
		{
			Key:              "auth.host",
			Kind:             domain.KindConfig,
			AllowedScopes:    []domain.Scope{domain.ScopeGlobal},
			DefaultValue:     "",
			ValueType:        domain.ValueTypeString,
			ApplyBehavior:    domain.ApplyBootstrapOnly,
			MutableAtRuntime: false,
			Description:      "Authentication service address",
			Group:            "auth",
			RedactPolicy:     domain.RedactNone,
		},
		{
			Key:              "auth.token_secret",
			Kind:             domain.KindConfig,
			AllowedScopes:    []domain.Scope{domain.ScopeGlobal},
			DefaultValue:     "",
			ValueType:        domain.ValueTypeString,
			ApplyBehavior:    domain.ApplyBootstrapOnly,
			MutableAtRuntime: false,
			Secret:           true,
			Description:      "JWT token signing secret",
			Group:            "auth",
			RedactPolicy:     domain.RedactFull,
		},

		// --- Swagger. ---
		{
			Key:              "swagger.enabled",
			Kind:             domain.KindConfig,
			AllowedScopes:    []domain.Scope{domain.ScopeGlobal},
			DefaultValue:     defaultSwaggerEnabled,
			ValueType:        domain.ValueTypeBool,
			ApplyBehavior:    domain.ApplyBundleRebuild,
			MutableAtRuntime: true,
			Description:      "Enable Swagger UI endpoint",
			Group:            "swagger",
			RedactPolicy:     domain.RedactNone,
		},
		{
			Key:              "swagger.host",
			Kind:             domain.KindConfig,
			AllowedScopes:    []domain.Scope{domain.ScopeGlobal},
			DefaultValue:     "",
			ValueType:        domain.ValueTypeString,
			ApplyBehavior:    domain.ApplyBundleRebuild,
			MutableAtRuntime: true,
			Description:      "Override the Swagger spec host field (empty uses request host)",
			Group:            "swagger",
			RedactPolicy:     domain.RedactNone,
		},
		{
			Key:              "swagger.schemes",
			Kind:             domain.KindConfig,
			AllowedScopes:    []domain.Scope{domain.ScopeGlobal},
			DefaultValue:     defaultSwaggerSchemes,
			ValueType:        domain.ValueTypeString,
			ApplyBehavior:    domain.ApplyBundleRebuild,
			MutableAtRuntime: true,
			Description:      "Comma-separated list of Swagger spec schemes (e.g., https or http,https)",
			Group:            "swagger",
			RedactPolicy:     domain.RedactNone,
		},

		// --- Telemetry. ---
		{
			Key:              "telemetry.enabled",
			Kind:             domain.KindConfig,
			AllowedScopes:    []domain.Scope{domain.ScopeGlobal},
			DefaultValue:     defaultTelemetryEnabled,
			ValueType:        domain.ValueTypeBool,
			ApplyBehavior:    domain.ApplyBootstrapOnly,
			MutableAtRuntime: false,
			Description:      "Enable OpenTelemetry telemetry exporter",
			Group:            "telemetry",
			RedactPolicy:     domain.RedactNone,
		},
		{
			Key:              "telemetry.service_name",
			Kind:             domain.KindConfig,
			AllowedScopes:    []domain.Scope{domain.ScopeGlobal},
			DefaultValue:     defaultTelemetryServiceName,
			ValueType:        domain.ValueTypeString,
			ApplyBehavior:    domain.ApplyBootstrapOnly,
			MutableAtRuntime: false,
			Description:      "OpenTelemetry resource service name",
			Group:            "telemetry",
			RedactPolicy:     domain.RedactNone,
		},
		{
			Key:              "telemetry.library_name",
			Kind:             domain.KindConfig,
			AllowedScopes:    []domain.Scope{domain.ScopeGlobal},
			DefaultValue:     defaultTelemetryLibraryName,
			ValueType:        domain.ValueTypeString,
			ApplyBehavior:    domain.ApplyBootstrapOnly,
			MutableAtRuntime: false,
			Description:      "OpenTelemetry instrumentation library name",
			Group:            "telemetry",
			RedactPolicy:     domain.RedactNone,
		},
		{
			Key:              "telemetry.service_version",
			Kind:             domain.KindConfig,
			AllowedScopes:    []domain.Scope{domain.ScopeGlobal},
			DefaultValue:     defaultTelemetryServiceVersion,
			ValueType:        domain.ValueTypeString,
			ApplyBehavior:    domain.ApplyBootstrapOnly,
			MutableAtRuntime: false,
			Description:      "OpenTelemetry resource service version",
			Group:            "telemetry",
			RedactPolicy:     domain.RedactNone,
		},
		{
			Key:              "telemetry.deployment_env",
			Kind:             domain.KindConfig,
			AllowedScopes:    []domain.Scope{domain.ScopeGlobal},
			DefaultValue:     defaultTelemetryDeploymentEnv,
			ValueType:        domain.ValueTypeString,
			ApplyBehavior:    domain.ApplyBootstrapOnly,
			MutableAtRuntime: false,
			Description:      "OpenTelemetry resource deployment environment",
			Group:            "telemetry",
			RedactPolicy:     domain.RedactNone,
		},
		{
			Key:              "telemetry.collector_endpoint",
			Kind:             domain.KindConfig,
			AllowedScopes:    []domain.Scope{domain.ScopeGlobal},
			DefaultValue:     defaultTelemetryCollectorEP,
			ValueType:        domain.ValueTypeString,
			ApplyBehavior:    domain.ApplyBootstrapOnly,
			MutableAtRuntime: false,
			Description:      "OpenTelemetry collector OTLP endpoint",
			Group:            "telemetry",
			RedactPolicy:     domain.RedactNone,
		},
		{
			Key:              "telemetry.db_metrics_interval_sec",
			Kind:             domain.KindConfig,
			AllowedScopes:    []domain.Scope{domain.ScopeGlobal},
			DefaultValue:     defaultTelemetryDBMetricsIntSec,
			ValueType:        domain.ValueTypeInt,
			Validator:        validatePositiveInt,
			ApplyBehavior:    domain.ApplyBootstrapOnly,
			MutableAtRuntime: false,
			Description:      "Database metrics collection interval in seconds",
			Group:            "telemetry",
			RedactPolicy:     domain.RedactNone,
		},

		// --- Rate Limit. ---
		{
			Key:              "rate_limit.enabled",
			Kind:             domain.KindConfig,
			AllowedScopes:    []domain.Scope{domain.ScopeGlobal},
			DefaultValue:     defaultRateLimitEnabled,
			ValueType:        domain.ValueTypeBool,
			ApplyBehavior:    domain.ApplyLiveRead,
			MutableAtRuntime: true,
			Description:      "Enable global rate limiting",
			Group:            "rate_limit",
			RedactPolicy:     domain.RedactNone,
		},
		{
			Key:              "rate_limit.max",
			Kind:             domain.KindConfig,
			AllowedScopes:    []domain.Scope{domain.ScopeGlobal},
			DefaultValue:     defaultRateLimitMax,
			ValueType:        domain.ValueTypeInt,
			Validator:        validatePositiveInt,
			ApplyBehavior:    domain.ApplyLiveRead,
			MutableAtRuntime: true,
			Description:      "Maximum requests per rate limit window",
			Group:            "rate_limit",
			RedactPolicy:     domain.RedactNone,
		},
		{
			Key:              "rate_limit.expiry_sec",
			Kind:             domain.KindConfig,
			AllowedScopes:    []domain.Scope{domain.ScopeGlobal},
			DefaultValue:     defaultRateLimitExpirySec,
			ValueType:        domain.ValueTypeInt,
			Validator:        validatePositiveInt,
			ApplyBehavior:    domain.ApplyLiveRead,
			MutableAtRuntime: true,
			Description:      "Rate limit window expiry in seconds",
			Group:            "rate_limit",
			RedactPolicy:     domain.RedactNone,
		},
		{
			Key:              "rate_limit.export_max",
			Kind:             domain.KindConfig,
			AllowedScopes:    []domain.Scope{domain.ScopeGlobal},
			DefaultValue:     defaultRateLimitExportMax,
			ValueType:        domain.ValueTypeInt,
			Validator:        validatePositiveInt,
			ApplyBehavior:    domain.ApplyLiveRead,
			MutableAtRuntime: true,
			Description:      "Maximum export requests per rate limit window",
			Group:            "rate_limit",
			RedactPolicy:     domain.RedactNone,
		},
		{
			Key:              "rate_limit.export_expiry_sec",
			Kind:             domain.KindConfig,
			AllowedScopes:    []domain.Scope{domain.ScopeGlobal},
			DefaultValue:     defaultRateLimitExportExpiry,
			ValueType:        domain.ValueTypeInt,
			Validator:        validatePositiveInt,
			ApplyBehavior:    domain.ApplyLiveRead,
			MutableAtRuntime: true,
			Description:      "Export rate limit window expiry in seconds",
			Group:            "rate_limit",
			RedactPolicy:     domain.RedactNone,
		},
		{
			Key:              "rate_limit.dispatch_max",
			Kind:             domain.KindConfig,
			AllowedScopes:    []domain.Scope{domain.ScopeGlobal},
			DefaultValue:     defaultRateLimitDispatchMax,
			ValueType:        domain.ValueTypeInt,
			Validator:        validatePositiveInt,
			ApplyBehavior:    domain.ApplyLiveRead,
			MutableAtRuntime: true,
			Description:      "Maximum dispatch requests per rate limit window",
			Group:            "rate_limit",
			RedactPolicy:     domain.RedactNone,
		},
		{
			Key:              "rate_limit.dispatch_expiry_sec",
			Kind:             domain.KindConfig,
			AllowedScopes:    []domain.Scope{domain.ScopeGlobal},
			DefaultValue:     defaultRateLimitDispatchExp,
			ValueType:        domain.ValueTypeInt,
			Validator:        validatePositiveInt,
			ApplyBehavior:    domain.ApplyLiveRead,
			MutableAtRuntime: true,
			Description:      "Dispatch rate limit window expiry in seconds",
			Group:            "rate_limit",
			RedactPolicy:     domain.RedactNone,
		},

		// --- Infrastructure. ---
		{
			Key:              "infrastructure.connect_timeout_sec",
			Kind:             domain.KindConfig,
			AllowedScopes:    []domain.Scope{domain.ScopeGlobal},
			DefaultValue:     defaultInfraConnectTimeout,
			ValueType:        domain.ValueTypeInt,
			Validator:        validatePositiveInt,
			ApplyBehavior:    domain.ApplyBundleRebuild,
			MutableAtRuntime: true,
			Description:      "Infrastructure connection timeout in seconds",
			Group:            "infrastructure",
			RedactPolicy:     domain.RedactNone,
		},
		{
			Key:              "infrastructure.health_check_timeout_sec",
			Kind:             domain.KindConfig,
			AllowedScopes:    []domain.Scope{domain.ScopeGlobal},
			DefaultValue:     defaultInfraHealthCheckTimeout,
			ValueType:        domain.ValueTypeInt,
			Validator:        validatePositiveInt,
			ApplyBehavior:    domain.ApplyLiveRead,
			MutableAtRuntime: true,
			Description:      "Health check probe timeout in seconds",
			Group:            "infrastructure",
			RedactPolicy:     domain.RedactNone,
		},

		// --- Idempotency. ---
		{
			Key:              "idempotency.retry_window_sec",
			Kind:             domain.KindConfig,
			AllowedScopes:    []domain.Scope{domain.ScopeGlobal},
			DefaultValue:     defaultIdempotencyRetryWindow,
			ValueType:        domain.ValueTypeInt,
			Validator:        validatePositiveInt,
			ApplyBehavior:    domain.ApplyBundleRebuild,
			MutableAtRuntime: true,
			Description:      "Failed idempotency key retry window in seconds",
			Group:            "idempotency",
			RedactPolicy:     domain.RedactNone,
		},
		{
			Key:              "idempotency.success_ttl_hours",
			Kind:             domain.KindConfig,
			AllowedScopes:    []domain.Scope{domain.ScopeGlobal},
			DefaultValue:     defaultIdempotencySuccessTTL,
			ValueType:        domain.ValueTypeInt,
			Validator:        validatePositiveInt,
			ApplyBehavior:    domain.ApplyBundleRebuild,
			MutableAtRuntime: true,
			Description:      "Completed idempotency key cache TTL in hours",
			Group:            "idempotency",
			RedactPolicy:     domain.RedactNone,
		},
		{
			Key:              "idempotency.hmac_secret",
			Kind:             domain.KindConfig,
			AllowedScopes:    []domain.Scope{domain.ScopeGlobal},
			DefaultValue:     "",
			ValueType:        domain.ValueTypeString,
			ApplyBehavior:    domain.ApplyBundleRebuild,
			MutableAtRuntime: true,
			Secret:           true,
			Description:      "HMAC secret for signing idempotency keys before storage",
			Group:            "idempotency",
			RedactPolicy:     domain.RedactFull,
		},

		// --- Callback Rate Limit. ---
		{
			Key:              "callback_rate_limit.per_minute",
			Kind:             domain.KindConfig,
			AllowedScopes:    []domain.Scope{domain.ScopeGlobal},
			DefaultValue:     defaultCallbackPerMinute,
			ValueType:        domain.ValueTypeInt,
			Validator:        validatePositiveInt,
			ApplyBehavior:    domain.ApplyLiveRead,
			MutableAtRuntime: true,
			Description:      "Maximum callbacks per external system per minute",
			Group:            "callback_rate_limit",
			RedactPolicy:     domain.RedactNone,
		},

		// --- Fetcher. ---
		{
			Key:              "fetcher.enabled",
			Kind:             domain.KindConfig,
			AllowedScopes:    []domain.Scope{domain.ScopeGlobal},
			DefaultValue:     defaultFetcherEnabled,
			ValueType:        domain.ValueTypeBool,
			ApplyBehavior:    domain.ApplyBundleRebuildAndReconcile,
			MutableAtRuntime: true,
			Description:      "Enable Fetcher-backed source discovery module",
			Group:            "fetcher",
			RedactPolicy:     domain.RedactNone,
		},
		{
			Key:              "fetcher.url",
			Kind:             domain.KindConfig,
			AllowedScopes:    []domain.Scope{domain.ScopeGlobal},
			DefaultValue:     defaultFetcherURL,
			ValueType:        domain.ValueTypeString,
			ApplyBehavior:    domain.ApplyBundleRebuild,
			MutableAtRuntime: true,
			Description:      "Fetcher service base URL",
			Group:            "fetcher",
			RedactPolicy:     domain.RedactNone,
		},
		{
			Key:              "fetcher.allow_private_ips",
			Kind:             domain.KindConfig,
			AllowedScopes:    []domain.Scope{domain.ScopeGlobal},
			DefaultValue:     defaultFetcherAllowPrivateIPs,
			ValueType:        domain.ValueTypeBool,
			ApplyBehavior:    domain.ApplyBundleRebuild,
			MutableAtRuntime: true,
			Description:      "Allow Fetcher to connect to private IP addresses",
			Group:            "fetcher",
			RedactPolicy:     domain.RedactNone,
		},
		{
			Key:              "fetcher.health_timeout_sec",
			Kind:             domain.KindConfig,
			AllowedScopes:    []domain.Scope{domain.ScopeGlobal},
			DefaultValue:     defaultKeyFetcherHealthTimeout,
			ValueType:        domain.ValueTypeInt,
			Validator:        validatePositiveInt,
			ApplyBehavior:    domain.ApplyBundleRebuild,
			MutableAtRuntime: true,
			Description:      "Fetcher health check timeout in seconds",
			Group:            "fetcher",
			RedactPolicy:     domain.RedactNone,
		},
		{
			Key:              "fetcher.request_timeout_sec",
			Kind:             domain.KindConfig,
			AllowedScopes:    []domain.Scope{domain.ScopeGlobal},
			DefaultValue:     defaultKeyFetcherRequestTimeout,
			ValueType:        domain.ValueTypeInt,
			Validator:        validatePositiveInt,
			ApplyBehavior:    domain.ApplyBundleRebuild,
			MutableAtRuntime: true,
			Description:      "Fetcher HTTP request timeout in seconds",
			Group:            "fetcher",
			RedactPolicy:     domain.RedactNone,
		},
		{
			Key:              "fetcher.discovery_interval_sec",
			Kind:             domain.KindConfig,
			AllowedScopes:    []domain.Scope{domain.ScopeGlobal},
			DefaultValue:     defaultFetcherDiscoveryInt,
			ValueType:        domain.ValueTypeInt,
			Validator:        validatePositiveInt,
			ApplyBehavior:    domain.ApplyWorkerReconcile,
			MutableAtRuntime: true,
			Description:      "Fetcher source discovery polling interval in seconds",
			Group:            "fetcher",
			RedactPolicy:     domain.RedactNone,
		},
		{
			Key:              "fetcher.schema_cache_ttl_sec",
			Kind:             domain.KindConfig,
			AllowedScopes:    []domain.Scope{domain.ScopeGlobal},
			DefaultValue:     defaultKeyFetcherSchemaCacheTTL,
			ValueType:        domain.ValueTypeInt,
			Validator:        validatePositiveInt,
			ApplyBehavior:    domain.ApplyBundleRebuild,
			MutableAtRuntime: true,
			Description:      "Fetcher schema cache TTL in seconds",
			Group:            "fetcher",
			RedactPolicy:     domain.RedactNone,
		},
		{
			Key:              "fetcher.extraction_poll_sec",
			Kind:             domain.KindConfig,
			AllowedScopes:    []domain.Scope{domain.ScopeGlobal},
			DefaultValue:     defaultFetcherExtractionPoll,
			ValueType:        domain.ValueTypeInt,
			Validator:        validatePositiveInt,
			ApplyBehavior:    domain.ApplyBundleRebuild,
			MutableAtRuntime: true,
			Description:      "Fetcher extraction job polling interval in seconds",
			Group:            "fetcher",
			RedactPolicy:     domain.RedactNone,
		},
		{
			Key:              "fetcher.extraction_timeout_sec",
			Kind:             domain.KindConfig,
			AllowedScopes:    []domain.Scope{domain.ScopeGlobal},
			DefaultValue:     defaultFetcherExtractionTO,
			ValueType:        domain.ValueTypeInt,
			Validator:        validatePositiveInt,
			ApplyBehavior:    domain.ApplyBundleRebuild,
			MutableAtRuntime: true,
			Description:      "Fetcher extraction job timeout in seconds",
			Group:            "fetcher",
			RedactPolicy:     domain.RedactNone,
		},

		// --- Deduplication. ---
		{
			Key:              "deduplication.ttl_sec",
			Kind:             domain.KindConfig,
			AllowedScopes:    []domain.Scope{domain.ScopeGlobal},
			DefaultValue:     defaultDedupeTTLSec,
			ValueType:        domain.ValueTypeInt,
			Validator:        validatePositiveInt,
			ApplyBehavior:    domain.ApplyBundleRebuild,
			MutableAtRuntime: true,
			Description:      "Deduplication key TTL in seconds",
			Group:            "deduplication",
			RedactPolicy:     domain.RedactNone,
		},

		// --- Object Storage. ---
		{
			Key:              "object_storage.endpoint",
			Kind:             domain.KindConfig,
			AllowedScopes:    []domain.Scope{domain.ScopeGlobal},
			DefaultValue:     defaultObjStorageEndpoint,
			ValueType:        domain.ValueTypeString,
			ApplyBehavior:    domain.ApplyBundleRebuild,
			MutableAtRuntime: true,
			Description:      "S3-compatible object storage endpoint URL",
			Group:            "object_storage",
			RedactPolicy:     domain.RedactNone,
		},
		{
			Key:              "object_storage.region",
			Kind:             domain.KindConfig,
			AllowedScopes:    []domain.Scope{domain.ScopeGlobal},
			DefaultValue:     defaultObjStorageRegion,
			ValueType:        domain.ValueTypeString,
			ApplyBehavior:    domain.ApplyBundleRebuild,
			MutableAtRuntime: true,
			Description:      "Object storage region",
			Group:            "object_storage",
			RedactPolicy:     domain.RedactNone,
		},
		{
			Key:              "object_storage.bucket",
			Kind:             domain.KindConfig,
			AllowedScopes:    []domain.Scope{domain.ScopeGlobal},
			DefaultValue:     defaultObjStorageBucket,
			ValueType:        domain.ValueTypeString,
			ApplyBehavior:    domain.ApplyBundleRebuild,
			MutableAtRuntime: true,
			Description:      "Object storage bucket name for exports",
			Group:            "object_storage",
			RedactPolicy:     domain.RedactNone,
		},
		{
			Key:              "object_storage.access_key_id",
			Kind:             domain.KindConfig,
			AllowedScopes:    []domain.Scope{domain.ScopeGlobal},
			DefaultValue:     "",
			ValueType:        domain.ValueTypeString,
			ApplyBehavior:    domain.ApplyBundleRebuild,
			MutableAtRuntime: true,
			Secret:           true,
			Description:      "Object storage access key ID",
			Group:            "object_storage",
			RedactPolicy:     domain.RedactFull,
		},
		{
			Key:              "object_storage.secret_access_key",
			Kind:             domain.KindConfig,
			AllowedScopes:    []domain.Scope{domain.ScopeGlobal},
			DefaultValue:     "",
			ValueType:        domain.ValueTypeString,
			ApplyBehavior:    domain.ApplyBundleRebuild,
			MutableAtRuntime: true,
			Secret:           true,
			Description:      "Object storage secret access key",
			Group:            "object_storage",
			RedactPolicy:     domain.RedactFull,
		},
		{
			Key:              "object_storage.use_path_style",
			Kind:             domain.KindConfig,
			AllowedScopes:    []domain.Scope{domain.ScopeGlobal},
			DefaultValue:     defaultObjStoragePathStyle,
			ValueType:        domain.ValueTypeBool,
			ApplyBehavior:    domain.ApplyBundleRebuild,
			MutableAtRuntime: true,
			Description:      "Use path-style addressing for object storage requests",
			Group:            "object_storage",
			RedactPolicy:     domain.RedactNone,
		},

		// --- Export Worker. ---
		{
			Key:              "export_worker.enabled",
			Kind:             domain.KindConfig,
			AllowedScopes:    []domain.Scope{domain.ScopeGlobal},
			DefaultValue:     defaultExportEnabled,
			ValueType:        domain.ValueTypeBool,
			ApplyBehavior:    domain.ApplyBundleRebuildAndReconcile,
			MutableAtRuntime: true,
			Description:      "Enable the export worker background processor",
			Group:            "export_worker",
			RedactPolicy:     domain.RedactNone,
		},
		{
			Key:              "export_worker.poll_interval_sec",
			Kind:             domain.KindConfig,
			AllowedScopes:    []domain.Scope{domain.ScopeGlobal},
			DefaultValue:     defaultExportPollInt,
			ValueType:        domain.ValueTypeInt,
			Validator:        validatePositiveInt,
			ApplyBehavior:    domain.ApplyWorkerReconcile,
			MutableAtRuntime: true,
			Description:      "Export worker polling interval in seconds",
			Group:            "export_worker",
			RedactPolicy:     domain.RedactNone,
		},
		{
			Key:              "export_worker.page_size",
			Kind:             domain.KindConfig,
			AllowedScopes:    []domain.Scope{domain.ScopeGlobal},
			DefaultValue:     defaultExportPageSize,
			ValueType:        domain.ValueTypeInt,
			Validator:        validatePositiveInt,
			ApplyBehavior:    domain.ApplyWorkerReconcile,
			MutableAtRuntime: true,
			Description:      "Number of rows per page in export queries",
			Group:            "export_worker",
			RedactPolicy:     domain.RedactNone,
		},
		{
			Key:              "export_worker.presign_expiry_sec",
			Kind:             domain.KindConfig,
			AllowedScopes:    []domain.Scope{domain.ScopeGlobal},
			DefaultValue:     defaultExportPresignExp,
			ValueType:        domain.ValueTypeInt,
			Validator:        validatePositiveInt,
			ApplyBehavior:    domain.ApplyLiveRead,
			MutableAtRuntime: true,
			Description:      "Pre-signed URL expiry for export downloads in seconds",
			Group:            "export_worker",
			RedactPolicy:     domain.RedactNone,
		},

		// --- Webhook. ---
		{
			Key:              "webhook.timeout_sec",
			Kind:             domain.KindConfig,
			AllowedScopes:    []domain.Scope{domain.ScopeGlobal},
			DefaultValue:     defaultWebhookTimeout,
			ValueType:        domain.ValueTypeInt,
			Validator:        validatePositiveInt,
			ApplyBehavior:    domain.ApplyBundleRebuild,
			MutableAtRuntime: true,
			Description:      "Default HTTP timeout for webhook dispatches in seconds",
			Group:            "webhook",
			RedactPolicy:     domain.RedactNone,
		},

		// --- Cleanup Worker. ---
		{
			Key:              "cleanup_worker.enabled",
			Kind:             domain.KindConfig,
			AllowedScopes:    []domain.Scope{domain.ScopeGlobal},
			DefaultValue:     defaultCleanupEnabled,
			ValueType:        domain.ValueTypeBool,
			ApplyBehavior:    domain.ApplyBundleRebuildAndReconcile,
			MutableAtRuntime: true,
			Description:      "Enable the cleanup worker background processor",
			Group:            "cleanup_worker",
			RedactPolicy:     domain.RedactNone,
		},
		{
			Key:              "cleanup_worker.interval_sec",
			Kind:             domain.KindConfig,
			AllowedScopes:    []domain.Scope{domain.ScopeGlobal},
			DefaultValue:     defaultCleanupInterval,
			ValueType:        domain.ValueTypeInt,
			Validator:        validatePositiveInt,
			ApplyBehavior:    domain.ApplyWorkerReconcile,
			MutableAtRuntime: true,
			Description:      "Cleanup worker execution interval in seconds",
			Group:            "cleanup_worker",
			RedactPolicy:     domain.RedactNone,
		},
		{
			Key:              "cleanup_worker.batch_size",
			Kind:             domain.KindConfig,
			AllowedScopes:    []domain.Scope{domain.ScopeGlobal},
			DefaultValue:     defaultCleanupBatchSize,
			ValueType:        domain.ValueTypeInt,
			Validator:        validatePositiveInt,
			ApplyBehavior:    domain.ApplyWorkerReconcile,
			MutableAtRuntime: true,
			Description:      "Number of items per cleanup batch",
			Group:            "cleanup_worker",
			RedactPolicy:     domain.RedactNone,
		},
		{
			Key:              "cleanup_worker.grace_period_sec",
			Kind:             domain.KindConfig,
			AllowedScopes:    []domain.Scope{domain.ScopeGlobal},
			DefaultValue:     defaultCleanupGracePeriod,
			ValueType:        domain.ValueTypeInt,
			Validator:        validatePositiveInt,
			ApplyBehavior:    domain.ApplyWorkerReconcile,
			MutableAtRuntime: true,
			Description:      "Grace period before cleanup of expired items in seconds",
			Group:            "cleanup_worker",
			RedactPolicy:     domain.RedactNone,
		},

		// --- Scheduler. ---
		{
			Key:              "scheduler.interval_sec",
			Kind:             domain.KindConfig,
			AllowedScopes:    []domain.Scope{domain.ScopeGlobal},
			DefaultValue:     defaultSchedulerInterval,
			ValueType:        domain.ValueTypeInt,
			Validator:        validatePositiveInt,
			ApplyBehavior:    domain.ApplyWorkerReconcile,
			MutableAtRuntime: true,
			Description:      "Scheduler tick interval in seconds",
			Group:            "scheduler",
			RedactPolicy:     domain.RedactNone,
		},

		// --- Archival. ---
		{
			Key:              "archival.enabled",
			Kind:             domain.KindConfig,
			AllowedScopes:    []domain.Scope{domain.ScopeGlobal},
			DefaultValue:     defaultArchivalEnabled,
			ValueType:        domain.ValueTypeBool,
			ApplyBehavior:    domain.ApplyBundleRebuildAndReconcile,
			MutableAtRuntime: true,
			Description:      "Enable the audit log archival worker",
			Group:            "archival",
			RedactPolicy:     domain.RedactNone,
		},
		{
			Key:              "archival.interval_hours",
			Kind:             domain.KindConfig,
			AllowedScopes:    []domain.Scope{domain.ScopeGlobal},
			DefaultValue:     defaultArchivalInterval,
			ValueType:        domain.ValueTypeInt,
			Validator:        validatePositiveInt,
			ApplyBehavior:    domain.ApplyWorkerReconcile,
			MutableAtRuntime: true,
			Description:      "Archival worker execution interval in hours",
			Group:            "archival",
			RedactPolicy:     domain.RedactNone,
		},
		{
			Key:              "archival.hot_retention_days",
			Kind:             domain.KindConfig,
			AllowedScopes:    []domain.Scope{domain.ScopeGlobal},
			DefaultValue:     defaultArchivalHotDays,
			ValueType:        domain.ValueTypeInt,
			Validator:        validatePositiveInt,
			ApplyBehavior:    domain.ApplyWorkerReconcile,
			MutableAtRuntime: true,
			Description:      "Days to retain audit logs in hot storage before archival",
			Group:            "archival",
			RedactPolicy:     domain.RedactNone,
		},
		{
			Key:              "archival.warm_retention_months",
			Kind:             domain.KindConfig,
			AllowedScopes:    []domain.Scope{domain.ScopeGlobal},
			DefaultValue:     defaultArchivalWarmMonths,
			ValueType:        domain.ValueTypeInt,
			Validator:        validatePositiveInt,
			ApplyBehavior:    domain.ApplyWorkerReconcile,
			MutableAtRuntime: true,
			Description:      "Months to retain audit logs in warm storage",
			Group:            "archival",
			RedactPolicy:     domain.RedactNone,
		},
		{
			Key:              "archival.cold_retention_months",
			Kind:             domain.KindConfig,
			AllowedScopes:    []domain.Scope{domain.ScopeGlobal},
			DefaultValue:     defaultArchivalColdMonths,
			ValueType:        domain.ValueTypeInt,
			Validator:        validatePositiveInt,
			ApplyBehavior:    domain.ApplyWorkerReconcile,
			MutableAtRuntime: true,
			Description:      "Months to retain audit logs in cold storage",
			Group:            "archival",
			RedactPolicy:     domain.RedactNone,
		},
		{
			Key:              "archival.batch_size",
			Kind:             domain.KindConfig,
			AllowedScopes:    []domain.Scope{domain.ScopeGlobal},
			DefaultValue:     defaultArchivalBatchSize,
			ValueType:        domain.ValueTypeInt,
			Validator:        validatePositiveInt,
			ApplyBehavior:    domain.ApplyWorkerReconcile,
			MutableAtRuntime: true,
			Description:      "Number of audit log records per archival batch",
			Group:            "archival",
			RedactPolicy:     domain.RedactNone,
		},
		{
			Key:              "archival.storage_bucket",
			Kind:             domain.KindConfig,
			AllowedScopes:    []domain.Scope{domain.ScopeGlobal},
			DefaultValue:     "",
			ValueType:        domain.ValueTypeString,
			ApplyBehavior:    domain.ApplyBundleRebuildAndReconcile,
			MutableAtRuntime: true,
			Description:      "Object storage bucket for archived audit logs",
			Group:            "archival",
			RedactPolicy:     domain.RedactNone,
		},
		{
			Key:              "archival.storage_prefix",
			Kind:             domain.KindConfig,
			AllowedScopes:    []domain.Scope{domain.ScopeGlobal},
			DefaultValue:     defaultArchivalStoragePrefix,
			ValueType:        domain.ValueTypeString,
			ApplyBehavior:    domain.ApplyBundleRebuildAndReconcile,
			MutableAtRuntime: true,
			Description:      "Object storage key prefix for archived audit logs",
			Group:            "archival",
			RedactPolicy:     domain.RedactNone,
		},
		{
			Key:              "archival.storage_class",
			Kind:             domain.KindConfig,
			AllowedScopes:    []domain.Scope{domain.ScopeGlobal},
			DefaultValue:     defaultArchivalStorageClass,
			ValueType:        domain.ValueTypeString,
			ApplyBehavior:    domain.ApplyBundleRebuildAndReconcile,
			MutableAtRuntime: true,
			Description:      "Object storage class for archived audit logs",
			Group:            "archival",
			RedactPolicy:     domain.RedactNone,
		},
		{
			Key:              "archival.partition_lookahead",
			Kind:             domain.KindConfig,
			AllowedScopes:    []domain.Scope{domain.ScopeGlobal},
			DefaultValue:     defaultArchivalPartitionLA,
			ValueType:        domain.ValueTypeInt,
			Validator:        validatePositiveInt,
			ApplyBehavior:    domain.ApplyWorkerReconcile,
			MutableAtRuntime: true,
			Description:      "Number of future partitions to pre-create for audit log tables",
			Group:            "archival",
			RedactPolicy:     domain.RedactNone,
		},
		{
			Key:              "archival.presign_expiry_sec",
			Kind:             domain.KindConfig,
			AllowedScopes:    []domain.Scope{domain.ScopeGlobal},
			DefaultValue:     defaultArchivalPresignExpiry,
			ValueType:        domain.ValueTypeInt,
			Validator:        validatePositiveInt,
			ApplyBehavior:    domain.ApplyLiveRead,
			MutableAtRuntime: true,
			Description:      "Pre-signed URL expiry for archive downloads in seconds",
			Group:            "archival",
			RedactPolicy:     domain.RedactNone,
		},
	}
}

// Validators for systemplane key registration.

// validatePositiveInt rejects zero and negative integers.
func validatePositiveInt(value any) error {
	intVal, ok := toInt(value)
	if !ok {
		return fmt.Errorf("expected integer value: %w", domain.ErrValueInvalid)
	}

	if intVal <= 0 {
		return fmt.Errorf("value must be a positive integer, got %d: %w", intVal, domain.ErrValueInvalid)
	}

	return nil
}

// validateNonNegativeInt rejects negative integers but allows zero.
func validateNonNegativeInt(value any) error {
	intVal, ok := toInt(value)
	if !ok {
		return fmt.Errorf("expected integer value: %w", domain.ErrValueInvalid)
	}

	if intVal < 0 {
		return fmt.Errorf("value must be a non-negative integer, got %d: %w", intVal, domain.ErrValueInvalid)
	}

	return nil
}

// validateLogLevel accepts only the standard structured log levels.
func validateLogLevel(value any) error {
	strVal, ok := value.(string)
	if !ok {
		return fmt.Errorf("expected string value: %w", domain.ErrValueInvalid)
	}

	switch strings.ToLower(strVal) {
	case "debug", "info", "warn", "error":
		return nil
	default:
		return fmt.Errorf("invalid log level %q, must be one of: debug, info, warn, error: %w", strVal, domain.ErrValueInvalid)
	}
}

// validateSSLMode accepts only valid PostgreSQL SSL modes.
func validateSSLMode(value any) error {
	strVal, ok := value.(string)
	if !ok {
		return fmt.Errorf("expected string value: %w", domain.ErrValueInvalid)
	}

	switch strVal {
	case "disable", "require", "verify-ca", "verify-full":
		return nil
	default:
		return fmt.Errorf("invalid SSL mode %q, must be one of: disable, require, verify-ca, verify-full: %w", strVal, domain.ErrValueInvalid)
	}
}

// validateOptionalSSLMode allows empty string (unset replica) or a valid SSL mode.
func validateOptionalSSLMode(value any) error {
	strVal, ok := value.(string)
	if !ok {
		return fmt.Errorf("expected string value: %w", domain.ErrValueInvalid)
	}

	if strVal == "" {
		return nil
	}

	return validateSSLMode(value)
}

// validateNonEmptyString rejects empty strings.
func validateNonEmptyString(value any) error {
	strVal, ok := value.(string)
	if !ok {
		return fmt.Errorf("expected string value: %w", domain.ErrValueInvalid)
	}

	if strings.TrimSpace(strVal) == "" {
		return fmt.Errorf("value must not be empty: %w", domain.ErrValueInvalid)
	}

	return nil
}

// toInt converts value to int64 for validation, handling int, int64, and
// whole-number float64 (which is how JSON numbers arrive).
func toInt(value any) (int64, bool) {
	switch typed := value.(type) {
	case int:
		return int64(typed), true
	case int64:
		return typed, true
	case float64:
		if typed == float64(int64(typed)) {
			return int64(typed), true
		}

		return 0, false
	default:
		return 0, false
	}
}
