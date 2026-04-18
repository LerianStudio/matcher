// Copyright 2025 Lerian Studio. All rights reserved.
// Use of this source code is governed by an Elastic License 2.0
// that can be found in the LICENSE.md file.

package bootstrap

import (
	"errors"
	"fmt"
	"net/url"
	"strings"

	"github.com/LerianStudio/lib-commons/v5/commons/systemplane"
)

var (
	errFetcherURLMustBeString      = errors.New("fetcher url must be a string")
	errFetcherURLMustBeAbsolute    = errors.New("fetcher url must be an absolute URL")
	errFetcherURLMustUseHTTPScheme = errors.New("fetcher url must use http or https")
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
	defaultKeyBodyLimitBytes     = 32 * 1024 * 1024 // 32 MiB
	defaultTLSTerminatedUpstream = false
	defaultServerTrustedProxies  = ""
	defaultServerTLSCertFile     = ""
	defaultServerTLSKeyFile      = ""

	// Tenancy defaults.
	defaultTenantID                         = "11111111-1111-1111-1111-111111111111"
	defaultTenantSlug                       = "default"
	defaultMultiTenantEnabled               = false
	defaultMultiTenantRedisPort             = "6379"
	defaultMultiTenantRedisTLS              = false
	defaultMultiTenantMaxTenantPools        = 100
	defaultMultiTenantIdleTimeoutSec        = 300
	defaultMultiTenantTimeout               = 30
	defaultMultiTenantCircuitBreakerThresh  = 5
	defaultMultiTenantCircuitBreakerSec     = 30
	defaultMultiTenantCacheTTLSec           = 120
	defaultMultiTenantConnsCheckIntervalSec = 30

	// PostgreSQL defaults.
	defaultPGHost            = "localhost"
	defaultPGPort            = "5432"
	defaultPGUser            = "matcher"
	defaultPGPassword        = "matcher_dev_password" // #nosec G101 -- Dev-mode default; rejected by validateProductionConfig in production. //nolint:gosec
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
	defaultRabbitPassword            = "matcher_dev_password" // #nosec G101 -- Dev-mode default; rejected by validateProductionConfig in production. //nolint:gosec
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
	defaultTelemetryServiceVersion  = "1.1.0"
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

	// M2M defaults.
	defaultM2MTargetService      = "fetcher"
	defaultM2MCredentialCacheTTL = 300

	// Deduplication defaults.
	defaultDedupeTTLSec = 3600

	// Object storage defaults.
	defaultObjStorageEndpoint      = "http://localhost:8333"
	defaultObjStorageRegion        = "us-east-1"
	defaultObjStorageBucket        = "matcher-exports"
	defaultObjStoragePathStyle     = true
	defaultObjStorageAllowInsecure = false

	// Export worker defaults.
	defaultExportEnabled    = true
	defaultExportPollInt    = 5
	defaultExportPageSize   = 1000
	defaultExportPresignExp = 3600

	// Webhook defaults.
	defaultWebhookTimeout = 30
	maxWebhookTimeoutSec  = 300

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
	defaultArchivalStorageBucket = "matcher-archives"
	defaultArchivalStoragePrefix = "archives/audit-logs"
	defaultArchivalStorageClass  = "GLACIER"
	defaultArchivalPartitionLA   = 3
	defaultArchivalPresignExpiry = 3600
	maxPresignExpirySec          = 604800
)

// validateLogLevel validates that the value is a supported log level string.
func validateLogLevel(v any) error {
	s, ok := v.(string)
	if !ok {
		return fmt.Errorf("log level must be a string, got %T", v)
	}

	switch strings.ToLower(s) {
	case "debug", "info", "warn", "error":
		return nil
	default:
		return fmt.Errorf("unsupported log level: %q (must be debug, info, warn, or error)", s)
	}
}

// validatePositiveInt validates that the value is a positive integer.
func validatePositiveInt(v any) error {
	switch n := v.(type) {
	case int:
		if n <= 0 {
			return fmt.Errorf("value must be positive, got %d", n)
		}

		return nil
	case float64:
		if n <= 0 {
			return fmt.Errorf("value must be positive, got %v", n)
		}

		return nil
	default:
		return fmt.Errorf("value must be numeric, got %T", v)
	}
}

// validateFetcherURL validates that the value is a well-formed HTTP(S) URL.
func validateFetcherURL(v any) error {
	s, ok := v.(string)
	if !ok {
		return errFetcherURLMustBeString
	}

	if s == "" {
		return nil // empty is allowed (disabled)
	}

	u, err := url.Parse(s)
	if err != nil {
		return fmt.Errorf("fetcher url parse: %w", err)
	}

	if !u.IsAbs() {
		return errFetcherURLMustBeAbsolute
	}

	if u.Scheme != "http" && u.Scheme != "https" {
		return errFetcherURLMustUseHTTPScheme
	}

	return nil
}

// systemplaneNamespace is the single namespace used for all Matcher
// runtime configuration keys. v5's Client uses (namespace, key) pairs;
// we use a flat namespace with dotted keys to preserve the existing key
// naming convention (e.g., "app.log_level").
const systemplaneNamespace = "matcher"

// RegisterMatcherKeys registers all runtime-mutable Matcher configuration
// keys on the v5 systemplane Client. Each key corresponds to a field in
// the Config struct and its sub-structs, using dotted mapstructure tag
// paths as key names.
//
// Must be called before Client.Start().
func RegisterMatcherKeys(client *systemplane.Client) error {
	defs := matcherKeyDefs()
	for _, def := range defs {
		opts := []systemplane.KeyOption{
			systemplane.WithDescription(def.description),
		}

		if def.validator != nil {
			opts = append(opts, systemplane.WithValidator(def.validator))
		}

		if def.redact != systemplane.RedactNone {
			opts = append(opts, systemplane.WithRedaction(def.redact))
		}

		if err := client.Register(systemplaneNamespace, def.key, def.defaultValue, opts...); err != nil {
			return fmt.Errorf("register matcher key %q: %w", def.key, err)
		}
	}

	return nil
}

// matcherKeyDef is a local definition struct for building registration calls.
type matcherKeyDef struct {
	key          string
	defaultValue any
	description  string
	validator    func(any) error
	redact       systemplane.RedactPolicy
}

// matcherKeyDefs returns all Matcher configuration key definitions.
// The definitions are the canonical source of truth for all default values.
func matcherKeyDefs() []matcherKeyDef {
	var defs []matcherKeyDef

	// --- App ---
	defs = append(defs,
		matcherKeyDef{key: "app.env_name", defaultValue: defaultEnvName, description: "Application environment name (e.g., development, staging, production)"},
		matcherKeyDef{key: "app.log_level", defaultValue: defaultLogLevel, description: "Application log level (debug, info, warn, error)", validator: validateLogLevel},
	)

	// --- Server ---
	defs = append(defs,
		matcherKeyDef{key: "server.address", defaultValue: defaultServerAddress, description: "HTTP server listen address (e.g., :4018)"},
		matcherKeyDef{key: "server.body_limit_bytes", defaultValue: defaultKeyBodyLimitBytes, description: "Maximum HTTP request body size in bytes", validator: validatePositiveInt},
		matcherKeyDef{key: "cors.allowed_origins", defaultValue: defaultCORSAllowedOrigins, description: "Comma-separated list of allowed CORS origins"},
		matcherKeyDef{key: "cors.allowed_methods", defaultValue: defaultCORSAllowedMethods, description: "Comma-separated list of allowed CORS methods"},
		matcherKeyDef{key: "cors.allowed_headers", defaultValue: defaultCORSAllowedHeaders, description: "Comma-separated list of allowed CORS headers"},
		matcherKeyDef{key: "server.tls_cert_file", defaultValue: defaultServerTLSCertFile, description: "Path to TLS certificate file"},
		matcherKeyDef{key: "server.tls_key_file", defaultValue: defaultServerTLSKeyFile, description: "Path to TLS private key file"},
		matcherKeyDef{key: "server.tls_terminated_upstream", defaultValue: defaultTLSTerminatedUpstream, description: "Whether TLS is terminated by an upstream proxy"},
		matcherKeyDef{key: "server.trusted_proxies", defaultValue: defaultServerTrustedProxies, description: "Comma-separated list of trusted proxy CIDRs"},
	)

	// --- Tenancy ---
	defs = append(defs,
		matcherKeyDef{key: "tenancy.default_tenant_id", defaultValue: defaultTenantID, description: "Default tenant ID for single-tenant mode"},
		matcherKeyDef{key: "tenancy.default_tenant_slug", defaultValue: defaultTenantSlug, description: "Default tenant slug for single-tenant mode"},
		matcherKeyDef{key: "tenancy.multi_tenant_enabled", defaultValue: defaultMultiTenantEnabled, description: "Enable multi-tenant mode"},
		matcherKeyDef{key: "tenancy.multi_tenant_url", defaultValue: "", description: "Multi-tenant service URL"},
		matcherKeyDef{key: "tenancy.multi_tenant_environment", defaultValue: "", description: "Multi-tenant environment identifier"},
		matcherKeyDef{key: "tenancy.multi_tenant_redis_host", defaultValue: "", description: "Multi-tenant Redis host"},
		matcherKeyDef{key: "tenancy.multi_tenant_redis_port", defaultValue: defaultMultiTenantRedisPort, description: "Multi-tenant Redis port"},
		matcherKeyDef{key: "tenancy.multi_tenant_redis_password", defaultValue: "", description: "Multi-tenant Redis password", redact: systemplane.RedactFull},
		matcherKeyDef{key: "tenancy.multi_tenant_redis_tls", defaultValue: defaultMultiTenantRedisTLS, description: "Multi-tenant Redis TLS enabled"},
		matcherKeyDef{key: "tenancy.multi_tenant_max_tenant_pools", defaultValue: defaultMultiTenantMaxTenantPools, description: "Maximum number of tenant connection pools"},
		matcherKeyDef{key: "tenancy.multi_tenant_idle_timeout_sec", defaultValue: defaultMultiTenantIdleTimeoutSec, description: "Tenant pool idle timeout in seconds"},
		matcherKeyDef{key: "tenancy.multi_tenant_timeout", defaultValue: defaultMultiTenantTimeout, description: "Multi-tenant operation timeout in seconds"},
		matcherKeyDef{key: "tenancy.multi_tenant_circuit_breaker_threshold", defaultValue: defaultMultiTenantCircuitBreakerThresh, description: "Multi-tenant circuit breaker threshold"},
		matcherKeyDef{key: "tenancy.multi_tenant_circuit_breaker_timeout_sec", defaultValue: defaultMultiTenantCircuitBreakerSec, description: "Multi-tenant circuit breaker timeout in seconds"},
		matcherKeyDef{key: "tenancy.multi_tenant_service_api_key", defaultValue: "", description: "Multi-tenant service API key", redact: systemplane.RedactFull},
		matcherKeyDef{key: "tenancy.multi_tenant_cache_ttl_sec", defaultValue: defaultMultiTenantCacheTTLSec, description: "Multi-tenant cache TTL in seconds"},
		matcherKeyDef{key: "tenancy.multi_tenant_connections_check_interval_sec", defaultValue: defaultMultiTenantConnsCheckIntervalSec, description: "Multi-tenant connections check interval in seconds"},
	)

	// --- PostgreSQL ---
	defs = append(defs,
		matcherKeyDef{key: "postgres.primary_host", defaultValue: defaultPGHost, description: "Primary PostgreSQL host"},
		matcherKeyDef{key: "postgres.primary_port", defaultValue: defaultPGPort, description: "Primary PostgreSQL port"},
		matcherKeyDef{key: "postgres.primary_user", defaultValue: defaultPGUser, description: "Primary PostgreSQL user"},
		matcherKeyDef{key: "postgres.primary_password", defaultValue: defaultPGPassword, description: "Primary PostgreSQL password", redact: systemplane.RedactFull},
		matcherKeyDef{key: "postgres.primary_db", defaultValue: defaultPGDB, description: "Primary PostgreSQL database"},
		matcherKeyDef{key: "postgres.primary_ssl_mode", defaultValue: defaultPGSSLMode, description: "Primary PostgreSQL SSL mode"},
		matcherKeyDef{key: "postgres.replica_host", defaultValue: "", description: "Replica PostgreSQL host"},
		matcherKeyDef{key: "postgres.replica_port", defaultValue: "", description: "Replica PostgreSQL port"},
		matcherKeyDef{key: "postgres.replica_user", defaultValue: "", description: "Replica PostgreSQL user"},
		matcherKeyDef{key: "postgres.replica_password", defaultValue: "", description: "Replica PostgreSQL password", redact: systemplane.RedactFull},
		matcherKeyDef{key: "postgres.replica_db", defaultValue: "", description: "Replica PostgreSQL database"},
		matcherKeyDef{key: "postgres.replica_ssl_mode", defaultValue: "", description: "Replica PostgreSQL SSL mode"},
		matcherKeyDef{key: "postgres.max_open_conns", defaultValue: defaultPGMaxOpenConns, description: "Maximum open database connections", validator: validatePositiveInt},
		matcherKeyDef{key: "postgres.max_idle_conns", defaultValue: defaultPGMaxIdleConns, description: "Maximum idle database connections"},
		matcherKeyDef{key: "postgres.conn_max_lifetime_mins", defaultValue: defaultPGConnMaxLifeMins, description: "Connection max lifetime in minutes"},
		matcherKeyDef{key: "postgres.conn_max_idle_time_mins", defaultValue: defaultPGConnMaxIdleMins, description: "Connection max idle time in minutes"},
		matcherKeyDef{key: "postgres.connect_timeout_sec", defaultValue: defaultPGConnectTimeout, description: "Database connect timeout in seconds"},
		matcherKeyDef{key: "postgres.query_timeout_sec", defaultValue: defaultPGQueryTimeout, description: "Database query timeout in seconds"},
		matcherKeyDef{key: "postgres.migrations_path", defaultValue: defaultPGMigrationsPath, description: "Path to database migrations"},
	)

	// --- Redis ---
	defs = append(defs,
		matcherKeyDef{key: "redis.host", defaultValue: defaultRedisHost, description: "Redis host"},
		matcherKeyDef{key: "redis.master_name", defaultValue: "", description: "Redis Sentinel master name"},
		matcherKeyDef{key: "redis.password", defaultValue: "", description: "Redis password", redact: systemplane.RedactFull},
		matcherKeyDef{key: "redis.db", defaultValue: defaultRedisDB, description: "Redis database number"},
		matcherKeyDef{key: "redis.protocol", defaultValue: defaultRedisProtocol, description: "Redis protocol version"},
		matcherKeyDef{key: "redis.tls", defaultValue: defaultRedisTLS, description: "Redis TLS enabled"},
		matcherKeyDef{key: "redis.ca_cert", defaultValue: "", description: "Redis CA certificate path"},
		matcherKeyDef{key: "redis.pool_size", defaultValue: defaultRedisPoolSize, description: "Redis connection pool size"},
		matcherKeyDef{key: "redis.min_idle_conns", defaultValue: defaultRedisMinIdleConn, description: "Redis minimum idle connections"},
		matcherKeyDef{key: "redis.read_timeout_ms", defaultValue: defaultRedisReadTimeout, description: "Redis read timeout in milliseconds"},
		matcherKeyDef{key: "redis.write_timeout_ms", defaultValue: defaultRedisWriteTimeout, description: "Redis write timeout in milliseconds"},
		matcherKeyDef{key: "redis.dial_timeout_ms", defaultValue: defaultRedisDialTimeout, description: "Redis dial timeout in milliseconds"},
	)

	// --- RabbitMQ ---
	defs = append(defs,
		matcherKeyDef{key: "rabbitmq.url", defaultValue: defaultRabbitURI, description: "RabbitMQ URI scheme"},
		matcherKeyDef{key: "rabbitmq.host", defaultValue: defaultRabbitHost, description: "RabbitMQ host"},
		matcherKeyDef{key: "rabbitmq.port", defaultValue: defaultRabbitPort, description: "RabbitMQ port"},
		matcherKeyDef{key: "rabbitmq.user", defaultValue: defaultRabbitUser, description: "RabbitMQ user"},
		matcherKeyDef{key: "rabbitmq.password", defaultValue: defaultRabbitPassword, description: "RabbitMQ password", redact: systemplane.RedactFull},
		matcherKeyDef{key: "rabbitmq.vhost", defaultValue: defaultRabbitVHost, description: "RabbitMQ virtual host"},
		matcherKeyDef{key: "rabbitmq.health_url", defaultValue: defaultRabbitHealthURL, description: "RabbitMQ health check URL"},
		matcherKeyDef{key: "rabbitmq.allow_insecure_health_check", defaultValue: defaultRabbitAllowInsecureHealth, description: "Allow insecure RabbitMQ health check"},
	)

	// --- Auth ---
	defs = append(defs,
		matcherKeyDef{key: "auth.enabled", defaultValue: defaultAuthEnabled, description: "Enable authentication"},
		matcherKeyDef{key: "auth.host", defaultValue: "", description: "Auth service host"},
		matcherKeyDef{key: "auth.token_secret", defaultValue: "", description: "JWT token signing secret", redact: systemplane.RedactFull},
	)

	// --- Telemetry ---
	defs = append(defs,
		matcherKeyDef{key: "telemetry.enabled", defaultValue: defaultTelemetryEnabled, description: "Enable OpenTelemetry"},
		matcherKeyDef{key: "telemetry.service_name", defaultValue: defaultTelemetryServiceName, description: "OTel service name"},
		matcherKeyDef{key: "telemetry.library_name", defaultValue: defaultTelemetryLibraryName, description: "OTel library name"},
		matcherKeyDef{key: "telemetry.service_version", defaultValue: defaultTelemetryServiceVersion, description: "OTel service version"},
		matcherKeyDef{key: "telemetry.deployment_env", defaultValue: defaultTelemetryDeploymentEnv, description: "OTel deployment environment"},
		matcherKeyDef{key: "telemetry.collector_endpoint", defaultValue: defaultTelemetryCollectorEP, description: "OTel collector endpoint"},
		matcherKeyDef{key: "telemetry.db_metrics_interval_sec", defaultValue: defaultTelemetryDBMetricsIntSec, description: "Database metrics collection interval in seconds"},
	)

	// --- Swagger ---
	defs = append(defs,
		matcherKeyDef{key: "swagger.enabled", defaultValue: defaultSwaggerEnabled, description: "Enable Swagger UI"},
		matcherKeyDef{key: "swagger.host", defaultValue: "", description: "Swagger host override"},
		matcherKeyDef{key: "swagger.schemes", defaultValue: defaultSwaggerSchemes, description: "Swagger URL schemes"},
	)

	// --- Rate Limit ---
	defs = append(defs,
		matcherKeyDef{key: "rate_limit.enabled", defaultValue: defaultRateLimitEnabled, description: "Enable rate limiting"},
		matcherKeyDef{key: "rate_limit.max", defaultValue: defaultRateLimitMax, description: "Rate limit max requests", validator: validatePositiveInt},
		matcherKeyDef{key: "rate_limit.expiry_sec", defaultValue: defaultRateLimitExpirySec, description: "Rate limit window in seconds", validator: validatePositiveInt},
		matcherKeyDef{key: "rate_limit.export_max", defaultValue: defaultRateLimitExportMax, description: "Export endpoint rate limit"},
		matcherKeyDef{key: "rate_limit.export_expiry_sec", defaultValue: defaultRateLimitExportExpiry, description: "Export rate limit window in seconds"},
		matcherKeyDef{key: "rate_limit.dispatch_max", defaultValue: defaultRateLimitDispatchMax, description: "Dispatch endpoint rate limit"},
		matcherKeyDef{key: "rate_limit.dispatch_expiry_sec", defaultValue: defaultRateLimitDispatchExp, description: "Dispatch rate limit window in seconds"},
	)

	// --- Infrastructure ---
	defs = append(defs,
		matcherKeyDef{key: "infrastructure.connect_timeout_sec", defaultValue: defaultInfraConnectTimeout, description: "Infrastructure connect timeout in seconds"},
		matcherKeyDef{key: "infrastructure.health_check_timeout_sec", defaultValue: defaultInfraHealthCheckTimeout, description: "Health check timeout in seconds"},
	)

	// --- Idempotency ---
	defs = append(defs,
		matcherKeyDef{key: "idempotency.retry_window_sec", defaultValue: defaultIdempotencyRetryWindow, description: "Idempotency retry window in seconds"},
		matcherKeyDef{key: "idempotency.success_ttl_hours", defaultValue: defaultIdempotencySuccessTTL, description: "Idempotency success TTL in hours"},
		matcherKeyDef{key: "idempotency.hmac_secret", defaultValue: "", description: "Idempotency HMAC signing secret", redact: systemplane.RedactFull},
	)

	// --- Deduplication ---
	defs = append(defs,
		matcherKeyDef{key: "deduplication.ttl_sec", defaultValue: defaultDedupeTTLSec, description: "Deduplication TTL in seconds"},
	)

	// --- Callback Rate Limit ---
	defs = append(defs,
		matcherKeyDef{key: "callback_rate_limit.per_minute", defaultValue: defaultCallbackPerMinute, description: "Callback rate limit per minute"},
	)

	// --- Webhook ---
	defs = append(defs,
		matcherKeyDef{key: "webhook.timeout_sec", defaultValue: defaultWebhookTimeout, description: "Webhook timeout in seconds"},
	)

	// --- Fetcher ---
	defs = append(defs,
		matcherKeyDef{key: "fetcher.enabled", defaultValue: defaultFetcherEnabled, description: "Enable Fetcher integration"},
		matcherKeyDef{key: "fetcher.url", defaultValue: defaultFetcherURL, description: "Fetcher service URL", validator: validateFetcherURL},
		matcherKeyDef{key: "fetcher.allow_private_ips", defaultValue: defaultFetcherAllowPrivateIPs, description: "Allow Fetcher to use private IPs"},
		matcherKeyDef{key: "fetcher.health_timeout_sec", defaultValue: defaultKeyFetcherHealthTimeout, description: "Fetcher health check timeout in seconds"},
		matcherKeyDef{key: "fetcher.request_timeout_sec", defaultValue: defaultKeyFetcherRequestTimeout, description: "Fetcher request timeout in seconds"},
		matcherKeyDef{key: "fetcher.discovery_interval_sec", defaultValue: defaultFetcherDiscoveryInt, description: "Fetcher discovery interval in seconds"},
		matcherKeyDef{key: "fetcher.schema_cache_ttl_sec", defaultValue: defaultKeyFetcherSchemaCacheTTL, description: "Fetcher schema cache TTL in seconds"},
		matcherKeyDef{key: "fetcher.extraction_poll_sec", defaultValue: defaultFetcherExtractionPoll, description: "Fetcher extraction poll interval in seconds"},
		matcherKeyDef{key: "fetcher.extraction_timeout_sec", defaultValue: defaultFetcherExtractionTO, description: "Fetcher extraction timeout in seconds"},
	)

	// --- M2M ---
	defs = append(defs,
		matcherKeyDef{key: "m2m.m2m_target_service", defaultValue: defaultM2MTargetService, description: "M2M target service name"},
		matcherKeyDef{key: "m2m.m2m_credential_cache_ttl_sec", defaultValue: defaultM2MCredentialCacheTTL, description: "M2M credential cache TTL in seconds"},
		matcherKeyDef{key: "m2m.aws_region", defaultValue: "", description: "M2M AWS region"},
	)

	// --- Object Storage ---
	defs = append(defs,
		matcherKeyDef{key: "object_storage.endpoint", defaultValue: defaultObjStorageEndpoint, description: "Object storage endpoint"},
		matcherKeyDef{key: "object_storage.region", defaultValue: defaultObjStorageRegion, description: "Object storage region"},
		matcherKeyDef{key: "object_storage.bucket", defaultValue: defaultObjStorageBucket, description: "Object storage bucket"},
		matcherKeyDef{key: "object_storage.access_key_id", defaultValue: "", description: "Object storage access key ID", redact: systemplane.RedactFull},
		matcherKeyDef{key: "object_storage.secret_access_key", defaultValue: "", description: "Object storage secret access key", redact: systemplane.RedactFull},
		matcherKeyDef{key: "object_storage.use_path_style", defaultValue: defaultObjStoragePathStyle, description: "Use path-style S3 addressing"},
		matcherKeyDef{key: "object_storage.allow_insecure_endpoint", defaultValue: defaultObjStorageAllowInsecure, description: "Allow insecure object storage endpoint"},
	)

	// --- Export Worker ---
	defs = append(defs,
		matcherKeyDef{key: "export_worker.enabled", defaultValue: defaultExportEnabled, description: "Enable export worker"},
		matcherKeyDef{key: "export_worker.poll_interval_sec", defaultValue: defaultExportPollInt, description: "Export worker poll interval in seconds"},
		matcherKeyDef{key: "export_worker.page_size", defaultValue: defaultExportPageSize, description: "Export worker page size"},
		matcherKeyDef{key: "export_worker.presign_expiry_sec", defaultValue: defaultExportPresignExp, description: "Export presigned URL expiry in seconds"},
	)

	// --- Cleanup Worker ---
	defs = append(defs,
		matcherKeyDef{key: "cleanup_worker.enabled", defaultValue: defaultCleanupEnabled, description: "Enable cleanup worker"},
		matcherKeyDef{key: "cleanup_worker.interval_sec", defaultValue: defaultCleanupInterval, description: "Cleanup worker interval in seconds"},
		matcherKeyDef{key: "cleanup_worker.batch_size", defaultValue: defaultCleanupBatchSize, description: "Cleanup worker batch size"},
		matcherKeyDef{key: "cleanup_worker.grace_period_sec", defaultValue: defaultCleanupGracePeriod, description: "Cleanup worker grace period in seconds"},
	)

	// --- Scheduler ---
	defs = append(defs,
		matcherKeyDef{key: "scheduler.interval_sec", defaultValue: defaultSchedulerInterval, description: "Scheduler interval in seconds"},
	)

	// --- Archival ---
	defs = append(defs,
		matcherKeyDef{key: "archival.enabled", defaultValue: defaultArchivalEnabled, description: "Enable archival worker"},
		matcherKeyDef{key: "archival.interval_hours", defaultValue: defaultArchivalInterval, description: "Archival interval in hours"},
		matcherKeyDef{key: "archival.hot_retention_days", defaultValue: defaultArchivalHotDays, description: "Hot retention in days"},
		matcherKeyDef{key: "archival.warm_retention_months", defaultValue: defaultArchivalWarmMonths, description: "Warm retention in months"},
		matcherKeyDef{key: "archival.cold_retention_months", defaultValue: defaultArchivalColdMonths, description: "Cold retention in months"},
		matcherKeyDef{key: "archival.batch_size", defaultValue: defaultArchivalBatchSize, description: "Archival batch size"},
		matcherKeyDef{key: "archival.partition_lookahead", defaultValue: defaultArchivalPartitionLA, description: "Archival partition lookahead"},
		matcherKeyDef{key: "archival.storage_bucket", defaultValue: defaultArchivalStorageBucket, description: "Archival storage bucket"},
		matcherKeyDef{key: "archival.storage_prefix", defaultValue: defaultArchivalStoragePrefix, description: "Archival storage prefix"},
		matcherKeyDef{key: "archival.storage_class", defaultValue: defaultArchivalStorageClass, description: "Archival storage class"},
		matcherKeyDef{key: "archival.presign_expiry_sec", defaultValue: defaultArchivalPresignExpiry, description: "Archival presigned URL expiry in seconds"},
	)

	return defs
}

// cloneMatcherEffectiveValues creates a shallow copy of a string->any map.
// Used by settings resolution to prevent caller mutation.
