// Copyright 2025 Lerian Studio. All rights reserved.
// Use of this source code is governed by an Elastic License 2.0
// that can be found in the LICENSE.md file.

package bootstrap

import (
	"errors"
	"fmt"
	"net/url"

	"github.com/LerianStudio/lib-commons/v5/commons/systemplane"
)

var (
	errFetcherURLMustBeString      = errors.New("fetcher url must be a string")
	errFetcherURLMustBeAbsolute    = errors.New("fetcher url must be an absolute URL")
	errFetcherURLMustUseHTTPScheme = errors.New("fetcher url must use http or https")
	errValueNotPositive            = errors.New("value must be positive")
	errValueNotNumeric             = errors.New("value must be numeric")
	errValueNotString              = errors.New("value must be a string")
	errCORSWildcardInProd          = errors.New("CORS_ALLOWED_ORIGINS must be restricted in production (exact \"*\" not allowed)")
	errBodyLimitExceedsCeiling     = errors.New("server.body_limit_bytes must not exceed the application ceiling")
)

// appBodyLimitCeilingBytes mirrors fiber_server.go's appBodyLimitCeilingBytes
// constant. Duplicated here (rather than imported) to keep the systemplane
// validator decoupled from fiber_server.go at package init time.
const keyBodyLimitCeilingBytes = 128 * 1024 * 1024

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
	defaultPGTLSRequired     = false
	defaultPGReplicaTLSReq   = false
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
	defaultRedisTLSRequired  = false
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
	defaultRabbitTLSRequired         = false

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
	// Admin plane (/system) tier defaults. Intentionally low — operator-only
	// surface. Enforced per-tier so admin traffic cannot starve tenant traffic
	// even under the shared fail-open mode.
	defaultRateLimitAdminMax = 30
	defaultRateLimitAdminExp = 60

	// Infrastructure defaults.
	defaultInfraConnectTimeout       = 30
	defaultInfraHealthCheckTimeout   = 5
	defaultInfraHealthCheckTimeoutMs = 800

	// Idempotency defaults.
	defaultIdempotencyRetryWindow = 300
	defaultIdempotencySuccessTTL  = 168

	// Outbox dispatcher defaults.
	defaultOutboxRetryWindow         = 300 // seconds (5 minutes, matches lib-commons default)
	defaultOutboxDispatchIntervalSec = 2   // seconds

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

	// Fetcher bridge worker defaults (Fix 4): mirror the Config struct
	// envDefault tags so snapshot-driven hydration falls back to the same
	// values an env-only deployment would see.
	defaultBridgeIntervalSec       = 30
	defaultBridgeBatchSize         = 50
	defaultBridgeTenantConcurrency = 4
	defaultBridgeStaleThresholdSec = 3600
	defaultBridgeRetryMaxAttempts  = 5

	// Custody retention sweep worker defaults (T-006). Mirror the Config
	// struct envDefault tags so snapshot-driven hydration matches env-only
	// deployments. 900s = 15 min sweep cadence, 3600s = 1 h grace period.
	defaultCustodyRetentionSweepIntervalSec = 900
	defaultCustodyRetentionGracePeriodSec   = 3600

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
	defaultObjStorageTLSRequired   = false

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

	// matcherKeyDefsCapacity is the total number of keys returned by
	// matcherKeyDefs(). Kept in sync manually (no strict upper bound — serves
	// only as a preallocation hint to avoid repeated slice growth).
	matcherKeyDefsCapacity = 108
)

// validatePositiveInt validates that the value is a positive integer.
func validatePositiveInt(value any) error {
	switch typed := value.(type) {
	case int:
		if typed <= 0 {
			return fmt.Errorf("%w, got %d", errValueNotPositive, typed)
		}

		return nil
	case float64:
		if typed <= 0 {
			return fmt.Errorf("%w, got %v", errValueNotPositive, typed)
		}

		return nil
	default:
		return fmt.Errorf("%w, got %T", errValueNotNumeric, value)
	}
}

// corsProductionValidator returns a validator that rejects wildcard CORS
// origins when envName is production. Used as a runtime guard on the
// `cors.allowed_origins` systemplane key so an admin PUT cannot widen the
// CORS policy past what validateProductionConfig enforced at startup.
//
// The envName is captured at registration time (from the bootstrap Config
// snapshot). This matches the semantics of the startup validator:
// ENV_NAME is bootstrap-only, so freezing it here does not race with
// runtime edits.
func corsProductionValidator(envName string) func(any) error {
	if !IsProductionEnvironment(envName) {
		return nil // no-op outside production; keep validator list lean
	}

	return func(value any) error {
		str, ok := value.(string)
		if !ok {
			return errValueNotString
		}

		if corsContainsWildcard(str) {
			return fmt.Errorf("%w: got %q", errCORSWildcardInProd, str)
		}

		return nil
	}
}

// validateBodyLimitBytes enforces the server.body_limit_bytes invariants:
// must be a positive integer AND must not exceed appBodyLimitCeilingBytes
// (128 MiB). An admin PUT beyond the ceiling would silently fail at
// request time (Fiber caps at the ceiling regardless), so we reject
// early with a clear error.
func validateBodyLimitBytes(value any) error {
	if err := validatePositiveInt(value); err != nil {
		return err
	}

	var bytes int

	switch typed := value.(type) {
	case int:
		bytes = typed
	case float64:
		bytes = int(typed)
	default:
		return fmt.Errorf("%w, got %T", errValueNotNumeric, value)
	}

	if bytes > keyBodyLimitCeilingBytes {
		return fmt.Errorf("%w: got %d, ceiling %d", errBodyLimitExceedsCeiling, bytes, keyBodyLimitCeilingBytes)
	}

	return nil
}

// validateFetcherURL validates that the value is a well-formed HTTP(S) URL.
func validateFetcherURL(value any) error {
	str, ok := value.(string)
	if !ok {
		return errFetcherURLMustBeString
	}

	if str == "" {
		// Empty URL is permitted because the Fetcher integration is gated by
		// the separate `fetcher.enabled` key (default false). When
		// `fetcher.enabled=true`, an empty URL will fail fast at Fetcher client
		// construction via dynamic_fetcher_client.go. See also init.go gating
		// at cfg.Fetcher.Enabled check sites.
		return nil
	}

	parsedURL, err := url.Parse(str)
	if err != nil {
		return fmt.Errorf("fetcher url parse: %w", err)
	}

	if !parsedURL.IsAbs() {
		return errFetcherURLMustBeAbsolute
	}

	if parsedURL.Scheme != "http" && parsedURL.Scheme != "https" {
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
// The registered default for each key is derived from `cfg`, which must be
// the env-resolved Config snapshot produced by LoadConfig/LoadConfigWithLogger.
// This seeds systemplane with operator intent so that env overrides like
// MATCHER_RATE_LIMIT_MAX=10000 propagate as the initial runtime value rather
// than being overridden by compile-time constants. Admin PUTs replace the
// stored value, and OnChange callbacks push updates back into *Config via
// applySystemplaneOverrides.
//
// Must be called before Client.Start().
func RegisterMatcherKeys(client *systemplane.Client, cfg *Config) error {
	if client == nil {
		return fmt.Errorf("register matcher keys: %w", ErrSystemplaneClientNil)
	}

	if cfg == nil {
		return fmt.Errorf("register matcher keys: %w", ErrConfigNil)
	}

	defs := matcherKeyDefs(cfg)
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

// matcherKeyDefs returns the list of runtime-mutable systemplane keys.
//
// PRINCIPLE: Only keys with live reload paths belong here. Keys that require a
// process restart to take effect (connection strings, bootstrap credentials,
// replica host/port/user/password) must NOT be registered here — they belong in
// environment variables and config/.config-map.example.
//
// Registering a bootstrap-only key would be a footgun: the admin API would
// appear to accept changes, but the running process would continue using the
// boot-time value. Operators would rotate a password, see GET return the new
// value, and trust it — while live traffic still uses the old secret. This was
// the H5 regression addressed on feat/lib-commons-v5.
//
// DEFAULT-VALUE PRINCIPLE: the registered default for every key is read from
// `cfg` (the env-resolved Config snapshot). This ensures env-var overrides like
// MATCHER_RATE_LIMIT_MAX=10000 beat the compile-time constants; the env-seeded
// defaults become the systemplane baseline, admin PUTs overwrite it, and
// OnChange callbacks push changes back into *Config.
//
// Callers that need defs before Config is built (e.g., test fixtures) can pass
// defaultConfig() — the return value will match the compile-time constants.
//
//nolint:funlen // pre-existing: large list-builder function; splitting across helpers hurts readability without reducing complexity.
func matcherKeyDefs(cfg *Config) []matcherKeyDef {
	if cfg == nil {
		cfg = defaultConfig()
	}

	defs := make([]matcherKeyDef, 0, matcherKeyDefsCapacity)

	// --- App ---
	// app.log_level intentionally NOT registered: LOG_LEVEL is bootstrap-only.
	// Runtime log-level swapping is not implemented. Set LOG_LEVEL via env var
	// and restart to change.
	//
	// app.mode intentionally NOT registered: DEPLOYMENT_MODE is bootstrap-only.
	// It is consumed at startup to seed logger configuration defaults and the
	// /readyz response envelope's "deployment_mode" field. A runtime PUT of
	// app.mode would appear to succeed but would be silently ineffective —
	// logger environment is already resolved and cached. Change via
	// DEPLOYMENT_MODE env var and restart the process.
	//
	// The following per-stack TLS enforcement flags are also intentionally NOT
	// registered because TLS posture is decided pre-connection at bootstrap:
	//   - POSTGRES_TLS_REQUIRED            (postgres.tls_required)
	//   - POSTGRES_REPLICA_TLS_REQUIRED    (postgres.replica_tls_required)
	//   - REDIS_TLS_REQUIRED               (redis.tls_required)
	//   - RABBITMQ_TLS_REQUIRED            (rabbitmq.tls_required)
	//   - OBJECT_STORAGE_TLS_REQUIRED      (object_storage.tls_required)
	// ValidateRequiredTLS runs once before any infra connection opens;
	// flipping a flag at runtime would appear to succeed but would have no
	// effect on the already-open connections. Change via env var and restart.
	defs = append(defs,
		matcherKeyDef{key: "app.env_name", defaultValue: cfg.App.EnvName, description: "Application environment name (e.g., development, staging, production)"},
	)

	// --- Server ---
	// The following server keys are intentionally NOT registered because they
	// are consumed once at startup and have no live-reload path:
	//   - server.address          (fiber listener bind address)
	//   - server.tls_cert_file    (ListenTLS argument, read once at boot)
	//   - server.tls_key_file     (ListenTLS argument, read once at boot)
	//   - server.tls_terminated_upstream (HSTS flag computed once in NewFiberApp)
	//   - server.trusted_proxies  (fiber ProxyHeader configured once in NewFiberApp)
	// Registering them on the systemplane would mislead operators: admin PUTs via
	// PUT /system/matcher/... would appear to accept changes, but the running
	// process would continue using the boot-time value. Change these via env
	// vars and restart the process — same precedent as app.log_level above.
	defs = append(defs,
		matcherKeyDef{key: "server.body_limit_bytes", defaultValue: cfg.Server.BodyLimitBytes, description: "Maximum HTTP request body size in bytes (must be positive and not exceed 128 MiB ceiling)", validator: validateBodyLimitBytes},
		matcherKeyDef{key: "cors.allowed_origins", defaultValue: cfg.Server.CORSAllowedOrigins, description: "Comma-separated list of allowed CORS origins", validator: corsProductionValidator(cfg.App.EnvName)},
		matcherKeyDef{key: "cors.allowed_methods", defaultValue: cfg.Server.CORSAllowedMethods, description: "Comma-separated list of allowed CORS methods"},
		matcherKeyDef{key: "cors.allowed_headers", defaultValue: cfg.Server.CORSAllowedHeaders, description: "Comma-separated list of allowed CORS headers"},
	)

	// --- Tenancy ---
	// Default-tenant values are bootstrap-only because buildTenantExtractor wires
	// them into auth globals once at startup. Multi-tenant manager knobs remain
	// registered because dynamicInfrastructureProvider re-reads configGetter and
	// rebuilds the canonical manager when manager-shaping values change.
	defs = append(defs,
		matcherKeyDef{key: "tenancy.multi_tenant_enabled", defaultValue: cfg.Tenancy.MultiTenantEnabled, description: "Enable multi-tenant mode"},
		matcherKeyDef{key: "tenancy.multi_tenant_url", defaultValue: cfg.Tenancy.MultiTenantURL, description: "Multi-tenant service URL"},
		matcherKeyDef{key: "tenancy.multi_tenant_environment", defaultValue: cfg.Tenancy.MultiTenantEnvironment, description: "Multi-tenant environment identifier"},
		matcherKeyDef{key: "tenancy.multi_tenant_redis_host", defaultValue: cfg.Tenancy.MultiTenantRedisHost, description: "Multi-tenant Redis host"},
		matcherKeyDef{key: "tenancy.multi_tenant_redis_port", defaultValue: cfg.Tenancy.MultiTenantRedisPort, description: "Multi-tenant Redis port"},
		matcherKeyDef{key: "tenancy.multi_tenant_redis_password", defaultValue: cfg.Tenancy.MultiTenantRedisPassword, description: "Multi-tenant Redis password", redact: systemplane.RedactFull},
		matcherKeyDef{key: "tenancy.multi_tenant_redis_tls", defaultValue: cfg.Tenancy.MultiTenantRedisTLS, description: "Multi-tenant Redis TLS enabled"},
		matcherKeyDef{key: "tenancy.multi_tenant_max_tenant_pools", defaultValue: cfg.Tenancy.MultiTenantMaxTenantPools, description: "Maximum number of tenant connection pools"},
		matcherKeyDef{key: "tenancy.multi_tenant_idle_timeout_sec", defaultValue: cfg.Tenancy.MultiTenantIdleTimeoutSec, description: "Tenant pool idle timeout in seconds"},
		matcherKeyDef{key: "tenancy.multi_tenant_timeout", defaultValue: cfg.Tenancy.MultiTenantTimeout, description: "Multi-tenant operation timeout in seconds"},
		matcherKeyDef{key: "tenancy.multi_tenant_circuit_breaker_threshold", defaultValue: cfg.Tenancy.MultiTenantCircuitBreakerThreshold, description: "Multi-tenant circuit breaker threshold"},
		matcherKeyDef{key: "tenancy.multi_tenant_circuit_breaker_timeout_sec", defaultValue: cfg.Tenancy.MultiTenantCircuitBreakerTimeoutSec, description: "Multi-tenant circuit breaker timeout in seconds"},
		matcherKeyDef{key: "tenancy.multi_tenant_service_api_key", defaultValue: cfg.Tenancy.MultiTenantServiceAPIKey, description: "Multi-tenant service API key", redact: systemplane.RedactFull},
		matcherKeyDef{key: "tenancy.multi_tenant_cache_ttl_sec", defaultValue: cfg.Tenancy.MultiTenantCacheTTLSec, description: "Multi-tenant cache TTL in seconds"},
		matcherKeyDef{key: "tenancy.multi_tenant_connections_check_interval_sec", defaultValue: cfg.Tenancy.MultiTenantConnectionsCheckIntervalSec, description: "Multi-tenant connections check interval in seconds"},
	)

	// --- PostgreSQL ---
	// Connection identity and credentials (host, port, user, password, db,
	// ssl_mode, connect_timeout_sec, migrations_path, replica_*) are
	// bootstrap-only — omitted here per the principle above. Only pool tunables
	// with live reload paths are registered.
	defs = append(defs,
		matcherKeyDef{key: "postgres.max_open_conns", defaultValue: cfg.Postgres.MaxOpenConnections, description: "Maximum open database connections", validator: validatePositiveInt},
		matcherKeyDef{key: "postgres.max_idle_conns", defaultValue: cfg.Postgres.MaxIdleConnections, description: "Maximum idle database connections"},
		matcherKeyDef{key: "postgres.conn_max_lifetime_mins", defaultValue: cfg.Postgres.ConnMaxLifetimeMins, description: "Connection max lifetime in minutes"},
		matcherKeyDef{key: "postgres.conn_max_idle_time_mins", defaultValue: cfg.Postgres.ConnMaxIdleTimeMins, description: "Connection max idle time in minutes"},
		matcherKeyDef{key: "postgres.query_timeout_sec", defaultValue: cfg.Postgres.QueryTimeoutSec, description: "Database query timeout in seconds"},
	)

	// --- Redis ---
	// Connection identity and credentials (host, master_name, password, db,
	// protocol, tls, ca_cert, dial_timeout_ms) are bootstrap-only — omitted
	// here per the principle above. Only pool/timeout tunables with live
	// reload paths are registered.
	defs = append(defs,
		matcherKeyDef{key: "redis.pool_size", defaultValue: cfg.Redis.PoolSize, description: "Redis connection pool size"},
		matcherKeyDef{key: "redis.min_idle_conns", defaultValue: cfg.Redis.MinIdleConn, description: "Redis minimum idle connections"},
		matcherKeyDef{key: "redis.read_timeout_ms", defaultValue: cfg.Redis.ReadTimeoutMs, description: "Redis read timeout in milliseconds"},
		matcherKeyDef{key: "redis.write_timeout_ms", defaultValue: cfg.Redis.WriteTimeoutMs, description: "Redis write timeout in milliseconds"},
	)

	// --- RabbitMQ ---
	// All RabbitMQ keys (url, host, port, user, password, vhost, health_url,
	// allow_insecure_health_check) are bootstrap-only. The broker connection
	// is created once at startup; changing any of these at runtime would
	// mislead operators without reconnecting live traffic. Manage these via
	// environment variables.

	// --- Auth ---
	// Auth middleware and tenant extraction are constructed once at startup, so
	// these values remain bootstrap-only to avoid misleading operators.

	// --- Telemetry ---
	// telemetry.collector_endpoint intentionally NOT registered: the OTel
	// exporter is wired once during bootstrap (observability.go). Changing the
	// collector endpoint at runtime would not re-create the exporter, so an
	// admin PUT would be silently ineffective. Change via env var and restart.
	defs = append(defs,
		matcherKeyDef{key: "telemetry.enabled", defaultValue: cfg.Telemetry.Enabled, description: "Enable OpenTelemetry"},
		matcherKeyDef{key: "telemetry.service_name", defaultValue: cfg.Telemetry.ServiceName, description: "OTel service name"},
		matcherKeyDef{key: "telemetry.library_name", defaultValue: cfg.Telemetry.LibraryName, description: "OTel library name"},
		matcherKeyDef{key: "telemetry.service_version", defaultValue: cfg.Telemetry.ServiceVersion, description: "OTel service version"},
		matcherKeyDef{key: "telemetry.deployment_env", defaultValue: cfg.Telemetry.DeploymentEnv, description: "OTel deployment environment"},
		matcherKeyDef{key: "telemetry.db_metrics_interval_sec", defaultValue: cfg.Telemetry.DBMetricsIntervalSec, description: "Database metrics collection interval in seconds"},
	)

	// --- Swagger ---
	defs = append(defs,
		matcherKeyDef{key: "swagger.enabled", defaultValue: cfg.Swagger.Enabled, description: "Enable Swagger UI"},
		matcherKeyDef{key: "swagger.host", defaultValue: cfg.Swagger.Host, description: "Swagger host override"},
		matcherKeyDef{key: "swagger.schemes", defaultValue: cfg.Swagger.Schemes, description: "Swagger URL schemes"},
	)

	// --- Rate Limit ---
	defs = append(defs,
		matcherKeyDef{key: "rate_limit.enabled", defaultValue: cfg.RateLimit.Enabled, description: "Enable rate limiting"},
		matcherKeyDef{key: "rate_limit.max", defaultValue: cfg.RateLimit.Max, description: "Rate limit max requests", validator: validatePositiveInt},
		matcherKeyDef{key: "rate_limit.expiry_sec", defaultValue: cfg.RateLimit.ExpirySec, description: "Rate limit window in seconds", validator: validatePositiveInt},
		matcherKeyDef{key: "rate_limit.export_max", defaultValue: cfg.RateLimit.ExportMax, description: "Export endpoint rate limit"},
		matcherKeyDef{key: "rate_limit.export_expiry_sec", defaultValue: cfg.RateLimit.ExportExpirySec, description: "Export rate limit window in seconds"},
		matcherKeyDef{key: "rate_limit.dispatch_max", defaultValue: cfg.RateLimit.DispatchMax, description: "Dispatch endpoint rate limit"},
		matcherKeyDef{key: "rate_limit.dispatch_expiry_sec", defaultValue: cfg.RateLimit.DispatchExpirySec, description: "Dispatch rate limit window in seconds"},
		matcherKeyDef{key: "rate_limit.admin_max", defaultValue: cfg.RateLimit.AdminMax, description: "Admin plane (/system) rate limit max requests", validator: validatePositiveInt},
		matcherKeyDef{key: "rate_limit.admin_expiry_sec", defaultValue: cfg.RateLimit.AdminExpirySec, description: "Admin plane (/system) rate limit window in seconds", validator: validatePositiveInt},
	)

	// --- Infrastructure ---
	defs = append(defs,
		matcherKeyDef{key: "infrastructure.connect_timeout_sec", defaultValue: cfg.Infrastructure.ConnectTimeoutSec, description: "Infrastructure connect timeout in seconds"},
		matcherKeyDef{key: "infrastructure.health_check_timeout_sec", defaultValue: cfg.Infrastructure.HealthCheckTimeoutSec, description: "Health check timeout in seconds (legacy; prefer health_check_timeout_ms)"},
		matcherKeyDef{key: "infrastructure.health_check_timeout_ms", defaultValue: cfg.Infrastructure.HealthCheckTimeoutMs, description: "Per-check health probe timeout in milliseconds"},
	)

	// --- Idempotency ---
	defs = append(defs,
		matcherKeyDef{key: "idempotency.retry_window_sec", defaultValue: cfg.Idempotency.RetryWindowSec, description: "Idempotency retry window in seconds"},
		matcherKeyDef{key: "idempotency.success_ttl_hours", defaultValue: cfg.Idempotency.SuccessTTLHours, description: "Idempotency success TTL in hours"},
		matcherKeyDef{key: "idempotency.hmac_secret", defaultValue: cfg.Idempotency.HMACSecret, description: "Idempotency HMAC signing secret", redact: systemplane.RedactFull},
	)

	// --- Outbox Dispatcher ---
	// Outbox dispatcher timing is wired once during bootstrap. Leave these as
	// restart-only until the dispatcher supports live reconfiguration.

	// --- Deduplication ---
	defs = append(defs,
		matcherKeyDef{key: "deduplication.ttl_sec", defaultValue: cfg.Dedupe.TTLSec, description: "Deduplication TTL in seconds"},
	)

	// --- Callback Rate Limit ---
	defs = append(defs,
		matcherKeyDef{key: "callback_rate_limit.per_minute", defaultValue: cfg.CallbackRateLimit.PerMinute, description: "Callback rate limit per minute"},
	)

	// --- Webhook ---
	defs = append(defs,
		matcherKeyDef{key: "webhook.timeout_sec", defaultValue: cfg.Webhook.TimeoutSec, description: "Webhook timeout in seconds"},
	)

	// --- Fetcher ---
	defs = append(defs,
		matcherKeyDef{key: "fetcher.enabled", defaultValue: cfg.Fetcher.Enabled, description: "Enable Fetcher integration"},
		matcherKeyDef{key: "fetcher.url", defaultValue: cfg.Fetcher.URL, description: "Fetcher service URL", validator: validateFetcherURL},
		matcherKeyDef{key: "fetcher.allow_private_ips", defaultValue: cfg.Fetcher.AllowPrivateIPs, description: "Allow Fetcher to use private IPs"},
		matcherKeyDef{key: "fetcher.health_timeout_sec", defaultValue: cfg.Fetcher.HealthTimeoutSec, description: "Fetcher health check timeout in seconds"},
		matcherKeyDef{key: "fetcher.request_timeout_sec", defaultValue: cfg.Fetcher.RequestTimeoutSec, description: "Fetcher request timeout in seconds"},
		matcherKeyDef{key: "fetcher.discovery_interval_sec", defaultValue: cfg.Fetcher.DiscoveryIntervalSec, description: "Fetcher discovery interval in seconds"},
		matcherKeyDef{key: "fetcher.schema_cache_ttl_sec", defaultValue: cfg.Fetcher.SchemaCacheTTLSec, description: "Fetcher schema cache TTL in seconds"},
		matcherKeyDef{key: "fetcher.extraction_poll_sec", defaultValue: cfg.Fetcher.ExtractionPollSec, description: "Fetcher extraction poll interval in seconds"},
		matcherKeyDef{key: "fetcher.extraction_timeout_sec", defaultValue: cfg.Fetcher.ExtractionTimeoutSec, description: "Fetcher extraction timeout in seconds"},
		matcherKeyDef{key: "fetcher.max_extraction_bytes", defaultValue: cfg.Fetcher.MaxExtractionBytes, description: "Max Fetcher extraction payload size in bytes"},
		matcherKeyDef{key: "fetcher.bridge_interval_sec", defaultValue: cfg.Fetcher.BridgeIntervalSec, description: "Fetcher bridge worker poll cadence in seconds"},
		matcherKeyDef{key: "fetcher.bridge_batch_size", defaultValue: cfg.Fetcher.BridgeBatchSize, description: "Fetcher bridge worker per-tenant batch size"},
		matcherKeyDef{key: "fetcher.bridge_tenant_concurrency", defaultValue: cfg.Fetcher.BridgeTenantConcurrency, description: "Fetcher bridge worker tenant-level fan-out ceiling per pollCycle", validator: validatePositiveInt},
		matcherKeyDef{key: "fetcher.bridge_stale_threshold_sec", defaultValue: cfg.Fetcher.BridgeStaleThresholdSec, description: "Fetcher bridge stale extraction dashboard threshold in seconds"},
		matcherKeyDef{key: "fetcher.bridge_retry_max_attempts", defaultValue: cfg.Fetcher.BridgeRetryMaxAttempts, description: "Fetcher bridge max retry attempts per extraction"},
		matcherKeyDef{key: "fetcher.custody_retention_sweep_interval_sec", defaultValue: cfg.Fetcher.CustodyRetentionSweepIntervalSec, description: "Custody retention sweep worker interval in seconds"},
		matcherKeyDef{key: "fetcher.custody_retention_grace_period_sec", defaultValue: cfg.Fetcher.CustodyRetentionGracePeriodSec, description: "Custody retention grace period for LATE-LINKED extractions in seconds"},
	)

	// --- M2M ---
	defs = append(defs,
		matcherKeyDef{key: "m2m.m2m_target_service", defaultValue: cfg.M2M.M2MTargetService, description: "M2M target service name"},
		matcherKeyDef{key: "m2m.m2m_credential_cache_ttl_sec", defaultValue: cfg.M2M.M2MCredentialCacheTTLSec, description: "M2M credential cache TTL in seconds"},
		matcherKeyDef{key: "m2m.aws_region", defaultValue: cfg.M2M.AWSRegion, description: "M2M AWS region"},
	)

	// --- Object Storage ---
	defs = append(defs,
		matcherKeyDef{key: "object_storage.endpoint", defaultValue: cfg.ObjectStorage.Endpoint, description: "Object storage endpoint"},
		matcherKeyDef{key: "object_storage.region", defaultValue: cfg.ObjectStorage.Region, description: "Object storage region"},
		matcherKeyDef{key: "object_storage.bucket", defaultValue: cfg.ObjectStorage.Bucket, description: "Object storage bucket"},
		matcherKeyDef{key: "object_storage.access_key_id", defaultValue: cfg.ObjectStorage.AccessKeyID, description: "Object storage access key ID", redact: systemplane.RedactFull},
		matcherKeyDef{key: "object_storage.secret_access_key", defaultValue: cfg.ObjectStorage.SecretAccessKey, description: "Object storage secret access key", redact: systemplane.RedactFull},
		matcherKeyDef{key: "object_storage.use_path_style", defaultValue: cfg.ObjectStorage.UsePathStyle, description: "Use path-style S3 addressing"},
		matcherKeyDef{key: "object_storage.allow_insecure_endpoint", defaultValue: cfg.ObjectStorage.AllowInsecure, description: "Allow insecure object storage endpoint"},
	)

	// --- Export Worker ---
	defs = append(defs,
		matcherKeyDef{key: "export_worker.enabled", defaultValue: cfg.ExportWorker.Enabled, description: "Enable export worker"},
		matcherKeyDef{key: "export_worker.poll_interval_sec", defaultValue: cfg.ExportWorker.PollIntervalSec, description: "Export worker poll interval in seconds"},
		matcherKeyDef{key: "export_worker.page_size", defaultValue: cfg.ExportWorker.PageSize, description: "Export worker page size"},
		matcherKeyDef{key: "export_worker.presign_expiry_sec", defaultValue: cfg.ExportWorker.PresignExpirySec, description: "Export presigned URL expiry in seconds"},
	)

	// --- Cleanup Worker ---
	defs = append(defs,
		matcherKeyDef{key: "cleanup_worker.enabled", defaultValue: cfg.CleanupWorker.Enabled, description: "Enable cleanup worker"},
		matcherKeyDef{key: "cleanup_worker.interval_sec", defaultValue: cfg.CleanupWorker.IntervalSec, description: "Cleanup worker interval in seconds"},
		matcherKeyDef{key: "cleanup_worker.batch_size", defaultValue: cfg.CleanupWorker.BatchSize, description: "Cleanup worker batch size"},
		matcherKeyDef{key: "cleanup_worker.grace_period_sec", defaultValue: cfg.CleanupWorker.GracePeriodSec, description: "Cleanup worker grace period in seconds"},
	)

	// --- Scheduler ---
	defs = append(defs,
		matcherKeyDef{key: "scheduler.interval_sec", defaultValue: cfg.Scheduler.IntervalSec, description: "Scheduler interval in seconds"},
	)

	// --- Archival ---
	defs = append(defs,
		matcherKeyDef{key: "archival.enabled", defaultValue: cfg.Archival.Enabled, description: "Enable archival worker"},
		matcherKeyDef{key: "archival.interval_hours", defaultValue: cfg.Archival.IntervalHours, description: "Archival interval in hours"},
		matcherKeyDef{key: "archival.hot_retention_days", defaultValue: cfg.Archival.HotRetentionDays, description: "Hot retention in days"},
		matcherKeyDef{key: "archival.warm_retention_months", defaultValue: cfg.Archival.WarmRetentionMonths, description: "Warm retention in months"},
		matcherKeyDef{key: "archival.cold_retention_months", defaultValue: cfg.Archival.ColdRetentionMonths, description: "Cold retention in months"},
		matcherKeyDef{key: "archival.batch_size", defaultValue: cfg.Archival.BatchSize, description: "Archival batch size"},
		matcherKeyDef{key: "archival.partition_lookahead", defaultValue: cfg.Archival.PartitionLookahead, description: "Archival partition lookahead"},
		matcherKeyDef{key: "archival.storage_bucket", defaultValue: cfg.Archival.StorageBucket, description: "Archival storage bucket"},
		matcherKeyDef{key: "archival.storage_prefix", defaultValue: cfg.Archival.StoragePrefix, description: "Archival storage prefix"},
		matcherKeyDef{key: "archival.storage_class", defaultValue: cfg.Archival.StorageClass, description: "Archival storage class"},
		matcherKeyDef{key: "archival.presign_expiry_sec", defaultValue: cfg.Archival.PresignExpirySec, description: "Archival presigned URL expiry in seconds"},
	)

	return defs
}

// cloneMatcherEffectiveValues creates a shallow copy of a string->any map.
// Used by settings resolution to prevent caller mutation.
