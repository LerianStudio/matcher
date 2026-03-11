package bootstrap

import (
	"os"
	"reflect"
	"strings"
)

// configFieldDef is the static definition of a config field.
// CurrentValue is populated at runtime from the live config.
type configFieldDef struct {
	Key           string
	Label         string
	Type          string // "string", "int", "bool"
	DefaultValue  any
	HotReloadable bool
	EnvVar        string
	Constraints   []string
	Description   string
	Section       string
	Secret        bool // if true, value is redacted in API responses
}

// redactedValue is the placeholder shown for secret fields.
const redactedValue = "********"

// Default values for config schema fields, grouped by section.
// Extracted as named constants to satisfy the mnd linter (no magic numbers).
const (
	// server defaults.
	defaultSchemaBodyLimitBytes = 104857600

	// postgres defaults.
	defaultMaxOpenConns = 25
	defaultMaxIdleConns = 5
	defaultQueryTimeout = 30

	// redis defaults.
	defaultRedisPoolSize = 10

	// rate_limit defaults.
	defaultRateLimitMax         = 100
	defaultRateLimitExpiry      = 60
	defaultExportRateLimitMax   = 10
	defaultExportRateLimitExp   = 60
	defaultDispatchRateLimitMax = 50
	defaultDispatchRateLimitExp = 60

	// export_worker defaults.
	defaultExportPollInterval = 5
	defaultExportPageSize     = 1000
	defaultPresignExpiry      = 3600

	// cleanup_worker defaults.
	defaultCleanupInterval    = 3600
	defaultCleanupBatchSize   = 100
	defaultCleanupGracePeriod = 3600

	// scheduler defaults.
	defaultSchedulerInterval = 60

	// webhook defaults.
	defaultWebhookTimeout = 30

	// callback_rate_limit defaults.
	defaultCallbackRatePerMin = 60

	// deduplication defaults.
	defaultDedupeTTL = 3600

	// idempotency defaults.
	defaultIdempotencyRetryWindow = 300
	defaultIdempotencySuccessTTL  = 168

	// fetcher defaults — use canonical constants from config_env.go.
	defaultFetcherHealthTimeout    = defaultFetcherHealthTimeoutSec
	defaultFetcherRequestTimeout   = defaultFetcherRequestTimeoutSec
	defaultFetcherDiscoveryInt     = defaultFetcherDiscoveryIntervalSec
	defaultFetcherSchemaCacheTTL   = defaultFetcherSchemaCacheTTLSec
	defaultFetcherExtractionPoll   = defaultFetcherExtractionPollSec
	defaultFetcherExtractionTimout = defaultFetcherExtractionTimeoutSec

	// archival defaults.
	defaultArchivalIntervalHours = 24
	defaultArchivalBatchSize     = 5000
)

// buildConfigSchema returns the static schema definitions for all YAML-managed fields.
func buildConfigSchema() []configFieldDef {
	return []configFieldDef{
		// ── app ──────────────────────────────────────────────────
		{Key: "app.env_name", Label: "Environment Name", Type: "string", DefaultValue: "development", HotReloadable: false, EnvVar: "ENV_NAME", Description: "Deployment environment (development, staging, production)", Section: "app"},
		{Key: "app.log_level", Label: "Log Level", Type: "string", DefaultValue: "info", HotReloadable: true, EnvVar: "LOG_LEVEL", Constraints: []string{"enum:debug,info,warn,error"}, Description: "Application log verbosity level", Section: "app"},

		// ── server ──────────────────────────────────────────────
		{Key: "server.address", Label: "Listen Address", Type: "string", DefaultValue: ":4018", HotReloadable: false, EnvVar: "SERVER_ADDRESS", Description: "HTTP server bind address", Section: "server"},
		{Key: "server.body_limit_bytes", Label: "Body Limit (bytes)", Type: "int", DefaultValue: defaultSchemaBodyLimitBytes, HotReloadable: false, EnvVar: "HTTP_BODY_LIMIT_BYTES", Constraints: []string{"min:1"}, Description: "Maximum HTTP request body size in bytes", Section: "server"},
		{Key: "server.cors_allowed_origins", Label: "CORS Allowed Origins", Type: "string", DefaultValue: "http://localhost:3000", HotReloadable: false, EnvVar: "CORS_ALLOWED_ORIGINS", Description: "Comma-separated list of allowed CORS origins", Section: "server"},
		{Key: "server.cors_allowed_methods", Label: "CORS Allowed Methods", Type: "string", DefaultValue: "GET,POST,PUT,PATCH,DELETE,OPTIONS", HotReloadable: false, EnvVar: "CORS_ALLOWED_METHODS", Description: "Comma-separated list of allowed HTTP methods", Section: "server"},
		{Key: "server.cors_allowed_headers", Label: "CORS Allowed Headers", Type: "string", DefaultValue: "Origin,Content-Type,Accept,Authorization,X-Request-ID", HotReloadable: false, EnvVar: "CORS_ALLOWED_HEADERS", Description: "Comma-separated list of allowed request headers", Section: "server"},
		{Key: "server.tls_cert_file", Label: "TLS Certificate File", Type: "string", DefaultValue: "", HotReloadable: false, EnvVar: "SERVER_TLS_CERT_FILE", Description: "Path to TLS certificate file", Section: "server"},
		{Key: "server.tls_key_file", Label: "TLS Key File", Type: "string", DefaultValue: "", HotReloadable: false, EnvVar: "SERVER_TLS_KEY_FILE", Description: "Path to TLS private key file", Section: "server"},
		{Key: "server.tls_terminated_upstream", Label: "TLS Terminated Upstream", Type: "bool", DefaultValue: false, HotReloadable: false, EnvVar: "TLS_TERMINATED_UPSTREAM", Description: "Whether TLS is terminated by a reverse proxy upstream", Section: "server"},

		// ── tenancy ─────────────────────────────────────────────
		{Key: "tenancy.default_tenant_id", Label: "Default Tenant ID", Type: "string", DefaultValue: "11111111-1111-1111-1111-111111111111", HotReloadable: false, EnvVar: "DEFAULT_TENANT_ID", Description: "UUID of the default tenant", Section: "tenancy"},
		{Key: "tenancy.default_tenant_slug", Label: "Default Tenant Slug", Type: "string", DefaultValue: "default", HotReloadable: false, EnvVar: "DEFAULT_TENANT_SLUG", Description: "URL slug for the default tenant", Section: "tenancy"},

		// ── postgres ────────────────────────────────────────────
		{Key: "postgres.primary_host", Label: "Primary Host", Type: "string", DefaultValue: "localhost", HotReloadable: false, EnvVar: "POSTGRES_HOST", Description: "PostgreSQL primary server hostname", Section: "postgres"},
		{Key: "postgres.primary_port", Label: "Primary Port", Type: "string", DefaultValue: "5432", HotReloadable: false, EnvVar: "POSTGRES_PORT", Description: "PostgreSQL primary server port", Section: "postgres"},
		{Key: "postgres.primary_user", Label: "Primary User", Type: "string", DefaultValue: "matcher", HotReloadable: false, EnvVar: "POSTGRES_USER", Description: "PostgreSQL primary username", Section: "postgres"},
		{Key: "postgres.primary_password", Label: "Primary Password", Type: "string", DefaultValue: "", HotReloadable: false, EnvVar: "POSTGRES_PASSWORD", Description: "PostgreSQL primary password", Section: "postgres", Secret: true},
		{Key: "postgres.primary_db", Label: "Primary Database", Type: "string", DefaultValue: "matcher", HotReloadable: false, EnvVar: "POSTGRES_DB", Description: "PostgreSQL primary database name", Section: "postgres"},
		{Key: "postgres.primary_ssl_mode", Label: "Primary SSL Mode", Type: "string", DefaultValue: "disable", HotReloadable: false, EnvVar: "POSTGRES_SSLMODE", Constraints: []string{"enum:disable,require,verify-ca,verify-full"}, Description: "PostgreSQL primary SSL mode", Section: "postgres"},
		{Key: "postgres.max_open_connections", Label: "Max Open Connections", Type: "int", DefaultValue: defaultMaxOpenConns, HotReloadable: false, EnvVar: "POSTGRES_MAX_OPEN_CONNS", Constraints: []string{"min:1"}, Description: "Maximum open database connections", Section: "postgres"},
		{Key: "postgres.max_idle_connections", Label: "Max Idle Connections", Type: "int", DefaultValue: defaultMaxIdleConns, HotReloadable: false, EnvVar: "POSTGRES_MAX_IDLE_CONNS", Constraints: []string{"min:0"}, Description: "Maximum idle database connections", Section: "postgres"},
		{Key: "postgres.query_timeout_sec", Label: "Query Timeout (sec)", Type: "int", DefaultValue: defaultQueryTimeout, HotReloadable: false, EnvVar: "POSTGRES_QUERY_TIMEOUT_SEC", Constraints: []string{"min:0"}, Description: "Database query timeout in seconds", Section: "postgres"},

		// ── redis ────────────────────────────────────────────────
		{Key: "redis.host", Label: "Host", Type: "string", DefaultValue: "localhost:6379", HotReloadable: false, EnvVar: "REDIS_HOST", Description: "Redis server address", Section: "redis"},
		{Key: "redis.password", Label: "Password", Type: "string", DefaultValue: "", HotReloadable: false, EnvVar: "REDIS_PASSWORD", Description: "Redis password", Section: "redis", Secret: true},
		{Key: "redis.db", Label: "Database", Type: "int", DefaultValue: 0, HotReloadable: false, EnvVar: "REDIS_DB", Constraints: []string{"min:0"}, Description: "Redis database number", Section: "redis"},
		{Key: "redis.pool_size", Label: "Pool Size", Type: "int", DefaultValue: defaultRedisPoolSize, HotReloadable: false, EnvVar: "REDIS_POOL_SIZE", Constraints: []string{"min:1"}, Description: "Redis connection pool size", Section: "redis"},

		// ── rabbitmq ────────────────────────────────────────────
		{Key: "rabbitmq.host", Label: "Host", Type: "string", DefaultValue: "localhost", HotReloadable: false, EnvVar: "RABBITMQ_HOST", Description: "RabbitMQ server hostname", Section: "rabbitmq"},
		{Key: "rabbitmq.port", Label: "Port", Type: "string", DefaultValue: "5672", HotReloadable: false, EnvVar: "RABBITMQ_PORT", Description: "RabbitMQ server port", Section: "rabbitmq"},
		{Key: "rabbitmq.user", Label: "User", Type: "string", DefaultValue: "guest", HotReloadable: false, EnvVar: "RABBITMQ_USER", Description: "RabbitMQ username", Section: "rabbitmq"},
		{Key: "rabbitmq.password", Label: "Password", Type: "string", DefaultValue: "guest", HotReloadable: false, EnvVar: "RABBITMQ_PASSWORD", Description: "RabbitMQ password", Section: "rabbitmq", Secret: true},

		// ── auth ─────────────────────────────────────────────────
		{Key: "auth.enabled", Label: "Auth Enabled", Type: "bool", DefaultValue: false, HotReloadable: false, EnvVar: "AUTH_ENABLED", Description: "Enable JWT authentication", Section: "auth"},
		{Key: "auth.host", Label: "Auth Host", Type: "string", DefaultValue: "", HotReloadable: false, EnvVar: "AUTH_SERVICE_ADDRESS", Description: "Auth service address", Section: "auth"},
		{Key: "auth.token_secret", Label: "JWT Secret", Type: "string", DefaultValue: "", HotReloadable: false, EnvVar: "AUTH_JWT_SECRET", Description: "JWT signing secret", Section: "auth", Secret: true},

		// ── rate_limit ──────────────────────────────────────────
		{Key: "rate_limit.enabled", Label: "Rate Limit Enabled", Type: "bool", DefaultValue: true, HotReloadable: true, EnvVar: "RATE_LIMIT_ENABLED", Description: "Enable global rate limiting", Section: "rate_limit"},
		{Key: "rate_limit.max", Label: "Max Requests", Type: "int", DefaultValue: defaultRateLimitMax, HotReloadable: true, EnvVar: "RATE_LIMIT_MAX", Constraints: []string{"min:1", "max:1000000"}, Description: "Maximum requests per window", Section: "rate_limit"},
		{Key: "rate_limit.expiry_sec", Label: "Window (sec)", Type: "int", DefaultValue: defaultRateLimitExpiry, HotReloadable: true, EnvVar: "RATE_LIMIT_EXPIRY_SEC", Constraints: []string{"min:1"}, Description: "Rate limit window duration in seconds", Section: "rate_limit"},
		{Key: "rate_limit.export_max", Label: "Export Max", Type: "int", DefaultValue: defaultExportRateLimitMax, HotReloadable: true, EnvVar: "EXPORT_RATE_LIMIT_MAX", Constraints: []string{"min:1"}, Description: "Maximum export requests per window", Section: "rate_limit"},
		{Key: "rate_limit.export_expiry_sec", Label: "Export Window (sec)", Type: "int", DefaultValue: defaultExportRateLimitExp, HotReloadable: true, EnvVar: "EXPORT_RATE_LIMIT_EXPIRY_SEC", Constraints: []string{"min:1"}, Description: "Export rate limit window in seconds", Section: "rate_limit"},
		{Key: "rate_limit.dispatch_max", Label: "Dispatch Max", Type: "int", DefaultValue: defaultDispatchRateLimitMax, HotReloadable: true, EnvVar: "DISPATCH_RATE_LIMIT_MAX", Constraints: []string{"min:1"}, Description: "Maximum dispatch requests per window", Section: "rate_limit"},
		{Key: "rate_limit.dispatch_expiry_sec", Label: "Dispatch Window (sec)", Type: "int", DefaultValue: defaultDispatchRateLimitExp, HotReloadable: true, EnvVar: "DISPATCH_RATE_LIMIT_EXPIRY_SEC", Constraints: []string{"min:1"}, Description: "Dispatch rate limit window in seconds", Section: "rate_limit"},

		// ── swagger ─────────────────────────────────────────────
		{Key: "swagger.enabled", Label: "Swagger Enabled", Type: "bool", DefaultValue: false, HotReloadable: true, EnvVar: "SWAGGER_ENABLED", Description: "Enable Swagger UI (non-production only)", Section: "swagger"},

		// ── export_worker ───────────────────────────────────────
		{Key: "export_worker.enabled", Label: "Export Worker Enabled", Type: "bool", DefaultValue: true, HotReloadable: true, EnvVar: "EXPORT_WORKER_ENABLED", Description: "Enable background export worker", Section: "export_worker"},
		{Key: "export_worker.poll_interval_sec", Label: "Poll Interval (sec)", Type: "int", DefaultValue: defaultExportPollInterval, HotReloadable: true, EnvVar: "EXPORT_WORKER_POLL_INTERVAL_SEC", Constraints: []string{"min:1"}, Description: "Export worker poll interval in seconds", Section: "export_worker"},
		{Key: "export_worker.page_size", Label: "Page Size", Type: "int", DefaultValue: defaultExportPageSize, HotReloadable: true, EnvVar: "EXPORT_WORKER_PAGE_SIZE", Constraints: []string{"min:1"}, Description: "Number of records per export page", Section: "export_worker"},
		{Key: "export_worker.presign_expiry_sec", Label: "Presign Expiry (sec)", Type: "int", DefaultValue: defaultPresignExpiry, HotReloadable: true, EnvVar: "EXPORT_PRESIGN_EXPIRY_SEC", Constraints: []string{"min:60"}, Description: "Presigned URL expiry in seconds", Section: "export_worker"},

		// ── cleanup_worker ──────────────────────────────────────
		{Key: "cleanup_worker.enabled", Label: "Cleanup Worker Enabled", Type: "bool", DefaultValue: true, HotReloadable: true, EnvVar: "CLEANUP_WORKER_ENABLED", Description: "Enable background cleanup worker", Section: "cleanup_worker"},
		{Key: "cleanup_worker.interval_sec", Label: "Interval (sec)", Type: "int", DefaultValue: defaultCleanupInterval, HotReloadable: true, EnvVar: "CLEANUP_WORKER_INTERVAL_SEC", Constraints: []string{"min:60"}, Description: "Cleanup worker interval in seconds", Section: "cleanup_worker"},
		{Key: "cleanup_worker.batch_size", Label: "Batch Size", Type: "int", DefaultValue: defaultCleanupBatchSize, HotReloadable: true, EnvVar: "CLEANUP_WORKER_BATCH_SIZE", Constraints: []string{"min:1"}, Description: "Number of records per cleanup batch", Section: "cleanup_worker"},
		{Key: "cleanup_worker.grace_period_sec", Label: "Grace Period (sec)", Type: "int", DefaultValue: defaultCleanupGracePeriod, HotReloadable: true, EnvVar: "CLEANUP_WORKER_GRACE_PERIOD_SEC", Constraints: []string{"min:60"}, Description: "Grace period before cleanup in seconds", Section: "cleanup_worker"},

		// ── scheduler ───────────────────────────────────────────
		{Key: "scheduler.interval_sec", Label: "Scheduler Interval (sec)", Type: "int", DefaultValue: defaultSchedulerInterval, HotReloadable: true, EnvVar: "SCHEDULER_INTERVAL_SEC", Constraints: []string{"min:1"}, Description: "Scheduler poll interval in seconds", Section: "scheduler"},

		// ── webhook ─────────────────────────────────────────────
		{Key: "webhook.timeout_sec", Label: "Webhook Timeout (sec)", Type: "int", DefaultValue: defaultWebhookTimeout, HotReloadable: true, EnvVar: "WEBHOOK_TIMEOUT_SEC", Constraints: []string{"min:1"}, Description: "HTTP timeout for webhook dispatches in seconds", Section: "webhook"},

		// ── callback_rate_limit ──────────────────────────────────
		{Key: "callback_rate_limit.per_minute", Label: "Callbacks Per Minute", Type: "int", DefaultValue: defaultCallbackRatePerMin, HotReloadable: true, EnvVar: "CALLBACK_RATE_LIMIT_PER_MIN", Constraints: []string{"min:1"}, Description: "Max callbacks per external system per minute", Section: "callback_rate_limit"},

		// ── deduplication ────────────────────────────────────────
		{Key: "deduplication.ttl_sec", Label: "Dedupe TTL (sec)", Type: "int", DefaultValue: defaultDedupeTTL, HotReloadable: true, EnvVar: "DEDUPE_TTL_SEC", Constraints: []string{"min:1"}, Description: "Deduplication key TTL in seconds", Section: "deduplication"},

		// ── idempotency ─────────────────────────────────────────
		{Key: "idempotency.retry_window_sec", Label: "Retry Window (sec)", Type: "int", DefaultValue: defaultIdempotencyRetryWindow, HotReloadable: true, EnvVar: "IDEMPOTENCY_RETRY_WINDOW_SEC", Constraints: []string{"min:1"}, Description: "Failed idempotency key retry window in seconds", Section: "idempotency"},
		{Key: "idempotency.success_ttl_hours", Label: "Success TTL (hours)", Type: "int", DefaultValue: defaultIdempotencySuccessTTL, HotReloadable: true, EnvVar: "IDEMPOTENCY_SUCCESS_TTL_HOURS", Constraints: []string{"min:1"}, Description: "Completed idempotency key cache duration in hours", Section: "idempotency"},
		{Key: "idempotency.hmac_secret", Label: "HMAC Secret", Type: "string", DefaultValue: "", HotReloadable: false, EnvVar: "IDEMPOTENCY_HMAC_SECRET", Description: "Server-side HMAC secret for idempotency key signing", Section: "idempotency", Secret: true},

		// ── fetcher ─────────────────────────────────────────────
		{Key: "fetcher.enabled", Label: "Fetcher Enabled", Type: "bool", DefaultValue: false, HotReloadable: true, EnvVar: "FETCHER_ENABLED", Description: "Enable Fetcher service integration", Section: "fetcher"},
		{Key: "fetcher.health_timeout_sec", Label: "Health Timeout (sec)", Type: "int", DefaultValue: defaultFetcherHealthTimeout, HotReloadable: true, EnvVar: "FETCHER_HEALTH_TIMEOUT_SEC", Constraints: []string{"min:1"}, Description: "Fetcher health check timeout in seconds", Section: "fetcher"},
		{Key: "fetcher.request_timeout_sec", Label: "Request Timeout (sec)", Type: "int", DefaultValue: defaultFetcherRequestTimeout, HotReloadable: true, EnvVar: "FETCHER_REQUEST_TIMEOUT_SEC", Constraints: []string{"min:1"}, Description: "Fetcher HTTP request timeout in seconds", Section: "fetcher"},
		{Key: "fetcher.discovery_interval_sec", Label: "Discovery Interval (sec)", Type: "int", DefaultValue: defaultFetcherDiscoveryInt, HotReloadable: true, EnvVar: "FETCHER_DISCOVERY_INTERVAL_SEC", Constraints: []string{"min:1"}, Description: "Fetcher schema discovery interval in seconds", Section: "fetcher"},
		{Key: "fetcher.schema_cache_ttl_sec", Label: "Schema Cache TTL (sec)", Type: "int", DefaultValue: defaultFetcherSchemaCacheTTL, HotReloadable: true, EnvVar: "FETCHER_SCHEMA_CACHE_TTL_SEC", Constraints: []string{"min:1"}, Description: "Fetcher schema cache TTL in seconds", Section: "fetcher"},
		{Key: "fetcher.extraction_poll_sec", Label: "Extraction Poll (sec)", Type: "int", DefaultValue: defaultFetcherExtractionPoll, HotReloadable: true, EnvVar: "FETCHER_EXTRACTION_POLL_INTERVAL_SEC", Constraints: []string{"min:1"}, Description: "Fetcher extraction poll interval in seconds", Section: "fetcher"},
		{Key: "fetcher.extraction_timeout_sec", Label: "Extraction Timeout (sec)", Type: "int", DefaultValue: defaultFetcherExtractionTimout, HotReloadable: true, EnvVar: "FETCHER_EXTRACTION_TIMEOUT_SEC", Constraints: []string{"min:1"}, Description: "Fetcher extraction timeout in seconds", Section: "fetcher"},

		// ── archival ────────────────────────────────────────────
		{Key: "archival.enabled", Label: "Archival Enabled", Type: "bool", DefaultValue: false, HotReloadable: true, EnvVar: "ARCHIVAL_WORKER_ENABLED", Description: "Enable audit log archival worker", Section: "archival"},
		{Key: "archival.interval_hours", Label: "Interval (hours)", Type: "int", DefaultValue: defaultArchivalIntervalHours, HotReloadable: true, EnvVar: "ARCHIVAL_WORKER_INTERVAL_HOURS", Constraints: []string{"min:1"}, Description: "Archival worker run interval in hours", Section: "archival"},
		{Key: "archival.batch_size", Label: "Batch Size", Type: "int", DefaultValue: defaultArchivalBatchSize, HotReloadable: true, EnvVar: "ARCHIVAL_BATCH_SIZE", Constraints: []string{"min:1"}, Description: "Records per archival batch", Section: "archival"},
	}
}

// isEnvOverridden returns true if the given env var is set in the process environment.
// It checks both legacy keys (e.g. LOG_LEVEL) and MATCHER-prefixed keys
// (e.g. MATCHER_APP_LOG_LEVEL) used by Viper.
func isEnvOverridden(envVar, key string) bool {
	if envVar == "" && key == "" {
		return false
	}

	if envVar != "" {
		if _, exists := os.LookupEnv(envVar); exists {
			return true
		}
	}

	if key == "" {
		return false
	}

	prefixedKey := "MATCHER_" + strings.ToUpper(strings.ReplaceAll(key, ".", "_"))
	_, exists := os.LookupEnv(prefixedKey)

	return exists
}

// buildSchemaResponse builds a ConfigSchemaResponse from the static schema
// and the current effective config snapshot (via viper for value resolution).
func buildSchemaResponse(cm *ConfigManager) ConfigSchemaResponse {
	defs := buildConfigSchema()
	sections := make(map[string][]ConfigFieldSchema, len(defs))

	for _, def := range defs {
		currentVal := resolveCurrentValue(cm, def)

		field := ConfigFieldSchema{
			Key:           def.Key,
			Label:         def.Label,
			Type:          def.Type,
			DefaultValue:  def.DefaultValue,
			CurrentValue:  currentVal,
			HotReloadable: def.HotReloadable,
			EnvOverride:   isEnvOverridden(def.EnvVar, def.Key),
			EnvVar:        def.EnvVar,
			Constraints:   def.Constraints,
			Description:   def.Description,
			Section:       def.Section,
		}

		sections[def.Section] = append(sections[def.Section], field)
	}

	return ConfigSchemaResponse{
		Sections:    sections,
		TotalFields: len(defs),
	}
}

// resolveCurrentValue reads the current value from viper (which merges YAML + env).
// Secret fields are redacted.
func resolveCurrentValue(cm *ConfigManager, def configFieldDef) any {
	if def.Secret {
		return redactedValue
	}

	if val, ok := resolveCurrentConfigValue(cm, def.Key); ok {
		return val
	}

	return def.DefaultValue
}

// buildRedactedConfig builds a map representation of the current config
// with secrets replaced by redactedValue.
func buildRedactedConfig(cm *ConfigManager) map[string]any {
	defs := buildConfigSchema()
	result := make(map[string]any, len(defs))

	for _, def := range defs {
		if def.Secret {
			result[def.Key] = redactedValue

			continue
		}

		if val, ok := resolveCurrentConfigValue(cm, def.Key); ok {
			result[def.Key] = val

			continue
		}

		result[def.Key] = def.DefaultValue
	}

	return result
}

// buildEnvOverridesList returns the list of config keys currently overridden by env vars.
func buildEnvOverridesList() []string {
	defs := buildConfigSchema()
	overrides := make([]string, 0)

	for _, def := range defs {
		if isEnvOverridden(def.EnvVar, def.Key) {
			overrides = append(overrides, def.Key)
		}
	}

	return overrides
}

func resolveConfigValue(cfg *Config, key string) (any, bool) {
	if cfg == nil || strings.TrimSpace(key) == "" {
		return nil, false
	}

	parts := strings.Split(key, ".")
	current, ok := derefPointerValue(reflect.ValueOf(cfg))

	if !ok {
		return nil, false
	}

	for idx, part := range parts {
		if current.Kind() != reflect.Struct {
			return nil, false
		}

		next, found := findMapstructureField(current, part)
		if !found {
			return nil, false
		}

		if idx == len(parts)-1 {
			return next.Interface(), true
		}

		current, ok = derefPointerValue(next)
		if !ok {
			return nil, false
		}
	}

	return nil, false
}

func resolveCurrentConfigValue(cm *ConfigManager, key string) (any, bool) {
	if cm == nil {
		return nil, false
	}

	if cfg := cm.Get(); cfg != nil {
		if val, ok := resolveConfigValue(cfg, key); ok {
			return val, true
		}
	}

	if cm.viper == nil {
		return nil, false
	}

	val := cm.viper.Get(key)
	if val == nil {
		return nil, false
	}

	return val, true
}

func derefPointerValue(value reflect.Value) (reflect.Value, bool) {
	if value.Kind() != reflect.Pointer {
		return value, true
	}

	if value.IsNil() {
		return reflect.Value{}, false
	}

	return value.Elem(), true
}

func findMapstructureField(current reflect.Value, part string) (reflect.Value, bool) {
	currentType := current.Type()

	for i := range currentType.NumField() {
		field := currentType.Field(i)
		if !field.IsExported() {
			continue
		}

		if field.Tag.Get("mapstructure") != part {
			continue
		}

		return current.Field(i), true
	}

	return reflect.Value{}, false
}
