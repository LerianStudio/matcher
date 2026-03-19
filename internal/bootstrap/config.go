// Copyright 2025 Lerian Studio. All rights reserved.
// Use of this source code is governed by an Elastic License 2.0
// that can be found in the LICENSE.md file.

// Package bootstrap provides application initialization and configuration
// for the Matcher service, including server setup, database connections,
// and observability infrastructure.
package bootstrap

import (
	"errors"
	"time"

	libLog "github.com/LerianStudio/lib-commons/v4/commons/log"
)

const (
	// envProduction is the production environment name.
	envProduction = "production"

	defaultHTTPBodyLimitBytes = 104857600

	// defaultExportWorkerPollIntervalSec is the default poll interval for export worker.
	defaultExportWorkerPollIntervalSec = 5

	// defaultQueryTimeoutSec is the default timeout for database query operations.
	// Applied when POSTGRES_QUERY_TIMEOUT_SEC is not set or is non-positive.
	defaultQueryTimeoutSec = 30

	// maxInfraConnectTimeoutSec caps infrastructure startup timeout to prevent
	// pathological values from causing startup hangs.
	maxInfraConnectTimeoutSec = 300
)

// ErrConfigNil indicates a nil configuration struct was provided.
var ErrConfigNil = errors.New("config must be provided")

// AppConfig holds core application metadata.
type AppConfig struct {
	EnvName  string `env:"ENV_NAME"  envDefault:"development" mapstructure:"env_name"`
	LogLevel string `env:"LOG_LEVEL" envDefault:"info"        mapstructure:"log_level"`
}

// ServerConfig configures the HTTP server and middleware.
type ServerConfig struct {
	Address               string `env:"SERVER_ADDRESS"          envDefault:":4018"                                                  mapstructure:"address"`
	BodyLimitBytes        int    `env:"HTTP_BODY_LIMIT_BYTES"   envDefault:"104857600"                                              mapstructure:"body_limit_bytes"`
	CORSAllowedOrigins    string `env:"CORS_ALLOWED_ORIGINS"    envDefault:"http://localhost:3000"                                  mapstructure:"cors_allowed_origins"`
	CORSAllowedMethods    string `env:"CORS_ALLOWED_METHODS"    envDefault:"GET,POST,PUT,PATCH,DELETE,OPTIONS"                      mapstructure:"cors_allowed_methods"`
	CORSAllowedHeaders    string `env:"CORS_ALLOWED_HEADERS"    envDefault:"Origin,Content-Type,Accept,Authorization,X-Request-ID"  mapstructure:"cors_allowed_headers"`
	TLSCertFile           string `env:"SERVER_TLS_CERT_FILE"                                                                        mapstructure:"tls_cert_file"`
	TLSKeyFile            string `env:"SERVER_TLS_KEY_FILE"                                                                         mapstructure:"tls_key_file"`
	TLSTerminatedUpstream bool   `env:"TLS_TERMINATED_UPSTREAM" envDefault:"false"                                                  mapstructure:"tls_terminated_upstream"`
	TrustedProxies        string `env:"TRUSTED_PROXIES"                                                                             mapstructure:"trusted_proxies"`
}

// TenancyConfig configures tenant defaults and multi-tenant infrastructure.
type TenancyConfig struct {
	DefaultTenantID   string `env:"DEFAULT_TENANT_ID"   envDefault:"11111111-1111-1111-1111-111111111111" mapstructure:"default_tenant_id"`
	DefaultTenantSlug string `env:"DEFAULT_TENANT_SLUG" envDefault:"default"                             mapstructure:"default_tenant_slug"`

	MultiTenantEnabled                  bool   `env:"MULTI_TENANT_ENABLED"                         envDefault:"false"   mapstructure:"multi_tenant_enabled"`
	MultiTenantURL                      string `env:"MULTI_TENANT_URL"                                                     mapstructure:"multi_tenant_url"`
	MultiTenantEnvironment              string `env:"MULTI_TENANT_ENVIRONMENT"                                             mapstructure:"multi_tenant_environment"`
	MultiTenantMaxTenantPools           int    `env:"MULTI_TENANT_MAX_TENANT_POOLS"                envDefault:"100"     mapstructure:"multi_tenant_max_tenant_pools"`
	MultiTenantIdleTimeoutSec           int    `env:"MULTI_TENANT_IDLE_TIMEOUT_SEC"                envDefault:"300"     mapstructure:"multi_tenant_idle_timeout_sec"`
	MultiTenantCircuitBreakerThreshold  int    `env:"MULTI_TENANT_CIRCUIT_BREAKER_THRESHOLD"       envDefault:"5"       mapstructure:"multi_tenant_circuit_breaker_threshold"`
	MultiTenantCircuitBreakerTimeoutSec int    `env:"MULTI_TENANT_CIRCUIT_BREAKER_TIMEOUT_SEC"     envDefault:"30"      mapstructure:"multi_tenant_circuit_breaker_timeout_sec"`
	MultiTenantServiceAPIKey            string `env:"MULTI_TENANT_SERVICE_API_KEY"                                         mapstructure:"multi_tenant_service_api_key"`

	// MultiTenantInfraEnabled is a deprecated backward-compatible alias for the
	// previous matcher-specific flag. The primary control surface is now
	// MultiTenantEnabled. Keep this field until callers fully migrate.
	MultiTenantInfraEnabled bool `env:"MULTI_TENANT_INFRA_ENABLED" envDefault:"false" mapstructure:"multi_tenant_infra_enabled"`
}

// PostgresConfig configures primary/replica connections and pooling.
type PostgresConfig struct {
	PrimaryHost     string `env:"POSTGRES_HOST"     envDefault:"localhost" mapstructure:"primary_host"`
	PrimaryPort     string `env:"POSTGRES_PORT"     envDefault:"5432"      mapstructure:"primary_port"`
	PrimaryUser     string `env:"POSTGRES_USER"     envDefault:"matcher"   mapstructure:"primary_user"`
	PrimaryPassword string `env:"POSTGRES_PASSWORD" envDefault:"matcher_dev_password" mapstructure:"primary_password"`
	PrimaryDB       string `env:"POSTGRES_DB"       envDefault:"matcher"   mapstructure:"primary_db"`
	PrimarySSLMode  string `env:"POSTGRES_SSLMODE"  envDefault:"disable"   mapstructure:"primary_ssl_mode"`

	ReplicaHost     string `env:"POSTGRES_REPLICA_HOST"                    mapstructure:"replica_host"`
	ReplicaPort     string `env:"POSTGRES_REPLICA_PORT"                    mapstructure:"replica_port"`
	ReplicaUser     string `env:"POSTGRES_REPLICA_USER"                    mapstructure:"replica_user"`
	ReplicaPassword string `env:"POSTGRES_REPLICA_PASSWORD"                mapstructure:"replica_password"`
	ReplicaDB       string `env:"POSTGRES_REPLICA_DB"                      mapstructure:"replica_db"`
	ReplicaSSLMode  string `env:"POSTGRES_REPLICA_SSLMODE"                 mapstructure:"replica_ssl_mode"`

	MaxOpenConnections  int    `env:"POSTGRES_MAX_OPEN_CONNS"          envDefault:"25"         mapstructure:"max_open_connections"`
	MaxIdleConnections  int    `env:"POSTGRES_MAX_IDLE_CONNS"          envDefault:"5"          mapstructure:"max_idle_connections"`
	ConnMaxLifetimeMins int    `env:"POSTGRES_CONN_MAX_LIFETIME_MINS"  envDefault:"30"         mapstructure:"conn_max_lifetime_mins"`
	ConnMaxIdleTimeMins int    `env:"POSTGRES_CONN_MAX_IDLE_TIME_MINS" envDefault:"5"          mapstructure:"conn_max_idle_time_mins"`
	ConnectTimeoutSec   int    `env:"POSTGRES_CONNECT_TIMEOUT_SEC"     envDefault:"10"         mapstructure:"connect_timeout_sec"`
	QueryTimeoutSec     int    `env:"POSTGRES_QUERY_TIMEOUT_SEC"       envDefault:"30"         mapstructure:"query_timeout_sec"`
	MigrationsPath      string `env:"MIGRATIONS_PATH"                  envDefault:"migrations" mapstructure:"migrations_path"`
}

// RedisConfig configures Redis connections.
type RedisConfig struct {
	Host           string `env:"REDIS_HOST"             envDefault:"localhost:6379" mapstructure:"host"`
	MasterName     string `env:"REDIS_MASTER_NAME"                                 mapstructure:"master_name"`
	Password       string `env:"REDIS_PASSWORD" json:"-"                           mapstructure:"password"`
	DB             int    `env:"REDIS_DB"               envDefault:"0"             mapstructure:"db"`
	Protocol       int    `env:"REDIS_PROTOCOL"         envDefault:"3"             mapstructure:"protocol"`
	TLS            bool   `env:"REDIS_TLS"              envDefault:"false"         mapstructure:"tls"`
	CACert         string `env:"REDIS_CA_CERT"                                     mapstructure:"ca_cert"`
	PoolSize       int    `env:"REDIS_POOL_SIZE"        envDefault:"10"            mapstructure:"pool_size"`
	MinIdleConn    int    `env:"REDIS_MIN_IDLE_CONNS"   envDefault:"2"             mapstructure:"min_idle_conn"`
	ReadTimeoutMs  int    `env:"REDIS_READ_TIMEOUT_MS"  envDefault:"3000"          mapstructure:"read_timeout_ms"`
	WriteTimeoutMs int    `env:"REDIS_WRITE_TIMEOUT_MS" envDefault:"3000"          mapstructure:"write_timeout_ms"`
	DialTimeoutMs  int    `env:"REDIS_DIAL_TIMEOUT_MS"  envDefault:"5000"          mapstructure:"dial_timeout_ms"`
}

// RabbitMQConfig configures RabbitMQ connection settings.
type RabbitMQConfig struct {
	URI                      string `env:"RABBITMQ_URI"                         envDefault:"amqp"                mapstructure:"uri"`
	Host                     string `env:"RABBITMQ_HOST"                        envDefault:"localhost"           mapstructure:"host"`
	Port                     string `env:"RABBITMQ_PORT"                        envDefault:"5672"                mapstructure:"port"`
	User                     string `env:"RABBITMQ_USER"                        envDefault:"matcher_admin"       mapstructure:"user"`
	Password                 string `env:"RABBITMQ_PASSWORD"                    envDefault:"matcher_dev_password" json:"-" mapstructure:"password"`
	VHost                    string `env:"RABBITMQ_VHOST"                       envDefault:"/"                   mapstructure:"vhost"`
	HealthURL                string `env:"RABBITMQ_HEALTH_URL"                  envDefault:"http://localhost:15672" mapstructure:"health_url"`
	AllowInsecureHealthCheck bool   `env:"RABBITMQ_ALLOW_INSECURE_HEALTH_CHECK" envDefault:"false"               mapstructure:"allow_insecure_health_check"`
}

// AuthConfig configures authentication and authorization.
type AuthConfig struct {
	Enabled     bool   `env:"AUTH_ENABLED"         envDefault:"false" mapstructure:"enabled"`
	Host        string `env:"AUTH_SERVICE_ADDRESS"                    mapstructure:"host"`
	TokenSecret string `env:"AUTH_JWT_SECRET"                         mapstructure:"token_secret"`
}

// SwaggerConfig toggles Swagger UI exposure.
// Host overrides the Swagger spec's "host" field at runtime.
// When empty, the spec defaults to the request's own host (ideal for most deployments).
// Set SWAGGER_HOST to an explicit value (e.g., "api.example.com") if the spec
// must advertise a fixed hostname (useful behind reverse proxies / API gateways).
// Schemes overrides the Swagger spec's "schemes" list at runtime.
// Accepts a comma-separated list (e.g., "https" or "http,https").
// Defaults to "https" so the generated spec never advertises plain HTTP
// unless explicitly opted in (e.g., SWAGGER_SCHEMES=http for local development).
type SwaggerConfig struct {
	Enabled bool   `env:"SWAGGER_ENABLED" envDefault:"false"  mapstructure:"enabled"`
	Host    string `env:"SWAGGER_HOST"    envDefault:""       mapstructure:"host"`
	Schemes string `env:"SWAGGER_SCHEMES" envDefault:"https"  mapstructure:"schemes"`
}

// TelemetryConfig configures OpenTelemetry settings.
type TelemetryConfig struct {
	Enabled           bool   `env:"ENABLE_TELEMETRY"                     envDefault:"false"                          mapstructure:"enabled"`
	ServiceName       string `env:"OTEL_RESOURCE_SERVICE_NAME"           envDefault:"matcher"                        mapstructure:"service_name"`
	LibraryName       string `env:"OTEL_LIBRARY_NAME"                    envDefault:"github.com/LerianStudio/matcher" mapstructure:"library_name"`
	ServiceVersion    string `env:"OTEL_RESOURCE_SERVICE_VERSION"        envDefault:"1.0.0"                          mapstructure:"service_version"`
	DeploymentEnv     string `env:"OTEL_RESOURCE_DEPLOYMENT_ENVIRONMENT" envDefault:"development"                    mapstructure:"deployment_env"`
	CollectorEndpoint string `env:"OTEL_EXPORTER_OTLP_ENDPOINT"          envDefault:"localhost:4317"                 mapstructure:"collector_endpoint"`
	// DBMetricsIntervalSec configures how often database connection pool metrics
	// are collected and reported to OpenTelemetry. Lower values provide more
	// granular monitoring but increase overhead. Default: 15 seconds.
	DBMetricsIntervalSec int `env:"DB_METRICS_INTERVAL_SEC"              envDefault:"15"                             mapstructure:"db_metrics_interval_sec"`
}

// RateLimitConfig configures global and export rate limiting.
type RateLimitConfig struct {
	Enabled           bool `env:"RATE_LIMIT_ENABLED"             envDefault:"true" mapstructure:"enabled"`
	Max               int  `env:"RATE_LIMIT_MAX"                 envDefault:"100"  mapstructure:"max"`
	ExpirySec         int  `env:"RATE_LIMIT_EXPIRY_SEC"          envDefault:"60"   mapstructure:"expiry_sec"`
	ExportMax         int  `env:"EXPORT_RATE_LIMIT_MAX"          envDefault:"10"   mapstructure:"export_max"`
	ExportExpirySec   int  `env:"EXPORT_RATE_LIMIT_EXPIRY_SEC"   envDefault:"60"   mapstructure:"export_expiry_sec"`
	DispatchMax       int  `env:"DISPATCH_RATE_LIMIT_MAX"        envDefault:"50"   mapstructure:"dispatch_max"`
	DispatchExpirySec int  `env:"DISPATCH_RATE_LIMIT_EXPIRY_SEC" envDefault:"60"   mapstructure:"dispatch_expiry_sec"`
}

// InfrastructureConfig configures infrastructure-level behavior.
type InfrastructureConfig struct {
	ConnectTimeoutSec     int `env:"INFRA_CONNECT_TIMEOUT_SEC"  envDefault:"30" mapstructure:"connect_timeout_sec"`
	HealthCheckTimeoutSec int `env:"HEALTH_CHECK_TIMEOUT_SEC"   envDefault:"5"  mapstructure:"health_check_timeout_sec"`
}

// IdempotencyConfig configures idempotency behavior.
type IdempotencyConfig struct {
	// RetryWindowSec configures how long failed idempotency keys remain blocked before allowing retry.
	// Default: 300 seconds (5 minutes).
	RetryWindowSec int `env:"IDEMPOTENCY_RETRY_WINDOW_SEC" envDefault:"300" mapstructure:"retry_window_sec"`

	// SuccessTTLHours configures how long completed idempotency keys remain cached
	// before expiring. This determines the window during which duplicate requests
	// return the cached response. Default: 168 hours (7 days).
	SuccessTTLHours int `env:"IDEMPOTENCY_SUCCESS_TTL_HOURS" envDefault:"168" mapstructure:"success_ttl_hours"`

	// HMACSecret is the server-side secret used to HMAC-sign client-provided
	// idempotency keys before storing them in Redis. This prevents key prediction
	// attacks by making stored keys unpredictable even if the client key format
	// is known. When empty, keys are stored unsigned (backward compatible).
	// SECURITY: Use a strong, random secret in production (minimum 32 bytes).
	HMACSecret string `env:"IDEMPOTENCY_HMAC_SECRET" mapstructure:"hmac_secret"`
}

// CallbackRateLimitConfig configures callback-specific rate limiting.
type CallbackRateLimitConfig struct {
	// PerMinute is the maximum number of callbacks allowed per external system
	// per minute. Each external system (JIRA, WEBHOOK, SERVICENOW, etc.) gets
	// its own independent rate limit budget. Default: 60 per minute.
	PerMinute int `env:"CALLBACK_RATE_LIMIT_PER_MIN" envDefault:"60" mapstructure:"per_minute"`
}

// FetcherConfig configures the optional Fetcher-backed discovery module.
type FetcherConfig struct {
	Enabled              bool   `env:"FETCHER_ENABLED"                envDefault:"false"                 mapstructure:"enabled"`
	URL                  string `env:"FETCHER_URL"                    envDefault:"http://localhost:4006" mapstructure:"url"`
	AllowPrivateIPs      bool   `env:"FETCHER_ALLOW_PRIVATE_IPS"      envDefault:"false"                 mapstructure:"allow_private_ips"`
	HealthTimeoutSec     int    `env:"FETCHER_HEALTH_TIMEOUT_SEC"     envDefault:"5"                     mapstructure:"health_timeout_sec"`
	RequestTimeoutSec    int    `env:"FETCHER_REQUEST_TIMEOUT_SEC"    envDefault:"30"                    mapstructure:"request_timeout_sec"`
	DiscoveryIntervalSec int    `env:"FETCHER_DISCOVERY_INTERVAL_SEC" envDefault:"60"                    mapstructure:"discovery_interval_sec"`
	SchemaCacheTTLSec    int    `env:"FETCHER_SCHEMA_CACHE_TTL_SEC"   envDefault:"300"                   mapstructure:"schema_cache_ttl_sec"`
	ExtractionPollSec    int    `env:"FETCHER_EXTRACTION_POLL_INTERVAL_SEC" envDefault:"5"              mapstructure:"extraction_poll_sec"`
	ExtractionTimeoutSec int    `env:"FETCHER_EXTRACTION_TIMEOUT_SEC" envDefault:"600"                   mapstructure:"extraction_timeout_sec"`
}

// DedupeConfig configures deduplication behavior.
type DedupeConfig struct {
	// TTLSec configures the TTL for deduplication keys in Redis. Default: 3600 seconds.
	TTLSec int `env:"DEDUPE_TTL_SEC" envDefault:"3600" mapstructure:"ttl_sec"`
}

// ObjectStorageConfig configures object storage (S3-compatible) settings.
type ObjectStorageConfig struct {
	Endpoint        string `env:"OBJECT_STORAGE_ENDPOINT"          envDefault:"http://localhost:8333" mapstructure:"endpoint"`
	Region          string `env:"OBJECT_STORAGE_REGION"            envDefault:"us-east-1"            mapstructure:"region"`
	Bucket          string `env:"OBJECT_STORAGE_BUCKET"            envDefault:"matcher-exports"      mapstructure:"bucket"`
	AccessKeyID     string `env:"OBJECT_STORAGE_ACCESS_KEY_ID"                                       mapstructure:"access_key_id"`
	SecretAccessKey string `env:"OBJECT_STORAGE_SECRET_ACCESS_KEY"                                   mapstructure:"secret_access_key"`
	UsePathStyle    bool   `env:"OBJECT_STORAGE_USE_PATH_STYLE"    envDefault:"true"                 mapstructure:"use_path_style"`
}

// ExportWorkerConfig configures reporting export workers.
type ExportWorkerConfig struct {
	Enabled          bool `env:"EXPORT_WORKER_ENABLED"           envDefault:"true" mapstructure:"enabled"`
	PollIntervalSec  int  `env:"EXPORT_WORKER_POLL_INTERVAL_SEC" envDefault:"5"    mapstructure:"poll_interval_sec"`
	PageSize         int  `env:"EXPORT_WORKER_PAGE_SIZE"         envDefault:"1000" mapstructure:"page_size"`
	PresignExpirySec int  `env:"EXPORT_PRESIGN_EXPIRY_SEC"       envDefault:"3600" mapstructure:"presign_expiry_sec"`
}

// WebhookConfig configures default webhook dispatch settings.
type WebhookConfig struct {
	// TimeoutSec configures the default HTTP timeout for webhook dispatches.
	// Individual webhooks may override this via the admin API in the future.
	// Default: 30 seconds.
	TimeoutSec int `env:"WEBHOOK_TIMEOUT_SEC" envDefault:"30" mapstructure:"timeout_sec"`
}

// CleanupWorkerConfig configures background cleanup workers.
type CleanupWorkerConfig struct {
	Enabled        bool `env:"CLEANUP_WORKER_ENABLED"          envDefault:"true" mapstructure:"enabled"`
	IntervalSec    int  `env:"CLEANUP_WORKER_INTERVAL_SEC"     envDefault:"3600" mapstructure:"interval_sec"`
	BatchSize      int  `env:"CLEANUP_WORKER_BATCH_SIZE"       envDefault:"100"  mapstructure:"batch_size"`
	GracePeriodSec int  `env:"CLEANUP_WORKER_GRACE_PERIOD_SEC" envDefault:"3600" mapstructure:"grace_period_sec"`
}

// SchedulerConfig configures the cron-based scheduler worker.
type SchedulerConfig struct {
	IntervalSec int `env:"SCHEDULER_INTERVAL_SEC" envDefault:"60" mapstructure:"interval_sec"`
}

// ArchivalConfig configures the audit log archival worker.
type ArchivalConfig struct {
	Enabled             bool   `env:"ARCHIVAL_WORKER_ENABLED"        envDefault:"false"               mapstructure:"enabled"`
	IntervalHours       int    `env:"ARCHIVAL_WORKER_INTERVAL_HOURS" envDefault:"24"                  mapstructure:"interval_hours"`
	HotRetentionDays    int    `env:"ARCHIVAL_HOT_RETENTION_DAYS"    envDefault:"90"                  mapstructure:"hot_retention_days"`
	WarmRetentionMonths int    `env:"ARCHIVAL_WARM_RETENTION_MONTHS" envDefault:"24"                  mapstructure:"warm_retention_months"`
	ColdRetentionMonths int    `env:"ARCHIVAL_COLD_RETENTION_MONTHS" envDefault:"84"                  mapstructure:"cold_retention_months"`
	BatchSize           int    `env:"ARCHIVAL_BATCH_SIZE"            envDefault:"5000"                mapstructure:"batch_size"`
	StorageBucket       string `env:"ARCHIVAL_STORAGE_BUCKET"                                         mapstructure:"storage_bucket"`
	StoragePrefix       string `env:"ARCHIVAL_STORAGE_PREFIX"        envDefault:"archives/audit-logs" mapstructure:"storage_prefix"`
	StorageClass        string `env:"ARCHIVAL_STORAGE_CLASS"         envDefault:"GLACIER"             mapstructure:"storage_class"`
	PartitionLookahead  int    `env:"ARCHIVAL_PARTITION_LOOKAHEAD"   envDefault:"3"                   mapstructure:"partition_lookahead"`
	PresignExpirySec    int    `env:"ARCHIVAL_PRESIGN_EXPIRY_SEC"    envDefault:"3600"                mapstructure:"presign_expiry_sec"`
}

// Config holds all configuration values for the Matcher service,
// including server, database, cache, messaging, and observability settings.
type Config struct {
	App               AppConfig               `mapstructure:"app"`
	Server            ServerConfig            `mapstructure:"server"`
	Tenancy           TenancyConfig           `mapstructure:"tenancy"`
	Postgres          PostgresConfig          `mapstructure:"postgres"`
	Redis             RedisConfig             `mapstructure:"redis"`
	RabbitMQ          RabbitMQConfig          `mapstructure:"rabbitmq"`
	Auth              AuthConfig              `mapstructure:"auth"`
	Swagger           SwaggerConfig           `mapstructure:"swagger"`
	Telemetry         TelemetryConfig         `mapstructure:"telemetry"`
	RateLimit         RateLimitConfig         `mapstructure:"rate_limit"`
	Infrastructure    InfrastructureConfig    `mapstructure:"infrastructure"`
	Idempotency       IdempotencyConfig       `mapstructure:"idempotency"`
	Dedupe            DedupeConfig            `mapstructure:"deduplication"`
	ObjectStorage     ObjectStorageConfig     `mapstructure:"object_storage"`
	ExportWorker      ExportWorkerConfig      `mapstructure:"export_worker"`
	CleanupWorker     CleanupWorkerConfig     `mapstructure:"cleanup_worker"`
	Scheduler         SchedulerConfig         `mapstructure:"scheduler"`
	Archival          ArchivalConfig          `mapstructure:"archival"`
	Webhook           WebhookConfig           `mapstructure:"webhook"`
	CallbackRateLimit CallbackRateLimitConfig `mapstructure:"callback_rate_limit"`
	Fetcher           FetcherConfig           `mapstructure:"fetcher"`

	// ShutdownGracePeriod is the time to wait for background workers to finish
	// after requesting stop, before closing infrastructure connections.
	// Zero means use the default (5 seconds).
	ShutdownGracePeriod time.Duration `mapstructure:"-"`

	// Logger is used for runtime warnings (e.g., capping invalid config values).
	// Set during LoadConfigWithLogger; may be nil if LoadConfig was used.
	Logger libLog.Logger `mapstructure:"-"`
}

// Options provides optional configuration overrides for server initialization.
type Options struct {
	Logger libLog.Logger
}
