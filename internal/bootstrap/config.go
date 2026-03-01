// Package bootstrap provides application initialization and configuration
// for the Matcher service, including server setup, database connections,
// and observability infrastructure.
package bootstrap

import (
	"context"
	"errors"
	"fmt"
	"net"
	"net/url"
	"strings"
	"time"

	libCommons "github.com/LerianStudio/lib-uncommons/v2/uncommons"
	"github.com/LerianStudio/lib-uncommons/v2/uncommons/assert"
	libLog "github.com/LerianStudio/lib-uncommons/v2/uncommons/log"
	libZap "github.com/LerianStudio/lib-uncommons/v2/uncommons/zap"

	"github.com/LerianStudio/matcher/internal/shared/constants"
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
	EnvName  string `env:"ENV_NAME"  envDefault:"development"`
	LogLevel string `env:"LOG_LEVEL" envDefault:"info"`
}

// ServerConfig configures the HTTP server and middleware.
type ServerConfig struct {
	Address               string `env:"SERVER_ADDRESS"          envDefault:":4018"`
	BodyLimitBytes        int    `env:"HTTP_BODY_LIMIT_BYTES"   envDefault:"104857600"`
	CORSAllowedOrigins    string `env:"CORS_ALLOWED_ORIGINS"    envDefault:"http://localhost:3000"`
	CORSAllowedMethods    string `env:"CORS_ALLOWED_METHODS"    envDefault:"GET,POST,PUT,PATCH,DELETE,OPTIONS"`
	CORSAllowedHeaders    string `env:"CORS_ALLOWED_HEADERS"    envDefault:"Origin,Content-Type,Accept,Authorization,X-Request-ID"`
	TLSCertFile           string `env:"SERVER_TLS_CERT_FILE"`
	TLSKeyFile            string `env:"SERVER_TLS_KEY_FILE"`
	TLSTerminatedUpstream bool   `env:"TLS_TERMINATED_UPSTREAM" envDefault:"false"`
}

// TenancyConfig configures tenant defaults and multi-tenant mode.
type TenancyConfig struct {
	DefaultTenantID           string `env:"DEFAULT_TENANT_ID"                        envDefault:"11111111-1111-1111-1111-111111111111"`
	DefaultTenantSlug         string `env:"DEFAULT_TENANT_SLUG"                      envDefault:"default"`
	MultiTenantEnabled        bool   `env:"MULTI_TENANT_ENABLED"                     envDefault:"false"`
	MultiTenantURL            string `env:"MULTI_TENANT_URL"`
	MultiTenantEnvironment    string `env:"MULTI_TENANT_ENVIRONMENT"                 envDefault:"staging"`
	MultiTenantMaxTenantPools int    `env:"MULTI_TENANT_MAX_TENANT_POOLS"            envDefault:"100"`
	MultiTenantIdleTimeoutSec int    `env:"MULTI_TENANT_IDLE_TIMEOUT_SEC"            envDefault:"300"`
	MultiTenantCBThreshold    int    `env:"MULTI_TENANT_CIRCUIT_BREAKER_THRESHOLD"   envDefault:"5"`
	MultiTenantCBTimeoutSec   int    `env:"MULTI_TENANT_CIRCUIT_BREAKER_TIMEOUT_SEC" envDefault:"30"`
}

// PostgresConfig configures primary/replica connections and pooling.
type PostgresConfig struct {
	PrimaryHost     string `env:"POSTGRES_HOST"     envDefault:"localhost"`
	PrimaryPort     string `env:"POSTGRES_PORT"     envDefault:"5432"`
	PrimaryUser     string `env:"POSTGRES_USER"     envDefault:"matcher"`
	PrimaryPassword string `env:"POSTGRES_PASSWORD"`
	PrimaryDB       string `env:"POSTGRES_DB"       envDefault:"matcher"`
	PrimarySSLMode  string `env:"POSTGRES_SSLMODE"  envDefault:"disable"`

	ReplicaHost     string `env:"POSTGRES_REPLICA_HOST"`
	ReplicaPort     string `env:"POSTGRES_REPLICA_PORT"`
	ReplicaUser     string `env:"POSTGRES_REPLICA_USER"`
	ReplicaPassword string `env:"POSTGRES_REPLICA_PASSWORD"`
	ReplicaDB       string `env:"POSTGRES_REPLICA_DB"`
	ReplicaSSLMode  string `env:"POSTGRES_REPLICA_SSLMODE"`

	MaxOpenConnections  int    `env:"POSTGRES_MAX_OPEN_CONNS"          envDefault:"25"`
	MaxIdleConnections  int    `env:"POSTGRES_MAX_IDLE_CONNS"          envDefault:"5"`
	ConnMaxLifetimeMins int    `env:"POSTGRES_CONN_MAX_LIFETIME_MINS"  envDefault:"30"`
	ConnMaxIdleTimeMins int    `env:"POSTGRES_CONN_MAX_IDLE_TIME_MINS" envDefault:"5"`
	ConnectTimeoutSec   int    `env:"POSTGRES_CONNECT_TIMEOUT_SEC"     envDefault:"10"`
	QueryTimeoutSec     int    `env:"POSTGRES_QUERY_TIMEOUT_SEC"       envDefault:"30"`
	MigrationsPath      string `env:"MIGRATIONS_PATH"                  envDefault:"migrations"`
}

// RedisConfig configures Redis connections.
type RedisConfig struct {
	Host           string `env:"REDIS_HOST"             envDefault:"localhost:6379"`
	MasterName     string `env:"REDIS_MASTER_NAME"`
	Password       string `env:"REDIS_PASSWORD" json:"-"`
	DB             int    `env:"REDIS_DB"               envDefault:"0"`
	Protocol       int    `env:"REDIS_PROTOCOL"         envDefault:"3"`
	TLS            bool   `env:"REDIS_TLS"              envDefault:"false"`
	CACert         string `env:"REDIS_CA_CERT"`
	PoolSize       int    `env:"REDIS_POOL_SIZE"        envDefault:"10"`
	MinIdleConn    int    `env:"REDIS_MIN_IDLE_CONNS"   envDefault:"2"`
	ReadTimeoutMs  int    `env:"REDIS_READ_TIMEOUT_MS"  envDefault:"3000"`
	WriteTimeoutMs int    `env:"REDIS_WRITE_TIMEOUT_MS" envDefault:"3000"`
	DialTimeoutMs  int    `env:"REDIS_DIAL_TIMEOUT_MS"  envDefault:"5000"`
}

// RabbitMQConfig configures RabbitMQ connection settings.
type RabbitMQConfig struct {
	URI       string `env:"RABBITMQ_URI"        envDefault:"amqp"`
	Host      string `env:"RABBITMQ_HOST"       envDefault:"localhost"`
	Port      string `env:"RABBITMQ_PORT"       envDefault:"5672"`
	User      string `env:"RABBITMQ_USER"       envDefault:"guest"`
	Password  string `env:"RABBITMQ_PASSWORD"   envDefault:"guest" json:"-"`
	VHost     string `env:"RABBITMQ_VHOST"      envDefault:"/"`
	HealthURL string `env:"RABBITMQ_HEALTH_URL" envDefault:"http://localhost:15672"`
}

// AuthConfig configures authentication and authorization.
type AuthConfig struct {
	Enabled     bool   `env:"AUTH_ENABLED"         envDefault:"false"`
	Host        string `env:"AUTH_SERVICE_ADDRESS"`
	TokenSecret string `env:"AUTH_JWT_SECRET"`
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
	Enabled bool   `env:"SWAGGER_ENABLED" envDefault:"false"`
	Host    string `env:"SWAGGER_HOST"    envDefault:""`
	Schemes string `env:"SWAGGER_SCHEMES" envDefault:"https"`
}

// TelemetryConfig configures OpenTelemetry settings.
type TelemetryConfig struct {
	Enabled           bool   `env:"ENABLE_TELEMETRY"                     envDefault:"false"`
	ServiceName       string `env:"OTEL_RESOURCE_SERVICE_NAME"           envDefault:"matcher"`
	LibraryName       string `env:"OTEL_LIBRARY_NAME"                    envDefault:"github.com/LerianStudio/matcher"`
	ServiceVersion    string `env:"OTEL_RESOURCE_SERVICE_VERSION"        envDefault:"1.0.0"`
	DeploymentEnv     string `env:"OTEL_RESOURCE_DEPLOYMENT_ENVIRONMENT" envDefault:"development"`
	CollectorEndpoint string `env:"OTEL_EXPORTER_OTLP_ENDPOINT"          envDefault:"localhost:4317"`
	// DBMetricsIntervalSec configures how often database connection pool metrics
	// are collected and reported to OpenTelemetry. Lower values provide more
	// granular monitoring but increase overhead. Default: 15 seconds.
	DBMetricsIntervalSec int `env:"DB_METRICS_INTERVAL_SEC"              envDefault:"15"`
}

// RateLimitConfig configures global and export rate limiting.
type RateLimitConfig struct {
	Enabled           bool `env:"RATE_LIMIT_ENABLED"             envDefault:"true"`
	Max               int  `env:"RATE_LIMIT_MAX"                 envDefault:"100"`
	ExpirySec         int  `env:"RATE_LIMIT_EXPIRY_SEC"          envDefault:"60"`
	ExportMax         int  `env:"EXPORT_RATE_LIMIT_MAX"          envDefault:"10"`
	ExportExpirySec   int  `env:"EXPORT_RATE_LIMIT_EXPIRY_SEC"   envDefault:"60"`
	DispatchMax       int  `env:"DISPATCH_RATE_LIMIT_MAX"        envDefault:"50"`
	DispatchExpirySec int  `env:"DISPATCH_RATE_LIMIT_EXPIRY_SEC" envDefault:"60"`
}

// InfrastructureConfig configures infrastructure-level behavior.
type InfrastructureConfig struct {
	ConnectTimeoutSec int `env:"INFRA_CONNECT_TIMEOUT_SEC" envDefault:"30"`
}

// IdempotencyConfig configures idempotency behavior.
type IdempotencyConfig struct {
	// RetryWindowSec configures how long failed idempotency keys remain blocked before allowing retry.
	// Default: 300 seconds (5 minutes).
	RetryWindowSec int `env:"IDEMPOTENCY_RETRY_WINDOW_SEC" envDefault:"300"`

	// SuccessTTLHours configures how long completed idempotency keys remain cached
	// before expiring. This determines the window during which duplicate requests
	// return the cached response. Default: 168 hours (7 days).
	SuccessTTLHours int `env:"IDEMPOTENCY_SUCCESS_TTL_HOURS" envDefault:"168"`

	// HMACSecret is the server-side secret used to HMAC-sign client-provided
	// idempotency keys before storing them in Redis. This prevents key prediction
	// attacks by making stored keys unpredictable even if the client key format
	// is known. When empty, keys are stored unsigned (backward compatible).
	// SECURITY: Use a strong, random secret in production (minimum 32 bytes).
	HMACSecret string `env:"IDEMPOTENCY_HMAC_SECRET"`
}

// CallbackRateLimitConfig configures callback-specific rate limiting.
type CallbackRateLimitConfig struct {
	// PerMinute is the maximum number of callbacks allowed per external system
	// per minute. Each external system (JIRA, WEBHOOK, SERVICENOW, etc.) gets
	// its own independent rate limit budget. Default: 60 per minute.
	PerMinute int `env:"CALLBACK_RATE_LIMIT_PER_MIN" envDefault:"60"`
}

// DedupeConfig configures deduplication behavior.
type DedupeConfig struct {
	// TTLSec configures the TTL for deduplication keys in Redis. Default: 3600 seconds.
	TTLSec int `env:"DEDUPE_TTL_SEC" envDefault:"3600"`
}

// ObjectStorageConfig configures object storage (S3-compatible) settings.
type ObjectStorageConfig struct {
	Endpoint        string `env:"OBJECT_STORAGE_ENDPOINT"          envDefault:"http://localhost:8333"`
	Region          string `env:"OBJECT_STORAGE_REGION"            envDefault:"us-east-1"`
	Bucket          string `env:"OBJECT_STORAGE_BUCKET"            envDefault:"matcher-exports"`
	AccessKeyID     string `env:"OBJECT_STORAGE_ACCESS_KEY_ID"`
	SecretAccessKey string `env:"OBJECT_STORAGE_SECRET_ACCESS_KEY"`
	UsePathStyle    bool   `env:"OBJECT_STORAGE_USE_PATH_STYLE"    envDefault:"true"`
}

// ExportWorkerConfig configures reporting export workers.
type ExportWorkerConfig struct {
	Enabled          bool `env:"EXPORT_WORKER_ENABLED"           envDefault:"true"`
	PollIntervalSec  int  `env:"EXPORT_WORKER_POLL_INTERVAL_SEC" envDefault:"5"`
	PageSize         int  `env:"EXPORT_WORKER_PAGE_SIZE"         envDefault:"1000"`
	PresignExpirySec int  `env:"EXPORT_PRESIGN_EXPIRY_SEC"       envDefault:"3600"`
}

// WebhookConfig configures default webhook dispatch settings.
type WebhookConfig struct {
	// TimeoutSec configures the default HTTP timeout for webhook dispatches.
	// Individual webhooks may override this via the admin API in the future.
	// Default: 30 seconds.
	TimeoutSec int `env:"WEBHOOK_TIMEOUT_SEC" envDefault:"30"`
}

// CleanupWorkerConfig configures background cleanup workers.
type CleanupWorkerConfig struct {
	Enabled        bool `env:"CLEANUP_WORKER_ENABLED"          envDefault:"true"`
	IntervalSec    int  `env:"CLEANUP_WORKER_INTERVAL_SEC"     envDefault:"3600"`
	BatchSize      int  `env:"CLEANUP_WORKER_BATCH_SIZE"       envDefault:"100"`
	GracePeriodSec int  `env:"CLEANUP_WORKER_GRACE_PERIOD_SEC" envDefault:"3600"`
}

// SchedulerConfig configures the cron-based scheduler worker.
type SchedulerConfig struct {
	IntervalSec int `env:"SCHEDULER_INTERVAL_SEC" envDefault:"60"`
}

// ArchivalConfig configures the audit log archival worker.
type ArchivalConfig struct {
	Enabled             bool   `env:"ARCHIVAL_WORKER_ENABLED"        envDefault:"false"`
	IntervalHours       int    `env:"ARCHIVAL_WORKER_INTERVAL_HOURS" envDefault:"24"`
	HotRetentionDays    int    `env:"ARCHIVAL_HOT_RETENTION_DAYS"    envDefault:"90"`
	WarmRetentionMonths int    `env:"ARCHIVAL_WARM_RETENTION_MONTHS" envDefault:"24"`
	ColdRetentionMonths int    `env:"ARCHIVAL_COLD_RETENTION_MONTHS" envDefault:"84"`
	BatchSize           int    `env:"ARCHIVAL_BATCH_SIZE"            envDefault:"5000"`
	StorageBucket       string `env:"ARCHIVAL_STORAGE_BUCKET"`
	StoragePrefix       string `env:"ARCHIVAL_STORAGE_PREFIX"        envDefault:"archives/audit-logs"`
	StorageClass        string `env:"ARCHIVAL_STORAGE_CLASS"         envDefault:"GLACIER"`
	PartitionLookahead  int    `env:"ARCHIVAL_PARTITION_LOOKAHEAD"   envDefault:"3"`
	PresignExpirySec    int    `env:"ARCHIVAL_PRESIGN_EXPIRY_SEC"    envDefault:"3600"`
}

// Config holds all configuration values for the Matcher service,
// including server, database, cache, messaging, and observability settings.
type Config struct {
	App               AppConfig
	Server            ServerConfig
	Tenancy           TenancyConfig
	Postgres          PostgresConfig
	Redis             RedisConfig
	RabbitMQ          RabbitMQConfig
	Auth              AuthConfig
	Swagger           SwaggerConfig
	Telemetry         TelemetryConfig
	RateLimit         RateLimitConfig
	Infrastructure    InfrastructureConfig
	Idempotency       IdempotencyConfig
	Dedupe            DedupeConfig
	ObjectStorage     ObjectStorageConfig
	ExportWorker      ExportWorkerConfig
	CleanupWorker     CleanupWorkerConfig
	Scheduler         SchedulerConfig
	Archival          ArchivalConfig
	Webhook           WebhookConfig
	CallbackRateLimit CallbackRateLimitConfig

	// ShutdownGracePeriod is the time to wait for background workers to finish
	// after requesting stop, before closing infrastructure connections.
	// Zero means use the default (5 seconds).
	ShutdownGracePeriod time.Duration

	// Logger is used for runtime warnings (e.g., capping invalid config values).
	// Set during LoadConfigWithLogger; may be nil if LoadConfig was used.
	Logger libLog.Logger
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

	if err := cfg.validateMultiTenantConfig(asserter); err != nil {
		return err
	}

	return nil
}

// validateMultiTenantConfig validates multi-tenant configuration.
// When multi-tenant mode is enabled, the tenant manager URL is required.
func (cfg *Config) validateMultiTenantConfig(asserter *assert.Asserter) error {
	if !cfg.Tenancy.MultiTenantEnabled {
		return nil
	}

	ctx := context.Background()

	if err := asserter.NotEmpty(ctx, strings.TrimSpace(cfg.Tenancy.MultiTenantURL), "MULTI_TENANT_URL is required when MULTI_TENANT_ENABLED=true"); err != nil {
		return fmt.Errorf("config validation: %w", err)
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
func (cfg *Config) validateRateLimitConfig(asserter *assert.Asserter) error {
	ctx := context.Background()

	if err := asserter.That(ctx, cfg.RateLimit.ExportMax > 0, "EXPORT_RATE_LIMIT_MAX must be positive", "export_rate_limit_max", cfg.RateLimit.ExportMax); err != nil {
		return fmt.Errorf("config validation: %w", err)
	}

	if err := asserter.That(ctx, cfg.RateLimit.ExportExpirySec > 0, "EXPORT_RATE_LIMIT_EXPIRY_SEC must be positive", "export_rate_limit_expiry", cfg.RateLimit.ExportExpirySec); err != nil {
		return fmt.Errorf("config validation: %w", err)
	}

	// Skip validation if rate limiting is disabled
	if !cfg.RateLimit.Enabled {
		return nil
	}

	if err := asserter.That(ctx, cfg.RateLimit.Max > 0, "RATE_LIMIT_MAX must be positive", "rate_limit_max", cfg.RateLimit.Max); err != nil {
		return fmt.Errorf("config validation: %w", err)
	}

	if err := asserter.That(ctx, cfg.RateLimit.ExpirySec > 0, "RATE_LIMIT_EXPIRY_SEC must be positive", "rate_limit_expiry", cfg.RateLimit.ExpirySec); err != nil {
		return fmt.Errorf("config validation: %w", err)
	}

	if err := asserter.That(ctx, cfg.RateLimit.DispatchMax > 0, "DISPATCH_RATE_LIMIT_MAX must be positive", "dispatch_rate_limit_max", cfg.RateLimit.DispatchMax); err != nil {
		return fmt.Errorf("config validation: %w", err)
	}

	if err := asserter.That(ctx, cfg.RateLimit.DispatchExpirySec > 0, "DISPATCH_RATE_LIMIT_EXPIRY_SEC must be positive", "dispatch_rate_limit_expiry", cfg.RateLimit.DispatchExpirySec); err != nil {
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

	if err := asserter.That(ctx, cfg.Auth.Enabled, "AUTH_ENABLED must be true in production"); err != nil {
		return fmt.Errorf("production config validation: %w", err)
	}

	if err := asserter.That(ctx, strings.TrimSpace(cfg.Server.CORSAllowedOrigins) != "" && !strings.Contains(cfg.Server.CORSAllowedOrigins, "*"), "CORS_ALLOWED_ORIGINS must be restricted in production", "cors_origins", cfg.Server.CORSAllowedOrigins); err != nil {
		return fmt.Errorf("production config validation: %w", err)
	}

	return nil
}

func (cfg *Config) validateProductionSecurityConfig(asserter *assert.Asserter) error {
	ctx := context.Background()
	primarySSLMode := strings.ToLower(strings.TrimSpace(cfg.Postgres.PrimarySSLMode))
	rabbitMQURI := strings.ToLower(strings.TrimSpace(cfg.RabbitMQ.URI))

	// Require inbound TLS in production unless explicitly terminated upstream (e.g., by load balancer)
	if !cfg.Server.TLSTerminatedUpstream {
		hasTLSCert := strings.TrimSpace(cfg.Server.TLSCertFile) != ""
		hasTLSKey := strings.TrimSpace(cfg.Server.TLSKeyFile) != ""

		if err := asserter.That(ctx, hasTLSCert && hasTLSKey,
			"production requires inbound TLS: either configure SERVER_TLS_CERT_FILE and SERVER_TLS_KEY_FILE, "+
				"or set TLS_TERMINATED_UPSTREAM=true if TLS termination happens at the load balancer/reverse proxy"); err != nil {
			return fmt.Errorf("production config validation: %w", err)
		}
	}

	if err := asserter.That(ctx, !strings.EqualFold(strings.TrimSpace(cfg.RabbitMQ.User), "guest") && !strings.EqualFold(strings.TrimSpace(cfg.RabbitMQ.Password), "guest"), "RABBITMQ credentials must be set to non-default values in production"); err != nil {
		return fmt.Errorf("production config validation: %w", err)
	}

	if err := asserter.That(ctx, primarySSLMode != "disable", "POSTGRES_SSLMODE must not be disable in production"); err != nil {
		return fmt.Errorf("production config validation: %w", err)
	}

	if strings.TrimSpace(cfg.Postgres.ReplicaHost) != "" {
		replicaSSLMode := strings.ToLower(strings.TrimSpace(cfg.Postgres.ReplicaSSLMode))
		if replicaSSLMode == "" {
			replicaSSLMode = primarySSLMode
		}

		if err := asserter.That(ctx, replicaSSLMode != "disable", "POSTGRES_REPLICA_SSLMODE must not be disable in production"); err != nil {
			return fmt.Errorf("production config validation: %w", err)
		}
	}

	if err := asserter.That(ctx, cfg.Redis.TLS, "REDIS_TLS must be true in production"); err != nil {
		return fmt.Errorf("production config validation: %w", err)
	}

	if err := asserter.That(ctx, rabbitMQURI == "amqps" || strings.HasPrefix(rabbitMQURI, "amqps://"), "RABBITMQ_URI must use amqps in production"); err != nil {
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

// PrimaryDSN returns the PostgreSQL connection string for the primary database.
func (cfg *Config) PrimaryDSN() string {
	return fmt.Sprintf(
		"host=%s port=%s user=%s password=%s dbname=%s sslmode=%s connect_timeout=%d",
		cfg.Postgres.PrimaryHost,
		cfg.Postgres.PrimaryPort,
		cfg.Postgres.PrimaryUser,
		cfg.Postgres.PrimaryPassword,
		cfg.Postgres.PrimaryDB,
		cfg.Postgres.PrimarySSLMode,
		cfg.Postgres.ConnectTimeoutSec,
	)
}

// ReplicaDSN returns the PostgreSQL connection string for the replica database,
// falling back to primary settings when replica-specific values are not configured.
func (cfg *Config) ReplicaDSN() string {
	if cfg.Postgres.ReplicaHost == "" {
		return cfg.PrimaryDSN()
	}

	host := cfg.Postgres.ReplicaHost

	port := cfg.Postgres.ReplicaPort
	if port == "" {
		port = cfg.Postgres.PrimaryPort
	}

	user := cfg.Postgres.ReplicaUser
	if user == "" {
		user = cfg.Postgres.PrimaryUser
	}

	password := cfg.Postgres.ReplicaPassword
	if password == "" {
		password = cfg.Postgres.PrimaryPassword
	}

	dbname := cfg.Postgres.ReplicaDB
	if dbname == "" {
		dbname = cfg.Postgres.PrimaryDB
	}

	sslmode := cfg.Postgres.ReplicaSSLMode
	if sslmode == "" {
		sslmode = cfg.Postgres.PrimarySSLMode
	}

	return fmt.Sprintf(
		"host=%s port=%s user=%s password=%s dbname=%s sslmode=%s connect_timeout=%d",
		host,
		port,
		user,
		password,
		dbname,
		sslmode,
		cfg.Postgres.ConnectTimeoutSec,
	)
}

// PrimaryDSNMasked returns the primary connection string with password redacted for logging.
func (cfg *Config) PrimaryDSNMasked() string {
	return fmt.Sprintf("host=%s port=%s user=%s password=***REDACTED*** dbname=%s sslmode=%s",
		cfg.Postgres.PrimaryHost, cfg.Postgres.PrimaryPort, cfg.Postgres.PrimaryUser,
		cfg.Postgres.PrimaryDB, cfg.Postgres.PrimarySSLMode)
}

// ReplicaDSNMasked returns the replica connection string with password redacted for logging.
func (cfg *Config) ReplicaDSNMasked() string {
	if cfg.Postgres.ReplicaHost == "" {
		return cfg.PrimaryDSNMasked()
	}

	host := cfg.Postgres.ReplicaHost

	port := cfg.Postgres.ReplicaPort
	if port == "" {
		port = cfg.Postgres.PrimaryPort
	}

	user := cfg.Postgres.ReplicaUser
	if user == "" {
		user = cfg.Postgres.PrimaryUser
	}

	dbname := cfg.Postgres.ReplicaDB
	if dbname == "" {
		dbname = cfg.Postgres.PrimaryDB
	}

	sslmode := cfg.Postgres.ReplicaSSLMode
	if sslmode == "" {
		sslmode = cfg.Postgres.PrimarySSLMode
	}

	return fmt.Sprintf("host=%s port=%s user=%s password=***REDACTED*** dbname=%s sslmode=%s",
		host, port, user, dbname, sslmode)
}

// RabbitMQDSN returns the AMQP connection string with properly URL-encoded credentials and vhost.
func (cfg *Config) RabbitMQDSN() string {
	var userinfo *url.Userinfo
	if cfg.RabbitMQ.Password == "" {
		userinfo = url.User(cfg.RabbitMQ.User)
	} else {
		userinfo = url.UserPassword(cfg.RabbitMQ.User, cfg.RabbitMQ.Password)
	}

	connURL := url.URL{
		Scheme: cfg.RabbitMQ.URI,
		User:   userinfo,
		Host:   net.JoinHostPort(cfg.RabbitMQ.Host, cfg.RabbitMQ.Port),
	}

	// RabbitMQ vhost is represented as the URL path segment and must be URL-encoded.
	// Default vhost is "/" which must be encoded as "%2F".
	vhostRaw := strings.TrimSpace(cfg.RabbitMQ.VHost)
	if vhostRaw != "" {
		if strings.Trim(vhostRaw, "/") == "" {
			connURL.Path = "//"
			connURL.RawPath = "/%2F"
		} else {
			vhost := strings.TrimPrefix(vhostRaw, "/")
			connURL.Path = "/" + vhost
			connURL.RawPath = "/" + url.PathEscape(vhost)
		}
	}

	return connURL.String()
}

// RedisReadTimeout returns the Redis read timeout as a time.Duration.
func (cfg *Config) RedisReadTimeout() time.Duration {
	return time.Duration(cfg.Redis.ReadTimeoutMs) * time.Millisecond
}

// RedisWriteTimeout returns the Redis write timeout as a time.Duration.
func (cfg *Config) RedisWriteTimeout() time.Duration {
	return time.Duration(cfg.Redis.WriteTimeoutMs) * time.Millisecond
}

// RedisDialTimeout returns the Redis dial timeout as a time.Duration.
func (cfg *Config) RedisDialTimeout() time.Duration {
	return time.Duration(cfg.Redis.DialTimeoutMs) * time.Millisecond
}

// ConnMaxLifetime returns the PostgreSQL connection max lifetime as a time.Duration.
func (cfg *Config) ConnMaxLifetime() time.Duration {
	return time.Duration(cfg.Postgres.ConnMaxLifetimeMins) * time.Minute
}

// ConnMaxIdleTime returns the PostgreSQL connection max idle time as a time.Duration.
func (cfg *Config) ConnMaxIdleTime() time.Duration {
	return time.Duration(cfg.Postgres.ConnMaxIdleTimeMins) * time.Minute
}

// QueryTimeout returns the PostgreSQL query timeout as a time.Duration.
// This bounds the maximum time any database operation (query, transaction) can take.
// Prevents indefinite hangs when the connection pool is exhausted and no context
// deadline is set by the caller. Returns defaultQueryTimeoutSec (30 seconds) if
// the configured value is non-positive.
func (cfg *Config) QueryTimeout() time.Duration {
	if cfg.Postgres.QueryTimeoutSec <= 0 {
		return defaultQueryTimeoutSec * time.Second
	}

	return time.Duration(cfg.Postgres.QueryTimeoutSec) * time.Second
}

// InfraConnectTimeout returns the overall infrastructure connection timeout as a time.Duration.
// This is the maximum time allowed for all infrastructure connections (PostgreSQL, Redis, RabbitMQ)
// to complete during application startup.
func (cfg *Config) InfraConnectTimeout() time.Duration {
	if cfg.Infrastructure.ConnectTimeoutSec <= 0 {
		return time.Second
	}

	if cfg.Infrastructure.ConnectTimeoutSec > maxInfraConnectTimeoutSec {
		if cfg.Logger != nil {
			cfg.Logger.Log(
				context.Background(),
				libLog.LevelWarn,
				fmt.Sprintf(
					"INFRA_CONNECT_TIMEOUT_SEC=%d exceeds maximum of %d seconds, capping to maximum",
					cfg.Infrastructure.ConnectTimeoutSec,
					maxInfraConnectTimeoutSec,
				),
			)
		}

		return time.Duration(maxInfraConnectTimeoutSec) * time.Second
	}

	return time.Duration(cfg.Infrastructure.ConnectTimeoutSec) * time.Second
}

// DBMetricsInterval returns the database metrics collection interval as a time.Duration.
// Returns a minimum of 1 second if configured value is non-positive.
func (cfg *Config) DBMetricsInterval() time.Duration {
	if cfg.Telemetry.DBMetricsIntervalSec <= 0 {
		return time.Second
	}

	return time.Duration(cfg.Telemetry.DBMetricsIntervalSec) * time.Second
}

// IdempotencyRetryWindow returns the idempotency retry window as a time.Duration.
// Returns a minimum of 1 minute if configured value is non-positive.
func (cfg *Config) IdempotencyRetryWindow() time.Duration {
	if cfg.Idempotency.RetryWindowSec <= 0 {
		return time.Minute
	}

	return time.Duration(cfg.Idempotency.RetryWindowSec) * time.Second
}

// IdempotencySuccessTTL returns the idempotency success TTL as a time.Duration.
// Returns a minimum of 1 hour if configured value is non-positive.
func (cfg *Config) IdempotencySuccessTTL() time.Duration {
	if cfg.Idempotency.SuccessTTLHours <= 0 {
		return time.Hour
	}

	return time.Duration(cfg.Idempotency.SuccessTTLHours) * time.Hour
}

// WebhookTimeout returns the default webhook dispatch timeout as a time.Duration.
// Returns a minimum of 1 second if configured value is non-positive.
// Caps at 300 seconds (5 minutes) to prevent runaway connections.
func (cfg *Config) WebhookTimeout() time.Duration {
	const (
		maxWebhookTimeoutSec     = 300 // 5 minutes
		defaultWebhookTimeoutSec = 30
	)

	if cfg.Webhook.TimeoutSec <= 0 {
		return time.Duration(defaultWebhookTimeoutSec) * time.Second
	}

	if cfg.Webhook.TimeoutSec > maxWebhookTimeoutSec {
		if cfg.Logger != nil {
			cfg.Logger.Log(context.Background(), libLog.LevelWarn, fmt.Sprintf("WEBHOOK_TIMEOUT_SEC=%d exceeds maximum of %d seconds, capping to maximum",
				cfg.Webhook.TimeoutSec, maxWebhookTimeoutSec))
		}

		return time.Duration(maxWebhookTimeoutSec) * time.Second
	}

	return time.Duration(cfg.Webhook.TimeoutSec) * time.Second
}

// ExportWorkerPollInterval returns the export worker poll interval as a time.Duration.
// Returns a default of 5 seconds if configured value is non-positive.
func (cfg *Config) ExportWorkerPollInterval() time.Duration {
	if cfg.ExportWorker.PollIntervalSec <= 0 {
		return defaultExportWorkerPollIntervalSec * time.Second
	}

	return time.Duration(cfg.ExportWorker.PollIntervalSec) * time.Second
}

// ExportPresignExpiry returns the presigned URL expiry duration for export downloads.
// Returns a default of 1 hour if configured value is non-positive.
// Caps at S3's maximum of 7 days (604800 seconds) if exceeded.
func (cfg *Config) ExportPresignExpiry() time.Duration {
	const (
		maxPresignExpiry     = 604800 // S3 maximum: 7 days in seconds
		defaultPresignExpiry = 3600   // 1 hour default
	)

	if cfg.ExportWorker.PresignExpirySec <= 0 {
		return time.Duration(defaultPresignExpiry) * time.Second
	}

	if cfg.ExportWorker.PresignExpirySec > maxPresignExpiry {
		if cfg.Logger != nil {
			cfg.Logger.Log(context.Background(), libLog.LevelWarn, fmt.Sprintf("EXPORT_PRESIGN_EXPIRY_SEC=%d exceeds S3 maximum of %d seconds, capping to maximum",
				cfg.ExportWorker.PresignExpirySec, maxPresignExpiry))
		}

		return time.Duration(maxPresignExpiry) * time.Second
	}

	return time.Duration(cfg.ExportWorker.PresignExpirySec) * time.Second
}

// CleanupWorkerInterval returns the cleanup worker run interval as a time.Duration.
// Returns a default of 1 hour if configured value is non-positive.
func (cfg *Config) CleanupWorkerInterval() time.Duration {
	const defaultInterval = 3600 // 1 hour default

	if cfg.CleanupWorker.IntervalSec <= 0 {
		return time.Duration(defaultInterval) * time.Second
	}

	return time.Duration(cfg.CleanupWorker.IntervalSec) * time.Second
}

// CleanupWorkerBatchSize returns the cleanup worker batch size.
// Returns a default of 100 if configured value is non-positive.
func (cfg *Config) CleanupWorkerBatchSize() int {
	const defaultBatch = 100

	if cfg.CleanupWorker.BatchSize <= 0 {
		return defaultBatch
	}

	return cfg.CleanupWorker.BatchSize
}

// CleanupWorkerGracePeriod returns the file deletion grace period as a time.Duration.
// This controls how long after expiry the worker waits before deleting S3 files,
// allowing presigned download URLs to complete.
// Returns a default of 1 hour if configured value is non-positive.
func (cfg *Config) CleanupWorkerGracePeriod() time.Duration {
	const defaultGrace = 3600 // 1 hour default

	if cfg.CleanupWorker.GracePeriodSec <= 0 {
		return time.Duration(defaultGrace) * time.Second
	}

	return time.Duration(cfg.CleanupWorker.GracePeriodSec) * time.Second
}

// ArchivalInterval returns the archival worker run interval as a time.Duration.
// Returns a minimum of 1 hour if configured value is non-positive.
func (cfg *Config) ArchivalInterval() time.Duration {
	if cfg.Archival.IntervalHours <= 0 {
		return time.Hour
	}

	return time.Duration(cfg.Archival.IntervalHours) * time.Hour
}

// ArchivalPresignExpiry returns the presigned URL expiry duration for archived audit log downloads.
// Returns a default of 1 hour if configured value is non-positive.
// Caps at S3's maximum of 7 days (604800 seconds) if exceeded.
func (cfg *Config) ArchivalPresignExpiry() time.Duration {
	const (
		maxPresignExpiry     = 604800 // S3 maximum: 7 days in seconds
		defaultPresignExpiry = 3600   // 1 hour default
	)

	if cfg.Archival.PresignExpirySec <= 0 {
		return time.Duration(defaultPresignExpiry) * time.Second
	}

	if cfg.Archival.PresignExpirySec > maxPresignExpiry {
		if cfg.Logger != nil {
			cfg.Logger.Log(context.Background(), libLog.LevelWarn, fmt.Sprintf("ARCHIVAL_PRESIGN_EXPIRY_SEC=%d exceeds S3 maximum of %d seconds, capping to maximum",
				cfg.Archival.PresignExpirySec, maxPresignExpiry))
		}

		return time.Duration(maxPresignExpiry) * time.Second
	}

	return time.Duration(cfg.Archival.PresignExpirySec) * time.Second
}

// CallbackRateLimitPerMinute returns the callback rate limit per minute.
// Returns a minimum of 1 if configured value is non-positive.
func (cfg *Config) CallbackRateLimitPerMinute() int {
	if cfg.CallbackRateLimit.PerMinute <= 0 {
		return 60 //nolint:mnd // sensible default: 60 callbacks per minute per external system
	}

	return cfg.CallbackRateLimit.PerMinute
}

// SchedulerInterval returns the scheduler worker poll interval as a time.Duration.
// Returns a default of 1 minute if configured value is non-positive.
func (cfg *Config) SchedulerInterval() time.Duration {
	if cfg.Scheduler.IntervalSec <= 0 {
		return time.Minute
	}

	return time.Duration(cfg.Scheduler.IntervalSec) * time.Second
}

// validateArchivalConfig validates archival worker configuration.
// Retention and batch validations only run when archival is enabled because
// lib-uncommons.SetConfigFromEnvVars does not apply envDefault tags -- fields
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

// LoadConfigWithLogger loads configuration from environment variables with an optional logger.
// If logger is nil, a default logger will be created for warning messages.
func LoadConfigWithLogger(logger libLog.Logger) (*Config, error) {
	cfg := &Config{}
	ctx := context.Background()
	asserter := newConfigAsserter(ctx, "config.load")

	if err := asserter.NoError(ctx, loadConfigFromEnv(cfg), "failed to load config from environment variables"); err != nil {
		return nil, fmt.Errorf("load config: %w", err)
	}

	if cfg.Server.BodyLimitBytes <= 0 {
		cfg.Server.BodyLimitBytes = defaultHTTPBodyLimitBytes
	}

	// Store logger for runtime warnings (e.g., capping invalid config values)
	if logger == nil {
		var logErr error

		logger, logErr = libZap.New(libZap.Config{
			Environment:     ResolveLoggerEnvironment(cfg.App.EnvName),
			Level:           ResolveLoggerLevel(cfg.App.LogLevel),
			OTelLibraryName: "github.com/LerianStudio/matcher",
		})
		if logErr != nil {
			return nil, fmt.Errorf("initialize default logger: %w", logErr)
		}
	}

	cfg.Logger = logger

	// Enforce production security defaults before validation
	cfg.enforceProductionSecurityDefaults(logger)

	if err := asserter.NoError(ctx, cfg.Validate(), "configuration validation failed"); err != nil {
		return nil, fmt.Errorf("load config: %w", err)
	}

	return cfg, nil
}

// enforceProductionSecurityDefaults enforces security-critical settings in production.
// This provides a safety net that prevents misconfiguration from disabling security features.
//
// This function is called exclusively from LoadConfigWithLogger, before Validate().
// Calling Validate() independently will NOT apply these enforcements — this is by design.
// Validate() checks constraints (returning errors for violations), while this function
// silently corrects misconfigured values with logged warnings.
func (cfg *Config) enforceProductionSecurityDefaults(logger libLog.Logger) {
	if !IsProductionEnvironment(cfg.App.EnvName) {
		return
	}

	ctx := context.Background()

	if logger == nil {
		var logErr error

		logger, logErr = libZap.New(libZap.Config{
			Environment:     libZap.EnvironmentProduction,
			Level:           ResolveLoggerLevel(cfg.App.LogLevel),
			OTelLibraryName: "github.com/LerianStudio/matcher",
		})
		if logErr != nil {
			// Cannot enforce security defaults without a logger to report warnings.
			// Note: Validate() does NOT check Swagger or rate-limit settings, so
			// returning here skips enforcement silently. In practice, the normal
			// bootstrap chain always provides a non-nil logger (initLogger fails
			// hard on error), so this path is a defensive fallback only.
			return
		}
	}

	// Disable Swagger in production. API documentation should not be exposed in production.
	if cfg.Swagger.Enabled {
		logger.Log(ctx, libLog.LevelWarn, "SECURITY: Swagger is enabled in production. Disabling it. env="+cfg.App.EnvName)
		cfg.Swagger.Enabled = false
	}

	// Enforce rate limiting in production - it cannot be disabled
	if !cfg.RateLimit.Enabled {
		logger.Log(ctx, libLog.LevelWarn, fmt.Sprintf("SECURITY: RATE_LIMIT_ENABLED=false is not allowed in production. "+
			"Forcing rate limiting to enabled. env=%s", cfg.App.EnvName))

		cfg.RateLimit.Enabled = true
	}
}

func loadConfigFromEnv(cfg *Config) error {
	if cfg == nil {
		return ErrConfigNil
	}

	var loadErr error

	loadErr = errors.Join(loadErr, libCommons.SetConfigFromEnvVars(&cfg.App))
	loadErr = errors.Join(loadErr, libCommons.SetConfigFromEnvVars(&cfg.Server))
	loadErr = errors.Join(loadErr, libCommons.SetConfigFromEnvVars(&cfg.Tenancy))
	loadErr = errors.Join(loadErr, libCommons.SetConfigFromEnvVars(&cfg.Postgres))
	loadErr = errors.Join(loadErr, libCommons.SetConfigFromEnvVars(&cfg.Redis))
	loadErr = errors.Join(loadErr, libCommons.SetConfigFromEnvVars(&cfg.RabbitMQ))
	loadErr = errors.Join(loadErr, libCommons.SetConfigFromEnvVars(&cfg.Auth))
	loadErr = errors.Join(loadErr, libCommons.SetConfigFromEnvVars(&cfg.Swagger))
	loadErr = errors.Join(loadErr, libCommons.SetConfigFromEnvVars(&cfg.Telemetry))
	loadErr = errors.Join(loadErr, libCommons.SetConfigFromEnvVars(&cfg.RateLimit))
	loadErr = errors.Join(loadErr, libCommons.SetConfigFromEnvVars(&cfg.Infrastructure))
	loadErr = errors.Join(loadErr, libCommons.SetConfigFromEnvVars(&cfg.Idempotency))
	loadErr = errors.Join(loadErr, libCommons.SetConfigFromEnvVars(&cfg.Dedupe))
	loadErr = errors.Join(loadErr, libCommons.SetConfigFromEnvVars(&cfg.ObjectStorage))
	loadErr = errors.Join(loadErr, libCommons.SetConfigFromEnvVars(&cfg.ExportWorker))
	loadErr = errors.Join(loadErr, libCommons.SetConfigFromEnvVars(&cfg.Scheduler))
	loadErr = errors.Join(loadErr, libCommons.SetConfigFromEnvVars(&cfg.Archival))
	loadErr = errors.Join(loadErr, libCommons.SetConfigFromEnvVars(&cfg.Webhook))
	loadErr = errors.Join(loadErr, libCommons.SetConfigFromEnvVars(&cfg.CallbackRateLimit))
	loadErr = errors.Join(loadErr, libCommons.SetConfigFromEnvVars(&cfg.CleanupWorker))

	return loadErr
}

func newConfigAsserter(ctx context.Context, operation string) *assert.Asserter {
	return assert.New(ctx, nil, constants.ApplicationName, operation)
}

// Options provides optional configuration overrides for server initialization.
type Options struct {
	Logger libLog.Logger
}
