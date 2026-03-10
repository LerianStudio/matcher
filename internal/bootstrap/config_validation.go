package bootstrap

import (
	"context"
	"errors"
	"fmt"
	"os"
	"reflect"
	"strings"

	libCommons "github.com/LerianStudio/lib-commons/v4/commons"
	"github.com/LerianStudio/lib-commons/v4/commons/assert"
	libLog "github.com/LerianStudio/lib-commons/v4/commons/log"
	libZap "github.com/LerianStudio/lib-commons/v4/commons/zap"

	"github.com/LerianStudio/matcher/internal/shared/constants"
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

// LoadConfigWithLogger loads configuration from environment variables with an optional logger.
// If logger is nil, a default logger will be created for warning messages.
func LoadConfigWithLogger(logger libLog.Logger) (*Config, error) {
	cfg := defaultConfig()
	ctx := context.Background()
	asserter := newConfigAsserter(ctx, "config.load")

	configFilePath := resolveConfigFilePath()
	if err := asserter.NoError(ctx, loadConfigFromYAML(cfg, configFilePath), "failed to load config from YAML file"); err != nil {
		return nil, fmt.Errorf("load config: %w", err)
	}

	yamlSnapshot := *cfg

	if err := asserter.NoError(ctx, loadConfigFromEnv(cfg), "failed to load config from environment variables"); err != nil {
		return nil, fmt.Errorf("load config: %w", err)
	}

	restoreZeroedFields(cfg, &yamlSnapshot)

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

// sanitizeEnvVarsForConfig trims trailing whitespace from all environment variables
// referenced by Config struct tags. This is defense-in-depth against .env files with
// inline comments (e.g., "RATE_LIMIT_MAX=100  # comment") that Make's -include
// directive loads verbatim, causing strconv.Atoi to fail on "100  # comment" or "100  ".
func sanitizeEnvVarsForConfig() {
	sanitizeEnvVarsForStruct(reflect.TypeOf(Config{}))
}

// sanitizeEnvVarsForStruct recursively walks a struct type and trims whitespace from
// all environment variables identified by `env:` struct tags.
func sanitizeEnvVarsForStruct(structType reflect.Type) {
	for structType.Kind() == reflect.Ptr {
		structType = structType.Elem()
	}

	if structType.Kind() != reflect.Struct {
		return
	}

	for i := range structType.NumField() {
		field := structType.Field(i)

		// Recurse into embedded struct fields (e.g., AppConfig, ServerConfig).
		if field.Type.Kind() == reflect.Struct && field.Type != reflect.TypeOf((*error)(nil)).Elem() {
			sanitizeEnvVarsForStruct(field.Type)

			continue
		}

		envTag := field.Tag.Get("env")
		if envTag == "" {
			continue
		}

		// Extract the env var name (first comma-separated token, matching lib-commons behavior).
		envName := strings.SplitN(envTag, ",", 2)[0] //nolint:mnd // split tag into name,options
		if envName == "" {
			continue
		}

		if val, ok := os.LookupEnv(envName); ok {
			trimmed := strings.TrimSpace(val)
			if trimmed != val {
				_ = os.Setenv(envName, trimmed)
			}
		}
	}
}

func loadConfigFromEnv(cfg *Config) error {
	if cfg == nil {
		return ErrConfigNil
	}

	// Trim trailing whitespace from all config-related env vars before parsing.
	// Prevents strconv.Atoi failures from inline .env comments loaded by Make.
	sanitizeEnvVarsForConfig()

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
	loadErr = errors.Join(loadErr, libCommons.SetConfigFromEnvVars(&cfg.Fetcher))

	return loadErr
}

func newConfigAsserter(ctx context.Context, operation string) *assert.Asserter {
	return assert.New(ctx, nil, constants.ApplicationName, operation)
}
