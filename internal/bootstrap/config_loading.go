package bootstrap

import (
	"context"
	"errors"
	"fmt"
	"os"
	"reflect"
	"strings"

	libCommons "github.com/LerianStudio/lib-commons/v4/commons"
	libLog "github.com/LerianStudio/lib-commons/v4/commons/log"
	libZap "github.com/LerianStudio/lib-commons/v4/commons/zap"
)

// LoadConfigWithLogger loads configuration from environment variables with an optional logger.
// If logger is nil, a default logger will be created for warning messages.
func LoadConfigWithLogger(logger libLog.Logger) (*Config, error) {
	cfg := defaultConfig()
	ctx := context.Background()
	asserter := newConfigAsserter(ctx, "config.load")

	configFilePath, pathErr := resolveConfigFilePathStrict()
	if err := asserter.NoError(ctx, pathErr, "invalid config file path override"); err != nil {
		return nil, fmt.Errorf("load config: %w", err)
	}

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
