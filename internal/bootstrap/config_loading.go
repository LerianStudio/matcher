// Copyright 2025 Lerian Studio. All rights reserved.
// Use of this source code is governed by an Elastic License 2.0
// that can be found in the LICENSE.md file.

package bootstrap

import (
	"context"
	"errors"
	"fmt"
	"os"
	"reflect"
	"strconv"
	"strings"
	"time"

	libCommons "github.com/LerianStudio/lib-commons/v5/commons"
	libLog "github.com/LerianStudio/lib-commons/v5/commons/log"
	libZap "github.com/LerianStudio/lib-commons/v5/commons/zap"

	sharedPorts "github.com/LerianStudio/matcher/internal/shared/ports"
)

type envAliasKind string

const (
	envAliasKindString envAliasKind = "string"
	envAliasKindBool   envAliasKind = "bool"
)

type envAlias struct {
	legacy  string
	current string
	kind    envAliasKind
}

var legacyAuthEnvAliases = []envAlias{
	{legacy: "AUTH_ENABLED", current: "PLUGIN_AUTH_ENABLED", kind: envAliasKindBool},
	{legacy: "AUTH_SERVICE_ADDRESS", current: "PLUGIN_AUTH_ADDRESS", kind: envAliasKindString},
}

var errConflictingEnvAliasValues = errors.New("conflicting environment alias values")

// Architecture Decision: Environment-Only Configuration
//
// defaultConfig() provides all defaults. libCommons.SetConfigFromEnvVars overlays
// environment variables on top. After bootstrap, the systemplane supervisor owns
// all runtime configuration changes.

// LoadConfigWithLogger loads configuration from environment variables with an optional logger.
// If logger is nil, a default logger will be created for warning messages.
func LoadConfigWithLogger(logger libLog.Logger) (*Config, error) {
	cfg := defaultConfig()
	ctx := context.Background()
	asserter := newConfigAsserter(ctx, "config.load")

	// Snapshot defaults before env overlay. SetConfigFromEnvVars zeros out fields
	// whose env vars are absent, so we restore non-zero defaults afterwards.
	defaults := *cfg

	if err := asserter.NoError(ctx, loadConfigFromEnvForStartup(cfg), "failed to load config from environment variables"); err != nil {
		return nil, fmt.Errorf("load config: %w", err)
	}

	cfg.normalizeTenancyConfig()

	restoreZeroedFields(cfg, &defaults)
	cfg.normalizeTenancyConfig()

	if cfg.Server.BodyLimitBytes <= 0 {
		cfg.Server.BodyLimitBytes = defaultHTTPBodyLimitBytes
	}

	// Store logger for runtime warnings (e.g., capping invalid config values)
	if sharedPorts.IsNilValue(logger) {
		var logErr error

		logger, logErr = buildLoggerFromConfig(cfg)
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
			// Fallback logger creation failed, but security enforcement must
			// still proceed. Use a no-op logger so enforcement logic executes
			// even though warnings won't be emitted.
			logger = &libLog.NopLogger{}
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

	if cfg.ObjectStorage.AllowInsecure {
		logger.Log(ctx, libLog.LevelWarn, fmt.Sprintf("SECURITY: OBJECT_STORAGE_ALLOW_INSECURE_ENDPOINT=true is not allowed in production. "+
			"Forcing insecure object storage endpoint support to disabled. env=%s", cfg.App.EnvName))

		cfg.ObjectStorage.AllowInsecure = false
	}
}

// sanitizeEnvVarsForConfig trims trailing whitespace from all environment variables
// referenced by Config struct tags. This is defense-in-depth against env vars with
// trailing whitespace (e.g., from shell quoting or process managers) that would cause
// strconv.Atoi to fail on "100  " instead of "100".
//
// This function mutates global process state via os.Setenv and is called exactly once
// during bootstrap (LoadConfig), before any goroutines are spawned. Do NOT call from
// concurrent contexts — it is not safe for concurrent use.
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

func loadConfigFromEnvForStartup(cfg *Config) error {
	if err := normalizeLegacyAuthEnvVars(); err != nil {
		return fmt.Errorf("normalize legacy auth env vars: %w", err)
	}

	// Trim trailing whitespace from all config-related env vars before parsing.
	// This is safe during bootstrap because it happens before background goroutines
	// are started. Handles edge cases like trailing whitespace from shell quoting
	// or process managers.
	sanitizeEnvVarsForConfig()

	return loadConfigFromEnv(cfg)
}

func normalizeLegacyAuthEnvVars() error {
	for _, alias := range legacyAuthEnvAliases {
		legacyRaw, legacySet := os.LookupEnv(alias.legacy)
		currentRaw, currentSet := os.LookupEnv(alias.current)

		legacyValue := strings.TrimSpace(legacyRaw)
		currentValue := strings.TrimSpace(currentRaw)

		if legacySet && currentSet && legacyValue != "" && currentValue != "" && envAliasValuesConflict(alias.kind, legacyValue, currentValue) {
			return fmt.Errorf(
				"%w: conflicting values for %s and %s",
				errConflictingEnvAliasValues,
				alias.legacy,
				alias.current,
			)
		}

		if legacySet && currentValue == "" {
			if err := os.Setenv(alias.current, legacyValue); err != nil {
				return fmt.Errorf("set %s from %s: %w", alias.current, alias.legacy, err)
			}
		}
	}

	return nil
}

func envAliasValuesConflict(kind envAliasKind, legacyValue, currentValue string) bool {
	normalizedLegacy, legacyNormalized := normalizeEnvAliasValue(kind, legacyValue)

	normalizedCurrent, currentNormalized := normalizeEnvAliasValue(kind, currentValue)

	if legacyNormalized && currentNormalized {
		return normalizedLegacy != normalizedCurrent
	}

	return legacyValue != currentValue
}

func normalizeEnvAliasValue(kind envAliasKind, value string) (string, bool) {
	trimmedValue := strings.TrimSpace(value)
	if trimmedValue == "" {
		return "", false
	}

	switch kind {
	case envAliasKindBool:
		parsedValue, err := strconv.ParseBool(trimmedValue)
		if err != nil {
			return "", false
		}

		return strconv.FormatBool(parsedValue), true
	default:
		return trimmedValue, true
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
	loadErr = errors.Join(loadErr, libCommons.SetConfigFromEnvVars(&cfg.Fetcher))

	// ShutdownGracePeriod is a top-level time.Duration field marked
	// mapstructure:"-" (bootstrap-only; not part of the runtime config plane).
	// lib-commons SetConfigFromEnvVars is only called against the sub-structs
	// above, so the env wiring here is explicit. SHUTDOWN_GRACE_PERIOD_SEC is
	// parsed as a positive integer number of seconds; non-positive or
	// unparseable values fall back to defaultShutdownGracePeriod via the
	// Shutdown path.
	if v := strings.TrimSpace(libCommons.GetenvOrDefault("SHUTDOWN_GRACE_PERIOD_SEC", "")); v != "" {
		if secs, err := strconv.Atoi(v); err == nil && secs > 0 {
			cfg.ShutdownGracePeriod = time.Duration(secs) * time.Second
		}
	}

	return loadErr
}
