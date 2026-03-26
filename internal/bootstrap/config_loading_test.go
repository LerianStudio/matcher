// Copyright 2025 Lerian Studio. All rights reserved.
// Use of this source code is governed by an Elastic License 2.0
// that can be found in the LICENSE.md file.

//go:build unit

package bootstrap

import (
	"os"
	"testing"

	libLog "github.com/LerianStudio/lib-commons/v4/commons/log"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestLoadConfigWithLogger_NilLogger_CreatesDefault(t *testing.T) {
	// Not parallel: modifies env vars.
	clearConfigEnvVars(t)

	cfg, err := LoadConfigWithLogger(nil)
	require.NoError(t, err)
	require.NotNil(t, cfg)

	// The function should have created a default logger.
	assert.NotNil(t, cfg.Logger)
}

func TestLoadConfigWithLogger_ExplicitLogger_Used(t *testing.T) {
	// Not parallel: modifies env vars.
	clearConfigEnvVars(t)

	logger := &libLog.NopLogger{}

	cfg, err := LoadConfigWithLogger(logger)
	require.NoError(t, err)
	require.NotNil(t, cfg)

	// Should use the provided logger, not create a new one.
	assert.Equal(t, logger, cfg.Logger)
}

func TestLoadConfigWithLogger_TypedNilLogger_CreatesDefault(t *testing.T) {
	// Not parallel: modifies env vars.
	clearConfigEnvVars(t)

	var typedNil *libLog.NopLogger

	cfg, err := LoadConfigWithLogger(typedNil)
	require.NoError(t, err)
	require.NotNil(t, cfg)
	assert.NotNil(t, cfg.Logger)
}

func TestEnforceProductionSecurityDefaults_Production_DisablesSwagger(t *testing.T) {
	t.Parallel()

	cfg := defaultConfig()
	cfg.App.EnvName = "production"
	cfg.Swagger.Enabled = true
	cfg.RateLimit.Enabled = true

	logger := &libLog.NopLogger{}
	cfg.enforceProductionSecurityDefaults(logger)

	assert.False(t, cfg.Swagger.Enabled, "swagger must be disabled in production")
}

func TestEnforceProductionSecurityDefaults_Production_ForcesRateLimit(t *testing.T) {
	t.Parallel()

	cfg := defaultConfig()
	cfg.App.EnvName = "production"
	cfg.RateLimit.Enabled = false

	logger := &libLog.NopLogger{}
	cfg.enforceProductionSecurityDefaults(logger)

	assert.True(t, cfg.RateLimit.Enabled, "rate limiting must be forced on in production")
}

func TestEnforceProductionSecurityDefaults_NonProduction_NoChange(t *testing.T) {
	t.Parallel()

	cfg := defaultConfig()
	cfg.App.EnvName = "development"
	cfg.Swagger.Enabled = true
	cfg.RateLimit.Enabled = false

	logger := &libLog.NopLogger{}
	cfg.enforceProductionSecurityDefaults(logger)

	assert.True(t, cfg.Swagger.Enabled, "swagger should remain enabled in development")
	assert.False(t, cfg.RateLimit.Enabled, "rate limiting should remain disabled in development")
}

func TestEnforceProductionSecurityDefaults_NilLogger_DoesNotPanic(t *testing.T) {
	t.Parallel()

	cfg := defaultConfig()
	cfg.App.EnvName = "production"
	cfg.Swagger.Enabled = true

	assert.NotPanics(t, func() {
		cfg.enforceProductionSecurityDefaults(nil)
	})
}

func TestSanitizeEnvVarsForConfig_TrimsWhitespace(t *testing.T) {
	// Not parallel: modifies env vars.
	t.Setenv("RATE_LIMIT_MAX", "100  ")

	sanitizeEnvVarsForConfig()

	assert.Equal(t, "100", os.Getenv("RATE_LIMIT_MAX"))
}

func TestSanitizeEnvVarsForConfig_LeavesCleanValuesUnchanged(t *testing.T) {
	// Not parallel: modifies env vars.
	t.Setenv("RATE_LIMIT_MAX", "200")

	sanitizeEnvVarsForConfig()

	assert.Equal(t, "200", os.Getenv("RATE_LIMIT_MAX"))
}

func TestSanitizeEnvVarsForConfig_TrimsLeadingAndTrailing(t *testing.T) {
	// Not parallel: modifies env vars.
	t.Setenv("LOG_LEVEL", "  debug  ")

	sanitizeEnvVarsForConfig()

	assert.Equal(t, "debug", os.Getenv("LOG_LEVEL"))
}

func TestLoadConfigFromEnv_NilConfig_ReturnsError(t *testing.T) {
	t.Parallel()

	err := loadConfigFromEnv(nil)
	require.Error(t, err)
	assert.ErrorIs(t, err, ErrConfigNil)
}

func TestLoadConfigFromEnvForStartup_TrimsWhitespaceBeforeParsing(t *testing.T) {
	// Not parallel: modifies env vars.
	clearConfigEnvVars(t)
	t.Setenv("RATE_LIMIT_MAX", "100  ")

	cfg := defaultConfig()
	err := loadConfigFromEnvForStartup(cfg)
	require.NoError(t, err)
	assert.Equal(t, 100, cfg.RateLimit.Max)
	assert.Equal(t, "100", os.Getenv("RATE_LIMIT_MAX"))
}

func TestLoadConfigFromEnvForStartup_LegacyAuthEnvVarsPopulatePluginAliases(t *testing.T) {
	// Not parallel: modifies env vars.
	clearConfigEnvVars(t)
	t.Setenv("AUTH_ENABLED", "true")
	t.Setenv("AUTH_SERVICE_ADDRESS", "http://legacy-auth:8081")
	t.Setenv("AUTH_JWT_SECRET", "legacy-secret")

	cfg := defaultConfig()
	err := loadConfigFromEnvForStartup(cfg)
	require.NoError(t, err)
	assert.True(t, cfg.Auth.Enabled)
	assert.Equal(t, "http://legacy-auth:8081", cfg.Auth.Host)
	assert.Equal(t, "legacy-secret", cfg.Auth.TokenSecret)
	assert.Equal(t, "true", os.Getenv("PLUGIN_AUTH_ENABLED"))
	assert.Equal(t, "http://legacy-auth:8081", os.Getenv("PLUGIN_AUTH_ADDRESS"))
}

func TestLoadConfigFromEnvForStartup_CanonicalAuthEnvVarsLoadDirectly(t *testing.T) {
	// Not parallel: modifies env vars.
	clearConfigEnvVars(t)
	t.Setenv("PLUGIN_AUTH_ENABLED", "true")
	t.Setenv("PLUGIN_AUTH_ADDRESS", "http://canonical-auth:8081")
	t.Setenv("AUTH_JWT_SECRET", "canonical-secret")

	cfg := defaultConfig()
	err := loadConfigFromEnvForStartup(cfg)
	require.NoError(t, err)
	assert.True(t, cfg.Auth.Enabled)
	assert.Equal(t, "http://canonical-auth:8081", cfg.Auth.Host)
	assert.Equal(t, "canonical-secret", cfg.Auth.TokenSecret)
}

func TestLoadConfigFromEnvForStartup_SemanticallyEquivalentAuthEnabledAliasesPass(t *testing.T) {
	// Not parallel: modifies env vars.
	clearConfigEnvVars(t)
	t.Setenv("AUTH_ENABLED", "1")
	t.Setenv("PLUGIN_AUTH_ENABLED", "true")

	cfg := defaultConfig()
	err := loadConfigFromEnvForStartup(cfg)
	require.NoError(t, err)
	assert.True(t, cfg.Auth.Enabled)
}

func TestLoadConfigFromEnvForStartup_ConflictingAuthEnvVarsFail(t *testing.T) {
	// Not parallel: modifies env vars.
	clearConfigEnvVars(t)
	t.Setenv("AUTH_ENABLED", "true")
	t.Setenv("PLUGIN_AUTH_ENABLED", "false")

	cfg := defaultConfig()
	err := loadConfigFromEnvForStartup(cfg)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "conflicting values for AUTH_ENABLED and PLUGIN_AUTH_ENABLED")
}

func TestLoadConfigFromEnvForStartup_ConflictingAuthAddressAliasesFail(t *testing.T) {
	// Not parallel: modifies env vars.
	clearConfigEnvVars(t)
	t.Setenv("AUTH_SERVICE_ADDRESS", "http://legacy-auth:8081")
	t.Setenv("PLUGIN_AUTH_ADDRESS", "http://canonical-auth:8081")

	cfg := defaultConfig()
	err := loadConfigFromEnvForStartup(cfg)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "conflicting values for AUTH_SERVICE_ADDRESS and PLUGIN_AUTH_ADDRESS")
}

func TestClearConfigEnvVars_UnsetsLegacyAuthAliases(t *testing.T) {
	// Not parallel: modifies env vars.
	t.Setenv("AUTH_ENABLED", "true")
	t.Setenv("AUTH_SERVICE_ADDRESS", "http://legacy-auth:8081")

	clearConfigEnvVars(t)

	_, authEnabledSet := os.LookupEnv("AUTH_ENABLED")
	_, authServiceAddressSet := os.LookupEnv("AUTH_SERVICE_ADDRESS")
	assert.False(t, authEnabledSet)
	assert.False(t, authServiceAddressSet)
}

func TestLoadConfigFromEnv_DoesNotMutateProcessEnvironment(t *testing.T) {
	// Not parallel: modifies env vars.
	clearConfigEnvVars(t)
	t.Setenv("LOG_LEVEL", "debug  ")

	cfg := defaultConfig()
	err := loadConfigFromEnv(cfg)
	require.NoError(t, err)
	assert.Equal(t, "debug  ", os.Getenv("LOG_LEVEL"))
	assert.Equal(t, "debug  ", cfg.App.LogLevel)
}

func TestLoadConfigFromEnv_OverlaysFetcherConfig(t *testing.T) {
	// Not parallel: modifies env vars.
	clearConfigEnvVars(t)
	t.Setenv("FETCHER_ENABLED", "true")
	t.Setenv("FETCHER_URL", "http://fetcher.local:4006")
	t.Setenv("FETCHER_DISCOVERY_INTERVAL_SEC", "45")

	cfg := defaultConfig()
	err := loadConfigFromEnv(cfg)
	require.NoError(t, err)

	assert.True(t, cfg.Fetcher.Enabled)
	assert.Equal(t, "http://fetcher.local:4006", cfg.Fetcher.URL)
	assert.Equal(t, 45, cfg.Fetcher.DiscoveryIntervalSec)
}
