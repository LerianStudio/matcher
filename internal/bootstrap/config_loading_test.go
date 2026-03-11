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
