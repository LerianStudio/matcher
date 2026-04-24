// Copyright 2025 Lerian Studio. All rights reserved.
// Use of this source code is governed by an Elastic License 2.0
// that can be found in the LICENSE.md file.

//go:build unit

package bootstrap

import (
	"testing"

	libLog "github.com/LerianStudio/lib-commons/v5/commons/log"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestLoadConfig_FromEnv(t *testing.T) {
	envVars := map[string]string{
		"ENV_NAME":                       "test",
		"POSTGRES_HOST":                  "testdb",
		"POSTGRES_PORT":                  "5433",
		"POSTGRES_USER":                  "testuser",
		"POSTGRES_PASSWORD":              "testpass",
		"POSTGRES_DB":                    "testdb",
		"POSTGRES_SSLMODE":               "disable",
		"DEFAULT_TENANT_ID":              "11111111-1111-1111-1111-111111111111",
		"DEFAULT_TENANT_SLUG":            "default",
		"HTTP_BODY_LIMIT_BYTES":          "1024",
		"LOG_LEVEL":                      "info",
		"RATE_LIMIT_MAX":                 "100",
		"RATE_LIMIT_EXPIRY_SEC":          "60",
		"EXPORT_RATE_LIMIT_MAX":          "10",
		"EXPORT_RATE_LIMIT_EXPIRY_SEC":   "60",
		"DISPATCH_RATE_LIMIT_MAX":        "100",
		"DISPATCH_RATE_LIMIT_EXPIRY_SEC": "60",
		"INFRA_CONNECT_TIMEOUT_SEC":      "30",
		"ARCHIVAL_WORKER_ENABLED":        "false",
	}

	for key, value := range envVars {
		t.Setenv(key, value)
	}

	cfg, err := LoadConfigWithLogger(nil)
	require.NoError(t, err)
	assert.Equal(t, "test", cfg.App.EnvName)
	assert.Equal(t, "testdb", cfg.Postgres.PrimaryHost)
	assert.Equal(t, "5433", cfg.Postgres.PrimaryPort)
}

func TestLoadConfig_InvalidIntEnv_Defaults(t *testing.T) {
	t.Setenv("REDIS_DB", "not-a-number")
	t.Setenv("DEFAULT_TENANT_ID", "11111111-1111-1111-1111-111111111111")
	t.Setenv("DEFAULT_TENANT_SLUG", "default")
	t.Setenv("HTTP_BODY_LIMIT_BYTES", "1024")
	t.Setenv("LOG_LEVEL", "info")
	t.Setenv("RATE_LIMIT_MAX", "100")
	t.Setenv("RATE_LIMIT_EXPIRY_SEC", "60")
	t.Setenv("EXPORT_RATE_LIMIT_MAX", "10")
	t.Setenv("EXPORT_RATE_LIMIT_EXPIRY_SEC", "60")
	t.Setenv("DISPATCH_RATE_LIMIT_MAX", "100")
	t.Setenv("DISPATCH_RATE_LIMIT_EXPIRY_SEC", "60")
	t.Setenv("INFRA_CONNECT_TIMEOUT_SEC", "30")
	t.Setenv("ARCHIVAL_WORKER_ENABLED", "false")

	cfg, err := LoadConfigWithLogger(nil)
	require.NoError(t, err)
	assert.Equal(t, 0, cfg.Redis.DB)
}

func TestConfig_LoadConfigWithLogger(t *testing.T) {
	t.Run("loads config with custom logger", func(t *testing.T) {
		t.Setenv("DEFAULT_TENANT_ID", "11111111-1111-1111-1111-111111111111")
		t.Setenv("DEFAULT_TENANT_SLUG", "default")
		t.Setenv("HTTP_BODY_LIMIT_BYTES", "1024")
		t.Setenv("LOG_LEVEL", "info")
		t.Setenv("RATE_LIMIT_MAX", "100")
		t.Setenv("RATE_LIMIT_EXPIRY_SEC", "60")
		t.Setenv("EXPORT_RATE_LIMIT_MAX", "10")
		t.Setenv("EXPORT_RATE_LIMIT_EXPIRY_SEC", "60")
		t.Setenv("DISPATCH_RATE_LIMIT_MAX", "100")
		t.Setenv("DISPATCH_RATE_LIMIT_EXPIRY_SEC", "60")
		t.Setenv("INFRA_CONNECT_TIMEOUT_SEC", "30")
		t.Setenv("ARCHIVAL_WORKER_ENABLED", "false")

		cfg, err := LoadConfigWithLogger(&libLog.NopLogger{})

		require.NoError(t, err)
		require.NotNil(t, cfg)
	})

	t.Run("applies default body limit when zero", func(t *testing.T) {
		t.Setenv("DEFAULT_TENANT_ID", "11111111-1111-1111-1111-111111111111")
		t.Setenv("DEFAULT_TENANT_SLUG", "default")
		t.Setenv("HTTP_BODY_LIMIT_BYTES", "0")
		t.Setenv("LOG_LEVEL", "info")
		t.Setenv("RATE_LIMIT_MAX", "100")
		t.Setenv("RATE_LIMIT_EXPIRY_SEC", "60")
		t.Setenv("EXPORT_RATE_LIMIT_MAX", "10")
		t.Setenv("EXPORT_RATE_LIMIT_EXPIRY_SEC", "60")
		t.Setenv("DISPATCH_RATE_LIMIT_MAX", "100")
		t.Setenv("DISPATCH_RATE_LIMIT_EXPIRY_SEC", "60")
		t.Setenv("INFRA_CONNECT_TIMEOUT_SEC", "30")
		t.Setenv("ARCHIVAL_WORKER_ENABLED", "false")

		cfg, err := LoadConfigWithLogger(&libLog.NopLogger{})

		require.NoError(t, err)
		require.NotNil(t, cfg)
		assert.Equal(t, defaultHTTPBodyLimitBytes, cfg.Server.BodyLimitBytes)
	})
}

func TestLoadConfigFromEnv_NilConfig(t *testing.T) {
	t.Parallel()

	err := loadConfigFromEnv(nil)

	require.Error(t, err)
	assert.ErrorIs(t, err, ErrConfigNil)
}

func TestErrConfigNil(t *testing.T) {
	t.Parallel()

	require.Error(t, ErrConfigNil)
	assert.Equal(t, "config must be provided", ErrConfigNil.Error())
}

func TestConfig_RabbitMQDSN_EdgeCases(t *testing.T) {
	t.Parallel()

	t.Run("handles empty vhost", func(t *testing.T) {
		t.Parallel()

		cfg := buildConfig(flatConfig{
			RabbitMQURI:      "amqp",
			RabbitMQHost:     "localhost",
			RabbitMQPort:     "5672",
			RabbitMQUser:     "guest",
			RabbitMQPassword: "guest",
			RabbitMQVHost:    "",
		})

		dsn := cfg.RabbitMQDSN()

		assert.Equal(t, "amqp://guest:guest@localhost:5672", dsn)
	})

	t.Run("handles whitespace-only vhost as default", func(t *testing.T) {
		t.Parallel()

		cfg := buildConfig(flatConfig{
			RabbitMQURI:      "amqp",
			RabbitMQHost:     "localhost",
			RabbitMQPort:     "5672",
			RabbitMQUser:     "guest",
			RabbitMQPassword: "guest",
			RabbitMQVHost:    "   ",
		})

		dsn := cfg.RabbitMQDSN()

		assert.Equal(t, "amqp://guest:guest@localhost:5672", dsn)
	})

	t.Run("handles vhost with leading slash", func(t *testing.T) {
		t.Parallel()

		cfg := buildConfig(flatConfig{
			RabbitMQURI:      "amqps",
			RabbitMQHost:     "rabbitmq.example.com",
			RabbitMQPort:     "5671",
			RabbitMQUser:     "matcher",
			RabbitMQPassword: "rmq-pr0d-s3cure!",
			RabbitMQVHost:    "/production",
		})

		dsn := cfg.RabbitMQDSN()

		assert.Contains(t, dsn, "/production")
		assert.NotContains(t, dsn, "//production")
	})

	t.Run("handles vhost without leading slash", func(t *testing.T) {
		t.Parallel()

		cfg := buildConfig(flatConfig{
			RabbitMQURI:      "amqp",
			RabbitMQHost:     "localhost",
			RabbitMQPort:     "5672",
			RabbitMQUser:     "user",
			RabbitMQPassword: "pass",
			RabbitMQVHost:    "myapp",
		})

		dsn := cfg.RabbitMQDSN()

		assert.Contains(t, dsn, "/myapp")
		assert.NotContains(t, dsn, "//myapp")
	})
}

