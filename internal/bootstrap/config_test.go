// Copyright 2025 Lerian Studio. All rights reserved.
// Use of this source code is governed by an Elastic License 2.0
// that can be found in the LICENSE.md file.

//go:build unit

package bootstrap

import (
	"testing"
	"time"

	libLog "github.com/LerianStudio/lib-commons/v4/commons/log"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

type flatConfig struct {
	EnvName                     string
	LogLevel                    string
	DefaultTenantID             string
	DefaultTenantSlug           string
	BodyLimitBytes              int
	CORSAllowedOrigins          string
	TLSTerminatedUpstream       bool
	ServerTLSCertFile           string
	ServerTLSKeyFile            string
	PrimaryDBHost               string
	PrimaryDBPort               string
	PrimaryDBUser               string
	PrimaryDBPassword           string
	PrimaryDBName               string
	PrimaryDBSSLMode            string
	ReplicaDBHost               string
	ReplicaDBPort               string
	ReplicaDBUser               string
	ReplicaDBPassword           string
	ReplicaDBName               string
	ReplicaDBSSLMode            string
	PostgresConnectTimeoutSec   int
	PostgresQueryTimeoutSec     int
	MaxOpenConnections          int
	MaxIdleConnections          int
	ConnMaxLifetimeMins         int
	ConnMaxIdleTimeMins         int
	RedisHost                   string
	RedisMasterName             string
	RedisPassword               string
	RedisDB                     int
	RedisProtocol               int
	RedisTLS                    bool
	RedisPoolSize               int
	RedisMinIdleConn            int
	RedisReadTimeoutMs          int
	RedisWriteTimeoutMs         int
	RedisDialTimeoutMs          int
	RabbitMQURI                 string
	RabbitMQHost                string
	RabbitMQPort                string
	RabbitMQUser                string
	RabbitMQPassword            string
	RabbitMQVHost               string
	RabbitMQHealthURL           string
	RabbitMQAllowInsecureHealth bool
	AuthEnabled                 bool
	AuthHost                    string
	AuthTokenSecret             string
	SwaggerEnabled              bool
	SwaggerHost                 string
	EnableTelemetry             bool
	OtelDeploymentEnv           string
	DBMetricsIntervalSec        int
	RateLimitEnabled            bool
	RateLimitMax                int
	RateLimitExpirySec          int
	ExportRateLimitMax          int
	ExportRateLimitExpirySec    int
	DispatchRateLimitMax        int
	DispatchRateLimitExpirySec  int
	InfraConnectTimeoutSec      int
	IdempotencyRetryWindowSec   int
	IdempotencySuccessTTLHours  int
	ExportWorkerPollIntervalSec int
	ObjectStorageEndpoint       string
	ObjectStorageBucket         string
	MultiTenantInfraEnabled     bool
	WebhookTimeoutSec           int
	ArchivalEnabled             bool
	ArchivalIntervalHours       int
	ArchivalHotRetentionDays    int
	ArchivalWarmRetentionMonths int
	ArchivalColdRetentionMonths int
	ArchivalBatchSize           int
	ArchivalStorageBucket       string
	ArchivalStoragePrefix       string
	ArchivalStorageClass        string
	ArchivalPartitionLookahead  int
	ArchivalPresignExpirySec    int
}

func buildConfig(fc flatConfig) Config {
	cfg := Config{}
	cfg.App.EnvName = fc.EnvName
	cfg.App.LogLevel = fc.LogLevel
	cfg.Tenancy.DefaultTenantID = fc.DefaultTenantID
	cfg.Tenancy.DefaultTenantSlug = fc.DefaultTenantSlug
	cfg.Tenancy.MultiTenantInfraEnabled = fc.MultiTenantInfraEnabled
	cfg.Server.BodyLimitBytes = fc.BodyLimitBytes
	cfg.Server.CORSAllowedOrigins = fc.CORSAllowedOrigins
	cfg.Server.TLSTerminatedUpstream = fc.TLSTerminatedUpstream
	cfg.Server.TLSCertFile = fc.ServerTLSCertFile
	cfg.Server.TLSKeyFile = fc.ServerTLSKeyFile
	cfg.Postgres.PrimaryHost = fc.PrimaryDBHost
	cfg.Postgres.PrimaryPort = fc.PrimaryDBPort
	cfg.Postgres.PrimaryUser = fc.PrimaryDBUser
	cfg.Postgres.PrimaryPassword = fc.PrimaryDBPassword
	cfg.Postgres.PrimaryDB = fc.PrimaryDBName
	cfg.Postgres.PrimarySSLMode = fc.PrimaryDBSSLMode
	cfg.Postgres.ReplicaHost = fc.ReplicaDBHost
	cfg.Postgres.ReplicaPort = fc.ReplicaDBPort
	cfg.Postgres.ReplicaUser = fc.ReplicaDBUser
	cfg.Postgres.ReplicaPassword = fc.ReplicaDBPassword
	cfg.Postgres.ReplicaDB = fc.ReplicaDBName
	cfg.Postgres.ReplicaSSLMode = fc.ReplicaDBSSLMode
	cfg.Postgres.ConnectTimeoutSec = fc.PostgresConnectTimeoutSec
	cfg.Postgres.QueryTimeoutSec = fc.PostgresQueryTimeoutSec
	cfg.Postgres.MaxOpenConnections = fc.MaxOpenConnections
	cfg.Postgres.MaxIdleConnections = fc.MaxIdleConnections
	cfg.Postgres.ConnMaxLifetimeMins = fc.ConnMaxLifetimeMins
	cfg.Postgres.ConnMaxIdleTimeMins = fc.ConnMaxIdleTimeMins
	cfg.Redis.Host = fc.RedisHost
	cfg.Redis.MasterName = fc.RedisMasterName
	cfg.Redis.Password = fc.RedisPassword
	cfg.Redis.DB = fc.RedisDB
	cfg.Redis.Protocol = fc.RedisProtocol
	cfg.Redis.TLS = fc.RedisTLS
	cfg.Redis.PoolSize = fc.RedisPoolSize
	cfg.Redis.MinIdleConn = fc.RedisMinIdleConn
	cfg.Redis.ReadTimeoutMs = fc.RedisReadTimeoutMs
	cfg.Redis.WriteTimeoutMs = fc.RedisWriteTimeoutMs
	cfg.Redis.DialTimeoutMs = fc.RedisDialTimeoutMs
	cfg.RabbitMQ.URI = fc.RabbitMQURI
	cfg.RabbitMQ.Host = fc.RabbitMQHost
	cfg.RabbitMQ.Port = fc.RabbitMQPort
	cfg.RabbitMQ.User = fc.RabbitMQUser
	cfg.RabbitMQ.Password = fc.RabbitMQPassword
	cfg.RabbitMQ.VHost = fc.RabbitMQVHost
	cfg.RabbitMQ.HealthURL = fc.RabbitMQHealthURL
	cfg.RabbitMQ.AllowInsecureHealthCheck = fc.RabbitMQAllowInsecureHealth
	cfg.Auth.Enabled = fc.AuthEnabled
	cfg.Auth.Host = fc.AuthHost
	cfg.Auth.TokenSecret = fc.AuthTokenSecret
	cfg.Swagger.Enabled = fc.SwaggerEnabled
	cfg.Swagger.Host = fc.SwaggerHost
	cfg.Telemetry.Enabled = fc.EnableTelemetry
	cfg.Telemetry.DeploymentEnv = fc.OtelDeploymentEnv
	cfg.Telemetry.DBMetricsIntervalSec = fc.DBMetricsIntervalSec
	cfg.RateLimit.Enabled = fc.RateLimitEnabled
	cfg.RateLimit.Max = fc.RateLimitMax
	cfg.RateLimit.ExpirySec = fc.RateLimitExpirySec
	cfg.RateLimit.ExportMax = fc.ExportRateLimitMax
	cfg.RateLimit.ExportExpirySec = fc.ExportRateLimitExpirySec
	cfg.RateLimit.DispatchMax = fc.DispatchRateLimitMax
	cfg.RateLimit.DispatchExpirySec = fc.DispatchRateLimitExpirySec
	cfg.Infrastructure.ConnectTimeoutSec = fc.InfraConnectTimeoutSec
	cfg.Idempotency.RetryWindowSec = fc.IdempotencyRetryWindowSec
	cfg.Idempotency.SuccessTTLHours = fc.IdempotencySuccessTTLHours
	cfg.ExportWorker.PollIntervalSec = fc.ExportWorkerPollIntervalSec
	cfg.ObjectStorage.Endpoint = fc.ObjectStorageEndpoint
	cfg.ObjectStorage.Bucket = fc.ObjectStorageBucket
	cfg.Webhook.TimeoutSec = fc.WebhookTimeoutSec
	cfg.Archival.Enabled = fc.ArchivalEnabled
	cfg.Archival.IntervalHours = fc.ArchivalIntervalHours
	cfg.Archival.StorageBucket = fc.ArchivalStorageBucket
	cfg.Archival.StoragePrefix = fc.ArchivalStoragePrefix
	cfg.Archival.StorageClass = fc.ArchivalStorageClass
	cfg.Archival.PresignExpirySec = fc.ArchivalPresignExpirySec

	// Apply archival defaults matching envDefault values to prevent
	// validation failures in tests that don't set archival fields.
	cfg.Archival.HotRetentionDays = applyDefault(fc.ArchivalHotRetentionDays, 90)
	cfg.Archival.WarmRetentionMonths = applyDefault(fc.ArchivalWarmRetentionMonths, 24)
	cfg.Archival.ColdRetentionMonths = applyDefault(fc.ArchivalColdRetentionMonths, 84)
	cfg.Archival.BatchSize = applyDefault(fc.ArchivalBatchSize, 5000)
	cfg.Archival.PartitionLookahead = applyDefault(fc.ArchivalPartitionLookahead, 3)

	return cfg
}

// applyDefault returns the value if non-zero, otherwise the default.
func applyDefault(value, defaultValue int) int {
	if value != 0 {
		return value
	}

	return defaultValue
}

func TestConfig_Validate(t *testing.T) {
	t.Parallel()

	t.Run("valid configs", func(t *testing.T) {
		t.Parallel()
		testConfigValidCases(t)
	})

	t.Run("production validations", func(t *testing.T) {
		t.Parallel()
		testConfigProductionValidations(t)
	})

	t.Run("auth validations", func(t *testing.T) {
		t.Parallel()
		testConfigAuthValidations(t)
	})

	t.Run("other validations", func(t *testing.T) {
		t.Parallel()
		testConfigOtherValidations(t)
	})
}

func testConfigValidCases(t *testing.T) {
	t.Helper()

	validTenantID := "11111111-1111-1111-1111-111111111111"

	tests := []struct {
		name   string
		config Config
	}{
		{
			name: "valid development config",
			config: buildConfig(flatConfig{
				EnvName:                  "development",
				DefaultTenantID:          validTenantID,
				PrimaryDBHost:            "localhost",
				PrimaryDBPort:            "5432",
				PrimaryDBUser:            "matcher",
				PrimaryDBName:            "matcher",
				PrimaryDBSSLMode:         "disable",
				BodyLimitBytes:           1024,
				LogLevel:                 "info",
				RateLimitMax:             100,
				RateLimitExpirySec:       60,
				ExportRateLimitMax:       10,
				ExportRateLimitExpirySec: 60,
				InfraConnectTimeoutSec:   30,
			}),
		},
		{
			name: "valid production config with password",
			config: buildConfig(flatConfig{
				EnvName:                  "production",
				DefaultTenantID:          validTenantID,
				PrimaryDBPassword:        "secret",
				RedisPassword:            "redis-secret",
				RabbitMQUser:             "matcher",
				RabbitMQPassword:         "secure",
				CORSAllowedOrigins:       "https://example.com",
				BodyLimitBytes:           1024,
				LogLevel:                 "info",
				EnableTelemetry:          true,
				OtelDeploymentEnv:        "production",
				RateLimitMax:             100,
				RateLimitExpirySec:       60,
				ExportRateLimitMax:       10,
				ExportRateLimitExpirySec: 60,
				TLSTerminatedUpstream:    true,
				InfraConnectTimeoutSec:   30,
			}),
		},
		{
			name: "valid config with auth",
			config: buildConfig(flatConfig{
				EnvName:                  "development",
				DefaultTenantID:          validTenantID,
				AuthEnabled:              true,
				AuthHost:                 "http://auth:8080",
				AuthTokenSecret:          "secret",
				BodyLimitBytes:           1024,
				LogLevel:                 "info",
				RateLimitMax:             100,
				RateLimitExpirySec:       60,
				ExportRateLimitMax:       10,
				ExportRateLimitExpirySec: 60,
				InfraConnectTimeoutSec:   30,
			}),
		},
		{
			name: "valid otel deployment env when otel enabled",
			config: buildConfig(flatConfig{
				EnvName:                  "development",
				DefaultTenantID:          validTenantID,
				BodyLimitBytes:           1024,
				LogLevel:                 "info",
				EnableTelemetry:          true,
				OtelDeploymentEnv:        "staging",
				RateLimitMax:             100,
				RateLimitExpirySec:       60,
				ExportRateLimitMax:       10,
				ExportRateLimitExpirySec: 60,
				InfraConnectTimeoutSec:   30,
			}),
		},
		{
			name: "otel deployment env not validated when otel disabled",
			config: buildConfig(flatConfig{
				EnvName:                  "development",
				DefaultTenantID:          validTenantID,
				BodyLimitBytes:           1024,
				LogLevel:                 "info",
				EnableTelemetry:          false,
				OtelDeploymentEnv:        "invalid",
				RateLimitMax:             100,
				RateLimitExpirySec:       60,
				ExportRateLimitMax:       10,
				ExportRateLimitExpirySec: 60,
				InfraConnectTimeoutSec:   30,
			}),
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			err := tt.config.Validate()
			require.NoError(t, err)
		})
	}
}

func testConfigProductionValidations(t *testing.T) {
	t.Helper()

	validTenantID := "11111111-1111-1111-1111-111111111111"

	tests := []struct {
		name   string
		config Config
		errMsg string
	}{
		{
			name: "production requires password",
			config: buildConfig(flatConfig{
				EnvName:            "production",
				DefaultTenantID:    validTenantID,
				PrimaryDBPassword:  "",
				RabbitMQUser:       "matcher",
				RabbitMQPassword:   "secure",
				CORSAllowedOrigins: "https://example.com",
				BodyLimitBytes:     1024,
				LogLevel:           "info",
			}),
			errMsg: "POSTGRES_PASSWORD is required in production",
		},
		{
			name: "production requires non-default rabbitmq credentials",
			config: buildConfig(flatConfig{
				EnvName:            "production",
				DefaultTenantID:    validTenantID,
				PrimaryDBPassword:  "secret",
				RabbitMQUser:       "guest",
				RabbitMQPassword:   "guest",
				CORSAllowedOrigins: "https://example.com",
				BodyLimitBytes:     1024,
				LogLevel:           "info",
			}),
			errMsg: "RABBITMQ credentials must be set to non-default values in production",
		},
		{
			name: "production requires restricted CORS",
			config: buildConfig(flatConfig{
				EnvName:            "production",
				DefaultTenantID:    validTenantID,
				PrimaryDBPassword:  "secret",
				RabbitMQUser:       "matcher",
				RabbitMQPassword:   "secure",
				CORSAllowedOrigins: "*",
				BodyLimitBytes:     1024,
				LogLevel:           "info",
			}),
			errMsg: "CORS_ALLOWED_ORIGINS must be restricted in production",
		},
		{
			name: "production requires insecure health check disabled",
			config: buildConfig(flatConfig{
				EnvName:                     "production",
				DefaultTenantID:             validTenantID,
				PrimaryDBPassword:           "secret",
				PrimaryDBSSLMode:            "require",
				RedisTLS:                    true,
				RedisPassword:               "redis-secret",
				RabbitMQURI:                 "amqps",
				RabbitMQUser:                "matcher",
				RabbitMQPassword:            "secure",
				RabbitMQAllowInsecureHealth: true,
				AuthEnabled:                 true,
				AuthHost:                    "http://auth:8080",
				AuthTokenSecret:             "secret",
				CORSAllowedOrigins:          "https://example.com",
				BodyLimitBytes:              1024,
				LogLevel:                    "info",
				TLSTerminatedUpstream:       true,
			}),
			errMsg: "RABBITMQ_ALLOW_INSECURE_HEALTH_CHECK must be false in production",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			err := tt.config.Validate()
			require.Error(t, err)
			assert.Contains(t, err.Error(), tt.errMsg)
		})
	}
}

func testConfigAuthValidations(t *testing.T) {
	t.Helper()

	validTenantID := "11111111-1111-1111-1111-111111111111"

	tests := []struct {
		name   string
		config Config
		errMsg string
	}{
		{
			name: "auth enabled requires host",
			config: buildConfig(flatConfig{
				EnvName:         "development",
				DefaultTenantID: validTenantID,
				AuthEnabled:     true,
				AuthHost:        "",
				AuthTokenSecret: "secret",
				BodyLimitBytes:  1024,
				LogLevel:        "info",
			}),
			errMsg: "AUTH_SERVICE_ADDRESS is required when AUTH_ENABLED=true",
		},
		{
			name: "auth enabled requires token secret",
			config: buildConfig(flatConfig{
				EnvName:         "development",
				DefaultTenantID: validTenantID,
				AuthEnabled:     true,
				AuthHost:        "http://auth:8080",
				BodyLimitBytes:  1024,
				LogLevel:        "info",
			}),
			errMsg: "AUTH_JWT_SECRET is required when AUTH_ENABLED=true",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			err := tt.config.Validate()
			require.Error(t, err)
			assert.Contains(t, err.Error(), tt.errMsg)
		})
	}
}

func testConfigOtherValidations(t *testing.T) {
	t.Helper()

	validTenantID := "11111111-1111-1111-1111-111111111111"

	tests := []struct {
		name   string
		config Config
		errMsg string
	}{
		{
			name: "invalid default tenant id",
			config: buildConfig(flatConfig{
				EnvName:         "development",
				DefaultTenantID: "default",
				BodyLimitBytes:  1024,
				LogLevel:        "info",
			}),
			errMsg: "DEFAULT_TENANT_ID must be a valid UUID",
		},
		{
			name: "tls cert and key must be paired",
			config: buildConfig(flatConfig{
				EnvName:           "development",
				DefaultTenantID:   validTenantID,
				ServerTLSCertFile: "/etc/tls/cert.pem",
				BodyLimitBytes:    1024,
				LogLevel:          "info",
			}),
			errMsg: "SERVER_TLS_CERT_FILE and SERVER_TLS_KEY_FILE must be set together",
		},
		{
			name: "body limit must be positive - zero",
			config: buildConfig(flatConfig{
				EnvName:         "development",
				DefaultTenantID: validTenantID,
				BodyLimitBytes:  0,
				LogLevel:        "info",
			}),
			errMsg: "HTTP_BODY_LIMIT_BYTES must be positive",
		},
		{
			name: "body limit must be positive - negative",
			config: buildConfig(flatConfig{
				EnvName:         "development",
				DefaultTenantID: validTenantID,
				BodyLimitBytes:  -100,
				LogLevel:        "info",
			}),
			errMsg: "HTTP_BODY_LIMIT_BYTES must be positive",
		},
		{
			name: "invalid log level",
			config: buildConfig(flatConfig{
				EnvName:                "development",
				DefaultTenantID:        validTenantID,
				BodyLimitBytes:         1024,
				LogLevel:               "invalid",
				InfraConnectTimeoutSec: 30,
			}),
			errMsg: "LOG_LEVEL must be one of: debug, info, warn, error, fatal",
		},
		{
			name: "invalid otel deployment env when otel enabled",
			config: buildConfig(flatConfig{
				EnvName:                "development",
				DefaultTenantID:        validTenantID,
				BodyLimitBytes:         1024,
				LogLevel:               "info",
				EnableTelemetry:        true,
				OtelDeploymentEnv:      "invalid",
				InfraConnectTimeoutSec: 30,
			}),
			errMsg: "OTEL_RESOURCE_DEPLOYMENT_ENVIRONMENT must be one of: development, staging, production",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			err := tt.config.Validate()
			require.Error(t, err)
			assert.Contains(t, err.Error(), tt.errMsg)
		})
	}
}

func TestConfig_PrimaryDSN(t *testing.T) {
	t.Parallel()

	cfg := buildConfig(flatConfig{
		PrimaryDBHost:             "localhost",
		PrimaryDBPort:             "5432",
		PrimaryDBUser:             "matcher",
		PrimaryDBPassword:         "secret",
		PrimaryDBName:             "matcher_db",
		PrimaryDBSSLMode:          "disable",
		PostgresConnectTimeoutSec: 10,
	})

	expected := "host=localhost port=5432 user=matcher password=secret dbname=matcher_db sslmode=disable connect_timeout=10"
	assert.Equal(t, expected, cfg.PrimaryDSN())
}

func TestConfig_ReplicaDSN_FallbackToPrimary(t *testing.T) {
	t.Parallel()

	cfg := buildConfig(flatConfig{
		PrimaryDBHost:             "primary.db",
		PrimaryDBPort:             "5432",
		PrimaryDBUser:             "matcher",
		PrimaryDBPassword:         "secret",
		PrimaryDBName:             "matcher_db",
		PrimaryDBSSLMode:          "require",
		PostgresConnectTimeoutSec: 10,
		ReplicaDBHost:             "",
	})

	assert.Equal(t, cfg.PrimaryDSN(), cfg.ReplicaDSN())
}

func TestConfig_ReplicaDSN_WithReplica(t *testing.T) {
	t.Parallel()

	cfg := buildConfig(flatConfig{
		PrimaryDBHost:             "primary.db",
		PrimaryDBPort:             "5432",
		PrimaryDBUser:             "matcher",
		PrimaryDBPassword:         "secret",
		PrimaryDBName:             "matcher_db",
		PrimaryDBSSLMode:          "require",
		PostgresConnectTimeoutSec: 10,
		ReplicaDBHost:             "replica.db",
		ReplicaDBPort:             "5433",
	})

	expected := "host=replica.db port=5433 user=matcher password=secret dbname=matcher_db sslmode=require connect_timeout=10"
	assert.Equal(t, expected, cfg.ReplicaDSN())
}

func TestConfig_RabbitMQDSN(t *testing.T) {
	t.Parallel()

	t.Run("encodes default vhost slash", func(t *testing.T) {
		t.Parallel()

		cfg := buildConfig(flatConfig{
			RabbitMQURI:      "amqp",
			RabbitMQHost:     "localhost",
			RabbitMQPort:     "5672",
			RabbitMQUser:     "guest",
			RabbitMQPassword: "guest",
			RabbitMQVHost:    "/",
		})

		expected := "amqp://guest:guest@localhost:5672/%2F"
		assert.Equal(t, expected, cfg.RabbitMQDSN())
	})

	t.Run("encodes credentials and vhost per RFC3986", func(t *testing.T) {
		t.Parallel()

		cfg := buildConfig(flatConfig{
			RabbitMQURI:      "amqp",
			RabbitMQHost:     "localhost",
			RabbitMQPort:     "5672",
			RabbitMQUser:     "us:er",
			RabbitMQPassword: "p@ss:word",
			RabbitMQVHost:    "my/vhost",
		})

		expected := "amqp://us%3Aer:p%40ss%3Aword@localhost:5672/my%2Fvhost"
		assert.Equal(t, expected, cfg.RabbitMQDSN())
	})

	t.Run("omits password separator when password empty", func(t *testing.T) {
		t.Parallel()

		cfg := buildConfig(flatConfig{
			RabbitMQURI:      "amqp",
			RabbitMQHost:     "localhost",
			RabbitMQPort:     "5672",
			RabbitMQUser:     "user",
			RabbitMQPassword: "",
			RabbitMQVHost:    "vhost",
		})

		expected := "amqp://user@localhost:5672/vhost"
		assert.Equal(t, expected, cfg.RabbitMQDSN())
	})
}

//nolint:paralleltest // Cannot use t.Parallel() with t.Setenv
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

func TestConfig_DBMetricsInterval(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name     string
		interval int
		expected time.Duration
	}{
		{
			name:     "positive value returns configured duration",
			interval: 30,
			expected: 30 * time.Second,
		},
		{
			name:     "zero returns minimum 1 second",
			interval: 0,
			expected: time.Second,
		},
		{
			name:     "negative returns minimum 1 second",
			interval: -10,
			expected: time.Second,
		},
		{
			name:     "default value 15 seconds",
			interval: 15,
			expected: 15 * time.Second,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			cfg := buildConfig(flatConfig{DBMetricsIntervalSec: tt.interval})
			assert.Equal(t, tt.expected, cfg.DBMetricsInterval())
		})
	}
}

func TestConfig_IdempotencyRetryWindow(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name     string
		window   int
		expected time.Duration
	}{
		{
			name:     "positive value returns configured duration",
			window:   600,
			expected: 600 * time.Second,
		},
		{
			name:     "zero returns minimum 1 minute",
			window:   0,
			expected: time.Minute,
		},
		{
			name:     "negative returns minimum 1 minute",
			window:   -100,
			expected: time.Minute,
		},
		{
			name:     "default value 300 seconds (5 minutes)",
			window:   300,
			expected: 5 * time.Minute,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			cfg := buildConfig(flatConfig{IdempotencyRetryWindowSec: tt.window})
			assert.Equal(t, tt.expected, cfg.IdempotencyRetryWindow())
		})
	}
}

func TestConfig_IdempotencySuccessTTL(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name     string
		hours    int
		expected time.Duration
	}{
		{
			name:     "positive value returns configured duration",
			hours:    48,
			expected: 48 * time.Hour,
		},
		{
			name:     "zero returns minimum 1 hour",
			hours:    0,
			expected: time.Hour,
		},
		{
			name:     "negative returns minimum 1 hour",
			hours:    -10,
			expected: time.Hour,
		},
		{
			name:     "default value 168 hours (7 days)",
			hours:    168,
			expected: 168 * time.Hour,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			cfg := buildConfig(flatConfig{IdempotencySuccessTTLHours: tt.hours})
			assert.Equal(t, tt.expected, cfg.IdempotencySuccessTTL())
		})
	}
}

func TestConfig_InfraConnectTimeout(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name     string
		timeout  int
		expected time.Duration
	}{
		{
			name:     "default 30 seconds",
			timeout:  30,
			expected: 30 * time.Second,
		},
		{
			name:     "custom 60 seconds",
			timeout:  60,
			expected: 60 * time.Second,
		},
		{
			name:     "lower timeout for fast-fail",
			timeout:  10,
			expected: 10 * time.Second,
		},
		{
			name:     "zero value returns minimum duration",
			timeout:  0,
			expected: 1 * time.Second,
		},
		{
			name:     "negative value returns minimum duration",
			timeout:  -1,
			expected: 1 * time.Second,
		},
		{
			name:     "caps absurdly high values",
			timeout:  9999,
			expected: 300 * time.Second,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			cfg := buildConfig(flatConfig{InfraConnectTimeoutSec: tt.timeout})
			assert.Equal(t, tt.expected, cfg.InfraConnectTimeout())
		})
	}
}

func TestConfig_ProductionRedisPasswordValidation(t *testing.T) {
	t.Parallel()

	validTenantID := "11111111-1111-1111-1111-111111111111"

	t.Run("requires redis password in production", func(t *testing.T) {
		t.Parallel()

		cfg := buildConfig(flatConfig{
			EnvName:                "production",
			DefaultTenantID:        validTenantID,
			PrimaryDBPassword:      "dbsecret",
			RedisHost:              "redis.example.com",
			RedisPassword:          "",
			RabbitMQUser:           "matcher",
			RabbitMQPassword:       "rabbitsecret",
			CORSAllowedOrigins:     "https://example.com",
			BodyLimitBytes:         1024,
			LogLevel:               "info",
			InfraConnectTimeoutSec: 30,
		})

		err := cfg.Validate()
		require.Error(t, err)
		assert.Contains(
			t,
			err.Error(),
			"REDIS_PASSWORD is required in production",
		)
	})

	t.Run("requires redis password even when host is empty", func(t *testing.T) {
		t.Parallel()

		cfg := buildConfig(flatConfig{
			EnvName:                  "production",
			DefaultTenantID:          validTenantID,
			PrimaryDBPassword:        "dbsecret",
			RedisHost:                "",
			RabbitMQUser:             "matcher",
			RabbitMQPassword:         "rabbitsecret",
			CORSAllowedOrigins:       "https://example.com",
			BodyLimitBytes:           1024,
			LogLevel:                 "info",
			RateLimitMax:             100,
			RateLimitExpirySec:       60,
			ExportRateLimitMax:       10,
			ExportRateLimitExpirySec: 60,
			InfraConnectTimeoutSec:   30,
		})

		err := cfg.Validate()
		require.Error(t, err)
		assert.Contains(t, err.Error(), "REDIS_PASSWORD is required in production")
	})

	t.Run("passes when redis password provided", func(t *testing.T) {
		t.Parallel()

		cfg := buildConfig(flatConfig{
			EnvName:                  "production",
			DefaultTenantID:          validTenantID,
			PrimaryDBPassword:        "dbsecret",
			RedisHost:                "redis.example.com",
			RedisPassword:            "redissecret",
			RabbitMQUser:             "matcher",
			RabbitMQPassword:         "rabbitsecret",
			CORSAllowedOrigins:       "https://example.com",
			BodyLimitBytes:           1024,
			LogLevel:                 "info",
			RateLimitMax:             100,
			RateLimitExpirySec:       60,
			ExportRateLimitMax:       10,
			ExportRateLimitExpirySec: 60,
			InfraConnectTimeoutSec:   30,
		})

		err := cfg.Validate()
		require.NoError(t, err)
	})
}

func TestConfig_ProductionRateLimitEnforcement(t *testing.T) {
	t.Parallel()

	validTenantID := "11111111-1111-1111-1111-111111111111"

	t.Run("enforces rate limiting when disabled in production", func(t *testing.T) {
		t.Parallel()

		cfg := buildConfig(flatConfig{
			EnvName:          envProduction,
			DefaultTenantID:  validTenantID,
			RateLimitEnabled: false, // Attempting to disable rate limiting
		})

		// Call enforcement directly
		cfg.enforceProductionSecurityDefaults(nil)

		// Rate limiting should be forced to enabled
		assert.True(
			t,
			cfg.RateLimit.Enabled,
			"rate limiting should be forced to enabled in production",
		)
	})

	t.Run("does not modify rate limiting when already enabled in production", func(t *testing.T) {
		t.Parallel()

		cfg := buildConfig(flatConfig{
			EnvName:          envProduction,
			DefaultTenantID:  validTenantID,
			RateLimitEnabled: true,
		})

		cfg.enforceProductionSecurityDefaults(nil)

		assert.True(t, cfg.RateLimit.Enabled)
	})

	t.Run("allows disabling rate limiting in non-production environments", func(t *testing.T) {
		t.Parallel()

		environments := []string{"development", "staging", "test", ""}

		for _, env := range environments {
			cfg := buildConfig(flatConfig{
				EnvName:          env,
				DefaultTenantID:  validTenantID,
				RateLimitEnabled: false,
			})

			cfg.enforceProductionSecurityDefaults(nil)

			assert.False(
				t,
				cfg.RateLimit.Enabled,
				"rate limiting should remain disabled in %s environment",
				env,
			)
		}
	})
}

func TestConfig_ReplicaDSN_ExtendedCases(t *testing.T) {
	t.Parallel()

	t.Run("uses replica settings when configured", func(t *testing.T) {
		t.Parallel()

		cfg := buildConfig(flatConfig{
			PrimaryDBHost:             "primary-host",
			PrimaryDBPort:             "5432",
			PrimaryDBUser:             "matcher",
			PrimaryDBPassword:         "secret",
			PrimaryDBName:             "matcher_db",
			PrimaryDBSSLMode:          "require",
			PostgresConnectTimeoutSec: 10,
			ReplicaDBHost:             "replica-host",
			ReplicaDBPort:             "5433",
			ReplicaDBUser:             "replica_user",
			ReplicaDBPassword:         "replica_secret",
			ReplicaDBName:             "replica_db",
			ReplicaDBSSLMode:          "disable",
		})

		dsn := cfg.ReplicaDSN()

		assert.Contains(t, dsn, "host=replica-host")
		assert.Contains(t, dsn, "port=5433")
		assert.Contains(t, dsn, "user=replica_user")
		assert.Contains(t, dsn, "password=replica_secret")
		assert.Contains(t, dsn, "dbname=replica_db")
		assert.Contains(t, dsn, "sslmode=disable")
	})

	t.Run("uses primary fallbacks for empty replica fields", func(t *testing.T) {
		t.Parallel()

		cfg := buildConfig(flatConfig{
			PrimaryDBHost:             "primary-host",
			PrimaryDBPort:             "5432",
			PrimaryDBUser:             "matcher",
			PrimaryDBPassword:         "secret",
			PrimaryDBName:             "matcher_db",
			PrimaryDBSSLMode:          "require",
			PostgresConnectTimeoutSec: 10,
			ReplicaDBHost:             "replica-host",
		})

		dsn := cfg.ReplicaDSN()

		assert.Contains(t, dsn, "host=replica-host")
		assert.Contains(t, dsn, "port=5432")
		assert.Contains(t, dsn, "user=matcher")
		assert.Contains(t, dsn, "password=secret")
		assert.Contains(t, dsn, "dbname=matcher_db")
		assert.Contains(t, dsn, "sslmode=require")
	})
}

func TestConfig_PrimaryDSNMasked(t *testing.T) {
	t.Parallel()

	t.Run("redacts password", func(t *testing.T) {
		t.Parallel()

		cfg := buildConfig(flatConfig{
			PrimaryDBHost:     "localhost",
			PrimaryDBPort:     "5432",
			PrimaryDBUser:     "matcher",
			PrimaryDBPassword: "super_secret_password",
			PrimaryDBName:     "matcher_db",
			PrimaryDBSSLMode:  "require",
		})

		dsn := cfg.PrimaryDSNMasked()

		assert.Contains(t, dsn, "password=***REDACTED***")
		assert.NotContains(t, dsn, "super_secret_password")
	})
}

func TestConfig_ReplicaDSNMasked(t *testing.T) {
	t.Parallel()

	t.Run("falls back to primary masked when replica not configured", func(t *testing.T) {
		t.Parallel()

		cfg := buildConfig(flatConfig{
			PrimaryDBHost:     "primary-host",
			PrimaryDBPort:     "5432",
			PrimaryDBUser:     "matcher",
			PrimaryDBPassword: "secret",
			PrimaryDBName:     "matcher_db",
			PrimaryDBSSLMode:  "require",
			ReplicaDBHost:     "",
		})

		dsn := cfg.ReplicaDSNMasked()

		assert.Contains(t, dsn, "host=primary-host")
		assert.Contains(t, dsn, "password=***REDACTED***")
	})

	t.Run("uses replica settings with masked password", func(t *testing.T) {
		t.Parallel()

		cfg := buildConfig(flatConfig{
			PrimaryDBHost:     "primary-host",
			PrimaryDBPort:     "5432",
			PrimaryDBUser:     "matcher",
			PrimaryDBPassword: "secret",
			PrimaryDBName:     "matcher_db",
			PrimaryDBSSLMode:  "require",
			ReplicaDBHost:     "replica-host",
			ReplicaDBPort:     "5433",
			ReplicaDBUser:     "replica_user",
			ReplicaDBPassword: "replica_secret",
			ReplicaDBName:     "replica_db",
			ReplicaDBSSLMode:  "disable",
		})

		dsn := cfg.ReplicaDSNMasked()

		assert.Contains(t, dsn, "host=replica-host")
		assert.Contains(t, dsn, "password=***REDACTED***")
		assert.NotContains(t, dsn, "replica_secret")
	})
}

func TestConfig_RedisTimeouts(t *testing.T) {
	t.Parallel()

	t.Run("RedisReadTimeout returns correct duration", func(t *testing.T) {
		t.Parallel()

		cfg := buildConfig(flatConfig{RedisReadTimeoutMs: 3000})
		assert.Equal(t, 3000*time.Millisecond, cfg.RedisReadTimeout())
	})

	t.Run("RedisWriteTimeout returns correct duration", func(t *testing.T) {
		t.Parallel()

		cfg := buildConfig(flatConfig{RedisWriteTimeoutMs: 2000})
		assert.Equal(t, 2000*time.Millisecond, cfg.RedisWriteTimeout())
	})

	t.Run("RedisDialTimeout returns correct duration", func(t *testing.T) {
		t.Parallel()

		cfg := buildConfig(flatConfig{RedisDialTimeoutMs: 5000})
		assert.Equal(t, 5000*time.Millisecond, cfg.RedisDialTimeout())
	})
}

func TestConfig_ConnMaxLifetimeAndIdleTime(t *testing.T) {
	t.Parallel()

	t.Run("ConnMaxLifetime returns correct duration", func(t *testing.T) {
		t.Parallel()

		cfg := buildConfig(flatConfig{ConnMaxLifetimeMins: 30})
		assert.Equal(t, 30*time.Minute, cfg.ConnMaxLifetime())
	})

	t.Run("ConnMaxIdleTime returns correct duration", func(t *testing.T) {
		t.Parallel()

		cfg := buildConfig(flatConfig{ConnMaxIdleTimeMins: 5})
		assert.Equal(t, 5*time.Minute, cfg.ConnMaxIdleTime())
	})
}

func TestConfig_ExportWorkerPollInterval(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name     string
		interval int
		expected time.Duration
	}{
		{
			name:     "positive value returns configured duration",
			interval: 10,
			expected: 10 * time.Second,
		},
		{
			name:     "zero returns default 5 seconds",
			interval: 0,
			expected: 5 * time.Second,
		},
		{
			name:     "negative returns default 5 seconds",
			interval: -5,
			expected: 5 * time.Second,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			cfg := buildConfig(flatConfig{ExportWorkerPollIntervalSec: tt.interval})
			assert.Equal(t, tt.expected, cfg.ExportWorkerPollInterval())
		})
	}
}

func TestConfig_WebhookTimeout(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name     string
		timeout  int
		expected time.Duration
	}{
		{
			name:     "positive value returns configured duration",
			timeout:  15,
			expected: 15 * time.Second,
		},
		{
			name:     "default value 30 seconds",
			timeout:  30,
			expected: 30 * time.Second,
		},
		{
			name:     "zero returns default 30 seconds",
			timeout:  0,
			expected: 30 * time.Second,
		},
		{
			name:     "negative returns default 30 seconds",
			timeout:  -10,
			expected: 30 * time.Second,
		},
		{
			name:     "caps at 300 seconds maximum",
			timeout:  600,
			expected: 300 * time.Second,
		},
		{
			name:     "exactly at maximum 300 seconds",
			timeout:  300,
			expected: 300 * time.Second,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			cfg := buildConfig(flatConfig{WebhookTimeoutSec: tt.timeout})
			assert.Equal(t, tt.expected, cfg.WebhookTimeout())
		})
	}
}

func TestConfig_WebhookTimeout_LogsWarningOnCap(t *testing.T) {
	t.Parallel()

	cfg := buildConfig(flatConfig{WebhookTimeoutSec: 600})
	cfg.Logger = &libLog.NopLogger{}

	result := cfg.WebhookTimeout()
	assert.Equal(t, 300*time.Second, result)
}

func TestConfig_ValidateServerConfig_TLSValidation(t *testing.T) {
	t.Parallel()

	validTenantID := "11111111-1111-1111-1111-111111111111"

	t.Run("fails when only cert file is set", func(t *testing.T) {
		t.Parallel()

		cfg := buildConfig(flatConfig{
			EnvName:                  "development",
			DefaultTenantID:          validTenantID,
			BodyLimitBytes:           1024,
			LogLevel:                 "info",
			RateLimitMax:             100,
			RateLimitExpirySec:       60,
			ExportRateLimitMax:       10,
			ExportRateLimitExpirySec: 60,
			InfraConnectTimeoutSec:   30,
			ServerTLSCertFile:        "/path/to/cert.pem",
			ServerTLSKeyFile:         "",
		})

		err := cfg.Validate()
		require.Error(t, err)
		assert.Contains(
			t,
			err.Error(),
			"SERVER_TLS_CERT_FILE and SERVER_TLS_KEY_FILE must be set together",
		)
	})

	t.Run("fails when only key file is set", func(t *testing.T) {
		t.Parallel()

		cfg := buildConfig(flatConfig{
			EnvName:                  "development",
			DefaultTenantID:          validTenantID,
			BodyLimitBytes:           1024,
			LogLevel:                 "info",
			RateLimitMax:             100,
			RateLimitExpirySec:       60,
			ExportRateLimitMax:       10,
			ExportRateLimitExpirySec: 60,
			InfraConnectTimeoutSec:   30,
			ServerTLSCertFile:        "",
			ServerTLSKeyFile:         "/path/to/key.pem",
		})

		err := cfg.Validate()
		require.Error(t, err)
		assert.Contains(
			t,
			err.Error(),
			"SERVER_TLS_CERT_FILE and SERVER_TLS_KEY_FILE must be set together",
		)
	})

	t.Run("passes when both TLS files are set", func(t *testing.T) {
		t.Parallel()

		cfg := buildConfig(flatConfig{
			EnvName:                  "development",
			DefaultTenantID:          validTenantID,
			BodyLimitBytes:           1024,
			LogLevel:                 "info",
			RateLimitMax:             100,
			RateLimitExpirySec:       60,
			ExportRateLimitMax:       10,
			ExportRateLimitExpirySec: 60,
			InfraConnectTimeoutSec:   30,
			ServerTLSCertFile:        "/path/to/cert.pem",
			ServerTLSKeyFile:         "/path/to/key.pem",
		})

		err := cfg.Validate()
		require.NoError(t, err)
	})
}

func TestConfig_ValidateInvalidLogLevel(t *testing.T) {
	t.Parallel()

	validTenantID := "11111111-1111-1111-1111-111111111111"

	cfg := buildConfig(flatConfig{
		EnvName:                  "development",
		DefaultTenantID:          validTenantID,
		BodyLimitBytes:           1024,
		LogLevel:                 "invalid_level",
		RateLimitMax:             100,
		RateLimitExpirySec:       60,
		ExportRateLimitMax:       10,
		ExportRateLimitExpirySec: 60,
		InfraConnectTimeoutSec:   30,
	})

	err := cfg.Validate()
	require.Error(t, err)
	assert.Contains(t, err.Error(), "LOG_LEVEL must be one of")
}

func TestConfig_ValidateInvalidOtelDeploymentEnv(t *testing.T) {
	t.Parallel()

	validTenantID := "11111111-1111-1111-1111-111111111111"

	cfg := buildConfig(flatConfig{
		EnvName:                  "development",
		DefaultTenantID:          validTenantID,
		BodyLimitBytes:           1024,
		LogLevel:                 "info",
		EnableTelemetry:          true,
		OtelDeploymentEnv:        "invalid_env",
		RateLimitMax:             100,
		RateLimitExpirySec:       60,
		ExportRateLimitMax:       10,
		ExportRateLimitExpirySec: 60,
		InfraConnectTimeoutSec:   30,
	})

	err := cfg.Validate()
	require.Error(t, err)
	assert.Contains(t, err.Error(), "OTEL_RESOURCE_DEPLOYMENT_ENVIRONMENT must be one of")
}

func TestConfig_QueryTimeout(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name     string
		timeout  int
		expected time.Duration
	}{
		{
			name:     "positive value returns configured duration",
			timeout:  45,
			expected: 45 * time.Second,
		},
		{
			name:     "default value 30 seconds",
			timeout:  30,
			expected: 30 * time.Second,
		},
		{
			name:     "zero returns default 30 seconds",
			timeout:  0,
			expected: 30 * time.Second,
		},
		{
			name:     "negative returns default 30 seconds",
			timeout:  -10,
			expected: 30 * time.Second,
		},
		{
			name:     "large value returns configured duration",
			timeout:  120,
			expected: 120 * time.Second,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			cfg := buildConfig(flatConfig{PostgresQueryTimeoutSec: tt.timeout})
			assert.Equal(t, tt.expected, cfg.QueryTimeout())
		})
	}
}

func TestConfig_ValidateNegativePostgresQueryTimeout(t *testing.T) {
	t.Parallel()

	validTenantID := "11111111-1111-1111-1111-111111111111"

	cfg := buildConfig(flatConfig{
		EnvName:                  "development",
		DefaultTenantID:          validTenantID,
		BodyLimitBytes:           1024,
		LogLevel:                 "info",
		RateLimitMax:             100,
		RateLimitExpirySec:       60,
		ExportRateLimitMax:       10,
		ExportRateLimitExpirySec: 60,
		InfraConnectTimeoutSec:   30,
		PostgresQueryTimeoutSec:  -1,
	})

	err := cfg.Validate()
	require.Error(t, err)
	assert.Contains(t, err.Error(), "PostgresQueryTimeoutSec must be non-negative")
}

func TestConfig_ValidateNegativePostgresConnectTimeout(t *testing.T) {
	t.Parallel()

	validTenantID := "11111111-1111-1111-1111-111111111111"

	cfg := buildConfig(flatConfig{
		EnvName:                   "development",
		DefaultTenantID:           validTenantID,
		BodyLimitBytes:            1024,
		LogLevel:                  "info",
		RateLimitMax:              100,
		RateLimitExpirySec:        60,
		ExportRateLimitMax:        10,
		ExportRateLimitExpirySec:  60,
		InfraConnectTimeoutSec:    30,
		PostgresConnectTimeoutSec: -1,
	})

	err := cfg.Validate()
	require.Error(t, err)
	assert.Contains(t, err.Error(), "PostgresConnectTimeoutSec must be non-negative")
}

func TestConfig_ValidateZeroInfraConnectTimeout(t *testing.T) {
	t.Parallel()

	validTenantID := "11111111-1111-1111-1111-111111111111"

	cfg := buildConfig(flatConfig{
		EnvName:                  "development",
		DefaultTenantID:          validTenantID,
		BodyLimitBytes:           1024,
		LogLevel:                 "info",
		RateLimitMax:             100,
		RateLimitExpirySec:       60,
		ExportRateLimitMax:       10,
		ExportRateLimitExpirySec: 60,
		InfraConnectTimeoutSec:   0,
	})

	err := cfg.Validate()
	require.Error(t, err)
	assert.Contains(t, err.Error(), "InfraConnectTimeoutSec must be positive")
}

func TestConfig_ValidateInvalidTenantID(t *testing.T) {
	t.Parallel()

	cfg := buildConfig(flatConfig{
		EnvName:                  "development",
		DefaultTenantID:          "not-a-uuid",
		BodyLimitBytes:           1024,
		LogLevel:                 "info",
		RateLimitMax:             100,
		RateLimitExpirySec:       60,
		ExportRateLimitMax:       10,
		ExportRateLimitExpirySec: 60,
		InfraConnectTimeoutSec:   30,
	})

	err := cfg.Validate()
	require.Error(t, err)
	assert.Contains(t, err.Error(), "DEFAULT_TENANT_ID must be a valid UUID")
}

func TestConfig_ValidateRateLimitConfig(t *testing.T) {
	t.Parallel()

	validTenantID := "11111111-1111-1111-1111-111111111111"

	t.Run("skips rate limit validation when disabled", func(t *testing.T) {
		t.Parallel()

		cfg := buildConfig(flatConfig{
			EnvName:                  "development",
			DefaultTenantID:          validTenantID,
			BodyLimitBytes:           1024,
			LogLevel:                 "info",
			RateLimitEnabled:         false,
			RateLimitMax:             0,
			RateLimitExpirySec:       0,
			ExportRateLimitMax:       10,
			ExportRateLimitExpirySec: 60,
			InfraConnectTimeoutSec:   30,
		})

		err := cfg.Validate()
		require.NoError(t, err)
	})

	t.Run("fails when export rate limit max is zero", func(t *testing.T) {
		t.Parallel()

		cfg := buildConfig(flatConfig{
			EnvName:                  "development",
			DefaultTenantID:          validTenantID,
			BodyLimitBytes:           1024,
			LogLevel:                 "info",
			RateLimitMax:             100,
			RateLimitExpirySec:       60,
			ExportRateLimitMax:       0,
			ExportRateLimitExpirySec: 60,
			InfraConnectTimeoutSec:   30,
		})

		err := cfg.Validate()
		require.Error(t, err)
		assert.Contains(t, err.Error(), "EXPORT_RATE_LIMIT_MAX must be positive")
	})

	t.Run("fails when export rate limit expiry is zero", func(t *testing.T) {
		t.Parallel()

		cfg := buildConfig(flatConfig{
			EnvName:                  "development",
			DefaultTenantID:          validTenantID,
			BodyLimitBytes:           1024,
			LogLevel:                 "info",
			RateLimitMax:             100,
			RateLimitExpirySec:       60,
			ExportRateLimitMax:       10,
			ExportRateLimitExpirySec: 0,
			InfraConnectTimeoutSec:   30,
		})

		err := cfg.Validate()
		require.Error(t, err)
		assert.Contains(t, err.Error(), "EXPORT_RATE_LIMIT_EXPIRY_SEC must be positive")
	})

	t.Run("fails when rate limit max is zero and enabled", func(t *testing.T) {
		t.Parallel()

		cfg := buildConfig(flatConfig{
			EnvName:                  "development",
			DefaultTenantID:          validTenantID,
			BodyLimitBytes:           1024,
			LogLevel:                 "info",
			RateLimitEnabled:         true,
			RateLimitMax:             0,
			RateLimitExpirySec:       60,
			ExportRateLimitMax:       10,
			ExportRateLimitExpirySec: 60,
			InfraConnectTimeoutSec:   30,
		})

		err := cfg.Validate()
		require.Error(t, err)
		assert.Contains(t, err.Error(), "RATE_LIMIT_MAX must be positive")
	})
}

func TestConfig_ValidateBodyLimitBytes(t *testing.T) {
	t.Parallel()

	validTenantID := "11111111-1111-1111-1111-111111111111"

	cfg := buildConfig(flatConfig{
		EnvName:                  "development",
		DefaultTenantID:          validTenantID,
		BodyLimitBytes:           0,
		LogLevel:                 "info",
		RateLimitMax:             100,
		RateLimitExpirySec:       60,
		ExportRateLimitMax:       10,
		ExportRateLimitExpirySec: 60,
		InfraConnectTimeoutSec:   30,
	})

	err := cfg.Validate()
	require.Error(t, err)
	assert.Contains(t, err.Error(), "HTTP_BODY_LIMIT_BYTES must be positive")
}

func TestConfig_ReplicaDSNMasked_PartialFallbacks(t *testing.T) {
	t.Parallel()

	t.Run("uses primary port when replica port empty", func(t *testing.T) {
		t.Parallel()

		cfg := buildConfig(flatConfig{
			PrimaryDBHost:     "primary-host",
			PrimaryDBPort:     "5432",
			PrimaryDBUser:     "matcher",
			PrimaryDBPassword: "secret",
			PrimaryDBName:     "matcher_db",
			PrimaryDBSSLMode:  "require",
			ReplicaDBHost:     "replica-host",
			ReplicaDBPort:     "",
		})

		dsn := cfg.ReplicaDSNMasked()

		assert.Contains(t, dsn, "host=replica-host")
		assert.Contains(t, dsn, "port=5432")
		assert.Contains(t, dsn, "password=***REDACTED***")
	})

	t.Run("uses primary user when replica user empty", func(t *testing.T) {
		t.Parallel()

		cfg := buildConfig(flatConfig{
			PrimaryDBHost:     "primary-host",
			PrimaryDBPort:     "5432",
			PrimaryDBUser:     "matcher",
			PrimaryDBPassword: "secret",
			PrimaryDBName:     "matcher_db",
			PrimaryDBSSLMode:  "require",
			ReplicaDBHost:     "replica-host",
			ReplicaDBUser:     "",
		})

		dsn := cfg.ReplicaDSNMasked()

		assert.Contains(t, dsn, "user=matcher")
	})

	t.Run("uses primary dbname when replica dbname empty", func(t *testing.T) {
		t.Parallel()

		cfg := buildConfig(flatConfig{
			PrimaryDBHost:     "primary-host",
			PrimaryDBPort:     "5432",
			PrimaryDBUser:     "matcher",
			PrimaryDBPassword: "secret",
			PrimaryDBName:     "matcher_db",
			PrimaryDBSSLMode:  "require",
			ReplicaDBHost:     "replica-host",
			ReplicaDBName:     "",
		})

		dsn := cfg.ReplicaDSNMasked()

		assert.Contains(t, dsn, "dbname=matcher_db")
	})

	t.Run("uses primary sslmode when replica sslmode empty", func(t *testing.T) {
		t.Parallel()

		cfg := buildConfig(flatConfig{
			PrimaryDBHost:     "primary-host",
			PrimaryDBPort:     "5432",
			PrimaryDBUser:     "matcher",
			PrimaryDBPassword: "secret",
			PrimaryDBName:     "matcher_db",
			PrimaryDBSSLMode:  "require",
			ReplicaDBHost:     "replica-host",
			ReplicaDBSSLMode:  "",
		})

		dsn := cfg.ReplicaDSNMasked()

		assert.Contains(t, dsn, "sslmode=require")
	})

	t.Run("uses all replica settings when all configured", func(t *testing.T) {
		t.Parallel()

		cfg := buildConfig(flatConfig{
			PrimaryDBHost:     "primary-host",
			PrimaryDBPort:     "5432",
			PrimaryDBUser:     "matcher",
			PrimaryDBPassword: "secret",
			PrimaryDBName:     "matcher_db",
			PrimaryDBSSLMode:  "require",
			ReplicaDBHost:     "replica-host",
			ReplicaDBPort:     "5433",
			ReplicaDBUser:     "replica_user",
			ReplicaDBPassword: "replica_secret",
			ReplicaDBName:     "replica_db",
			ReplicaDBSSLMode:  "disable",
		})

		dsn := cfg.ReplicaDSNMasked()

		assert.Contains(t, dsn, "host=replica-host")
		assert.Contains(t, dsn, "port=5433")
		assert.Contains(t, dsn, "user=replica_user")
		assert.Contains(t, dsn, "dbname=replica_db")
		assert.Contains(t, dsn, "sslmode=disable")
		assert.Contains(t, dsn, "password=***REDACTED***")
		assert.NotContains(t, dsn, "replica_secret")
	})
}

func TestConfig_EnforceProductionSecurityDefaults_SwaggerWarning(t *testing.T) {
	t.Parallel()

	t.Run("disables swagger when enabled in production", func(t *testing.T) {
		t.Parallel()

		cfg := buildConfig(flatConfig{
			EnvName:          envProduction,
			SwaggerEnabled:   true,
			RateLimitEnabled: true,
		})

		cfg.enforceProductionSecurityDefaults(&libLog.NopLogger{})

		assert.False(t, cfg.Swagger.Enabled, "swagger should be disabled in production")
	})

	t.Run("no warning when swagger disabled in production", func(t *testing.T) {
		t.Parallel()

		cfg := buildConfig(flatConfig{
			EnvName:          envProduction,
			SwaggerEnabled:   false,
			RateLimitEnabled: true,
		})

		cfg.enforceProductionSecurityDefaults(&libLog.NopLogger{})

		assert.False(t, cfg.Swagger.Enabled)
	})
}

func TestConfig_ValidateRateLimitConfig_ExpiryValidation(t *testing.T) {
	t.Parallel()

	validTenantID := "11111111-1111-1111-1111-111111111111"

	t.Run("fails when rate limit expiry is zero and enabled", func(t *testing.T) {
		t.Parallel()

		cfg := buildConfig(flatConfig{
			EnvName:                  "development",
			DefaultTenantID:          validTenantID,
			BodyLimitBytes:           1024,
			LogLevel:                 "info",
			RateLimitEnabled:         true,
			RateLimitMax:             100,
			RateLimitExpirySec:       0,
			ExportRateLimitMax:       10,
			ExportRateLimitExpirySec: 60,
			InfraConnectTimeoutSec:   30,
		})

		err := cfg.Validate()
		require.Error(t, err)
		assert.Contains(t, err.Error(), "RATE_LIMIT_EXPIRY_SEC must be positive")
	})

	t.Run("fails when dispatch rate limit max is zero and enabled", func(t *testing.T) {
		t.Parallel()

		cfg := buildConfig(flatConfig{
			EnvName:                    "development",
			DefaultTenantID:            validTenantID,
			BodyLimitBytes:             1024,
			LogLevel:                   "info",
			RateLimitEnabled:           true,
			RateLimitMax:               100,
			RateLimitExpirySec:         60,
			ExportRateLimitMax:         10,
			ExportRateLimitExpirySec:   60,
			DispatchRateLimitMax:       0,
			DispatchRateLimitExpirySec: 60,
			InfraConnectTimeoutSec:     30,
		})

		err := cfg.Validate()
		require.Error(t, err)
		assert.Contains(t, err.Error(), "DISPATCH_RATE_LIMIT_MAX must be positive")
	})

	t.Run("fails when dispatch rate limit expiry is zero and enabled", func(t *testing.T) {
		t.Parallel()

		cfg := buildConfig(flatConfig{
			EnvName:                    "development",
			DefaultTenantID:            validTenantID,
			BodyLimitBytes:             1024,
			LogLevel:                   "info",
			RateLimitEnabled:           true,
			RateLimitMax:               100,
			RateLimitExpirySec:         60,
			ExportRateLimitMax:         10,
			ExportRateLimitExpirySec:   60,
			DispatchRateLimitMax:       50,
			DispatchRateLimitExpirySec: 0,
			InfraConnectTimeoutSec:     30,
		})

		err := cfg.Validate()
		require.Error(t, err)
		assert.Contains(t, err.Error(), "DISPATCH_RATE_LIMIT_EXPIRY_SEC must be positive")
	})

	t.Run("passes when all rate limit values are positive", func(t *testing.T) {
		t.Parallel()

		cfg := buildConfig(flatConfig{
			EnvName:                    "development",
			DefaultTenantID:            validTenantID,
			BodyLimitBytes:             1024,
			LogLevel:                   "info",
			RateLimitEnabled:           true,
			RateLimitMax:               100,
			RateLimitExpirySec:         60,
			ExportRateLimitMax:         10,
			ExportRateLimitExpirySec:   60,
			DispatchRateLimitMax:       50,
			DispatchRateLimitExpirySec: 60,
			InfraConnectTimeoutSec:     30,
		})

		err := cfg.Validate()
		require.NoError(t, err)
	})
}

func TestConfig_ValidateAllLogLevels(t *testing.T) {
	t.Parallel()

	validTenantID := "11111111-1111-1111-1111-111111111111"
	validLevels := []string{
		"debug",
		"info",
		"warn",
		"error",
		"fatal",
		"DEBUG",
		"INFO",
		"WARN",
		"ERROR",
		"FATAL",
	}

	for _, level := range validLevels {
		t.Run("accepts log level "+level, func(t *testing.T) {
			t.Parallel()

			cfg := buildConfig(flatConfig{
				EnvName:                  "development",
				DefaultTenantID:          validTenantID,
				BodyLimitBytes:           1024,
				LogLevel:                 level,
				RateLimitMax:             100,
				RateLimitExpirySec:       60,
				ExportRateLimitMax:       10,
				ExportRateLimitExpirySec: 60,
				InfraConnectTimeoutSec:   30,
			})

			err := cfg.Validate()
			require.NoError(t, err)
		})
	}
}

func TestConfig_ValidateAllOtelEnvs(t *testing.T) {
	t.Parallel()

	validTenantID := "11111111-1111-1111-1111-111111111111"
	validEnvs := []string{
		"development",
		"staging",
		"production",
		"DEVELOPMENT",
		"STAGING",
		"PRODUCTION",
	}

	for _, env := range validEnvs {
		t.Run("accepts otel env "+env, func(t *testing.T) {
			t.Parallel()

			cfg := buildConfig(flatConfig{
				EnvName:                  "development",
				DefaultTenantID:          validTenantID,
				BodyLimitBytes:           1024,
				LogLevel:                 "info",
				EnableTelemetry:          true,
				OtelDeploymentEnv:        env,
				RateLimitMax:             100,
				RateLimitExpirySec:       60,
				ExportRateLimitMax:       10,
				ExportRateLimitExpirySec: 60,
				InfraConnectTimeoutSec:   30,
			})

			err := cfg.Validate()
			require.NoError(t, err)
		})
	}
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

		assert.Contains(t, dsn, "amqp://guest:guest@localhost:5672")
	})

	t.Run("handles vhost with leading slash", func(t *testing.T) {
		t.Parallel()

		cfg := buildConfig(flatConfig{
			RabbitMQURI:      "amqps",
			RabbitMQHost:     "rabbitmq.example.com",
			RabbitMQPort:     "5671",
			RabbitMQUser:     "matcher",
			RabbitMQPassword: "secret",
			RabbitMQVHost:    "/production",
		})

		dsn := cfg.RabbitMQDSN()

		assert.Contains(t, dsn, "/production")
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
	})
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
		t.Setenv("HTTP_BODY_LIMIT_BYTES", "-1")
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

func TestConfig_ProductionTLSValidation(t *testing.T) {
	t.Parallel()

	validTenantID := "11111111-1111-1111-1111-111111111111"

	t.Run("passes with TLS terminated upstream", func(t *testing.T) {
		t.Parallel()

		cfg := buildConfig(flatConfig{
			EnvName:                  "production",
			DefaultTenantID:          validTenantID,
			PrimaryDBPassword:        "secret",
			PrimaryDBSSLMode:         "require",
			RedisTLS:                 true,
			RedisHost:                "redis.example.com",
			RedisPassword:            "redissecret",
			RabbitMQURI:              "amqps",
			RabbitMQUser:             "matcher",
			RabbitMQPassword:         "rabbitsecret",
			AuthEnabled:              true,
			AuthHost:                 "http://auth:8080",
			AuthTokenSecret:          "jwtsecret",
			CORSAllowedOrigins:       "https://example.com",
			BodyLimitBytes:           1024,
			LogLevel:                 "info",
			RateLimitMax:             100,
			RateLimitExpirySec:       60,
			ExportRateLimitMax:       10,
			ExportRateLimitExpirySec: 60,
			TLSTerminatedUpstream:    true,
			InfraConnectTimeoutSec:   30,
		})

		err := cfg.Validate()
		require.NoError(t, err)
	})

	t.Run("passes with server TLS configured", func(t *testing.T) {
		t.Parallel()

		cfg := buildConfig(flatConfig{
			EnvName:                  "production",
			DefaultTenantID:          validTenantID,
			PrimaryDBPassword:        "secret",
			PrimaryDBSSLMode:         "require",
			RedisTLS:                 true,
			RedisHost:                "redis.example.com",
			RedisPassword:            "redissecret",
			RabbitMQURI:              "amqps",
			RabbitMQUser:             "matcher",
			RabbitMQPassword:         "rabbitsecret",
			AuthEnabled:              true,
			AuthHost:                 "http://auth:8080",
			AuthTokenSecret:          "jwtsecret",
			CORSAllowedOrigins:       "https://example.com",
			BodyLimitBytes:           1024,
			LogLevel:                 "info",
			RateLimitMax:             100,
			RateLimitExpirySec:       60,
			ExportRateLimitMax:       10,
			ExportRateLimitExpirySec: 60,
			TLSTerminatedUpstream:    false,
			ServerTLSCertFile:        "/path/to/cert.pem",
			ServerTLSKeyFile:         "/path/to/key.pem",
			InfraConnectTimeoutSec:   30,
		})

		err := cfg.Validate()
		require.NoError(t, err)
	})
}

func TestConfig_AuthHostAndSecretValidation(t *testing.T) {
	t.Parallel()

	validTenantID := "11111111-1111-1111-1111-111111111111"

	t.Run("fails when auth enabled but host missing", func(t *testing.T) {
		t.Parallel()

		cfg := buildConfig(flatConfig{
			EnvName:                  "development",
			DefaultTenantID:          validTenantID,
			BodyLimitBytes:           1024,
			LogLevel:                 "info",
			AuthEnabled:              true,
			AuthHost:                 "",
			AuthTokenSecret:          "secret",
			RateLimitMax:             100,
			RateLimitExpirySec:       60,
			ExportRateLimitMax:       10,
			ExportRateLimitExpirySec: 60,
			InfraConnectTimeoutSec:   30,
		})

		err := cfg.Validate()
		require.Error(t, err)
		assert.Contains(t, err.Error(), "AUTH_SERVICE_ADDRESS is required when AUTH_ENABLED=true")
	})

	t.Run("fails when auth enabled but secret missing", func(t *testing.T) {
		t.Parallel()

		cfg := buildConfig(flatConfig{
			EnvName:                  "development",
			DefaultTenantID:          validTenantID,
			BodyLimitBytes:           1024,
			LogLevel:                 "info",
			AuthEnabled:              true,
			AuthHost:                 "http://auth:8080",
			AuthTokenSecret:          "",
			RateLimitMax:             100,
			RateLimitExpirySec:       60,
			ExportRateLimitMax:       10,
			ExportRateLimitExpirySec: 60,
			InfraConnectTimeoutSec:   30,
		})

		err := cfg.Validate()
		require.Error(t, err)
		assert.Contains(t, err.Error(), "AUTH_JWT_SECRET is required when AUTH_ENABLED=true")
	})

	t.Run("fails when auth enabled but host is whitespace only", func(t *testing.T) {
		t.Parallel()

		cfg := buildConfig(flatConfig{
			EnvName:                  "development",
			DefaultTenantID:          validTenantID,
			BodyLimitBytes:           1024,
			LogLevel:                 "info",
			AuthEnabled:              true,
			AuthHost:                 "   ",
			AuthTokenSecret:          "secret",
			RateLimitMax:             100,
			RateLimitExpirySec:       60,
			ExportRateLimitMax:       10,
			ExportRateLimitExpirySec: 60,
			InfraConnectTimeoutSec:   30,
		})

		err := cfg.Validate()
		require.Error(t, err)
		assert.Contains(t, err.Error(), "AUTH_SERVICE_ADDRESS is required when AUTH_ENABLED=true")
	})
}

func TestConfig_NegativeBodyLimit(t *testing.T) {
	t.Parallel()

	validTenantID := "11111111-1111-1111-1111-111111111111"

	cfg := buildConfig(flatConfig{
		EnvName:                  "development",
		DefaultTenantID:          validTenantID,
		BodyLimitBytes:           -100,
		LogLevel:                 "info",
		RateLimitMax:             100,
		RateLimitExpirySec:       60,
		ExportRateLimitMax:       10,
		ExportRateLimitExpirySec: 60,
		InfraConnectTimeoutSec:   30,
	})

	err := cfg.Validate()
	require.Error(t, err)
	assert.Contains(t, err.Error(), "HTTP_BODY_LIMIT_BYTES must be positive")
}

func TestConfig_ProductionCORSWildcard(t *testing.T) {
	t.Parallel()

	validTenantID := "11111111-1111-1111-1111-111111111111"

	t.Run("fails with wildcard CORS in production", func(t *testing.T) {
		t.Parallel()

		cfg := buildConfig(flatConfig{
			EnvName:               "production",
			DefaultTenantID:       validTenantID,
			PrimaryDBPassword:     "secret",
			PrimaryDBSSLMode:      "require",
			RedisTLS:              true,
			RabbitMQURI:           "amqps",
			RabbitMQUser:          "matcher",
			RabbitMQPassword:      "rabbitsecret",
			AuthEnabled:           true,
			AuthHost:              "http://auth:8080",
			AuthTokenSecret:       "jwtsecret",
			CORSAllowedOrigins:    "*",
			BodyLimitBytes:        1024,
			LogLevel:              "info",
			TLSTerminatedUpstream: true,
		})

		err := cfg.Validate()
		require.Error(t, err)
		assert.Contains(t, err.Error(), "CORS_ALLOWED_ORIGINS must be restricted in production")
	})

	t.Run("fails with empty CORS in production", func(t *testing.T) {
		t.Parallel()

		cfg := buildConfig(flatConfig{
			EnvName:               "production",
			DefaultTenantID:       validTenantID,
			PrimaryDBPassword:     "secret",
			PrimaryDBSSLMode:      "require",
			RedisTLS:              true,
			RabbitMQURI:           "amqps",
			RabbitMQUser:          "matcher",
			RabbitMQPassword:      "rabbitsecret",
			AuthEnabled:           true,
			AuthHost:              "http://auth:8080",
			AuthTokenSecret:       "jwtsecret",
			CORSAllowedOrigins:    "",
			BodyLimitBytes:        1024,
			LogLevel:              "info",
			TLSTerminatedUpstream: true,
		})

		err := cfg.Validate()
		require.Error(t, err)
		assert.Contains(t, err.Error(), "CORS_ALLOWED_ORIGINS must be restricted in production")
	})
}

func TestConfig_ArchivalInterval(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name     string
		interval int
		expected time.Duration
	}{
		{
			name:     "positive value returns configured duration",
			interval: 12,
			expected: 12 * time.Hour,
		},
		{
			name:     "default value 24 hours",
			interval: 24,
			expected: 24 * time.Hour,
		},
		{
			name:     "zero returns minimum 1 hour",
			interval: 0,
			expected: time.Hour,
		},
		{
			name:     "negative returns minimum 1 hour",
			interval: -5,
			expected: time.Hour,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			cfg := buildConfig(flatConfig{ArchivalIntervalHours: tt.interval})
			assert.Equal(t, tt.expected, cfg.ArchivalInterval())
		})
	}
}

func TestConfig_ArchivalPresignExpiry(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name     string
		expiry   int
		expected time.Duration
	}{
		{
			name:     "positive value returns configured duration",
			expiry:   1800,
			expected: 1800 * time.Second,
		},
		{
			name:     "default value 3600 seconds (1 hour)",
			expiry:   3600,
			expected: 3600 * time.Second,
		},
		{
			name:     "zero returns default 1 hour",
			expiry:   0,
			expected: 3600 * time.Second,
		},
		{
			name:     "negative returns default 1 hour",
			expiry:   -10,
			expected: 3600 * time.Second,
		},
		{
			name:     "caps at S3 maximum of 7 days",
			expiry:   700000,
			expected: 604800 * time.Second,
		},
		{
			name:     "exactly at S3 maximum",
			expiry:   604800,
			expected: 604800 * time.Second,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			cfg := buildConfig(flatConfig{ArchivalPresignExpirySec: tt.expiry})
			assert.Equal(t, tt.expected, cfg.ArchivalPresignExpiry())
		})
	}
}

func TestConfig_ArchivalPresignExpiry_LogsWarningOnCap(t *testing.T) {
	t.Parallel()

	cfg := buildConfig(flatConfig{ArchivalPresignExpirySec: 700000})
	cfg.Logger = &libLog.NopLogger{}

	result := cfg.ArchivalPresignExpiry()
	assert.Equal(t, 604800*time.Second, result)
}

func TestConfig_SchedulerInterval(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name     string
		interval int
		expected time.Duration
	}{
		{
			name:     "positive value returns configured duration",
			interval: 120,
			expected: 120 * time.Second,
		},
		{
			name:     "default value 60 seconds (1 minute)",
			interval: 60,
			expected: 60 * time.Second,
		},
		{
			name:     "zero returns default 1 minute",
			interval: 0,
			expected: time.Minute,
		},
		{
			name:     "negative returns default 1 minute",
			interval: -10,
			expected: time.Minute,
		},
		{
			name:     "small positive value",
			interval: 5,
			expected: 5 * time.Second,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			cfg := Config{}
			cfg.Scheduler.IntervalSec = tt.interval
			assert.Equal(t, tt.expected, cfg.SchedulerInterval())
		})
	}
}

func TestConfig_ExportPresignExpiry(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name     string
		expiry   int
		expected time.Duration
	}{
		{
			name:     "positive value returns configured duration",
			expiry:   1800,
			expected: 1800 * time.Second,
		},
		{
			name:     "default value 3600 seconds (1 hour)",
			expiry:   3600,
			expected: 3600 * time.Second,
		},
		{
			name:     "zero returns default 1 hour",
			expiry:   0,
			expected: 3600 * time.Second,
		},
		{
			name:     "negative returns default 1 hour",
			expiry:   -10,
			expected: 3600 * time.Second,
		},
		{
			name:     "caps at S3 maximum of 7 days",
			expiry:   700000,
			expected: 604800 * time.Second,
		},
		{
			name:     "exactly at S3 maximum",
			expiry:   604800,
			expected: 604800 * time.Second,
		},
		{
			name:     "just below S3 maximum",
			expiry:   604799,
			expected: 604799 * time.Second,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			cfg := Config{}
			cfg.ExportWorker.PresignExpirySec = tt.expiry
			assert.Equal(t, tt.expected, cfg.ExportPresignExpiry())
		})
	}
}

func TestConfig_ExportPresignExpiry_LogsWarningOnCap(t *testing.T) {
	t.Parallel()

	cfg := Config{}
	cfg.ExportWorker.PresignExpirySec = 700000
	cfg.Logger = &libLog.NopLogger{}

	result := cfg.ExportPresignExpiry()
	assert.Equal(t, 604800*time.Second, result)
}

func TestLoadConfigFromEnv_NilConfig(t *testing.T) {
	t.Parallel()

	err := loadConfigFromEnv(nil)

	require.Error(t, err)
	assert.ErrorIs(t, err, ErrConfigNil)
}

func TestConfig_ValidateNegativeWebhookTimeout(t *testing.T) {
	t.Parallel()

	validTenantID := "11111111-1111-1111-1111-111111111111"

	cfg := buildConfig(flatConfig{
		EnvName:                  "development",
		DefaultTenantID:          validTenantID,
		BodyLimitBytes:           1024,
		LogLevel:                 "info",
		RateLimitMax:             100,
		RateLimitExpirySec:       60,
		ExportRateLimitMax:       10,
		ExportRateLimitExpirySec: 60,
		InfraConnectTimeoutSec:   30,
		WebhookTimeoutSec:        -1,
	})

	err := cfg.Validate()
	require.Error(t, err)
	assert.Contains(t, err.Error(), "WEBHOOK_TIMEOUT_SEC must be non-negative")
}

func TestErrConfigNil(t *testing.T) {
	t.Parallel()

	require.Error(t, ErrConfigNil)
	assert.Equal(t, "config must be provided", ErrConfigNil.Error())
}

func TestConfig_ValidateArchivalConfig(t *testing.T) {
	t.Parallel()

	validTenantID := "11111111-1111-1111-1111-111111111111"

	t.Run("passes with valid archival config disabled", func(t *testing.T) {
		t.Parallel()

		cfg := buildConfig(flatConfig{
			EnvName:                  "development",
			DefaultTenantID:          validTenantID,
			BodyLimitBytes:           1024,
			LogLevel:                 "info",
			RateLimitMax:             100,
			RateLimitExpirySec:       60,
			ExportRateLimitMax:       10,
			ExportRateLimitExpirySec: 60,
			InfraConnectTimeoutSec:   30,
			ArchivalEnabled:          false,
		})

		err := cfg.Validate()
		require.NoError(t, err)
	})

	t.Run("passes with valid archival config enabled", func(t *testing.T) {
		t.Parallel()

		cfg := buildConfig(flatConfig{
			EnvName:                     "development",
			DefaultTenantID:             validTenantID,
			BodyLimitBytes:              1024,
			LogLevel:                    "info",
			RateLimitMax:                100,
			RateLimitExpirySec:          60,
			ExportRateLimitMax:          10,
			ExportRateLimitExpirySec:    60,
			InfraConnectTimeoutSec:      30,
			ArchivalEnabled:             true,
			ArchivalStorageBucket:       "my-bucket",
			ArchivalHotRetentionDays:    90,
			ArchivalWarmRetentionMonths: 24,
			ArchivalColdRetentionMonths: 84,
			ArchivalBatchSize:           5000,
			ArchivalPartitionLookahead:  3,
		})

		err := cfg.Validate()
		require.NoError(t, err)
	})

	t.Run("fails when enabled but no bucket", func(t *testing.T) {
		t.Parallel()

		cfg := buildConfig(flatConfig{
			EnvName:                  "development",
			DefaultTenantID:          validTenantID,
			BodyLimitBytes:           1024,
			LogLevel:                 "info",
			RateLimitMax:             100,
			RateLimitExpirySec:       60,
			ExportRateLimitMax:       10,
			ExportRateLimitExpirySec: 60,
			InfraConnectTimeoutSec:   30,
			ArchivalEnabled:          true,
			ArchivalStorageBucket:    "",
		})

		err := cfg.Validate()
		require.Error(t, err)
		assert.Contains(t, err.Error(), "ARCHIVAL_STORAGE_BUCKET is required when ARCHIVAL_WORKER_ENABLED=true")
	})

	t.Run("fails when enabled but bucket is whitespace", func(t *testing.T) {
		t.Parallel()

		cfg := buildConfig(flatConfig{
			EnvName:                  "development",
			DefaultTenantID:          validTenantID,
			BodyLimitBytes:           1024,
			LogLevel:                 "info",
			RateLimitMax:             100,
			RateLimitExpirySec:       60,
			ExportRateLimitMax:       10,
			ExportRateLimitExpirySec: 60,
			InfraConnectTimeoutSec:   30,
			ArchivalEnabled:          true,
			ArchivalStorageBucket:    "   ",
		})

		err := cfg.Validate()
		require.Error(t, err)
		assert.Contains(t, err.Error(), "ARCHIVAL_STORAGE_BUCKET is required when ARCHIVAL_WORKER_ENABLED=true")
	})

	t.Run("fails when warm retention <= hot retention / 30", func(t *testing.T) {
		t.Parallel()

		cfg := buildConfig(flatConfig{
			EnvName:                     "development",
			DefaultTenantID:             validTenantID,
			BodyLimitBytes:              1024,
			LogLevel:                    "info",
			RateLimitMax:                100,
			RateLimitExpirySec:          60,
			ExportRateLimitMax:          10,
			ExportRateLimitExpirySec:    60,
			InfraConnectTimeoutSec:      30,
			ArchivalEnabled:             true,
			ArchivalStorageBucket:       "my-bucket",
			ArchivalHotRetentionDays:    90, // 90 / 30 = 3
			ArchivalWarmRetentionMonths: 3,  // 3 is not > 3
			ArchivalColdRetentionMonths: 84,
		})

		err := cfg.Validate()
		require.Error(t, err)
		assert.Contains(t, err.Error(), "ARCHIVAL_WARM_RETENTION_MONTHS must be greater than ARCHIVAL_HOT_RETENTION_DAYS / 30")
	})

	t.Run("fails when cold retention < warm retention", func(t *testing.T) {
		t.Parallel()

		cfg := buildConfig(flatConfig{
			EnvName:                     "development",
			DefaultTenantID:             validTenantID,
			BodyLimitBytes:              1024,
			LogLevel:                    "info",
			RateLimitMax:                100,
			RateLimitExpirySec:          60,
			ExportRateLimitMax:          10,
			ExportRateLimitExpirySec:    60,
			InfraConnectTimeoutSec:      30,
			ArchivalEnabled:             true,
			ArchivalStorageBucket:       "my-bucket",
			ArchivalHotRetentionDays:    90,
			ArchivalWarmRetentionMonths: 24,
			ArchivalColdRetentionMonths: 12, // 12 < 24
		})

		err := cfg.Validate()
		require.Error(t, err)
		assert.Contains(t, err.Error(), "ARCHIVAL_COLD_RETENTION_MONTHS must be >= ARCHIVAL_WARM_RETENTION_MONTHS")
	})

	t.Run("fails when hot retention days is zero", func(t *testing.T) {
		t.Parallel()

		cfg := buildConfig(flatConfig{
			EnvName:                  "development",
			DefaultTenantID:          validTenantID,
			BodyLimitBytes:           1024,
			LogLevel:                 "info",
			RateLimitMax:             100,
			RateLimitExpirySec:       60,
			ExportRateLimitMax:       10,
			ExportRateLimitExpirySec: 60,
			InfraConnectTimeoutSec:   30,
			ArchivalEnabled:          true,
			ArchivalStorageBucket:    "my-bucket",
			ArchivalHotRetentionDays: -1,
		})

		err := cfg.Validate()
		require.Error(t, err)
		assert.Contains(t, err.Error(), "ARCHIVAL_HOT_RETENTION_DAYS must be positive")
	})

	t.Run("fails when batch size is zero", func(t *testing.T) {
		t.Parallel()

		cfg := buildConfig(flatConfig{
			EnvName:                  "development",
			DefaultTenantID:          validTenantID,
			BodyLimitBytes:           1024,
			LogLevel:                 "info",
			RateLimitMax:             100,
			RateLimitExpirySec:       60,
			ExportRateLimitMax:       10,
			ExportRateLimitExpirySec: 60,
			InfraConnectTimeoutSec:   30,
			ArchivalEnabled:          true,
			ArchivalStorageBucket:    "my-bucket",
			ArchivalBatchSize:        -1,
		})

		err := cfg.Validate()
		require.Error(t, err)
		assert.Contains(t, err.Error(), "ARCHIVAL_BATCH_SIZE must be positive")
	})

	t.Run("fails when partition lookahead is zero", func(t *testing.T) {
		t.Parallel()

		cfg := buildConfig(flatConfig{
			EnvName:                    "development",
			DefaultTenantID:            validTenantID,
			BodyLimitBytes:             1024,
			LogLevel:                   "info",
			RateLimitMax:               100,
			RateLimitExpirySec:         60,
			ExportRateLimitMax:         10,
			ExportRateLimitExpirySec:   60,
			InfraConnectTimeoutSec:     30,
			ArchivalEnabled:            true,
			ArchivalStorageBucket:      "my-bucket",
			ArchivalPartitionLookahead: -1,
		})

		err := cfg.Validate()
		require.Error(t, err)
		assert.Contains(t, err.Error(), "ARCHIVAL_PARTITION_LOOKAHEAD must be positive")
	})

	t.Run("does not require bucket when disabled", func(t *testing.T) {
		t.Parallel()

		cfg := buildConfig(flatConfig{
			EnvName:                  "development",
			DefaultTenantID:          validTenantID,
			BodyLimitBytes:           1024,
			LogLevel:                 "info",
			RateLimitMax:             100,
			RateLimitExpirySec:       60,
			ExportRateLimitMax:       10,
			ExportRateLimitExpirySec: 60,
			InfraConnectTimeoutSec:   30,
			ArchivalEnabled:          false,
			ArchivalStorageBucket:    "",
		})

		err := cfg.Validate()
		require.NoError(t, err)
	})
}
