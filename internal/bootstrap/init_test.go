// Copyright 2025 Lerian Studio. All rights reserved.
// Use of this source code is governed by an Elastic License 2.0
// that can be found in the LICENSE.md file.

//go:build unit

package bootstrap

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	sqlmock "github.com/DATA-DOG/go-sqlmock"
	"github.com/bxcodec/dbresolver/v2"
	"github.com/google/uuid"
	amqp "github.com/rabbitmq/amqp091-go"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	libAssert "github.com/LerianStudio/lib-commons/v4/commons/assert"
	libLog "github.com/LerianStudio/lib-commons/v4/commons/log"
	libPostgres "github.com/LerianStudio/lib-commons/v4/commons/postgres"
	libRabbitmq "github.com/LerianStudio/lib-commons/v4/commons/rabbitmq"

	"github.com/LerianStudio/matcher/internal/auth"
	discoveryWorker "github.com/LerianStudio/matcher/internal/discovery/services/worker"
	governanceWorker "github.com/LerianStudio/matcher/internal/governance/services/worker"
	ingestionRabbitmq "github.com/LerianStudio/matcher/internal/ingestion/adapters/rabbitmq"
	matchingRabbitmq "github.com/LerianStudio/matcher/internal/matching/adapters/rabbitmq"
	reportingStorage "github.com/LerianStudio/matcher/internal/reporting/adapters/storage"
	reportingWorker "github.com/LerianStudio/matcher/internal/reporting/services/worker"
	sharedRabbitmq "github.com/LerianStudio/matcher/internal/shared/adapters/rabbitmq"
	"github.com/LerianStudio/matcher/internal/shared/infrastructure/testutil"
	sharedPorts "github.com/LerianStudio/matcher/internal/shared/ports"
)

// errMatchRuleAdapterRequired is a test-only sentinel used to verify error handling paths.
var errMatchRuleAdapterRequired = errors.New("match rule repository adapter is required")

var errS3ClientCreation = errors.New("s3 client creation failed")

func TestErrMatchRuleAdapterRequired(t *testing.T) {
	t.Parallel()

	require.Error(t, errMatchRuleAdapterRequired)
	assert.Equal(
		t,
		"match rule repository adapter is required",
		errMatchRuleAdapterRequired.Error(),
	)
}

func TestInitLogger(t *testing.T) {
	t.Parallel()

	t.Run("with nil options returns zap logger", func(t *testing.T) {
		t.Parallel()

		logger, err := initLogger(nil)

		require.NoError(t, err)
		require.NotNil(t, logger)
	})

	t.Run("with nil logger in options returns zap logger", func(t *testing.T) {
		t.Parallel()

		opts := &Options{Logger: nil}

		logger, err := initLogger(opts)

		require.NoError(t, err)
		require.NotNil(t, logger)
	})

	t.Run("with custom logger returns that logger", func(t *testing.T) {
		t.Parallel()

		customLogger := &libLog.NopLogger{}
		opts := &Options{Logger: customLogger}

		logger, err := initLogger(opts)

		require.NoError(t, err)
		require.NotNil(t, logger)
		assert.Equal(t, customLogger, logger)
	})
}

func TestCreatePostgresConnection(t *testing.T) {
	t.Parallel()

	t.Run("creates connection with provided config", func(t *testing.T) {
		t.Parallel()

		cfg := &Config{
			Postgres: PostgresConfig{
				PrimaryHost:        "localhost",
				PrimaryPort:        "5432",
				PrimaryUser:        "test",
				PrimaryPassword:    "test",
				PrimaryDB:          "matcher",
				PrimarySSLMode:     "disable",
				MaxOpenConnections: 10,
				MaxIdleConnections: 5,
			},
		}

		conn, err := createPostgresConnection(cfg, &libLog.NopLogger{})

		require.NoError(t, err)
		require.NotNil(t, conn)

		connected, connectedErr := conn.IsConnected()
		require.NoError(t, connectedErr)
		assert.False(t, connected)

		_, primaryErr := conn.Primary()
		require.Error(t, primaryErr)
		assert.ErrorIs(t, primaryErr, libPostgres.ErrNotConnected)
	})

	t.Run("uses primary db name as replica when replica not set", func(t *testing.T) {
		t.Parallel()

		cfg := &Config{
			Postgres: PostgresConfig{
				PrimaryHost:     "localhost",
				PrimaryPort:     "5432",
				PrimaryUser:     "test",
				PrimaryPassword: "test",
				PrimaryDB:       "matcher",
				PrimarySSLMode:  "disable",
				ReplicaDB:       "",
			},
		}

		conn, err := createPostgresConnection(cfg, &libLog.NopLogger{})

		require.NoError(t, err)
		require.NotNil(t, conn)

		connected, connectedErr := conn.IsConnected()
		require.NoError(t, connectedErr)
		assert.False(t, connected)
	})

	t.Run("uses replica db name when set", func(t *testing.T) {
		t.Parallel()

		cfg := &Config{
			Postgres: PostgresConfig{
				PrimaryHost:     "localhost",
				PrimaryPort:     "5432",
				PrimaryUser:     "test",
				PrimaryPassword: "test",
				PrimaryDB:       "matcher",
				PrimarySSLMode:  "disable",
				ReplicaDB:       "matcher_replica",
			},
		}

		conn, err := createPostgresConnection(cfg, &libLog.NopLogger{})

		require.NoError(t, err)
		require.NotNil(t, conn)

		connected, connectedErr := conn.IsConnected()
		require.NoError(t, connectedErr)
		assert.False(t, connected)
	})
}

func TestCreateRedisConnection(t *testing.T) {
	t.Parallel()

	// In lib-commons v4, New() eagerly connects to Redis.
	// Without a running Redis server, connection attempts will fail.
	t.Run("returns error when redis unreachable", func(t *testing.T) {
		t.Parallel()

		cfg := &Config{
			Redis: RedisConfig{
				Host:           "localhost:6379",
				Password:       "secret",
				DB:             0,
				Protocol:       3,
				MasterName:     "mymaster",
				TLS:            false,
				PoolSize:       10,
				MinIdleConn:    2,
				ReadTimeoutMs:  1000,
				WriteTimeoutMs: 1000,
				DialTimeoutMs:  1000,
			},
		}

		ctx := context.Background()
		conn, err := createRedisConnection(ctx, cfg, &libLog.NopLogger{})

		require.Error(t, err)
		require.Nil(t, conn)
	})

	t.Run("returns error for comma-separated unreachable addresses", func(t *testing.T) {
		t.Parallel()

		cfg := &Config{
			Redis: RedisConfig{
				Host: "redis1:6379,redis2:6379,redis3:6379",
			},
		}

		ctx := context.Background()
		conn, err := createRedisConnection(ctx, cfg, &libLog.NopLogger{})

		require.Error(t, err)
		require.Nil(t, conn)
	})
}

func TestCreateRabbitMQConnection(t *testing.T) {
	t.Parallel()

	t.Run("creates connection with provided config", func(t *testing.T) {
		t.Parallel()

		cfg := &Config{
			RabbitMQ: RabbitMQConfig{
				Host:      "localhost",
				Port:      "5672",
				User:      "guest",
				Password:  "guest",
				HealthURL: "http://localhost:15672/api/health/checks/alarms",
			},
		}

		conn := createRabbitMQConnection(cfg, &libLog.NopLogger{})

		require.NotNil(t, conn)
		assert.Equal(t, "localhost", conn.Host)
		assert.Equal(t, "5672", conn.Port)
		assert.Equal(t, "guest", conn.User)
		assert.False(t, conn.AllowInsecureHealthCheck)
	})

	t.Run("does not allow insecure health check for https url by default", func(t *testing.T) {
		t.Parallel()

		cfg := &Config{
			RabbitMQ: RabbitMQConfig{
				HealthURL: "https://rabbitmq.example.com/api/health/checks/alarms",
			},
		}

		conn := createRabbitMQConnection(cfg, &libLog.NopLogger{})

		require.NotNil(t, conn)
		assert.False(t, conn.AllowInsecureHealthCheck)
	})

	t.Run("warns when HTTP health URL is configured but insecure checks stay disabled", func(t *testing.T) {
		t.Parallel()

		cfg := &Config{
			RabbitMQ: RabbitMQConfig{
				HealthURL:                "http://localhost:15672/api/health/checks/alarms",
				AllowInsecureHealthCheck: false,
			},
		}

		logger := &recordingInitLogger{}
		conn := createRabbitMQConnection(cfg, logger)

		require.NotNil(t, conn)
		assert.False(t, conn.AllowInsecureHealthCheck)
		assert.True(
			t,
			logger.hasEntry(
				libLog.LevelWarn,
				"RabbitMQ health URL uses HTTP while insecure checks are disabled; set RABBITMQ_ALLOW_INSECURE_HEALTH_CHECK=true only for local/internal non-production environments",
			),
		)
	})

	t.Run("allows insecure health check when explicitly enabled for local http URL", func(t *testing.T) {
		t.Parallel()

		cfg := &Config{
			RabbitMQ: RabbitMQConfig{
				HealthURL:                "http://localhost:15672/api/health/checks/alarms",
				AllowInsecureHealthCheck: true,
			},
		}

		conn := createRabbitMQConnection(cfg, &libLog.NopLogger{})

		require.NotNil(t, conn)
		assert.True(t, conn.AllowInsecureHealthCheck)
	})

	t.Run("disables insecure health check for external host", func(t *testing.T) {
		t.Parallel()

		cfg := &Config{
			App: AppConfig{EnvName: "staging"},
			RabbitMQ: RabbitMQConfig{
				HealthURL:                "http://rabbitmq.example.com/api/health/checks/alarms",
				AllowInsecureHealthCheck: true,
			},
		}

		logger := &recordingInitLogger{}
		conn := createRabbitMQConnection(cfg, logger)

		require.NotNil(t, conn)
		assert.False(t, conn.AllowInsecureHealthCheck)
		assert.True(
			t,
			logger.hasEntry(libLog.LevelWarn, "RabbitMQ insecure health check is restricted to local/internal hosts"),
		)
	})

	t.Run("warns when insecure health check is enabled in production", func(t *testing.T) {
		t.Parallel()

		cfg := &Config{
			App: AppConfig{EnvName: "production"},
			RabbitMQ: RabbitMQConfig{
				HealthURL:                "http://rabbitmq.example.com/api/health/checks/alarms",
				AllowInsecureHealthCheck: true,
			},
		}

		logger := &recordingInitLogger{}
		conn := createRabbitMQConnection(cfg, logger)

		require.NotNil(t, conn)
		assert.False(t, conn.AllowInsecureHealthCheck)
		assert.True(
			t,
			logger.hasEntry(libLog.LevelWarn, "RabbitMQ health check insecure HTTP is disabled in production"),
		)
	})

	t.Run("handles nil logger safely", func(t *testing.T) {
		t.Parallel()

		cfg := &Config{
			RabbitMQ: RabbitMQConfig{
				HealthURL:                "http://localhost:15672/api/health/checks/alarms",
				AllowInsecureHealthCheck: true,
			},
		}

		assert.NotPanics(t, func() {
			conn := createRabbitMQConnection(cfg, nil)
			require.NotNil(t, conn)
		})
	})

	t.Run("handles nil config safely", func(t *testing.T) {
		t.Parallel()

		logger := &recordingInitLogger{}
		conn := createRabbitMQConnection(nil, logger)

		require.NotNil(t, conn)
		assert.Empty(t, conn.Host)
		assert.Empty(t, conn.HealthCheckURL)
		assert.False(t, conn.AllowInsecureHealthCheck)
		assert.True(
			t,
			logger.hasEntry(
				libLog.LevelError,
				"RabbitMQ connection configuration is nil; using empty defaults and disabling insecure health checks",
			),
		)
	})
}

func TestEvaluateInsecureRabbitMQHealthCheckPolicy(t *testing.T) {
	t.Parallel()

	t.Run("nil config returns false", func(t *testing.T) {
		t.Parallel()

		allowed, reason := evaluateInsecureRabbitMQHealthCheckPolicy(nil)

		assert.False(t, allowed)
		assert.Equal(t, "RabbitMQ health check insecure HTTP is disabled because configuration is nil", reason)
	})

	t.Run("default config returns false", func(t *testing.T) {
		t.Parallel()

		cfg := &Config{}
		allowed, reason := evaluateInsecureRabbitMQHealthCheckPolicy(cfg)

		assert.False(t, allowed)
		assert.Empty(t, reason)
	})

	t.Run("explicit true returns true", func(t *testing.T) {
		t.Parallel()

		cfg := &Config{RabbitMQ: RabbitMQConfig{
			AllowInsecureHealthCheck: true,
			HealthURL:                "http://localhost:15672/api/health/checks/alarms",
		}}

		allowed, reason := evaluateInsecureRabbitMQHealthCheckPolicy(cfg)

		assert.True(t, allowed)
		assert.Empty(t, reason)
	})

	t.Run("external host returns false", func(t *testing.T) {
		t.Parallel()

		cfg := &Config{App: AppConfig{EnvName: "staging"}, RabbitMQ: RabbitMQConfig{
			AllowInsecureHealthCheck: true,
			HealthURL:                "http://rabbitmq.example.com/api/health/checks/alarms",
		}}

		allowed, reason := evaluateInsecureRabbitMQHealthCheckPolicy(cfg)

		assert.False(t, allowed)
		assert.Equal(t, "RabbitMQ insecure health check is restricted to local/internal hosts", reason)
	})

	t.Run("single-label host requires configured RabbitMQ host match", func(t *testing.T) {
		t.Parallel()

		cfg := &Config{App: AppConfig{EnvName: "staging"}, RabbitMQ: RabbitMQConfig{
			AllowInsecureHealthCheck: true,
			Host:                     "broker",
			HealthURL:                "http://rabbitmq:15672/api/health/checks/alarms",
		}}

		allowed, reason := evaluateInsecureRabbitMQHealthCheckPolicy(cfg)

		assert.False(t, allowed)
		assert.Equal(t, "RabbitMQ insecure health check is restricted to local/internal hosts", reason)
	})

	t.Run("single-label host is allowed when it matches configured RabbitMQ host", func(t *testing.T) {
		t.Parallel()

		cfg := &Config{App: AppConfig{EnvName: "staging"}, RabbitMQ: RabbitMQConfig{
			AllowInsecureHealthCheck: true,
			Host:                     "rabbitmq",
			HealthURL:                "http://rabbitmq:15672/api/health/checks/alarms",
		}}

		allowed, reason := evaluateInsecureRabbitMQHealthCheckPolicy(cfg)

		assert.True(t, allowed)
		assert.Empty(t, reason)
	})

	t.Run("requires HTTP health URL when insecure checks are requested", func(t *testing.T) {
		t.Parallel()

		cfg := &Config{App: AppConfig{EnvName: "staging"}, RabbitMQ: RabbitMQConfig{
			AllowInsecureHealthCheck: true,
			HealthURL:                "https://rabbitmq.internal/api/health/checks/alarms",
		}}

		allowed, reason := evaluateInsecureRabbitMQHealthCheckPolicy(cfg)

		assert.False(t, allowed)
		assert.Equal(t, "RabbitMQ insecure health check requires an HTTP health URL", reason)
	})

	t.Run("allows private IP host in non production", func(t *testing.T) {
		t.Parallel()

		cfg := &Config{App: AppConfig{EnvName: "staging"}, RabbitMQ: RabbitMQConfig{
			AllowInsecureHealthCheck: true,
			HealthURL:                "http://10.42.0.7:15672/api/health/checks/alarms",
		}}

		allowed, reason := evaluateInsecureRabbitMQHealthCheckPolicy(cfg)

		assert.True(t, allowed)
		assert.Empty(t, reason)
	})
}

func TestIsInsecureHTTPHealthCheckURL(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name     string
		url      string
		expected bool
	}{
		{"HTTP URL returns true", "http://localhost:15672/api/health", true},
		{"HTTPS URL returns false", "https://localhost:15672/api/health", false},
		{"empty string returns false", "", false},
		{"malformed URL returns false", "://bad-url", false},
		{"HTTP uppercase scheme returns true", "HTTP://localhost:15672/api/health", true},
		{"no scheme returns false", "localhost:15672/api/health", false},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			result := isInsecureHTTPHealthCheckURL(tt.url)
			assert.Equal(t, tt.expected, result)
		})
	}
}

func TestIsAllowedInsecureHealthCheckHost(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name           string
		healthURL      string
		configuredHost string
		expected       bool
	}{
		{"localhost returns true", "http://localhost:15672/api/health", "", true},
		{"IPv4 loopback returns true", "http://127.0.0.1:15672/api/health", "", true},
		{"IPv6 loopback returns true", "http://[::1]:15672/api/health", "", true},
		{"private 10.x IP returns true", "http://10.0.0.5:15672/api/health", "", true},
		{"private 172.16.x IP returns true", "http://172.16.0.1:15672/api/health", "", true},
		{"private 192.168.x IP returns true", "http://192.168.1.1:15672/api/health", "", true},
		{"public IP returns false", "http://8.8.8.8:15672/api/health", "", false},
		{".local suffix returns true", "http://rabbitmq.local:15672/api/health", "", true},
		{".internal suffix returns true", "http://rabbitmq.internal:15672/api/health", "", true},
		{".cluster.local suffix returns true", "http://rabbitmq.svc.cluster.local:15672/api/health", "", true},
		{"external FQDN returns false", "http://rabbitmq.example.com:15672/api/health", "", false},
		{"single-label matching configured host returns true", "http://rabbitmq:15672/api/health", "rabbitmq", true},
		{"single-label not matching configured host returns false", "http://rabbitmq:15672/api/health", "broker", false},
		{"single-label with empty configured host returns false", "http://rabbitmq:15672/api/health", "", false},
		{"malformed URL returns false", "://bad-url", "", false},
		{"empty hostname returns false", "http://:15672/api/health", "", false},
		{"configured host case insensitive match", "http://RABBITMQ:15672/api/health", "rabbitmq", true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			result := isAllowedInsecureHealthCheckHost(tt.healthURL, tt.configuredHost)
			assert.Equal(t, tt.expected, result)
		})
	}
}

func TestInitializeAuthBoundaryLogger(t *testing.T) {
	originalFn := initializeAuthBoundaryLoggerFn
	t.Cleanup(func() {
		initializeAuthBoundaryLoggerFn = originalFn
	})

	t.Run("wraps initializer error", func(t *testing.T) {
		initializeAuthBoundaryLoggerFn = func() (libLog.Logger, error) {
			return nil, errMatchRuleAdapterRequired
		}

		logger, err := initializeAuthBoundaryLogger()

		require.Error(t, err)
		assert.Nil(t, logger)
		assert.Contains(t, err.Error(), "initialize auth boundary logger")
		assert.ErrorIs(t, err, errMatchRuleAdapterRequired)
	})

	t.Run("returns logger on success", func(t *testing.T) {
		expectedLogger := libLog.NewNop()
		initializeAuthBoundaryLoggerFn = func() (libLog.Logger, error) {
			return expectedLogger, nil
		}

		logger, err := initializeAuthBoundaryLogger()

		require.NoError(t, err)
		assert.Equal(t, expectedLogger, logger)
	})

	t.Run("returns error when initializer yields nil logger", func(t *testing.T) {
		initializeAuthBoundaryLoggerFn = func() (libLog.Logger, error) {
			return nil, nil
		}

		logger, err := initializeAuthBoundaryLogger()

		require.Error(t, err)
		assert.Nil(t, logger)
		assert.ErrorIs(t, err, errAuthBoundaryLoggerNil)
	})
}

type initLogEntry struct {
	level libLog.Level
	msg   string
}

type recordingInitLogger struct {
	mu      sync.Mutex
	entries []initLogEntry
}

func (l *recordingInitLogger) Log(_ context.Context, level libLog.Level, msg string, _ ...libLog.Field) {
	l.mu.Lock()
	defer l.mu.Unlock()

	l.entries = append(l.entries, initLogEntry{level: level, msg: msg})
}

func (l *recordingInitLogger) With(_ ...libLog.Field) libLog.Logger {
	return l
}

func (l *recordingInitLogger) WithGroup(_ string) libLog.Logger {
	return l
}

func (l *recordingInitLogger) Enabled(_ libLog.Level) bool {
	return true
}

func (l *recordingInitLogger) Sync(_ context.Context) error {
	return nil
}

func (l *recordingInitLogger) hasEntry(level libLog.Level, msg string) bool {
	l.mu.Lock()
	defer l.mu.Unlock()

	for _, entry := range l.entries {
		if entry.level == level && entry.msg == msg {
			return true
		}
	}

	return false
}

func TestCleanupPostgres(t *testing.T) {
	t.Parallel()

	t.Run("with nil postgres does not panic", func(t *testing.T) {
		t.Parallel()

		assert.NotPanics(t, func() {
			cleanupPostgres(context.Background(), nil, &libLog.NopLogger{})
		})
	})
}

func TestCleanupRedis(t *testing.T) {
	t.Parallel()

	t.Run("with nil redis does not panic", func(t *testing.T) {
		t.Parallel()

		assert.NotPanics(t, func() {
			cleanupRedis(context.Background(), nil, &libLog.NopLogger{})
		})
	})
}

func TestCleanupRabbitMQ(t *testing.T) {
	t.Parallel()

	t.Run("with nil rabbitmq does not panic", func(t *testing.T) {
		t.Parallel()

		assert.NotPanics(t, func() {
			cleanupRabbitMQ(context.Background(), nil, &libLog.NopLogger{})
		})
	})
}

func TestCleanupConnections(t *testing.T) {
	t.Parallel()

	t.Run("with all nil connections does not panic", func(t *testing.T) {
		t.Parallel()

		assert.NotPanics(t, func() {
			cleanupConnections(context.Background(), nil, nil, nil, &libLog.NopLogger{})
		})
	})
}

func TestLogStartupInfo(t *testing.T) {
	t.Parallel()

	t.Run("does not panic with valid config", func(t *testing.T) {
		t.Parallel()

		cfg := &Config{
			App:    AppConfig{EnvName: "test", LogLevel: "info"},
			Server: ServerConfig{Address: ":4018"},
			Postgres: PostgresConfig{
				PrimaryHost: "localhost",
				PrimaryDB:   "matcher",
			},
			Redis:    RedisConfig{Host: "localhost:6379"},
			RabbitMQ: RabbitMQConfig{Host: "localhost"},
			Auth:     AuthConfig{Enabled: false},
			Telemetry: TelemetryConfig{
				Enabled: false,
			},
		}

		status := &InfraStatus{
			PostgresConnected: true,
			RedisConnected:    true,
			RedisMode:         "standalone",
			RabbitMQConnected: true,
		}

		assert.NotPanics(t, func() {
			logStartupInfo(&libLog.NopLogger{}, cfg, status)
		})
	})

	t.Run("logs with all features enabled", func(t *testing.T) {
		t.Parallel()

		cfg := &Config{
			App:    AppConfig{EnvName: "production", LogLevel: "info"},
			Server: ServerConfig{Address: ":8443"},
			Postgres: PostgresConfig{
				PrimaryHost: "db.example.com",
				PrimaryDB:   "matcher_prod",
				ReplicaHost: "replica.example.com",
			},
			Redis:    RedisConfig{Host: "redis.example.com:6379"},
			RabbitMQ: RabbitMQConfig{Host: "rabbitmq.example.com"},
			ObjectStorage: ObjectStorageConfig{
				Endpoint: "https://s3.example.com",
				Bucket:   "matcher-exports",
			},
			Auth:      AuthConfig{Enabled: true},
			Telemetry: TelemetryConfig{Enabled: true},
			Tenancy:   TenancyConfig{MultiTenantEnabled: true},
			ExportWorker: ExportWorkerConfig{
				Enabled:         true,
				PollIntervalSec: 5,
			},
			CleanupWorker: CleanupWorkerConfig{
				Enabled: true,
			},
		}

		status := &InfraStatus{
			PostgresConnected:    true,
			RedisConnected:       true,
			RedisMode:            "standalone",
			RabbitMQConnected:    true,
			ObjectStorageEnabled: true,
			HasReplica:           true,
			ExportWorkerEnabled:  true,
			CleanupWorkerEnabled: true,
		}

		assert.NotPanics(t, func() {
			logStartupInfo(&libLog.NopLogger{}, cfg, status)
		})
	})
}

func TestFormatFeatureStatus_Enabled(t *testing.T) {
	t.Parallel()

	t.Run("returns enabled string when true", func(t *testing.T) {
		t.Parallel()

		result := formatFeatureStatus(true)

		assert.Equal(t, "enabled ✓", result)
	})

	t.Run("returns disabled string when false", func(t *testing.T) {
		t.Parallel()

		result := formatFeatureStatus(false)

		assert.Equal(t, statusDisabled, result)
	})
}

func TestDetectRedisMode(t *testing.T) {
	t.Parallel()

	t.Run("returns sentinel when master name set", func(t *testing.T) {
		t.Parallel()

		cfg := &Config{Redis: RedisConfig{Host: "host1:6379", MasterName: "mymaster"}}

		result := detectRedisMode(cfg)

		assert.Equal(t, "sentinel", result)
	})

	t.Run("returns cluster when multiple hosts", func(t *testing.T) {
		t.Parallel()

		cfg := &Config{Redis: RedisConfig{Host: "host1:6379,host2:6379,host3:6379"}}

		result := detectRedisMode(cfg)

		assert.Equal(t, "cluster", result)
	})

	t.Run("returns standalone for single host", func(t *testing.T) {
		t.Parallel()

		cfg := &Config{Redis: RedisConfig{Host: "localhost:6379"}}

		result := detectRedisMode(cfg)

		assert.Equal(t, "standalone", result)
	})
}

func TestFormatConnStatus(t *testing.T) {
	t.Parallel()

	t.Run("returns checkmark when connected", func(t *testing.T) {
		t.Parallel()

		result := formatConnStatus(true)

		assert.Equal(t, "✅", result)
	})

	t.Run("returns X when not connected", func(t *testing.T) {
		t.Parallel()

		result := formatConnStatus(false)

		assert.Equal(t, "❌", result)
	})
}

func TestFormatWorkerStatus(t *testing.T) {
	t.Parallel()

	t.Run("shows interval when enabled", func(t *testing.T) {
		t.Parallel()

		result := formatWorkerStatus(true, 5*time.Second)

		assert.Equal(t, "enabled (interval: 5s)", result)
	})

	t.Run("shows disabled when not enabled", func(t *testing.T) {
		t.Parallel()

		result := formatWorkerStatus(false, 5*time.Second)

		assert.Equal(t, statusDisabled, result)
	})
}

func TestTenantExtractorAdapter(t *testing.T) {
	adapter := &tenantExtractorAdapter{}

	t.Run("returns tenant UUID from context value", func(t *testing.T) {
		t.Parallel()

		tenantID := "550e8400-e29b-41d4-a716-446655440000"
		ctx := context.WithValue(context.Background(), auth.TenantIDKey, tenantID)

		result := adapter.GetTenantID(ctx)

		expected, err := uuid.Parse(tenantID)
		require.NoError(t, err)
		assert.Equal(t, expected, result)
	})

	t.Run("returns nil UUID for invalid context tenant", func(t *testing.T) {
		t.Parallel()

		ctx := context.WithValue(context.Background(), auth.TenantIDKey, "not-a-uuid")

		result := adapter.GetTenantID(ctx)

		assert.Equal(t, uuid.Nil, result)
	})

	t.Run("uses deterministic default tenant when context has no tenant", func(t *testing.T) {
		originalDefault := auth.GetDefaultTenantID()
		t.Cleanup(func() {
			restoreErr := auth.SetDefaultTenantID(originalDefault)
			require.NoError(t, restoreErr)
		})

		const deterministicTenant = "11111111-1111-1111-1111-111111111111"
		require.NoError(t, auth.SetDefaultTenantID(deterministicTenant))

		result := adapter.GetTenantID(context.Background())

		expected, err := uuid.Parse(deterministicTenant)
		require.NoError(t, err)
		assert.Equal(t, expected, result)
	})
}

func TestErrObjectStorageBucketRequired(t *testing.T) {
	t.Parallel()

	require.Error(t, ErrObjectStorageBucketRequired)
	assert.Equal(
		t,
		"OBJECT_STORAGE_BUCKET is required when EXPORT_WORKER_ENABLED=true",
		ErrObjectStorageBucketRequired.Error(),
	)
}

func TestBuildTenantExtractor(t *testing.T) {
	// Not parallel: buildTenantExtractor mutates process-global auth defaults.

	t.Run("creates extractor with valid config", func(t *testing.T) {
		originalID := auth.GetDefaultTenantID()
		originalSlug := auth.GetTenantSlug(context.Background())
		t.Cleanup(func() {
			require.NoError(t, auth.SetDefaultTenantID(originalID))
			require.NoError(t, auth.SetDefaultTenantSlug(originalSlug))
		})

		cfg := &Config{
			Auth: AuthConfig{Enabled: false, TokenSecret: ""},
			Tenancy: TenancyConfig{
				DefaultTenantID:   "11111111-1111-1111-1111-111111111111",
				DefaultTenantSlug: "default",
			},
			App: AppConfig{EnvName: "test"},
		}

		extractor, err := buildTenantExtractor(cfg)

		require.NoError(t, err)
		require.NotNil(t, extractor)
		assert.Equal(t, cfg.Tenancy.DefaultTenantID, auth.GetDefaultTenantID())
		assert.Equal(t, cfg.Tenancy.DefaultTenantSlug, auth.GetTenantSlug(context.Background()))
	})

	t.Run("creates extractor with auth enabled", func(t *testing.T) {
		originalID := auth.GetDefaultTenantID()
		originalSlug := auth.GetTenantSlug(context.Background())
		t.Cleanup(func() {
			require.NoError(t, auth.SetDefaultTenantID(originalID))
			require.NoError(t, auth.SetDefaultTenantSlug(originalSlug))
		})

		cfg := &Config{
			Auth: AuthConfig{Enabled: true, TokenSecret: "secret"},
			Tenancy: TenancyConfig{
				DefaultTenantID:   "11111111-1111-1111-1111-111111111111",
				DefaultTenantSlug: "default",
			},
			App: AppConfig{EnvName: "test"},
		}

		extractor, err := buildTenantExtractor(cfg)

		require.NoError(t, err)
		require.NotNil(t, extractor)
	})

	t.Run("returns error when auth enabled but secret missing", func(t *testing.T) {
		originalID := auth.GetDefaultTenantID()
		originalSlug := auth.GetTenantSlug(context.Background())
		t.Cleanup(func() {
			require.NoError(t, auth.SetDefaultTenantID(originalID))
			require.NoError(t, auth.SetDefaultTenantSlug(originalSlug))
		})

		cfg := &Config{
			Auth: AuthConfig{Enabled: true, TokenSecret: ""},
			Tenancy: TenancyConfig{
				DefaultTenantID:   "11111111-1111-1111-1111-111111111111",
				DefaultTenantSlug: "default",
			},
			App: AppConfig{EnvName: "test"},
		}

		extractor, err := buildTenantExtractor(cfg)

		require.Error(t, err)
		require.Nil(t, extractor)
	})
}

//nolint:paralleltest // Cannot use t.Parallel() with t.Setenv
func TestInitServers_InvalidEnv(t *testing.T) {
	t.Setenv("DEFAULT_TENANT_ID", "not-a-uuid")
	t.Setenv("INFRA_CONNECT_TIMEOUT_SEC", "1")

	svc, err := InitServersWithOptions(nil)

	assert.Error(t, err)
	assert.Nil(t, svc)
}

func TestCreateRedisConnection_SingleAddress(t *testing.T) {
	t.Parallel()

	cfg := &Config{
		Redis: RedisConfig{
			Host:           "localhost:1",
			Password:       "",
			DB:             1,
			Protocol:       3,
			TLS:            false,
			PoolSize:       5,
			MinIdleConn:    1,
			ReadTimeoutMs:  50,
			WriteTimeoutMs: 50,
			DialTimeoutMs:  50,
		},
	}

	ctx := context.Background()
	conn, err := createRedisConnection(ctx, cfg, &libLog.NopLogger{})

	require.Error(t, err)
	require.Nil(t, conn)
}

func TestBuildRedisConfig(t *testing.T) {
	t.Parallel()

	t.Run("builds standalone topology with defaults", func(t *testing.T) {
		t.Parallel()

		cfg := &Config{Redis: RedisConfig{}}

		redisCfg := buildRedisConfig(cfg, &libLog.NopLogger{})

		require.NotNil(t, redisCfg.Topology.Standalone)
		assert.Equal(t, "localhost:6379", redisCfg.Topology.Standalone.Address)
		assert.Nil(t, redisCfg.Topology.Cluster)
		assert.Nil(t, redisCfg.Topology.Sentinel)
	})

	t.Run("builds standalone topology with explicit host", func(t *testing.T) {
		t.Parallel()

		cfg := &Config{Redis: RedisConfig{Host: "cache.internal:6379"}}

		redisCfg := buildRedisConfig(cfg, &libLog.NopLogger{})

		require.NotNil(t, redisCfg.Topology.Standalone)
		assert.Equal(t, "cache.internal:6379", redisCfg.Topology.Standalone.Address)
	})

	t.Run("builds cluster topology when multiple addresses", func(t *testing.T) {
		t.Parallel()

		cfg := &Config{Redis: RedisConfig{Host: "redis-1:6379,redis-2:6379"}}

		redisCfg := buildRedisConfig(cfg, &libLog.NopLogger{})

		require.NotNil(t, redisCfg.Topology.Cluster)
		assert.Equal(t, []string{"redis-1:6379", "redis-2:6379"}, redisCfg.Topology.Cluster.Addresses)
		assert.Nil(t, redisCfg.Topology.Standalone)
		assert.Nil(t, redisCfg.Topology.Sentinel)
	})

	t.Run("builds sentinel topology when master name is configured", func(t *testing.T) {
		t.Parallel()

		cfg := &Config{Redis: RedisConfig{Host: "sentinel-1:26379,sentinel-2:26379", MasterName: "mymaster"}}

		redisCfg := buildRedisConfig(cfg, &libLog.NopLogger{})

		require.NotNil(t, redisCfg.Topology.Sentinel)
		assert.Equal(t, "mymaster", redisCfg.Topology.Sentinel.MasterName)
		assert.Equal(t, []string{"sentinel-1:26379", "sentinel-2:26379"}, redisCfg.Topology.Sentinel.Addresses)
		assert.Nil(t, redisCfg.Topology.Standalone)
		assert.Nil(t, redisCfg.Topology.Cluster)
	})

	t.Run("builds tls config when enabled", func(t *testing.T) {
		t.Parallel()

		cfg := &Config{Redis: RedisConfig{Host: "cache.internal:6379", TLS: true, CACert: "ZmFrZS1jZXJ0"}}

		redisCfg := buildRedisConfig(cfg, &libLog.NopLogger{})

		require.NotNil(t, redisCfg.TLS)
		assert.Equal(t, "ZmFrZS1jZXJ0", redisCfg.TLS.CACertBase64)
	})
}

func TestCleanupRabbitMQ_WithNilConnection(t *testing.T) {
	t.Parallel()

	assert.NotPanics(t, func() {
		cleanupRabbitMQ(context.Background(), nil, &libLog.NopLogger{})
	})
}

func TestCreateArchivalStorage(t *testing.T) {
	t.Parallel()

	t.Run("returns nil when archival bucket is empty", func(t *testing.T) {
		t.Parallel()

		cfg := &Config{
			Archival: ArchivalConfig{
				StorageBucket: "",
			},
			ObjectStorage: ObjectStorageConfig{
				Endpoint: "http://localhost:8333",
			},
		}

		client, err := createArchivalStorage(context.Background(), cfg)

		assert.NoError(t, err)
		assert.Nil(t, client)
	})

	t.Run("returns nil when endpoint is empty", func(t *testing.T) {
		t.Parallel()

		cfg := &Config{
			Archival: ArchivalConfig{
				StorageBucket: "my-archive-bucket",
			},
			ObjectStorage: ObjectStorageConfig{
				Endpoint: "",
			},
		}

		client, err := createArchivalStorage(context.Background(), cfg)

		assert.NoError(t, err)
		assert.Nil(t, client)
	})

	t.Run("returns nil when both bucket and endpoint are empty", func(t *testing.T) {
		t.Parallel()

		cfg := &Config{
			Archival:      ArchivalConfig{},
			ObjectStorage: ObjectStorageConfig{},
		}

		client, err := createArchivalStorage(context.Background(), cfg)

		assert.NoError(t, err)
		assert.Nil(t, client)
	})
}

func TestInitArchivalComponents_DisabledNoStorage(t *testing.T) {
	t.Parallel()

	t.Run("returns nil worker when archival is disabled and no storage", func(t *testing.T) {
		t.Parallel()

		cfg := &Config{
			Archival: ArchivalConfig{
				Enabled: false,
			},
			ObjectStorage: ObjectStorageConfig{},
		}

		var cleanups []func()
		worker, err := initArchivalComponents(nil, cfg, nil, nil, &libLog.NopLogger{}, &cleanups)

		assert.NoError(t, err)
		assert.Nil(t, worker)
	})
}

func TestLogStartupInfo_WithArchivalWorker(t *testing.T) {
	t.Parallel()

	t.Run("does not panic with archival worker enabled", func(t *testing.T) {
		t.Parallel()

		cfg := &Config{
			App:    AppConfig{EnvName: "test", LogLevel: "info"},
			Server: ServerConfig{Address: ":4018"},
			Postgres: PostgresConfig{
				PrimaryHost: "localhost",
				PrimaryDB:   "matcher",
			},
			Redis:    RedisConfig{Host: "localhost:6379"},
			RabbitMQ: RabbitMQConfig{Host: "localhost"},
			Auth:     AuthConfig{Enabled: false},
			Telemetry: TelemetryConfig{
				Enabled: false,
			},
			Archival: ArchivalConfig{
				Enabled:       true,
				IntervalHours: 24,
			},
			ExportWorker: ExportWorkerConfig{
				PollIntervalSec: 5,
			},
		}

		status := &InfraStatus{
			PostgresConnected:     true,
			RedisConnected:        true,
			RedisMode:             "standalone",
			RabbitMQConnected:     true,
			ArchivalWorkerEnabled: true,
		}

		assert.NotPanics(t, func() {
			logStartupInfo(&libLog.NopLogger{}, cfg, status)
		})
	})
}

func TestModulesResult_ArchivalWorkerField(t *testing.T) {
	t.Parallel()

	t.Run("default nil archival worker in modules result", func(t *testing.T) {
		t.Parallel()

		modules := &modulesResult{}

		assert.Nil(t, modules.archivalWorker)
	})
}

func TestBuildInfraStatus(t *testing.T) {
	t.Parallel()

	t.Run("all nil components", func(t *testing.T) {
		t.Parallel()

		cfg := &Config{
			Postgres: PostgresConfig{},
			Redis:    RedisConfig{},
		}

		status := buildInfraStatus(cfg, nil, nil, nil, nil, nil, nil)

		require.NotNil(t, status)
		assert.False(t, status.PostgresConnected)
		assert.False(t, status.RedisConnected)
		assert.False(t, status.RabbitMQConnected)
		assert.False(t, status.HasReplica)
		assert.False(t, status.ObjectStorageEnabled)
		assert.False(t, status.ExportWorkerEnabled)
		assert.False(t, status.CleanupWorkerEnabled)
		assert.False(t, status.ArchivalWorkerEnabled)
		assert.False(t, status.SchedulerWorkerEnabled)
		assert.False(t, status.TelemetryConfigured)
		assert.False(t, status.TelemetryActive)
		assert.False(t, status.TelemetryDegraded)
		assert.Empty(t, status.RedisMode)
	})

	t.Run("telemetry degraded when configured but inactive", func(t *testing.T) {
		t.Parallel()

		cfg := &Config{
			Telemetry: TelemetryConfig{Enabled: true},
		}

		status := buildInfraStatus(cfg, nil, nil, nil, nil, nil, nil)

		require.NotNil(t, status)
		assert.True(t, status.TelemetryConfigured)
		assert.False(t, status.TelemetryActive)
		assert.True(t, status.TelemetryDegraded)
	})

	t.Run("all connected with replica", func(t *testing.T) {
		t.Parallel()

		cfg := &Config{
			Postgres: PostgresConfig{
				PrimaryHost: "primary.db",
				ReplicaHost: "replica.db",
			},
			Redis: RedisConfig{
				Host: "redis:6379",
			},
		}

		postgres := testutil.NewClientWithResolver(&mockDBResolver{})
		redis := testutil.NewRedisClientConnected()
		rabbitmq := &libRabbitmq.RabbitMQConnection{}

		modules := &modulesResult{
			exportWorker:  nil, // just checking nil safety
			cleanupWorker: nil,
		}

		healthDeps := &HealthDependencies{}

		status := buildInfraStatus(cfg, postgres, redis, rabbitmq, modules, healthDeps, nil)

		require.NotNil(t, status)
		assert.True(t, status.PostgresConnected)
		assert.True(t, status.RedisConnected)
		assert.True(t, status.HasReplica)
		assert.Equal(t, "standalone", status.RedisMode)
	})

	t.Run("worker status follows effective enablement not object existence", func(t *testing.T) {
		t.Parallel()

		cfg := &Config{
			ExportWorker:  ExportWorkerConfig{Enabled: false},
			CleanupWorker: CleanupWorkerConfig{Enabled: false},
			Archival:      ArchivalConfig{Enabled: false},
		}

		modules := &modulesResult{
			exportWorker:   &reportingWorker.ExportWorker{},
			cleanupWorker:  &reportingWorker.CleanupWorker{},
			archivalWorker: &governanceWorker.ArchivalWorker{},
		}

		status := buildInfraStatus(cfg, nil, nil, nil, modules, nil, nil)

		assert.False(t, status.ExportWorkerEnabled)
		assert.False(t, status.CleanupWorkerEnabled)
		assert.False(t, status.ArchivalWorkerEnabled)
	})

	t.Run("same primary and replica host means no replica", func(t *testing.T) {
		t.Parallel()

		cfg := &Config{
			Postgres: PostgresConfig{
				PrimaryHost: "same.db",
				ReplicaHost: "same.db",
			},
			Redis: RedisConfig{},
		}

		status := buildInfraStatus(cfg, nil, nil, nil, nil, nil, nil)

		assert.False(t, status.HasReplica)
	})

	t.Run("redis sentinel mode detected", func(t *testing.T) {
		t.Parallel()

		cfg := &Config{
			Redis: RedisConfig{
				Host:       "redis:6379",
				MasterName: "mymaster",
			},
		}
		redis := testutil.NewRedisClientConnected()

		status := buildInfraStatus(cfg, nil, redis, nil, nil, nil, nil)

		assert.Equal(t, "sentinel", status.RedisMode)
	})

	t.Run("redis cluster mode detected", func(t *testing.T) {
		t.Parallel()

		cfg := &Config{
			Redis: RedisConfig{
				Host: "redis1:6379,redis2:6379",
			},
		}
		redis := testutil.NewRedisClientConnected()

		status := buildInfraStatus(cfg, nil, redis, nil, nil, nil, nil)

		assert.Equal(t, "cluster", status.RedisMode)
	})

	t.Run("redis mode empty when redis nil", func(t *testing.T) {
		t.Parallel()

		cfg := &Config{
			Redis: RedisConfig{
				Host: "redis:6379",
			},
		}

		status := buildInfraStatus(cfg, nil, nil, nil, nil, nil, nil)

		assert.Empty(t, status.RedisMode)
	})
}

func TestRecordCleanup(t *testing.T) {
	t.Parallel()

	t.Run("with nil context does not panic", func(t *testing.T) {
		t.Parallel()

		assert.NotPanics(t, func() {
			//nolint:staticcheck // Intentionally passing nil context to test nil-safety
			recordCleanup(nil, "test", true, time.Millisecond)
		})
	})

	t.Run("success recording does not panic", func(t *testing.T) {
		t.Parallel()

		assert.NotPanics(t, func() {
			recordCleanup(context.Background(), "postgres", true, time.Second)
		})
	})

	t.Run("error recording does not panic", func(t *testing.T) {
		t.Parallel()

		assert.NotPanics(t, func() {
			recordCleanup(context.Background(), "redis", false, time.Second)
		})
	})

	t.Run("with canceled context falls back to background", func(t *testing.T) {
		t.Parallel()

		ctx, cancel := context.WithCancel(context.Background())
		cancel()

		assert.NotPanics(t, func() {
			recordCleanup(ctx, "rabbitmq", true, time.Second)
		})
	})
}

func TestInitCleanupMetrics(t *testing.T) {
	t.Parallel()

	t.Run("returns non-nil collector", func(t *testing.T) {
		t.Parallel()

		metrics := initCleanupMetrics()
		assert.NotNil(t, metrics)
	})
}

func TestFetcherHTTPClientConfig(t *testing.T) {
	t.Parallel()

	cfg := defaultConfig()
	cfg.Fetcher.URL = "http://fetcher.internal:4006"
	cfg.Fetcher.AllowPrivateIPs = false
	cfg.Fetcher.HealthTimeoutSec = 9
	cfg.Fetcher.RequestTimeoutSec = 41

	clientCfg := fetcherHTTPClientConfig(cfg)

	assert.Equal(t, "http://fetcher.internal:4006", clientCfg.BaseURL)
	assert.False(t, clientCfg.AllowPrivateIPs)
	assert.Equal(t, 9*time.Second, clientCfg.HealthTimeout)
	assert.Equal(t, 41*time.Second, clientCfg.RequestTimeout)
	assert.Equal(t, defaultFetcherClientMaxRetries, clientCfg.MaxRetries)
	assert.Equal(t, defaultFetcherClientRetryBaseDelay, clientCfg.RetryBaseDelay)
}

func TestInitOptionalDiscoveryWorker(t *testing.T) {
	t.Parallel()

	t.Run("enabled_fetcher_returns_initialized_worker", func(t *testing.T) {
		t.Parallel()

		logger := &recordingInitLogger{}
		cfg := defaultConfig()
		cfg.Fetcher.Enabled = true

		expectedWorker := &discoveryWorker.DiscoveryWorker{}
		called := false

		worker, err := initOptionalDiscoveryWorker(
			nil,
			cfg,
			nil,
			nil,
			nil,
			logger,
			func(_ *Routes, _ *Config, _ func() *Config, _ sharedPorts.InfrastructureProvider, _ sharedPorts.TenantLister, _ libLog.Logger, _ ...sharedPorts.M2MProvider) (*discoveryWorker.DiscoveryWorker, error) {
				called = true
				return expectedWorker, nil
			},
		)

		require.NoError(t, err)
		assert.True(t, called)
		assert.Same(t, expectedWorker, worker)
		assert.False(t, logger.hasEntry(libLog.LevelWarn, "discovery module failed to initialize"))
	})

	t.Run("disabled_fetcher_still_initializes_shell", func(t *testing.T) {
		t.Parallel()

		logger := &recordingInitLogger{}
		cfg := defaultConfig()
		cfg.Fetcher.Enabled = false

		expectedWorker := &discoveryWorker.DiscoveryWorker{}
		called := false
		worker, err := initOptionalDiscoveryWorker(
			nil,
			cfg,
			nil,
			nil,
			nil,
			logger,
			func(_ *Routes, _ *Config, _ func() *Config, _ sharedPorts.InfrastructureProvider, _ sharedPorts.TenantLister, _ libLog.Logger, _ ...sharedPorts.M2MProvider) (*discoveryWorker.DiscoveryWorker, error) {
				called = true
				return expectedWorker, nil
			},
		)

		require.NoError(t, err)
		assert.Same(t, expectedWorker, worker)
		assert.True(t, called)
	})

	t.Run("enabled_fetcher_failure_returns_error", func(t *testing.T) {
		t.Parallel()

		logger := &recordingInitLogger{}
		cfg := defaultConfig()
		cfg.Fetcher.Enabled = true

		worker, err := initOptionalDiscoveryWorker(
			nil,
			cfg,
			nil,
			nil,
			nil,
			logger,
			func(_ *Routes, _ *Config, _ func() *Config, _ sharedPorts.InfrastructureProvider, _ sharedPorts.TenantLister, _ libLog.Logger, _ ...sharedPorts.M2MProvider) (*discoveryWorker.DiscoveryWorker, error) {
				return nil, errors.New("fetcher bootstrap failed")
			},
		)

		assert.Nil(t, worker)
		require.Error(t, err)
		assert.Contains(t, err.Error(), "initialize discovery module")
	})
}

func TestCreateObjectStorage_NotEnabled(t *testing.T) {
	t.Parallel()

	cfg := &Config{
		ExportWorker: ExportWorkerConfig{
			Enabled: false,
		},
	}

	client, err := createObjectStorage(context.Background(), cfg)

	assert.NoError(t, err)
	assert.Nil(t, client)
}

func TestCreateObjectStorage_EnabledWithoutBucket(t *testing.T) {
	t.Parallel()

	cfg := &Config{
		ExportWorker: ExportWorkerConfig{
			Enabled: true,
		},
		ObjectStorage: ObjectStorageConfig{
			Bucket: "",
		},
	}

	client, err := createObjectStorage(context.Background(), cfg)

	assert.Error(t, err)
	assert.ErrorIs(t, err, ErrObjectStorageBucketRequired)
	assert.Nil(t, client)
}

func TestCreateObjectStorage_CleanupEnabledWithoutBucket(t *testing.T) {
	t.Parallel()

	cfg := &Config{
		ExportWorker:  ExportWorkerConfig{Enabled: false},
		CleanupWorker: CleanupWorkerConfig{Enabled: true},
		ObjectStorage: ObjectStorageConfig{Bucket: ""},
	}

	client, err := createObjectStorage(context.Background(), cfg)

	assert.Error(t, err)
	assert.ErrorIs(t, err, ErrObjectStorageBucketRequired)
	assert.Nil(t, client)
}

func TestReportingStorageRequired(t *testing.T) {
	t.Parallel()

	assert.False(t, reportingStorageRequired(nil))
	assert.False(t, reportingStorageRequired(&Config{}))
	assert.True(t, reportingStorageRequired(&Config{ExportWorker: ExportWorkerConfig{Enabled: true}}))
	assert.True(t, reportingStorageRequired(&Config{CleanupWorker: CleanupWorkerConfig{Enabled: true}}))
}

func TestCreateObjectStorageForHealth_EmptyEndpoint(t *testing.T) {
	t.Parallel()

	cfg := &Config{
		ObjectStorage: ObjectStorageConfig{
			Endpoint: "",
			Bucket:   "my-bucket",
		},
	}

	client, err := createObjectStorageForHealth(context.Background(), cfg)

	assert.NoError(t, err)
	assert.Nil(t, client)
}

func TestCreateObjectStorageForHealth_EmptyBucket(t *testing.T) {
	t.Parallel()

	cfg := &Config{
		ObjectStorage: ObjectStorageConfig{
			Endpoint: "http://s3:8333",
			Bucket:   "",
		},
	}

	client, err := createObjectStorageForHealth(context.Background(), cfg)

	assert.NoError(t, err)
	assert.Nil(t, client)
}

func TestCreateObjectStorageForHealth_BothEmpty(t *testing.T) {
	t.Parallel()

	cfg := &Config{
		ObjectStorage: ObjectStorageConfig{
			Endpoint: "",
			Bucket:   "",
		},
	}

	client, err := createObjectStorageForHealth(context.Background(), cfg)

	assert.NoError(t, err)
	assert.Nil(t, client)
}

func TestErrArchivalStorageRequired(t *testing.T) {
	t.Parallel()

	require.Error(t, ErrArchivalStorageRequired)
	assert.Equal(
		t,
		"archival storage is required when ARCHIVAL_WORKER_ENABLED=true",
		ErrArchivalStorageRequired.Error(),
	)
}

func TestErrAuditPublisherRequired(t *testing.T) {
	t.Parallel()

	// Verify the sentinel error exists and is properly defined.
	// The bootstrap nil guard at startup uses this error to abort
	// when audit publishing capability is missing (SOX compliance).
	require.Error(t, ErrAuditPublisherRequired)
	assert.Equal(
		t,
		"audit publisher is required: compliance-critical audit events must not be dropped",
		ErrAuditPublisherRequired.Error(),
	)

	// Verify error identity works with errors.Is for guard code.
	wrappedErr := fmt.Errorf("init modules: %w", ErrAuditPublisherRequired)
	assert.ErrorIs(t, wrappedErr, ErrAuditPublisherRequired)
}

func TestInfraStatusStruct(t *testing.T) {
	t.Parallel()

	t.Run("default values are all false", func(t *testing.T) {
		t.Parallel()

		status := InfraStatus{}

		assert.False(t, status.PostgresConnected)
		assert.False(t, status.RedisConnected)
		assert.False(t, status.RabbitMQConnected)
		assert.False(t, status.HasReplica)
		assert.False(t, status.ObjectStorageEnabled)
		assert.False(t, status.ExportWorkerEnabled)
		assert.False(t, status.CleanupWorkerEnabled)
		assert.False(t, status.ArchivalWorkerEnabled)
		assert.False(t, status.SchedulerWorkerEnabled)
		assert.False(t, status.TelemetryConfigured)
		assert.False(t, status.TelemetryActive)
		assert.False(t, status.TelemetryDegraded)
		assert.Empty(t, status.RedisMode)
	})
}

func TestCreateIdempotencyRepository_NilProvider(t *testing.T) {
	t.Parallel()

	cfg := &Config{
		Idempotency: IdempotencyConfig{
			RetryWindowSec:  300,
			SuccessTTLHours: 168,
		},
	}

	repo := createIdempotencyRepository(cfg, nil, nil, &libLog.NopLogger{})
	assert.Nil(t, repo)
}

func TestModulesResult_SchedulerWorkerField(t *testing.T) {
	t.Parallel()

	modules := &modulesResult{}
	assert.Nil(t, modules.schedulerWorker)
}

func TestCleanupPostgres_SuccessfulClose(t *testing.T) {
	t.Parallel()

	db, mock, err := sqlmock.New()
	require.NoError(t, err)

	mock.ExpectClose()

	resolver := dbresolver.New(dbresolver.WithPrimaryDBs(db))
	postgres := testutil.NewClientWithResolver(resolver)

	assert.NotPanics(t, func() {
		cleanupPostgres(context.Background(), postgres, &libLog.NopLogger{})
	})

	require.NoError(t, mock.ExpectationsWereMet())
}

func TestCleanupPostgres_CloseError(t *testing.T) {
	t.Parallel()

	db, mock, err := sqlmock.New()
	require.NoError(t, err)

	mock.ExpectClose().WillReturnError(errMatchRuleAdapterRequired) // reuse existing sentinel for test

	resolver := dbresolver.New(dbresolver.WithPrimaryDBs(db))
	postgres := testutil.NewClientWithResolver(resolver)

	assert.NotPanics(t, func() {
		cleanupPostgres(context.Background(), postgres, &libLog.NopLogger{})
	})
}

func TestCleanupPostgres_NilConnectionDB(t *testing.T) {
	t.Parallel()

	postgres := &libPostgres.Client{}

	assert.NotPanics(t, func() {
		cleanupPostgres(context.Background(), postgres, &libLog.NopLogger{})
	})
}

func TestCleanupRedis_NonNilConnection(t *testing.T) {
	t.Parallel()

	// A Redis connection that has not been connected will return nil client,
	// which Close() handles gracefully.
	redis := testutil.NewRedisClientWithMock(nil)

	assert.NotPanics(t, func() {
		cleanupRedis(context.Background(), redis, &libLog.NopLogger{})
	})
}

func TestCleanupRabbitMQ_WithNilChannelAndConnection(t *testing.T) {
	t.Parallel()

	rabbitmq := &libRabbitmq.RabbitMQConnection{
		Channel:    nil,
		Connection: nil,
	}

	assert.NotPanics(t, func() {
		cleanupRabbitMQ(context.Background(), rabbitmq, &libLog.NopLogger{})
	})
}

func TestCleanupConnections_WithAllNonNilButUnconnected(t *testing.T) {
	t.Parallel()

	postgres := &libPostgres.Client{}
	redis := testutil.NewRedisClientWithMock(nil)
	rabbitmq := &libRabbitmq.RabbitMQConnection{}

	assert.NotPanics(t, func() {
		cleanupConnections(context.Background(), postgres, redis, rabbitmq, &libLog.NopLogger{})
	})
}

func TestCreateObjectStorage_ValidBucketButNoEndpoint(t *testing.T) {
	originalNewS3Client := newS3ClientFn
	t.Cleanup(func() { newS3ClientFn = originalNewS3Client })

	newS3ClientFn = func(context.Context, reportingStorage.S3Config) (*reportingStorage.S3Client, error) {
		return nil, errS3ClientCreation
	}

	cfg := &Config{
		ExportWorker: ExportWorkerConfig{
			Enabled: true,
		},
		ObjectStorage: ObjectStorageConfig{
			Bucket:   "my-bucket",
			Endpoint: "",
		},
	}

	client, err := createObjectStorage(context.Background(), cfg)
	require.Error(t, err)
	assert.Nil(t, client)
	assert.Contains(t, err.Error(), "create S3 client")
}

func TestCreateObjectStorageForHealth_ValidConfig(t *testing.T) {
	originalNewS3Client := newS3ClientFn
	t.Cleanup(func() { newS3ClientFn = originalNewS3Client })

	newS3ClientFn = func(context.Context, reportingStorage.S3Config) (*reportingStorage.S3Client, error) {
		return &reportingStorage.S3Client{}, nil
	}

	cfg := &Config{
		ObjectStorage: ObjectStorageConfig{
			Endpoint:     "http://localhost:8333",
			Bucket:       "test-bucket",
			Region:       "us-east-1",
			UsePathStyle: true,
		},
	}

	client, err := createObjectStorageForHealth(context.Background(), cfg)
	require.NoError(t, err)
	assert.NotNil(t, client)
}

func TestCreateArchivalStorage_ValidConfig(t *testing.T) {
	originalNewS3Client := newS3ClientFn
	t.Cleanup(func() { newS3ClientFn = originalNewS3Client })

	newS3ClientFn = func(context.Context, reportingStorage.S3Config) (*reportingStorage.S3Client, error) {
		return &reportingStorage.S3Client{}, nil
	}

	cfg := &Config{
		Archival: ArchivalConfig{
			StorageBucket: "archive-bucket",
		},
		ObjectStorage: ObjectStorageConfig{
			Endpoint:     "http://localhost:8333",
			Region:       "us-east-1",
			UsePathStyle: true,
		},
	}

	client, err := createArchivalStorage(context.Background(), cfg)
	require.NoError(t, err)
	assert.NotNil(t, client)
}

func TestHealthConnMaxLifetime(t *testing.T) {
	t.Parallel()

	assert.Equal(t, 5*time.Minute, healthConnMaxLifetime)
}

func TestArchivalPoolConstants(t *testing.T) {
	t.Parallel()

	t.Run("archival pool constants are reasonable", func(t *testing.T) {
		t.Parallel()

		assert.Equal(t, 3, archivalMaxOpenConns)
		assert.Equal(t, 1, archivalMaxIdleConns)
		assert.True(t, archivalMaxOpenConns >= archivalMaxIdleConns)
	})
}

func TestOpenDedicatedChannel(t *testing.T) {
	t.Parallel()

	t.Run("returns error when connection is nil", func(t *testing.T) {
		t.Parallel()

		ch, err := openDedicatedChannel(nil)

		assert.Nil(t, ch)
		require.Error(t, err)
		assert.ErrorIs(t, err, errRabbitMQConnectionNil)
	})

	t.Run("returns error when underlying AMQP connection is nil", func(t *testing.T) {
		t.Parallel()

		conn := &libRabbitmq.RabbitMQConnection{
			Connection: nil,
		}

		ch, err := openDedicatedChannel(conn)

		assert.Nil(t, ch)
		require.Error(t, err)
		assert.ErrorIs(t, err, errRabbitMQConnectionNil)
	})
}

func TestInitEventPublishers_OpenChannelFailure_CleansUpOpenedChannel(t *testing.T) {
	var openCalls atomic.Int32

	var closedChannels atomic.Int32

	restore := setEventPublisherFnsForTest(eventPublisherFnOverrides{
		openDedicatedChannelFn: func(*libRabbitmq.RabbitMQConnection) (*amqp.Channel, error) {
			if openCalls.Add(1) == 1 {
				return new(amqp.Channel), nil
			}

			return nil, errors.New("open failed")
		},
		closeAMQPChannelFn: func(*amqp.Channel) error {
			closedChannels.Add(1)

			return nil
		},
		newMatchingEventPublisherFromChannelFn: func(*amqp.Channel, ...sharedRabbitmq.ConfirmablePublisherOption) (*matchingRabbitmq.EventPublisher, error) {
			return nil, errors.New("unexpected matching publisher creation")
		},
		newIngestionEventPublisherFromChannelFn: func(*amqp.Channel, ...sharedRabbitmq.ConfirmablePublisherOption) (*ingestionRabbitmq.EventPublisher, error) {
			return nil, errors.New("unexpected ingestion publisher creation")
		},
		closeMatchingEventPublisherFn:  func(*matchingRabbitmq.EventPublisher) error { return nil },
		closeIngestionEventPublisherFn: func(*ingestionRabbitmq.EventPublisher) error { return nil },
	})
	t.Cleanup(restore)

	matchingPublisher, ingestionPublisher, err := initEventPublishers(context.Background(), &libRabbitmq.RabbitMQConnection{}, &libLog.NopLogger{}, nil)

	require.Error(t, err)
	assert.Contains(t, err.Error(), "open AMQP channels")
	assert.Nil(t, matchingPublisher)
	assert.Nil(t, ingestionPublisher)
	assert.EqualValues(t, 1, closedChannels.Load())
}

func TestInitEventPublishers_MatchingPublisherFailure_CleansUpChannels(t *testing.T) {
	var closedChannels atomic.Int32

	restore := setEventPublisherFnsForTest(eventPublisherFnOverrides{
		openDedicatedChannelFn: func(*libRabbitmq.RabbitMQConnection) (*amqp.Channel, error) {
			return new(amqp.Channel), nil
		},
		closeAMQPChannelFn: func(*amqp.Channel) error {
			closedChannels.Add(1)

			return nil
		},
		newMatchingEventPublisherFromChannelFn: func(*amqp.Channel, ...sharedRabbitmq.ConfirmablePublisherOption) (*matchingRabbitmq.EventPublisher, error) {
			return nil, errors.New("matching constructor failed")
		},
		newIngestionEventPublisherFromChannelFn: func(*amqp.Channel, ...sharedRabbitmq.ConfirmablePublisherOption) (*ingestionRabbitmq.EventPublisher, error) {
			return nil, errors.New("unexpected ingestion publisher creation")
		},
		closeMatchingEventPublisherFn:  func(*matchingRabbitmq.EventPublisher) error { return nil },
		closeIngestionEventPublisherFn: func(*ingestionRabbitmq.EventPublisher) error { return nil },
	})
	t.Cleanup(restore)

	matchingPublisher, ingestionPublisher, err := initEventPublishers(context.Background(), &libRabbitmq.RabbitMQConnection{}, &libLog.NopLogger{}, nil)

	require.Error(t, err)
	assert.Contains(t, err.Error(), "create matching event publisher")
	assert.Nil(t, matchingPublisher)
	assert.Nil(t, ingestionPublisher)
	assert.EqualValues(t, 2, closedChannels.Load())
}

func TestInitEventPublishers_IngestionPublisherFailure_CleansUpPublisherAndChannels(t *testing.T) {
	var closedChannels atomic.Int32

	var closedMatchingPublishers atomic.Int32

	restore := setEventPublisherFnsForTest(eventPublisherFnOverrides{
		openDedicatedChannelFn: func(*libRabbitmq.RabbitMQConnection) (*amqp.Channel, error) {
			return new(amqp.Channel), nil
		},
		closeAMQPChannelFn: func(*amqp.Channel) error {
			closedChannels.Add(1)

			return nil
		},
		newMatchingEventPublisherFromChannelFn: func(*amqp.Channel, ...sharedRabbitmq.ConfirmablePublisherOption) (*matchingRabbitmq.EventPublisher, error) {
			return new(matchingRabbitmq.EventPublisher), nil
		},
		newIngestionEventPublisherFromChannelFn: func(*amqp.Channel, ...sharedRabbitmq.ConfirmablePublisherOption) (*ingestionRabbitmq.EventPublisher, error) {
			return nil, errors.New("ingestion constructor failed")
		},
		closeMatchingEventPublisherFn: func(*matchingRabbitmq.EventPublisher) error {
			closedMatchingPublishers.Add(1)

			return nil
		},
		closeIngestionEventPublisherFn: func(*ingestionRabbitmq.EventPublisher) error { return nil },
	})
	t.Cleanup(restore)

	matchingPublisher, ingestionPublisher, err := initEventPublishers(context.Background(), &libRabbitmq.RabbitMQConnection{}, &libLog.NopLogger{}, nil)

	require.Error(t, err)
	assert.Contains(t, err.Error(), "create ingestion event publisher")
	assert.Nil(t, matchingPublisher)
	assert.Nil(t, ingestionPublisher)
	assert.EqualValues(t, 1, closedMatchingPublishers.Load())
	assert.EqualValues(t, 2, closedChannels.Load())
}

func TestErrRabbitMQConnectionNil(t *testing.T) {
	t.Parallel()

	require.Error(t, errRabbitMQConnectionNil)
	assert.Equal(t, "rabbitmq connection or underlying AMQP connection is nil", errRabbitMQConnectionNil.Error())
}

func TestSharedRepositoriesStruct(t *testing.T) {
	t.Parallel()

	t.Run("has expected zero value fields", func(t *testing.T) {
		t.Parallel()

		repos := &sharedRepositories{}

		assert.Nil(t, repos.configContext)
		assert.Nil(t, repos.configSource)
		assert.Nil(t, repos.configFieldMap)
		assert.Nil(t, repos.configMatchRule)
		assert.Nil(t, repos.governanceAuditLog)
		assert.Nil(t, repos.ingestionTx)
		assert.Nil(t, repos.ingestionJob)
		assert.Nil(t, repos.feeSchedule)
		assert.Nil(t, repos.adjustment)
	})
}

func TestShouldAllowDirtyMigrationRecovery(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name     string
		env      string
		expected bool
	}{
		{name: "development", env: "development", expected: true},
		{name: "local", env: "local", expected: true},
		{name: "test", env: "test", expected: true},
		{name: "staging", env: "staging", expected: false},
		{name: "production", env: "production", expected: false},
	}

	for _, tt := range tests {
		tt := tt

		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			assert.Equal(t, tt.expected, shouldAllowDirtyMigrationRecovery(tt.env))
		})
	}
}

func TestConnectInfrastructure_RunsAllServicesAndMaintainsDependencyOrder(t *testing.T) {
	originalRunMigrationsFn := runMigrationsFn
	originalConnectPostgresFn := connectPostgresFn
	originalEnsureRabbitChannelFn := ensureRabbitChannelFn

	t.Cleanup(func() {
		runMigrationsFn = originalRunMigrationsFn
		connectPostgresFn = originalConnectPostgresFn
		ensureRabbitChannelFn = originalEnsureRabbitChannelFn
	})

	// Thread-safe tracking: operations run in parallel goroutines.
	var mu sync.Mutex
	order := make([]string, 0, 3)
	capturedAllowDirtyRecovery := true

	appendOp := func(op string) {
		mu.Lock()
		defer mu.Unlock()

		order = append(order, op)
	}

	runMigrationsFn = func(
		_ context.Context,
		_, _, _ string,
		_ libLog.Logger,
		allowDirtyRecovery bool,
	) error {
		appendOp("migrate")

		mu.Lock()
		capturedAllowDirtyRecovery = allowDirtyRecovery
		mu.Unlock()

		return nil
	}

	connectPostgresFn = func(_ context.Context, _ *libPostgres.Client) error {
		appendOp("postgres")

		return nil
	}

	ensureRabbitChannelFn = func(_ *libRabbitmq.RabbitMQConnection) error {
		appendOp("rabbitmq")

		return nil
	}

	cfg := &Config{
		App: AppConfig{EnvName: "staging"},
		Postgres: PostgresConfig{
			PrimaryDB:      "matcher",
			MigrationsPath: "migrations",
		},
	}

	asserter := libAssert.New(context.Background(), &libLog.NopLogger{}, "matcher", "connect_infrastructure.test")

	err := connectInfrastructure(
		context.Background(),
		asserter,
		cfg,
		&libPostgres.Client{},
		&libRabbitmq.RabbitMQConnection{},
		&libLog.NopLogger{},
	)

	require.NoError(t, err)

	mu.Lock()
	defer mu.Unlock()

	// All three operations must have executed (Redis verification removed — handled by createRedisConnection).
	assert.Len(t, order, 3)
	assert.Contains(t, order, "migrate")
	assert.Contains(t, order, "postgres")
	assert.Contains(t, order, "rabbitmq")

	// Migrations must complete before postgres connect (they're in the same goroutine).
	migrateIdx := -1
	postgresIdx := -1

	for i, op := range order {
		if op == "migrate" {
			migrateIdx = i
		}

		if op == "postgres" {
			postgresIdx = i
		}
	}

	assert.Greater(t, postgresIdx, migrateIdx, "postgres must connect after migrations complete")

	// Staging environment should NOT allow dirty recovery.
	assert.False(t, capturedAllowDirtyRecovery)
}

func TestConnectInfrastructure_NilDependenciesReturnErrors(t *testing.T) {
	t.Parallel()

	asserter := libAssert.New(context.Background(), &libLog.NopLogger{}, "matcher", "connect_infrastructure.nil")
	cfg := &Config{}

	err := connectInfrastructure(context.Background(), asserter, cfg, nil, &libRabbitmq.RabbitMQConnection{}, &libLog.NopLogger{})
	require.ErrorIs(t, err, errPostgresClientRequired)

	err = connectInfrastructure(context.Background(), asserter, cfg, &libPostgres.Client{}, nil, &libLog.NopLogger{})
	require.ErrorIs(t, err, errRabbitMQClientRequired)
}

func TestConnectInfrastructure_RunMigrationsError_ReturnsWrappedError(t *testing.T) {
	originalRunMigrationsFn := runMigrationsFn
	originalConnectPostgresFn := connectPostgresFn
	originalEnsureRabbitChannelFn := ensureRabbitChannelFn

	t.Cleanup(func() {
		runMigrationsFn = originalRunMigrationsFn
		connectPostgresFn = originalConnectPostgresFn
		ensureRabbitChannelFn = originalEnsureRabbitChannelFn
	})

	runMigrationsFn = func(context.Context, string, string, string, libLog.Logger, bool) error {
		return errors.New("migrations failed")
	}

	connectPostgresFn = func(context.Context, *libPostgres.Client) error { return nil }
	ensureRabbitChannelFn = func(*libRabbitmq.RabbitMQConnection) error { return nil }

	asserter := libAssert.New(context.Background(), &libLog.NopLogger{}, "matcher", "connect_infrastructure.migrations_error")
	err := connectInfrastructure(
		context.Background(),
		asserter,
		&Config{Postgres: PostgresConfig{PrimaryDB: "matcher"}},
		&libPostgres.Client{},
		&libRabbitmq.RabbitMQConnection{},
		&libLog.NopLogger{},
	)

	require.Error(t, err)
	assert.ErrorContains(t, err, "run migrations")
}

func TestConnectInfrastructure_ConnectPostgresError_ReturnsWrappedError(t *testing.T) {
	originalRunMigrationsFn := runMigrationsFn
	originalConnectPostgresFn := connectPostgresFn
	originalEnsureRabbitChannelFn := ensureRabbitChannelFn

	t.Cleanup(func() {
		runMigrationsFn = originalRunMigrationsFn
		connectPostgresFn = originalConnectPostgresFn
		ensureRabbitChannelFn = originalEnsureRabbitChannelFn
	})

	runMigrationsFn = func(context.Context, string, string, string, libLog.Logger, bool) error { return nil }
	connectPostgresFn = func(context.Context, *libPostgres.Client) error { return errors.New("postgres connect failed") }
	ensureRabbitChannelFn = func(*libRabbitmq.RabbitMQConnection) error { return nil }

	asserter := libAssert.New(context.Background(), &libLog.NopLogger{}, "matcher", "connect_infrastructure.postgres_error")
	err := connectInfrastructure(
		context.Background(),
		asserter,
		&Config{Postgres: PostgresConfig{PrimaryDB: "matcher"}},
		&libPostgres.Client{},
		&libRabbitmq.RabbitMQConnection{},
		&libLog.NopLogger{},
	)

	require.Error(t, err)
	assert.ErrorContains(t, err, "connect postgres")
}

func TestConnectInfrastructure_EnsureRabbitChannelError_ReturnsWrappedError(t *testing.T) {
	originalRunMigrationsFn := runMigrationsFn
	originalConnectPostgresFn := connectPostgresFn
	originalEnsureRabbitChannelFn := ensureRabbitChannelFn

	t.Cleanup(func() {
		runMigrationsFn = originalRunMigrationsFn
		connectPostgresFn = originalConnectPostgresFn
		ensureRabbitChannelFn = originalEnsureRabbitChannelFn
	})

	runMigrationsFn = func(context.Context, string, string, string, libLog.Logger, bool) error { return nil }
	connectPostgresFn = func(context.Context, *libPostgres.Client) error { return nil }
	ensureRabbitChannelFn = func(*libRabbitmq.RabbitMQConnection) error { return errors.New("rabbit channel failed") }

	asserter := libAssert.New(context.Background(), &libLog.NopLogger{}, "matcher", "connect_infrastructure.rabbit_error")
	err := connectInfrastructure(
		context.Background(),
		asserter,
		&Config{Postgres: PostgresConfig{PrimaryDB: "matcher"}},
		&libPostgres.Client{},
		&libRabbitmq.RabbitMQConnection{},
		&libLog.NopLogger{},
	)

	require.Error(t, err)
	assert.ErrorContains(t, err, "connect rabbitmq")
}

func TestConnectInfrastructure_MigrationsFinishBeforeRabbitConnect(t *testing.T) {
	originalRunMigrationsFn := runMigrationsFn
	originalConnectPostgresFn := connectPostgresFn
	originalEnsureRabbitChannelFn := ensureRabbitChannelFn

	t.Cleanup(func() {
		runMigrationsFn = originalRunMigrationsFn
		connectPostgresFn = originalConnectPostgresFn
		ensureRabbitChannelFn = originalEnsureRabbitChannelFn
	})

	var migrationsFinished atomic.Bool

	runMigrationsFn = func(context.Context, string, string, string, libLog.Logger, bool) error {
		migrationsFinished.Store(true)

		return nil
	}

	connectPostgresFn = func(context.Context, *libPostgres.Client) error { return nil }
	ensureRabbitChannelFn = func(*libRabbitmq.RabbitMQConnection) error {
		if !migrationsFinished.Load() {
			return errors.New("rabbit connected before migrations")
		}

		return nil
	}

	asserter := libAssert.New(context.Background(), &libLog.NopLogger{}, "matcher", "connect_infrastructure.migration_order")
	err := connectInfrastructure(
		context.Background(),
		asserter,
		&Config{Postgres: PostgresConfig{PrimaryDB: "matcher"}},
		&libPostgres.Client{},
		&libRabbitmq.RabbitMQConnection{},
		&libLog.NopLogger{},
	)

	require.NoError(t, err)
	assert.True(t, migrationsFinished.Load())
}

func TestConnectInfrastructure_ContextCanceled_PropagatesError(t *testing.T) {
	originalRunMigrationsFn := runMigrationsFn
	originalConnectPostgresFn := connectPostgresFn
	originalEnsureRabbitChannelFn := ensureRabbitChannelFn

	t.Cleanup(func() {
		runMigrationsFn = originalRunMigrationsFn
		connectPostgresFn = originalConnectPostgresFn
		ensureRabbitChannelFn = originalEnsureRabbitChannelFn
	})

	ctx, cancel := context.WithCancel(context.Background())

	cancel()

	runMigrationsFn = func(fnCtx context.Context, _, _, _ string, _ libLog.Logger, _ bool) error {
		return fnCtx.Err()
	}
	connectPostgresFn = func(context.Context, *libPostgres.Client) error { return nil }
	ensureRabbitChannelFn = func(*libRabbitmq.RabbitMQConnection) error { return nil }

	asserter := libAssert.New(context.Background(), &libLog.NopLogger{}, "matcher", "connect_infrastructure.timeout")
	err := connectInfrastructure(
		ctx,
		asserter,
		&Config{Postgres: PostgresConfig{PrimaryDB: "matcher"}},
		&libPostgres.Client{},
		&libRabbitmq.RabbitMQConnection{},
		&libLog.NopLogger{},
	)

	require.Error(t, err)
	// The error should indicate context cancellation propagated through.
	assert.True(t, errors.Is(err, context.Canceled), "expected context.Canceled in error chain, got: %v", err)
}
