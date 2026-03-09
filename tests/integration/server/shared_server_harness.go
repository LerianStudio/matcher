//go:build integration

package server

import (
	"context"
	"fmt"
	"net/url"
	"os"
	"path/filepath"
	"runtime"
	"strings"
	"sync"
	"testing"

	libLog "github.com/LerianStudio/lib-commons/v4/commons/log"
	"github.com/redis/go-redis/v9"

	"github.com/LerianStudio/matcher/internal/bootstrap"
	"github.com/LerianStudio/matcher/tests/integration"
)

// sharedService holds the singleton service instance shared across all tests.
// This prevents connection pool exhaustion by reusing the same service.
var (
	sharedService     *bootstrap.Service
	sharedServiceOnce sync.Once
	sharedServiceErr  error
)

// SharedServerHarness provides a full-stack test environment using shared infrastructure.
type SharedServerHarness struct {
	*integration.SharedTestHarness
	serverHarnessBase

	Service *bootstrap.Service
}

// NewSharedServerHarness creates a server harness using shared infrastructure.
func NewSharedServerHarness(ctx context.Context, t *testing.T) (*SharedServerHarness, error) {
	t.Helper()

	// Get shared test harness (uses shared containers)
	baseHarness, err := integration.NewSharedTestHarness(t)
	if err != nil {
		return nil, fmt.Errorf("failed to create shared test harness: %w", err)
	}

	sh := &SharedServerHarness{
		SharedTestHarness: baseHarness,
	}
	sh.serverHarnessBase = serverHarnessBase{
		t:                 t,
		PostgresDSN:       baseHarness.PostgresDSN,
		RedisAddr:         baseHarness.RedisAddr,
		RabbitMQHost:      baseHarness.RabbitMQHost,
		RabbitMQPort:      baseHarness.RabbitMQPort,
		RabbitMQHealthURL: baseHarness.RabbitMQHealthURL,
		Seed:              baseHarness.Seed,
	}

	// Flush Redis to clear stale idempotency keys from previous test runs
	if err := flushRedis(ctx, baseHarness.RedisAddr); err != nil {
		t.Logf("warning: failed to flush redis: %v", err)
	}

	// Get or create shared service (reused across tests)
	svc, err := sh.getOrCreateSharedService(t)
	if err != nil {
		if cleanupErr := baseHarness.Cleanup(); cleanupErr != nil {
			t.Logf("cleanup error after service init failure: %v", cleanupErr)
		}
		return nil, fmt.Errorf("failed to initialize service: %w", err)
	}

	sh.Service = svc
	sh.App = svc.GetApp()

	// Extract outbox dispatcher for controlled dispatch
	if dispatcher, ok := extractOutboxDispatcher(svc); ok {
		sh.OutboxDispatcher = dispatcher
	}

	// Setup RabbitMQ consumer spy (per-test, lightweight)
	if err := sh.setupRabbitSpy(t); err != nil {
		if cleanupErr := baseHarness.Cleanup(); cleanupErr != nil {
			t.Logf("cleanup error after rabbit spy setup failure: %v", cleanupErr)
		}
		return nil, fmt.Errorf("failed to setup rabbit spy: %w", err)
	}

	return sh, nil
}

// getOrCreateSharedService returns the singleton service, creating it if needed.
func (sh *SharedServerHarness) getOrCreateSharedService(t *testing.T) (*bootstrap.Service, error) {
	t.Helper()

	sharedServiceOnce.Do(func() {
		// Set environment variables for bootstrap (must happen before InitServersWithOptions)
		if err := setSharedEnvFromContainers(t, sh); err != nil {
			sharedServiceErr = fmt.Errorf("failed to set environment: %w", err)
			return
		}

		// Initialize the service once
		svc, err := bootstrap.InitServersWithOptions(&bootstrap.Options{
			Logger: &libLog.NopLogger{},
		})
		if err != nil {
			sharedServiceErr = err
			return
		}

		sharedService = svc
	})

	return sharedService, sharedServiceErr
}

// setSharedEnvFromContainers sets environment variables from container info.
// This is called only once during service initialization.
func setSharedEnvFromContainers(t *testing.T, sh *SharedServerHarness) error {
	t.Helper()

	// Parse PostgreSQL DSN
	pgURL, err := url.Parse(sh.PostgresDSN)
	if err != nil {
		return fmt.Errorf("failed to parse postgres DSN: %w", err)
	}

	pgHost, pgPort, _ := strings.Cut(pgURL.Host, ":")
	if pgPort == "" {
		pgPort = "5432"
	}
	pgUser := pgURL.User.Username()
	pgPass, _ := pgURL.User.Password()
	pgDB := strings.TrimPrefix(pgURL.Path, "/")

	// Parse Redis address
	redisURL, err := url.Parse(sh.RedisAddr)
	if err != nil {
		return fmt.Errorf("failed to parse redis address: %w", err)
	}
	redisHost := redisURL.Host

	// Compute absolute path to migrations folder
	_, currentFile, _, ok := runtime.Caller(0)
	if !ok {
		return fmt.Errorf("failed to get current file path for migrations")
	}
	migrationsPath := filepath.Clean(
		filepath.Join(filepath.Dir(currentFile), "../../../migrations"),
	)

	// Set environment variables using os.Setenv (not t.Setenv) since this is shared
	envVars := map[string]string{
		// Postgres
		"POSTGRES_HOST":     pgHost,
		"POSTGRES_PORT":     pgPort,
		"POSTGRES_USER":     pgUser,
		"POSTGRES_PASSWORD": pgPass,
		"POSTGRES_DB":       pgDB,
		"POSTGRES_SSLMODE":  "disable",

		// Migrations path (absolute)
		"MIGRATIONS_PATH": migrationsPath,

		// Redis
		"REDIS_HOST":     redisHost,
		"REDIS_PASSWORD": "",
		"REDIS_TLS":      "false",

		// RabbitMQ
		"RABBITMQ_HOST":                        sh.RabbitMQHost,
		"RABBITMQ_PORT":                        sh.RabbitMQPort,
		"RABBITMQ_USER":                        "guest",
		"RABBITMQ_PASSWORD":                    "guest",
		"RABBITMQ_VHOST":                       "/",
		"RABBITMQ_URI":                         "amqp",
		"RABBITMQ_HEALTH_URL":                  sh.RabbitMQHealthURL,
		"RABBITMQ_ALLOW_INSECURE_HEALTH_CHECK": "true",

		// Auth disabled for tests
		"AUTH_ENABLED": "false",

		// Body limit: 110 MiB / 115343360 bytes
		"HTTP_BODY_LIMIT_BYTES": "115343360",

		// Tenant defaults
		"DEFAULT_TENANT_ID":   sh.SharedTestHarness.Seed.TenantID.String(),
		"DEFAULT_TENANT_SLUG": "default",

		// Disable telemetry for tests
		"ENABLE_TELEMETRY": "false",

		// Use development environment
		"ENV_NAME": "development",

		// Logging
		"LOG_LEVEL": "debug",

		// Rate limiting (disabled for tests)
		"RATE_LIMIT_MAX":                 "10000",
		"RATE_LIMIT_EXPIRY_SEC":          "60",
		"EXPORT_RATE_LIMIT_MAX":          "1000",
		"EXPORT_RATE_LIMIT_EXPIRY_SEC":   "300",
		"DISPATCH_RATE_LIMIT_MAX":        "100",
		"DISPATCH_RATE_LIMIT_EXPIRY_SEC": "60",

		// Connection pool limits (moderate sizes for shared service)
		"POSTGRES_MAX_OPEN_CONNS": "15",
		"POSTGRES_MAX_IDLE_CONNS": "5",

		// Infrastructure connection timeout
		"INFRA_CONNECT_TIMEOUT_SEC": "30",

		// Disable background workers not needed in tests
		"ARCHIVAL_WORKER_ENABLED": "false",
	}

	for key, val := range envVars {
		if err := os.Setenv(key, val); err != nil {
			return fmt.Errorf("failed to set %s: %w", key, err)
		}
	}

	return nil
}

// Cleanup releases test-specific resources (not shared containers or service).
func (sh *SharedServerHarness) Cleanup() error {
	var errs []error

	// Close RabbitMQ spy (per-test resource)
	if sh.rabbitCh != nil {
		if err := sh.rabbitCh.Close(); err != nil {
			errs = append(errs, fmt.Errorf("rabbit channel: %w", err))
		}
	}
	if sh.rabbitConn != nil {
		if err := sh.rabbitConn.Close(); err != nil {
			errs = append(errs, fmt.Errorf("rabbit connection: %w", err))
		}
	}

	// NOTE: Do NOT shutdown Fiber app or close service - it's shared across tests

	// Cleanup database seed data tracking (containers stay up)
	if err := sh.SharedTestHarness.Cleanup(); err != nil {
		errs = append(errs, err)
	}

	if len(errs) > 0 {
		return fmt.Errorf("cleanup errors: %v", errs)
	}
	return nil
}

// RunWithSharedServer runs a test with the shared server harness.
func RunWithSharedServer(t *testing.T, testFn func(t *testing.T, sh *SharedServerHarness)) {
	ctx := context.Background()

	harness, err := NewSharedServerHarness(ctx, t)
	if err != nil {
		t.Fatalf("failed to create shared server harness: %v", err)
	}

	t.Cleanup(func() {
		if err := harness.Cleanup(); err != nil {
			t.Logf("failed to cleanup shared server harness: %v", err)
		}
	})

	testFn(t, harness)
}

// ToLegacyServerHarness converts SharedServerHarness to the legacy ServerHarness format
// for compatibility with existing test code that expects *ServerHarness.
func (sh *SharedServerHarness) ToLegacyServerHarness() *ServerHarness {
	return &ServerHarness{
		TestHarness:       sh.ToLegacyHarness(),
		serverHarnessBase: sh.serverHarnessBase,
		Service:           sh.Service,
	}
}

// flushRedis clears all keys from Redis to prevent idempotency key collisions between tests.
func flushRedis(ctx context.Context, redisAddr string) error {
	parsedURL, err := url.Parse(redisAddr)
	if err != nil {
		return fmt.Errorf("failed to parse redis address: %w", err)
	}

	client := redis.NewClient(&redis.Options{
		Addr: parsedURL.Host,
	})
	defer client.Close()

	return client.FlushAll(ctx).Err()
}
