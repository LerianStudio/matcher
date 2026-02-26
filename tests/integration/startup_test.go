//go:build integration

package integration

import (
	"context"
	"net/http"
	"net/url"
	"os"
	"path/filepath"
	"runtime"
	"testing"
	"time"

	"github.com/LerianStudio/matcher/internal/bootstrap"
	"github.com/stretchr/testify/require"
)

func TestServiceStartup_Integration(t *testing.T) {
	RunWithHarness(t, func(t *testing.T, h *TestHarness) {
		setProjectRoot(t)
		postgresHost, postgresPort := extractHostPort(t, h.PostgresDSN)
		redisAddr := extractRedisAddress(t, h.RedisAddr)

		_, currentFile, _, ok := runtime.Caller(0)
		require.True(t, ok, "failed to get current file path")
		migrationsPath := filepath.Clean(
			filepath.Join(filepath.Dir(currentFile), "../../migrations"),
		)

		t.Setenv("ENV_NAME", "test")
		t.Setenv("SERVER_ADDRESS", ":18080")
		t.Setenv("INFRA_CONNECT_TIMEOUT_SEC", "30")
		t.Setenv("DEFAULT_TENANT_ID", "11111111-1111-1111-1111-111111111111")
		t.Setenv("DEFAULT_TENANT_SLUG", "default")
		t.Setenv("POSTGRES_HOST", postgresHost)
		t.Setenv("POSTGRES_PORT", postgresPort)
		t.Setenv("POSTGRES_USER", "matcher")
		t.Setenv("POSTGRES_PASSWORD", "matcher_test")
		t.Setenv("POSTGRES_DB", "matcher_test")
		t.Setenv("POSTGRES_SSLMODE", "disable")
		t.Setenv("MIGRATIONS_PATH", migrationsPath)
		t.Setenv("REDIS_HOST", redisAddr)
		t.Setenv("RABBITMQ_URI", "amqp")
		t.Setenv("RABBITMQ_HOST", h.RabbitMQHost)
		t.Setenv("RABBITMQ_PORT", h.RabbitMQPort)
		t.Setenv("RABBITMQ_HEALTH_URL", h.RabbitMQHealthURL)
		t.Setenv("RABBITMQ_ALLOW_INSECURE_HEALTH_CHECK", "true")
		t.Setenv("RABBITMQ_USER", "guest")
		t.Setenv("RABBITMQ_PASSWORD", "guest")
		t.Setenv("RABBITMQ_VHOST", "/")
		t.Setenv("AUTH_ENABLED", "false")
		t.Setenv("ENABLE_TELEMETRY", "false")
		t.Setenv("LOG_LEVEL", "debug")
		t.Setenv("RATE_LIMIT_MAX", "1000")
		t.Setenv("RATE_LIMIT_EXPIRY_SEC", "60")
		t.Setenv("EXPORT_RATE_LIMIT_MAX", "10")
		t.Setenv("EXPORT_RATE_LIMIT_EXPIRY_SEC", "300")
		t.Setenv("DISPATCH_RATE_LIMIT_MAX", "100")
		t.Setenv("DISPATCH_RATE_LIMIT_EXPIRY_SEC", "60")
		t.Setenv("OBJECT_STORAGE_ENDPOINT", "")
		t.Setenv("ARCHIVAL_WORKER_ENABLED", "false")

		service, err := bootstrap.InitServersWithOptions(nil)
		require.NoError(t, err)
		require.NotNil(t, service)

		runErr := make(chan error, 1)

		t.Cleanup(func() {
			ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			defer cancel()

			if err := service.Shutdown(ctx); err != nil {
				t.Logf("failed to shutdown service: %v", err)
			}

			select {
			case err := <-runErr:
				if err != nil {
					t.Logf("server run error: %v", err)
				}
			case <-time.After(2 * time.Second):
				t.Log("server did not shutdown within timeout")
			}
		})

		go func() {
			runErr <- service.Server.Run(nil)
		}()

		client := &http.Client{Timeout: 2 * time.Second}

		require.Eventually(t, func() bool {
			select {
			case err := <-runErr:
				t.Fatalf("server run error: %v", err)
			default:
			}

			return hasStatus(client, "http://localhost:18080/health", http.StatusOK)
		}, 30*time.Second, 200*time.Millisecond)

		require.Eventually(t, func() bool {
			select {
			case err := <-runErr:
				t.Fatalf("server run error: %v", err)
			default:
			}

			return hasStatus(client, "http://localhost:18080/ready", http.StatusOK)
		}, 30*time.Second, 200*time.Millisecond)
	})
}

func setProjectRoot(t *testing.T) {
	t.Helper()

	cwd, err := os.Getwd()
	require.NoError(t, err)

	_, currentFile, _, ok := runtime.Caller(0)
	require.True(t, ok)

	root := filepath.Clean(filepath.Join(filepath.Dir(currentFile), "..", ".."))
	if cwd == root {
		return
	}

	require.NoError(t, os.Chdir(root))
	t.Cleanup(func() {
		if err := os.Chdir(cwd); err != nil {
			t.Logf("failed to restore working directory: %v", err)
		}
	})
}

func extractRedisAddress(t *testing.T, address string) string {
	t.Helper()

	parsed, err := url.Parse(address)
	if err != nil {
		t.Fatalf("failed to parse redis address %q: %v", address, err)
	}

	if parsed.Host != "" {
		return parsed.Host
	}

	return address
}

func extractHostPort(t *testing.T, dsn string) (string, string) {
	t.Helper()

	parsed, err := url.Parse(dsn)
	require.NoError(t, err)

	host := parsed.Hostname()
	port := parsed.Port()

	require.NotEmpty(t, host)

	if port == "" {
		port = "5432"
	}

	return host, port
}

func hasStatus(client *http.Client, target string, expected int) bool {
	resp, err := client.Get(target)
	if err != nil {
		return false
	}
	defer resp.Body.Close()

	return resp.StatusCode == expected
}

func TestServiceStartupAndShutdown_Integration(t *testing.T) {
	RunWithHarness(t, func(t *testing.T, h *TestHarness) {
		setProjectRoot(t)
		postgresHost, postgresPort := extractHostPort(t, h.PostgresDSN)
		redisAddr := extractRedisAddress(t, h.RedisAddr)

		_, currentFile, _, ok := runtime.Caller(0)
		require.True(t, ok, "failed to get current file path")
		migrationsPath := filepath.Clean(
			filepath.Join(filepath.Dir(currentFile), "../../migrations"),
		)

		t.Setenv("ENV_NAME", "test")
		t.Setenv("SERVER_ADDRESS", ":18081")
		t.Setenv("INFRA_CONNECT_TIMEOUT_SEC", "30")
		t.Setenv("DEFAULT_TENANT_ID", "11111111-1111-1111-1111-111111111111")
		t.Setenv("DEFAULT_TENANT_SLUG", "default")
		t.Setenv("POSTGRES_HOST", postgresHost)
		t.Setenv("POSTGRES_PORT", postgresPort)
		t.Setenv("POSTGRES_USER", "matcher")
		t.Setenv("POSTGRES_PASSWORD", "matcher_test")
		t.Setenv("POSTGRES_DB", "matcher_test")
		t.Setenv("POSTGRES_SSLMODE", "disable")
		t.Setenv("MIGRATIONS_PATH", migrationsPath)
		t.Setenv("REDIS_HOST", redisAddr)
		t.Setenv("RABBITMQ_URI", "amqp")
		t.Setenv("RABBITMQ_HOST", h.RabbitMQHost)
		t.Setenv("RABBITMQ_PORT", h.RabbitMQPort)
		t.Setenv("RABBITMQ_HEALTH_URL", h.RabbitMQHealthURL)
		t.Setenv("RABBITMQ_ALLOW_INSECURE_HEALTH_CHECK", "true")
		t.Setenv("RABBITMQ_USER", "guest")
		t.Setenv("RABBITMQ_PASSWORD", "guest")
		t.Setenv("RABBITMQ_VHOST", "/")
		t.Setenv("AUTH_ENABLED", "false")
		t.Setenv("ENABLE_TELEMETRY", "false")
		t.Setenv("LOG_LEVEL", "debug")
		t.Setenv("RATE_LIMIT_MAX", "1000")
		t.Setenv("RATE_LIMIT_EXPIRY_SEC", "60")
		t.Setenv("EXPORT_RATE_LIMIT_MAX", "10")
		t.Setenv("EXPORT_RATE_LIMIT_EXPIRY_SEC", "300")
		t.Setenv("DISPATCH_RATE_LIMIT_MAX", "100")
		t.Setenv("DISPATCH_RATE_LIMIT_EXPIRY_SEC", "60")
		t.Setenv("OBJECT_STORAGE_ENDPOINT", "")
		t.Setenv("ARCHIVAL_WORKER_ENABLED", "false")

		service, err := bootstrap.InitServersWithOptions(nil)
		require.NoError(t, err)
		require.NotNil(t, service)

		runErr := make(chan error, 1)

		go func() {
			runErr <- service.Server.Run(nil)
		}()

		client := &http.Client{Timeout: 2 * time.Second}

		require.Eventually(t, func() bool {
			return hasStatus(client, "http://localhost:18081/health", http.StatusOK)
		}, 30*time.Second, 200*time.Millisecond)

		require.Eventually(t, func() bool {
			return hasStatus(client, "http://localhost:18081/ready", http.StatusOK)
		}, 30*time.Second, 200*time.Millisecond)

		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()

		shutdownErr := service.Shutdown(ctx)
		require.NoError(t, shutdownErr, "shutdown should complete without error")

		select {
		case err := <-runErr:
			require.NoError(t, err, "server should exit cleanly")
		case <-time.After(5 * time.Second):
			t.Fatal("server did not stop within timeout after shutdown")
		}

		_, err = client.Get("http://localhost:18081/health")
		require.Error(t, err, "server should not accept connections after shutdown")
	})
}
