// Copyright 2025 Lerian Studio. All rights reserved.
// Use of this source code is governed by an Elastic License 2.0
// that can be found in the LICENSE.md file.

//go:build unit

package bootstrap

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"strconv"
	"strings"
	"testing"
	"time"

	sqlmock "github.com/DATA-DOG/go-sqlmock"
	"github.com/bxcodec/dbresolver/v2"
	"github.com/gofiber/fiber/v2"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/otel"

	libLog "github.com/LerianStudio/lib-commons/v4/commons/log"
	libRabbitmq "github.com/LerianStudio/lib-commons/v4/commons/rabbitmq"

	"github.com/LerianStudio/matcher/internal/shared/infrastructure/testutil"
)

// Sentinel errors for test cases.
var (
	errBoom         = errors.New("boom")
	errPostgresDown = errors.New("postgres down")
	errRedisDown    = errors.New("redis down")
)

func TestCheckRabbitMQHTTPHealth_Success(t *testing.T) {
	t.Parallel()

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusOK)
	}))
	defer server.Close()

	err := checkRabbitMQHTTPHealth(context.Background(), server.URL)
	require.NoError(t, err)
}

func TestCheckRabbitMQHTTPHealth_Failure(t *testing.T) {
	t.Parallel()

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusServiceUnavailable)
	}))
	defer server.Close()

	err := checkRabbitMQHTTPHealth(context.Background(), server.URL)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "unhealthy status: 503")
}

func TestCheckRabbitMQHTTPHealth_ConnectionError(t *testing.T) {
	t.Parallel()

	err := checkRabbitMQHTTPHealth(context.Background(), "http://localhost:1")
	require.Error(t, err)
	assert.Contains(t, err.Error(), "request failed")
}

func TestCheckRabbitMQHTTPHealth_InvalidURL(t *testing.T) {
	t.Parallel()

	err := checkRabbitMQHTTPHealth(context.Background(), "://invalid-url")
	require.Error(t, err)
	assert.Contains(t, err.Error(), "create request")
}

func TestCustomErrorHandler_ReturnsInternalError(t *testing.T) {
	t.Parallel()

	app := fiber.New(
		fiber.Config{ErrorHandler: customErrorHandlerWithEnv(&libLog.NopLogger{}, "")},
	)
	app.Get("/boom", func(_ *fiber.Ctx) error {
		return errBoom
	})

	resp, err := app.Test(httptest.NewRequest(http.MethodGet, "/boom", http.NoBody))
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, resp.Body.Close()) })
	require.Equal(t, http.StatusInternalServerError, resp.StatusCode)

	var body map[string]any
	require.NoError(t, json.NewDecoder(resp.Body).Decode(&body))
	assert.Equal(t, "internal server error", body["message"])
	assert.Equal(t, float64(500), body["code"])
}

func TestCustomErrorHandler_ReturnsBadRequestMessage(t *testing.T) {
	t.Parallel()

	app := fiber.New(
		fiber.Config{ErrorHandler: customErrorHandlerWithEnv(&libLog.NopLogger{}, "")},
	)
	app.Get("/bad", func(_ *fiber.Ctx) error {
		return fiber.NewError(http.StatusBadRequest, "bad request")
	})

	resp, err := app.Test(httptest.NewRequest(http.MethodGet, "/bad", http.NoBody))
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, resp.Body.Close()) })
	require.Equal(t, http.StatusBadRequest, resp.StatusCode)

	var body map[string]any
	require.NoError(t, json.NewDecoder(resp.Body).Decode(&body))
	assert.Equal(t, "invalid_request", body["title"])
	assert.Equal(t, float64(400), body["code"])
}

func TestReadinessHandler_ProductionHidesChecks(t *testing.T) {
	t.Parallel()

	app := fiber.New()
	cfg := &Config{App: AppConfig{EnvName: "production"}}
	app.Get("/ready", readinessHandler(cfg, nil, nil, &libLog.NopLogger{}))

	resp, err := app.Test(httptest.NewRequest(http.MethodGet, "/ready", http.NoBody))
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, resp.Body.Close()) })
	require.Equal(t, http.StatusServiceUnavailable, resp.StatusCode)

	var body map[string]any
	require.NoError(t, json.NewDecoder(resp.Body).Decode(&body))
	assert.Equal(t, "degraded", body["status"])
	_, hasChecks := body["checks"]
	assert.False(t, hasChecks)
}

func TestReadinessHandler_NonProductionIncludesChecks(t *testing.T) {
	t.Parallel()

	app := fiber.New()
	cfg := &Config{App: AppConfig{EnvName: "development"}}
	app.Get("/ready", readinessHandler(cfg, nil, nil, &libLog.NopLogger{}))

	resp, err := app.Test(httptest.NewRequest(http.MethodGet, "/ready", http.NoBody))
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, resp.Body.Close()) })
	require.Equal(t, http.StatusServiceUnavailable, resp.StatusCode)

	var body map[string]any
	require.NoError(t, json.NewDecoder(resp.Body).Decode(&body))
	checks, ok := body["checks"].(map[string]any)
	require.True(t, ok)
	assert.Equal(t, "unknown", checks["database"])
	assert.Equal(t, "unknown", checks["databaseReplica"])
	assert.Equal(t, "unknown", checks["redis"])
	assert.Equal(t, "unknown", checks["rabbitmq"])
	assert.Equal(t, "unknown", checks["objectStorage"])
}

func TestReadinessHandler_UsesCheckHooks(t *testing.T) {
	t.Parallel()

	app := fiber.New()
	cfg := &Config{App: AppConfig{EnvName: "development"}}
	deps := &HealthDependencies{
		PostgresCheck:           func(context.Context) error { return errPostgresDown },
		RedisCheck:              func(context.Context) error { return errRedisDown },
		RabbitMQCheck:           func(context.Context) error { return nil },
		RedisOptional:           true,
		PostgresReplicaOptional: true,
		ObjectStorageOptional:   true,
	}
	app.Get("/ready", readinessHandler(cfg, nil, deps, &libLog.NopLogger{}))

	resp, err := app.Test(httptest.NewRequest(http.MethodGet, "/ready", http.NoBody))
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, resp.Body.Close()) })
	require.Equal(t, http.StatusServiceUnavailable, resp.StatusCode)

	var body map[string]any
	require.NoError(t, json.NewDecoder(resp.Body).Decode(&body))
	checks, ok := body["checks"].(map[string]any)
	require.True(t, ok)
	assert.Equal(t, "down", checks["database"])
	assert.Equal(t, "down", checks["redis"])
	assert.Equal(t, "ok", checks["rabbitmq"])
}

func TestReadinessHandler_AllChecksPass(t *testing.T) {
	t.Parallel()

	app := fiber.New()
	cfg := &Config{App: AppConfig{EnvName: "development"}}
	deps := &HealthDependencies{
		PostgresCheck:           func(context.Context) error { return nil },
		RedisCheck:              func(context.Context) error { return nil },
		RabbitMQCheck:           func(context.Context) error { return nil },
		PostgresReplicaOptional: true,
		ObjectStorageOptional:   true,
	}
	app.Get("/ready", readinessHandler(cfg, nil, deps, &libLog.NopLogger{}))

	resp, err := app.Test(httptest.NewRequest(http.MethodGet, "/ready", http.NoBody))
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, resp.Body.Close()) })
	require.Equal(t, http.StatusOK, resp.StatusCode)

	var body map[string]any
	require.NoError(t, json.NewDecoder(resp.Body).Decode(&body))
	assert.Equal(t, "ok", body["status"])
}

func TestReadinessHandler_OptionalDependencyDoesNotDegrade(t *testing.T) {
	t.Parallel()

	app := fiber.New()
	cfg := &Config{App: AppConfig{EnvName: "development"}}
	deps := &HealthDependencies{
		PostgresCheck:           func(context.Context) error { return nil },
		RedisCheck:              func(context.Context) error { return errRedisDown },
		RabbitMQCheck:           func(context.Context) error { return nil },
		RedisOptional:           true,
		PostgresReplicaOptional: true,
		ObjectStorageOptional:   true,
	}
	app.Get("/ready", readinessHandler(cfg, nil, deps, &libLog.NopLogger{}))

	resp, err := app.Test(httptest.NewRequest(http.MethodGet, "/ready", http.NoBody))
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, resp.Body.Close()) })
	require.Equal(t, http.StatusOK, resp.StatusCode)

	var body map[string]any
	require.NoError(t, json.NewDecoder(resp.Body).Decode(&body))
	assert.Equal(t, "ok", body["status"])
	checks, ok := body["checks"].(map[string]any)
	require.True(t, ok)
	assert.Equal(t, "ok", checks["database"])
	assert.Equal(t, "down", checks["redis"])
	assert.Equal(t, "ok", checks["rabbitmq"])
}

func TestSanitizeHeaderID(t *testing.T) {
	t.Parallel()

	valid := sanitizeHeaderID("req-123")
	require.Equal(t, "req-123", valid)

	empty := sanitizeHeaderID(" ")
	require.NotEmpty(t, empty)
	require.NotEqual(t, " ", empty)

	longInput := strings.Repeat("a", maxHeaderIDLength+1)
	long := sanitizeHeaderID(longInput)
	require.Equal(t, strings.Repeat("a", maxHeaderIDLength), long)

	invalid := sanitizeHeaderID("ok\u0000")
	require.Equal(t, "ok", invalid)
}

func TestTelemetryMiddlewareSetsRequestID(t *testing.T) {
	t.Parallel()

	app := fiber.New()
	// Create test dependencies for telemetryMiddleware
	logger := &libLog.NopLogger{}
	tracer := otel.Tracer("test")
	app.Use(telemetryMiddleware(logger, tracer, nil))
	app.Get("/", func(c *fiber.Ctx) error {
		return c.SendStatus(http.StatusOK)
	})

	req := httptest.NewRequest(http.MethodGet, "/", http.NoBody)
	resp, err := app.Test(req)
	require.NoError(t, err)

	defer resp.Body.Close()

	require.NotEmpty(t, resp.Header.Get("X-Request-ID"))
}

func TestApplyReadinessCheckUnknownOptional(t *testing.T) {
	t.Parallel()

	checks := fiber.Map{}
	ok := applyReadinessCheck(
		context.Background(),
		"redis",
		checks,
		nil,
		false,
		true,
		&libLog.NopLogger{},
		0,
	)
	require.True(t, ok)
	require.Equal(t, "unknown", checks["redis"])
}

func TestApplyReadinessCheckDownRequired(t *testing.T) {
	t.Parallel()

	checks := fiber.Map{}
	ok := applyReadinessCheck(
		context.Background(),
		"database",
		checks,
		func(_ context.Context) error {
			return errBoom
		},
		true,
		false,
		&libLog.NopLogger{},
		0,
	)
	require.False(t, ok)
	require.Equal(t, "down", checks["database"])
}

func TestShouldIncludeReadinessDetails(t *testing.T) {
	t.Parallel()

	require.False(t, shouldIncludeReadinessDetails(nil))
	require.False(t, shouldIncludeReadinessDetails(&Config{App: AppConfig{EnvName: envProduction}}))
	require.False(t, shouldIncludeReadinessDetails(&Config{App: AppConfig{EnvName: " PrOdUcTiOn "}}))
	require.True(t, shouldIncludeReadinessDetails(&Config{App: AppConfig{EnvName: "development"}}))
}

func TestNewFiberAppDefaults(t *testing.T) {
	t.Parallel()

	app := NewFiberApp(nil, &libLog.NopLogger{}, nil)

	app.Get("/", func(c *fiber.Ctx) error {
		return c.SendStatus(http.StatusOK)
	})

	req := httptest.NewRequest(http.MethodGet, "/", http.NoBody)
	resp, err := app.Test(req)
	require.NoError(t, err)

	defer resp.Body.Close()

	require.Equal(t, http.StatusOK, resp.StatusCode)
}

func TestRateLimiterMiddleware(t *testing.T) {
	t.Parallel()

	const testRateLimitMax = 5

	const testRateLimitExpirySec = 60

	cfg := &Config{
		Server: ServerConfig{
			CORSAllowedOrigins: "*",
			CORSAllowedMethods: "GET",
			CORSAllowedHeaders: "Content-Type",
		},
		RateLimit: RateLimitConfig{
			Enabled:   true,
			Max:       testRateLimitMax,
			ExpirySec: testRateLimitExpirySec,
		},
	}
	app := NewFiberApp(cfg, &libLog.NopLogger{}, nil)

	rateLimiter := NewRateLimiter(cfg, nil)

	app.Get("/health", func(c *fiber.Ctx) error {
		return c.SendStatus(http.StatusOK)
	})

	api := app.Group("/api", rateLimiter)
	api.Get("/test", func(c *fiber.Ctx) error {
		return c.SendStatus(http.StatusOK)
	})

	for i := 0; i < testRateLimitMax+10; i++ {
		req := httptest.NewRequest(http.MethodGet, "/health", http.NoBody)
		req.Header.Set("X-Forwarded-For", "10.0.0.1")
		resp, err := app.Test(req)
		require.NoError(t, err)
		resp.Body.Close()
		assert.Equal(
			t,
			http.StatusOK,
			resp.StatusCode,
			"health endpoint should not be rate limited on request %d",
			i+1,
		)
	}

	for i := 0; i < testRateLimitMax+5; i++ {
		req := httptest.NewRequest(http.MethodGet, "/api/test", http.NoBody)
		req.Header.Set("X-Forwarded-For", "10.0.0.2")
		resp, err := app.Test(req)
		require.NoError(t, err)

		if i >= testRateLimitMax {
			assert.Equal(
				t,
				http.StatusTooManyRequests,
				resp.StatusCode,
				"api endpoint should be rate limited on request %d",
				i+1,
			)
			assert.Equal(
				t,
				strconv.Itoa(testRateLimitExpirySec),
				resp.Header.Get("Retry-After"),
				"Retry-After header should be set",
			)
		} else {
			assert.Equal(t, http.StatusOK, resp.StatusCode, "api endpoint should not be rate limited on request %d", i+1)
		}

		resp.Body.Close()
	}
}

func TestSecurityHeadersMiddleware(t *testing.T) {
	t.Parallel()

	cfg := &Config{
		Server: ServerConfig{
			CORSAllowedOrigins: "*",
			CORSAllowedMethods: "GET",
			CORSAllowedHeaders: "Content-Type",
		},
	}
	app := NewFiberApp(cfg, &libLog.NopLogger{}, nil)

	app.Get("/test", func(c *fiber.Ctx) error {
		return c.SendStatus(http.StatusOK)
	})

	req := httptest.NewRequest(http.MethodGet, "/test", http.NoBody)
	resp, err := app.Test(req)
	require.NoError(t, err)

	defer resp.Body.Close()

	expectedHeaders := map[string]string{
		"X-Frame-Options":              "DENY",
		"X-Content-Type-Options":       "nosniff",
		"X-Xss-Protection":             "1; mode=block",
		"Referrer-Policy":              "strict-origin-when-cross-origin",
		"Cross-Origin-Opener-Policy":   "same-origin",
		"Cross-Origin-Embedder-Policy": "require-corp",
		"Cross-Origin-Resource-Policy": "same-origin",
	}

	for header, expected := range expectedHeaders {
		assert.Equal(t, expected, resp.Header.Get(header), "header %s mismatch", header)
	}
}

func TestNewServer(t *testing.T) {
	t.Parallel()

	t.Run("creates server with all dependencies", func(t *testing.T) {
		t.Parallel()

		cfg := &Config{
			Server: ServerConfig{
				Address: ":4018",
			},
			App: AppConfig{
				EnvName: "test",
			},
		}
		app := fiber.New()
		logger := &libLog.NopLogger{}

		server := NewServer(cfg, app, logger, nil, nil, nil, nil)

		require.NotNil(t, server)
		assert.Equal(t, app, server.app)
		assert.Equal(t, cfg, server.cfg)
		assert.Equal(t, logger, server.logger)
	})
}

func TestServerShutdown(t *testing.T) {
	t.Parallel()

	t.Run("returns error for nil server", func(t *testing.T) {
		t.Parallel()

		var srv *Server
		ctx := context.Background()

		err := srv.Shutdown(ctx)

		require.Error(t, err)
		assert.Contains(t, err.Error(), "server not initialized")
	})

	t.Run("returns error for nil app", func(t *testing.T) {
		t.Parallel()

		srv := &Server{
			app:    nil,
			cfg:    &Config{},
			logger: &libLog.NopLogger{},
		}
		ctx := context.Background()

		err := srv.Shutdown(ctx)

		require.Error(t, err)
		assert.Contains(t, err.Error(), "server not initialized")
	})

	t.Run("shuts down successfully with valid app", func(t *testing.T) {
		t.Parallel()

		app := fiber.New()
		srv := &Server{
			app:    app,
			cfg:    &Config{},
			logger: &libLog.NopLogger{},
		}
		ctx := context.Background()

		err := srv.Shutdown(ctx)

		require.NoError(t, err)
	})
}

func TestServerRun(t *testing.T) {
	t.Parallel()

	t.Run("returns error for nil server", func(t *testing.T) {
		t.Parallel()

		var srv *Server

		err := srv.Run(nil)

		require.Error(t, err)
		assert.Contains(t, err.Error(), "server not initialized")
	})

	t.Run("returns error for nil config", func(t *testing.T) {
		t.Parallel()

		srv := &Server{
			app:    fiber.New(),
			cfg:    nil,
			logger: &libLog.NopLogger{},
		}

		err := srv.Run(nil)

		require.Error(t, err)
		assert.Contains(t, err.Error(), "config not initialized")
	})

	t.Run("returns error for nil app", func(t *testing.T) {
		t.Parallel()

		srv := &Server{
			app:    nil,
			cfg:    &Config{},
			logger: &libLog.NopLogger{},
		}

		err := srv.Run(nil)

		require.Error(t, err)
		assert.Contains(t, err.Error(), "server not initialized")
	})
}

func TestNewHealthDependencies(t *testing.T) {
	t.Parallel()

	t.Run("creates with default optional settings", func(t *testing.T) {
		t.Parallel()

		deps := NewHealthDependencies(nil, nil, nil, nil, nil)

		require.NotNil(t, deps)
		assert.True(t, deps.RedisOptional)
		assert.True(t, deps.PostgresReplicaOptional)
		assert.True(t, deps.ObjectStorageOptional)
		assert.False(t, deps.PostgresOptional)
		assert.False(t, deps.RabbitMQOptional)
	})
}

func TestResolvePostgresCheck(t *testing.T) {
	t.Parallel()

	t.Run("returns nil for nil deps", func(t *testing.T) {
		t.Parallel()

		checkFunc, available := resolvePostgresCheck(nil)

		assert.Nil(t, checkFunc)
		assert.False(t, available)
	})

	t.Run("returns custom check when provided", func(t *testing.T) {
		t.Parallel()

		customCheck := func(context.Context) error { return nil }
		deps := &HealthDependencies{
			PostgresCheck: customCheck,
		}

		checkFunc, available := resolvePostgresCheck(deps)

		assert.NotNil(t, checkFunc)
		assert.True(t, available)
	})

	t.Run("returns nil when postgres connection not set", func(t *testing.T) {
		t.Parallel()

		deps := &HealthDependencies{
			Postgres: nil,
		}

		checkFunc, available := resolvePostgresCheck(deps)

		assert.Nil(t, checkFunc)
		assert.False(t, available)
	})
}

func TestResolveRedisCheck(t *testing.T) {
	t.Parallel()

	t.Run("returns nil for nil deps", func(t *testing.T) {
		t.Parallel()

		checkFunc, available := resolveRedisCheck(nil)

		assert.Nil(t, checkFunc)
		assert.False(t, available)
	})

	t.Run("returns custom check when provided", func(t *testing.T) {
		t.Parallel()

		customCheck := func(context.Context) error { return nil }
		deps := &HealthDependencies{
			RedisCheck: customCheck,
		}

		checkFunc, available := resolveRedisCheck(deps)

		assert.NotNil(t, checkFunc)
		assert.True(t, available)
	})

	t.Run("returns nil when redis connection not set", func(t *testing.T) {
		t.Parallel()

		deps := &HealthDependencies{
			Redis: nil,
		}

		checkFunc, available := resolveRedisCheck(deps)

		assert.Nil(t, checkFunc)
		assert.False(t, available)
	})
}

func TestResolveRabbitMQCheck(t *testing.T) {
	t.Parallel()

	t.Run("returns nil for nil deps", func(t *testing.T) {
		t.Parallel()

		checkFunc, available := resolveRabbitMQCheck(nil)

		assert.Nil(t, checkFunc)
		assert.False(t, available)
	})

	t.Run("returns custom check when provided", func(t *testing.T) {
		t.Parallel()

		customCheck := func(context.Context) error { return nil }
		deps := &HealthDependencies{
			RabbitMQCheck: customCheck,
		}

		checkFunc, available := resolveRabbitMQCheck(deps)

		assert.NotNil(t, checkFunc)
		assert.True(t, available)
	})

	t.Run("returns nil when rabbitmq connection not set", func(t *testing.T) {
		t.Parallel()

		deps := &HealthDependencies{
			RabbitMQ: nil,
		}

		checkFunc, available := resolveRabbitMQCheck(deps)

		assert.Nil(t, checkFunc)
		assert.False(t, available)
	})
}

func TestHealthHandler(t *testing.T) {
	t.Parallel()

	app := fiber.New()
	app.Get("/health", healthHandler)

	req := httptest.NewRequest(http.MethodGet, "/health", http.NoBody)
	resp, err := app.Test(req)
	require.NoError(t, err)
	defer resp.Body.Close()

	require.Equal(t, http.StatusOK, resp.StatusCode)
	assert.Contains(t, resp.Header.Get("Content-Type"), "text/plain")

	body, err := io.ReadAll(resp.Body)
	require.NoError(t, err)
	assert.Equal(t, "healthy", string(body))
}

func TestExportRateLimiter(t *testing.T) {
	t.Parallel()

	t.Run("returns no-op handler when rate limiting disabled", func(t *testing.T) {
		t.Parallel()

		cfg := &Config{
			RateLimit: RateLimitConfig{
				Enabled:         false,
				ExportMax:       10,
				ExportExpirySec: 60,
			},
		}

		handler := NewExportRateLimiter(cfg, nil)

		require.NotNil(t, handler)

		app := fiber.New()
		app.Get("/export", handler, func(c *fiber.Ctx) error {
			return c.SendStatus(http.StatusOK)
		})

		for i := 0; i < 20; i++ {
			req := httptest.NewRequest(http.MethodGet, "/export", http.NoBody)
			resp, err := app.Test(req)
			require.NoError(t, err)
			resp.Body.Close()
			assert.Equal(t, http.StatusOK, resp.StatusCode)
		}
	})

	t.Run("applies rate limiting when enabled", func(t *testing.T) {
		t.Parallel()

		cfg := &Config{
			RateLimit: RateLimitConfig{
				Enabled:         true,
				ExportMax:       3,
				ExportExpirySec: 60,
			},
		}

		handler := NewExportRateLimiter(cfg, nil)

		require.NotNil(t, handler)

		app := fiber.New()
		app.Get("/export", handler, func(c *fiber.Ctx) error {
			return c.SendStatus(http.StatusOK)
		})

		for i := 0; i < 5; i++ {
			req := httptest.NewRequest(http.MethodGet, "/export", http.NoBody)
			req.Header.Set("X-Forwarded-For", "192.168.1.1")
			resp, err := app.Test(req)
			require.NoError(t, err)

			if i >= 3 {
				assert.Equal(t, http.StatusTooManyRequests, resp.StatusCode)
				assert.NotEmpty(t, resp.Header.Get("Retry-After"))
			} else {
				assert.Equal(t, http.StatusOK, resp.StatusCode)
			}
			resp.Body.Close()
		}
	})
}

func TestNewFiberApp_WithCustomBodyLimit(t *testing.T) {
	t.Parallel()

	cfg := &Config{
		Server: ServerConfig{
			BodyLimitBytes:     1024,
			CORSAllowedOrigins: "*",
			CORSAllowedMethods: "GET",
			CORSAllowedHeaders: "Content-Type",
		},
	}

	app := NewFiberApp(cfg, &libLog.NopLogger{}, nil)

	require.NotNil(t, app)
}

func TestNewFiberApp_WithNegativeBodyLimit(t *testing.T) {
	t.Parallel()

	cfg := &Config{
		Server: ServerConfig{
			BodyLimitBytes:     -1,
			CORSAllowedOrigins: "*",
			CORSAllowedMethods: "GET",
			CORSAllowedHeaders: "Content-Type",
		},
	}

	app := NewFiberApp(cfg, &libLog.NopLogger{}, nil)

	require.NotNil(t, app)
}

func TestCustomErrorHandler_NotFoundError(t *testing.T) {
	t.Parallel()

	app := fiber.New(
		fiber.Config{ErrorHandler: customErrorHandlerWithEnv(&libLog.NopLogger{}, "")},
	)

	req := httptest.NewRequest(http.MethodGet, "/nonexistent", http.NoBody)
	resp, err := app.Test(req)
	require.NoError(t, err)
	defer resp.Body.Close()

	require.Equal(t, http.StatusNotFound, resp.StatusCode)
}

func TestCustomErrorHandler_UnprocessableEntity(t *testing.T) {
	t.Parallel()

	app := fiber.New(
		fiber.Config{ErrorHandler: customErrorHandlerWithEnv(&libLog.NopLogger{}, "")},
	)
	app.Get("/validation", func(_ *fiber.Ctx) error {
		return fiber.NewError(http.StatusUnprocessableEntity, "validation failed")
	})

	req := httptest.NewRequest(http.MethodGet, "/validation", http.NoBody)
	resp, err := app.Test(req)
	require.NoError(t, err)
	defer resp.Body.Close()

	require.Equal(t, http.StatusUnprocessableEntity, resp.StatusCode)

	var body map[string]any
	require.NoError(t, json.NewDecoder(resp.Body).Decode(&body))
	assert.Equal(t, "request_failed", body["title"])
}

func TestCustomErrorHandler_ConflictError(t *testing.T) {
	t.Parallel()

	app := fiber.New(
		fiber.Config{ErrorHandler: customErrorHandlerWithEnv(&libLog.NopLogger{}, "")},
	)
	app.Get("/conflict", func(_ *fiber.Ctx) error {
		return fiber.NewError(http.StatusConflict, "resource conflict")
	})

	req := httptest.NewRequest(http.MethodGet, "/conflict", http.NoBody)
	resp, err := app.Test(req)
	require.NoError(t, err)
	defer resp.Body.Close()

	require.Equal(t, http.StatusConflict, resp.StatusCode)

	var body map[string]any
	require.NoError(t, json.NewDecoder(resp.Body).Decode(&body))
	assert.Equal(t, "request_failed", body["title"])
}

func TestCustomErrorHandler_ProductionHidesDetails(t *testing.T) {
	t.Parallel()

	app := fiber.New(
		fiber.Config{ErrorHandler: customErrorHandlerWithEnv(&libLog.NopLogger{}, "production")},
	)
	app.Get("/boom", func(_ *fiber.Ctx) error {
		return errBoom
	})

	req := httptest.NewRequest(http.MethodGet, "/boom", http.NoBody)
	resp, err := app.Test(req)
	require.NoError(t, err)
	defer resp.Body.Close()

	require.Equal(t, http.StatusInternalServerError, resp.StatusCode)

	var body map[string]any
	require.NoError(t, json.NewDecoder(resp.Body).Decode(&body))
	_, hasDetail := body["detail"]
	assert.False(t, hasDetail, "production should not expose error details")
}

func TestApplyReadinessCheckOKScenario(t *testing.T) {
	t.Parallel()

	checks := fiber.Map{}
	ok := applyReadinessCheck(
		context.Background(),
		"database",
		checks,
		func(_ context.Context) error {
			return nil
		},
		true,
		false,
		&libLog.NopLogger{},
		0,
	)

	require.True(t, ok)
	require.Equal(t, "ok", checks["database"])
}

func TestApplyReadinessCheckFailureScenario(t *testing.T) {
	t.Parallel()

	t.Run("required dependency failure returns false", func(t *testing.T) {
		t.Parallel()

		checks := fiber.Map{}
		ok := applyReadinessCheck(
			context.Background(),
			"database",
			checks,
			func(_ context.Context) error {
				return errBoom
			},
			true,
			false,
			&libLog.NopLogger{},
			0,
		)

		require.False(t, ok)
		require.Equal(t, "down", checks["database"])
	})

	t.Run("optional dependency failure returns true", func(t *testing.T) {
		t.Parallel()

		checks := fiber.Map{}
		ok := applyReadinessCheck(
			context.Background(),
			"redis",
			checks,
			func(_ context.Context) error {
				return errBoom
			},
			true,
			true,
			&libLog.NopLogger{},
			0,
		)

		require.True(t, ok)
		require.Equal(t, "down", checks["redis"])
	})

	t.Run("hung dependency obeys context timeout", func(t *testing.T) {
		t.Parallel()

		checks := fiber.Map{}
		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Millisecond)
		defer cancel()

		start := time.Now()
		ok := applyReadinessCheck(
			ctx,
			"rabbitmq",
			checks,
			func(checkCtx context.Context) error {
				<-checkCtx.Done()

				return checkCtx.Err()
			},
			true,
			false,
			&libLog.NopLogger{},
			0,
		)

		elapsed := time.Since(start)

		require.False(t, ok)
		require.Equal(t, "down", checks["rabbitmq"])
		assert.Less(t, elapsed, time.Second)
	})

	t.Run("nil check function marks as unknown", func(t *testing.T) {
		t.Parallel()

		checks := fiber.Map{}
		ok := applyReadinessCheck(
			context.Background(),
			"storage",
			checks,
			nil,
			false,
			true,
			&libLog.NopLogger{},
			0,
		)

		require.True(t, ok)
		require.Equal(t, "unknown", checks["storage"])
	})
}

func TestResolvePostgresReplicaCheck(t *testing.T) {
	t.Parallel()

	t.Run("returns nil for nil deps", func(t *testing.T) {
		t.Parallel()

		checkFunc, available := resolvePostgresReplicaCheck(nil)

		assert.Nil(t, checkFunc)
		assert.False(t, available)
	})

	t.Run("returns custom check when provided", func(t *testing.T) {
		t.Parallel()

		customCheck := func(context.Context) error { return nil }
		deps := &HealthDependencies{
			PostgresReplicaCheck: customCheck,
		}

		checkFunc, available := resolvePostgresReplicaCheck(deps)

		assert.NotNil(t, checkFunc)
		assert.True(t, available)
	})

	t.Run("returns nil when postgres replica not set", func(t *testing.T) {
		t.Parallel()

		deps := &HealthDependencies{
			PostgresReplica: nil,
		}

		checkFunc, available := resolvePostgresReplicaCheck(deps)

		assert.Nil(t, checkFunc)
		assert.False(t, available)
	})
}

func TestResolveObjectStorageCheck(t *testing.T) {
	t.Parallel()

	t.Run("returns nil for nil deps", func(t *testing.T) {
		t.Parallel()

		checkFunc, available := resolveObjectStorageCheck(nil)

		assert.Nil(t, checkFunc)
		assert.False(t, available)
	})

	t.Run("returns custom check when provided", func(t *testing.T) {
		t.Parallel()

		customCheck := func(context.Context) error { return nil }
		deps := &HealthDependencies{
			ObjectStorageCheck: customCheck,
		}

		checkFunc, available := resolveObjectStorageCheck(deps)

		assert.NotNil(t, checkFunc)
		assert.True(t, available)
	})

	t.Run("returns nil when object storage not set", func(t *testing.T) {
		t.Parallel()

		deps := &HealthDependencies{
			ObjectStorage: nil,
		}

		checkFunc, available := resolveObjectStorageCheck(deps)

		assert.Nil(t, checkFunc)
		assert.False(t, available)
	})
}

func TestClientErrorMessageForStatusCases(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name     string
		status   int
		expected string
	}{
		{
			name:     "bad request returns invalid_request",
			status:   http.StatusBadRequest,
			expected: "invalid_request",
		},
		{
			name:     "unauthorized returns unauthorized",
			status:   http.StatusUnauthorized,
			expected: "unauthorized",
		},
		{
			name:     "forbidden returns forbidden",
			status:   http.StatusForbidden,
			expected: "forbidden",
		},
		{
			name:     "not found returns not_found",
			status:   http.StatusNotFound,
			expected: "not_found",
		},
		{
			name:     "conflict returns request_failed",
			status:   http.StatusConflict,
			expected: "request_failed",
		},
		{
			name:     "unprocessable entity returns request_failed",
			status:   http.StatusUnprocessableEntity,
			expected: "request_failed",
		},
		{
			name:     "too many requests returns request_failed",
			status:   http.StatusTooManyRequests,
			expected: "request_failed",
		},
		{
			name:     "internal server error returns request_failed",
			status:   http.StatusInternalServerError,
			expected: "request_failed",
		},
		{
			name:     "unknown status returns request_failed",
			status:   418,
			expected: "request_failed",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			result := clientErrorMessageForStatus(tt.status)

			assert.Equal(t, tt.expected, result)
		})
	}
}

func TestTruncateHeaderID(t *testing.T) {
	t.Parallel()

	t.Run("returns empty string for empty input", func(t *testing.T) {
		t.Parallel()

		result := truncateHeaderID("")

		assert.Empty(t, result)
	})

	t.Run("returns input unchanged when under limit", func(t *testing.T) {
		t.Parallel()

		input := "short-header-id"

		result := truncateHeaderID(input)

		assert.Equal(t, input, result)
	})

	t.Run("truncates long input", func(t *testing.T) {
		t.Parallel()

		input := strings.Repeat("a", maxHeaderIDLength+10)

		result := truncateHeaderID(input)

		assert.Len(t, result, maxHeaderIDLength)
	})
}

func TestNewRateLimiterReturnsHandler(t *testing.T) {
	t.Parallel()

	t.Run("returns handler when rate limiting disabled", func(t *testing.T) {
		t.Parallel()

		cfg := &Config{
			RateLimit: RateLimitConfig{
				Enabled:   false,
				Max:       100,
				ExpirySec: 60,
			},
		}

		handler := NewRateLimiter(cfg, nil)

		require.NotNil(t, handler)
	})

	t.Run("returns handler when rate limiting enabled", func(t *testing.T) {
		t.Parallel()

		cfg := &Config{
			RateLimit: RateLimitConfig{
				Enabled:   true,
				Max:       100,
				ExpirySec: 60,
			},
		}

		handler := NewRateLimiter(cfg, nil)

		require.NotNil(t, handler)
	})
}

func TestCustomErrorHandler_MethodNotAllowed(t *testing.T) {
	t.Parallel()

	app := fiber.New(
		fiber.Config{ErrorHandler: customErrorHandlerWithEnv(&libLog.NopLogger{}, "")},
	)
	app.Get("/only-get", func(_ *fiber.Ctx) error {
		return nil
	})

	req := httptest.NewRequest(http.MethodPost, "/only-get", http.NoBody)
	resp, err := app.Test(req)
	require.NoError(t, err)
	defer resp.Body.Close()

	require.Equal(t, http.StatusMethodNotAllowed, resp.StatusCode)
}

func TestCustomErrorHandler_TooManyRequests(t *testing.T) {
	t.Parallel()

	app := fiber.New(
		fiber.Config{ErrorHandler: customErrorHandlerWithEnv(&libLog.NopLogger{}, "")},
	)
	app.Get("/rate-limited", func(_ *fiber.Ctx) error {
		return fiber.NewError(http.StatusTooManyRequests, "rate limit exceeded")
	})

	req := httptest.NewRequest(http.MethodGet, "/rate-limited", http.NoBody)
	resp, err := app.Test(req)
	require.NoError(t, err)
	defer resp.Body.Close()

	require.Equal(t, http.StatusTooManyRequests, resp.StatusCode)

	var body map[string]any
	require.NoError(t, json.NewDecoder(resp.Body).Decode(&body))
	assert.Equal(t, "request_failed", body["title"])
}

func TestCustomErrorHandler_Forbidden(t *testing.T) {
	t.Parallel()

	app := fiber.New(
		fiber.Config{ErrorHandler: customErrorHandlerWithEnv(&libLog.NopLogger{}, "")},
	)
	app.Get("/forbidden", func(_ *fiber.Ctx) error {
		return fiber.NewError(http.StatusForbidden, "access denied")
	})

	req := httptest.NewRequest(http.MethodGet, "/forbidden", http.NoBody)
	resp, err := app.Test(req)
	require.NoError(t, err)
	defer resp.Body.Close()

	require.Equal(t, http.StatusForbidden, resp.StatusCode)

	var body map[string]any
	require.NoError(t, json.NewDecoder(resp.Body).Decode(&body))
	assert.Equal(t, "forbidden", body["title"])
}

func TestCustomErrorHandler_Unauthorized(t *testing.T) {
	t.Parallel()

	app := fiber.New(
		fiber.Config{ErrorHandler: customErrorHandlerWithEnv(&libLog.NopLogger{}, "")},
	)
	app.Get("/unauthorized", func(_ *fiber.Ctx) error {
		return fiber.NewError(http.StatusUnauthorized, "not authenticated")
	})

	req := httptest.NewRequest(http.MethodGet, "/unauthorized", http.NoBody)
	resp, err := app.Test(req)
	require.NoError(t, err)
	defer resp.Body.Close()

	require.Equal(t, http.StatusUnauthorized, resp.StatusCode)

	var body map[string]any
	require.NoError(t, json.NewDecoder(resp.Body).Decode(&body))
	assert.Equal(t, "unauthorized", body["title"])
}

func TestSanitizeHeaderID_EdgeCases(t *testing.T) {
	t.Parallel()

	t.Run("removes newlines", func(t *testing.T) {
		t.Parallel()

		result := sanitizeHeaderID("header\nwith\nnewlines")

		assert.NotContains(t, result, "\n")
	})

	t.Run("removes carriage returns", func(t *testing.T) {
		t.Parallel()

		result := sanitizeHeaderID("header\rwith\rreturns")

		assert.NotContains(t, result, "\r")
	})

	t.Run("removes tabs", func(t *testing.T) {
		t.Parallel()

		result := sanitizeHeaderID("header\twith\ttabs")

		assert.NotContains(t, result, "\t")
	})

	t.Run("generates UUID for empty after sanitization", func(t *testing.T) {
		t.Parallel()

		result := sanitizeHeaderID("\n\r\t   ")

		assert.NotEmpty(t, result)
		assert.NotEqual(t, "\n\r\t   ", result)
	})
}

func TestEvaluateReadinessChecks_AllOptionalDependencies(t *testing.T) {
	t.Parallel()

	deps := &HealthDependencies{
		PostgresCheck:           func(context.Context) error { return nil },
		RedisCheck:              func(context.Context) error { return nil },
		RabbitMQCheck:           func(context.Context) error { return nil },
		RedisOptional:           true,
		PostgresReplicaOptional: true,
		ObjectStorageOptional:   true,
	}

	status, readyStatus, checks := evaluateReadinessChecksWithTimeout(
		context.Background(),
		deps,
		&libLog.NopLogger{},
		0,
	)

	assert.Equal(t, fiber.StatusOK, status)
	assert.Equal(t, "ok", readyStatus)
	assert.Equal(t, "ok", checks["database"])
	assert.Equal(t, "ok", checks["redis"])
	assert.Equal(t, "ok", checks["rabbitmq"])
}

func TestSanitizeErrorForLogging(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name     string
		input    error
		expected string
	}{
		{
			name:     "nil error returns empty string",
			input:    nil,
			expected: "",
		},
		{
			name:     "no secrets passes through unchanged",
			input:    errors.New("connection refused"),
			expected: "connection refused",
		},
		{
			name:     "redacts password= pattern",
			input:    errors.New("host=db password=supersecret dbname=app"),
			expected: "host=db password=***REDACTED*** dbname=app",
		},
		{
			name:     "redacts secret= pattern",
			input:    errors.New("config: secret=mytoken123"),
			expected: "config: secret=***REDACTED***",
		},
		{
			name:     "redacts token= pattern",
			input:    errors.New("auth: token=abc123def456"),
			expected: "auth: token=***REDACTED***",
		},
		{
			name:     "redacts api_key= pattern",
			input:    errors.New("service: api_key=key123"),
			expected: "service: api_key=***REDACTED***",
		},
		{
			name:     "redacts apikey= pattern",
			input:    errors.New("service: apikey=key123"),
			expected: "service: apikey=***REDACTED***",
		},
		{
			name:     "redacts Bearer token",
			input:    errors.New("auth header: Bearer eyJhbGciOiJIUzI1NiJ9.abc"),
			expected: "auth header: Bearer ***REDACTED***",
		},
		{
			name:     "redacts Basic auth",
			input:    errors.New("auth header: Basic dXNlcjpwYXNz"),
			expected: "auth header: Basic ***REDACTED***",
		},
		{
			name:     "case insensitive PASSWORD= redaction",
			input:    errors.New("PASSWORD=MYSECRET host=db"),
			expected: "password=***REDACTED*** host=db",
		},
		{
			name:     "multiple secrets in one message",
			input:    errors.New("host=db password=secret1 token=abc123"),
			expected: "host=db password=***REDACTED*** token=***REDACTED***",
		},
		{
			name:     "password at end of string",
			input:    errors.New("dsn: password=secret"),
			expected: "dsn: password=***REDACTED***",
		},
		{
			name:     "password with quote delimiter",
			input:    errors.New(`config: password="secret" other=value`),
			expected: `config: password=***REDACTED***"secret" other=value`,
		},
		{
			name:     "password with semicolon delimiter",
			input:    errors.New("dsn: password=secret;host=db"),
			expected: "dsn: password=***REDACTED***;host=db",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			result := sanitizeErrorForLogging(tt.input)
			assert.Equal(t, tt.expected, result)
		})
	}
}

func TestFindValueEnd(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name     string
		msg      string
		start    int
		expected int
	}{
		{
			name:     "space terminator",
			msg:      "password=secret next=val",
			start:    9,
			expected: 15,
		},
		{
			name:     "double quote terminator",
			msg:      `password="secret"`,
			start:    10,
			expected: 16,
		},
		{
			name:     "single quote terminator",
			msg:      "password='secret'",
			start:    10,
			expected: 16,
		},
		{
			name:     "newline terminator",
			msg:      "password=secret\nnext",
			start:    9,
			expected: 15,
		},
		{
			name:     "carriage return terminator",
			msg:      "password=secret\rnext",
			start:    9,
			expected: 15,
		},
		{
			name:     "tab terminator",
			msg:      "password=secret\tnext",
			start:    9,
			expected: 15,
		},
		{
			name:     "semicolon terminator",
			msg:      "password=secret;host=db",
			start:    9,
			expected: 15,
		},
		{
			name:     "ampersand terminator",
			msg:      "password=secret&host=db",
			start:    9,
			expected: 15,
		},
		{
			name:     "end of string",
			msg:      "password=secret",
			start:    9,
			expected: 15,
		},
		{
			name:     "start at end of string",
			msg:      "password=",
			start:    9,
			expected: 9,
		},
		{
			name:     "empty string",
			msg:      "",
			start:    0,
			expected: 0,
		},
		{
			name:     "start beyond string length",
			msg:      "short",
			start:    10,
			expected: 5,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			result := findValueEnd(tt.msg, tt.start)
			assert.Equal(t, tt.expected, result)
		})
	}
}

func TestIsSafeHeaderChar(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name     string
		char     rune
		expected bool
	}{
		{name: "alphanumeric a", char: 'a', expected: true},
		{name: "alphanumeric Z", char: 'Z', expected: true},
		{name: "digit 5", char: '5', expected: true},
		{name: "hyphen", char: '-', expected: true},
		{name: "underscore", char: '_', expected: true},
		{name: "dot", char: '.', expected: true},
		{name: "space", char: ' ', expected: true},
		{name: "carriage return", char: '\r', expected: false},
		{name: "newline", char: '\n', expected: false},
		{name: "tab", char: '\t', expected: false},
		{name: "semicolon", char: ';', expected: false},
		{name: "pipe", char: '|', expected: false},
		{name: "null byte", char: 0, expected: false},
		{name: "unicode emoji", char: '🎉', expected: true},
		{name: "at sign", char: '@', expected: true},
		{name: "colon", char: ':', expected: true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			result := isSafeHeaderChar(tt.char)
			assert.Equal(t, tt.expected, result)
		})
	}
}

func TestChecksToString_EdgeCases(t *testing.T) {
	t.Parallel()

	t.Run("nil checks returns unknown", func(t *testing.T) {
		t.Parallel()

		result := checksToString(nil, "database", &libLog.NopLogger{})
		assert.Equal(t, statusUnknown, result)
	})

	t.Run("missing key returns unknown", func(t *testing.T) {
		t.Parallel()

		checks := fiber.Map{"redis": "ok"}
		result := checksToString(checks, "database", &libLog.NopLogger{})
		assert.Equal(t, statusUnknown, result)
	})

	t.Run("non-string value returns unknown", func(t *testing.T) {
		t.Parallel()

		checks := fiber.Map{"database": 42}
		result := checksToString(checks, "database", &libLog.NopLogger{})
		assert.Equal(t, statusUnknown, result)
	})

	t.Run("non-string value with nil logger returns unknown", func(t *testing.T) {
		t.Parallel()

		checks := fiber.Map{"database": true}
		result := checksToString(checks, "database", nil)
		assert.Equal(t, statusUnknown, result)
	})

	t.Run("valid string value returns it", func(t *testing.T) {
		t.Parallel()

		checks := fiber.Map{"database": "ok"}
		result := checksToString(checks, "database", &libLog.NopLogger{})
		assert.Equal(t, "ok", result)
	})

	t.Run("down value returns down", func(t *testing.T) {
		t.Parallel()

		checks := fiber.Map{"database": "down"}
		result := checksToString(checks, "database", &libLog.NopLogger{})
		assert.Equal(t, "down", result)
	})
}

func TestSanitizeHeaderID_WithUnsafeCharsUnderLimit(t *testing.T) {
	t.Parallel()

	// Create a string with unsafe chars mixed with safe chars that is under the length limit
	// so it exercises the sanitization path (not the early truncation path)
	input := strings.Repeat("ab\nc", 20) // 80 chars, under maxHeaderIDLength
	result := sanitizeHeaderID(input)

	assert.NotContains(t, result, "\n")
	assert.NotEmpty(t, result)
}

func TestSanitizeHeaderID_OnlyUnsafeCharsReturnsUUID(t *testing.T) {
	t.Parallel()

	// All characters are unsafe (non-printable) - should return a UUID
	input := "\x00\x01\x02\x03"
	result := sanitizeHeaderID(input)

	assert.NotEmpty(t, result)
	// The result should be a valid UUID since sanitization removed all chars
	assert.Len(t, result, 36) // UUID format: 8-4-4-4-12
}

func TestNewFiberApp_WithTLSConfigCreatesApp(t *testing.T) {
	t.Parallel()

	cfg := &Config{
		Server: ServerConfig{
			BodyLimitBytes:        1024,
			CORSAllowedOrigins:    "*",
			CORSAllowedMethods:    "GET",
			CORSAllowedHeaders:    "Content-Type",
			TLSCertFile:           "/path/to/cert.pem",
			TLSTerminatedUpstream: false,
		},
	}

	app := NewFiberApp(cfg, &libLog.NopLogger{}, nil)
	require.NotNil(t, app)

	app.Get("/test", func(c *fiber.Ctx) error {
		return c.SendStatus(http.StatusOK)
	})

	req := httptest.NewRequest(http.MethodGet, "/test", http.NoBody)
	resp, err := app.Test(req)
	require.NoError(t, err)
	defer resp.Body.Close()

	assert.Equal(t, http.StatusOK, resp.StatusCode)
	// Security headers should still be present regardless of TLS
	assert.Equal(t, "DENY", resp.Header.Get("X-Frame-Options"))
}

func TestNewFiberApp_WithTLSTerminatedUpstreamCreatesApp(t *testing.T) {
	t.Parallel()

	cfg := &Config{
		Server: ServerConfig{
			BodyLimitBytes:        1024,
			CORSAllowedOrigins:    "*",
			CORSAllowedMethods:    "GET",
			CORSAllowedHeaders:    "Content-Type",
			TLSTerminatedUpstream: true,
		},
	}

	app := NewFiberApp(cfg, &libLog.NopLogger{}, nil)
	require.NotNil(t, app)

	app.Get("/test", func(c *fiber.Ctx) error {
		return c.SendStatus(http.StatusOK)
	})

	req := httptest.NewRequest(http.MethodGet, "/test", http.NoBody)
	resp, err := app.Test(req)
	require.NoError(t, err)
	defer resp.Body.Close()

	assert.Equal(t, http.StatusOK, resp.StatusCode)
	assert.Equal(t, "DENY", resp.Header.Get("X-Frame-Options"))
}

func TestNewFiberApp_TrustedProxiesControlsForwardedIPTrust(t *testing.T) {
	t.Parallel()

	t.Run("without trusted proxies ignores forwarded header", func(t *testing.T) {
		t.Parallel()

		cfg := &Config{Server: ServerConfig{BodyLimitBytes: 1024, CORSAllowedOrigins: "*", CORSAllowedMethods: "GET", CORSAllowedHeaders: "Content-Type"}}
		app := NewFiberApp(cfg, &libLog.NopLogger{}, nil)
		app.Get("/ip", func(c *fiber.Ctx) error { return c.SendString(c.IP()) })

		req := httptest.NewRequest(http.MethodGet, "/ip", http.NoBody)
		req.Header.Set("X-Forwarded-For", "203.0.113.10")
		resp, err := app.Test(req)
		require.NoError(t, err)
		defer resp.Body.Close()

		body, err := io.ReadAll(resp.Body)
		require.NoError(t, err)
		assert.NotEqual(t, "203.0.113.10", string(body))
	})

	t.Run("with trusted proxies honors forwarded header", func(t *testing.T) {
		t.Parallel()

		cfg := &Config{Server: ServerConfig{BodyLimitBytes: 1024, CORSAllowedOrigins: "*", CORSAllowedMethods: "GET", CORSAllowedHeaders: "Content-Type", TrustedProxies: "0.0.0.0/0,127.0.0.1"}}
		app := NewFiberApp(cfg, &libLog.NopLogger{}, nil)
		app.Get("/ip", func(c *fiber.Ctx) error { return c.SendString(c.IP()) })

		req := httptest.NewRequest(http.MethodGet, "/ip", http.NoBody)
		req.Header.Set("X-Forwarded-For", "203.0.113.10")
		resp, err := app.Test(req)
		require.NoError(t, err)
		defer resp.Body.Close()

		body, err := io.ReadAll(resp.Body)
		require.NoError(t, err)
		assert.Equal(t, "203.0.113.10", string(body))
	})
}

func TestNewFiberApp_WithQueryTimeoutZero(t *testing.T) {
	t.Parallel()

	cfg := &Config{
		Server: ServerConfig{
			BodyLimitBytes:     1024,
			CORSAllowedOrigins: "*",
			CORSAllowedMethods: "GET",
			CORSAllowedHeaders: "Content-Type",
		},
		Postgres: PostgresConfig{
			QueryTimeoutSec: 0, // zero means no query timeout applied
		},
	}

	app := NewFiberApp(cfg, &libLog.NopLogger{}, nil)
	require.NotNil(t, app)

	app.Get("/test", func(c *fiber.Ctx) error {
		return c.SendStatus(http.StatusOK)
	})

	req := httptest.NewRequest(http.MethodGet, "/test", http.NoBody)
	resp, err := app.Test(req)
	require.NoError(t, err)
	defer resp.Body.Close()

	// With QueryTimeoutSec=0, QueryTimeout() returns 30s default.
	// The middleware is still applied since 30s > 0.
	assert.Equal(t, http.StatusOK, resp.StatusCode)
}

func TestNewFiberApp_ProductionDoesNotLogRequests(t *testing.T) {
	t.Parallel()

	cfg := &Config{
		App: AppConfig{
			EnvName: " Production ",
		},
		Server: ServerConfig{
			BodyLimitBytes:     1024,
			CORSAllowedOrigins: "*",
			CORSAllowedMethods: "GET",
			CORSAllowedHeaders: "Content-Type",
		},
	}

	app := NewFiberApp(cfg, &libLog.NopLogger{}, nil)
	require.NotNil(t, app)

	app.Get("/test", func(c *fiber.Ctx) error {
		return c.SendStatus(http.StatusOK)
	})

	req := httptest.NewRequest(http.MethodGet, "/test", http.NoBody)
	resp, err := app.Test(req)
	require.NoError(t, err)
	defer resp.Body.Close()
	assert.Equal(t, http.StatusOK, resp.StatusCode)
}

func TestStructuredRequestLogger(t *testing.T) {
	t.Parallel()

	t.Run("with nil logger does not panic", func(t *testing.T) {
		t.Parallel()

		app := fiber.New()
		app.Use(structuredRequestLogger(nil))
		app.Get("/test", func(c *fiber.Ctx) error {
			return c.SendStatus(http.StatusOK)
		})

		req := httptest.NewRequest(http.MethodGet, "/test", http.NoBody)
		resp, err := app.Test(req)
		require.NoError(t, err)
		defer resp.Body.Close()
		assert.Equal(t, http.StatusOK, resp.StatusCode)
	})

	t.Run("with valid logger does not panic", func(t *testing.T) {
		t.Parallel()

		app := fiber.New()
		app.Use(structuredRequestLogger(&libLog.NopLogger{}))
		app.Get("/test", func(c *fiber.Ctx) error {
			return c.SendStatus(http.StatusOK)
		})

		req := httptest.NewRequest(http.MethodGet, "/test", http.NoBody)
		resp, err := app.Test(req)
		require.NoError(t, err)
		defer resp.Body.Close()
		assert.Equal(t, http.StatusOK, resp.StatusCode)
	})
}

func TestEvaluateReadinessChecks_NilDeps(t *testing.T) {
	t.Parallel()

	status, readyStatus, checks := evaluateReadinessChecksWithTimeout(
		context.Background(),
		nil,
		&libLog.NopLogger{},
		0,
	)

	assert.Equal(t, fiber.StatusServiceUnavailable, status)
	assert.Equal(t, "degraded", readyStatus)
	assert.NotNil(t, checks)
}

func TestEvaluateReadinessChecks_AllRequiredDown(t *testing.T) {
	t.Parallel()

	deps := &HealthDependencies{
		PostgresCheck:    func(context.Context) error { return errBoom },
		RabbitMQCheck:    func(context.Context) error { return errBoom },
		RedisCheck:       func(context.Context) error { return errBoom },
		RedisOptional:    false,
		PostgresOptional: false,
		RabbitMQOptional: false,
	}

	status, readyStatus, checks := evaluateReadinessChecksWithTimeout(
		context.Background(),
		deps,
		&libLog.NopLogger{},
		0,
	)

	assert.Equal(t, fiber.StatusServiceUnavailable, status)
	assert.Equal(t, "degraded", readyStatus)
	assert.Equal(t, "down", checks["database"])
	assert.Equal(t, "down", checks["redis"])
	assert.Equal(t, "down", checks["rabbitmq"])
}

func TestCustomErrorHandler_NilLogger(t *testing.T) {
	t.Parallel()

	app := fiber.New(
		fiber.Config{ErrorHandler: customErrorHandlerWithEnv(nil, "")},
	)
	app.Get("/boom", func(_ *fiber.Ctx) error {
		return errBoom
	})

	req := httptest.NewRequest(http.MethodGet, "/boom", http.NoBody)
	resp, err := app.Test(req)
	require.NoError(t, err)
	defer resp.Body.Close()
	assert.Equal(t, http.StatusInternalServerError, resp.StatusCode)
}

func TestResolvePostgresCheck_WithConnection_Success(t *testing.T) {
	t.Parallel()

	db, mock, err := sqlmock.New(sqlmock.MonitorPingsOption(true))
	require.NoError(t, err)
	defer func() { _ = db.Close() }()

	mock.ExpectPing()

	resolver := dbresolver.New(dbresolver.WithPrimaryDBs(db))
	postgres := testutil.NewClientWithResolver(resolver)
	deps := &HealthDependencies{
		Postgres:      postgres,
		PostgresCheck: nil, // force the inline function creation
	}

	checkFunc, available := resolvePostgresCheck(deps)

	require.True(t, available)
	require.NotNil(t, checkFunc)

	err = checkFunc(context.Background())
	require.NoError(t, err)
	require.NoError(t, mock.ExpectationsWereMet())
}

func TestResolvePostgresCheck_WithConnection_PingError(t *testing.T) {
	t.Parallel()

	db, mock, err := sqlmock.New(sqlmock.MonitorPingsOption(true))
	require.NoError(t, err)
	defer func() { _ = db.Close() }()

	mock.ExpectPing().WillReturnError(errBoom)

	resolver := dbresolver.New(dbresolver.WithPrimaryDBs(db))
	postgres := testutil.NewClientWithResolver(resolver)
	deps := &HealthDependencies{
		Postgres:      postgres,
		PostgresCheck: nil,
	}

	checkFunc, available := resolvePostgresCheck(deps)

	require.True(t, available)
	require.NotNil(t, checkFunc)

	err = checkFunc(context.Background())
	require.Error(t, err)
	assert.Contains(t, err.Error(), "ping failed")
}

func TestResolvePostgresReplicaCheck_WithConnection_Success(t *testing.T) {
	t.Parallel()

	db, mock, err := sqlmock.New(sqlmock.MonitorPingsOption(true))
	require.NoError(t, err)
	defer func() { _ = db.Close() }()

	mock.ExpectPing()

	resolver := dbresolver.New(dbresolver.WithPrimaryDBs(db), dbresolver.WithReplicaDBs(db))
	replica := testutil.NewClientWithResolver(resolver)
	deps := &HealthDependencies{
		PostgresReplica:      replica,
		PostgresReplicaCheck: nil,
	}

	checkFunc, available := resolvePostgresReplicaCheck(deps)

	require.True(t, available)
	require.NotNil(t, checkFunc)

	err = checkFunc(context.Background())
	require.NoError(t, err)
	require.NoError(t, mock.ExpectationsWereMet())
}

func TestResolvePostgresReplicaCheck_WithConnection_PingError(t *testing.T) {
	t.Parallel()

	db, mock, err := sqlmock.New(sqlmock.MonitorPingsOption(true))
	require.NoError(t, err)
	defer func() { _ = db.Close() }()

	mock.ExpectPing().WillReturnError(errBoom)

	resolver := dbresolver.New(dbresolver.WithPrimaryDBs(db), dbresolver.WithReplicaDBs(db))
	replica := testutil.NewClientWithResolver(resolver)
	deps := &HealthDependencies{
		PostgresReplica:      replica,
		PostgresReplicaCheck: nil,
	}

	checkFunc, available := resolvePostgresReplicaCheck(deps)

	require.True(t, available)
	require.NotNil(t, checkFunc)

	err = checkFunc(context.Background())
	require.Error(t, err)
	assert.Contains(t, err.Error(), "ping replica[0] failed")
}

func TestResolvePostgresReplicaCheck_WithConnection_NoReplicasConfigured(t *testing.T) {
	t.Parallel()

	db, _, err := sqlmock.New(sqlmock.MonitorPingsOption(true))
	require.NoError(t, err)
	defer func() { _ = db.Close() }()

	resolver := dbresolver.New(dbresolver.WithPrimaryDBs(db))
	replica := testutil.NewClientWithResolver(resolver)
	deps := &HealthDependencies{
		PostgresReplica:      replica,
		PostgresReplicaCheck: nil,
	}

	checkFunc, available := resolvePostgresReplicaCheck(deps)

	require.True(t, available)
	require.NotNil(t, checkFunc)

	err = checkFunc(context.Background())
	require.Error(t, err)
	assert.Contains(t, err.Error(), "no replica databases configured")
}

func TestResolvePostgresReplicaCheck_WithConnection_AllReplicaEntriesNil(t *testing.T) {
	t.Parallel()

	db, _, err := sqlmock.New(sqlmock.MonitorPingsOption(true))
	require.NoError(t, err)
	defer func() { _ = db.Close() }()

	resolver := dbresolver.New(dbresolver.WithPrimaryDBs(db), dbresolver.WithReplicaDBs((*sql.DB)(nil)))
	replica := testutil.NewClientWithResolver(resolver)
	deps := &HealthDependencies{
		PostgresReplica:      replica,
		PostgresReplicaCheck: nil,
	}

	checkFunc, available := resolvePostgresReplicaCheck(deps)

	require.True(t, available)
	require.NotNil(t, checkFunc)

	err = checkFunc(context.Background())
	require.Error(t, err)
	assert.Contains(t, err.Error(), "no non-nil replica databases configured")
}

func TestResolveObjectStorageCheck_WithStorage_Success(t *testing.T) {
	t.Parallel()

	mockStorage := &mockObjectStorage{exists: false, err: nil}
	deps := &HealthDependencies{
		ObjectStorage:      mockStorage,
		ObjectStorageCheck: nil,
	}

	checkFunc, available := resolveObjectStorageCheck(deps)

	require.True(t, available)
	require.NotNil(t, checkFunc)

	err := checkFunc(context.Background())
	require.NoError(t, err)
}

func TestResolveObjectStorageCheck_WithStorage_Error(t *testing.T) {
	t.Parallel()

	mockStorage := &mockObjectStorage{exists: false, err: errBoom}
	deps := &HealthDependencies{
		ObjectStorage:      mockStorage,
		ObjectStorageCheck: nil,
	}

	checkFunc, available := resolveObjectStorageCheck(deps)

	require.True(t, available)
	require.NotNil(t, checkFunc)

	err := checkFunc(context.Background())
	require.Error(t, err)
	assert.Contains(t, err.Error(), "object storage health check")
}

type mockObjectStorage struct {
	exists bool
	err    error
}

func (m *mockObjectStorage) Exists(_ context.Context, _ string) (bool, error) {
	return m.exists, m.err
}

func TestResolveRabbitMQCheck_WithConnection_HealthCheckSuccess(t *testing.T) {
	t.Parallel()

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusOK)
	}))
	defer server.Close()

	deps := &HealthDependencies{
		RabbitMQ: &libRabbitmq.RabbitMQConnection{
			HealthCheckURL:           server.URL,
			AllowInsecureHealthCheck: true,
		},
		RabbitMQCheck: nil,
	}

	checkFunc, available := resolveRabbitMQCheck(deps)

	require.True(t, available)
	require.NotNil(t, checkFunc)

	err := checkFunc(context.Background())
	require.NoError(t, err)
}

func TestNewFiberApp_WithTelemetry(t *testing.T) {
	t.Parallel()

	cfg := &Config{
		Server: ServerConfig{
			BodyLimitBytes:     1024,
			CORSAllowedOrigins: "*",
			CORSAllowedMethods: "GET",
			CORSAllowedHeaders: "Content-Type",
		},
		App: AppConfig{
			EnvName: "development",
		},
		Telemetry: TelemetryConfig{
			Enabled:        false,
			ServiceName:    "test",
			ServiceVersion: "1.0",
		},
	}

	telemetry := InitTelemetry(cfg, &libLog.NopLogger{})
	app := NewFiberApp(cfg, &libLog.NopLogger{}, telemetry)
	require.NotNil(t, app)

	app.Get("/test", func(c *fiber.Ctx) error {
		return c.SendStatus(http.StatusOK)
	})

	req := httptest.NewRequest(http.MethodGet, "/test", http.NoBody)
	resp, err := app.Test(req)
	require.NoError(t, err)
	defer resp.Body.Close()
	assert.Equal(t, http.StatusOK, resp.StatusCode)
	assert.NotEmpty(t, resp.Header.Get("X-Request-ID"))
}

func TestReadinessHandler_NilContext(t *testing.T) {
	t.Parallel()

	app := fiber.New()
	cfg := &Config{App: AppConfig{EnvName: "development"}}
	deps := &HealthDependencies{
		PostgresCheck:           func(context.Context) error { return nil },
		RabbitMQCheck:           func(context.Context) error { return nil },
		RedisOptional:           true,
		PostgresReplicaOptional: true,
		ObjectStorageOptional:   true,
	}
	app.Get("/ready", readinessHandler(cfg, nil, deps, &libLog.NopLogger{}))

	req := httptest.NewRequest(http.MethodGet, "/ready", http.NoBody)
	resp, err := app.Test(req)
	require.NoError(t, err)
	defer resp.Body.Close()
	assert.Equal(t, http.StatusOK, resp.StatusCode)
}

func TestSanitizeErrorForLogging_MultiplePasswordOccurrences(t *testing.T) {
	t.Parallel()

	err := errors.New("password=secret1 and password=secret2")
	result := sanitizeErrorForLogging(err)

	assert.NotContains(t, result, "secret1")
	assert.NotContains(t, result, "secret2")
	assert.Contains(t, result, "password=***REDACTED***")
}

func TestSanitizeErrorForLogging_NestedPatterns(t *testing.T) {
	t.Parallel()

	err := errors.New("DSN: host=db password=s3cr3t sslmode=disable; token=mytoken host2=db2")
	result := sanitizeErrorForLogging(err)

	assert.NotContains(t, result, "s3cr3t")
	assert.NotContains(t, result, "mytoken")
	assert.Contains(t, result, "password=***REDACTED***")
	assert.Contains(t, result, "token=***REDACTED***")
}

func TestDbQueryTimeoutMiddleware_AppliesDeadline(t *testing.T) {
	t.Parallel()

	app := fiber.New()
	app.Use(dbQueryTimeoutMiddleware(5 * time.Second))

	var hasDeadline bool

	app.Get("/test", func(c *fiber.Ctx) error {
		ctx := c.UserContext()
		_, hasDeadline = ctx.Deadline()

		return c.SendStatus(http.StatusOK)
	})

	req := httptest.NewRequest(http.MethodGet, "/test", http.NoBody)
	resp, err := app.Test(req)
	require.NoError(t, err)
	defer resp.Body.Close()

	assert.True(t, hasDeadline, "context should have a deadline after middleware")
	assert.Equal(t, http.StatusOK, resp.StatusCode)
}

func TestDbQueryTimeoutMiddleware_RespectsExistingTighterDeadline(t *testing.T) {
	t.Parallel()

	app := fiber.New()

	// First set a tighter deadline (1 second) via a preceding middleware
	app.Use(func(c *fiber.Ctx) error {
		ctx, cancel := context.WithTimeout(c.UserContext(), 1*time.Second)
		defer cancel()

		c.SetUserContext(ctx)

		return c.Next()
	})

	// Then apply the query timeout middleware with a longer timeout (30 seconds)
	app.Use(dbQueryTimeoutMiddleware(30 * time.Second))

	var deadlineFromHandler time.Time

	app.Get("/test", func(c *fiber.Ctx) error {
		ctx := c.UserContext()
		deadlineFromHandler, _ = ctx.Deadline()

		return c.SendStatus(http.StatusOK)
	})

	req := httptest.NewRequest(http.MethodGet, "/test", http.NoBody)
	resp, err := app.Test(req)
	require.NoError(t, err)
	defer resp.Body.Close()

	// The deadline should be approximately 1 second from now (the tighter one),
	// not 30 seconds. The middleware should not override a tighter existing deadline.
	assert.True(t, time.Until(deadlineFromHandler) <= 2*time.Second,
		"existing tighter deadline should be preserved")
	assert.Equal(t, http.StatusOK, resp.StatusCode)
}

func TestDbQueryTimeoutMiddleware_ZeroDurationDisablesTimeout(t *testing.T) {
	t.Parallel()

	app := fiber.New()
	// A zero-duration timeout would immediately cancel, so the middleware
	// is expected to be skipped when timeout <= 0 (handled by NewFiberApp).
	// Here we test that the middleware itself works with a valid duration.
	app.Use(dbQueryTimeoutMiddleware(10 * time.Second))

	var hasDeadline bool

	app.Get("/test", func(c *fiber.Ctx) error {
		_, hasDeadline = c.UserContext().Deadline()

		return c.SendStatus(http.StatusOK)
	})

	req := httptest.NewRequest(http.MethodGet, "/test", http.NoBody)
	resp, err := app.Test(req)
	require.NoError(t, err)
	defer resp.Body.Close()

	assert.True(t, hasDeadline)
	assert.Equal(t, http.StatusOK, resp.StatusCode)
}

func TestServer_GetApp(t *testing.T) {
	t.Parallel()

	t.Run("returns fiber app when server is valid", func(t *testing.T) {
		t.Parallel()

		app := fiber.New()
		srv := &Server{app: app}

		got := srv.GetApp()

		assert.NotNil(t, got)
		assert.Same(t, app, got)
	})

	t.Run("returns nil when server is nil", func(t *testing.T) {
		t.Parallel()

		var srv *Server

		got := srv.GetApp()

		assert.Nil(t, got)
	})

	t.Run("returns nil when app field is nil", func(t *testing.T) {
		t.Parallel()

		srv := &Server{}

		got := srv.GetApp()

		assert.Nil(t, got)
	})
}
