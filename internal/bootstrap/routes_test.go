// Copyright 2025 Lerian Studio. All rights reserved.
// Use of this source code is governed by an Elastic License 2.0
// that can be found in the LICENSE.md file.

//go:build unit

package bootstrap

import (
	"context"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/alicebob/miniredis/v2"
	"github.com/gofiber/fiber/v2"
	goredis "github.com/redis/go-redis/v9"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	authMiddleware "github.com/LerianStudio/lib-auth/v3/auth/middleware"
	libLog "github.com/LerianStudio/lib-commons/v5/commons/log"
	"github.com/LerianStudio/lib-commons/v5/commons/net/http/ratelimit"

	swagger "github.com/LerianStudio/matcher/docs/swagger"
	"github.com/LerianStudio/matcher/internal/auth"
	"github.com/LerianStudio/matcher/internal/shared/infrastructure/testutil"
)

func TestRegisterRoutes_NilTenantExtractor_ReturnsError(t *testing.T) {
	t.Parallel()

	app := fiber.New()
	cfg := &Config{App: AppConfig{EnvName: "test"}}
	client := authMiddleware.NewAuthClient("", false, nil)

	routes, err := RegisterRoutes(app, cfg, nil, nil, nil, nil, &libLog.NopLogger{}, client, nil, nil, nil, nil)
	require.Error(t, err)
	require.Nil(t, routes)
	require.ErrorContains(t, err, "tenant extractor is required")
}

func TestRegisterRoutes_NilApp_ReturnsError(t *testing.T) {
	t.Parallel()

	cfg := &Config{App: AppConfig{EnvName: "test"}}
	client := authMiddleware.NewAuthClient("", false, nil)
	extractor, err := auth.NewTenantExtractor(
		false,
		false,
		"11111111-1111-1111-1111-111111111111",
		"default",
		"",
		"test",
	)
	require.NoError(t, err)

	routes, err := RegisterRoutes(nil, cfg, nil, nil, nil, nil, &libLog.NopLogger{}, client, extractor, nil, nil, nil)
	require.Error(t, err)
	require.Nil(t, routes)
	require.ErrorContains(t, err, "fiber app is required")
}

func TestRegisterRoutes_NilConfig_ReturnsError(t *testing.T) {
	t.Parallel()

	app := fiber.New()
	client := authMiddleware.NewAuthClient("", false, nil)
	extractor, err := auth.NewTenantExtractor(
		false,
		false,
		"11111111-1111-1111-1111-111111111111",
		"default",
		"",
		"test",
	)
	require.NoError(t, err)

	routes, err := RegisterRoutes(app, nil, nil, nil, nil, nil, &libLog.NopLogger{}, client, extractor, nil, nil, nil)
	require.Error(t, err)
	require.Nil(t, routes)
	require.ErrorContains(t, err, "config is required")
}

func TestRegisterRoutes_Success(t *testing.T) {
	t.Parallel()

	t.Run("with rate limiting enabled", func(t *testing.T) {
		t.Parallel()

		app := fiber.New()
		cfg := &Config{
			App: AppConfig{EnvName: "test"},
			RateLimit: RateLimitConfig{
				Enabled:   true,
				Max:       100,
				ExpirySec: 60,
			},
		}
		client := authMiddleware.NewAuthClient("", false, nil)
		extractor, err := auth.NewTenantExtractor(
			false,
			false,
			"11111111-1111-1111-1111-111111111111",
			"default",
			"",
			"test",
		)
		require.NoError(t, err)

		routes, err := RegisterRoutes(
			app,
			cfg,
			nil,
			nil,
			nil,
			nil,
			&libLog.NopLogger{},
			client,
			extractor,
			nil,
			nil,
			nil,
		)

		require.NoError(t, err)
		require.NotNil(t, routes)
		require.NotNil(t, routes.API)
		require.NotNil(t, routes.Protected)
	})

	t.Run("with rate limiting disabled", func(t *testing.T) {
		t.Parallel()

		app := fiber.New()
		cfg := &Config{
			App:       AppConfig{EnvName: "test"},
			RateLimit: RateLimitConfig{Enabled: false},
		}
		client := authMiddleware.NewAuthClient("", false, nil)
		extractor, err := auth.NewTenantExtractor(
			false,
			false,
			"11111111-1111-1111-1111-111111111111",
			"default",
			"",
			"test",
		)
		require.NoError(t, err)

		routes, err := RegisterRoutes(
			app,
			cfg,
			nil,
			nil,
			nil,
			nil,
			&libLog.NopLogger{},
			client,
			extractor,
			nil,
			nil,
			nil,
		)

		require.NoError(t, err)
		require.NotNil(t, routes)
		require.NotNil(t, routes.API)
		require.NotNil(t, routes.Protected)
	})

	t.Run("with swagger enabled", func(t *testing.T) {
		t.Parallel()

		app := fiber.New()
		cfg := &Config{
			App:     AppConfig{EnvName: "development"},
			Swagger: SwaggerConfig{Enabled: true},
		}
		client := authMiddleware.NewAuthClient("", false, nil)
		extractor, err := auth.NewTenantExtractor(
			false,
			false,
			"11111111-1111-1111-1111-111111111111",
			"default",
			"",
			"test",
		)
		require.NoError(t, err)

		routes, err := RegisterRoutes(
			app,
			cfg,
			nil,
			nil,
			nil,
			nil,
			&libLog.NopLogger{},
			client,
			extractor,
			nil,
			nil,
			nil,
		)

		require.NoError(t, err)
		require.NotNil(t, routes)
	})

	// Swagger host override tests mutate the package-level swagger.SwaggerInfo.Host
	// global, so they must run sequentially within this group (no t.Parallel).
	t.Run("swagger host override", func(t *testing.T) {
		// Save and restore the global after all sub-tests complete.
		originalHost := swagger.SwaggerInfo.Host
		t.Cleanup(func() { swagger.SwaggerInfo.Host = originalHost })

		t.Run("sets host when configured", func(t *testing.T) {
			swagger.SwaggerInfo.Host = "" // reset before test

			app := fiber.New()
			cfg := &Config{
				App:     AppConfig{EnvName: "development"},
				Swagger: SwaggerConfig{Enabled: true, Host: "api.example.com"},
			}
			client := authMiddleware.NewAuthClient("", false, nil)
			extractor, err := auth.NewTenantExtractor(
				false,
				false,
				"11111111-1111-1111-1111-111111111111",
				"default",
				"",
				"test",
			)
			require.NoError(t, err)

			_, err = RegisterRoutes(
				app,
				cfg,
				nil,
				nil,
				nil,
				nil,
				&libLog.NopLogger{},
				client,
				extractor,
				nil,
				nil,
				nil,
			)

			require.NoError(t, err)
			assert.Equal(t, "api.example.com", swagger.SwaggerInfo.Host)
		})

		t.Run("keeps default when host is empty", func(t *testing.T) {
			swagger.SwaggerInfo.Host = "" // reset before test

			app := fiber.New()
			cfg := &Config{
				App:     AppConfig{EnvName: "development"},
				Swagger: SwaggerConfig{Enabled: true, Host: ""},
			}
			client := authMiddleware.NewAuthClient("", false, nil)
			extractor, err := auth.NewTenantExtractor(
				false,
				false,
				"11111111-1111-1111-1111-111111111111",
				"default",
				"",
				"test",
			)
			require.NoError(t, err)

			_, err = RegisterRoutes(
				app,
				cfg,
				nil,
				nil,
				nil,
				nil,
				&libLog.NopLogger{},
				client,
				extractor,
				nil,
				nil,
				nil,
			)

			require.NoError(t, err)
			assert.Equal(t, "", swagger.SwaggerInfo.Host)
		})
	})

	// Swagger schemes override tests mutate the package-level swagger.SwaggerInfo.Schemes
	// global, so they must run sequentially within this group (no t.Parallel).
	t.Run("swagger schemes override", func(t *testing.T) {
		// Save and restore the global after all sub-tests complete.
		originalSchemes := swagger.SwaggerInfo.Schemes
		t.Cleanup(func() { swagger.SwaggerInfo.Schemes = originalSchemes })

		t.Run("sets http when configured", func(t *testing.T) {
			swagger.SwaggerInfo.Schemes = []string{"https"} // reset before test

			app := fiber.New()
			cfg := &Config{
				App:     AppConfig{EnvName: "development"},
				Swagger: SwaggerConfig{Enabled: true, Schemes: "http"},
			}
			client := authMiddleware.NewAuthClient("", false, nil)
			extractor, err := auth.NewTenantExtractor(
				false,
				false,
				"11111111-1111-1111-1111-111111111111",
				"default",
				"",
				"test",
			)
			require.NoError(t, err)

			_, err = RegisterRoutes(
				app,
				cfg,
				nil,
				nil,
				nil,
				nil,
				&libLog.NopLogger{},
				client,
				extractor,
				nil,
				nil,
				nil,
			)

			require.NoError(t, err)
			assert.Equal(t, []string{"http"}, swagger.SwaggerInfo.Schemes)
		})

		t.Run("sets multiple schemes when configured", func(t *testing.T) {
			swagger.SwaggerInfo.Schemes = []string{"https"} // reset before test

			app := fiber.New()
			cfg := &Config{
				App:     AppConfig{EnvName: "development"},
				Swagger: SwaggerConfig{Enabled: true, Schemes: "http,https"},
			}
			client := authMiddleware.NewAuthClient("", false, nil)
			extractor, err := auth.NewTenantExtractor(
				false,
				false,
				"11111111-1111-1111-1111-111111111111",
				"default",
				"",
				"test",
			)
			require.NoError(t, err)

			_, err = RegisterRoutes(
				app,
				cfg,
				nil,
				nil,
				nil,
				nil,
				&libLog.NopLogger{},
				client,
				extractor,
				nil,
				nil,
				nil,
			)

			require.NoError(t, err)
			assert.Equal(t, []string{"http", "https"}, swagger.SwaggerInfo.Schemes)
		})

		t.Run("keeps default when schemes is empty", func(t *testing.T) {
			swagger.SwaggerInfo.Schemes = []string{"https"} // reset before test

			app := fiber.New()
			cfg := &Config{
				App:     AppConfig{EnvName: "development"},
				Swagger: SwaggerConfig{Enabled: true, Schemes: ""},
			}
			client := authMiddleware.NewAuthClient("", false, nil)
			extractor, err := auth.NewTenantExtractor(
				false,
				false,
				"11111111-1111-1111-1111-111111111111",
				"default",
				"",
				"test",
			)
			require.NoError(t, err)

			_, err = RegisterRoutes(
				app,
				cfg,
				nil,
				nil,
				nil,
				nil,
				&libLog.NopLogger{},
				client,
				extractor,
				nil,
				nil,
				nil,
			)

			require.NoError(t, err)
			assert.Equal(t, []string{"https"}, swagger.SwaggerInfo.Schemes)
		})
	})

	t.Run("with swagger enabled in production is not exposed", func(t *testing.T) {
		t.Parallel()

		app := fiber.New()
		cfg := &Config{
			App:     AppConfig{EnvName: " Production "},
			Swagger: SwaggerConfig{Enabled: true},
		}
		client := authMiddleware.NewAuthClient("", false, nil)
		extractor, err := auth.NewTenantExtractor(
			false,
			false,
			"11111111-1111-1111-1111-111111111111",
			"default",
			"",
			"test",
		)
		require.NoError(t, err)

		_, err = RegisterRoutes(app, cfg, nil, nil, nil, nil, &libLog.NopLogger{}, client, extractor, nil, nil, nil)
		require.NoError(t, err)

		req := httptest.NewRequest(http.MethodGet, "/swagger/index.html", http.NoBody)
		resp, err := app.Test(req)
		require.NoError(t, err)
		defer resp.Body.Close()

		assert.Equal(t, http.StatusNotFound, resp.StatusCode)
	})
}

func TestRegisterRoutes_HealthEndpoints(t *testing.T) {
	// No t.Parallel(): this test mutates the package-level selfProbeOK flag
	// and must not race with other tests reading it (e.g., parallel readiness
	// tests that assume the flag's default state).

	app := fiber.New()
	cfg := &Config{App: AppConfig{EnvName: "development"}}
	client := authMiddleware.NewAuthClient("", false, nil)
	extractor, err := auth.NewTenantExtractor(
		false,
		false,
		"11111111-1111-1111-1111-111111111111",
		"default",
		"",
		"test",
	)
	require.NoError(t, err)

	_, err = RegisterRoutes(app, cfg, nil, nil, nil, nil, &libLog.NopLogger{}, client, extractor, nil, nil, nil)
	require.NoError(t, err)

	t.Run("health endpoint gated by self-probe", func(t *testing.T) {
		// /health returns 503 until RunSelfProbe has confirmed dependencies;
		// it returns 200 once the flag is flipped. Exercise both branches so
		// the gate is exercised and the happy path still works.
		t.Cleanup(resetSelfProbeStateForTest)
		resetSelfProbeStateForTest()

		req := httptest.NewRequest(http.MethodGet, "/health", http.NoBody)
		resp, err := app.Test(req)
		require.NoError(t, err)
		defer resp.Body.Close()

		assert.Equal(t, http.StatusServiceUnavailable, resp.StatusCode,
			"/health is 503 before self-probe flips the flag")

		// Simulate a successful self-probe and re-check.
		require.NoError(t, RunSelfProbe(context.Background(), &HealthDependencies{
			PostgresCheck:           func(context.Context) error { return nil },
			RedisCheck:              func(context.Context) error { return nil },
			RabbitMQCheck:           func(context.Context) error { return nil },
			RedisOptional:           true,
			PostgresReplicaOptional: true,
			ObjectStorageOptional:   true,
		}, &libLog.NopLogger{}))

		req2 := httptest.NewRequest(http.MethodGet, "/health", http.NoBody)
		resp2, err := app.Test(req2)
		require.NoError(t, err)
		defer resp2.Body.Close()

		assert.Equal(t, http.StatusOK, resp2.StatusCode,
			"/health returns 200 once self-probe succeeds")
	})

	t.Run("readyz endpoint is accessible pre-auth", func(t *testing.T) {
		// /readyz must mount BEFORE the auth chain — K8s readiness probes
		// are unauthenticated. Hitting it with no headers must yield 200
		// or 503, never 401.
		req := httptest.NewRequest(http.MethodGet, "/readyz", http.NoBody)
		resp, err := app.Test(req)
		require.NoError(t, err)
		defer resp.Body.Close()

		assert.NotEqual(t, http.StatusUnauthorized, resp.StatusCode,
			"/readyz must mount before auth middleware")
		assert.Contains(t, []int{http.StatusOK, http.StatusServiceUnavailable}, resp.StatusCode,
			"/readyz responds with 200 or 503 only")
	})

	t.Run("version endpoint is accessible", func(t *testing.T) {
		req := httptest.NewRequest(http.MethodGet, "/version", http.NoBody)
		resp, err := app.Test(req)
		require.NoError(t, err)
		defer resp.Body.Close()

		assert.Equal(t, http.StatusOK, resp.StatusCode)
	})
}

func TestRoutesStruct(t *testing.T) {
	t.Parallel()

	t.Run("can access API and Protected fields", func(t *testing.T) {
		t.Parallel()

		app := fiber.New()
		cfg := &Config{App: AppConfig{EnvName: "test"}}
		client := authMiddleware.NewAuthClient("", false, nil)
		extractor, err := auth.NewTenantExtractor(
			false,
			false,
			"11111111-1111-1111-1111-111111111111",
			"default",
			"",
			"test",
		)
		require.NoError(t, err)

		routes, err := RegisterRoutes(
			app,
			cfg,
			nil,
			nil,
			nil,
			nil,
			&libLog.NopLogger{},
			client,
			extractor,
			nil,
			nil,
			nil,
		)

		require.NoError(t, err)
		assert.NotNil(t, routes.API)
		assert.NotNil(t, routes.Protected)

		req := httptest.NewRequest(http.MethodGet, "/", http.NoBody)
		router := routes.Protected("configuration", "read")
		router.Get("/", func(c *fiber.Ctx) error {
			return c.SendStatus(http.StatusOK)
		})

		resp, err := app.Test(req)
		require.NoError(t, err)
		defer resp.Body.Close()

		assert.Equal(t, http.StatusOK, resp.StatusCode)
	})
}

func TestRegisterRoutes_DynamicRateLimitToggle(t *testing.T) {
	t.Parallel()

	// Set up an in-memory Redis via miniredis so the lib-commons rate limiter
	// can execute its Lua-based counter logic without a real Redis instance.
	server := miniredis.RunT(t)
	redisClient := goredis.NewClient(&goredis.Options{Addr: server.Addr()})
	conn := testutil.NewRedisClientWithMock(redisClient)
	rl := ratelimit.New(conn, ratelimit.WithFailOpen(false))
	require.NotNil(t, rl, "rate limiter must be non-nil with a valid Redis connection")

	rateLimiterGetter := func() *ratelimit.RateLimiter { return rl }

	app := fiber.New()
	cfg := &Config{
		App: AppConfig{EnvName: "test"},
		RateLimit: RateLimitConfig{
			Enabled:   false,
			Max:       1,
			ExpirySec: 60,
		},
	}

	currentCfg := cfg
	configGetter := func() *Config {
		return currentCfg
	}

	client := authMiddleware.NewAuthClient("", false, nil)
	extractor, err := auth.NewTenantExtractor(
		false,
		false,
		"11111111-1111-1111-1111-111111111111",
		"default",
		"",
		"test",
	)
	require.NoError(t, err)

	routes, err := RegisterRoutes(app, cfg, configGetter, nil, nil, nil, &libLog.NopLogger{}, client, extractor, rateLimiterGetter, nil, nil)
	require.NoError(t, err)

	router := routes.Protected("configuration", "read")
	router.Get("/dynamic-rate-limit", func(c *fiber.Ctx) error {
		return c.SendStatus(http.StatusOK)
	})

	// Initially disabled: request should pass.
	firstReq := httptest.NewRequest(http.MethodGet, "/dynamic-rate-limit", http.NoBody)
	firstResp, firstErr := app.Test(firstReq)
	require.NoError(t, firstErr)
	defer firstResp.Body.Close()
	assert.Equal(t, http.StatusOK, firstResp.StatusCode)

	// Toggle on at runtime using config getter.
	currentCfg = &Config{
		App: AppConfig{EnvName: "test"},
		RateLimit: RateLimitConfig{
			Enabled:   true,
			Max:       1,
			ExpirySec: 60,
		},
	}

	secondReq := httptest.NewRequest(http.MethodGet, "/dynamic-rate-limit", http.NoBody)
	secondResp, secondErr := app.Test(secondReq)
	require.NoError(t, secondErr)
	defer secondResp.Body.Close()
	assert.Equal(t, http.StatusOK, secondResp.StatusCode)

	thirdReq := httptest.NewRequest(http.MethodGet, "/dynamic-rate-limit", http.NoBody)
	thirdResp, thirdErr := app.Test(thirdReq)
	require.NoError(t, thirdErr)
	defer thirdResp.Body.Close()
	assert.Equal(t, http.StatusTooManyRequests, thirdResp.StatusCode)
}

func TestWhenEnabled_NilMiddleware_CallsNext(t *testing.T) {
	t.Parallel()

	app := fiber.New()

	var nextCalled bool

	app.Get("/test", WhenEnabled(nil), func(c *fiber.Ctx) error {
		nextCalled = true
		return c.SendStatus(http.StatusOK)
	})

	req := httptest.NewRequest(http.MethodGet, "/test", http.NoBody)
	resp, err := app.Test(req)
	require.NoError(t, err)
	defer resp.Body.Close()

	assert.True(t, nextCalled, "WhenEnabled(nil) must call c.Next() (passthrough)")
	assert.Equal(t, http.StatusOK, resp.StatusCode)
}

func TestWhenEnabled_NonNilMiddleware_InvokesHandler(t *testing.T) {
	t.Parallel()

	app := fiber.New()

	var middlewareCalled bool

	handler := func(c *fiber.Ctx) error {
		middlewareCalled = true
		return c.Next()
	}

	app.Get("/test", WhenEnabled(handler), func(c *fiber.Ctx) error {
		return c.SendStatus(http.StatusOK)
	})

	req := httptest.NewRequest(http.MethodGet, "/test", http.NoBody)
	resp, err := app.Test(req)
	require.NoError(t, err)
	defer resp.Body.Close()

	assert.True(t, middlewareCalled, "WhenEnabled(handler) must invoke the given handler")
	assert.Equal(t, http.StatusOK, resp.StatusCode)
}

func TestParseSchemes(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name     string
		input    string
		expected []string
	}{
		{
			name:     "single https",
			input:    "https",
			expected: []string{"https"},
		},
		{
			name:     "single http",
			input:    "http",
			expected: []string{"http"},
		},
		{
			name:     "both schemes",
			input:    "http,https",
			expected: []string{"http", "https"},
		},
		{
			name:     "with whitespace",
			input:    " http , https ",
			expected: []string{"http", "https"},
		},
		{
			name:     "uppercase normalized",
			input:    "HTTP,HTTPS",
			expected: []string{"http", "https"},
		},
		{
			name:     "invalid values dropped",
			input:    "ftp,http,ws",
			expected: []string{"http"},
		},
		{
			name:     "all invalid falls back to https",
			input:    "ftp,ws",
			expected: []string{"https"},
		},
		{
			name:     "empty string falls back to https",
			input:    "",
			expected: []string{"https"},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			result := parseSchemes(tt.input)
			assert.Equal(t, tt.expected, result)
		})
	}
}
