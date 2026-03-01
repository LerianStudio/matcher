//go:build unit

package bootstrap

import (
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/gofiber/fiber/v2"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	authMiddleware "github.com/LerianStudio/lib-auth/v2/auth/middleware"
	libLog "github.com/LerianStudio/lib-uncommons/v2/uncommons/log"

	swagger "github.com/LerianStudio/matcher/docs/swagger"
	"github.com/LerianStudio/matcher/internal/auth"
)

func TestRegisterRoutes_NilTenantExtractor_ReturnsError(t *testing.T) {
	t.Parallel()

	app := fiber.New()
	cfg := &Config{App: AppConfig{EnvName: "test"}}
	client := authMiddleware.NewAuthClient("", false, nil)

	routes, err := RegisterRoutes(app, cfg, nil, &libLog.NopLogger{}, client, nil, nil, nil)
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
		"11111111-1111-1111-1111-111111111111",
		"default",
		"",
		"test",
	)
	require.NoError(t, err)

	routes, err := RegisterRoutes(nil, cfg, nil, &libLog.NopLogger{}, client, extractor, nil, nil)
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
		"11111111-1111-1111-1111-111111111111",
		"default",
		"",
		"test",
	)
	require.NoError(t, err)

	routes, err := RegisterRoutes(app, nil, nil, &libLog.NopLogger{}, client, extractor, nil, nil)
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
			&libLog.NopLogger{},
			client,
			extractor,
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
			&libLog.NopLogger{},
			client,
			extractor,
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
			&libLog.NopLogger{},
			client,
			extractor,
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
				&libLog.NopLogger{},
				client,
				extractor,
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
				&libLog.NopLogger{},
				client,
				extractor,
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
				&libLog.NopLogger{},
				client,
				extractor,
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
				&libLog.NopLogger{},
				client,
				extractor,
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
				&libLog.NopLogger{},
				client,
				extractor,
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
			"11111111-1111-1111-1111-111111111111",
			"default",
			"",
			"test",
		)
		require.NoError(t, err)

		_, err = RegisterRoutes(app, cfg, nil, &libLog.NopLogger{}, client, extractor, nil, nil)
		require.NoError(t, err)

		req := httptest.NewRequest(http.MethodGet, "/swagger/index.html", http.NoBody)
		resp, err := app.Test(req)
		require.NoError(t, err)
		defer resp.Body.Close()

		assert.Equal(t, http.StatusNotFound, resp.StatusCode)
	})
}

func TestRegisterRoutes_HealthEndpoints(t *testing.T) {
	t.Parallel()

	app := fiber.New()
	cfg := &Config{App: AppConfig{EnvName: "development"}}
	client := authMiddleware.NewAuthClient("", false, nil)
	extractor, err := auth.NewTenantExtractor(
		false,
		"11111111-1111-1111-1111-111111111111",
		"default",
		"",
		"test",
	)
	require.NoError(t, err)

	_, err = RegisterRoutes(app, cfg, nil, &libLog.NopLogger{}, client, extractor, nil, nil)
	require.NoError(t, err)

	t.Run("health endpoint is accessible", func(t *testing.T) {
		req := httptest.NewRequest(http.MethodGet, "/health", http.NoBody)
		resp, err := app.Test(req)
		require.NoError(t, err)
		defer resp.Body.Close()

		assert.Equal(t, http.StatusOK, resp.StatusCode)
	})

	t.Run("ready endpoint is accessible", func(t *testing.T) {
		req := httptest.NewRequest(http.MethodGet, "/ready", http.NoBody)
		resp, err := app.Test(req)
		require.NoError(t, err)
		defer resp.Body.Close()

		assert.Equal(t, http.StatusServiceUnavailable, resp.StatusCode)
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
			&libLog.NopLogger{},
			client,
			extractor,
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

func TestBuildProtectedMiddlewares_IdempotencyOnly(t *testing.T) {
	t.Parallel()

	idempotency := func(c *fiber.Ctx) error { return c.Next() }

	result := buildProtectedMiddlewares(idempotency, nil, nil)

	assert.Len(t, result, 1)
}

func TestBuildProtectedMiddlewares_WithRateLimiter(t *testing.T) {
	t.Parallel()

	idempotency := func(c *fiber.Ctx) error { return c.Next() }
	rateLimiter := func(c *fiber.Ctx) error { return c.Next() }

	result := buildProtectedMiddlewares(idempotency, rateLimiter, nil)

	assert.Len(t, result, 2)
}

func TestBuildProtectedMiddlewares_WithTenantDB(t *testing.T) {
	t.Parallel()

	idempotency := func(c *fiber.Ctx) error { return c.Next() }
	tenantDB := func(c *fiber.Ctx) error { return c.Next() }

	result := buildProtectedMiddlewares(idempotency, nil, tenantDB)

	// Should have tenantDB + idempotency
	assert.Len(t, result, 2)
}

func TestBuildProtectedMiddlewares_AllMiddlewares(t *testing.T) {
	t.Parallel()

	idempotency := func(c *fiber.Ctx) error { return c.Next() }
	rateLimiter := func(c *fiber.Ctx) error { return c.Next() }
	tenantDB := func(c *fiber.Ctx) error { return c.Next() }

	result := buildProtectedMiddlewares(idempotency, rateLimiter, tenantDB)

	// Should have tenantDB + idempotency + rateLimiter
	assert.Len(t, result, 3)
}
