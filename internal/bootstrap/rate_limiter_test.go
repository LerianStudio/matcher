//go:build unit

package bootstrap

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/gofiber/fiber/v2"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/LerianStudio/matcher/internal/auth"
)

func TestNewDispatchRateLimiter_DisabledMode(t *testing.T) {
	t.Parallel()

	cfg := &Config{
		RateLimit: RateLimitConfig{
			Enabled:           false,
			DispatchMax:       50,
			DispatchExpirySec: 60,
		},
	}

	app := fiber.New()
	app.Use(NewDispatchRateLimiter(cfg, nil))
	app.Post("/dispatch", func(c *fiber.Ctx) error {
		return c.SendStatus(fiber.StatusOK)
	})

	req := httptest.NewRequest(http.MethodPost, "/dispatch", http.NoBody)
	resp, err := app.Test(req)
	require.NoError(t, err)

	defer resp.Body.Close()

	assert.Equal(t, http.StatusOK, resp.StatusCode)
}

func TestNewDispatchRateLimiter_KeyGenerator_WithUserID(t *testing.T) {
	t.Parallel()

	cfg := &Config{
		RateLimit: RateLimitConfig{
			Enabled:           true,
			DispatchMax:       1,
			DispatchExpirySec: 60,
		},
	}

	app := fiber.New()
	app.Use(func(c *fiber.Ctx) error {
		ctx := context.WithValue(c.UserContext(), auth.UserIDKey, "user-123")
		c.SetUserContext(ctx)

		return c.Next()
	})
	app.Use(NewDispatchRateLimiter(cfg, nil))
	app.Post("/dispatch", func(c *fiber.Ctx) error {
		return c.SendStatus(fiber.StatusOK)
	})

	req := httptest.NewRequest(http.MethodPost, "/dispatch", http.NoBody)
	resp, err := app.Test(req)
	require.NoError(t, err)

	defer resp.Body.Close()

	assert.Equal(t, http.StatusOK, resp.StatusCode)

	req2 := httptest.NewRequest(http.MethodPost, "/dispatch", http.NoBody)
	resp2, err := app.Test(req2)
	require.NoError(t, err)

	defer resp2.Body.Close()

	assert.Equal(t, http.StatusTooManyRequests, resp2.StatusCode)
}

func TestNewDispatchRateLimiter_KeyGenerator_WithTenantID(t *testing.T) {
	t.Parallel()

	cfg := &Config{
		RateLimit: RateLimitConfig{
			Enabled:           true,
			DispatchMax:       1,
			DispatchExpirySec: 60,
		},
	}

	app := fiber.New()
	app.Use(func(c *fiber.Ctx) error {
		ctx := context.WithValue(c.UserContext(), auth.TenantIDKey, "tenant-456")
		c.SetUserContext(ctx)

		return c.Next()
	})
	app.Use(NewDispatchRateLimiter(cfg, nil))
	app.Post("/dispatch", func(c *fiber.Ctx) error {
		return c.SendStatus(fiber.StatusOK)
	})

	req := httptest.NewRequest(http.MethodPost, "/dispatch", http.NoBody)
	resp, err := app.Test(req)
	require.NoError(t, err)

	defer resp.Body.Close()

	assert.Equal(t, http.StatusOK, resp.StatusCode)

	req2 := httptest.NewRequest(http.MethodPost, "/dispatch", http.NoBody)
	resp2, err := app.Test(req2)
	require.NoError(t, err)

	defer resp2.Body.Close()

	assert.Equal(t, http.StatusTooManyRequests, resp2.StatusCode)
}

func TestNewDispatchRateLimiter_KeyGenerator_WithoutAuthContext(t *testing.T) {
	t.Parallel()

	cfg := &Config{
		RateLimit: RateLimitConfig{
			Enabled:           true,
			DispatchMax:       1,
			DispatchExpirySec: 60,
		},
	}

	app := fiber.New()
	app.Use(NewDispatchRateLimiter(cfg, nil))
	app.Post("/dispatch", func(c *fiber.Ctx) error {
		return c.SendStatus(fiber.StatusOK)
	})

	req := httptest.NewRequest(http.MethodPost, "/dispatch", http.NoBody)
	resp, err := app.Test(req)
	require.NoError(t, err)

	defer resp.Body.Close()

	assert.Equal(t, http.StatusOK, resp.StatusCode)

	req2 := httptest.NewRequest(http.MethodPost, "/dispatch", http.NoBody)
	resp2, err := app.Test(req2)
	require.NoError(t, err)

	defer resp2.Body.Close()

	assert.Equal(t, http.StatusTooManyRequests, resp2.StatusCode)
}

func TestNewDispatchRateLimiter_LimitReached(t *testing.T) {
	t.Parallel()

	cfg := &Config{
		RateLimit: RateLimitConfig{
			Enabled:           true,
			DispatchMax:       1,
			DispatchExpirySec: 120,
		},
	}

	app := fiber.New()
	app.Use(NewDispatchRateLimiter(cfg, nil))
	app.Post("/dispatch", func(c *fiber.Ctx) error {
		return c.SendStatus(fiber.StatusOK)
	})

	req := httptest.NewRequest(http.MethodPost, "/dispatch", http.NoBody)
	resp, err := app.Test(req)
	require.NoError(t, err)

	defer resp.Body.Close()

	assert.Equal(t, http.StatusOK, resp.StatusCode)

	req2 := httptest.NewRequest(http.MethodPost, "/dispatch", http.NoBody)
	resp2, err := app.Test(req2)
	require.NoError(t, err)

	defer resp2.Body.Close()

	assert.Equal(t, http.StatusTooManyRequests, resp2.StatusCode)
	assert.Equal(t, "120", resp2.Header.Get("Retry-After"))

	var body map[string]any
	require.NoError(t, json.NewDecoder(resp2.Body).Decode(&body))
	assert.Equal(t, float64(429), body["code"])
	assert.Equal(t, "dispatch_rate_limit_exceeded", body["title"])
	assert.Equal(t, "too many dispatch requests, please try again later", body["message"])
}

func TestNewRateLimiter_KeyGenerator_WithUserID(t *testing.T) {
	t.Parallel()

	cfg := &Config{
		RateLimit: RateLimitConfig{
			Enabled:   true,
			Max:       1,
			ExpirySec: 60,
		},
	}

	app := fiber.New()
	app.Use(func(c *fiber.Ctx) error {
		ctx := context.WithValue(c.UserContext(), auth.UserIDKey, "user-abc")
		c.SetUserContext(ctx)

		return c.Next()
	})
	app.Use(NewRateLimiter(cfg, nil))
	app.Get("/api/test", func(c *fiber.Ctx) error {
		return c.SendStatus(fiber.StatusOK)
	})

	req := httptest.NewRequest(http.MethodGet, "/api/test", http.NoBody)
	resp, err := app.Test(req)
	require.NoError(t, err)
	defer resp.Body.Close()
	assert.Equal(t, http.StatusOK, resp.StatusCode)

	req2 := httptest.NewRequest(http.MethodGet, "/api/test", http.NoBody)
	resp2, err := app.Test(req2)
	require.NoError(t, err)
	defer resp2.Body.Close()
	assert.Equal(t, http.StatusTooManyRequests, resp2.StatusCode)
}

func TestNewRateLimiter_KeyGenerator_WithTenantID(t *testing.T) {
	t.Parallel()

	cfg := &Config{
		RateLimit: RateLimitConfig{
			Enabled:   true,
			Max:       1,
			ExpirySec: 60,
		},
	}

	app := fiber.New()
	app.Use(func(c *fiber.Ctx) error {
		ctx := context.WithValue(c.UserContext(), auth.TenantIDKey, "tenant-xyz")
		c.SetUserContext(ctx)

		return c.Next()
	})
	app.Use(NewRateLimiter(cfg, nil))
	app.Get("/api/test", func(c *fiber.Ctx) error {
		return c.SendStatus(fiber.StatusOK)
	})

	req := httptest.NewRequest(http.MethodGet, "/api/test", http.NoBody)
	resp, err := app.Test(req)
	require.NoError(t, err)
	defer resp.Body.Close()
	assert.Equal(t, http.StatusOK, resp.StatusCode)

	req2 := httptest.NewRequest(http.MethodGet, "/api/test", http.NoBody)
	resp2, err := app.Test(req2)
	require.NoError(t, err)
	defer resp2.Body.Close()
	assert.Equal(t, http.StatusTooManyRequests, resp2.StatusCode)
}

func TestNewExportRateLimiter_KeyGenerator_WithUserID(t *testing.T) {
	t.Parallel()

	cfg := &Config{
		RateLimit: RateLimitConfig{
			Enabled:         true,
			ExportMax:       1,
			ExportExpirySec: 60,
		},
	}

	app := fiber.New()
	app.Use(func(c *fiber.Ctx) error {
		ctx := context.WithValue(c.UserContext(), auth.UserIDKey, "user-export")
		c.SetUserContext(ctx)

		return c.Next()
	})
	app.Use(NewExportRateLimiter(cfg, nil))
	app.Get("/export", func(c *fiber.Ctx) error {
		return c.SendStatus(fiber.StatusOK)
	})

	req := httptest.NewRequest(http.MethodGet, "/export", http.NoBody)
	resp, err := app.Test(req)
	require.NoError(t, err)
	defer resp.Body.Close()
	assert.Equal(t, http.StatusOK, resp.StatusCode)

	req2 := httptest.NewRequest(http.MethodGet, "/export", http.NoBody)
	resp2, err := app.Test(req2)
	require.NoError(t, err)
	defer resp2.Body.Close()
	assert.Equal(t, http.StatusTooManyRequests, resp2.StatusCode)
}

func TestNewExportRateLimiter_KeyGenerator_WithTenantID(t *testing.T) {
	t.Parallel()

	cfg := &Config{
		RateLimit: RateLimitConfig{
			Enabled:         true,
			ExportMax:       1,
			ExportExpirySec: 60,
		},
	}

	app := fiber.New()
	app.Use(func(c *fiber.Ctx) error {
		ctx := context.WithValue(c.UserContext(), auth.TenantIDKey, "tenant-export")
		c.SetUserContext(ctx)

		return c.Next()
	})
	app.Use(NewExportRateLimiter(cfg, nil))
	app.Get("/export", func(c *fiber.Ctx) error {
		return c.SendStatus(fiber.StatusOK)
	})

	req := httptest.NewRequest(http.MethodGet, "/export", http.NoBody)
	resp, err := app.Test(req)
	require.NoError(t, err)
	defer resp.Body.Close()
	assert.Equal(t, http.StatusOK, resp.StatusCode)

	req2 := httptest.NewRequest(http.MethodGet, "/export", http.NoBody)
	resp2, err := app.Test(req2)
	require.NoError(t, err)
	defer resp2.Body.Close()
	assert.Equal(t, http.StatusTooManyRequests, resp2.StatusCode)
}

func TestNewDispatchRateLimiter_UserIDTakesPrecedenceOverTenantID(t *testing.T) {
	t.Parallel()

	cfg := &Config{
		RateLimit: RateLimitConfig{
			Enabled:           true,
			DispatchMax:       1,
			DispatchExpirySec: 60,
		},
	}

	app := fiber.New()
	app.Use(func(c *fiber.Ctx) error {
		ctx := context.WithValue(c.UserContext(), auth.TenantIDKey, "tenant-456")
		ctx = context.WithValue(ctx, auth.UserIDKey, "user-123")
		c.SetUserContext(ctx)

		return c.Next()
	})
	app.Use(NewDispatchRateLimiter(cfg, nil))
	app.Post("/dispatch", func(c *fiber.Ctx) error {
		return c.SendStatus(fiber.StatusOK)
	})

	req := httptest.NewRequest(http.MethodPost, "/dispatch", http.NoBody)
	resp, err := app.Test(req)
	require.NoError(t, err)

	defer resp.Body.Close()

	assert.Equal(t, http.StatusOK, resp.StatusCode)

	req2 := httptest.NewRequest(http.MethodPost, "/dispatch", http.NoBody)
	resp2, err := app.Test(req2)
	require.NoError(t, err)

	defer resp2.Body.Close()

	assert.Equal(t, http.StatusTooManyRequests, resp2.StatusCode)
}
