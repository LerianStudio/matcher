// Copyright 2025 Lerian Studio. All rights reserved.
// Use of this source code is governed by an Elastic License 2.0
// that can be found in the LICENSE.md file.

//go:build unit

package bootstrap

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"sync"
	"testing"
	"time"

	"github.com/gofiber/fiber/v2"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/LerianStudio/matcher/internal/auth"
)

type testFiberStorage struct {
	mu   sync.Mutex
	data map[string][]byte
}

func (storage *testFiberStorage) Get(key string) ([]byte, error) {
	storage.mu.Lock()
	defer storage.mu.Unlock()

	if storage.data == nil {
		return nil, nil
	}

	value, ok := storage.data[key]
	if !ok {
		return nil, nil
	}

	cloned := make([]byte, len(value))
	copy(cloned, value)

	return cloned, nil
}

func (storage *testFiberStorage) Set(key string, val []byte, _ time.Duration) error {
	storage.mu.Lock()
	defer storage.mu.Unlock()

	if storage.data == nil {
		storage.data = make(map[string][]byte)
	}

	cloned := make([]byte, len(val))
	copy(cloned, val)
	storage.data[key] = cloned

	return nil
}

func (storage *testFiberStorage) Delete(key string) error {
	storage.mu.Lock()
	defer storage.mu.Unlock()

	delete(storage.data, key)

	return nil
}

func (storage *testFiberStorage) Reset() error {
	storage.mu.Lock()
	defer storage.mu.Unlock()

	storage.data = make(map[string][]byte)

	return nil
}

func (storage *testFiberStorage) Close() error {
	return nil
}

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

func TestNewDynamicDispatchRateLimiter_RuntimeMaxUpdate_InMemory(t *testing.T) {
	t.Parallel()

	activeCfg := &Config{RateLimit: RateLimitConfig{Enabled: true, DispatchMax: 1, DispatchExpirySec: 60}}

	app := fiber.New()
	app.Use(NewDynamicDispatchRateLimiter(func() *Config { return activeCfg }, nil))
	app.Post("/dispatch", func(c *fiber.Ctx) error {
		return c.SendStatus(fiber.StatusOK)
	})

	req1 := httptest.NewRequest(http.MethodPost, "/dispatch", http.NoBody)
	resp1, err := app.Test(req1)
	require.NoError(t, err)
	defer resp1.Body.Close()
	assert.Equal(t, http.StatusOK, resp1.StatusCode)

	req2 := httptest.NewRequest(http.MethodPost, "/dispatch", http.NoBody)
	resp2, err := app.Test(req2)
	require.NoError(t, err)
	defer resp2.Body.Close()
	assert.Equal(t, http.StatusTooManyRequests, resp2.StatusCode)

	activeCfg = &Config{RateLimit: RateLimitConfig{Enabled: true, DispatchMax: 3, DispatchExpirySec: 60}}

	req3 := httptest.NewRequest(http.MethodPost, "/dispatch", http.NoBody)
	resp3, err := app.Test(req3)
	require.NoError(t, err)
	defer resp3.Body.Close()
	assert.Equal(t, http.StatusOK, resp3.StatusCode)
	assert.Equal(t, "3", resp3.Header.Get("X-RateLimit-Limit"))
}

func TestNewDynamicDispatchRateLimiter_EmitsRateLimitHeaders(t *testing.T) {
	t.Parallel()

	activeCfg := &Config{RateLimit: RateLimitConfig{Enabled: true, DispatchMax: 1, DispatchExpirySec: 60}}

	app := fiber.New()
	app.Use(NewDynamicDispatchRateLimiter(func() *Config { return activeCfg }, nil))
	app.Post("/dispatch", func(c *fiber.Ctx) error {
		return c.SendStatus(fiber.StatusOK)
	})

	resp1, err := app.Test(httptest.NewRequest(http.MethodPost, "/dispatch", http.NoBody))
	require.NoError(t, err)
	defer resp1.Body.Close()
	assert.Equal(t, http.StatusOK, resp1.StatusCode)
	assert.Equal(t, "1", resp1.Header.Get("X-RateLimit-Limit"))
	assert.Equal(t, "0", resp1.Header.Get("X-RateLimit-Remaining"))
	assert.NotEmpty(t, resp1.Header.Get("X-RateLimit-Reset"))

	resp2, err := app.Test(httptest.NewRequest(http.MethodPost, "/dispatch", http.NoBody))
	require.NoError(t, err)
	defer resp2.Body.Close()
	assert.Equal(t, http.StatusTooManyRequests, resp2.StatusCode)
	assert.Equal(t, "1", resp2.Header.Get("X-RateLimit-Limit"))
	assert.Equal(t, "0", resp2.Header.Get("X-RateLimit-Remaining"))
	assert.NotEmpty(t, resp2.Header.Get("X-RateLimit-Reset"))
	assert.Equal(t, "60", resp2.Header.Get("Retry-After"))
}

func TestNewDynamicDispatchRateLimiter_DistributedHandlerRebuildsOnConfigChange(t *testing.T) {
	t.Parallel()

	activeCfg := &Config{RateLimit: RateLimitConfig{Enabled: true, DispatchMax: 1, DispatchExpirySec: 60}}
	storage := &testFiberStorage{}

	app := fiber.New()
	app.Use(NewDynamicDispatchRateLimiter(func() *Config { return activeCfg }, storage))
	app.Post("/dispatch", func(c *fiber.Ctx) error {
		return c.SendStatus(fiber.StatusOK)
	})

	req1 := httptest.NewRequest(http.MethodPost, "/dispatch", http.NoBody)
	resp1, err := app.Test(req1)
	require.NoError(t, err)
	defer resp1.Body.Close()
	assert.Equal(t, http.StatusOK, resp1.StatusCode)
	assert.Equal(t, "1", resp1.Header.Get("X-RateLimit-Limit"))

	activeCfg = &Config{RateLimit: RateLimitConfig{Enabled: true, DispatchMax: 2, DispatchExpirySec: 60}}

	req2 := httptest.NewRequest(http.MethodPost, "/dispatch", http.NoBody)
	resp2, err := app.Test(req2)
	require.NoError(t, err)
	defer resp2.Body.Close()
	assert.Equal(t, http.StatusOK, resp2.StatusCode)
	assert.Equal(t, "2", resp2.Header.Get("X-RateLimit-Limit"))
}

func TestNewDynamicExportRateLimiter_RuntimeMaxUpdate(t *testing.T) {
	t.Parallel()

	activeCfg := &Config{RateLimit: RateLimitConfig{Enabled: true, ExportMax: 1, ExportExpirySec: 60}}

	app := fiber.New()
	app.Use(NewDynamicExportRateLimiter(func() *Config { return activeCfg }, nil))
	app.Get("/export", func(c *fiber.Ctx) error {
		return c.SendStatus(fiber.StatusOK)
	})

	req1 := httptest.NewRequest(http.MethodGet, "/export", http.NoBody)
	resp1, err := app.Test(req1)
	require.NoError(t, err)
	defer resp1.Body.Close()
	assert.Equal(t, http.StatusOK, resp1.StatusCode)

	req2 := httptest.NewRequest(http.MethodGet, "/export", http.NoBody)
	resp2, err := app.Test(req2)
	require.NoError(t, err)
	defer resp2.Body.Close()
	assert.Equal(t, http.StatusTooManyRequests, resp2.StatusCode)

	activeCfg = &Config{RateLimit: RateLimitConfig{Enabled: true, ExportMax: 3, ExportExpirySec: 60}}

	req3 := httptest.NewRequest(http.MethodGet, "/export", http.NoBody)
	resp3, err := app.Test(req3)
	require.NoError(t, err)
	defer resp3.Body.Close()
	assert.Equal(t, http.StatusOK, resp3.StatusCode)
	assert.Equal(t, "3", resp3.Header.Get("X-RateLimit-Limit"))
}

func TestNewDynamicRateLimiter_RuntimeToggleEnabled(t *testing.T) {
	t.Parallel()

	activeCfg := &Config{RateLimit: RateLimitConfig{Enabled: false, Max: 1, ExpirySec: 60}}

	app := fiber.New()
	app.Use(NewDynamicRateLimiter(func() *Config { return activeCfg }, nil))
	app.Get("/api/test", func(c *fiber.Ctx) error {
		return c.SendStatus(fiber.StatusOK)
	})

	req1 := httptest.NewRequest(http.MethodGet, "/api/test", http.NoBody)
	resp1, err := app.Test(req1)
	require.NoError(t, err)
	defer resp1.Body.Close()
	assert.Equal(t, http.StatusOK, resp1.StatusCode)

	req2 := httptest.NewRequest(http.MethodGet, "/api/test", http.NoBody)
	resp2, err := app.Test(req2)
	require.NoError(t, err)
	defer resp2.Body.Close()
	assert.Equal(t, http.StatusOK, resp2.StatusCode)

	activeCfg = &Config{RateLimit: RateLimitConfig{Enabled: true, Max: 1, ExpirySec: 60}}

	req3 := httptest.NewRequest(http.MethodGet, "/api/test", http.NoBody)
	resp3, err := app.Test(req3)
	require.NoError(t, err)
	defer resp3.Body.Close()
	assert.Equal(t, http.StatusOK, resp3.StatusCode)

	req4 := httptest.NewRequest(http.MethodGet, "/api/test", http.NoBody)
	resp4, err := app.Test(req4)
	require.NoError(t, err)
	defer resp4.Body.Close()
	assert.Equal(t, http.StatusTooManyRequests, resp4.StatusCode)
}
