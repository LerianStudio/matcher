// Copyright 2025 Lerian Studio. All rights reserved.
// Use of this source code is governed by an Elastic License 2.0
// that can be found in the LICENSE.md file.

//go:build unit

package bootstrap

import (
	"net/http/httptest"
	"sync"
	"testing"

	"github.com/gofiber/fiber/v2"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestNewDynamicRateLimiter_NilConfigGetter_ReturnsPassthrough(t *testing.T) {
	t.Parallel()

	handler := NewDynamicRateLimiter(nil, nil)
	require.NotNil(t, handler)

	app := fiber.New()
	app.Get("/test", handler, func(c *fiber.Ctx) error {
		return c.SendString("ok")
	})

	resp, err := app.Test(httptest.NewRequest("GET", "/test", nil))
	require.NoError(t, err)
	assert.Equal(t, fiber.StatusOK, resp.StatusCode)
}

func TestNewDynamicRateLimiter_NilInitialConfig_ReturnsPassthrough(t *testing.T) {
	t.Parallel()

	handler := NewDynamicRateLimiter(func() *Config { return nil }, nil)
	require.NotNil(t, handler)

	app := fiber.New()
	app.Get("/test", handler, func(c *fiber.Ctx) error {
		return c.SendString("ok")
	})

	resp, err := app.Test(httptest.NewRequest("GET", "/test", nil))
	require.NoError(t, err)
	assert.Equal(t, fiber.StatusOK, resp.StatusCode)
}

func TestNewDynamicRateLimiter_ValidConfig_EnforcesLimit(t *testing.T) {
	t.Parallel()

	cfg := defaultConfig()
	cfg.RateLimit.Enabled = true
	cfg.RateLimit.Max = 2
	cfg.RateLimit.ExpirySec = 60

	handler := NewDynamicRateLimiter(func() *Config { return cfg }, nil)
	require.NotNil(t, handler)

	app := fiber.New()
	app.Get("/test", handler, func(c *fiber.Ctx) error {
		return c.SendString("ok")
	})

	// First two requests should pass.
	for range 2 {
		resp, err := app.Test(httptest.NewRequest("GET", "/test", nil))
		require.NoError(t, err)
		assert.Equal(t, fiber.StatusOK, resp.StatusCode)
	}

	// Third request should be rate-limited.
	resp, err := app.Test(httptest.NewRequest("GET", "/test", nil))
	require.NoError(t, err)
	assert.Equal(t, fiber.StatusTooManyRequests, resp.StatusCode)
}

func TestNewDynamicExportRateLimiter_NilConfigGetter_ReturnsPassthrough(t *testing.T) {
	t.Parallel()

	handler := NewDynamicExportRateLimiter(nil, nil)
	require.NotNil(t, handler)

	app := fiber.New()
	app.Get("/export", handler, func(c *fiber.Ctx) error {
		return c.SendString("ok")
	})

	resp, err := app.Test(httptest.NewRequest("GET", "/export", nil))
	require.NoError(t, err)
	assert.Equal(t, fiber.StatusOK, resp.StatusCode)
}

func TestNewDynamicDispatchRateLimiter_NilConfigGetter_ReturnsPassthrough(t *testing.T) {
	t.Parallel()

	handler := NewDynamicDispatchRateLimiter(nil, nil)
	require.NotNil(t, handler)

	app := fiber.New()
	app.Get("/dispatch", handler, func(c *fiber.Ctx) error {
		return c.SendString("ok")
	})

	resp, err := app.Test(httptest.NewRequest("GET", "/dispatch", nil))
	require.NoError(t, err)
	assert.Equal(t, fiber.StatusOK, resp.StatusCode)
}

func TestNewDynamicRateLimiter_RateLimitDisabled_Passthrough(t *testing.T) {
	t.Parallel()

	cfg := defaultConfig()
	cfg.RateLimit.Enabled = false

	handler := NewDynamicRateLimiter(func() *Config { return cfg }, nil)
	require.NotNil(t, handler)

	app := fiber.New()
	app.Get("/test", handler, func(c *fiber.Ctx) error {
		return c.SendString("ok")
	})

	// Should pass even though max=100 (default) because rate limiting is disabled.
	resp, err := app.Test(httptest.NewRequest("GET", "/test", nil))
	require.NoError(t, err)
	assert.Equal(t, fiber.StatusOK, resp.StatusCode)
}

// TestDynamicRateLimiter_ConcurrentRebuild hammers the rate limiter with concurrent
// requests while changing config values. This exercises the double-checked locking
// pattern in buildDistributedDynamicHandler / buildInMemoryDynamicHandler.
func TestDynamicRateLimiter_ConcurrentRebuild(t *testing.T) {
	t.Parallel()

	var mu sync.Mutex

	cfg := defaultConfig()
	cfg.RateLimit.Enabled = true
	cfg.RateLimit.Max = 1000
	cfg.RateLimit.ExpirySec = 60

	handler := NewDynamicRateLimiter(func() *Config {
		mu.Lock()
		defer mu.Unlock()

		return cfg
	}, nil)
	require.NotNil(t, handler)

	app := fiber.New()
	app.Get("/test", handler, func(c *fiber.Ctx) error {
		return c.SendString("ok")
	})

	const goroutines = 10
	const requestsPerGoroutine = 20

	var wg sync.WaitGroup

	wg.Add(goroutines)

	for range goroutines {
		go func() {
			defer wg.Done()

			for range requestsPerGoroutine {
				resp, err := app.Test(httptest.NewRequest("GET", "/test", nil))
				if err != nil {
					continue
				}
				resp.Body.Close()
			}
		}()
	}

	// Concurrently change config max to trigger handler rebuild.
	go func() {
		for i := range 5 {
			mu.Lock()
			newCfg := *cfg
			newCfg.RateLimit.Max = 500 + i*100
			cfg = &newCfg
			mu.Unlock()
		}
	}()

	wg.Wait()

	// If we get here without panics or data races, the test passes.
	// The goal is to detect race conditions under concurrent access.
}
