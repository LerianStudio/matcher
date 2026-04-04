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

	"github.com/gofiber/fiber/v2"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/LerianStudio/lib-commons/v4/commons/net/http/ratelimit"
	libRedis "github.com/LerianStudio/lib-commons/v4/commons/redis"
	spdomain "github.com/LerianStudio/lib-commons/v4/commons/systemplane/domain"
	spservice "github.com/LerianStudio/lib-commons/v4/commons/systemplane/service"

	"github.com/LerianStudio/matcher/internal/auth"
)

// nilRLGetter is a rate limiter getter that always returns nil.
// A nil *ratelimit.RateLimiter produces pass-through handlers.
func nilRLGetter() *ratelimit.RateLimiter { return nil }

func TestRateLimitIdentityFunc_WithUserID(t *testing.T) {
	t.Parallel()

	app := fiber.New()
	identityFn := rateLimitIdentityFunc()

	var identity string

	app.Use(func(c *fiber.Ctx) error {
		ctx := context.WithValue(c.UserContext(), auth.UserIDKey, "user-123")
		ctx = context.WithValue(ctx, auth.TenantIDKey, "tenant-456")
		c.SetUserContext(ctx)

		return c.Next()
	})
	app.Get("/test", func(c *fiber.Ctx) error {
		identity = identityFn(c)

		return c.SendStatus(fiber.StatusOK)
	})

	req := httptest.NewRequest(http.MethodGet, "/test", http.NoBody)
	resp, err := app.Test(req)
	require.NoError(t, err)

	defer resp.Body.Close()

	assert.Equal(t, http.StatusOK, resp.StatusCode)
	assert.Equal(t, "user:user-123", identity, "UserID should take precedence over TenantID")
}

func TestRateLimitIdentityFunc_WithTenantIDOnly(t *testing.T) {
	t.Parallel()

	app := fiber.New()
	identityFn := rateLimitIdentityFunc()

	var identity string

	app.Use(func(c *fiber.Ctx) error {
		ctx := context.WithValue(c.UserContext(), auth.TenantIDKey, "tenant-456")
		c.SetUserContext(ctx)

		return c.Next()
	})
	app.Get("/test", func(c *fiber.Ctx) error {
		identity = identityFn(c)

		return c.SendStatus(fiber.StatusOK)
	})

	req := httptest.NewRequest(http.MethodGet, "/test", http.NoBody)
	resp, err := app.Test(req)
	require.NoError(t, err)

	defer resp.Body.Close()

	assert.Equal(t, http.StatusOK, resp.StatusCode)
	assert.Contains(t, identity, "tenant:tenant-456#ip:")
}

func TestRateLimitIdentityFunc_WithoutAuthContext(t *testing.T) {
	t.Parallel()

	app := fiber.New()
	identityFn := rateLimitIdentityFunc()

	var identity string

	app.Get("/test", func(c *fiber.Ctx) error {
		identity = identityFn(c)

		return c.SendStatus(fiber.StatusOK)
	})

	req := httptest.NewRequest(http.MethodGet, "/test", http.NoBody)
	resp, err := app.Test(req)
	require.NoError(t, err)

	defer resp.Body.Close()

	assert.Equal(t, http.StatusOK, resp.StatusCode)
	assert.Contains(t, identity, "ip:")
}

func TestNewGlobalRateLimit_NilRateLimiter_ReturnsPassthrough(t *testing.T) {
	t.Parallel()

	cfg := &Config{RateLimit: RateLimitConfig{Enabled: true, Max: 1, ExpirySec: 60}}
	handler := NewGlobalRateLimit(nilRLGetter, cfg, nil, nil)
	require.NotNil(t, handler)

	app := fiber.New()
	app.Get("/test", handler, func(c *fiber.Ctx) error {
		return c.SendString("ok")
	})

	resp, err := app.Test(httptest.NewRequest(http.MethodGet, "/test", http.NoBody))
	require.NoError(t, err)

	defer resp.Body.Close()

	// nil RateLimiter produces pass-through — all requests succeed.
	assert.Equal(t, http.StatusOK, resp.StatusCode)
}

func TestNewGlobalRateLimit_DisabledConfig_ReturnsPassthrough(t *testing.T) {
	t.Parallel()

	cfg := &Config{RateLimit: RateLimitConfig{Enabled: false, Max: 1, ExpirySec: 60}}
	handler := NewGlobalRateLimit(nilRLGetter, cfg, nil, nil)
	require.NotNil(t, handler)

	app := fiber.New()
	app.Get("/test", handler, func(c *fiber.Ctx) error {
		return c.SendString("ok")
	})

	resp, err := app.Test(httptest.NewRequest(http.MethodGet, "/test", http.NoBody))
	require.NoError(t, err)

	defer resp.Body.Close()

	assert.Equal(t, http.StatusOK, resp.StatusCode)
}

func TestNewExportRateLimit_NilRateLimiter_ReturnsPassthrough(t *testing.T) {
	t.Parallel()

	cfg := &Config{RateLimit: RateLimitConfig{Enabled: true, ExportMax: 1, ExportExpirySec: 60}}
	handler := NewExportRateLimit(nilRLGetter, cfg, nil, nil)
	require.NotNil(t, handler)

	app := fiber.New()
	app.Get("/export", handler, func(c *fiber.Ctx) error {
		return c.SendString("ok")
	})

	resp, err := app.Test(httptest.NewRequest(http.MethodGet, "/export", http.NoBody))
	require.NoError(t, err)

	defer resp.Body.Close()

	assert.Equal(t, http.StatusOK, resp.StatusCode)
}

func TestNewDispatchRateLimit_NilRateLimiter_ReturnsPassthrough(t *testing.T) {
	t.Parallel()

	cfg := &Config{RateLimit: RateLimitConfig{Enabled: true, DispatchMax: 1, DispatchExpirySec: 60}}
	handler := NewDispatchRateLimit(nilRLGetter, cfg, nil, nil)
	require.NotNil(t, handler)

	app := fiber.New()
	app.Post("/dispatch", handler, func(c *fiber.Ctx) error {
		return c.SendString("ok")
	})

	resp, err := app.Test(httptest.NewRequest(http.MethodPost, "/dispatch", http.NoBody))
	require.NoError(t, err)

	defer resp.Body.Close()

	assert.Equal(t, http.StatusOK, resp.StatusCode)
}

func TestNewGlobalRateLimit_DynamicDisabled_ReturnsPassthrough(t *testing.T) {
	t.Parallel()

	activeCfg := &Config{RateLimit: RateLimitConfig{Enabled: false, Max: 1, ExpirySec: 60}}

	handler := NewGlobalRateLimit(nilRLGetter, activeCfg, func() *Config { return activeCfg }, nil)
	require.NotNil(t, handler)

	app := fiber.New()
	app.Get("/test", handler, func(c *fiber.Ctx) error {
		return c.SendString("ok")
	})

	// Multiple requests should all pass when disabled.
	for range 3 {
		resp, err := app.Test(httptest.NewRequest(http.MethodGet, "/test", http.NoBody))
		require.NoError(t, err)

		defer resp.Body.Close()

		assert.Equal(t, http.StatusOK, resp.StatusCode)
	}
}

func TestNewGlobalRateLimit_DynamicNilConfig_ReturnsPassthrough(t *testing.T) {
	t.Parallel()

	handler := NewGlobalRateLimit(nilRLGetter, nil, func() *Config { return nil }, nil)
	require.NotNil(t, handler)

	app := fiber.New()
	app.Get("/test", handler, func(c *fiber.Ctx) error {
		return c.SendString("ok")
	})

	resp, err := app.Test(httptest.NewRequest(http.MethodGet, "/test", http.NoBody))
	require.NoError(t, err)

	defer resp.Body.Close()

	assert.Equal(t, http.StatusOK, resp.StatusCode)
}

func TestNewGlobalRateLimit_SettingsResolverCanDisableBeforeLimiterLookup(t *testing.T) {
	t.Parallel()

	baseCfg := &Config{RateLimit: RateLimitConfig{Enabled: true, Max: 10, ExpirySec: 60}}
	settingsResolver := newRuntimeSettingsResolver(func() spservice.Manager {
		return &runtimeSettingsManagerStub{
			getSettings: func(context.Context, spservice.Subject) (spservice.ResolvedSet, error) {
				return spservice.ResolvedSet{
					Values: map[string]spdomain.EffectiveValue{
						"rate_limit.enabled": {Key: "rate_limit.enabled", Value: false, Source: "tenant-override"},
					},
				}, nil
			},
		}
	})

	handler := NewGlobalRateLimit(func() *ratelimit.RateLimiter {
		panic("rate limiter should not be resolved when settings disable the route")
	}, baseCfg, func() *Config { return baseCfg }, settingsResolver)
	require.NotNil(t, handler)

	app := fiber.New()
	app.Get("/test", func(c *fiber.Ctx) error {
		ctx := context.WithValue(c.UserContext(), auth.TenantIDKey, "tenant-123")
		c.SetUserContext(ctx)
		return c.Next()
	}, handler, func(c *fiber.Ctx) error {
		return c.SendString("ok")
	})

	resp, err := app.Test(httptest.NewRequest(http.MethodGet, "/test", http.NoBody))
	require.NoError(t, err)
	defer resp.Body.Close()

	assert.Equal(t, http.StatusOK, resp.StatusCode)
}

func TestNewExportRateLimit_SettingsResolverCanDisableBeforeLimiterLookup(t *testing.T) {
	t.Parallel()

	baseCfg := &Config{RateLimit: RateLimitConfig{Enabled: true, ExportMax: 10, ExportExpirySec: 60}}
	settingsResolver := newRuntimeSettingsResolver(func() spservice.Manager {
		return &runtimeSettingsManagerStub{
			getSettings: func(context.Context, spservice.Subject) (spservice.ResolvedSet, error) {
				return spservice.ResolvedSet{
					Values: map[string]spdomain.EffectiveValue{
						"rate_limit.export_max": {Key: "rate_limit.export_max", Value: 0, Source: "tenant-override"},
					},
				}, nil
			},
		}
	})

	handler := NewExportRateLimit(func() *ratelimit.RateLimiter {
		panic("rate limiter should not be resolved when export settings disable the route")
	}, baseCfg, func() *Config { return baseCfg }, settingsResolver)
	require.NotNil(t, handler)

	app := fiber.New()
	app.Get("/export", func(c *fiber.Ctx) error {
		ctx := context.WithValue(c.UserContext(), auth.TenantIDKey, "tenant-123")
		c.SetUserContext(ctx)
		return c.Next()
	}, handler, func(c *fiber.Ctx) error {
		return c.SendString("ok")
	})

	resp, err := app.Test(httptest.NewRequest(http.MethodGet, "/export", http.NoBody))
	require.NoError(t, err)
	defer resp.Body.Close()

	assert.Equal(t, http.StatusOK, resp.StatusCode)
}

func TestNewDispatchRateLimit_SettingsResolverCanDisableBeforeLimiterLookup(t *testing.T) {
	t.Parallel()

	baseCfg := &Config{RateLimit: RateLimitConfig{Enabled: true, DispatchMax: 10, DispatchExpirySec: 60}}
	settingsResolver := newRuntimeSettingsResolver(func() spservice.Manager {
		return &runtimeSettingsManagerStub{
			getSettings: func(context.Context, spservice.Subject) (spservice.ResolvedSet, error) {
				return spservice.ResolvedSet{
					Values: map[string]spdomain.EffectiveValue{
						"rate_limit.dispatch_max": {Key: "rate_limit.dispatch_max", Value: 0, Source: "tenant-override"},
					},
				}, nil
			},
		}
	})

	handler := NewDispatchRateLimit(func() *ratelimit.RateLimiter {
		panic("rate limiter should not be resolved when dispatch settings disable the route")
	}, baseCfg, func() *Config { return baseCfg }, settingsResolver)
	require.NotNil(t, handler)

	app := fiber.New()
	app.Post("/dispatch", func(c *fiber.Ctx) error {
		ctx := context.WithValue(c.UserContext(), auth.TenantIDKey, "tenant-123")
		c.SetUserContext(ctx)
		return c.Next()
	}, handler, func(c *fiber.Ctx) error {
		return c.SendString("ok")
	})

	resp, err := app.Test(httptest.NewRequest(http.MethodPost, "/dispatch", http.NoBody))
	require.NoError(t, err)
	defer resp.Body.Close()

	assert.Equal(t, http.StatusOK, resp.StatusCode)
}

func TestSafeExpiry_ClampsToMinimumOne(t *testing.T) {
	t.Parallel()

	assert.Equal(t, 1, safeExpiry(0))
	assert.Equal(t, 1, safeExpiry(-5))
	assert.Equal(t, 1, safeExpiry(1))
	assert.Equal(t, 60, safeExpiry(60))
}

func TestNewLibRateLimiter_NilConnection_ReturnsNil(t *testing.T) {
	t.Parallel()

	rl := NewLibRateLimiter(nil, nil)
	assert.Nil(t, rl, "nil Redis connection should produce nil RateLimiter")
}

func TestRateLimiterProvider_CachesUntilRedisChanges(t *testing.T) {
	t.Parallel()

	redisA := &libRedis.Client{}
	redisB := &libRedis.Client{}
	current := redisA

	provider := newRateLimiterProvider(func() *libRedis.Client {
		return current
	}, nil)

	// First call builds a limiter from redisA.
	rl1 := provider.Get()
	// Second call should return the cached limiter (same Redis pointer).
	rl2 := provider.Get()
	assert.Same(t, rl1, rl2, "should cache the limiter when Redis client has not changed")

	// Swap the Redis client pointer — simulates bundle reload.
	current = redisB

	rl3 := provider.Get()
	assert.NotSame(t, rl1, rl3, "should rebuild the limiter when Redis client changes")

	// Subsequent calls with same redisB should cache again.
	rl4 := provider.Get()
	assert.Same(t, rl3, rl4, "should cache after rebuild")
}

func TestRateLimiterProvider_NilRedisReturnsNilLimiter(t *testing.T) {
	t.Parallel()

	provider := newRateLimiterProvider(func() *libRedis.Client {
		return nil
	}, nil)

	rl := provider.Get()
	assert.Nil(t, rl, "nil Redis should produce nil RateLimiter")
}

func TestRateLimiterProvider_TransitionsFromNilToRedis(t *testing.T) {
	t.Parallel()

	var current *libRedis.Client

	provider := newRateLimiterProvider(func() *libRedis.Client {
		return current
	}, nil)

	// Initially nil — should return nil limiter.
	rl1 := provider.Get()
	assert.Nil(t, rl1)

	// Now Redis becomes available — should rebuild.
	current = &libRedis.Client{}

	rl2 := provider.Get()
	// NewLibRateLimiter with a non-nil Client struct still may return nil
	// if the Client's internal state is empty (no DSN). But the provider
	// should have attempted a rebuild (lastRedis changed).
	// The key assertion: the provider detected the change.
	assert.NotSame(t, rl1, rl2, "should detect transition from nil to non-nil Redis")
}
