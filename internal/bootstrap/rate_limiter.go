// Copyright 2025 Lerian Studio. All rights reserved.
// Use of this source code is governed by an Elastic License 2.0
// that can be found in the LICENSE.md file.

package bootstrap

import (
	"sync"
	"time"

	"github.com/gofiber/fiber/v2"

	"github.com/LerianStudio/lib-commons/v5/commons/log"
	"github.com/LerianStudio/lib-commons/v5/commons/net/http/ratelimit"
	libRedis "github.com/LerianStudio/lib-commons/v5/commons/redis"

	"github.com/LerianStudio/matcher/internal/auth"
)

// passthrough is a no-op middleware that passes control to the next handler.
var passthrough = func(c *fiber.Ctx) error {
	return c.Next()
}

// rateLimitIdentityFunc returns a ratelimit.IdentityFunc that extracts the client
// identity from JWT context values. Priority: UserID -> TenantID#IP -> IP fallback.
// This MUST be applied AFTER auth middleware to access user context.
func rateLimitIdentityFunc() ratelimit.IdentityFunc {
	return func(fiberCtx *fiber.Ctx) string {
		ctx := fiberCtx.UserContext()
		if ctx != nil {
			if userID, ok := ctx.Value(auth.UserIDKey).(string); ok && userID != "" {
				return "user:" + userID
			}

			if tenantID, ok := ctx.Value(auth.TenantIDKey).(string); ok && tenantID != "" {
				return "tenant:" + tenantID + "#ip:" + fiberCtx.IP()
			}
		}

		return "ip:" + fiberCtx.IP()
	}
}

// NewLibRateLimiter creates a lib-commons RateLimiter instance configured with
// Matcher's custom identity function and key prefix. Returns nil (safe to use)
// when rate limiting is disabled or conn is nil.
func NewLibRateLimiter(conn *libRedis.Client, logger log.Logger) *ratelimit.RateLimiter {
	return ratelimit.New(conn,
		ratelimit.WithKeyPrefix("matcher"),
		ratelimit.WithIdentityFunc(rateLimitIdentityFunc()),
		ratelimit.WithLogger(logger),
		ratelimit.WithFailOpen(true),
	)
}

// rateLimiterProvider resolves the current *ratelimit.RateLimiter at request time.
// It caches the limiter and rebuilds only when the underlying Redis client changes
// (detected by pointer identity). This ensures that after a systemplane bundle reload
// swaps the Redis connection, subsequent rate-limited requests use the new client.
type rateLimiterProvider struct {
	mu        sync.Mutex
	current   *ratelimit.RateLimiter
	lastRedis *libRedis.Client
	redisFunc func() *libRedis.Client
	logger    log.Logger
}

// newRateLimiterProvider creates a provider that lazily builds and caches a
// *ratelimit.RateLimiter, rebuilding when the Redis client returned by redisFunc changes.
func newRateLimiterProvider(redisFunc func() *libRedis.Client, logger log.Logger) *rateLimiterProvider {
	return &rateLimiterProvider{
		redisFunc: redisFunc,
		logger:    logger,
	}
}

// Get returns the current rate limiter, rebuilding it if the underlying Redis client
// has changed since the last call. Safe for concurrent use.
func (rlp *rateLimiterProvider) Get() *ratelimit.RateLimiter {
	redis := rlp.redisFunc()

	// Fast path: no Redis client change since last build.
	rlp.mu.Lock()
	defer rlp.mu.Unlock()

	if redis == rlp.lastRedis && rlp.current != nil {
		return rlp.current
	}

	rlp.current = NewLibRateLimiter(redis, rlp.logger)
	rlp.lastRedis = redis

	return rlp.current
}

// NewGlobalRateLimit creates a fiber.Handler for the global rate limiter tier.
// The rlGetter is called at request time to obtain the current rate limiter,
// allowing transparent Redis client swaps via systemplane bundle reloads.
func NewGlobalRateLimit(
	rlGetter func() *ratelimit.RateLimiter,
	cfg *Config,
	configGetter func() *Config,
	settingsResolver *runtimeSettingsResolver,
) fiber.Handler {
	if settingsResolver != nil {
		return settingsBackedRateLimitHandler(rlGetter, cfg, configGetter, settingsResolver, func(currentCfg RateLimitConfig) ratelimit.Tier {
			return ratelimit.Tier{
				Name:   "global",
				Max:    currentCfg.Max,
				Window: time.Duration(safeExpiry(currentCfg.ExpirySec)) * time.Second,
			}
		})
	}

	if configGetter != nil {
		return dynamicRateLimitHandler(rlGetter, configGetter, func(c *Config) ratelimit.Tier {
			return ratelimit.Tier{
				Name:   "global",
				Max:    c.RateLimit.Max,
				Window: time.Duration(safeExpiry(c.RateLimit.ExpirySec)) * time.Second,
			}
		})
	}

	if cfg == nil || !cfg.RateLimit.Enabled {
		return passthrough
	}

	return staticRateLimitHandler(rlGetter, ratelimit.Tier{
		Name:   "global",
		Max:    cfg.RateLimit.Max,
		Window: time.Duration(safeExpiry(cfg.RateLimit.ExpirySec)) * time.Second,
	})
}

// NewExportRateLimit creates a fiber.Handler for the export rate limiter tier.
// The rlGetter is called at request time to obtain the current rate limiter,
// allowing transparent Redis client swaps via systemplane bundle reloads.
func NewExportRateLimit(
	rlGetter func() *ratelimit.RateLimiter,
	cfg *Config,
	configGetter func() *Config,
	settingsResolver *runtimeSettingsResolver,
) fiber.Handler {
	if settingsResolver != nil {
		return settingsBackedRateLimitHandler(rlGetter, cfg, configGetter, settingsResolver, func(currentCfg RateLimitConfig) ratelimit.Tier {
			return ratelimit.Tier{
				Name:   "export",
				Max:    currentCfg.ExportMax,
				Window: time.Duration(safeExpiry(currentCfg.ExportExpirySec)) * time.Second,
			}
		})
	}

	if configGetter != nil {
		return dynamicRateLimitHandler(rlGetter, configGetter, func(c *Config) ratelimit.Tier {
			return ratelimit.Tier{
				Name:   "export",
				Max:    c.RateLimit.ExportMax,
				Window: time.Duration(safeExpiry(c.RateLimit.ExportExpirySec)) * time.Second,
			}
		})
	}

	if cfg == nil || !cfg.RateLimit.Enabled {
		return passthrough
	}

	return staticRateLimitHandler(rlGetter, ratelimit.Tier{
		Name:   "export",
		Max:    cfg.RateLimit.ExportMax,
		Window: time.Duration(safeExpiry(cfg.RateLimit.ExportExpirySec)) * time.Second,
	})
}

// NewDispatchRateLimit creates a fiber.Handler for the dispatch rate limiter tier.
// The rlGetter is called at request time to obtain the current rate limiter,
// allowing transparent Redis client swaps via systemplane bundle reloads.
func NewDispatchRateLimit(
	rlGetter func() *ratelimit.RateLimiter,
	cfg *Config,
	configGetter func() *Config,
	settingsResolver *runtimeSettingsResolver,
) fiber.Handler {
	if settingsResolver != nil {
		return settingsBackedRateLimitHandler(rlGetter, cfg, configGetter, settingsResolver, func(currentCfg RateLimitConfig) ratelimit.Tier {
			return ratelimit.Tier{
				Name:   "dispatch",
				Max:    currentCfg.DispatchMax,
				Window: time.Duration(safeExpiry(currentCfg.DispatchExpirySec)) * time.Second,
			}
		})
	}

	if configGetter != nil {
		return dynamicRateLimitHandler(rlGetter, configGetter, func(c *Config) ratelimit.Tier {
			return ratelimit.Tier{
				Name:   "dispatch",
				Max:    c.RateLimit.DispatchMax,
				Window: time.Duration(safeExpiry(c.RateLimit.DispatchExpirySec)) * time.Second,
			}
		})
	}

	if cfg == nil || !cfg.RateLimit.Enabled {
		return passthrough
	}

	return staticRateLimitHandler(rlGetter, ratelimit.Tier{
		Name:   "dispatch",
		Max:    cfg.RateLimit.DispatchMax,
		Window: time.Duration(safeExpiry(cfg.RateLimit.DispatchExpirySec)) * time.Second,
	})
}

// NewAdminRateLimit creates a fiber.Handler for the admin plane (/system)
// rate limiter tier. Intentionally scoped separately from the global tier
// so admin traffic and tenant traffic cannot starve each other — admin
// storms hit their own quota and vice versa. The rlGetter is called at
// request time to obtain the current rate limiter, allowing transparent
// Redis client swaps via systemplane bundle reloads.
func NewAdminRateLimit(
	rlGetter func() *ratelimit.RateLimiter,
	cfg *Config,
	configGetter func() *Config,
	settingsResolver *runtimeSettingsResolver,
) fiber.Handler {
	if settingsResolver != nil {
		return settingsBackedRateLimitHandler(rlGetter, cfg, configGetter, settingsResolver, func(currentCfg RateLimitConfig) ratelimit.Tier {
			return ratelimit.Tier{
				Name:   "admin",
				Max:    currentCfg.AdminMax,
				Window: time.Duration(safeExpiry(currentCfg.AdminExpirySec)) * time.Second,
			}
		})
	}

	if configGetter != nil {
		return dynamicRateLimitHandler(rlGetter, configGetter, func(c *Config) ratelimit.Tier {
			return ratelimit.Tier{
				Name:   "admin",
				Max:    c.RateLimit.AdminMax,
				Window: time.Duration(safeExpiry(c.RateLimit.AdminExpirySec)) * time.Second,
			}
		})
	}

	if cfg == nil || !cfg.RateLimit.Enabled {
		return passthrough
	}

	return staticRateLimitHandler(rlGetter, ratelimit.Tier{
		Name:   "admin",
		Max:    cfg.RateLimit.AdminMax,
		Window: time.Duration(safeExpiry(cfg.RateLimit.AdminExpirySec)) * time.Second,
	})
}

// resolveRL safely calls the getter and returns the current rate limiter.
// Returns nil (which produces pass-through handlers) when getter is nil.
func resolveRL(rlGetter func() *ratelimit.RateLimiter) *ratelimit.RateLimiter {
	if rlGetter == nil {
		return nil
	}

	return rlGetter()
}

// staticRateLimitHandler resolves the current rate limiter at request time and applies
// a fixed tier. Used when no configGetter is available (static configuration).
func staticRateLimitHandler(rlGetter func() *ratelimit.RateLimiter, tier ratelimit.Tier) fiber.Handler {
	return func(fiberCtx *fiber.Ctx) error {
		// resolveRL handles nil getter; WithRateLimit handles nil receiver.
		return resolveRL(rlGetter).WithRateLimit(tier)(fiberCtx)
	}
}

// dynamicRateLimitHandler resolves both the current rate limiter and config at request
// time, enabling transparent Redis client swaps and hot-reload of rate limit settings.
func dynamicRateLimitHandler(
	rlGetter func() *ratelimit.RateLimiter,
	configGetter func() *Config,
	tierBuilder func(*Config) ratelimit.Tier,
) fiber.Handler {
	return func(fiberCtx *fiber.Ctx) error {
		currentCfg := configGetter()
		if currentCfg == nil || !currentCfg.RateLimit.Enabled {
			return fiberCtx.Next()
		}

		tier := tierBuilder(currentCfg)
		if tier.Max <= 0 {
			return fiberCtx.Next()
		}

		// resolveRL handles nil getter; WithDynamicRateLimit handles nil receiver.
		rl := resolveRL(rlGetter)

		return rl.WithDynamicRateLimit(func(_ *fiber.Ctx) ratelimit.Tier {
			return tier
		})(fiberCtx)
	}
}

func settingsBackedRateLimitHandler(
	rlGetter func() *ratelimit.RateLimiter,
	cfg *Config,
	configGetter func() *Config,
	settingsResolver *runtimeSettingsResolver,
	tierBuilder func(RateLimitConfig) ratelimit.Tier,
) fiber.Handler {
	return func(fiberCtx *fiber.Ctx) error {
		currentCfg := currentRateLimitConfig(cfg, configGetter)

		currentCfg = settingsResolver.rateLimit(currentCfg)
		if !currentCfg.Enabled {
			return fiberCtx.Next()
		}

		tier := tierBuilder(currentCfg)
		if tier.Max <= 0 {
			return fiberCtx.Next()
		}

		rl := resolveRL(rlGetter)

		return rl.WithDynamicRateLimit(func(_ *fiber.Ctx) ratelimit.Tier {
			return tier
		})(fiberCtx)
	}
}

func currentRateLimitConfig(cfg *Config, configGetter func() *Config) RateLimitConfig {
	if configGetter != nil {
		if runtimeCfg := configGetter(); runtimeCfg != nil {
			return runtimeCfg.RateLimit
		}
	}

	if cfg != nil {
		return cfg.RateLimit
	}

	return RateLimitConfig{}
}

// safeExpiry clamps expiry seconds to a minimum of 1.
func safeExpiry(expirySec int) int {
	if expirySec < 1 {
		return 1
	}

	return expirySec
}
