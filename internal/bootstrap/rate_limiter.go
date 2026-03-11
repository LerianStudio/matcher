// Copyright 2025 Lerian Studio. All rights reserved.
// Use of this source code is governed by an Elastic License 2.0
// that can be found in the LICENSE.md file.

package bootstrap

import (
	"strconv"
	"sync"
	"time"

	"github.com/gofiber/fiber/v2"
	"github.com/gofiber/fiber/v2/middleware/limiter"

	sharedhttp "github.com/LerianStudio/lib-commons/v4/commons/net/http"

	"github.com/LerianStudio/matcher/internal/auth"
)

// rateLimitKeyGenerator returns a key generator closure for rate limiting.
// The key prefers UserID (from JWT) → TenantID+IP composite → raw IP fallback.
// The prefix parameter namespaces keys for different limiter tiers (e.g., "export:", "dispatch:").
func rateLimitKeyGenerator(prefix string) func(*fiber.Ctx) string {
	return func(fiberCtx *fiber.Ctx) string {
		ctx := fiberCtx.UserContext()
		if ctx != nil {
			if userID, ok := ctx.Value(auth.UserIDKey).(string); ok && userID != "" {
				return prefix + userID
			}

			if tenantID, ok := ctx.Value(auth.TenantIDKey).(string); ok && tenantID != "" {
				return prefix + tenantID + ":" + fiberCtx.IP()
			}
		}

		return prefix + fiberCtx.IP()
	}
}

// dynamicLimiterConfig holds the parameters for a dynamic rate limiter.
type dynamicLimiterConfig struct {
	// maxFunc returns the current per-window request limit. Called per-request.
	maxFunc func() int
	// expiration is the fixed window duration (static — Fiber v2 limitation).
	expiration time.Duration
	// expirationFunc returns the current window duration (optional, per-request).
	expirationFunc func() time.Duration
	// keyGenerator produces a per-client key from the request context.
	keyGenerator func(*fiber.Ctx) string
	// limitReached is invoked when a request exceeds the limit.
	limitReached fiber.Handler
}

// newDynamicLimiter creates a fixed-window rate limiter middleware that reads Max
// from a closure on every request, enabling hot-reload of rate limits.
//
// Fiber v2's built-in FixedWindow captures cfg.Max once at creation (both as an int
// for limit checks and a string for X-RateLimit-Limit headers). This wrapper reimplements
// the fixed-window algorithm with a dynamic max while preserving identical counting
// and expiration semantics.
//
// The rate limiter uses an in-memory map for hit counting with periodic GC to evict
// expired entries (sweep interval = 2x window duration). This prevents unbounded
// memory growth from unique client keys that stop sending requests.
func newDynamicLimiter(cfg dynamicLimiterConfig) fiber.Handler {
	// Validate required function pointers — callers must provide all three.
	if cfg.maxFunc == nil || cfg.keyGenerator == nil || cfg.limitReached == nil {
		return func(fiberCtx *fiber.Ctx) error {
			return fiberCtx.Next()
		}
	}

	type rateLimitEntry struct {
		hits int
		exp  int64 // unix timestamp when window expires
	}

	// NOTE: In-memory rate limit counters are per-instance. Multi-instance
	// deployments have per-node limits. Use a shared storage backend (Redis)
	// via NewRateLimiter/NewExportRateLimiter for cross-instance consistency.
	var (
		mu      sync.Mutex
		entries = make(map[string]*rateLimitEntry)
		lastGC  int64
	)

	return func(fiberCtx *fiber.Ctx) error {
		currentMax := cfg.maxFunc()

		// Guard: if max is zero or negative, treat rate limiting as disabled
		// for this request. This prevents accidentally blocking all traffic
		// when config returns a non-positive value (e.g., during hot-reload
		// with an invalid value).
		if currentMax <= 0 {
			return fiberCtx.Next()
		}

		// NOTE: When expiry changes via hot-reload, existing rate-limit entries
		// retain their original expiration timestamp. This means the old window
		// may persist until entries naturally expire. This is inherent to
		// fixed-window rate limiting and self-corrects within one window duration.
		expirationDuration := cfg.expiration
		if cfg.expirationFunc != nil {
			expirationDuration = cfg.expirationFunc()
		}

		expirationSec := int64(expirationDuration.Seconds())
		if expirationSec < 1 {
			expirationSec = 1
		}

		gcInterval := expirationSec * 2 //nolint:mnd // sweep every 2x the window duration

		key := cfg.keyGenerator(fiberCtx)
		now := time.Now().Unix()

		mu.Lock()

		// Lazy GC: sweep expired entries periodically to prevent unbounded memory growth.
		if now-lastGC > gcInterval {
			for k, ent := range entries {
				if now >= ent.exp {
					delete(entries, k)
				}
			}

			lastGC = now
		}

		entry, exists := entries[key]
		if !exists || now >= entry.exp {
			entry = &rateLimitEntry{hits: 0, exp: now + expirationSec}
			entries[key] = entry
		}

		entry.hits++
		hits := entry.hits
		resetInSec := entry.exp - now

		mu.Unlock()

		remaining := currentMax - hits

		// Set rate limit headers on ALL responses (including 429).
		fiberCtx.Set(headerRateLimitLimit, strconv.Itoa(currentMax))
		fiberCtx.Set(headerRateLimitReset, strconv.FormatInt(resetInSec, 10))

		if remaining < 0 {
			fiberCtx.Set(headerRateLimitRemaining, "0")
			fiberCtx.Set(fiber.HeaderRetryAfter, strconv.FormatInt(resetInSec, 10))

			return cfg.limitReached(fiberCtx)
		}

		fiberCtx.Set(headerRateLimitRemaining, strconv.Itoa(remaining))

		return fiberCtx.Next()
	}
}

// NewRateLimiter creates a rate limiter middleware that uses UserID/TenantID from context.
// This middleware MUST be applied AFTER auth middleware to access user context.
// Order: Auth → RateLimiter → Handlers
// If storage is provided, uses it for distributed rate limiting across multiple instances.
// Returns a no-op middleware if rate limiting is disabled or cfg is nil.
func NewRateLimiter(cfg *Config, storage fiber.Storage) fiber.Handler {
	if cfg == nil || !cfg.RateLimit.Enabled {
		return func(c *fiber.Ctx) error {
			return c.Next()
		}
	}

	limiterCfg := limiter.Config{
		Max:          cfg.RateLimit.Max,
		Expiration:   time.Duration(cfg.RateLimit.ExpirySec) * time.Second,
		KeyGenerator: rateLimitKeyGenerator(""),
		LimitReached: func(fiberCtx *fiber.Ctx) error {
			fiberCtx.Set("Retry-After", strconv.Itoa(cfg.RateLimit.ExpirySec))

			return sharedhttp.RespondError(
				fiberCtx,
				fiber.StatusTooManyRequests,
				"rate_limit_exceeded",
				"rate limit exceeded",
			)
		},
	}

	if storage != nil {
		limiterCfg.Storage = storage
	}

	return limiter.New(limiterCfg)
}

type dynamicLimiterOptions struct {
	prefix      string
	errorCode   string
	errorMsg    string
	getMax      func(*Config) int
	getExpiry   func(*Config) int
	isRateLimit func(*Config) bool
}

// dynamicLimiterContext holds the shared state used by both in-memory and
// distributed limiter paths in newRuntimeDynamicLimiter.
type dynamicLimiterContext struct {
	configGetter func() *Config
	initialCfg   *Config
	opts         dynamicLimiterOptions
	keyGen       func(*fiber.Ctx) string
	safeExpiry   func(*Config) int
	limitReached func(*fiber.Ctx) error
}

// buildLimitReachedHandler creates the handler invoked when a client exceeds the rate limit.
func buildLimitReachedHandler(dlCtx *dynamicLimiterContext) func(*fiber.Ctx) error {
	return func(fiberCtx *fiber.Ctx) error {
		cfg := dlCtx.configGetter()
		if cfg == nil {
			cfg = dlCtx.initialCfg
		}

		expiry := dlCtx.safeExpiry(cfg)
		rateMax := dlCtx.opts.getMax(cfg)

		if rateMax < 0 {
			rateMax = 0
		}

		fiberCtx.Set(fiber.HeaderRetryAfter, strconv.Itoa(expiry))
		fiberCtx.Set(headerRateLimitLimit, strconv.Itoa(rateMax))
		fiberCtx.Set(headerRateLimitRemaining, "0")
		fiberCtx.Set(headerRateLimitReset, strconv.Itoa(expiry))

		return sharedhttp.RespondError(
			fiberCtx,
			fiber.StatusTooManyRequests,
			dlCtx.opts.errorCode,
			dlCtx.opts.errorMsg,
		)
	}
}

// buildInMemoryDynamicHandler creates a limiter middleware backed by an in-memory store
// that reads Max dynamically per request (hot-reload of rate limits).
func buildInMemoryDynamicHandler(dlCtx *dynamicLimiterContext) fiber.Handler {
	limiterHandler := newDynamicLimiter(dynamicLimiterConfig{
		maxFunc: func() int {
			if cfg := dlCtx.configGetter(); cfg != nil {
				return dlCtx.opts.getMax(cfg)
			}

			return dlCtx.opts.getMax(dlCtx.initialCfg)
		},
		expiration: time.Duration(dlCtx.safeExpiry(dlCtx.initialCfg)) * time.Second,
		expirationFunc: func() time.Duration {
			if cfg := dlCtx.configGetter(); cfg != nil {
				return time.Duration(dlCtx.safeExpiry(cfg)) * time.Second
			}

			return time.Duration(dlCtx.safeExpiry(dlCtx.initialCfg)) * time.Second
		},
		keyGenerator: dlCtx.keyGen,
		limitReached: dlCtx.limitReached,
	})

	return func(fiberCtx *fiber.Ctx) error {
		cfg := dlCtx.configGetter()
		if cfg == nil || !dlCtx.opts.isRateLimit(cfg) || dlCtx.opts.getMax(cfg) <= 0 {
			return fiberCtx.Next()
		}

		return limiterHandler(fiberCtx)
	}
}

// NewExportRateLimiter creates a rate limiter middleware for export endpoints.
// It applies stricter limits than the global rate limiter to protect resource-intensive
// report generation operations.
// If storage is provided, uses it for distributed rate limiting across multiple instances.
// Returns a no-op middleware if rate limiting is disabled or cfg is nil.
func NewExportRateLimiter(cfg *Config, storage fiber.Storage) fiber.Handler {
	if cfg == nil || !cfg.RateLimit.Enabled {
		return func(c *fiber.Ctx) error {
			return c.Next()
		}
	}

	limiterCfg := limiter.Config{
		Max:          cfg.RateLimit.ExportMax,
		Expiration:   time.Duration(cfg.RateLimit.ExportExpirySec) * time.Second,
		KeyGenerator: rateLimitKeyGenerator("export:"),
		LimitReached: func(fiberCtx *fiber.Ctx) error {
			fiberCtx.Set("Retry-After", strconv.Itoa(cfg.RateLimit.ExportExpirySec))

			return sharedhttp.RespondError(
				fiberCtx,
				fiber.StatusTooManyRequests,
				"export_rate_limit_exceeded",
				"too many export requests, please try again later",
			)
		},
	}

	if storage != nil {
		limiterCfg.Storage = storage
	}

	return limiter.New(limiterCfg)
}

// NewDispatchRateLimiter creates a rate limiter middleware for exception dispatch endpoints.
// It applies moderate limits to protect external system integrations from overload.
// If storage is provided, uses it for distributed rate limiting across multiple instances.
// Returns a no-op middleware if rate limiting is disabled or cfg is nil.
func NewDispatchRateLimiter(cfg *Config, storage fiber.Storage) fiber.Handler {
	if cfg == nil || !cfg.RateLimit.Enabled {
		return func(c *fiber.Ctx) error {
			return c.Next()
		}
	}

	limiterCfg := limiter.Config{
		Max:          cfg.RateLimit.DispatchMax,
		Expiration:   time.Duration(cfg.RateLimit.DispatchExpirySec) * time.Second,
		KeyGenerator: rateLimitKeyGenerator("dispatch:"),
		LimitReached: func(fiberCtx *fiber.Ctx) error {
			fiberCtx.Set("Retry-After", strconv.Itoa(cfg.RateLimit.DispatchExpirySec))

			return sharedhttp.RespondError(
				fiberCtx,
				fiber.StatusTooManyRequests,
				"dispatch_rate_limit_exceeded",
				"too many dispatch requests, please try again later",
			)
		},
	}

	if storage != nil {
		limiterCfg.Storage = storage
	}

	return limiter.New(limiterCfg)
}
