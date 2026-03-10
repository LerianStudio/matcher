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

// buildDistributedDynamicHandler creates a limiter middleware backed by external storage
// that rebuilds the Fiber limiter when max/expiry change to preserve distributed
// counter semantics.
func buildDistributedDynamicHandler(dlCtx *dynamicLimiterContext, storage fiber.Storage) fiber.Handler {
	var (
		mu            sync.RWMutex
		activeHandler fiber.Handler
		activeMax     int
		activeExpiry  int
	)

	buildHandler := func(rateMax, expiry int) fiber.Handler {
		return limiter.New(limiter.Config{
			Max:          rateMax,
			Expiration:   time.Duration(expiry) * time.Second,
			Storage:      storage,
			KeyGenerator: dlCtx.keyGen,
			LimitReached: dlCtx.limitReached,
		})
	}

	return func(fiberCtx *fiber.Ctx) error {
		cfg := dlCtx.configGetter()
		if cfg == nil || !dlCtx.opts.isRateLimit(cfg) {
			return fiberCtx.Next()
		}

		rateMax := dlCtx.opts.getMax(cfg)
		if rateMax <= 0 {
			return fiberCtx.Next()
		}

		expiry := dlCtx.safeExpiry(cfg)

		mu.RLock()

		handler := activeHandler
		currentMax := activeMax
		currentExpiry := activeExpiry

		mu.RUnlock()

		if handler == nil || currentMax != rateMax || currentExpiry != expiry {
			mu.Lock()
			if activeHandler == nil || activeMax != rateMax || activeExpiry != expiry {
				activeHandler = buildHandler(rateMax, expiry)
				activeMax = rateMax
				activeExpiry = expiry
			}

			handler = activeHandler
			mu.Unlock()
		}

		return handler(fiberCtx)
	}
}

// newRuntimeDynamicLimiter builds a hot-reloadable limiter middleware that reads
// limit values per request. When storage is nil, it uses an in-memory implementation
// with dynamic max support; when storage is non-nil, it rebuilds Fiber's limiter
// handler when max/expiry change to preserve distributed counter semantics.
func newRuntimeDynamicLimiter(
	configGetter func() *Config,
	storage fiber.Storage,
	opts dynamicLimiterOptions,
) fiber.Handler {
	if configGetter == nil {
		return func(fiberCtx *fiber.Ctx) error {
			return fiberCtx.Next()
		}
	}

	initialCfg := configGetter()
	if initialCfg == nil {
		return func(fiberCtx *fiber.Ctx) error {
			return fiberCtx.Next()
		}
	}

	dlCtx := &dynamicLimiterContext{
		configGetter: configGetter,
		initialCfg:   initialCfg,
		opts:         opts,
		keyGen:       rateLimitKeyGenerator(opts.prefix),
		safeExpiry: func(cfg *Config) int {
			expiry := opts.getExpiry(cfg)
			if expiry < 1 {
				return 1
			}

			return expiry
		},
	}

	dlCtx.limitReached = buildLimitReachedHandler(dlCtx)

	if storage == nil {
		return buildInMemoryDynamicHandler(dlCtx)
	}

	return buildDistributedDynamicHandler(dlCtx, storage)
}

// NewDynamicRateLimiter creates a rate limiter whose Max value and Enabled flag are
// read per-request from configGetter, enabling hot-reload of rate limits without
// service restart.
func NewDynamicRateLimiter(configGetter func() *Config, storage fiber.Storage) fiber.Handler {
	return newRuntimeDynamicLimiter(configGetter, storage, dynamicLimiterOptions{
		prefix:    "",
		errorCode: "rate_limit_exceeded",
		errorMsg:  "rate limit exceeded",
		getMax: func(cfg *Config) int {
			if cfg == nil {
				return 0
			}

			return cfg.RateLimit.Max
		},
		getExpiry: func(cfg *Config) int {
			if cfg == nil {
				return 1
			}

			return cfg.RateLimit.ExpirySec
		},
		isRateLimit: func(cfg *Config) bool {
			return cfg != nil && cfg.RateLimit.Enabled
		},
	})
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

// NewDynamicExportRateLimiter creates an export rate limiter whose Max value and
// Enabled flag are read per-request from configGetter, enabling hot-reload without
// service restart. Returns a no-op middleware if configGetter returns nil.
func NewDynamicExportRateLimiter(configGetter func() *Config, storage fiber.Storage) fiber.Handler {
	return newRuntimeDynamicLimiter(configGetter, storage, dynamicLimiterOptions{
		prefix:    "export:",
		errorCode: "export_rate_limit_exceeded",
		errorMsg:  "too many export requests, please try again later",
		getMax: func(cfg *Config) int {
			if cfg == nil {
				return 0
			}

			return cfg.RateLimit.ExportMax
		},
		getExpiry: func(cfg *Config) int {
			if cfg == nil {
				return 1
			}

			return cfg.RateLimit.ExportExpirySec
		},
		isRateLimit: func(cfg *Config) bool {
			return cfg != nil && cfg.RateLimit.Enabled
		},
	})
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

// NewDynamicDispatchRateLimiter creates a dispatch rate limiter whose Max value and
// Enabled flag are read per-request from configGetter, enabling hot-reload without
// service restart. Returns a no-op middleware if configGetter returns nil.
func NewDynamicDispatchRateLimiter(configGetter func() *Config, storage fiber.Storage) fiber.Handler {
	return newRuntimeDynamicLimiter(configGetter, storage, dynamicLimiterOptions{
		prefix:    "dispatch:",
		errorCode: "dispatch_rate_limit_exceeded",
		errorMsg:  "too many dispatch requests, please try again later",
		getMax: func(cfg *Config) int {
			if cfg == nil {
				return 0
			}

			return cfg.RateLimit.DispatchMax
		},
		getExpiry: func(cfg *Config) int {
			if cfg == nil {
				return 1
			}

			return cfg.RateLimit.DispatchExpirySec
		},
		isRateLimit: func(cfg *Config) bool {
			return cfg != nil && cfg.RateLimit.Enabled
		},
	})
}
