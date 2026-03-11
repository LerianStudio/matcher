package bootstrap

import (
	"sync"
	"time"

	"github.com/gofiber/fiber/v2"
	"github.com/gofiber/fiber/v2/middleware/limiter"
)

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

		// NOTE: Rebuilding the Fiber limiter clears in-progress rate-limit counters
		// for all clients. This is a known tradeoff — counter state is lost during
		// config transitions but self-corrects within one window duration.
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
