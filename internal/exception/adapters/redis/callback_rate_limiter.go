package redis

import (
	"context"
	"errors"
	"fmt"
	"time"

	"go.opentelemetry.io/otel/trace/noop"

	libCommons "github.com/LerianStudio/lib-commons/v5/commons"
	libLog "github.com/LerianStudio/lib-commons/v5/commons/log"
	libOpentelemetry "github.com/LerianStudio/lib-commons/v5/commons/opentelemetry"
	"github.com/LerianStudio/lib-commons/v5/commons/tenant-manager/valkey"

	"github.com/LerianStudio/matcher/internal/exception/ports"
	sharedPorts "github.com/LerianStudio/matcher/internal/shared/ports"
)

const (
	callbackRateLimitKeyPrefix = "matcher:callback:ratelimit"
)

// DefaultCallbackRateLimitPerMin is the default maximum number of callbacks
// allowed per minute per key. Configurable via CALLBACK_RATE_LIMIT_PER_MIN.
const DefaultCallbackRateLimitPerMin = 60

// CallbackRateLimiter errors.
var (
	ErrRateLimiterNotInitialized = errors.New("callback rate limiter not initialized")
	ErrRateLimiterRedisClientNil = errors.New("callback rate limiter: redis client is nil")
	ErrRateLimiterNilProvider    = errors.New("callback rate limiter: infrastructure provider is nil")
	errUnexpectedResultType      = errors.New("rate limiter unexpected result type")
)

// CallbackRateLimiter implements a Redis-based sliding window rate limiter
// for callback processing. It uses a simple counter with TTL approach:
// each key gets a counter that resets every window period.
//
// The sliding window counter pattern works as follows:
//   - On each request, INCR the counter for the current window key
//   - If this is the first request (counter == 1), set the TTL to the window duration
//   - If the counter exceeds the limit, deny the request
//   - When the TTL expires, the counter resets automatically
type CallbackRateLimiter struct {
	provider      sharedPorts.InfrastructureProvider
	limit         int
	window        time.Duration
	limitResolver func(context.Context) int
}

// NewCallbackRateLimiter creates a new Redis-based callback rate limiter
// with the specified limit per window duration.
func NewCallbackRateLimiter(
	provider sharedPorts.InfrastructureProvider,
	limitPerWindow int,
	window time.Duration,
) (*CallbackRateLimiter, error) {
	if provider == nil {
		return nil, ErrRateLimiterNilProvider
	}

	if limitPerWindow <= 0 {
		limitPerWindow = DefaultCallbackRateLimitPerMin
	}

	if window <= 0 {
		window = time.Minute
	}

	return &CallbackRateLimiter{
		provider: provider,
		limit:    limitPerWindow,
		window:   window,
	}, nil
}

// SetRuntimeLimitResolver injects a context-aware runtime callback limit source.
func (rl *CallbackRateLimiter) SetRuntimeLimitResolver(resolver func(context.Context) int) {
	if rl == nil {
		return
	}

	rl.limitResolver = resolver
}

func (rl *CallbackRateLimiter) currentLimit(ctx context.Context) int {
	if rl == nil {
		return DefaultCallbackRateLimitPerMin
	}

	if rl.limitResolver != nil {
		if limit := rl.limitResolver(ctx); limit > 0 {
			return limit
		}
	}

	if rl.limit > 0 {
		return rl.limit
	}

	return DefaultCallbackRateLimitPerMin
}

// Allow checks whether a callback identified by key is within the configured rate limit.
// Uses a Redis INCR + EXPIRE pattern for a fixed-window counter.
func (rl *CallbackRateLimiter) Allow(ctx context.Context, key string) (bool, error) {
	if rl == nil || rl.provider == nil {
		return false, ErrRateLimiterNotInitialized
	}

	logger, tracer, _, _ := libCommons.NewTrackingFromContext(ctx)

	if tracer == nil {
		tracer = noop.NewTracerProvider().Tracer("")
	}

	ctx, span := tracer.Start(ctx, "redis.callback_rate_limiter.allow")
	defer span.End()

	connLease, err := rl.provider.GetRedisConnection(ctx)
	if err != nil {
		libOpentelemetry.HandleSpanError(span, "failed to get redis connection", err)
		return false, fmt.Errorf("rate limiter get redis connection: %w", err)
	}
	defer connLease.Release()

	conn := connLease.Connection()
	if conn == nil {
		return false, ErrRateLimiterRedisClientNil
	}

	rdb, err := conn.GetClient(ctx)
	if err != nil {
		libOpentelemetry.HandleSpanError(span, "failed to get redis client", err)
		return false, fmt.Errorf("%w: %w", ErrRateLimiterRedisClientNil, err)
	}

	redisKey, err := scopedRateLimitRedisKey(ctx, key)
	if err != nil {
		libOpentelemetry.HandleSpanError(span, "failed to build scoped redis key", err)
		return false, fmt.Errorf("rate limiter build scoped key: %w", err)
	}

	// Lua script atomically increments counter and sets TTL on first request.
	// This avoids race conditions between INCR and EXPIRE.
	//
	// KEYS[1] = rate limit key
	// ARGV[1] = max allowed requests
	// ARGV[2] = window duration in milliseconds
	//
	// Returns: 1 if allowed, 0 if rate limited
	script := `
local current = redis.call("INCR", KEYS[1])
if current == 1 then
  redis.call("PEXPIRE", KEYS[1], ARGV[2])
end
if current > tonumber(ARGV[1]) then
  return 0
end
return 1
`

	result, err := rdb.Eval(
		ctx,
		script,
		[]string{redisKey},
		rl.currentLimit(ctx),
		rl.window.Milliseconds(),
	).Result()
	if err != nil {
		wrappedErr := fmt.Errorf("rate limiter eval: %w", err)
		libOpentelemetry.HandleSpanError(span, "rate limiter script failed", wrappedErr)

		if logger != nil {
			logger.Log(ctx, libLog.LevelError, fmt.Sprintf("callback rate limiter failed: %v", wrappedErr))
		}

		return false, wrappedErr
	}

	allowed, ok := result.(int64)
	if !ok {
		return false, fmt.Errorf("%w: %T", errUnexpectedResultType, result)
	}

	return allowed == 1, nil
}

// Ensure CallbackRateLimiter implements the port interface.
var _ ports.CallbackRateLimiter = (*CallbackRateLimiter)(nil)

func scopedRateLimitRedisKey(ctx context.Context, key string) (string, error) {
	rawKey := callbackRateLimitKeyPrefix + ":" + key

	result, err := valkey.GetKeyContext(ctx, rawKey)
	if err != nil {
		return "", fmt.Errorf("scoped rate limit redis key: %w", err)
	}

	return result, nil
}
