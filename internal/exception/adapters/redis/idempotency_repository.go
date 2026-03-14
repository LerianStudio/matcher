// Package redis provides Redis-based adapters for the exception bounded context.
package redis

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/redis/go-redis/v9"

	libCommons "github.com/LerianStudio/lib-commons/v4/commons"
	libLog "github.com/LerianStudio/lib-commons/v4/commons/log"
	libOpentelemetry "github.com/LerianStudio/lib-commons/v4/commons/opentelemetry"

	"github.com/LerianStudio/matcher/internal/auth"
	"github.com/LerianStudio/matcher/internal/exception/domain/repositories"
	"github.com/LerianStudio/matcher/internal/exception/domain/value_objects"
	"github.com/LerianStudio/matcher/internal/shared/ports"
)

const (
	idempotencyKeyPrefix = "matcher:idempotency:"
	statusPending        = "pending"
)

// DefaultSuccessTTL is the default duration that completed idempotency keys
// remain cached before expiring. Configurable via IDEMPOTENCY_SUCCESS_TTL_HOURS.
const DefaultSuccessTTL = 7 * 24 * time.Hour

// idempotencyCacheEntry is the structure stored in Redis for completed requests.
type idempotencyCacheEntry struct {
	Status     string `json:"status"`
	Response   []byte `json:"response,omitempty"`
	HTTPStatus int    `json:"httpStatus,omitempty"`
}

// DefaultFailedRetryWindow is the default duration that failed idempotency keys
// remain blocked before allowing retry. Configurable via IDEMPOTENCY_RETRY_WINDOW_SEC.
const DefaultFailedRetryWindow = 5 * time.Minute

// IdempotencyRepository errors.
var (
	ErrRepoNotInitialized = errors.New("idempotency repository not initialized")
	ErrRedisClientNil     = errors.New("redis client is nil")
	ErrCacheEntryNoStatus = errors.New("cache entry missing status field")
	ErrRepoNilProvider    = errors.New("idempotency repository: infrastructure provider is nil")
)

// IdempotencyRepository manages callback idempotency using Redis.
type IdempotencyRepository struct {
	provider          ports.InfrastructureProvider
	failedRetryWindow time.Duration
	successTTL        time.Duration
	hmacSecret        string
}

// NewIdempotencyRepository creates a new callback idempotency repository
// with the default failed retry window and success TTL.
func NewIdempotencyRepository(provider ports.InfrastructureProvider) (*IdempotencyRepository, error) {
	if provider == nil {
		return nil, ErrRepoNilProvider
	}

	return &IdempotencyRepository{
		provider:          provider,
		failedRetryWindow: DefaultFailedRetryWindow,
		successTTL:        DefaultSuccessTTL,
	}, nil
}

// NewIdempotencyRepositoryWithConfig creates a new callback idempotency repository
// with configurable failed retry window, success TTL, and optional HMAC secret.
// When hmacSecret is non-empty, client-provided idempotency keys are HMAC-signed
// before storage in Redis, preventing key prediction attacks.
func NewIdempotencyRepositoryWithConfig(
	provider ports.InfrastructureProvider,
	failedRetryWindow time.Duration,
	successTTL time.Duration,
	hmacSecret string,
) (*IdempotencyRepository, error) {
	if provider == nil {
		return nil, ErrRepoNilProvider
	}

	if failedRetryWindow <= 0 {
		failedRetryWindow = DefaultFailedRetryWindow
	}

	if successTTL <= 0 {
		successTTL = DefaultSuccessTTL
	}

	return &IdempotencyRepository{
		provider:          provider,
		failedRetryWindow: failedRetryWindow,
		successTTL:        successTTL,
		hmacSecret:        hmacSecret,
	}, nil
}

// storageKey returns the tenant-scoped Redis key for an idempotency key,
// optionally HMAC-signed if a secret is configured.
func (repo *IdempotencyRepository) storageKey(ctx context.Context, key value_objects.IdempotencyKey) string {
	tenantID := strings.TrimSpace(auth.GetTenantID(ctx))
	signedKey := key.SignKey(repo.hmacSecret)

	if tenantID == "" {
		tenantID = auth.DefaultTenantID
	}

	return idempotencyKeyPrefix + tenantID + ":" + signedKey
}

// TryAcquire attempts to acquire an idempotency lock. Returns true if acquired (first time).
func (repo *IdempotencyRepository) TryAcquire(
	ctx context.Context,
	key value_objects.IdempotencyKey,
) (bool, error) {
	if repo == nil || repo.provider == nil {
		return false, ErrRepoNotInitialized
	}

	logger, tracer, _, _ := libCommons.NewTrackingFromContext(ctx)
	ctx, span := tracer.Start(ctx, "redis.idempotency.try_acquire")

	defer span.End()

	connLease, err := repo.provider.GetRedisConnection(ctx)
	if err != nil {
		libOpentelemetry.HandleSpanError(span, "failed to get redis connection", err)
		return false, fmt.Errorf("get redis connection: %w", err)
	}
	defer connLease.Release()

	conn := connLease.Connection()
	if conn == nil {
		return false, ErrRedisClientNil
	}

	rdb, err := conn.GetClient(ctx)
	if err != nil {
		libOpentelemetry.HandleSpanError(span, "failed to get redis client", err)
		return false, fmt.Errorf("%w: %w", ErrRedisClientNil, err)
	}

	redisKey := repo.storageKey(ctx, key)

	// Use failedRetryWindow (short TTL, typically 5 min) for the initial pending
	// marker. If the request crashes before MarkComplete/MarkFailed is called,
	// the key auto-expires after this shorter window instead of blocking retries
	// for the full successTTL (typically 7 days). MarkComplete extends the TTL
	// to successTTL when it overwrites the pending marker with the cached response.
	ok, err := rdb.SetNX(ctx, redisKey, statusPending, repo.failedRetryWindow).Result()
	if err != nil {
		wrappedErr := fmt.Errorf("failed to acquire idempotency lock: %w", err)
		libOpentelemetry.HandleSpanError(span, "redis setnx failed", wrappedErr)

		logger.Log(ctx, libLog.LevelError, fmt.Sprintf("failed to acquire idempotency lock: %v", wrappedErr))

		return false, wrappedErr
	}

	return ok, nil
}

// TryReacquireFromFailed atomically transitions a failed key back to pending.
// Returns true only when the key was in failed status and this caller reclaimed it.
func (repo *IdempotencyRepository) TryReacquireFromFailed(
	ctx context.Context,
	key value_objects.IdempotencyKey,
) (bool, error) {
	if repo == nil || repo.provider == nil {
		return false, ErrRepoNotInitialized
	}

	logger, tracer, _, _ := libCommons.NewTrackingFromContext(ctx)
	ctx, span := tracer.Start(ctx, "redis.idempotency.try_reacquire_from_failed")

	defer span.End()

	connLease, err := repo.provider.GetRedisConnection(ctx)
	if err != nil {
		libOpentelemetry.HandleSpanError(span, "failed to get redis connection", err)
		return false, fmt.Errorf("get redis connection: %w", err)
	}
	defer connLease.Release()

	conn := connLease.Connection()
	if conn == nil {
		return false, ErrRedisClientNil
	}

	rdb, err := conn.GetClient(ctx)
	if err != nil {
		libOpentelemetry.HandleSpanError(span, "failed to get redis client", err)
		return false, fmt.Errorf("%w: %w", ErrRedisClientNil, err)
	}

	redisKey := repo.storageKey(ctx, key)

	retryTTLSeconds := int64(repo.failedRetryWindow / time.Second)
	if retryTTLSeconds <= 0 {
		retryTTLSeconds = int64(DefaultFailedRetryWindow / time.Second)
	}

	const reacquireFromFailedScript = `
local current = redis.call("GET", KEYS[1])
if not current then
  return 0
end
if current == ARGV[1] then
  return 0
end
local ok, decoded = pcall(cjson.decode, current)
if not ok or type(decoded) ~= "table" then
  return 0
end
if decoded["status"] ~= ARGV[2] then
  return 0
end
redis.call("SET", KEYS[1], ARGV[1], "EX", ARGV[3])
return 1
`

	result, err := rdb.Eval(
		ctx,
		reacquireFromFailedScript,
		[]string{redisKey},
		statusPending,
		string(value_objects.IdempotencyStatusFailed),
		retryTTLSeconds,
	).Int64()
	if err != nil {
		wrappedErr := fmt.Errorf("failed to reacquire failed idempotency key: %w", err)
		libOpentelemetry.HandleSpanError(span, "redis eval failed", wrappedErr)

		logger.Log(ctx, libLog.LevelError, fmt.Sprintf("failed to reacquire failed idempotency key: %v", wrappedErr))

		return false, wrappedErr
	}

	return result == 1, nil
}

// MarkComplete marks the callback as successfully processed with the response to cache.
func (repo *IdempotencyRepository) MarkComplete(
	ctx context.Context,
	key value_objects.IdempotencyKey,
	response []byte,
	httpStatus int,
) error {
	if repo == nil || repo.provider == nil {
		return ErrRepoNotInitialized
	}

	logger, tracer, _, _ := libCommons.NewTrackingFromContext(ctx)
	ctx, span := tracer.Start(ctx, "redis.idempotency.mark_complete")

	defer span.End()

	connLease, err := repo.provider.GetRedisConnection(ctx)
	if err != nil {
		libOpentelemetry.HandleSpanError(span, "failed to get redis connection", err)
		return fmt.Errorf("get redis connection: %w", err)
	}
	defer connLease.Release()

	conn := connLease.Connection()
	if conn == nil {
		return ErrRedisClientNil
	}

	rdb, err := conn.GetClient(ctx)
	if err != nil {
		libOpentelemetry.HandleSpanError(span, "failed to get redis client", err)
		return fmt.Errorf("%w: %w", ErrRedisClientNil, err)
	}

	entry := idempotencyCacheEntry{
		Status:     string(value_objects.IdempotencyStatusComplete),
		Response:   response,
		HTTPStatus: httpStatus,
	}

	data, err := json.Marshal(entry)
	if err != nil {
		libOpentelemetry.HandleSpanError(span, "failed to marshal cache entry", err)
		return fmt.Errorf("marshal cache entry: %w", err)
	}

	redisKey := repo.storageKey(ctx, key)

	if err := rdb.Set(ctx, redisKey, data, repo.successTTL).Err(); err != nil {
		wrappedErr := fmt.Errorf("failed to mark idempotency complete: %w", err)
		libOpentelemetry.HandleSpanError(span, "redis set failed", wrappedErr)

		logger.Log(ctx, libLog.LevelError, fmt.Sprintf("failed to mark idempotency complete: %v", wrappedErr))

		return wrappedErr
	}

	return nil
}

// MarkFailed marks a callback as failed so it can be retried after the retry window.
func (repo *IdempotencyRepository) MarkFailed(
	ctx context.Context,
	key value_objects.IdempotencyKey,
) error {
	if repo == nil || repo.provider == nil {
		return ErrRepoNotInitialized
	}

	logger, tracer, _, _ := libCommons.NewTrackingFromContext(ctx)
	ctx, span := tracer.Start(ctx, "redis.idempotency.mark_failed")

	defer span.End()

	connLease, err := repo.provider.GetRedisConnection(ctx)
	if err != nil {
		libOpentelemetry.HandleSpanError(span, "failed to get redis connection", err)
		return fmt.Errorf("get redis connection: %w", err)
	}
	defer connLease.Release()

	conn := connLease.Connection()
	if conn == nil {
		return ErrRedisClientNil
	}

	rdb, err := conn.GetClient(ctx)
	if err != nil {
		libOpentelemetry.HandleSpanError(span, "failed to get redis client", err)
		return fmt.Errorf("%w: %w", ErrRedisClientNil, err)
	}

	entry := idempotencyCacheEntry{
		Status: string(value_objects.IdempotencyStatusFailed),
	}

	data, err := json.Marshal(entry)
	if err != nil {
		libOpentelemetry.HandleSpanError(span, "failed to marshal failed entry", err)
		return fmt.Errorf("marshal failed entry: %w", err)
	}

	redisKey := repo.storageKey(ctx, key)

	if err := rdb.Set(ctx, redisKey, data, repo.failedRetryWindow).Err(); err != nil {
		wrappedErr := fmt.Errorf("failed to mark idempotency as failed: %w", err)
		libOpentelemetry.HandleSpanError(span, "redis set failed status failed", wrappedErr)

		logger.Log(ctx, libLog.LevelError, fmt.Sprintf("failed to mark idempotency as failed: %v", wrappedErr))

		return wrappedErr
	}

	return nil
}

// GetCachedResult retrieves the cached result for an idempotency key.
// Returns IdempotencyStatusUnknown if the key does not exist.
func (repo *IdempotencyRepository) GetCachedResult(
	ctx context.Context,
	key value_objects.IdempotencyKey,
) (*value_objects.IdempotencyResult, error) {
	if repo == nil || repo.provider == nil {
		return nil, ErrRepoNotInitialized
	}

	logger, tracer, _, _ := libCommons.NewTrackingFromContext(ctx)
	ctx, span := tracer.Start(ctx, "redis.idempotency.get_cached_result")

	defer span.End()

	connLease, err := repo.provider.GetRedisConnection(ctx)
	if err != nil {
		libOpentelemetry.HandleSpanError(span, "failed to get redis connection", err)
		return nil, fmt.Errorf("get redis connection: %w", err)
	}
	defer connLease.Release()

	conn := connLease.Connection()
	if conn == nil {
		return nil, ErrRedisClientNil
	}

	rdb, err := conn.GetClient(ctx)
	if err != nil {
		libOpentelemetry.HandleSpanError(span, "failed to get redis client", err)
		return nil, fmt.Errorf("%w: %w", ErrRedisClientNil, err)
	}

	redisKey := repo.storageKey(ctx, key)

	data, err := rdb.Get(ctx, redisKey).Bytes()
	if err != nil {
		if errors.Is(err, redis.Nil) {
			return &value_objects.IdempotencyResult{
				Status: value_objects.IdempotencyStatusUnknown,
			}, nil
		}

		wrappedErr := fmt.Errorf("failed to get cached result: %w", err)
		libOpentelemetry.HandleSpanError(span, "redis get failed", wrappedErr)

		logger.Log(ctx, libLog.LevelError, fmt.Sprintf("failed to get cached result: %v", wrappedErr))

		return nil, wrappedErr
	}

	// Handle legacy "pending" marker (plain string, not JSON)
	if string(data) == statusPending {
		return &value_objects.IdempotencyResult{
			Status: value_objects.IdempotencyStatusPending,
		}, nil
	}

	var entry idempotencyCacheEntry
	if err := json.Unmarshal(data, &entry); err != nil {
		libOpentelemetry.HandleSpanError(span, "failed to unmarshal cache entry", err)
		return nil, fmt.Errorf("unmarshal cache entry: %w", err)
	}

	if entry.Status == "" {
		return nil, ErrCacheEntryNoStatus
	}

	return &value_objects.IdempotencyResult{
		Status:     value_objects.IdempotencyStatus(entry.Status),
		Response:   entry.Response,
		HTTPStatus: entry.HTTPStatus,
	}, nil
}

var _ repositories.CallbackIdempotencyRepository = (*IdempotencyRepository)(nil)
