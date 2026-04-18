// Package redis provides Redis-based implementations for ingestion services.
package redis

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"time"

	"github.com/google/uuid"

	"github.com/LerianStudio/lib-commons/v5/commons/backoff"
	"github.com/LerianStudio/lib-commons/v5/commons/tenant-manager/valkey"

	"github.com/LerianStudio/matcher/internal/ingestion/ports"
	sharedPorts "github.com/LerianStudio/matcher/internal/shared/ports"
)

var (
	errRedisConnRequired  = errors.New("redis connection is required")
	errMaxRetriesExceeded = errors.New("max retries exceeded for dedupe marking")
)

const (
	dedupeKeyPrefix      = "matcher:dedupe"
	retryBackoffBaseTime = 50 * time.Millisecond
)

// DedupeService implements ports.DedupeService.
type DedupeService struct {
	provider sharedPorts.InfrastructureProvider
}

// NewDedupeService creates a new deduplication service.
// If provider is nil, methods will return errRedisConnRequired on use.
func NewDedupeService(provider sharedPorts.InfrastructureProvider) *DedupeService {
	return &DedupeService{provider: provider}
}

// CalculateHash generates SHA256 hash of source_id + external_id
// This matches the idempotency key spec in data-model.md.
func (svc *DedupeService) CalculateHash(sourceID uuid.UUID, externalID string) string {
	data := fmt.Sprintf("%s:%s", sourceID.String(), externalID)
	hash := sha256.Sum256([]byte(data))

	return hex.EncodeToString(hash[:])
}

func (svc *DedupeService) buildKey(ctx context.Context, contextID uuid.UUID, hash string) (string, error) {
	rawKey := dedupeKeyPrefix + ":" + contextID.String() + ":" + hash

	result, err := valkey.GetKeyContext(ctx, rawKey)
	if err != nil {
		return "", fmt.Errorf("build dedupe redis key: %w", err)
	}

	return result, nil
}

// IsDuplicate checks if transaction hash exists in Redis.
func (svc *DedupeService) IsDuplicate(
	ctx context.Context,
	contextID uuid.UUID,
	hash string,
) (bool, error) {
	if svc == nil || svc.provider == nil {
		return false, errRedisConnRequired
	}

	connLease, err := svc.provider.GetRedisConnection(ctx)
	if err != nil {
		return false, fmt.Errorf("failed to get redis connection: %w", err)
	}
	defer connLease.Release()

	conn := connLease.Connection()
	if conn == nil {
		return false, errRedisConnRequired
	}

	rdb, err := conn.GetClient(ctx)
	if err != nil {
		return false, fmt.Errorf("get redis client for dedupe check: %w", err)
	}

	key, err := svc.buildKey(ctx, contextID, hash)
	if err != nil {
		return false, fmt.Errorf("build dedupe key: %w", err)
	}

	exists, err := rdb.Exists(ctx, key).Result()
	if err != nil {
		return false, fmt.Errorf("redis exists check failed: %w", err)
	}

	return exists > 0, nil
}

// MarkSeen records transaction hash with TTL.
func (svc *DedupeService) MarkSeen(
	ctx context.Context,
	contextID uuid.UUID,
	hash string,
	ttl time.Duration,
) error {
	if svc == nil || svc.provider == nil {
		return errRedisConnRequired
	}

	connLease, err := svc.provider.GetRedisConnection(ctx)
	if err != nil {
		return fmt.Errorf("failed to get redis connection: %w", err)
	}
	defer connLease.Release()

	conn := connLease.Connection()
	if conn == nil {
		return errRedisConnRequired
	}

	rdb, err := conn.GetClient(ctx)
	if err != nil {
		return fmt.Errorf("get redis client for mark seen: %w", err)
	}

	// TTL=0 means no expiration per interface contract.
	key, err := svc.buildKey(ctx, contextID, hash)
	if err != nil {
		return fmt.Errorf("build dedupe key: %w", err)
	}

	if err := rdb.Set(ctx, key, "1", ttl).Err(); err != nil {
		return fmt.Errorf("failed to mark seen: %w", err)
	}

	return nil
}

// MarkSeenWithRetry implements retry-safe marking using SETNX.
func (svc *DedupeService) MarkSeenWithRetry(
	ctx context.Context,
	contextID uuid.UUID,
	hash string,
	ttl time.Duration,
	maxRetries int,
) error {
	if svc == nil || svc.provider == nil {
		return errRedisConnRequired
	}

	if maxRetries <= 0 {
		maxRetries = 1
	}

	connLease, err := svc.provider.GetRedisConnection(ctx)
	if err != nil {
		return fmt.Errorf("failed to get redis connection: %w", err)
	}
	defer connLease.Release()

	conn := connLease.Connection()
	if conn == nil {
		return errRedisConnRequired
	}

	rdb, err := conn.GetClient(ctx)
	if err != nil {
		return fmt.Errorf("get redis client for mark seen with retry: %w", err)
	}

	key, err := svc.buildKey(ctx, contextID, hash)
	if err != nil {
		return fmt.Errorf("build dedupe key: %w", err)
	}

	var lastErr error

	for i := 0; i < maxRetries; i++ {
		set, err := rdb.SetNX(ctx, key, "1", ttl).Result()
		if err != nil {
			lastErr = fmt.Errorf("redis setnx failed: %w", err)

			delay := backoff.ExponentialWithJitter(retryBackoffBaseTime, i)
			if sleepErr := backoff.WaitContext(ctx, delay); sleepErr != nil {
				return fmt.Errorf("retry interrupted: %w", sleepErr)
			}

			continue
		}

		if !set {
			return ports.ErrDuplicateTransaction
		}

		return nil
	}

	if lastErr != nil {
		return lastErr
	}

	return errMaxRetriesExceeded
}

// Clear removes a deduplication key from Redis.
func (svc *DedupeService) Clear(ctx context.Context, contextID uuid.UUID, hash string) error {
	if svc == nil || svc.provider == nil {
		return errRedisConnRequired
	}

	connLease, err := svc.provider.GetRedisConnection(ctx)
	if err != nil {
		return fmt.Errorf("failed to get redis connection: %w", err)
	}
	defer connLease.Release()

	conn := connLease.Connection()
	if conn == nil {
		return errRedisConnRequired
	}

	rdb, err := conn.GetClient(ctx)
	if err != nil {
		return fmt.Errorf("get redis client for dedupe clear: %w", err)
	}

	key, err := svc.buildKey(ctx, contextID, hash)
	if err != nil {
		return fmt.Errorf("build dedupe key: %w", err)
	}

	if err := rdb.Del(ctx, key).Err(); err != nil {
		return fmt.Errorf("failed to clear dedupe key: %w", err)
	}

	return nil
}

// ClearBatch removes multiple deduplication keys from Redis.
func (svc *DedupeService) ClearBatch(
	ctx context.Context,
	contextID uuid.UUID,
	hashes []string,
) error {
	if svc == nil || svc.provider == nil {
		return errRedisConnRequired
	}

	if len(hashes) == 0 {
		return nil
	}

	connLease, err := svc.provider.GetRedisConnection(ctx)
	if err != nil {
		return fmt.Errorf("failed to get redis connection: %w", err)
	}
	defer connLease.Release()

	conn := connLease.Connection()
	if conn == nil {
		return errRedisConnRequired
	}

	rdb, err := conn.GetClient(ctx)
	if err != nil {
		return fmt.Errorf("get redis client for dedupe clear batch: %w", err)
	}

	keys := make([]string, len(hashes))
	for i, hash := range hashes {
		key, err := svc.buildKey(ctx, contextID, hash)
		if err != nil {
			return fmt.Errorf("build dedupe key for batch: %w", err)
		}

		keys[i] = key
	}

	if err := rdb.Del(ctx, keys...).Err(); err != nil {
		return fmt.Errorf("failed to clear dedupe keys: %w", err)
	}

	return nil
}
