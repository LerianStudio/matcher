package repositories

import (
	"context"

	vo "github.com/LerianStudio/matcher/internal/exception/domain/value_objects"
)

//go:generate mockgen -destination=mocks/callback_idempotency_repository_mock.go -package=mocks . CallbackIdempotencyRepository

// CallbackIdempotencyRepository manages callback idempotency keys.
type CallbackIdempotencyRepository interface {
	// TryAcquire attempts to acquire an idempotency lock. Returns true if acquired (first time).
	TryAcquire(ctx context.Context, key vo.IdempotencyKey) (acquired bool, err error)
	// MarkComplete marks the callback as successfully processed with the response to cache.
	MarkComplete(ctx context.Context, key vo.IdempotencyKey, response []byte, httpStatus int) error
	// MarkFailed releases or marks a failed callback for retry.
	MarkFailed(ctx context.Context, key vo.IdempotencyKey) error
	// GetCachedResult retrieves the cached result for an idempotency key.
	// Returns IdempotencyStatusUnknown if the key does not exist.
	GetCachedResult(ctx context.Context, key vo.IdempotencyKey) (*vo.IdempotencyResult, error)
}
