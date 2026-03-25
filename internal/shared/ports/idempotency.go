package ports

import (
	"context"

	shared "github.com/LerianStudio/matcher/internal/shared/domain"
)

// IdempotencyRepository defines the shared storage contract for request
// idempotency across HTTP middleware and callback processing.
type IdempotencyRepository interface {
	TryAcquire(ctx context.Context, key shared.IdempotencyKey) (acquired bool, err error)
	TryReacquireFromFailed(ctx context.Context, key shared.IdempotencyKey) (acquired bool, err error)
	MarkComplete(ctx context.Context, key shared.IdempotencyKey, response []byte, httpStatus int) error
	MarkFailed(ctx context.Context, key shared.IdempotencyKey) error
	GetCachedResult(ctx context.Context, key shared.IdempotencyKey) (*shared.IdempotencyResult, error)
}
