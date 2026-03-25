//go:build unit

package repositories

import (
	"context"
	"testing"

	shared "github.com/LerianStudio/matcher/internal/shared/domain"
)

type callbackIdempotencyRepositoryStub struct{}

func (callbackIdempotencyRepositoryStub) TryAcquire(_ context.Context, _ shared.IdempotencyKey) (bool, error) {
	return true, nil
}

func (callbackIdempotencyRepositoryStub) TryReacquireFromFailed(_ context.Context, _ shared.IdempotencyKey) (bool, error) {
	return true, nil
}

func (callbackIdempotencyRepositoryStub) MarkComplete(
	_ context.Context,
	_ shared.IdempotencyKey,
	_ []byte,
	_ int,
) error {
	return nil
}

func (callbackIdempotencyRepositoryStub) MarkFailed(_ context.Context, _ shared.IdempotencyKey) error {
	return nil
}

func (callbackIdempotencyRepositoryStub) GetCachedResult(
	_ context.Context,
	_ shared.IdempotencyKey,
) (*shared.IdempotencyResult, error) {
	return nil, nil
}

func TestCallbackIdempotencyRepository_TypeAlias(t *testing.T) {
	t.Parallel()

	var repo CallbackIdempotencyRepository = callbackIdempotencyRepositoryStub{}
	if repo == nil {
		t.Fatal("expected callback idempotency repository alias to be assignable")
	}
}
