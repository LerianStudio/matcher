package http

import (
	"context"
	"fmt"
	"sync"

	libCommons "github.com/LerianStudio/lib-commons/v4/commons"
	libLog "github.com/LerianStudio/lib-commons/v4/commons/log"

	vo "github.com/LerianStudio/matcher/internal/exception/domain/value_objects"
)

// ExceptionIdempotencyRepository is the interface from the exception bounded context.
// This mirrors the CallbackIdempotencyRepository interface.
type ExceptionIdempotencyRepository interface {
	TryAcquire(ctx context.Context, key vo.IdempotencyKey) (acquired bool, err error)
	TryReacquireFromFailed(ctx context.Context, key vo.IdempotencyKey) (acquired bool, err error)
	MarkComplete(ctx context.Context, key vo.IdempotencyKey, response []byte, httpStatus int) error
	MarkFailed(ctx context.Context, key vo.IdempotencyKey) error
	GetCachedResult(ctx context.Context, key vo.IdempotencyKey) (*vo.IdempotencyResult, error)
}

// IdempotencyRepositoryAdapter adapts the exception bounded context's repository
// to the shared middleware's IdempotencyRepository interface.
type IdempotencyRepositoryAdapter struct {
	repo       ExceptionIdempotencyRepository
	nilRepoLog sync.Once
}

// NewIdempotencyRepositoryAdapter creates an adapter wrapping the exception repository.
func NewIdempotencyRepositoryAdapter(
	repo ExceptionIdempotencyRepository,
) *IdempotencyRepositoryAdapter {
	return &IdempotencyRepositoryAdapter{repo: repo}
}

// warnNilRepo logs a one-time warning when the idempotency repository is nil.
func (adapter *IdempotencyRepositoryAdapter) warnNilRepo(ctx context.Context) {
	adapter.nilRepoLog.Do(func() {
		logger, _, _, _ := libCommons.NewTrackingFromContext(ctx)
		logger.Log(ctx, libLog.LevelWarn, "idempotency repository is nil, idempotency protection is disabled")
	})
}

// TryAcquire attempts to acquire an idempotency lock.
func (adapter *IdempotencyRepositoryAdapter) TryAcquire(
	ctx context.Context,
	key IdempotencyKey,
) (bool, error) {
	if adapter.repo == nil {
		adapter.warnNilRepo(ctx)

		return false, nil
	}

	acquired, err := adapter.repo.TryAcquire(ctx, vo.IdempotencyKey(key))
	if err != nil {
		return false, fmt.Errorf("idempotency try acquire: %w", err)
	}

	return acquired, nil
}

// MarkComplete marks the request as successfully processed with the response to cache.
func (adapter *IdempotencyRepositoryAdapter) MarkComplete(
	ctx context.Context,
	key IdempotencyKey,
	response []byte,
	httpStatus int,
) error {
	if adapter.repo == nil {
		adapter.warnNilRepo(ctx)

		return nil
	}

	if err := adapter.repo.MarkComplete(ctx, vo.IdempotencyKey(key), response, httpStatus); err != nil {
		return fmt.Errorf("idempotency mark complete: %w", err)
	}

	return nil
}

// TryReacquireFromFailed attempts an atomic failed->pending reclaim.
func (adapter *IdempotencyRepositoryAdapter) TryReacquireFromFailed(
	ctx context.Context,
	key IdempotencyKey,
) (bool, error) {
	if adapter.repo == nil {
		adapter.warnNilRepo(ctx)

		return false, nil
	}

	acquired, err := adapter.repo.TryReacquireFromFailed(ctx, vo.IdempotencyKey(key))
	if err != nil {
		return false, fmt.Errorf("idempotency reacquire from failed: %w", err)
	}

	return acquired, nil
}

// MarkFailed marks the request as failed so it can be retried.
func (adapter *IdempotencyRepositoryAdapter) MarkFailed(
	ctx context.Context,
	key IdempotencyKey,
) error {
	if adapter.repo == nil {
		adapter.warnNilRepo(ctx)

		return nil
	}

	if err := adapter.repo.MarkFailed(ctx, vo.IdempotencyKey(key)); err != nil {
		return fmt.Errorf("idempotency mark failed: %w", err)
	}

	return nil
}

// GetCachedResult retrieves the cached result for an idempotency key.
func (adapter *IdempotencyRepositoryAdapter) GetCachedResult(
	ctx context.Context,
	key IdempotencyKey,
) (*IdempotencyResult, error) {
	if adapter.repo == nil {
		adapter.warnNilRepo(ctx)

		return &IdempotencyResult{Status: IdempotencyStatusUnknown}, nil
	}

	result, err := adapter.repo.GetCachedResult(ctx, vo.IdempotencyKey(key))
	if err != nil {
		return nil, fmt.Errorf("idempotency get cached result: %w", err)
	}

	if result == nil {
		return &IdempotencyResult{Status: IdempotencyStatusUnknown}, nil
	}

	return &IdempotencyResult{
		Status:     IdempotencyStatus(result.Status),
		Response:   result.Response,
		HTTPStatus: result.HTTPStatus,
	}, nil
}

var _ IdempotencyRepository = (*IdempotencyRepositoryAdapter)(nil)
