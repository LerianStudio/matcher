// Copyright 2025 Lerian Studio. All rights reserved.
// Use of this source code is governed by an Elastic License 2.0
// that can be found in the LICENSE.md file.

//go:build unit

package ports

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	shared "github.com/LerianStudio/matcher/internal/shared/domain"
)

type stubIdempotencyRepository struct {
	acquired   bool
	reacquired bool
	result     *shared.IdempotencyResult
}

func (stub stubIdempotencyRepository) TryAcquire(context.Context, shared.IdempotencyKey) (bool, error) {
	return stub.acquired, nil
}

func (stub stubIdempotencyRepository) TryReacquireFromFailed(context.Context, shared.IdempotencyKey) (bool, error) {
	return stub.reacquired, nil
}

func (stub stubIdempotencyRepository) MarkComplete(context.Context, shared.IdempotencyKey, []byte, int) error {
	return nil
}

func (stub stubIdempotencyRepository) MarkFailed(context.Context, shared.IdempotencyKey) error {
	return nil
}

func (stub stubIdempotencyRepository) GetCachedResult(context.Context, shared.IdempotencyKey) (*shared.IdempotencyResult, error) {
	return stub.result, nil
}

var _ IdempotencyRepository = (*stubIdempotencyRepository)(nil)

func TestIdempotencyRepository_StubBehavior(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	key := shared.IdempotencyKey("test-key")
	want := &shared.IdempotencyResult{Status: shared.IdempotencyStatusFailed}
	repo := stubIdempotencyRepository{acquired: true, reacquired: true, result: want}

	acquired, err := repo.TryAcquire(ctx, key)
	require.NoError(t, err)
	assert.True(t, acquired)

	reacquired, err := repo.TryReacquireFromFailed(ctx, key)
	require.NoError(t, err)
	assert.True(t, reacquired)

	result, err := repo.GetCachedResult(ctx, key)
	require.NoError(t, err)
	assert.Equal(t, want, result)
}
