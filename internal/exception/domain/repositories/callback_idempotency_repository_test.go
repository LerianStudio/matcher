// Copyright 2025 Lerian Studio. All rights reserved.
// Use of this source code is governed by an Elastic License 2.0
// that can be found in the LICENSE.md file.

//go:build unit

package repositories

import (
	"context"
	"testing"

	shared "github.com/LerianStudio/matcher/internal/shared/domain"
	sharedPorts "github.com/LerianStudio/matcher/internal/shared/ports"
)

type idempotencyRepositoryStub struct{}

func (idempotencyRepositoryStub) TryAcquire(_ context.Context, _ shared.IdempotencyKey) (bool, error) {
	return true, nil
}

func (idempotencyRepositoryStub) TryReacquireFromFailed(_ context.Context, _ shared.IdempotencyKey) (bool, error) {
	return true, nil
}

func (idempotencyRepositoryStub) MarkComplete(
	_ context.Context,
	_ shared.IdempotencyKey,
	_ []byte,
	_ int,
) error {
	return nil
}

func (idempotencyRepositoryStub) MarkFailed(_ context.Context, _ shared.IdempotencyKey) error {
	return nil
}

func (idempotencyRepositoryStub) GetCachedResult(
	_ context.Context,
	_ shared.IdempotencyKey,
) (*shared.IdempotencyResult, error) {
	return nil, nil
}

func TestIdempotencyRepository_Interface(t *testing.T) {
	t.Parallel()

	var repo sharedPorts.IdempotencyRepository = idempotencyRepositoryStub{}
	if repo == nil {
		t.Fatal("expected idempotency repository interface to be assignable")
	}
}
