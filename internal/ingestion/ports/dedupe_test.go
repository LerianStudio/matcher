// Copyright 2025 Lerian Studio. All rights reserved.
// Use of this source code is governed by an Elastic License 2.0
// that can be found in the LICENSE.md file.

//go:build unit

package ports

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

var errWrappedDuplicateTransaction = fmt.Errorf("wrapped: %w", ErrDuplicateTransaction)

func TestErrDuplicateTransaction(t *testing.T) {
	t.Parallel()

	require.Error(t, ErrDuplicateTransaction)
	assert.Equal(t, "duplicate transaction detected", ErrDuplicateTransaction.Error())
}

func TestErrDuplicateTransactionIs(t *testing.T) {
	t.Parallel()

	wrappedErr := errWrappedDuplicateTransaction
	require.ErrorIs(t, wrappedErr, ErrDuplicateTransaction)

	directErr := ErrDuplicateTransaction
	require.ErrorIs(t, directErr, ErrDuplicateTransaction)
}

func TestDedupeServiceInterfaceCompiles(t *testing.T) {
	t.Parallel()

	var _ DedupeService = (*mockDedupeService)(nil)
}

type mockDedupeService struct {
	seen map[string]bool
}

func (m *mockDedupeService) CalculateHash(sourceID uuid.UUID, externalID string) string {
	return sourceID.String() + ":" + externalID
}

func (m *mockDedupeService) IsDuplicate(_ context.Context, _ uuid.UUID, hash string) (bool, error) {
	return m.seen[hash], nil
}

func (m *mockDedupeService) MarkSeen(
	_ context.Context,
	_ uuid.UUID,
	hash string,
	_ time.Duration,
) error {
	if m.seen == nil {
		m.seen = make(map[string]bool)
	}

	m.seen[hash] = true

	return nil
}

func (m *mockDedupeService) MarkSeenWithRetry(
	_ context.Context,
	_ uuid.UUID,
	hash string,
	_ time.Duration,
	_ int,
) error {
	if m.seen == nil {
		m.seen = make(map[string]bool)
	}

	m.seen[hash] = true

	return nil
}

func (m *mockDedupeService) MarkSeenBulk(
	_ context.Context,
	_ uuid.UUID,
	hashes []string,
	_ time.Duration,
) (map[string]bool, error) {
	if m.seen == nil {
		m.seen = make(map[string]bool)
	}

	result := make(map[string]bool, len(hashes))

	for _, hash := range hashes {
		if m.seen[hash] {
			result[hash] = false

			continue
		}

		m.seen[hash] = true
		result[hash] = true
	}

	return result, nil
}

func (m *mockDedupeService) Clear(_ context.Context, _ uuid.UUID, hash string) error {
	if m.seen != nil {
		delete(m.seen, hash)
	}

	return nil
}

func (m *mockDedupeService) ClearBatch(_ context.Context, _ uuid.UUID, hashes []string) error {
	if m.seen != nil {
		for _, hash := range hashes {
			delete(m.seen, hash)
		}
	}

	return nil
}

func TestMockDedupeServiceCalculateHash(t *testing.T) {
	t.Parallel()

	service := &mockDedupeService{}
	sourceID := uuid.New()
	externalID := "tx-123"

	hash := service.CalculateHash(sourceID, externalID)

	assert.Equal(t, sourceID.String()+":"+externalID, hash)
}

func TestMockDedupeServiceIsDuplicate(t *testing.T) {
	t.Parallel()

	contextID := uuid.New()

	t.Run("returns false for unseen hash", func(t *testing.T) {
		t.Parallel()

		service := &mockDedupeService{seen: make(map[string]bool)}
		ctx := t.Context()

		isDup, err := service.IsDuplicate(ctx, contextID, "new-hash")
		require.NoError(t, err)
		assert.False(t, isDup)
	})

	t.Run("returns true for seen hash", func(t *testing.T) {
		t.Parallel()

		service := &mockDedupeService{seen: map[string]bool{"existing-hash": true}}
		ctx := t.Context()

		isDup, err := service.IsDuplicate(ctx, contextID, "existing-hash")
		require.NoError(t, err)
		assert.True(t, isDup)
	})
}

func TestMockDedupeServiceMarkSeen(t *testing.T) {
	t.Parallel()

	service := &mockDedupeService{}
	contextID := uuid.New()
	hash := "test-hash"

	ctx := t.Context()
	err := service.MarkSeen(ctx, contextID, hash, time.Hour)
	require.NoError(t, err)

	isDup, err := service.IsDuplicate(ctx, contextID, hash)
	require.NoError(t, err)
	assert.True(t, isDup)
}

func TestMockDedupeServiceMarkSeenWithRetry(t *testing.T) {
	t.Parallel()

	service := &mockDedupeService{}
	contextID := uuid.New()
	hash := "retry-hash"

	ctx := t.Context()
	err := service.MarkSeenWithRetry(ctx, contextID, hash, time.Hour, 3)
	require.NoError(t, err)

	isDup, err := service.IsDuplicate(ctx, contextID, hash)
	require.NoError(t, err)
	assert.True(t, isDup)
}

func TestMockDedupeServiceMarkSeenWithZeroTTL(t *testing.T) {
	t.Parallel()

	service := &mockDedupeService{}
	contextID := uuid.New()
	hash := "no-expiry-hash"

	ctx := t.Context()
	err := service.MarkSeen(ctx, contextID, hash, 0)
	require.NoError(t, err)

	isDup, err := service.IsDuplicate(ctx, contextID, hash)
	require.NoError(t, err)
	assert.True(t, isDup)
}

func TestMockDedupeServiceClear(t *testing.T) {
	t.Parallel()

	service := &mockDedupeService{seen: make(map[string]bool)}
	contextID := uuid.New()
	hash := "clear-hash"

	ctx := t.Context()

	err := service.MarkSeen(ctx, contextID, hash, time.Hour)
	require.NoError(t, err)

	isDup, err := service.IsDuplicate(ctx, contextID, hash)
	require.NoError(t, err)
	assert.True(t, isDup)

	err = service.Clear(ctx, contextID, hash)
	require.NoError(t, err)

	isDup, err = service.IsDuplicate(ctx, contextID, hash)
	require.NoError(t, err)
	assert.False(t, isDup)
}

func TestMockDedupeServiceClearBatch(t *testing.T) {
	t.Parallel()

	service := &mockDedupeService{seen: make(map[string]bool)}
	contextID := uuid.New()
	hashes := []string{"batch-hash-1", "batch-hash-2", "batch-hash-3"}

	ctx := t.Context()

	for _, hash := range hashes {
		err := service.MarkSeen(ctx, contextID, hash, time.Hour)
		require.NoError(t, err)
	}

	for _, hash := range hashes {
		isDup, err := service.IsDuplicate(ctx, contextID, hash)
		require.NoError(t, err)
		assert.True(t, isDup)
	}

	err := service.ClearBatch(ctx, contextID, hashes)
	require.NoError(t, err)

	for _, hash := range hashes {
		isDup, err := service.IsDuplicate(ctx, contextID, hash)
		require.NoError(t, err)
		assert.False(t, isDup)
	}
}

func TestMockDedupeServiceClearBatchEmpty(t *testing.T) {
	t.Parallel()

	service := &mockDedupeService{seen: make(map[string]bool)}
	contextID := uuid.New()

	ctx := t.Context()
	err := service.ClearBatch(ctx, contextID, []string{})
	require.NoError(t, err)
}
