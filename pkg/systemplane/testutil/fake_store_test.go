//go:build unit

// Copyright 2025 Lerian Studio.

package testutil

import (
	"context"
	"testing"

	"github.com/LerianStudio/matcher/pkg/systemplane/domain"
	"github.com/LerianStudio/matcher/pkg/systemplane/ports"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestNewFakeStore_Empty(t *testing.T) {
	t.Parallel()

	store := NewFakeStore()

	require.NotNil(t, store)

	// Verify it satisfies the Store interface.
	var _ ports.Store = store
}

func TestFakeStore_Get_EmptyStore(t *testing.T) {
	t.Parallel()

	store := NewFakeStore()
	target := domain.Target{Kind: domain.KindConfig, Scope: domain.ScopeGlobal}

	result, err := store.Get(context.Background(), target)

	require.NoError(t, err)
	assert.Nil(t, result.Entries)
	assert.Equal(t, domain.RevisionZero, result.Revision)
}

func TestFakeStore_Seed_PopulatesEntries(t *testing.T) {
	t.Parallel()

	store := NewFakeStore()
	target := domain.Target{Kind: domain.KindConfig, Scope: domain.ScopeGlobal}
	entries := []domain.Entry{
		{Key: "key-a", Value: "val-a"},
		{Key: "key-b", Value: "val-b"},
	}
	seedRev := domain.Revision(5)

	store.Seed(target, entries, seedRev)

	result, err := store.Get(context.Background(), target)

	require.NoError(t, err)
	assert.Equal(t, seedRev, result.Revision)
	assert.Len(t, result.Entries, 2)

	// Verify entries are accessible by key.
	found := make(map[string]domain.Entry, len(result.Entries))
	for _, e := range result.Entries {
		found[e.Key] = e
	}

	assert.Equal(t, "val-a", found["key-a"].Value)
	assert.Equal(t, "val-b", found["key-b"].Value)
}

func TestFakeStore_Put_CreatesEntries(t *testing.T) {
	t.Parallel()

	store := NewFakeStore()
	target := domain.Target{Kind: domain.KindConfig, Scope: domain.ScopeGlobal}
	actor := domain.Actor{ID: "user-1"}
	ops := []ports.WriteOp{
		{Key: "color", Value: "blue"},
	}

	newRev, err := store.Put(context.Background(), target, ops, domain.RevisionZero, actor, "test")

	require.NoError(t, err)
	assert.Equal(t, domain.Revision(1), newRev)

	result, err := store.Get(context.Background(), target)

	require.NoError(t, err)
	assert.Len(t, result.Entries, 1)
	assert.Equal(t, "blue", result.Entries[0].Value)
	assert.Equal(t, "user-1", result.Entries[0].UpdatedBy)
	assert.Equal(t, "test", result.Entries[0].Source)
}

func TestFakeStore_Put_RevisionMismatch(t *testing.T) {
	t.Parallel()

	store := NewFakeStore()
	target := domain.Target{Kind: domain.KindConfig, Scope: domain.ScopeGlobal}
	actor := domain.Actor{ID: "user-1"}

	// First write succeeds.
	_, err := store.Put(context.Background(), target, []ports.WriteOp{{Key: "k", Value: "v"}}, domain.RevisionZero, actor, "test")
	require.NoError(t, err)

	// Second write with stale revision fails.
	_, err = store.Put(context.Background(), target, []ports.WriteOp{{Key: "k", Value: "v2"}}, domain.RevisionZero, actor, "test")

	require.ErrorIs(t, err, domain.ErrRevisionMismatch)
}

func TestFakeStore_Put_ResetOpDeletesEntry(t *testing.T) {
	t.Parallel()

	store := NewFakeStore()
	target := domain.Target{Kind: domain.KindConfig, Scope: domain.ScopeGlobal}
	actor := domain.Actor{ID: "user-1"}

	// Create entry.
	rev, err := store.Put(context.Background(), target, []ports.WriteOp{{Key: "k", Value: "v"}}, domain.RevisionZero, actor, "test")
	require.NoError(t, err)

	// Reset entry.
	newRev, err := store.Put(context.Background(), target, []ports.WriteOp{{Key: "k", Reset: true}}, rev, actor, "test")
	require.NoError(t, err)
	assert.Equal(t, rev.Next(), newRev)

	result, err := store.Get(context.Background(), target)
	require.NoError(t, err)
	assert.Empty(t, result.Entries)
}

func TestFakeStore_Put_NilValueDeletesEntry(t *testing.T) {
	t.Parallel()

	store := NewFakeStore()
	target := domain.Target{Kind: domain.KindConfig, Scope: domain.ScopeGlobal}
	actor := domain.Actor{ID: "user-1"}

	// Create entry.
	rev, err := store.Put(context.Background(), target, []ports.WriteOp{{Key: "k", Value: "v"}}, domain.RevisionZero, actor, "test")
	require.NoError(t, err)

	// Delete via nil value.
	newRev, err := store.Put(context.Background(), target, []ports.WriteOp{{Key: "k", Value: nil}}, rev, actor, "test")
	require.NoError(t, err)
	assert.Equal(t, rev.Next(), newRev)

	result, err := store.Get(context.Background(), target)
	require.NoError(t, err)
	assert.Empty(t, result.Entries)
}

func TestFakeStore_Put_EmptyOpsReturnsSameRevision(t *testing.T) {
	t.Parallel()

	store := NewFakeStore()
	target := domain.Target{Kind: domain.KindConfig, Scope: domain.ScopeGlobal}
	actor := domain.Actor{ID: "user-1"}

	rev, err := store.Put(context.Background(), target, []ports.WriteOp{}, domain.RevisionZero, actor, "test")

	require.NoError(t, err)
	assert.Equal(t, domain.RevisionZero, rev)
}

func TestFakeStore_Put_OptimisticConcurrency(t *testing.T) {
	t.Parallel()

	store := NewFakeStore()
	target := domain.Target{Kind: domain.KindConfig, Scope: domain.ScopeGlobal}
	actor := domain.Actor{ID: "user-1"}

	// First writer succeeds.
	rev1, err := store.Put(context.Background(), target, []ports.WriteOp{{Key: "k", Value: "v1"}}, domain.RevisionZero, actor, "test")
	require.NoError(t, err)
	assert.Equal(t, domain.Revision(1), rev1)

	// Second writer using correct revision succeeds.
	rev2, err := store.Put(context.Background(), target, []ports.WriteOp{{Key: "k", Value: "v2"}}, rev1, actor, "test")
	require.NoError(t, err)
	assert.Equal(t, domain.Revision(2), rev2)

	// Third writer using stale revision fails.
	_, err = store.Put(context.Background(), target, []ports.WriteOp{{Key: "k", Value: "v3"}}, rev1, actor, "test")
	require.ErrorIs(t, err, domain.ErrRevisionMismatch)

	// Verify final state.
	result, err := store.Get(context.Background(), target)
	require.NoError(t, err)
	assert.Equal(t, rev2, result.Revision)
	require.Len(t, result.Entries, 1)
	assert.Equal(t, "v2", result.Entries[0].Value)
}

func TestFakeStore_Put_MultipleTargetsIndependent(t *testing.T) {
	t.Parallel()

	store := NewFakeStore()
	targetA := domain.Target{Kind: domain.KindConfig, Scope: domain.ScopeGlobal}
	targetB := domain.Target{Kind: domain.KindSetting, Scope: domain.ScopeGlobal}
	actor := domain.Actor{ID: "user-1"}

	// Write to target A.
	revA, err := store.Put(context.Background(), targetA, []ports.WriteOp{{Key: "k", Value: "a"}}, domain.RevisionZero, actor, "test")
	require.NoError(t, err)

	// Write to target B — independent revision.
	revB, err := store.Put(context.Background(), targetB, []ports.WriteOp{{Key: "k", Value: "b"}}, domain.RevisionZero, actor, "test")
	require.NoError(t, err)

	assert.Equal(t, domain.Revision(1), revA)
	assert.Equal(t, domain.Revision(1), revB)
}

func TestFakeStore_Seed_OverwritesExistingState(t *testing.T) {
	t.Parallel()

	store := NewFakeStore()
	target := domain.Target{Kind: domain.KindConfig, Scope: domain.ScopeGlobal}

	// First seed.
	store.Seed(target, []domain.Entry{{Key: "old", Value: "old-val"}}, domain.Revision(3))

	// Overwrite with new seed.
	store.Seed(target, []domain.Entry{{Key: "new", Value: "new-val"}}, domain.Revision(10))

	result, err := store.Get(context.Background(), target)
	require.NoError(t, err)
	assert.Equal(t, domain.Revision(10), result.Revision)
	require.Len(t, result.Entries, 1)
	assert.Equal(t, "new", result.Entries[0].Key)
}
