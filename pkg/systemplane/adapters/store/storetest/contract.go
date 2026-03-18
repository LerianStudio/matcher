// Copyright 2025 Lerian Studio.

// Package storetest provides shared contract tests that validate any
// ports.Store + ports.HistoryStore implementation behaves correctly.
// Concrete adapters (PostgreSQL, MongoDB, FakeStore, etc.) run the
// full suite by supplying a Factory that creates fresh, isolated
// instances for each test.
package storetest

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/LerianStudio/lib-commons/v4/commons/runtime"

	"github.com/LerianStudio/matcher/pkg/systemplane/domain"
	"github.com/LerianStudio/matcher/pkg/systemplane/ports"
	"github.com/LerianStudio/matcher/pkg/systemplane/testutil"
)

const (
	firstRevision  = domain.Revision(1)
	secondRevision = domain.Revision(2)
	thirdRevision  = domain.Revision(3)

	maxRetriesValue                = 5
	timeoutMSInitialValue          = 1000
	timeoutMSUpdatedValue          = 2000
	timeoutMSBatchValue            = 3000
	cacheTTLValue                  = 60
	batchEntriesCount              = 3
	globalHistoryCount             = 2
	historyEntriesCount            = 5
	paginationPageSize             = 2
	paginationOffset               = 2
	paginationOffsetLast           = 4
	paginationLastPageSize         = 10
	paginationOffsetBeyond         = 100
	concurrentGoroutines           = 10
	noUnexpectedErrors             = 0
	expectedBatchHistoryEntryCount = 2
)

// Factory creates fresh Store + HistoryStore instances for each test.
// Each call must return isolated instances (no shared state between tests).
// The cleanup function allows integration tests to tear down resources.
type Factory func(t *testing.T) (ports.Store, ports.HistoryStore, func())

// CombinedFakeStore wraps FakeStore and FakeHistoryStore so that Put()
// automatically records history entries in the FakeHistoryStore.
type CombinedFakeStore struct {
	*testutil.FakeStore
	History *testutil.FakeHistoryStore
}

// NewCombinedFakeStore creates a CombinedFakeStore with fresh underlying fakes.
func NewCombinedFakeStore() *CombinedFakeStore {
	return &CombinedFakeStore{
		FakeStore: testutil.NewFakeStore(),
		History:   testutil.NewFakeHistoryStore(),
	}
}

// Put applies writes to the underlying FakeStore and, on success, appends
// corresponding HistoryEntry records to the FakeHistoryStore.
//
// The method captures old values before the mutation so history entries
// contain accurate OldValue/NewValue pairs.
func (combinedStore *CombinedFakeStore) Put(ctx context.Context, target domain.Target, ops []ports.WriteOp,
	expected domain.Revision, actor domain.Actor, source string,
) (domain.Revision, error) {
	// Capture old values before the mutation.
	oldResult, err := combinedStore.Get(ctx, target)
	if err != nil {
		return domain.RevisionZero, fmt.Errorf("combined fake store get old values: %w", err)
	}

	oldByKey := make(map[string]any, len(oldResult.Entries))
	for _, e := range oldResult.Entries {
		oldByKey[e.Key] = e.Value
	}

	// Apply the mutation.
	newRev, err := combinedStore.FakeStore.Put(ctx, target, ops, expected, actor, source)
	if err != nil {
		return newRev, fmt.Errorf("combined fake store put: %w", err)
	}

	// Record history entries for each op.
	now := time.Now().UTC()

	for _, op := range ops {
		entry := ports.HistoryEntry{
			Revision:  newRev,
			Key:       op.Key,
			Scope:     target.Scope,
			SubjectID: target.SubjectID,
			OldValue:  oldByKey[op.Key],
			ActorID:   actor.ID,
			ChangedAt: now,
		}

		if op.Reset || domain.IsNilValue(op.Value) {
			entry.NewValue = nil
		} else {
			entry.NewValue = op.Value
		}

		combinedStore.History.AppendForKind(target.Kind, entry)
	}

	return newRev, nil
}

// ---------------------------------------------------------------------------.
// Test helpers
// ---------------------------------------------------------------------------.

// globalTarget returns a validated global config target for use in tests.
func globalTarget(t *testing.T) domain.Target {
	t.Helper()

	target, err := domain.NewTarget(domain.KindConfig, domain.ScopeGlobal, "")
	require.NoError(t, err)

	return target
}

// testActor returns a fixed actor for use in tests.
func testActor() domain.Actor {
	return domain.Actor{ID: "test-user"}
}

// findEntry searches for an entry by key in a slice. Returns nil when absent.
func findEntry(entries []domain.Entry, key string) *domain.Entry {
	for i := range entries {
		if entries[i].Key == key {
			return &entries[i]
		}
	}

	return nil
}

// ---------------------------------------------------------------------------.
// Store contract tests
// ---------------------------------------------------------------------------.

// TestGetEmptyTarget verifies that Get on an unwritten target returns
// nil entries and RevisionZero.
func TestGetEmptyTarget(t *testing.T, factory Factory) {
	store, _, cleanup := factory(t)
	defer cleanup()

	ctx := context.Background()
	target := globalTarget(t)

	result, err := store.Get(ctx, target)

	require.NoError(t, err)
	assert.Nil(t, result.Entries)
	assert.Equal(t, domain.RevisionZero, result.Revision)
}

// TestPutSingleOp verifies that a single key write can be read back correctly.
func TestPutSingleOp(t *testing.T, factory Factory) {
	store, _, cleanup := factory(t)
	defer cleanup()

	ctx := context.Background()
	target := globalTarget(t)
	actor := testActor()

	ops := []ports.WriteOp{
		{Key: "max_retries", Value: maxRetriesValue},
	}

	rev, err := store.Put(ctx, target, ops, domain.RevisionZero, actor, "api")
	require.NoError(t, err)
	assert.Equal(t, firstRevision, rev)

	result, err := store.Get(ctx, target)
	require.NoError(t, err)
	assert.Equal(t, firstRevision, result.Revision)
	require.Len(t, result.Entries, 1)
	assert.Equal(t, "max_retries", result.Entries[0].Key)
	assert.Equal(t, maxRetriesValue, result.Entries[0].Value)
	assert.Equal(t, "test-user", result.Entries[0].UpdatedBy)
	assert.Equal(t, "api", result.Entries[0].Source)
}

// TestPutBatch verifies that multiple ops in one Put share the same revision.
func TestPutBatch(t *testing.T, factory Factory) {
	store, _, cleanup := factory(t)
	defer cleanup()

	ctx := context.Background()
	target := globalTarget(t)
	actor := testActor()

	ops := []ports.WriteOp{
		{Key: "timeout_ms", Value: timeoutMSBatchValue},
		{Key: "max_retries", Value: maxRetriesValue},
		{Key: "log_level", Value: "debug"},
	}

	rev, err := store.Put(ctx, target, ops, domain.RevisionZero, actor, "api")
	require.NoError(t, err)
	assert.Equal(t, firstRevision, rev)

	result, err := store.Get(ctx, target)
	require.NoError(t, err)
	assert.Equal(t, firstRevision, result.Revision)
	require.Len(t, result.Entries, batchEntriesCount)

	// All entries must share the same revision.
	for _, e := range result.Entries {
		assert.Equal(t, firstRevision, e.Revision,
			"entry %q should have revision 1", e.Key)
	}
}

// TestOptimisticConcurrency verifies that a mismatched revision returns
// ErrRevisionMismatch along with the current revision.
func TestOptimisticConcurrency(t *testing.T, factory Factory) {
	store, _, cleanup := factory(t)
	defer cleanup()

	ctx := context.Background()
	target := globalTarget(t)
	actor := testActor()

	// First write succeeds.
	rev, err := store.Put(ctx, target, []ports.WriteOp{
		{Key: "timeout_ms", Value: timeoutMSInitialValue},
	}, domain.RevisionZero, actor, "api")
	require.NoError(t, err)
	assert.Equal(t, firstRevision, rev)

	// Attempt with stale revision (0) must fail.
	currentRev, err := store.Put(ctx, target, []ports.WriteOp{
		{Key: "timeout_ms", Value: timeoutMSUpdatedValue},
	}, domain.RevisionZero, actor, "api")

	require.Error(t, err)
	require.ErrorIs(t, err, domain.ErrRevisionMismatch,
		"expected ErrRevisionMismatch")
	assert.Equal(t, firstRevision, currentRev,
		"on conflict, Put must return the current revision")

	// Original value must be unchanged.
	result, err := store.Get(ctx, target)
	require.NoError(t, err)

	entry := findEntry(result.Entries, "timeout_ms")
	require.NotNil(t, entry)
	assert.Equal(t, timeoutMSInitialValue, entry.Value)
}

// TestResetOp verifies that an op with Reset=true removes the entry.
func TestResetOp(t *testing.T, factory Factory) {
	store, _, cleanup := factory(t)
	defer cleanup()

	ctx := context.Background()
	target := globalTarget(t)
	actor := testActor()

	// Write a value.
	rev, err := store.Put(ctx, target, []ports.WriteOp{
		{Key: "feature_flag", Value: true},
	}, domain.RevisionZero, actor, "api")
	require.NoError(t, err)

	// Reset it.
	rev2, err := store.Put(ctx, target, []ports.WriteOp{
		{Key: "feature_flag", Reset: true},
	}, rev, actor, "api")
	require.NoError(t, err)
	assert.Equal(t, secondRevision, rev2)

	// Entry must be gone.
	result, err := store.Get(ctx, target)
	require.NoError(t, err)
	assert.Nil(t, findEntry(result.Entries, "feature_flag"),
		"reset entry should not appear in Get results")
}

// TestNilValueOp verifies that Value=nil removes the entry (same as Reset).
func TestNilValueOp(t *testing.T, factory Factory) {
	store, _, cleanup := factory(t)
	defer cleanup()

	ctx := context.Background()
	target := globalTarget(t)
	actor := testActor()

	// Write a value.
	rev, err := store.Put(ctx, target, []ports.WriteOp{
		{Key: "cache_ttl", Value: cacheTTLValue},
	}, domain.RevisionZero, actor, "api")
	require.NoError(t, err)

	// Write nil to remove it.
	rev2, err := store.Put(ctx, target, []ports.WriteOp{
		{Key: "cache_ttl", Value: nil},
	}, rev, actor, "api")
	require.NoError(t, err)
	assert.Equal(t, secondRevision, rev2)

	// Entry must be gone.
	result, err := store.Get(ctx, target)
	require.NoError(t, err)
	assert.Nil(t, findEntry(result.Entries, "cache_ttl"),
		"nil-value entry should not appear in Get results")
}

// TestEmptyBatchIsNoOp verifies that an empty Put does not advance revision or write history.
func TestEmptyBatchIsNoOp(t *testing.T, factory Factory) {
	store, history, cleanup := factory(t)
	defer cleanup()

	ctx := context.Background()
	target := globalTarget(t)
	actor := testActor()

	rev, err := store.Put(ctx, target, []ports.WriteOp{{Key: "feature_flag", Value: true}}, domain.RevisionZero, actor, "api")
	require.NoError(t, err)

	rev2, err := store.Put(ctx, target, nil, rev, actor, "api")
	require.NoError(t, err)
	assert.Equal(t, rev, rev2)

	result, err := store.Get(ctx, target)
	require.NoError(t, err)
	require.Len(t, result.Entries, 1)
	assert.Equal(t, rev, result.Revision)

	entries, err := history.ListHistory(ctx, ports.HistoryFilter{Key: "feature_flag"})
	require.NoError(t, err)
	require.Len(t, entries, 1)
	assert.Equal(t, rev, entries[0].Revision)
}

// TestTypedNilValueOp verifies that a typed-nil Value removes the entry.
func TestTypedNilValueOp(t *testing.T, factory Factory) {
	store, _, cleanup := factory(t)
	defer cleanup()

	type typedNilPayload struct {
		Enabled bool
	}

	ctx := context.Background()
	target := globalTarget(t)
	actor := testActor()

	rev, err := store.Put(ctx, target, []ports.WriteOp{{Key: "cache_ttl", Value: cacheTTLValue}}, domain.RevisionZero, actor, "api")
	require.NoError(t, err)

	var typedNilValue *typedNilPayload

	rev2, err := store.Put(ctx, target, []ports.WriteOp{{Key: "cache_ttl", Value: typedNilValue}}, rev, actor, "api")
	require.NoError(t, err)
	assert.Equal(t, secondRevision, rev2)

	result, err := store.Get(ctx, target)
	require.NoError(t, err)
	assert.Nil(t, findEntry(result.Entries, "cache_ttl"),
		"typed-nil value should be treated as a reset and not appear in Get results")
}

// TestPutPreservesOtherKeys verifies that writing key A does not affect key B.
func TestPutPreservesOtherKeys(t *testing.T, factory Factory) {
	store, _, cleanup := factory(t)
	defer cleanup()

	ctx := context.Background()
	target := globalTarget(t)
	actor := testActor()

	// Write two keys.
	rev, err := store.Put(ctx, target, []ports.WriteOp{
		{Key: "key_a", Value: "alpha"},
		{Key: "key_b", Value: "bravo"},
	}, domain.RevisionZero, actor, "api")
	require.NoError(t, err)

	// Update only key_a.
	_, err = store.Put(ctx, target, []ports.WriteOp{
		{Key: "key_a", Value: "alpha_updated"},
	}, rev, actor, "api")
	require.NoError(t, err)

	// key_b must be untouched.
	result, err := store.Get(ctx, target)
	require.NoError(t, err)

	entryB := findEntry(result.Entries, "key_b")
	require.NotNil(t, entryB, "key_b must still exist")
	assert.Equal(t, "bravo", entryB.Value)

	entryA := findEntry(result.Entries, "key_a")
	require.NotNil(t, entryA)
	assert.Equal(t, "alpha_updated", entryA.Value)
}

// TestRevisionMonotonicallyIncreasing verifies that multiple Puts always
// increment the revision.
func TestRevisionMonotonicallyIncreasing(t *testing.T, factory Factory) {
	store, _, cleanup := factory(t)
	defer cleanup()

	ctx := context.Background()
	target := globalTarget(t)
	actor := testActor()

	var prevRev domain.Revision

	for i := 1; i <= 5; i++ {
		rev, err := store.Put(ctx, target, []ports.WriteOp{
			{Key: "counter", Value: i},
		}, prevRev, actor, "api")
		require.NoError(t, err)
		assert.Equal(t, domain.Revision(i), rev,
			"revision should be %d after Put #%d", i, i)
		assert.Greater(t, rev, prevRev,
			"revision must be strictly greater than previous")

		prevRev = rev
	}
}

// TestConcurrentPuts verifies goroutine safety: many concurrent Put attempts
// on the same target, where exactly one must win per round.
func TestConcurrentPuts(t *testing.T, factory Factory) {
	store, _, cleanup := factory(t)
	defer cleanup()

	ctx := context.Background()
	target := globalTarget(t)
	actor := testActor()

	const goroutines = concurrentGoroutines

	var (
		wg          sync.WaitGroup
		muResult    sync.Mutex
		successes   int
		conflicts   int
		errorsOther int
	)

	wg.Add(goroutines)

	for i := 0; i < goroutines; i++ {
		value := i

		runtime.SafeGo(nil, "storetest.concurrent_put", runtime.KeepRunning, func() {
			defer wg.Done()

			_, err := store.Put(ctx, target, []ports.WriteOp{
				{Key: "race_key", Value: value},
			}, domain.RevisionZero, actor, "api")

			muResult.Lock()
			defer muResult.Unlock()

			switch {
			case err == nil:
				successes++
			case errors.Is(err, domain.ErrRevisionMismatch):
				conflicts++
			default:
				errorsOther++
			}
		})
	}

	wg.Wait()

	assert.Equal(t, 1, successes,
		"exactly one goroutine should win the first Put")
	assert.Equal(t, goroutines-1, conflicts,
		"all other goroutines should receive ErrRevisionMismatch")
	assert.Equal(t, noUnexpectedErrors, errorsOther,
		"no unexpected errors should occur")

	// Verify the store has exactly one entry at revision 1.
	result, err := store.Get(ctx, target)
	require.NoError(t, err)
	assert.Equal(t, firstRevision, result.Revision)
	require.Len(t, result.Entries, 1)
}

// ---------------------------------------------------------------------------.
// History contract tests
// ---------------------------------------------------------------------------.

// TestHistoryRecording verifies that after Put, history has entries with
// correct old/new values.
func TestHistoryRecording(t *testing.T, factory Factory) {
	store, history, cleanup := factory(t)
	defer cleanup()

	ctx := context.Background()
	target := globalTarget(t)
	actor := testActor()

	// First write: old value is nil.
	rev, err := store.Put(ctx, target, []ports.WriteOp{
		{Key: "timeout_ms", Value: timeoutMSInitialValue},
	}, domain.RevisionZero, actor, "api")
	require.NoError(t, err)

	entries, err := history.ListHistory(ctx, ports.HistoryFilter{
		Key: "timeout_ms",
	})
	require.NoError(t, err)
	require.Len(t, entries, 1)

	assert.Equal(t, rev, entries[0].Revision)
	assert.Equal(t, "timeout_ms", entries[0].Key)
	assert.Nil(t, entries[0].OldValue)
	assert.Equal(t, timeoutMSInitialValue, entries[0].NewValue)
	assert.Equal(t, "test-user", entries[0].ActorID)

	// Second write: old value is 1000, new value is 2000.
	rev2, err := store.Put(ctx, target, []ports.WriteOp{
		{Key: "timeout_ms", Value: timeoutMSUpdatedValue},
	}, rev, actor, "api")
	require.NoError(t, err)

	entries, err = history.ListHistory(ctx, ports.HistoryFilter{
		Key: "timeout_ms",
	})
	require.NoError(t, err)
	require.Len(t, entries, globalHistoryCount)

	// Newest first.
	assert.Equal(t, rev2, entries[0].Revision)
	assert.Equal(t, timeoutMSInitialValue, entries[0].OldValue)
	assert.Equal(t, timeoutMSUpdatedValue, entries[0].NewValue)

	// Reset: new value is nil.
	rev3, err := store.Put(ctx, target, []ports.WriteOp{
		{Key: "timeout_ms", Reset: true},
	}, rev2, actor, "api")
	require.NoError(t, err)

	entries, err = history.ListHistory(ctx, ports.HistoryFilter{
		Key: "timeout_ms",
	})
	require.NoError(t, err)
	require.Len(t, entries, batchEntriesCount)

	assert.Equal(t, rev3, entries[0].Revision)
	assert.Equal(t, timeoutMSUpdatedValue, entries[0].OldValue)
	assert.Nil(t, entries[0].NewValue)
}

// TestBatchHistoryConsistency verifies that a multi-key Put records one history
// entry per key with a shared revision and correct old/new values.
func TestBatchHistoryConsistency(t *testing.T, factory Factory) {
	store, history, cleanup := factory(t)
	defer cleanup()

	ctx := context.Background()
	target := globalTarget(t)
	actor := testActor()

	rev, err := store.Put(ctx, target, []ports.WriteOp{{Key: "a", Value: "alpha"}, {Key: "b", Value: "bravo"}}, domain.RevisionZero, actor, "api")
	require.NoError(t, err)

	entries, err := history.ListHistory(ctx, ports.HistoryFilter{Scope: domain.ScopeGlobal})
	require.NoError(t, err)
	require.Len(t, entries, expectedBatchHistoryEntryCount)

	assert.Equal(t, rev, entries[0].Revision)
	assert.Equal(t, rev, entries[1].Revision)
	assert.ElementsMatch(t, []string{"a", "b"}, []string{entries[0].Key, entries[1].Key})
	assert.Nil(t, entries[0].OldValue)
	assert.Nil(t, entries[1].OldValue)

	newValues := map[string]any{}
	for _, entry := range entries {
		newValues[entry.Key] = entry.NewValue
	}

	assert.Equal(t, "alpha", newValues["a"])
	assert.Equal(t, "bravo", newValues["b"])
}

// TestHistoryFiltering verifies that ListHistory filters by key, scope, etc.
func TestHistoryFiltering(t *testing.T, factory Factory) {
	store, history, cleanup := factory(t)
	defer cleanup()

	ctx := context.Background()
	actor := testActor()

	globalTgt := globalTarget(t)

	tenantTgt, err := domain.NewTarget(domain.KindSetting, domain.ScopeTenant, "tenant-abc")
	require.NoError(t, err)

	// Write to global target.
	rev1, err := store.Put(ctx, globalTgt, []ports.WriteOp{
		{Key: "global_key", Value: "gv1"},
	}, domain.RevisionZero, actor, "api")
	require.NoError(t, err)

	// Write another key to global target.
	_, err = store.Put(ctx, globalTgt, []ports.WriteOp{
		{Key: "other_global_key", Value: "gv2"},
	}, rev1, actor, "api")
	require.NoError(t, err)

	// Write to tenant target.
	_, err = store.Put(ctx, tenantTgt, []ports.WriteOp{
		{Key: "tenant_key", Value: "tv1"},
	}, domain.RevisionZero, actor, "api")
	require.NoError(t, err)

	// Filter by key: only "global_key".
	entries, err := history.ListHistory(ctx, ports.HistoryFilter{
		Key: "global_key",
	})
	require.NoError(t, err)
	require.Len(t, entries, 1)
	assert.Equal(t, "global_key", entries[0].Key)

	// Filter by scope: only global entries.
	entries, err = history.ListHistory(ctx, ports.HistoryFilter{
		Scope: domain.ScopeGlobal,
	})
	require.NoError(t, err)
	assert.Len(t, entries, globalHistoryCount, "should have 2 global entries")

	for _, e := range entries {
		assert.Equal(t, domain.ScopeGlobal, e.Scope)
	}

	// Filter by scope: only tenant entries.
	entries, err = history.ListHistory(ctx, ports.HistoryFilter{
		Scope: domain.ScopeTenant,
	})
	require.NoError(t, err)
	require.Len(t, entries, 1)
	assert.Equal(t, "tenant_key", entries[0].Key)

	// Filter by subject ID.
	entries, err = history.ListHistory(ctx, ports.HistoryFilter{
		SubjectID: "tenant-abc",
	})
	require.NoError(t, err)
	require.Len(t, entries, 1)
	assert.Equal(t, "tenant-abc", entries[0].SubjectID)

	// Combined filter: scope + key that matches nothing.
	entries, err = history.ListHistory(ctx, ports.HistoryFilter{
		Scope: domain.ScopeTenant,
		Key:   "global_key",
	})
	require.NoError(t, err)
	assert.Empty(t, entries)
}

// TestHistoryPagination verifies that Limit and Offset work correctly.
func TestHistoryPagination(t *testing.T, factory Factory) {
	store, history, cleanup := factory(t)
	defer cleanup()

	ctx := context.Background()
	target := globalTarget(t)
	actor := testActor()

	// Create 5 history entries via sequential writes.
	var prevRev domain.Revision

	for i := 1; i <= historyEntriesCount; i++ {
		rev, err := store.Put(ctx, target, []ports.WriteOp{
			{Key: "counter", Value: i},
		}, prevRev, actor, "api")
		require.NoError(t, err)

		prevRev = rev
	}

	// No filter: all 5 entries, newest first.
	all, err := history.ListHistory(ctx, ports.HistoryFilter{
		Key: "counter",
	})
	require.NoError(t, err)
	require.Len(t, all, historyEntriesCount)

	// Verify newest-first ordering.
	for i := 0; i < len(all)-1; i++ {
		assert.GreaterOrEqual(t, all[i].Revision.Uint64(), all[i+1].Revision.Uint64(),
			"history must be newest-first")
	}

	// Limit=2: only the 2 most recent entries.
	page1, err := history.ListHistory(ctx, ports.HistoryFilter{
		Key:   "counter",
		Limit: paginationPageSize,
	})
	require.NoError(t, err)
	require.Len(t, page1, paginationPageSize)
	assert.Equal(t, all[0].Revision, page1[0].Revision)
	assert.Equal(t, all[1].Revision, page1[1].Revision)

	// Offset=2, Limit=2: entries at positions 2 and 3 (0-indexed).
	page2, err := history.ListHistory(ctx, ports.HistoryFilter{
		Key:    "counter",
		Offset: paginationOffset,
		Limit:  paginationPageSize,
	})
	require.NoError(t, err)
	require.Len(t, page2, paginationPageSize)
	assert.Equal(t, all[2].Revision, page2[0].Revision)
	assert.Equal(t, all[3].Revision, page2[1].Revision)

	// Offset=4, Limit=10: only 1 entry remaining.
	page3, err := history.ListHistory(ctx, ports.HistoryFilter{
		Key:    "counter",
		Offset: paginationOffsetLast,
		Limit:  paginationLastPageSize,
	})
	require.NoError(t, err)
	require.Len(t, page3, 1)
	assert.Equal(t, all[4].Revision, page3[0].Revision)

	// Offset beyond total: empty result.
	empty, err := history.ListHistory(ctx, ports.HistoryFilter{
		Key:    "counter",
		Offset: paginationOffsetBeyond,
	})
	require.NoError(t, err)
	assert.Empty(t, empty)
}

// ---------------------------------------------------------------------------.
// RunAll convenience
// ---------------------------------------------------------------------------.

// RunAll runs every contract test in the suite against the provided factory.
// It uses t.Run for clear sub-test grouping.
func RunAll(t *testing.T, factory Factory) {
	t.Helper()

	// Store contract tests.
	t.Run("Store", func(t *testing.T) {
		t.Run("GetEmptyTarget", func(t *testing.T) { TestGetEmptyTarget(t, factory) })
		t.Run("PutSingleOp", func(t *testing.T) { TestPutSingleOp(t, factory) })
		t.Run("PutBatch", func(t *testing.T) { TestPutBatch(t, factory) })
		t.Run("OptimisticConcurrency", func(t *testing.T) { TestOptimisticConcurrency(t, factory) })
		t.Run("ResetOp", func(t *testing.T) { TestResetOp(t, factory) })
		t.Run("NilValueOp", func(t *testing.T) { TestNilValueOp(t, factory) })
		t.Run("EmptyBatchIsNoOp", func(t *testing.T) { TestEmptyBatchIsNoOp(t, factory) })
		t.Run("TypedNilValueOp", func(t *testing.T) { TestTypedNilValueOp(t, factory) })
		t.Run("PutPreservesOtherKeys", func(t *testing.T) { TestPutPreservesOtherKeys(t, factory) })
		t.Run("RevisionMonotonicallyIncreasing", func(t *testing.T) { TestRevisionMonotonicallyIncreasing(t, factory) })
		t.Run("ConcurrentPuts", func(t *testing.T) { TestConcurrentPuts(t, factory) })
	})

	// History contract tests.
	t.Run("History", func(t *testing.T) {
		t.Run("HistoryRecording", func(t *testing.T) { TestHistoryRecording(t, factory) })
		t.Run("BatchHistoryConsistency", func(t *testing.T) { TestBatchHistoryConsistency(t, factory) })
		t.Run("HistoryFiltering", func(t *testing.T) { TestHistoryFiltering(t, factory) })
		t.Run("HistoryPagination", func(t *testing.T) { TestHistoryPagination(t, factory) })
	})
}
