//go:build unit

// Copyright 2025 Lerian Studio.

package changefeed

import (
	"testing"
	"time"

	"github.com/LerianStudio/matcher/pkg/systemplane/domain"
	"github.com/LerianStudio/matcher/pkg/systemplane/ports"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// ---------------------------------------------------------------------------
// debounceState.stopTimer
// ---------------------------------------------------------------------------

func TestDebounceState_StopTimer_NilTimer(t *testing.T) {
	t.Parallel()

	ds := &debounceState{
		pending: make(map[string]*debounceEntry),
	}

	// Should not panic when timer is nil.
	assert.NotPanics(t, func() {
		ds.stopTimer()
	})

	assert.Nil(t, ds.timer)
	assert.Nil(t, ds.timerCh)
}

func TestDebounceState_StopTimer_ActiveTimer(t *testing.T) {
	t.Parallel()

	ds := &debounceState{
		pending: make(map[string]*debounceEntry),
	}

	ds.timer = time.NewTimer(1 * time.Hour)
	ds.timerCh = ds.timer.C

	ds.stopTimer()

	assert.Nil(t, ds.timer)
	assert.Nil(t, ds.timerCh)
}

func TestDebounceState_StopTimer_FiredTimer(t *testing.T) {
	t.Parallel()

	ds := &debounceState{
		pending: make(map[string]*debounceEntry),
	}

	// Create a timer that fires immediately.
	ds.timer = time.NewTimer(0)
	ds.timerCh = ds.timer.C

	// Wait for it to fire.
	time.Sleep(10 * time.Millisecond)

	// stopTimer should drain the channel without blocking.
	ds.stopTimer()

	assert.Nil(t, ds.timer)
	assert.Nil(t, ds.timerCh)
}

// ---------------------------------------------------------------------------
// debounceState.resetTimer
// ---------------------------------------------------------------------------

func TestDebounceState_ResetTimer_NoPending_StopsTimer(t *testing.T) {
	t.Parallel()

	ds := &debounceState{
		pending: make(map[string]*debounceEntry),
	}

	// Pre-set a timer so we can verify it's stopped.
	ds.timer = time.NewTimer(1 * time.Hour)
	ds.timerCh = ds.timer.C

	ds.resetTimer()

	assert.Nil(t, ds.timer)
	assert.Nil(t, ds.timerCh)
}

func TestDebounceState_ResetTimer_WithPending_CreatesTimer(t *testing.T) {
	t.Parallel()

	target := makeTarget(domain.KindConfig, domain.ScopeGlobal, "")

	ds := &debounceState{
		pending: map[string]*debounceEntry{
			target.String(): {
				signal: ports.ChangeSignal{Target: target, Revision: domain.Revision(1)},
				dueAt:  time.Now().Add(50 * time.Millisecond),
			},
		},
	}

	ds.resetTimer()

	assert.NotNil(t, ds.timer)
	assert.NotNil(t, ds.timerCh)

	// Clean up.
	ds.stopTimer()
}

func TestDebounceState_ResetTimer_ResetsExistingTimer(t *testing.T) {
	t.Parallel()

	target := makeTarget(domain.KindConfig, domain.ScopeGlobal, "")

	ds := &debounceState{
		pending: map[string]*debounceEntry{
			target.String(): {
				signal: ports.ChangeSignal{Target: target, Revision: domain.Revision(1)},
				dueAt:  time.Now().Add(1 * time.Hour),
			},
		},
	}

	// First call: creates timer.
	ds.resetTimer()
	firstTimer := ds.timer

	assert.NotNil(t, firstTimer)

	// Update the entry with a sooner due time.
	ds.pending[target.String()].dueAt = time.Now().Add(10 * time.Millisecond)

	// Second call: should reset the same timer (not create a new one).
	ds.resetTimer()

	assert.NotNil(t, ds.timer)
	assert.NotNil(t, ds.timerCh)

	// Clean up.
	ds.stopTimer()
}

func TestDebounceState_ResetTimer_PastDueAt_ZeroDelay(t *testing.T) {
	t.Parallel()

	target := makeTarget(domain.KindConfig, domain.ScopeGlobal, "")

	ds := &debounceState{
		pending: map[string]*debounceEntry{
			target.String(): {
				signal: ports.ChangeSignal{Target: target, Revision: domain.Revision(1)},
				dueAt:  time.Now().Add(-1 * time.Second), // Already past due.
			},
		},
	}

	ds.resetTimer()

	assert.NotNil(t, ds.timer)
	assert.NotNil(t, ds.timerCh)

	// Timer should fire very quickly since delay is clamped to 0.
	select {
	case <-ds.timerCh:
		// Expected: fired immediately.
	case <-time.After(100 * time.Millisecond):
		t.Fatal("timer should have fired immediately for past-due entry")
	}
}

func TestDebounceState_ResetTimer_MultiplePending_UsesEarliest(t *testing.T) {
	t.Parallel()

	targetA := makeTarget(domain.KindConfig, domain.ScopeGlobal, "")
	targetB := makeTarget(domain.KindSetting, domain.ScopeTenant, "tenant-42")

	now := time.Now()

	ds := &debounceState{
		pending: map[string]*debounceEntry{
			targetA.String(): {
				signal: ports.ChangeSignal{Target: targetA, Revision: domain.Revision(1)},
				dueAt:  now.Add(500 * time.Millisecond), // Later.
			},
			targetB.String(): {
				signal: ports.ChangeSignal{Target: targetB, Revision: domain.Revision(2)},
				dueAt:  now.Add(50 * time.Millisecond), // Earlier.
			},
		},
	}

	ds.resetTimer()

	assert.NotNil(t, ds.timer)
	assert.NotNil(t, ds.timerCh)

	// Timer should fire around 50ms, well before 500ms.
	select {
	case <-ds.timerCh:
		// Expected: fires at the earlier deadline.
	case <-time.After(200 * time.Millisecond):
		t.Fatal("timer should have fired near the earliest entry's deadline")
	}
}

// ---------------------------------------------------------------------------
// debounceState.collectSignals
// ---------------------------------------------------------------------------

func TestDebounceState_CollectSignals_Empty(t *testing.T) {
	t.Parallel()

	ds := &debounceState{
		pending: make(map[string]*debounceEntry),
	}

	signals := ds.collectSignals(time.Now(), false)

	assert.Empty(t, signals)
}

func TestDebounceState_CollectSignals_DueEntries(t *testing.T) {
	t.Parallel()

	target := makeTarget(domain.KindConfig, domain.ScopeGlobal, "")
	now := time.Now()

	ds := &debounceState{
		pending: map[string]*debounceEntry{
			target.String(): {
				signal: ports.ChangeSignal{Target: target, Revision: domain.Revision(5)},
				dueAt:  now.Add(-10 * time.Millisecond), // Past due.
			},
		},
	}

	signals := ds.collectSignals(now, false)

	require.Len(t, signals, 1)
	assert.Equal(t, domain.Revision(5), signals[0].Revision)
	assert.Equal(t, target, signals[0].Target)

	// Entry should be removed from pending.
	assert.Empty(t, ds.pending)
}

func TestDebounceState_CollectSignals_FutureEntriesNotCollected(t *testing.T) {
	t.Parallel()

	target := makeTarget(domain.KindConfig, domain.ScopeGlobal, "")
	now := time.Now()

	ds := &debounceState{
		pending: map[string]*debounceEntry{
			target.String(): {
				signal: ports.ChangeSignal{Target: target, Revision: domain.Revision(3)},
				dueAt:  now.Add(1 * time.Hour), // Far in the future.
			},
		},
	}

	signals := ds.collectSignals(now, false)

	assert.Empty(t, signals)
	assert.Len(t, ds.pending, 1, "future entry should remain in pending")
}

func TestDebounceState_CollectSignals_FlushAll(t *testing.T) {
	t.Parallel()

	target := makeTarget(domain.KindConfig, domain.ScopeGlobal, "")
	now := time.Now()

	ds := &debounceState{
		pending: map[string]*debounceEntry{
			target.String(): {
				signal: ports.ChangeSignal{Target: target, Revision: domain.Revision(7)},
				dueAt:  now.Add(1 * time.Hour), // Far in the future.
			},
		},
	}

	signals := ds.collectSignals(now, true)

	require.Len(t, signals, 1, "flushAll should collect even future entries")
	assert.Equal(t, domain.Revision(7), signals[0].Revision)
	assert.Empty(t, ds.pending)
}

func TestDebounceState_CollectSignals_SortedByKey(t *testing.T) {
	t.Parallel()

	targetA := makeTarget(domain.KindConfig, domain.ScopeGlobal, "")
	targetB := makeTarget(domain.KindSetting, domain.ScopeTenant, "tenant-1")
	now := time.Now()

	ds := &debounceState{
		pending: map[string]*debounceEntry{
			targetB.String(): {
				signal: ports.ChangeSignal{Target: targetB, Revision: domain.Revision(2)},
				dueAt:  now.Add(-1 * time.Millisecond),
			},
			targetA.String(): {
				signal: ports.ChangeSignal{Target: targetA, Revision: domain.Revision(1)},
				dueAt:  now.Add(-1 * time.Millisecond),
			},
		},
	}

	signals := ds.collectSignals(now, false)

	require.Len(t, signals, 2)

	// Signals should be sorted by key.
	assert.True(t, signals[0].Target.String() < signals[1].Target.String(),
		"signals should be sorted by key: %q < %q",
		signals[0].Target.String(), signals[1].Target.String())
}

func TestDebounceState_CollectSignals_MixedDueness(t *testing.T) {
	t.Parallel()

	targetDue := makeTarget(domain.KindConfig, domain.ScopeGlobal, "")
	targetFuture := makeTarget(domain.KindSetting, domain.ScopeTenant, "tenant-42")
	now := time.Now()

	ds := &debounceState{
		pending: map[string]*debounceEntry{
			targetDue.String(): {
				signal: ports.ChangeSignal{Target: targetDue, Revision: domain.Revision(1)},
				dueAt:  now.Add(-10 * time.Millisecond), // Due.
			},
			targetFuture.String(): {
				signal: ports.ChangeSignal{Target: targetFuture, Revision: domain.Revision(2)},
				dueAt:  now.Add(1 * time.Hour), // Not due.
			},
		},
	}

	signals := ds.collectSignals(now, false)

	require.Len(t, signals, 1)
	assert.Equal(t, targetDue, signals[0].Target)
	assert.Len(t, ds.pending, 1, "future entry should remain")
	assert.Contains(t, ds.pending, targetFuture.String())
}

// ---------------------------------------------------------------------------
// DebouncedFeed.debounceDuration
// ---------------------------------------------------------------------------

func TestDebounceDuration_NegativeJitter_ReturnsWindow(t *testing.T) {
	t.Parallel()

	df := &DebouncedFeed{window: 200 * time.Millisecond, jitter: -10 * time.Millisecond}

	for range 10 {
		assert.Equal(t, 200*time.Millisecond, df.debounceDuration(),
			"negative jitter should be treated as zero jitter")
	}
}

// ---------------------------------------------------------------------------
// isNilChangeFeed
// ---------------------------------------------------------------------------

func TestIsNilChangeFeed_NilInterface(t *testing.T) {
	t.Parallel()

	assert.True(t, isNilChangeFeed(nil))
}

func TestIsNilChangeFeed_TypedNil(t *testing.T) {
	t.Parallel()

	var f *fakeFeed
	var cf ports.ChangeFeed = f

	assert.True(t, isNilChangeFeed(cf))
}

func TestIsNilChangeFeed_NonNil(t *testing.T) {
	t.Parallel()

	f := &fakeFeed{}

	assert.False(t, isNilChangeFeed(f))
}
