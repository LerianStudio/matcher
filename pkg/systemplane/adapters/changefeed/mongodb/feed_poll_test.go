//go:build unit

// Copyright 2025 Lerian Studio.

package mongodb

import (
	"testing"
	"time"

	"github.com/LerianStudio/matcher/pkg/systemplane/bootstrap"
	"github.com/LerianStudio/matcher/pkg/systemplane/domain"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// ---------------------------------------------------------------------------
// pollSnapshot struct
// ---------------------------------------------------------------------------

func TestPollSnapshot_Fields(t *testing.T) {
	t.Parallel()

	target, err := domain.NewTarget(domain.KindConfig, domain.ScopeGlobal, "")
	require.NoError(t, err)

	snap := pollSnapshot{
		Target:        target,
		Revision:      domain.Revision(42),
		ApplyBehavior: domain.ApplyWorkerReconcile,
	}

	assert.Equal(t, domain.KindConfig, snap.Target.Kind)
	assert.Equal(t, domain.ScopeGlobal, snap.Target.Scope)
	assert.Equal(t, domain.Revision(42), snap.Revision)
	assert.Equal(t, domain.ApplyWorkerReconcile, snap.ApplyBehavior)
}

func TestPollSnapshot_ZeroValue(t *testing.T) {
	t.Parallel()

	var snap pollSnapshot

	assert.Equal(t, domain.Target{}, snap.Target)
	assert.Equal(t, domain.RevisionZero, snap.Revision)
	assert.Equal(t, domain.ApplyBehavior(""), snap.ApplyBehavior)
}

// ---------------------------------------------------------------------------
// Poll mode — revision change detection (unit-level logic verification)
// ---------------------------------------------------------------------------

func TestPollRevisionChangeDetection_NewTarget(t *testing.T) {
	t.Parallel()

	target, err := domain.NewTarget(domain.KindConfig, domain.ScopeGlobal, "")
	require.NoError(t, err)

	key := target.String()

	known := map[string]pollSnapshot{}

	current := pollSnapshot{
		Target:   target,
		Revision: domain.Revision(1),
	}

	// A new target (not in known) should trigger a signal.
	_, exists := known[key]
	assert.False(t, exists)

	// After processing, it should be added to known.
	known[key] = current
	assert.Equal(t, domain.Revision(1), known[key].Revision)
}

func TestPollRevisionChangeDetection_RevisionAdvanced(t *testing.T) {
	t.Parallel()

	target, err := domain.NewTarget(domain.KindConfig, domain.ScopeGlobal, "")
	require.NoError(t, err)

	key := target.String()

	known := map[string]pollSnapshot{
		key: {Target: target, Revision: domain.Revision(3), ApplyBehavior: domain.ApplyLiveRead},
	}

	current := pollSnapshot{Target: target, Revision: domain.Revision(4), ApplyBehavior: domain.ApplyLiveRead}

	prev := known[key]

	// Revision advanced from 3 to 4 — should trigger a signal.
	assert.NotEqual(t, prev.Revision, current.Revision)

	// Sequential advance (no gap).
	assert.Equal(t, prev.Revision.Next(), current.Revision)
}

func TestPollRevisionChangeDetection_SameRevision_NoSignal(t *testing.T) {
	t.Parallel()

	target, err := domain.NewTarget(domain.KindConfig, domain.ScopeGlobal, "")
	require.NoError(t, err)

	key := target.String()

	known := map[string]pollSnapshot{
		key: {Target: target, Revision: domain.Revision(5)},
	}

	current := pollSnapshot{Target: target, Revision: domain.Revision(5)}

	// Same revision — should NOT trigger a signal.
	assert.Equal(t, known[key].Revision, current.Revision)
}

func TestPollRevisionGapDetection_Escalation(t *testing.T) {
	t.Parallel()

	target, err := domain.NewTarget(domain.KindConfig, domain.ScopeGlobal, "")
	require.NoError(t, err)

	key := target.String()

	tests := []struct {
		name             string
		prevRevision     domain.Revision
		currentRevision  domain.Revision
		originalBehavior domain.ApplyBehavior
		expectRebuild    bool
	}{
		{
			name:             "no gap (sequential)",
			prevRevision:     domain.Revision(3),
			currentRevision:  domain.Revision(4),
			originalBehavior: domain.ApplyLiveRead,
			expectRebuild:    false,
		},
		{
			name:             "gap (3 to 6)",
			prevRevision:     domain.Revision(3),
			currentRevision:  domain.Revision(6),
			originalBehavior: domain.ApplyLiveRead,
			expectRebuild:    true,
		},
		{
			name:             "gap (3 to 5, exactly 2 apart)",
			prevRevision:     domain.Revision(3),
			currentRevision:  domain.Revision(5),
			originalBehavior: domain.ApplyLiveRead,
			expectRebuild:    true,
		},
		{
			name:             "gap preserves original behavior escalation",
			prevRevision:     domain.Revision(1),
			currentRevision:  domain.Revision(10),
			originalBehavior: domain.ApplyWorkerReconcile,
			expectRebuild:    true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			prev := pollSnapshot{Target: target, Revision: tt.prevRevision, ApplyBehavior: tt.originalBehavior}
			rev := pollSnapshot{Target: target, Revision: tt.currentRevision, ApplyBehavior: tt.originalBehavior}

			known := map[string]pollSnapshot{key: prev}

			// Simulate the subscribePoll gap detection.
			if prev.Revision != rev.Revision {
				if rev.Revision > prev.Revision.Next() {
					rev.ApplyBehavior = domain.ApplyBundleRebuild
				}

				known[key] = rev
			}

			if tt.expectRebuild {
				assert.Equal(t, domain.ApplyBundleRebuild, known[key].ApplyBehavior,
					"revision gap should escalate to bundle-rebuild")
			} else {
				assert.Equal(t, tt.originalBehavior, known[key].ApplyBehavior,
					"sequential revision should keep original behavior")
			}
		})
	}
}

// ---------------------------------------------------------------------------
// Feed constructor for poll mode
// ---------------------------------------------------------------------------

func TestNew_PollMode_DefaultInterval(t *testing.T) {
	t.Parallel()

	feed := New(nil, WatchModePoll, 0)

	assert.Equal(t, WatchModePoll, feed.watchMode)
	assert.Equal(t, bootstrap.DefaultMongoPollInterval, feed.pollInterval)
}

func TestNew_PollMode_CustomInterval(t *testing.T) {
	t.Parallel()

	feed := New(nil, WatchModePoll, 3*time.Second)

	assert.Equal(t, WatchModePoll, feed.watchMode)
	assert.Equal(t, 3*time.Second, feed.pollInterval)
}

func TestNew_PollMode_NegativeInterval_UsesDefault(t *testing.T) {
	t.Parallel()

	feed := New(nil, WatchModePoll, -500*time.Millisecond)

	assert.Equal(t, bootstrap.DefaultMongoPollInterval, feed.pollInterval)
}

// ---------------------------------------------------------------------------
// Poll snapshot map operations
// ---------------------------------------------------------------------------

func TestPollSnapshotMap_MultipleTargets(t *testing.T) {
	t.Parallel()

	targetA, err := domain.NewTarget(domain.KindConfig, domain.ScopeGlobal, "")
	require.NoError(t, err)

	targetB, err := domain.NewTarget(domain.KindSetting, domain.ScopeTenant, "tenant-42")
	require.NoError(t, err)

	known := map[string]pollSnapshot{
		targetA.String(): {Target: targetA, Revision: domain.Revision(1)},
		targetB.String(): {Target: targetB, Revision: domain.Revision(5)},
	}

	assert.Len(t, known, 2)
	assert.Equal(t, domain.Revision(1), known[targetA.String()].Revision)
	assert.Equal(t, domain.Revision(5), known[targetB.String()].Revision)
}

func TestPollSnapshotMap_Update(t *testing.T) {
	t.Parallel()

	target, err := domain.NewTarget(domain.KindConfig, domain.ScopeGlobal, "")
	require.NoError(t, err)

	key := target.String()

	known := map[string]pollSnapshot{
		key: {Target: target, Revision: domain.Revision(3)},
	}

	// Update with new revision.
	known[key] = pollSnapshot{Target: target, Revision: domain.Revision(4)}

	assert.Equal(t, domain.Revision(4), known[key].Revision)
}

func TestPollSnapshotMap_Overwrite(t *testing.T) {
	t.Parallel()

	// The subscribePoll code overwrites the entire known map with current on each cycle:
	//   known = current
	// Verify this is safe.
	target, err := domain.NewTarget(domain.KindConfig, domain.ScopeGlobal, "")
	require.NoError(t, err)

	known := map[string]pollSnapshot{
		target.String(): {Target: target, Revision: domain.Revision(1)},
	}

	current := map[string]pollSnapshot{
		target.String(): {Target: target, Revision: domain.Revision(5)},
	}

	known = current

	assert.Equal(t, domain.Revision(5), known[target.String()].Revision)
}
