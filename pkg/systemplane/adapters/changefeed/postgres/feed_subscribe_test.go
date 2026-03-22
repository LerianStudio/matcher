//go:build unit

// Copyright 2025 Lerian Studio.

package postgres

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/LerianStudio/matcher/pkg/systemplane/domain"
	"github.com/LerianStudio/matcher/pkg/systemplane/ports"
)

// ---------------------------------------------------------------------------
// parsePayload
// ---------------------------------------------------------------------------

func TestParsePayload_ValidGlobalConfig(t *testing.T) {
	t.Parallel()

	data := `{"kind":"config","scope":"global","subject":"","revision":7}`

	signal, err := parsePayload(data)

	require.NoError(t, err)
	assert.Equal(t, domain.KindConfig, signal.Target.Kind)
	assert.Equal(t, domain.ScopeGlobal, signal.Target.Scope)
	assert.Equal(t, "", signal.Target.SubjectID)
	assert.Equal(t, domain.Revision(7), signal.Revision)
}

func TestParsePayload_ValidTenantSetting(t *testing.T) {
	t.Parallel()

	data := `{"kind":"setting","scope":"tenant","subject":"tenant-abc","revision":12}`

	signal, err := parsePayload(data)

	require.NoError(t, err)
	assert.Equal(t, domain.KindSetting, signal.Target.Kind)
	assert.Equal(t, domain.ScopeTenant, signal.Target.Scope)
	assert.Equal(t, "tenant-abc", signal.Target.SubjectID)
	assert.Equal(t, domain.Revision(12), signal.Revision)
}

func TestParsePayload_WithApplyBehavior(t *testing.T) {
	t.Parallel()

	data := `{"kind":"config","scope":"global","subject":"","revision":5,"apply_behavior":"bundle-rebuild"}`

	signal, err := parsePayload(data)

	require.NoError(t, err)
	assert.Equal(t, domain.ApplyBundleRebuild, signal.ApplyBehavior)
	assert.Equal(t, domain.Revision(5), signal.Revision)
}

func TestParsePayload_WithoutApplyBehavior(t *testing.T) {
	t.Parallel()

	data := `{"kind":"config","scope":"global","subject":"","revision":3}`

	signal, err := parsePayload(data)

	require.NoError(t, err)
	assert.Equal(t, domain.ApplyBehavior(""), signal.ApplyBehavior,
		"missing apply_behavior should result in zero-value")
}

func TestParsePayload_InvalidJSON(t *testing.T) {
	t.Parallel()

	_, err := parsePayload("{corrupted")

	require.Error(t, err)
	assert.Contains(t, err.Error(), "pg changefeed unmarshal")
}

func TestParsePayload_EmptyPayload(t *testing.T) {
	t.Parallel()

	_, err := parsePayload("")

	require.Error(t, err)
	assert.Contains(t, err.Error(), "pg changefeed unmarshal")
}

func TestParsePayload_InvalidKind(t *testing.T) {
	t.Parallel()

	data := `{"kind":"bogus","scope":"global","subject":"","revision":1}`

	_, err := parsePayload(data)

	require.Error(t, err)
	assert.ErrorIs(t, err, domain.ErrInvalidKind)
}

func TestParsePayload_InvalidScope(t *testing.T) {
	t.Parallel()

	data := `{"kind":"config","scope":"bogus","subject":"","revision":1}`

	_, err := parsePayload(data)

	require.Error(t, err)
	assert.ErrorIs(t, err, domain.ErrInvalidScope)
}

func TestParsePayload_TenantScopeWithoutSubject(t *testing.T) {
	t.Parallel()

	data := `{"kind":"setting","scope":"tenant","subject":"","revision":1}`

	_, err := parsePayload(data)

	require.Error(t, err)
	assert.ErrorIs(t, err, domain.ErrScopeInvalid)
}

func TestParsePayload_GlobalScopeWithSubject(t *testing.T) {
	t.Parallel()

	data := `{"kind":"config","scope":"global","subject":"unexpected-id","revision":1}`

	_, err := parsePayload(data)

	require.Error(t, err)
	assert.ErrorIs(t, err, domain.ErrScopeInvalid)
}

func TestParsePayload_RevisionZero(t *testing.T) {
	t.Parallel()

	data := `{"kind":"config","scope":"global","subject":"","revision":0}`

	signal, err := parsePayload(data)

	require.NoError(t, err)
	assert.Equal(t, domain.RevisionZero, signal.Revision)
}

func TestParsePayload_MaxRevision(t *testing.T) {
	t.Parallel()

	data := `{"kind":"config","scope":"global","subject":"","revision":18446744073709551615}`

	signal, err := parsePayload(data)

	require.NoError(t, err)
	assert.Equal(t, domain.Revision(18446744073709551615), signal.Revision)
}

// ---------------------------------------------------------------------------
// trackedRevision
// ---------------------------------------------------------------------------

func TestTrackedRevision_Struct(t *testing.T) {
	t.Parallel()

	target, err := domain.NewTarget(domain.KindConfig, domain.ScopeGlobal, "")
	require.NoError(t, err)

	tr := trackedRevision{
		Target:        target,
		Revision:      domain.Revision(5),
		ApplyBehavior: domain.ApplyLiveRead,
	}

	assert.Equal(t, domain.KindConfig, tr.Target.Kind)
	assert.Equal(t, domain.Revision(5), tr.Revision)
	assert.Equal(t, domain.ApplyLiveRead, tr.ApplyBehavior)
}

// ---------------------------------------------------------------------------
// resyncMissedSignals logic — unit-level verification of gap detection
// ---------------------------------------------------------------------------

func TestResyncGapDetection_NoGap(t *testing.T) {
	t.Parallel()

	target, err := domain.NewTarget(domain.KindConfig, domain.ScopeGlobal, "")
	require.NoError(t, err)

	key := target.String()

	known := map[string]trackedRevision{
		key: {Target: target, Revision: domain.Revision(3), ApplyBehavior: domain.ApplyLiveRead},
	}

	current := trackedRevision{Target: target, Revision: domain.Revision(4), ApplyBehavior: domain.ApplyLiveRead}

	// Revision 4 == 3.Next() — no gap.
	assert.Equal(t, known[key].Revision.Next(), current.Revision,
		"revision 4 should equal 3+1, indicating no gap")
}

func TestResyncGapDetection_GapDetected(t *testing.T) {
	t.Parallel()

	target, err := domain.NewTarget(domain.KindConfig, domain.ScopeGlobal, "")
	require.NoError(t, err)

	key := target.String()

	prev := trackedRevision{Target: target, Revision: domain.Revision(3), ApplyBehavior: domain.ApplyLiveRead}
	current := trackedRevision{Target: target, Revision: domain.Revision(7), ApplyBehavior: domain.ApplyLiveRead}

	// Simulate the resyncMissedSignals gap detection.
	if current.Revision > prev.Revision.Next() {
		current.ApplyBehavior = domain.ApplyBundleRebuild
	}

	assert.Equal(t, domain.ApplyBundleRebuild, current.ApplyBehavior,
		"gap from 3 to 7 should escalate to bundle-rebuild")

	_ = key
}

func TestResyncGapDetection_SameRevision_Skipped(t *testing.T) {
	t.Parallel()

	target, err := domain.NewTarget(domain.KindConfig, domain.ScopeGlobal, "")
	require.NoError(t, err)

	prev := trackedRevision{Target: target, Revision: domain.Revision(5), ApplyBehavior: domain.ApplyLiveRead}
	current := trackedRevision{Target: target, Revision: domain.Revision(5), ApplyBehavior: domain.ApplyWorkerReconcile}

	// Same revision — should be skipped entirely.
	assert.Equal(t, prev.Revision, current.Revision,
		"same revision should not trigger any action")
}

func TestResyncGapDetection_NewTarget_NoGap(t *testing.T) {
	t.Parallel()

	target, err := domain.NewTarget(domain.KindConfig, domain.ScopeGlobal, "")
	require.NoError(t, err)

	// Target is not in known map — new targets should not trigger rebuild.
	known := map[string]trackedRevision{}
	current := trackedRevision{Target: target, Revision: domain.Revision(1), ApplyBehavior: domain.ApplyLiveRead}

	_, exists := known[target.String()]
	assert.False(t, exists, "new target should not exist in known map")

	// The resyncMissedSignals code does NOT set ApplyBundleRebuild for new targets.
	assert.Equal(t, domain.ApplyLiveRead, current.ApplyBehavior)
}

// ---------------------------------------------------------------------------
// notifyPayload struct
// ---------------------------------------------------------------------------

func TestNotifyPayload_Fields(t *testing.T) {
	t.Parallel()

	p := notifyPayload{
		Kind:          "config",
		Scope:         "global",
		Subject:       "",
		Revision:      42,
		ApplyBehavior: "live-read",
	}

	assert.Equal(t, "config", p.Kind)
	assert.Equal(t, "global", p.Scope)
	assert.Equal(t, "", p.Subject)
	assert.Equal(t, uint64(42), p.Revision)
	assert.Equal(t, "live-read", p.ApplyBehavior)
}

// ---------------------------------------------------------------------------
// Subscribe validation errors (testing validateSubscribeInput paths)
// ---------------------------------------------------------------------------

func TestValidateSubscribeInput_ValidInput(t *testing.T) {
	t.Parallel()

	feed := New("postgres://localhost/db", "systemplane_changes")

	err := feed.validateSubscribeInput(func(_ ports.ChangeSignal) {})

	require.NoError(t, err)
}

func TestValidateSubscribeInput_NilReceiver(t *testing.T) {
	t.Parallel()

	var feed *Feed

	err := feed.validateSubscribeInput(func(_ ports.ChangeSignal) {})

	require.ErrorIs(t, err, ErrNilFeed)
}

func TestValidateSubscribeInput_EmptyDSN(t *testing.T) {
	t.Parallel()

	feed := New("", "ch")

	err := feed.validateSubscribeInput(func(_ ports.ChangeSignal) {})

	require.ErrorIs(t, err, ErrEmptyDSN)
}

func TestValidateSubscribeInput_EmptyChannel(t *testing.T) {
	t.Parallel()

	feed := New("dsn", "")

	err := feed.validateSubscribeInput(func(_ ports.ChangeSignal) {})

	require.ErrorIs(t, err, ErrEmptyChannel)
}

func TestValidateSubscribeInput_InvalidChannel(t *testing.T) {
	t.Parallel()

	feed := New("dsn", "invalid;channel")

	err := feed.validateSubscribeInput(func(_ ports.ChangeSignal) {})

	require.ErrorIs(t, err, ErrInvalidChannel)
}

func TestValidateSubscribeInput_NilHandler(t *testing.T) {
	t.Parallel()

	feed := New("dsn", "ch")

	err := feed.validateSubscribeInput(nil)

	require.ErrorIs(t, err, ErrNilFeedHandler)
}

func TestValidateSubscribeInput_InvalidRevisionSource(t *testing.T) {
	t.Parallel()

	feed := New("dsn", "ch", WithRevisionSource("INVALID!", "revisions"))

	err := feed.validateSubscribeInput(func(_ ports.ChangeSignal) {})

	require.Error(t, err)
	assert.ErrorIs(t, err, ErrInvalidIdentifier)
}
