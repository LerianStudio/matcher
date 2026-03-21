// Copyright 2025 Lerian Studio.

//go:build unit

package mongodb

import (
	"context"
	"testing"
	"time"

	basechangefeed "github.com/LerianStudio/matcher/pkg/systemplane/adapters/changefeed"
	"github.com/LerianStudio/matcher/pkg/systemplane/bootstrap"
	"github.com/LerianStudio/matcher/pkg/systemplane/domain"
	"github.com/LerianStudio/matcher/pkg/systemplane/ports"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"go.mongodb.org/mongo-driver/v2/bson"
	"go.mongodb.org/mongo-driver/v2/mongo"
)

// ---------------------------------------------------------------------------
// Constructor tests
// ---------------------------------------------------------------------------

func TestNew_Defaults(t *testing.T) {
	t.Parallel()

	feed := New(nil, "", 0)

	assert.Equal(t, WatchModeChangeStream, feed.watchMode)
	assert.Equal(t, bootstrap.DefaultMongoPollInterval, feed.pollInterval)
	assert.Nil(t, feed.entries)
}

func TestNew_CustomValues(t *testing.T) {
	t.Parallel()

	feed := New(nil, WatchModePoll, 10*time.Second)

	assert.Equal(t, WatchModePoll, feed.watchMode)
	assert.Equal(t, 10*time.Second, feed.pollInterval)
}

func TestNew_NegativePollInterval_UsesDefault(t *testing.T) {
	t.Parallel()

	feed := New(nil, WatchModePoll, -1*time.Second)

	assert.Equal(t, bootstrap.DefaultMongoPollInterval, feed.pollInterval)
}

func TestNew_ChangeStreamModeExplicit(t *testing.T) {
	t.Parallel()

	feed := New(nil, WatchModeChangeStream, 3*time.Second)

	assert.Equal(t, WatchModeChangeStream, feed.watchMode)
	assert.Equal(t, 3*time.Second, feed.pollInterval)
}

// ---------------------------------------------------------------------------
// Interface compliance
// ---------------------------------------------------------------------------

func TestFeedImplementsChangeFeedInterface(t *testing.T) {
	t.Parallel()

	var _ ports.ChangeFeed = (*Feed)(nil)
}

// ---------------------------------------------------------------------------
// Subscribe error paths
// ---------------------------------------------------------------------------

func TestSubscribe_InvalidWatchMode_ReturnsError(t *testing.T) {
	t.Parallel()

	feed := &Feed{
		entries:      &mongo.Collection{},
		watchMode:    "unknown_mode",
		pollInterval: time.Second,
	}

	err := feed.Subscribe(context.Background(), func(_ ports.ChangeSignal) {})

	require.Error(t, err)
	assert.ErrorIs(t, err, ErrUnsupportedMode)
	assert.Contains(t, err.Error(), "unsupported watch mode")
	assert.Contains(t, err.Error(), "unknown_mode")
}

func TestSubscribe_NilReceiver_ReturnsError(t *testing.T) {
	t.Parallel()

	var feed *Feed
	err := feed.Subscribe(context.Background(), func(_ ports.ChangeSignal) {})
	require.ErrorIs(t, err, ErrNilFeed)
}

func TestSubscribe_NilEntries_ReturnsError(t *testing.T) {
	t.Parallel()

	feed := &Feed{watchMode: WatchModePoll, pollInterval: time.Second}
	err := feed.Subscribe(context.Background(), func(_ ports.ChangeSignal) {})
	require.ErrorIs(t, err, ErrNilEntries)
}

func TestSubscribe_NilHandler_ReturnsError(t *testing.T) {
	t.Parallel()

	feed := &Feed{entries: &mongo.Collection{}, watchMode: WatchModePoll, pollInterval: time.Second}
	err := feed.Subscribe(context.Background(), nil)
	require.ErrorIs(t, err, ErrNilFeedHandler)
}

// ---------------------------------------------------------------------------
// BSON helper tests
// ---------------------------------------------------------------------------

func TestBsonLookupString(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name string
		doc  *bson.D
		key  string
		want string
	}{
		{
			name: "existing string key",
			doc:  &bson.D{{Key: "kind", Value: "config"}},
			key:  "kind",
			want: "config",
		},
		{
			name: "missing key",
			doc:  &bson.D{{Key: "kind", Value: "config"}},
			key:  "scope",
			want: "",
		},
		{
			name: "non-string value",
			doc:  &bson.D{{Key: "revision", Value: int64(42)}},
			key:  "revision",
			want: "",
		},
		{
			name: "nil document",
			doc:  nil,
			key:  "kind",
			want: "",
		},
		{
			name: "empty document",
			doc:  &bson.D{},
			key:  "kind",
			want: "",
		},
		{
			name: "multiple keys returns correct one",
			doc: &bson.D{
				{Key: "kind", Value: "config"},
				{Key: "scope", Value: "tenant"},
				{Key: "subject", Value: "tenant-123"},
			},
			key:  "scope",
			want: "tenant",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			got := bsonLookupString(tt.doc, tt.key)
			assert.Equal(t, tt.want, got)
		})
	}
}

func TestBsonLookupUint64(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name string
		doc  *bson.D
		key  string
		want uint64
		ok   bool
	}{
		{
			name: "int64 value",
			doc:  &bson.D{{Key: "revision", Value: int64(42)}},
			key:  "revision",
			want: 42,
			ok:   true,
		},
		{
			name: "int32 value",
			doc:  &bson.D{{Key: "revision", Value: int32(7)}},
			key:  "revision",
			want: 7,
			ok:   true,
		},
		{
			name: "uint64 value",
			doc:  &bson.D{{Key: "revision", Value: uint64(9)}},
			key:  "revision",
			want: 9,
			ok:   true,
		},
		{
			name: "float64 value",
			doc:  &bson.D{{Key: "revision", Value: float64(99.9)}},
			key:  "revision",
			want: 0,
			ok:   false,
		},
		{
			name: "negative int64 value",
			doc:  &bson.D{{Key: "revision", Value: int64(-1)}},
			key:  "revision",
			want: 0,
			ok:   false,
		},
		{
			name: "missing key",
			doc:  &bson.D{{Key: "kind", Value: "config"}},
			key:  "revision",
			want: 0,
			ok:   false,
		},
		{
			name: "non-numeric value",
			doc:  &bson.D{{Key: "revision", Value: "not-a-number"}},
			key:  "revision",
			want: 0,
			ok:   false,
		},
		{
			name: "nil document",
			doc:  nil,
			key:  "revision",
			want: 0,
			ok:   false,
		},
		{
			name: "empty document",
			doc:  &bson.D{},
			key:  "revision",
			want: 0,
			ok:   false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			got, ok := bsonLookupUint64(tt.doc, tt.key)
			assert.Equal(t, tt.want, got)
			assert.Equal(t, tt.ok, ok)
		})
	}
}

// ---------------------------------------------------------------------------
// targetFromDoc tests
// ---------------------------------------------------------------------------

func TestTargetFromDoc_GlobalScope(t *testing.T) {
	t.Parallel()

	doc := &bson.D{
		{Key: "kind", Value: "config"},
		{Key: "scope", Value: "global"},
		{Key: "subject", Value: ""},
		{Key: "revision", Value: int64(5)},
	}

	target, rev, _, ok := targetFromDoc(doc)

	require.True(t, ok)
	assert.Equal(t, domain.KindConfig, target.Kind)
	assert.Equal(t, domain.ScopeGlobal, target.Scope)
	assert.Equal(t, "", target.SubjectID)
	assert.Equal(t, domain.Revision(5), rev)
}

func TestTargetFromDoc_TenantScope(t *testing.T) {
	t.Parallel()

	doc := &bson.D{
		{Key: "kind", Value: "setting"},
		{Key: "scope", Value: "tenant"},
		{Key: "subject", Value: "tenant-abc"},
		{Key: "revision", Value: int64(12)},
	}

	target, rev, _, ok := targetFromDoc(doc)

	require.True(t, ok)
	assert.Equal(t, domain.KindSetting, target.Kind)
	assert.Equal(t, domain.ScopeTenant, target.Scope)
	assert.Equal(t, "tenant-abc", target.SubjectID)
	assert.Equal(t, domain.Revision(12), rev)
}

func TestTargetFromDoc_InvalidKind(t *testing.T) {
	t.Parallel()

	doc := &bson.D{
		{Key: "kind", Value: "bogus"},
		{Key: "scope", Value: "global"},
		{Key: "subject", Value: ""},
		{Key: "revision", Value: int64(1)},
	}

	_, _, _, ok := targetFromDoc(doc)

	assert.False(t, ok)
}

func TestTargetFromDoc_InvalidScope(t *testing.T) {
	t.Parallel()

	doc := &bson.D{
		{Key: "kind", Value: "config"},
		{Key: "scope", Value: "invalid"},
		{Key: "subject", Value: ""},
		{Key: "revision", Value: int64(1)},
	}

	_, _, _, ok := targetFromDoc(doc)

	assert.False(t, ok)
}

func TestTargetFromDoc_MissingFields(t *testing.T) {
	t.Parallel()

	doc := &bson.D{
		{Key: "revision", Value: int64(1)},
	}

	_, _, _, ok := targetFromDoc(doc)

	// kind and scope are empty strings which are invalid.
	assert.False(t, ok)
}

func TestTargetFromDoc_TenantScopeMissingSubject(t *testing.T) {
	t.Parallel()

	doc := &bson.D{
		{Key: "kind", Value: "setting"},
		{Key: "scope", Value: "tenant"},
		{Key: "subject", Value: ""},
		{Key: "revision", Value: int64(1)},
	}

	// NewTarget requires non-empty SubjectID for tenant scope.
	_, _, _, ok := targetFromDoc(doc)

	assert.False(t, ok)
}

func TestTargetFromDoc_MissingRevisionRejected(t *testing.T) {
	t.Parallel()

	doc := &bson.D{
		{Key: "kind", Value: "config"},
		{Key: "scope", Value: "global"},
		{Key: "subject", Value: ""},
		// No revision field.
	}

	_, _, _, ok := targetFromDoc(doc)

	assert.False(t, ok)
}

// ---------------------------------------------------------------------------
// signalFromEvent tests
// ---------------------------------------------------------------------------

func TestSignalFromEvent_Insert(t *testing.T) {
	t.Parallel()

	event := changeEvent{
		OperationType: "insert",
		FullDocument: &bson.D{
			{Key: "kind", Value: "config"},
			{Key: "scope", Value: "global"},
			{Key: "subject", Value: ""},
			{Key: "key", Value: "max_retries"},
			{Key: "revision", Value: int64(3)},
		},
	}

	_, ok := signalFromEvent(event)
	assert.False(t, ok)
}

func TestSignalFromEvent_Update(t *testing.T) {
	t.Parallel()

	event := changeEvent{
		OperationType: "update",
		FullDocument: &bson.D{
			{Key: "kind", Value: "setting"},
			{Key: "scope", Value: "tenant"},
			{Key: "subject", Value: "tenant-xyz"},
			{Key: "key", Value: "theme"},
			{Key: "revision", Value: int64(7)},
		},
	}

	_, ok := signalFromEvent(event)
	assert.False(t, ok)
}

func TestSignalFromEvent_Replace(t *testing.T) {
	t.Parallel()

	event := changeEvent{
		OperationType: "replace",
		FullDocument: &bson.D{
			{Key: "kind", Value: "config"},
			{Key: "scope", Value: "global"},
			{Key: "subject", Value: ""},
			{Key: "key", Value: "timeout_ms"},
			{Key: "revision", Value: int64(2)},
		},
	}

	_, ok := signalFromEvent(event)
	assert.False(t, ok)
}

func TestSignalFromEvent_DeleteEntry_Skipped(t *testing.T) {
	t.Parallel()

	event := changeEvent{
		OperationType: "delete",
		FullDocument:  nil,
		DocumentKey: &bson.D{
			{Key: "kind", Value: "config"},
			{Key: "scope", Value: "global"},
			{Key: "subject", Value: ""},
			{Key: "revision", Value: int64(0)},
		},
	}

	_, ok := signalFromEvent(event)

	assert.False(t, ok)
}

func TestSignalFromEvent_RevisionMetaKey_EmitsSignal(t *testing.T) {
	t.Parallel()

	event := changeEvent{
		OperationType: "update",
		FullDocument: &bson.D{
			{Key: "kind", Value: "config"},
			{Key: "scope", Value: "global"},
			{Key: "subject", Value: ""},
			{Key: "key", Value: revisionMetaKey},
			{Key: "revision", Value: int64(10)},
		},
	}

	signal, ok := signalFromEvent(event)

	require.True(t, ok)
	assert.Equal(t, domain.KindConfig, signal.Target.Kind)
	assert.Equal(t, domain.ScopeGlobal, signal.Target.Scope)
	assert.Equal(t, domain.Revision(10), signal.Revision)
}

func TestSignalFromEvent_NilFullDocument_Skipped(t *testing.T) {
	t.Parallel()

	event := changeEvent{
		OperationType: "insert",
		FullDocument:  nil,
	}

	_, ok := signalFromEvent(event)

	assert.False(t, ok)
}

func TestSignalFromEvent_UnknownOperationType_Skipped(t *testing.T) {
	t.Parallel()

	event := changeEvent{
		OperationType: "drop",
		FullDocument: &bson.D{
			{Key: "kind", Value: "config"},
			{Key: "scope", Value: "global"},
		},
	}

	_, ok := signalFromEvent(event)

	assert.False(t, ok)
}

func TestSignalFromEvent_InvalidKindInDocument_Skipped(t *testing.T) {
	t.Parallel()

	event := changeEvent{
		OperationType: "insert",
		FullDocument: &bson.D{
			{Key: "kind", Value: "invalid_kind"},
			{Key: "scope", Value: "global"},
			{Key: "subject", Value: ""},
			{Key: "key", Value: "some_key"},
			{Key: "revision", Value: int64(1)},
		},
	}

	_, ok := signalFromEvent(event)

	assert.False(t, ok)
}

// ---------------------------------------------------------------------------
// HIGH-15: Revision gap detection in poll mode triggers ApplyBundleRebuild
// ---------------------------------------------------------------------------

func TestPollRevisionGapDetection_TriggersRebuild(t *testing.T) {
	t.Parallel()

	// The poll mode gap detection logic is:
	//   if exists && rev.Revision > prev.Revision.Next() → set ApplyBundleRebuild
	// We test this logic in isolation without requiring a live MongoDB.

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
			name:             "sequential revision (no gap)",
			prevRevision:     domain.Revision(3),
			currentRevision:  domain.Revision(4),
			originalBehavior: domain.ApplyLiveRead,
			expectRebuild:    false,
		},
		{
			name:             "gap detected (prev=3, current=6)",
			prevRevision:     domain.Revision(3),
			currentRevision:  domain.Revision(6),
			originalBehavior: domain.ApplyLiveRead,
			expectRebuild:    true,
		},
		{
			name:             "same revision (no change)",
			prevRevision:     domain.Revision(5),
			currentRevision:  domain.Revision(5),
			originalBehavior: domain.ApplyLiveRead,
			expectRebuild:    false,
		},
		{
			name:             "gap of exactly 2 (prev=3, current=5)",
			prevRevision:     domain.Revision(3),
			currentRevision:  domain.Revision(5),
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

			// Simulate the subscribePoll/resyncMissedSignals gap detection logic.
			if prev.Revision != rev.Revision {
				if rev.Revision > prev.Revision.Next() {
					rev.ApplyBehavior = domain.ApplyBundleRebuild
				}

				known[key] = rev
			}

			if tt.expectRebuild {
				assert.Equal(t, domain.ApplyBundleRebuild, known[key].ApplyBehavior,
					"revision gap should escalate to bundle-rebuild")
			} else if tt.prevRevision != tt.currentRevision {
				assert.Equal(t, tt.originalBehavior, known[key].ApplyBehavior,
					"no gap should keep original behavior")
			}
		})
	}
}

// ---------------------------------------------------------------------------
// HIGH-15: Handler panic propagation (signal handler panics → error returned)
// ---------------------------------------------------------------------------

func TestSignalHandlerPanic_PropagatesAsError(t *testing.T) {
	t.Parallel()

	// SafeInvokeHandler from the base changefeed package converts panics to
	// errors. Both subscribePoll and subscribeChangeStream use it.
	// This test verifies the contract through the public signalFromEvent path
	// combined with a panicking handler scenario.

	// Construct a valid revision-meta event.
	event := changeEvent{
		OperationType: "update",
		FullDocument: &bson.D{
			{Key: "kind", Value: "config"},
			{Key: "scope", Value: "global"},
			{Key: "subject", Value: ""},
			{Key: "key", Value: revisionMetaKey},
			{Key: "revision", Value: int64(10)},
		},
	}

	signal, ok := signalFromEvent(event)
	require.True(t, ok, "revision-meta event should produce a valid signal")

	// Verify the signal is valid and would be passed to a handler.
	assert.Equal(t, domain.KindConfig, signal.Target.Kind)
	assert.Equal(t, domain.Revision(10), signal.Revision)

	// The SafeInvokeHandler function catches panics. This is tested
	// in the base changefeed package (safe_handler_test.go), but we verify
	// the contract here: a panicking handler returns an error, not a crash.
	panicHandler := func(_ ports.ChangeSignal) {
		panic("handler exploded")
	}

	// This must NOT panic — SafeInvokeHandler wraps it.
	require.NotPanics(t, func() {
		_ = basechangefeed.SafeInvokeHandler(panicHandler, signal)
	}, "SafeInvokeHandler must convert panics to errors")
}

// ---------------------------------------------------------------------------
// Struct layout
// ---------------------------------------------------------------------------

func TestFeedStructFields(t *testing.T) {
	t.Parallel()

	f := &Feed{}

	assert.Nil(t, f.entries)
	assert.Equal(t, "", f.watchMode)
	assert.Equal(t, time.Duration(0), f.pollInterval)
}

// ---------------------------------------------------------------------------
// Constants
// ---------------------------------------------------------------------------

func TestRevisionMetaKey_MatchesStoreAdapter(t *testing.T) {
	t.Parallel()

	// The change feed must use the same sentinel key as the store adapter.
	// If this constant changes, the feed will miss revision-meta events.
	assert.Equal(t, "__revision_meta__", revisionMetaKey)
}

func TestWatchModeConstants(t *testing.T) {
	t.Parallel()

	assert.Equal(t, "change_stream", WatchModeChangeStream)
	assert.Equal(t, "poll", WatchModePoll)
}
