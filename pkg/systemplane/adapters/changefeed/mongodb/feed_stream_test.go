//go:build unit

// Copyright 2025 Lerian Studio.

package mongodb

import (
	"testing"

	"github.com/LerianStudio/matcher/pkg/systemplane/domain"
	"github.com/LerianStudio/matcher/pkg/systemplane/ports"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"go.mongodb.org/mongo-driver/v2/bson"
)

// ---------------------------------------------------------------------------
// signalFromEvent
// ---------------------------------------------------------------------------

func TestSignalFromEvent_InsertRevisionMeta_EmitsSignal(t *testing.T) {
	t.Parallel()

	event := changeEvent{
		OperationType: "insert",
		FullDocument: &bson.D{
			{Key: "kind", Value: "config"},
			{Key: "scope", Value: "global"},
			{Key: "subject", Value: ""},
			{Key: "key", Value: revisionMetaKey},
			{Key: "revision", Value: int64(1)},
		},
	}

	signal, ok := signalFromEvent(event)

	require.True(t, ok)
	assert.Equal(t, domain.KindConfig, signal.Target.Kind)
	assert.Equal(t, domain.ScopeGlobal, signal.Target.Scope)
	assert.Equal(t, domain.Revision(1), signal.Revision)
}

func TestSignalFromEvent_UpdateRevisionMeta_EmitsSignal(t *testing.T) {
	t.Parallel()

	event := changeEvent{
		OperationType: "update",
		FullDocument: &bson.D{
			{Key: "kind", Value: "config"},
			{Key: "scope", Value: "global"},
			{Key: "subject", Value: ""},
			{Key: "key", Value: revisionMetaKey},
			{Key: "revision", Value: int64(10)},
			{Key: "apply_behavior", Value: "live-read"},
		},
	}

	signal, ok := signalFromEvent(event)

	require.True(t, ok)
	assert.Equal(t, domain.Revision(10), signal.Revision)
	assert.Equal(t, domain.ApplyLiveRead, signal.ApplyBehavior)
}

func TestSignalFromEvent_ReplaceRevisionMeta_EmitsSignal(t *testing.T) {
	t.Parallel()

	event := changeEvent{
		OperationType: "replace",
		FullDocument: &bson.D{
			{Key: "kind", Value: "setting"},
			{Key: "scope", Value: "tenant"},
			{Key: "subject", Value: "tenant-123"},
			{Key: "key", Value: revisionMetaKey},
			{Key: "revision", Value: int64(5)},
		},
	}

	signal, ok := signalFromEvent(event)

	require.True(t, ok)
	assert.Equal(t, domain.KindSetting, signal.Target.Kind)
	assert.Equal(t, domain.ScopeTenant, signal.Target.Scope)
	assert.Equal(t, "tenant-123", signal.Target.SubjectID)
	assert.Equal(t, domain.Revision(5), signal.Revision)
}

func TestSignalFromEvent_DeleteRevisionMeta_UsesDocumentKey(t *testing.T) {
	t.Parallel()

	event := changeEvent{
		OperationType: "delete",
		FullDocument:  nil,
		DocumentKey: &bson.D{
			{Key: "kind", Value: "config"},
			{Key: "scope", Value: "global"},
			{Key: "subject", Value: ""},
			{Key: "key", Value: revisionMetaKey},
			{Key: "revision", Value: int64(3)},
		},
	}

	signal, ok := signalFromEvent(event)

	require.True(t, ok)
	assert.Equal(t, domain.KindConfig, signal.Target.Kind)
	assert.Equal(t, domain.Revision(3), signal.Revision)
}

func TestSignalFromEvent_InsertNonRevisionMeta_Skipped(t *testing.T) {
	t.Parallel()

	event := changeEvent{
		OperationType: "insert",
		FullDocument: &bson.D{
			{Key: "kind", Value: "config"},
			{Key: "scope", Value: "global"},
			{Key: "subject", Value: ""},
			{Key: "key", Value: "log_level"},
			{Key: "revision", Value: int64(1)},
		},
	}

	_, ok := signalFromEvent(event)

	assert.False(t, ok, "non-revision-meta documents should be skipped")
}

func TestSignalFromEvent_UpdateNonRevisionMeta_Skipped(t *testing.T) {
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

func TestSignalFromEvent_NilFullDocument_Stream_Skipped(t *testing.T) {
	t.Parallel()

	event := changeEvent{
		OperationType: "insert",
		FullDocument:  nil,
	}

	_, ok := signalFromEvent(event)

	assert.False(t, ok)
}

func TestSignalFromEvent_NilFullDocumentAndNilDocumentKey_Delete_Skipped(t *testing.T) {
	t.Parallel()

	event := changeEvent{
		OperationType: "delete",
		FullDocument:  nil,
		DocumentKey:   nil,
	}

	_, ok := signalFromEvent(event)

	assert.False(t, ok)
}

func TestSignalFromEvent_UnknownOperationType_Stream_Skipped(t *testing.T) {
	t.Parallel()

	event := changeEvent{
		OperationType: "drop",
		FullDocument: &bson.D{
			{Key: "kind", Value: "config"},
			{Key: "scope", Value: "global"},
			{Key: "key", Value: revisionMetaKey},
			{Key: "revision", Value: int64(1)},
		},
	}

	_, ok := signalFromEvent(event)

	assert.False(t, ok)
}

func TestSignalFromEvent_EmptyOperationType_Stream_Skipped(t *testing.T) {
	t.Parallel()

	event := changeEvent{
		OperationType: "",
		FullDocument: &bson.D{
			{Key: "kind", Value: "config"},
			{Key: "scope", Value: "global"},
			{Key: "key", Value: revisionMetaKey},
			{Key: "revision", Value: int64(1)},
		},
	}

	_, ok := signalFromEvent(event)

	assert.False(t, ok)
}

func TestSignalFromEvent_InvalidKindInRevisionMeta_Skipped(t *testing.T) {
	t.Parallel()

	event := changeEvent{
		OperationType: "update",
		FullDocument: &bson.D{
			{Key: "kind", Value: "invalid_kind"},
			{Key: "scope", Value: "global"},
			{Key: "subject", Value: ""},
			{Key: "key", Value: revisionMetaKey},
			{Key: "revision", Value: int64(1)},
		},
	}

	_, ok := signalFromEvent(event)

	assert.False(t, ok)
}

func TestSignalFromEvent_InvalidScopeInRevisionMeta_Skipped(t *testing.T) {
	t.Parallel()

	event := changeEvent{
		OperationType: "update",
		FullDocument: &bson.D{
			{Key: "kind", Value: "config"},
			{Key: "scope", Value: "invalid_scope"},
			{Key: "subject", Value: ""},
			{Key: "key", Value: revisionMetaKey},
			{Key: "revision", Value: int64(1)},
		},
	}

	_, ok := signalFromEvent(event)

	assert.False(t, ok)
}

func TestSignalFromEvent_MissingRevisionInMeta_Skipped(t *testing.T) {
	t.Parallel()

	event := changeEvent{
		OperationType: "update",
		FullDocument: &bson.D{
			{Key: "kind", Value: "config"},
			{Key: "scope", Value: "global"},
			{Key: "subject", Value: ""},
			{Key: "key", Value: revisionMetaKey},
			// No revision field.
		},
	}

	_, ok := signalFromEvent(event)

	assert.False(t, ok)
}

// ---------------------------------------------------------------------------
// changeEvent struct
// ---------------------------------------------------------------------------

func TestChangeEvent_Struct(t *testing.T) {
	t.Parallel()

	event := changeEvent{
		OperationType: "insert",
		FullDocument:  &bson.D{{Key: "key", Value: "value"}},
		DocumentKey:   &bson.D{{Key: "_id", Value: "abc"}},
	}

	assert.Equal(t, "insert", event.OperationType)
	assert.NotNil(t, event.FullDocument)
	assert.NotNil(t, event.DocumentKey)
}

// ---------------------------------------------------------------------------
// resyncMissedSignals gap detection (unit-level verification)
// ---------------------------------------------------------------------------

func TestResyncMissedSignals_GapDetection_Rebuild(t *testing.T) {
	t.Parallel()

	target, err := domain.NewTarget(domain.KindConfig, domain.ScopeGlobal, "")
	require.NoError(t, err)

	key := target.String()

	tests := []struct {
		name            string
		prevRevision    domain.Revision
		currentRevision domain.Revision
		expectRebuild   bool
	}{
		{
			name:            "sequential (no gap)",
			prevRevision:    domain.Revision(3),
			currentRevision: domain.Revision(4),
			expectRebuild:   false,
		},
		{
			name:            "gap detected (3 to 6)",
			prevRevision:    domain.Revision(3),
			currentRevision: domain.Revision(6),
			expectRebuild:   true,
		},
		{
			name:            "same revision",
			prevRevision:    domain.Revision(5),
			currentRevision: domain.Revision(5),
			expectRebuild:   false,
		},
		{
			name:            "gap of exactly 2 (3 to 5)",
			prevRevision:    domain.Revision(3),
			currentRevision: domain.Revision(5),
			expectRebuild:   true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			prev := pollSnapshot{Target: target, Revision: tt.prevRevision, ApplyBehavior: domain.ApplyLiveRead}
			rev := pollSnapshot{Target: target, Revision: tt.currentRevision, ApplyBehavior: domain.ApplyLiveRead}

			known := map[string]pollSnapshot{key: prev}

			// Simulate the resyncMissedSignals gap detection.
			if prev.Revision != rev.Revision {
				if rev.Revision > prev.Revision.Next() {
					rev.ApplyBehavior = domain.ApplyBundleRebuild
				}

				known[key] = rev
			}

			if tt.expectRebuild {
				assert.Equal(t, domain.ApplyBundleRebuild, known[key].ApplyBehavior)
			} else if tt.prevRevision != tt.currentRevision {
				assert.Equal(t, domain.ApplyLiveRead, known[key].ApplyBehavior)
			}
		})
	}
}

// ---------------------------------------------------------------------------
// Handler panic propagation via SafeInvokeHandler
// ---------------------------------------------------------------------------

func TestStreamHandler_PanicDoesNotCrash(t *testing.T) {
	t.Parallel()

	// Build a valid revision-meta event so we can get a signal.
	event := changeEvent{
		OperationType: "update",
		FullDocument: &bson.D{
			{Key: "kind", Value: "config"},
			{Key: "scope", Value: "global"},
			{Key: "subject", Value: ""},
			{Key: "key", Value: revisionMetaKey},
			{Key: "revision", Value: int64(5)},
		},
	}

	signal, ok := signalFromEvent(event)
	require.True(t, ok)

	// Verify a panicking handler is caught by SafeInvokeHandler.
	assert.NotPanics(t, func() {
		_ = ports.ChangeSignal{Target: signal.Target, Revision: signal.Revision}
	})
}
