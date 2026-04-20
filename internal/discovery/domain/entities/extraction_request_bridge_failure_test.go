// Copyright 2025 Lerian Studio. All rights reserved.
// Use of this source code is governed by an Elastic License 2.0
// that can be found in the LICENSE.md file.

//go:build unit

package entities_test

import (
	"errors"
	"strings"
	"testing"

	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/LerianStudio/matcher/internal/discovery/domain/entities"
	vo "github.com/LerianStudio/matcher/internal/discovery/domain/value_objects"
)

func newCompleteExtraction() *entities.ExtractionRequest {
	return &entities.ExtractionRequest{
		ID:           uuid.New(),
		ConnectionID: uuid.New(),
		Status:       vo.ExtractionStatusComplete,
		FetcherJobID: "fetcher-job-1",
	}
}

func TestMarkBridgeFailed_NilReceiver_ReturnsNil(t *testing.T) {
	t.Parallel()

	var er *entities.ExtractionRequest
	assert.NoError(t, er.MarkBridgeFailed(vo.BridgeErrorClassIntegrityFailed, "msg"))
}

func TestMarkBridgeFailed_InvalidClass_Rejected(t *testing.T) {
	t.Parallel()

	er := newCompleteExtraction()
	err := er.MarkBridgeFailed(vo.BridgeErrorClass(""), "msg")
	require.Error(t, err)
	assert.True(t, errors.Is(err, entities.ErrBridgeFailureClassRequired))
	assert.Empty(t, er.BridgeLastError)
}

func TestMarkBridgeFailed_EmptyMessage_Rejected(t *testing.T) {
	t.Parallel()

	er := newCompleteExtraction()
	err := er.MarkBridgeFailed(vo.BridgeErrorClassIntegrityFailed, "   ")
	require.Error(t, err)
	assert.True(t, errors.Is(err, entities.ErrBridgeFailureMessageRequired))
}

func TestMarkBridgeFailed_HappyPath_PersistsClassAndTimestamp(t *testing.T) {
	t.Parallel()

	er := newCompleteExtraction()
	require.NoError(t, er.MarkBridgeFailed(vo.BridgeErrorClassArtifactNotFound, "404 from fetcher"))

	assert.Equal(t, vo.BridgeErrorClassArtifactNotFound, er.BridgeLastError)
	assert.Equal(t, "404 from fetcher", er.BridgeLastErrorMessage)
	assert.False(t, er.BridgeFailedAt.IsZero())
	assert.True(t, er.HasTerminalBridgeFailure())
	// Status is left alone because bridge failure is independent.
	assert.Equal(t, vo.ExtractionStatusComplete, er.Status)
}

func TestMarkBridgeFailed_LongMessage_Truncated(t *testing.T) {
	t.Parallel()

	er := newCompleteExtraction()
	long := strings.Repeat("x", entities.MaxBridgeFailureMessageLength+500)
	require.NoError(t, er.MarkBridgeFailed(vo.BridgeErrorClassIntegrityFailed, long))

	assert.Len(t, er.BridgeLastErrorMessage, entities.MaxBridgeFailureMessageLength)
}

func TestMarkBridgeFailed_SameClass_AppendsMessageAndFreezesTimestamp(t *testing.T) {
	t.Parallel()

	er := newCompleteExtraction()
	require.NoError(t, er.MarkBridgeFailed(vo.BridgeErrorClassArtifactNotFound, "HMAC mismatch"))
	firstFailedAt := er.BridgeFailedAt
	firstUpdatedAt := er.UpdatedAt

	require.NoError(t, er.MarkBridgeFailed(
		vo.BridgeErrorClassArtifactNotFound,
		"escalated after 5 attempts",
	))

	assert.Equal(t, vo.BridgeErrorClassArtifactNotFound, er.BridgeLastError)
	// Primary reason is preserved at the head, newer entry appended with
	// the separator marker. This is the append-history contract: audit
	// forensics retain the original terminal reason even as worker retries
	// escalate it.
	assert.Equal(
		t,
		"HMAC mismatch\n→ escalated after 5 attempts",
		er.BridgeLastErrorMessage,
	)
	// BridgeFailedAt is frozen at the first failure — it's the primary
	// terminal timestamp. Only UpdatedAt reflects the row change.
	assert.Equal(t, firstFailedAt, er.BridgeFailedAt)
	assert.True(t, !er.UpdatedAt.Before(firstUpdatedAt))
}

func TestMarkBridgeFailed_SameClass_DeduplicatesIdenticalMessage(t *testing.T) {
	t.Parallel()

	er := newCompleteExtraction()
	require.NoError(t, er.MarkBridgeFailed(vo.BridgeErrorClassArtifactNotFound, "HMAC mismatch"))
	// Re-call with the exact same message (after trimming) should NOT
	// bloat the column — a worker retrying with the same error is a common
	// pattern and we don't want the history to fill with duplicates.
	require.NoError(t, er.MarkBridgeFailed(vo.BridgeErrorClassArtifactNotFound, "  HMAC mismatch  "))

	assert.Equal(t, "HMAC mismatch", er.BridgeLastErrorMessage)
}

func TestMarkBridgeFailed_SameClass_MessageLengthBounded(t *testing.T) {
	t.Parallel()

	er := newCompleteExtraction()
	// First entry: half the budget so appending a second entry of equal
	// length would exceed the cap.
	firstEntry := strings.Repeat("a", entities.MaxBridgeFailureMessageLength/2)
	require.NoError(t, er.MarkBridgeFailed(vo.BridgeErrorClassIntegrityFailed, firstEntry))

	secondEntry := strings.Repeat("b", entities.MaxBridgeFailureMessageLength)
	require.NoError(t, er.MarkBridgeFailed(vo.BridgeErrorClassIntegrityFailed, secondEntry))

	// Total bounded by the cap; primary (first) entry intact at the head;
	// tail truncated with '...' suffix so the cut point is self-evident.
	assert.LessOrEqual(t, len(er.BridgeLastErrorMessage), entities.MaxBridgeFailureMessageLength)
	assert.True(
		t,
		strings.HasPrefix(er.BridgeLastErrorMessage, firstEntry),
		"primary reason must survive at the head of the history",
	)
	assert.True(
		t,
		strings.HasSuffix(er.BridgeLastErrorMessage, "..."),
		"truncation suffix must mark the clipped tail",
	)
}

func TestMarkBridgeFailed_DifferentClass_Rejected(t *testing.T) {
	t.Parallel()

	er := newCompleteExtraction()
	require.NoError(t, er.MarkBridgeFailed(vo.BridgeErrorClassArtifactNotFound, "first"))

	err := er.MarkBridgeFailed(vo.BridgeErrorClassIntegrityFailed, "second")
	require.Error(t, err)
	assert.True(t, errors.Is(err, entities.ErrBridgeFailureClassRequired))
	// Original class persists; second class is rejected.
	assert.Equal(t, vo.BridgeErrorClassArtifactNotFound, er.BridgeLastError)
}

func TestRecordBridgeAttempt_IncrementsCounter(t *testing.T) {
	t.Parallel()

	er := newCompleteExtraction()
	assert.Equal(t, 1, er.RecordBridgeAttempt())
	assert.Equal(t, 2, er.RecordBridgeAttempt())
	assert.Equal(t, 2, er.BridgeAttempts)
}

func TestRecordBridgeAttempt_NilReceiver_ReturnsZero(t *testing.T) {
	t.Parallel()

	var er *entities.ExtractionRequest
	assert.Equal(t, 0, er.RecordBridgeAttempt())
}

func TestHasTerminalBridgeFailure_NilReceiver_ReturnsFalse(t *testing.T) {
	t.Parallel()

	var er *entities.ExtractionRequest
	assert.False(t, er.HasTerminalBridgeFailure())
}

func TestHasTerminalBridgeFailure_FreshExtraction_False(t *testing.T) {
	t.Parallel()
	er := newCompleteExtraction()
	assert.False(t, er.HasTerminalBridgeFailure())
}

// TestMarkBridgeFailed_ControlBytesStripped asserts S6-5: incoming messages
// containing NUL / CR / LF / other C0-range control bytes are sanitized
// before the entry is appended or persisted, so audit tooling and log viewers
// do not see corrupted rows. Tab is intentionally exempt because it is legible
// in most log viewers and appears in formatted upstream errors.
func TestMarkBridgeFailed_ControlBytesStripped(t *testing.T) {
	t.Parallel()

	cases := []struct {
		name string
		in   string
		want string
	}{
		{
			name: "NUL byte stripped",
			in:   "custody\x00failed",
			want: "custodyfailed",
		},
		{
			name: "CR stripped",
			in:   "line1\rline2",
			want: "line1line2",
		},
		{
			name: "embedded LF stripped (separator LF is added by the appender)",
			in:   "first\nsecond",
			want: "firstsecond",
		},
		{
			name: "DEL (0x7F) stripped",
			in:   "tenant\x7fID",
			want: "tenantID",
		},
		{
			name: "tab preserved",
			in:   "field\tvalue",
			want: "field\tvalue",
		},
		{
			name: "non-ASCII UTF-8 preserved",
			in:   "custódia falhou",
			want: "custódia falhou",
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			er := newCompleteExtraction()
			require.NoError(t, er.MarkBridgeFailed(vo.BridgeErrorClassIntegrityFailed, tc.in))
			assert.Equal(t, tc.want, er.BridgeLastErrorMessage)
		})
	}
}

// TestMarkBridgeFailed_OnlyControlBytes_RejectedAsEmpty asserts that a message
// containing ONLY control bytes is treated as empty after sanitization, so the
// "message required" invariant still holds — attackers or wiring bugs cannot
// bypass the empty-check by padding with NUL bytes.
func TestMarkBridgeFailed_OnlyControlBytes_RejectedAsEmpty(t *testing.T) {
	t.Parallel()

	er := newCompleteExtraction()
	err := er.MarkBridgeFailed(vo.BridgeErrorClassIntegrityFailed, "\x00\x01\x02\x03")
	require.Error(t, err)
	assert.True(t, errors.Is(err, entities.ErrBridgeFailureMessageRequired))
}

// TestMarkBridgeFailed_AppendHistory_SanitizesBeforeSeparator asserts the
// append-history path preserves the intentional separator LF while stripping
// control bytes inside each entry. Regression guard: a worker retrying with
// a message that has an embedded CR must NOT corrupt the accumulated history.
func TestMarkBridgeFailed_AppendHistory_SanitizesBeforeSeparator(t *testing.T) {
	t.Parallel()

	er := newCompleteExtraction()
	require.NoError(t, er.MarkBridgeFailed(vo.BridgeErrorClassArtifactNotFound, "first\rreason"))
	require.NoError(t, er.MarkBridgeFailed(vo.BridgeErrorClassArtifactNotFound, "second\x00reason"))

	// Only the separator LF remains; embedded CR and NUL are stripped from
	// each entry before concatenation.
	assert.Equal(t, "firstreason\n→ secondreason", er.BridgeLastErrorMessage)
}

// TestMarkBridgeFailed_NonCompleteStatus_Rejected is the Polish Fix 5
// regression: the bridge state machine only operates on extractions whose
// upstream pipeline finished successfully (Status=COMPLETE). Calling
// MarkBridgeFailed on any other status corrupts the two-state-machine
// invariant — even though the worker pre-filters today, domain invariants
// must not depend on adapter-layer filters.
func TestMarkBridgeFailed_NonCompleteStatus_Rejected(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name   string
		status vo.ExtractionStatus
	}{
		{"pending", vo.ExtractionStatusPending},
		{"submitted", vo.ExtractionStatusSubmitted},
		{"extracting", vo.ExtractionStatusExtracting},
		{"failed", vo.ExtractionStatusFailed},
		{"cancelled", vo.ExtractionStatusCancelled},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			er := &entities.ExtractionRequest{
				ID:           uuid.New(),
				ConnectionID: uuid.New(),
				Status:       tt.status,
				FetcherJobID: "job",
			}

			err := er.MarkBridgeFailed(vo.BridgeErrorClassIntegrityFailed, "msg")
			require.Error(t, err)
			assert.True(t, errors.Is(err, entities.ErrInvalidTransition),
				"non-complete status must reject MarkBridgeFailed via ErrInvalidTransition")
			assert.Empty(t, er.BridgeLastError, "no mutation when guard rejects")
			assert.Empty(t, er.BridgeLastErrorMessage)
			assert.True(t, er.BridgeFailedAt.IsZero())
		})
	}
}
