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

func TestMarkBridgeFailed_SameClassReinvocation_Idempotent(t *testing.T) {
	t.Parallel()

	er := newCompleteExtraction()
	require.NoError(t, er.MarkBridgeFailed(vo.BridgeErrorClassArtifactNotFound, "first"))
	first := er.BridgeFailedAt

	require.NoError(t, er.MarkBridgeFailed(vo.BridgeErrorClassArtifactNotFound, "second"))
	assert.Equal(t, vo.BridgeErrorClassArtifactNotFound, er.BridgeLastError)
	assert.Equal(t, "second", er.BridgeLastErrorMessage)
	// Timestamp refreshes on idempotent re-call so callers can see the row
	// is still being touched (matters for the readiness drilldown).
	assert.True(t, !er.BridgeFailedAt.Before(first))
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
