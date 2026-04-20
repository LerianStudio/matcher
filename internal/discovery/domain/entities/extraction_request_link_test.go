// Copyright 2025 Lerian Studio. All rights reserved.
// Use of this source code is governed by an Elastic License 2.0
// that can be found in the LICENSE.md file.

//go:build unit

package entities_test

import (
	"testing"

	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/LerianStudio/matcher/internal/discovery/domain/entities"
	vo "github.com/LerianStudio/matcher/internal/discovery/domain/value_objects"
	sharedPorts "github.com/LerianStudio/matcher/internal/shared/ports"
)

// TestLinkToIngestion_CompleteExtraction_AcceptsLink exercises the happy
// path: a COMPLETE + unlinked extraction can be linked to a fresh
// ingestion job id.
func TestLinkToIngestion_CompleteExtraction_AcceptsLink(t *testing.T) {
	t.Parallel()

	er := &entities.ExtractionRequest{
		ID:     uuid.New(),
		Status: vo.ExtractionStatusComplete,
	}
	jobID := uuid.New()

	err := er.LinkToIngestion(jobID)
	require.NoError(t, err)
	assert.Equal(t, jobID, er.IngestionJobID)
	assert.False(t, er.UpdatedAt.IsZero(), "UpdatedAt must bump on link")
}

// TestLinkToIngestion_NilIngestionJobID_Rejected exercises input
// validation: uuid.Nil is not a valid ingestion job id.
func TestLinkToIngestion_NilIngestionJobID_Rejected(t *testing.T) {
	t.Parallel()

	er := &entities.ExtractionRequest{Status: vo.ExtractionStatusComplete}

	err := er.LinkToIngestion(uuid.Nil)
	require.ErrorIs(t, err, entities.ErrInvalidTransition)
	assert.Equal(t, uuid.Nil, er.IngestionJobID, "no mutation on validation failure")
}

// TestLinkToIngestion_PendingExtraction_Rejected exercises the state-
// machine invariant: a PENDING extraction has no output to link to.
func TestLinkToIngestion_PendingExtraction_Rejected(t *testing.T) {
	t.Parallel()

	for _, status := range []vo.ExtractionStatus{
		vo.ExtractionStatusPending,
		vo.ExtractionStatusSubmitted,
		vo.ExtractionStatusExtracting,
		vo.ExtractionStatusFailed,
		vo.ExtractionStatusCancelled,
	} {
		t.Run(string(status), func(t *testing.T) {
			t.Parallel()

			er := &entities.ExtractionRequest{Status: status}

			err := er.LinkToIngestion(uuid.New())
			require.ErrorIs(t, err, entities.ErrInvalidTransition)
			assert.Equal(t, uuid.Nil, er.IngestionJobID, "no mutation on state-machine reject")
		})
	}
}

// TestLinkToIngestion_AlreadyLinkedToDifferentJob_Rejected asserts the
// one-extraction-to-one-ingestion invariant at the domain layer.
func TestLinkToIngestion_AlreadyLinkedToDifferentJob_Rejected(t *testing.T) {
	t.Parallel()

	existing := uuid.New()
	er := &entities.ExtractionRequest{
		Status:         vo.ExtractionStatusComplete,
		IngestionJobID: existing,
	}

	err := er.LinkToIngestion(uuid.New())
	// Cross-job collision now surfaces sharedPorts.ErrExtractionAlreadyLinked
	// (Fix 6) so domain rejection and atomic-SQL rejection share one identity.
	require.ErrorIs(t, err, sharedPorts.ErrExtractionAlreadyLinked)
	assert.Equal(t, existing, er.IngestionJobID, "pre-existing link preserved on reject")
}

// TestLinkToIngestion_AlreadyLinkedToSameJob_Idempotent asserts that
// re-linking to the SAME job id is a no-op (technically validated by the
// entity; the adapter's atomic SQL handles the actual deduplication).
func TestLinkToIngestion_AlreadyLinkedToSameJob_Idempotent(t *testing.T) {
	t.Parallel()

	jobID := uuid.New()
	er := &entities.ExtractionRequest{
		Status:         vo.ExtractionStatusComplete,
		IngestionJobID: jobID,
	}

	// Linking to the same job id is treated by the domain as a no-op
	// (no error, no mutation of IngestionJobID).
	err := er.LinkToIngestion(jobID)
	assert.NoError(t, err)
	assert.Equal(t, jobID, er.IngestionJobID)
}

// TestLinkToIngestion_NilReceiver_NoOp exercises the defensive nil guard.
func TestLinkToIngestion_NilReceiver_NoOp(t *testing.T) {
	t.Parallel()

	var er *entities.ExtractionRequest

	err := er.LinkToIngestion(uuid.New())
	require.NoError(t, err)
}
