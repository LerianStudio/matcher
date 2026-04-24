// Copyright 2025 Lerian Studio. All rights reserved.
// Use of this source code is governed by an Elastic License 2.0
// that can be found in the LICENSE.md file.

//go:build unit

package dispute_test

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/LerianStudio/matcher/internal/exception/domain/dispute"
)

func TestAllowedDisputeTransitions(t *testing.T) {
	t.Parallel()

	transitions := dispute.AllowedDisputeTransitions()

	require.Len(t, transitions[dispute.DisputeStateDraft], 1)
	require.Len(t, transitions[dispute.DisputeStateOpen], 3)
	require.Len(t, transitions[dispute.DisputeStatePendingEvidence], 3)
	require.Empty(t, transitions[dispute.DisputeStateWon])
	require.Len(t, transitions[dispute.DisputeStateLost], 1)

	assert.Contains(t, transitions[dispute.DisputeStateDraft], dispute.DisputeStateOpen)
	assert.Contains(t, transitions[dispute.DisputeStateOpen], dispute.DisputeStatePendingEvidence)
	assert.Contains(t, transitions[dispute.DisputeStateOpen], dispute.DisputeStateWon)
	assert.Contains(t, transitions[dispute.DisputeStateOpen], dispute.DisputeStateLost)
	assert.Contains(t, transitions[dispute.DisputeStatePendingEvidence], dispute.DisputeStateOpen)
	assert.Contains(t, transitions[dispute.DisputeStatePendingEvidence], dispute.DisputeStateWon)
	assert.Contains(t, transitions[dispute.DisputeStatePendingEvidence], dispute.DisputeStateLost)
	assert.Contains(t, transitions[dispute.DisputeStateLost], dispute.DisputeStateOpen)
}

func TestValidateDisputeTransition_ValidTransitions(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name string
		from dispute.DisputeState
		to   dispute.DisputeState
	}{
		{name: "Draft to Open", from: dispute.DisputeStateDraft, to: dispute.DisputeStateOpen},
		{
			name: "Open to PendingEvidence",
			from: dispute.DisputeStateOpen,
			to:   dispute.DisputeStatePendingEvidence,
		},
		{name: "Open to Won", from: dispute.DisputeStateOpen, to: dispute.DisputeStateWon},
		{name: "Open to Lost", from: dispute.DisputeStateOpen, to: dispute.DisputeStateLost},
		{
			name: "PendingEvidence to Open",
			from: dispute.DisputeStatePendingEvidence,
			to:   dispute.DisputeStateOpen,
		},
		{
			name: "PendingEvidence to Won",
			from: dispute.DisputeStatePendingEvidence,
			to:   dispute.DisputeStateWon,
		},
		{
			name: "PendingEvidence to Lost",
			from: dispute.DisputeStatePendingEvidence,
			to:   dispute.DisputeStateLost,
		},
		{
			name: "Lost to Open (reopen)",
			from: dispute.DisputeStateLost,
			to:   dispute.DisputeStateOpen,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			require.NoError(t, dispute.ValidateDisputeTransition(tt.from, tt.to))
		})
	}
}

func TestValidateDisputeTransition_InvalidTransitions(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name string
		from dispute.DisputeState
		to   dispute.DisputeState
	}{
		{name: "Draft to Won", from: dispute.DisputeStateDraft, to: dispute.DisputeStateWon},
		{name: "Draft to Lost", from: dispute.DisputeStateDraft, to: dispute.DisputeStateLost},
		{
			name: "Draft to PendingEvidence",
			from: dispute.DisputeStateDraft,
			to:   dispute.DisputeStatePendingEvidence,
		},
		{name: "Draft to Draft", from: dispute.DisputeStateDraft, to: dispute.DisputeStateDraft},
		{name: "Open to Draft", from: dispute.DisputeStateOpen, to: dispute.DisputeStateDraft},
		{name: "Open to Open", from: dispute.DisputeStateOpen, to: dispute.DisputeStateOpen},
		{
			name: "PendingEvidence to Draft",
			from: dispute.DisputeStatePendingEvidence,
			to:   dispute.DisputeStateDraft,
		},
		{name: "Won to Open", from: dispute.DisputeStateWon, to: dispute.DisputeStateOpen},
		{name: "Won to Lost", from: dispute.DisputeStateWon, to: dispute.DisputeStateLost},
		{name: "Won to Draft", from: dispute.DisputeStateWon, to: dispute.DisputeStateDraft},
		{name: "Won to Won", from: dispute.DisputeStateWon, to: dispute.DisputeStateWon},
		{name: "Lost to Draft", from: dispute.DisputeStateLost, to: dispute.DisputeStateDraft},
		{name: "Lost to Lost", from: dispute.DisputeStateLost, to: dispute.DisputeStateLost},
		{name: "Lost to Won", from: dispute.DisputeStateLost, to: dispute.DisputeStateWon},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			err := dispute.ValidateDisputeTransition(tt.from, tt.to)
			require.ErrorIs(t, err, dispute.ErrInvalidDisputeTransition)
		})
	}
}

func TestValidateDisputeTransition_InvalidState(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name string
		from dispute.DisputeState
		to   dispute.DisputeState
	}{
		{
			name: "Invalid from state",
			from: dispute.DisputeState("INVALID"),
			to:   dispute.DisputeStateOpen,
		},
		{
			name: "Invalid to state",
			from: dispute.DisputeStateOpen,
			to:   dispute.DisputeState("INVALID"),
		},
		{
			name: "Both invalid",
			from: dispute.DisputeState("BAD"),
			to:   dispute.DisputeState("WORSE"),
		},
		{name: "Empty from", from: dispute.DisputeState(""), to: dispute.DisputeStateOpen},
		{name: "Empty to", from: dispute.DisputeStateOpen, to: dispute.DisputeState("")},
		{name: "Both empty", from: dispute.DisputeState(""), to: dispute.DisputeState("")},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			err := dispute.ValidateDisputeTransition(tt.from, tt.to)
			require.ErrorIs(t, err, dispute.ErrInvalidDisputeState)
		})
	}
}
