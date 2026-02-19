//go:build unit

package dispute_test

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/LerianStudio/matcher/internal/exception/domain/dispute"
)

func TestDisputeState_Constants(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name     string
		state    dispute.DisputeState
		expected string
	}{
		{name: "Draft", state: dispute.DisputeStateDraft, expected: "DRAFT"},
		{name: "Open", state: dispute.DisputeStateOpen, expected: "OPEN"},
		{
			name:     "PendingEvidence",
			state:    dispute.DisputeStatePendingEvidence,
			expected: "PENDING_EVIDENCE",
		},
		{name: "Won", state: dispute.DisputeStateWon, expected: "WON"},
		{name: "Lost", state: dispute.DisputeStateLost, expected: "LOST"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			assert.Equal(t, tt.expected, string(tt.state))
		})
	}
}

func TestDisputeState_IsValid(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name     string
		state    dispute.DisputeState
		expected bool
	}{
		{name: "Draft valid", state: dispute.DisputeStateDraft, expected: true},
		{name: "Open valid", state: dispute.DisputeStateOpen, expected: true},
		{name: "PendingEvidence valid", state: dispute.DisputeStatePendingEvidence, expected: true},
		{name: "Won valid", state: dispute.DisputeStateWon, expected: true},
		{name: "Lost valid", state: dispute.DisputeStateLost, expected: true},
		{name: "Empty invalid", state: dispute.DisputeState(""), expected: false},
		{name: "Lowercase invalid", state: dispute.DisputeState("open"), expected: false},
		{name: "Unknown invalid", state: dispute.DisputeState("UNKNOWN"), expected: false},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			assert.Equal(t, tt.expected, tt.state.IsValid())
		})
	}
}

func TestDisputeState_String(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name     string
		state    dispute.DisputeState
		expected string
	}{
		{name: "Draft", state: dispute.DisputeStateDraft, expected: "DRAFT"},
		{name: "Open", state: dispute.DisputeStateOpen, expected: "OPEN"},
		{
			name:     "PendingEvidence",
			state:    dispute.DisputeStatePendingEvidence,
			expected: "PENDING_EVIDENCE",
		},
		{name: "Won", state: dispute.DisputeStateWon, expected: "WON"},
		{name: "Lost", state: dispute.DisputeStateLost, expected: "LOST"},
		{name: "Custom", state: dispute.DisputeState("CUSTOM"), expected: "CUSTOM"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			assert.Equal(t, tt.expected, tt.state.String())
		})
	}
}

func TestDisputeState_IsTerminal(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name     string
		state    dispute.DisputeState
		expected bool
	}{
		{name: "Draft not terminal", state: dispute.DisputeStateDraft, expected: false},
		{name: "Open not terminal", state: dispute.DisputeStateOpen, expected: false},
		{
			name:     "PendingEvidence not terminal",
			state:    dispute.DisputeStatePendingEvidence,
			expected: false,
		},
		{name: "Won is terminal", state: dispute.DisputeStateWon, expected: true},
		{name: "Lost is terminal", state: dispute.DisputeStateLost, expected: true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			assert.Equal(t, tt.expected, tt.state.IsTerminal())
		})
	}
}

func TestDisputeState_CanReopen(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name     string
		state    dispute.DisputeState
		expected bool
	}{
		{name: "Draft cannot reopen", state: dispute.DisputeStateDraft, expected: false},
		{name: "Open cannot reopen", state: dispute.DisputeStateOpen, expected: false},
		{
			name:     "PendingEvidence cannot reopen",
			state:    dispute.DisputeStatePendingEvidence,
			expected: false,
		},
		{name: "Won cannot reopen", state: dispute.DisputeStateWon, expected: false},
		{name: "Lost can reopen", state: dispute.DisputeStateLost, expected: true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			assert.Equal(t, tt.expected, tt.state.CanReopen())
		})
	}
}

func TestParseDisputeState(t *testing.T) {
	t.Parallel()

	valid := []struct {
		input    string
		expected dispute.DisputeState
	}{
		{"DRAFT", dispute.DisputeStateDraft},
		{"OPEN", dispute.DisputeStateOpen},
		{"PENDING_EVIDENCE", dispute.DisputeStatePendingEvidence},
		{"WON", dispute.DisputeStateWon},
		{"LOST", dispute.DisputeStateLost},
		{"draft", dispute.DisputeStateDraft},
		{"open", dispute.DisputeStateOpen},
		{"  WON  ", dispute.DisputeStateWon},
		{"pending_evidence", dispute.DisputeStatePendingEvidence},
		{"Lost", dispute.DisputeStateLost},
	}
	for _, tt := range valid {
		t.Run("Valid_"+tt.input, func(t *testing.T) {
			t.Parallel()

			parsed, err := dispute.ParseDisputeState(tt.input)
			require.NoError(t, err)
			assert.Equal(t, tt.expected, parsed)
		})
	}

	invalid := []string{"", "UNKNOWN", "invalid", "  "}
	for _, state := range invalid {
		t.Run("Invalid_"+state, func(t *testing.T) {
			t.Parallel()

			parsed, err := dispute.ParseDisputeState(state)
			require.Error(t, err)
			require.ErrorIs(t, err, dispute.ErrInvalidDisputeState)
			assert.Equal(t, dispute.DisputeState(""), parsed)
		})
	}
}
