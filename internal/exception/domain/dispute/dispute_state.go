package dispute

import (
	"errors"
	"strings"
)

// ErrInvalidDisputeState is returned when parsing an invalid dispute state.
var ErrInvalidDisputeState = errors.New("invalid dispute state")

// DisputeState represents lifecycle state for a dispute.
type DisputeState string

// DisputeState values.
const (
	DisputeStateDraft           DisputeState = "DRAFT"
	DisputeStateOpen            DisputeState = "OPEN"
	DisputeStatePendingEvidence DisputeState = "PENDING_EVIDENCE"
	DisputeStateWon             DisputeState = "WON"
	DisputeStateLost            DisputeState = "LOST"
)

// IsValid checks if the state is valid.
func (state DisputeState) IsValid() bool {
	switch state {
	case DisputeStateDraft,
		DisputeStateOpen,
		DisputeStatePendingEvidence,
		DisputeStateWon,
		DisputeStateLost:
		return true
	default:
		return false
	}
}

// String returns the string representation of the state.
func (state DisputeState) String() string {
	return string(state)
}

// IsTerminal returns true if the state is a terminal state (Won or Lost).
func (state DisputeState) IsTerminal() bool {
	return state == DisputeStateWon || state == DisputeStateLost
}

// CanReopen returns true if a dispute in this state can be reopened.
func (state DisputeState) CanReopen() bool {
	return state == DisputeStateLost
}

// ParseDisputeState parses a string into a DisputeState.
// Input is normalized to uppercase for case-insensitive parsing.
func ParseDisputeState(value string) (DisputeState, error) {
	state := DisputeState(strings.ToUpper(strings.TrimSpace(value)))
	if !state.IsValid() {
		return "", ErrInvalidDisputeState
	}

	return state, nil
}
