package dispute

import (
	"errors"
	"slices"
)

// ErrInvalidDisputeTransition is returned when a state transition is not allowed.
var ErrInvalidDisputeTransition = errors.New("invalid dispute transition")

// allowedDisputeTransitions holds the static map of valid state transitions,
// allocated once at package init to avoid per-call allocation.
//
//nolint:gochecknoglobals // package-level read-only lookup table for state machine transitions.
var allowedDisputeTransitions = map[DisputeState][]DisputeState{
	DisputeStateDraft: {DisputeStateOpen},
	DisputeStateOpen: {
		DisputeStatePendingEvidence,
		DisputeStateWon,
		DisputeStateLost,
	},
	DisputeStatePendingEvidence: {DisputeStateOpen, DisputeStateWon, DisputeStateLost},
	DisputeStateWon:             {},
	DisputeStateLost:            {DisputeStateOpen},
}

// AllowedDisputeTransitions returns a deep copy of the valid state transitions
// so callers cannot mutate the package-level map.
func AllowedDisputeTransitions() map[DisputeState][]DisputeState {
	cp := make(map[DisputeState][]DisputeState, len(allowedDisputeTransitions))
	for k, v := range allowedDisputeTransitions {
		dst := make([]DisputeState, len(v))
		copy(dst, v)
		cp[k] = dst
	}

	return cp
}

// ValidateDisputeTransition checks if transitioning from one state to another is allowed.
func ValidateDisputeTransition(from, to DisputeState) error {
	if !from.IsValid() || !to.IsValid() {
		return ErrInvalidDisputeState
	}

	if slices.Contains(allowedDisputeTransitions[from], to) {
		return nil
	}

	return ErrInvalidDisputeTransition
}
