package value_objects

import (
	"errors"
	"slices"
)

// ErrInvalidResolutionTransition is returned when a status transition is not allowed.
var ErrInvalidResolutionTransition = errors.New("invalid resolution transition")

// This table covers resolution-direction transitions. Unassign (ASSIGNED->OPEN)
// is a separate domain operation with its own validation in the entity.
//
//nolint:gochecknoglobals // package-level read-only lookup table for state machine transitions.
var allowedResolutionTransitions = map[ExceptionStatus][]ExceptionStatus{
	ExceptionStatusOpen:              {ExceptionStatusAssigned, ExceptionStatusResolved, ExceptionStatusPendingResolution},
	ExceptionStatusAssigned:          {ExceptionStatusResolved, ExceptionStatusPendingResolution},
	ExceptionStatusPendingResolution: {ExceptionStatusResolved, ExceptionStatusOpen, ExceptionStatusAssigned},
	ExceptionStatusResolved:          {},
}

// AllowedResolutionTransitions returns a deep copy of the valid status transitions
// so callers cannot mutate the package-level map.
func AllowedResolutionTransitions() map[ExceptionStatus][]ExceptionStatus {
	cp := make(map[ExceptionStatus][]ExceptionStatus, len(allowedResolutionTransitions))
	for k, v := range allowedResolutionTransitions {
		dst := make([]ExceptionStatus, len(v))
		copy(dst, v)
		cp[k] = dst
	}

	return cp
}

// ValidateResolutionTransition checks if transitioning from one status to another is allowed.
func ValidateResolutionTransition(from, to ExceptionStatus) error {
	if !from.IsValid() || !to.IsValid() {
		return ErrInvalidExceptionStatus
	}

	if slices.Contains(allowedResolutionTransitions[from], to) {
		return nil
	}

	return ErrInvalidResolutionTransition
}
