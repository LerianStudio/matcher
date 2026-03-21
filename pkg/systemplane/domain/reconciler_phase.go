// Copyright 2025 Lerian Studio.

package domain

import (
	"errors"
	"fmt"
	"strings"
)

// ReconcilerPhase classifies when a reconciler runs relative to others during
// a reload cycle. The supervisor sorts reconcilers by phase before execution,
// ensuring deterministic ordering without relying on slice position.
//
// Phases execute in ascending order:
//
//	PhaseStateSync  (0) → update shared state (ConfigManager, caches)
//	PhaseValidation (1) → gates that can reject the change
//	PhaseSideEffect (2) → external side effects (worker restarts, notifications)
//
// Error semantics differ by phase:
//   - PhaseStateSync failure: fatal, immediate rollback
//   - PhaseValidation failure: change rejected, rollback
//   - PhaseSideEffect failure: config is valid but side effect failed
type ReconcilerPhase int

// Supported ReconcilerPhase values.
const (
	// PhaseStateSync reconcilers run first. They update shared in-process
	// state (e.g., ConfigManager's atomic pointer) so downstream reconcilers
	// see consistent data through shared accessors like configManager.Get().
	PhaseStateSync ReconcilerPhase = iota

	// PhaseValidation reconcilers run second. They act as gates — if a
	// validation reconciler returns an error, the entire reload rolls back.
	// Use for structural validation of the candidate bundle (e.g., HTTP
	// policy constraints, connection string format checks).
	PhaseValidation

	// PhaseSideEffect reconcilers run last. They trigger external side
	// effects such as restarting background workers, warming caches, or
	// sending notifications. The configuration change is already committed
	// to shared state by the time these run.
	PhaseSideEffect
)

// ErrInvalidReconcilerPhase indicates an unrecognized reconciler phase.
var ErrInvalidReconcilerPhase = errors.New("invalid reconciler phase")

// String returns a human-readable label for the phase.
func (phase ReconcilerPhase) String() string {
	switch phase {
	case PhaseStateSync:
		return "state-sync"
	case PhaseValidation:
		return "validation"
	case PhaseSideEffect:
		return "side-effect"
	default:
		return fmt.Sprintf("unknown(%d)", int(phase))
	}
}

// IsValid reports whether the phase is a recognized value.
func (phase ReconcilerPhase) IsValid() bool {
	switch phase {
	case PhaseStateSync, PhaseValidation, PhaseSideEffect:
		return true
	}

	return false
}

// ParseReconcilerPhase parses a string into a ReconcilerPhase (case-insensitive).
func ParseReconcilerPhase(rawValue string) (ReconcilerPhase, error) {
	switch strings.ToLower(strings.TrimSpace(rawValue)) {
	case "state-sync":
		return PhaseStateSync, nil
	case "validation":
		return PhaseValidation, nil
	case "side-effect":
		return PhaseSideEffect, nil
	default:
		return -1, fmt.Errorf("parse %q: %w", rawValue, ErrInvalidReconcilerPhase)
	}
}
