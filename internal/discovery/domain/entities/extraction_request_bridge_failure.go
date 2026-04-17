// Copyright 2025 Lerian Studio. All rights reserved.
// Use of this source code is governed by an Elastic License 2.0
// that can be found in the LICENSE.md file.

package entities

import (
	"errors"
	"fmt"
	"strings"
	"time"

	vo "github.com/LerianStudio/matcher/internal/discovery/domain/value_objects"
)

// Sentinel errors for bridge failure transitions. Distinct from the
// extraction status sentinels because bridge failure is a separate state
// machine — see the docstring on ExtractionRequest.
var (
	// ErrBridgeFailureClassRequired indicates MarkBridgeFailed was called
	// with an invalid (empty / unknown) BridgeErrorClass. Returned without
	// mutating the entity.
	ErrBridgeFailureClassRequired = errors.New("bridge failure class is required")

	// ErrBridgeFailureMessageRequired indicates MarkBridgeFailed was called
	// with an empty error message. Operators rely on the message to triage,
	// so silently persisting an empty value would be worse than failing
	// loudly here.
	ErrBridgeFailureMessageRequired = errors.New("bridge failure message is required")
)

// MaxBridgeFailureMessageLength caps the operator-facing message persisted in
// extraction_requests.bridge_last_error_message. The DB column is TEXT so it
// will accept anything; this entity-level bound prevents unbounded blobs from
// upstream errors leaking into the row. 1024 chars covers a typical wrapped
// error chain (5–10 layers) without being so generous that an attacker-
// controlled path can bloat the column.
const MaxBridgeFailureMessageLength = 1024

// MarkBridgeFailed records a terminal bridge failure on the extraction. The
// extraction's discovery-side Status is left untouched: the upstream pipeline
// already finished (Status=COMPLETE) and the bridge worker is the one giving
// up. Setting BridgeLastError is what excludes the row from
// FindEligibleForBridge — see migration 000026.
//
// Idempotency: re-calling with the same class returns nil after refreshing
// UpdatedAt and bumping BridgeFailedAt. Re-calling with a *different* class
// is rejected because the first terminal classification wins; the worker
// should never observe a different class after the row is already terminal
// (FindEligibleForBridge filters terminal rows out before they reach the
// orchestrator), so a different class here means a wiring bug.
//
// BridgeAttempts is NOT bumped here — incrementing attempts is the worker's
// responsibility (see RecordBridgeAttempt), which runs unconditionally on
// every tick whether or not the failure ends up terminal.
func (er *ExtractionRequest) MarkBridgeFailed(class vo.BridgeErrorClass, message string) error {
	if er == nil {
		return nil
	}

	if !class.IsValid() {
		return fmt.Errorf("%w: %q", ErrBridgeFailureClassRequired, string(class))
	}

	trimmed := strings.TrimSpace(message)
	if trimmed == "" {
		return ErrBridgeFailureMessageRequired
	}

	if len(trimmed) > MaxBridgeFailureMessageLength {
		trimmed = trimmed[:MaxBridgeFailureMessageLength]
	}

	// Polish Fix 5: status guard. The bridge state machine only operates on
	// extractions whose upstream pipeline has already finished successfully
	// (Status=COMPLETE). Marking a still-in-flight or already-failed
	// extraction as bridge-failed would corrupt the two-state-machine
	// invariant documented on ExtractionRequest. Today the only caller is
	// the bridge worker, which pre-filters via FindEligibleForBridge — but
	// domain invariants must not depend on adapter-layer filters. Mirrors
	// LinkToIngestion's identical guard.
	if er.Status != vo.ExtractionStatusComplete {
		return fmt.Errorf(
			"%w: cannot mark bridge failure on extraction in state %s",
			ErrInvalidTransition,
			er.Status,
		)
	}

	// Idempotency: same-class re-call refreshes the timestamp without
	// destroying the original signal.
	if er.BridgeLastError != "" && er.BridgeLastError != class {
		return fmt.Errorf(
			"%w: extraction already terminally failed with class %q",
			ErrBridgeFailureClassRequired,
			er.BridgeLastError,
		)
	}

	now := time.Now().UTC()
	er.BridgeLastError = class
	er.BridgeLastErrorMessage = trimmed
	er.BridgeFailedAt = now
	er.UpdatedAt = now

	return nil
}

// RecordBridgeAttempt increments BridgeAttempts and bumps UpdatedAt. Called
// by the worker on every tick that picks the extraction up, regardless of
// outcome. Combined with the terminal-failure check (BridgeAttempts ≥ max),
// this is what drives the max_attempts_exceeded transition.
//
// Returns the new attempts value so the caller can compare against the
// configured max without re-reading the field.
func (er *ExtractionRequest) RecordBridgeAttempt() int {
	if er == nil {
		return 0
	}

	er.BridgeAttempts++
	er.UpdatedAt = time.Now().UTC()

	return er.BridgeAttempts
}

// HasTerminalBridgeFailure reports whether this extraction has been written
// off by the bridge worker. True when BridgeLastError is set; the readiness
// projection uses this to bucket the row as "failed" instead of "pending"
// or "stale".
func (er *ExtractionRequest) HasTerminalBridgeFailure() bool {
	if er == nil {
		return false
	}

	return er.BridgeLastError != ""
}
