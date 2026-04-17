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

// bridgeMessageSeparator prefixes appended history entries in
// BridgeLastErrorMessage. The arrow glyph is chosen so the field remains
// human-readable in CLI output and audit views while being distinct from any
// marker a wrapped Go error would produce.
const bridgeMessageSeparator = "\n→ "

// bridgeMessageTruncationSuffix marks the end of a message that was bounded
// by MaxBridgeFailureMessageLength. Kept short so the suffix fits within
// virtually any realistic cap without shaving meaningful content.
const bridgeMessageTruncationSuffix = "..."

// MarkBridgeFailed records a terminal bridge failure on the extraction. The
// extraction's discovery-side Status is left untouched: the upstream pipeline
// already finished (Status=COMPLETE) and the bridge worker is the one giving
// up. Setting BridgeLastError is what excludes the row from
// FindEligibleForBridge — see migration 000026.
//
// Idempotency with history: re-calling with the same class preserves the
// original failure reason (first message + BridgeFailedAt are frozen as the
// primary terminal event) and appends subsequent unique messages to
// BridgeLastErrorMessage separated by '→ '. Total message bounded by
// MaxBridgeFailureMessageLength — when appending would exceed the cap, the
// tail is truncated with a '...' suffix so the first reason stays intact.
// Re-calling with a *different* class is rejected because the first terminal
// classification wins; the worker should never observe a different class
// after the row is already terminal (FindEligibleForBridge filters terminal
// rows out before they reach the orchestrator), so a different class here
// means a wiring bug.
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

	trimmed := sanitizeBridgeMessage(strings.TrimSpace(message))
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

	// Idempotency with history: same-class re-call preserves the original
	// failure reason and appends subsequent unique messages so audit
	// forensics retain the full progression (e.g., "HMAC mismatch" stays
	// as the primary reason even when a later "escalated after 5 attempts"
	// message arrives).
	if er.BridgeLastError != "" && er.BridgeLastError == class {
		// Dedup: if the new entry text is already present anywhere in the
		// accumulated history, skip the append — this stops a worker that
		// retries with the same error string from bloating the column.
		if !strings.Contains(er.BridgeLastErrorMessage, trimmed) {
			er.BridgeLastErrorMessage = appendBridgeFailureMessage(
				er.BridgeLastErrorMessage,
				trimmed,
			)
		}

		// BridgeFailedAt is frozen at the first failure (it's the primary
		// terminal timestamp); only UpdatedAt bumps to reflect the row
		// change.
		er.UpdatedAt = time.Now().UTC()

		return nil
	}

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

// sanitizeBridgeMessage strips control bytes that would corrupt log output,
// audit-line parsing, or DB tooling when persisted in bridge_last_error_message.
// This runs on every caller-supplied entry BEFORE the entry is combined with
// bridgeMessageSeparator so the separator's intentional LF is preserved as
// the only newline in the final value.
//
// Stripped: NUL (0x00), CR (0x0D), LF (0x0A), and all other control characters
// in the 0x00–0x1F and 0x7F ranges EXCEPT tab (0x09) which remains legible in
// most log viewers. Upstream errors can legitimately contain tabs (e.g.,
// formatted multiline messages) so keeping them preserves useful context
// without opening the log-injection surface control bytes create.
//
// Non-ASCII UTF-8 bytes (0x80+) are kept verbatim — operator-facing messages
// can legitimately contain non-ASCII text (tenant names, record identifiers).
// This is a control-byte filter, not a charset restriction.
func sanitizeBridgeMessage(message string) string {
	if message == "" {
		return message
	}

	// Fast path: if no sanitizable byte is present, return the input unchanged
	// so the common case avoids allocation.
	if !strings.ContainsFunc(message, isBridgeControlByte) {
		return message
	}

	cleaned := make([]byte, 0, len(message))
	for _, r := range message {
		if isBridgeControlByte(r) {
			continue
		}

		cleaned = append(cleaned, string(r)...)
	}

	return string(cleaned)
}

// isBridgeControlByte reports whether a rune is a control byte that
// sanitizeBridgeMessage strips. Tab is exempt (handled separately in the
// caller) because it is legible in most log viewers and can appear in
// formatted upstream errors.
func isBridgeControlByte(r rune) bool {
	if r == '\t' {
		return false
	}

	return r < 0x20 || r == 0x7F
}

// appendBridgeFailureMessage concatenates a new history entry to an existing
// bridge failure message, keeping total length within
// MaxBridgeFailureMessageLength. If the appended total would overflow, the
// tail (newest entry) is truncated with '...' so the primary (first) reason
// stays intact — audit forensics are lossy at the edge, not the root.
func appendBridgeFailureMessage(existing, addition string) string {
	candidate := existing + bridgeMessageSeparator + addition
	if len(candidate) <= MaxBridgeFailureMessageLength {
		return candidate
	}

	// Reserve room for the suffix so the truncated column cleanly indicates
	// that the tail was clipped.
	budget := MaxBridgeFailureMessageLength - len(bridgeMessageTruncationSuffix)
	if budget <= len(existing) {
		// Even the primary message plus suffix no longer fits cleanly; keep
		// the existing history unchanged rather than corrupting it.
		return existing
	}

	return candidate[:budget] + bridgeMessageTruncationSuffix
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
