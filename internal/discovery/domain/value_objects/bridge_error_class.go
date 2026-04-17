// Copyright 2025 Lerian Studio. All rights reserved.
// Use of this source code is governed by an Elastic License 2.0
// that can be found in the LICENSE.md file.

package value_objects

import (
	"errors"
	"fmt"
	"strings"
)

// ErrInvalidBridgeErrorClass indicates an unrecognised bridge error class
// label. Returned by ParseBridgeErrorClass for inputs the persistence layer
// cannot map back to a domain enum value.
var ErrInvalidBridgeErrorClass = errors.New("invalid bridge error class")

// BridgeErrorClass enumerates the terminal failure classes the bridge worker
// persists on an extraction. Transient failures (network blips, custody
// 5xx) are NOT enumerated here — they live in the retry classifier and are
// never written to the DB; instead the worker increments bridge_attempts and
// applies backoff. Only terminal classifications make it into this enum, and
// only those values are valid for ExtractionRequest.MarkBridgeFailed.
//
// The string values are persisted in extraction_requests.bridge_last_error
// (VARCHAR(64)) so they MUST stay short, lowercase, and stable. Renaming a
// value is a breaking change because the column is read by support tooling
// and the readiness drilldown.
type BridgeErrorClass string

const (
	// BridgeErrorClassIntegrityFailed indicates the artifact failed HMAC
	// verification or AES-GCM authentication. Per T-002 design (D4 / D9),
	// integrity failures are terminal — the ciphertext is either tampered
	// or the key contract drifted; retrying changes nothing.
	BridgeErrorClassIntegrityFailed BridgeErrorClass = "integrity_failed"

	// BridgeErrorClassArtifactNotFound indicates Fetcher returned 404 for
	// the artifact URL. After a long-completed extraction, Fetcher may GC
	// the underlying object; once it is gone, no amount of retry will
	// recover it. Marking it terminal stops the livelock the bridge worker
	// would otherwise enter (P2 fix from T-005 preconditions).
	BridgeErrorClassArtifactNotFound BridgeErrorClass = "artifact_not_found"

	// BridgeErrorClassSourceUnresolved indicates no reconciliation source is
	// wired for the extraction's Fetcher connection. This is a configuration
	// gap. The current worker treats this as a soft skip (logs WARN, leaves
	// extraction unlinked), but if the gap persists past max attempts it
	// upgrades to terminal so the extraction exits the eligibility queue
	// until an operator wires the source manually.
	BridgeErrorClassSourceUnresolved BridgeErrorClass = "source_unresolved"

	// BridgeErrorClassMaxAttemptsExceeded indicates a transient class of
	// failure was retried up to the configured max_attempts ceiling without
	// succeeding. Set when bridge_attempts ≥ max_attempts and the underlying
	// error is in the transient bucket. Operators should investigate the
	// extraction's failure history (the underlying transient error is logged
	// at each attempt) and either wait for the upstream to recover and
	// re-enable bridging via support tool, or accept the extraction as lost.
	BridgeErrorClassMaxAttemptsExceeded BridgeErrorClass = "max_attempts_exceeded"
)

// IsValid reports whether the class is one of the four enumerated terminal
// classes. Used by ExtractionRequest.MarkBridgeFailed to refuse persisting an
// unknown class string.
func (c BridgeErrorClass) IsValid() bool {
	switch c {
	case BridgeErrorClassIntegrityFailed,
		BridgeErrorClassArtifactNotFound,
		BridgeErrorClassSourceUnresolved,
		BridgeErrorClassMaxAttemptsExceeded:
		return true
	}

	return false
}

// String returns the persisted string form of the class.
func (c BridgeErrorClass) String() string {
	return string(c)
}

// ParseBridgeErrorClass converts the persisted form back into the enum.
// Empty input is a programming error (callers should treat NULL DB values as
// "no terminal failure" before calling this); all other unrecognised inputs
// return ErrInvalidBridgeErrorClass so adapters can surface schema drift
// loudly instead of silently coercing the value.
func ParseBridgeErrorClass(raw string) (BridgeErrorClass, error) {
	trimmed := strings.TrimSpace(raw)
	if trimmed == "" {
		return "", fmt.Errorf("%w: empty", ErrInvalidBridgeErrorClass)
	}

	class := BridgeErrorClass(strings.ToLower(trimmed))
	if !class.IsValid() {
		return "", fmt.Errorf("%w: %s", ErrInvalidBridgeErrorClass, raw)
	}

	return class, nil
}
