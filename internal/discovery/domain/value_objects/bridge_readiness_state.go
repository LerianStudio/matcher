package value_objects

import (
	"errors"
	"fmt"
	"strings"
)

// ErrInvalidBridgeReadinessState indicates an unrecognised readiness label.
var ErrInvalidBridgeReadinessState = errors.New("invalid bridge readiness state")

// BridgeReadinessState represents the observable state of a FETCHER extraction
// with respect to the bridge pipeline. It is derived at query time from
// extraction_requests.status and extraction_requests.ingestion_job_id, plus a
// configurable stale threshold; it is NOT persisted as a column.
//
// State transitions (informational — T-004 is read-only):
//
//	CREATED   → IN_FLIGHT (extraction issued, status in PENDING/SUBMITTED/EXTRACTING
//	                     — bridge work has not started because extraction is still running)
//	IN_FLIGHT → PENDING (becomes eligible when status=COMPLETE and unlinked)
//	PENDING   → READY   (when bridge worker links to ingestion job)
//	PENDING   → STALE   (when NOW() - created_at exceeds the stale threshold
//	                     without bridging — flag, not terminal)
//	STALE     → READY   (if worker eventually succeeds)
//	any       → FAILED  (when extraction's own pipeline fails at discovery
//	                     layer — status IN ('FAILED','CANCELLED'))
//
// T-005 will introduce explicit terminal-fail semantics for *bridge* failures
// (currently absent); for now, FAILED here corresponds to the original
// extraction failure, not a bridge-specific failure.
//
// @Description Observable bridge readiness state derived from extraction lifecycle
// @Enum pending,ready,stale,failed,in_flight
type BridgeReadinessState string

const (
	// BridgeReadinessPending indicates a COMPLETE extraction whose bridge work
	// is still inside the staleness window (worker is expected to drain it).
	BridgeReadinessPending BridgeReadinessState = "pending"
	// BridgeReadinessReady indicates a COMPLETE extraction successfully linked
	// to an ingestion job. This is the happy-path terminal state for bridging.
	BridgeReadinessReady BridgeReadinessState = "ready"
	// BridgeReadinessStale indicates a COMPLETE extraction that has remained
	// unlinked beyond the configured staleness threshold. Operators should
	// investigate; the worker may still succeed eventually.
	BridgeReadinessStale BridgeReadinessState = "stale"
	// BridgeReadinessFailed indicates the extraction itself failed or was
	// cancelled at the discovery layer; bridging will never run.
	BridgeReadinessFailed BridgeReadinessState = "failed"
	// BridgeReadinessInFlight indicates the upstream extraction is still
	// running (status PENDING/SUBMITTED/EXTRACTING). Bridge work has not
	// started because the extraction itself has not produced output yet.
	// Operators should expect this bucket to be non-zero whenever Fetcher is
	// actively working — empty means nothing is being extracted.
	BridgeReadinessInFlight BridgeReadinessState = "in_flight"
)

// IsValid reports whether the readiness label is supported.
func (b BridgeReadinessState) IsValid() bool {
	switch b {
	case BridgeReadinessPending, BridgeReadinessReady, BridgeReadinessStale,
		BridgeReadinessFailed, BridgeReadinessInFlight:
		return true
	}

	return false
}

// String returns the lowercase string representation suitable for API output.
func (b BridgeReadinessState) String() string {
	return string(b)
}

// IsTerminal reports whether the readiness state represents a final outcome
// from the bridge pipeline's perspective. READY and FAILED are terminal;
// PENDING, STALE, and IN_FLIGHT are still in flight.
//
// Reserved for T-005's bridge-failure classifier; T-004 does not call this in
// production code. Kept on the value object because the staging implementation
// will need it without changing the type's exported surface.
func (b BridgeReadinessState) IsTerminal() bool {
	return b == BridgeReadinessReady || b == BridgeReadinessFailed
}

// IsActionable reports whether operators should investigate this state.
// STALE and FAILED are actionable; PENDING, READY, and IN_FLIGHT are not.
//
// Reserved for T-005's alerting integration; T-004 does not call this in
// production code. Kept on the value object so the alerting layer can rely on
// the same predicate the dashboard documents.
func (b BridgeReadinessState) IsActionable() bool {
	return b == BridgeReadinessStale || b == BridgeReadinessFailed
}

// ParseBridgeReadinessState parses a string into a BridgeReadinessState.
// Accepts case-insensitive input so query strings like "?state=Ready" work.
func ParseBridgeReadinessState(s string) (BridgeReadinessState, error) {
	state := BridgeReadinessState(strings.ToLower(strings.TrimSpace(s)))
	if !state.IsValid() {
		return "", fmt.Errorf("%w: %s", ErrInvalidBridgeReadinessState, s)
	}

	return state, nil
}
