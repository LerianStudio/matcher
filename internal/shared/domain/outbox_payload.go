// Package shared provides shared domain types used across bounded contexts.
package shared

import (
	"github.com/google/uuid"

	"github.com/LerianStudio/lib-commons/v5/commons/outbox"
)

// DefaultOutboxMaxPayloadBytes re-exports lib-commons v5's canonical
// per-event outbox payload cap so callers in the matcher tree can gate on
// the broker limit without pulling the outbox package directly for a
// single constant.
const DefaultOutboxMaxPayloadBytes = outbox.DefaultMaxPayloadBytes

// Truncation marker keys embedded into the Changes map when an outbox
// event payload exceeds the broker cap. These are the authoritative
// constants; the governance DTO converter and both audit publishers
// reference them so marker shape does not drift between producer and
// consumer.
const (
	// TruncatedMarkerKey is the top-level boolean flag that identifies a
	// truncated Changes map to downstream consumers.
	TruncatedMarkerKey = "_truncated"

	// TruncatedOriginalSizeKey is the byte size of the original Changes
	// payload before truncation, stored alongside TruncatedMarkerKey.
	TruncatedOriginalSizeKey = "_originalSize"

	// TruncatedNoteKey is a human-readable description of the truncation
	// event, persisted for operator convenience.
	TruncatedNoteKey = "_note"

	// TruncatedMaxAllowedKey is the cap that was enforced at truncation
	// time. Persisted so operators can trace which policy applied to a
	// given truncation event.
	TruncatedMaxAllowedKey = "_maxAllowed"
)

// TruncationNoteAuditDiff is the operator-facing message embedded in an
// audit event's Changes marker when the serialized envelope exceeded the
// outbox cap.
const TruncationNoteAuditDiff = "audit diff exceeded outbox payload cap; original not persisted"

// BuildAuditChangesTruncationMarker returns the canonical marker map that
// replaces an audit event's Changes when its serialized envelope exceeded
// the outbox cap. originalSize is the byte length of the oversize payload;
// maxAllowed is the cap that triggered truncation. Both values round-trip
// through the outbox so the governance DTO layer can surface them as
// first-class fields.
//
// This constructor is pure: emission of the WARN log line and truncation
// metric is the caller's responsibility via the outboxtelemetry adapter.
// Separating the constructor from the effects keeps this package inside
// the depguard domain-no-logging rule and makes it easy to build a marker
// at test time without a full tracking context.
func BuildAuditChangesTruncationMarker(originalSize, maxAllowed int) map[string]any {
	return map[string]any{
		TruncatedMarkerKey:       true,
		TruncatedOriginalSizeKey: originalSize,
		TruncatedNoteKey:         TruncationNoteAuditDiff,
		TruncatedMaxAllowedKey:   maxAllowed,
	}
}

// TruncateIDListIfTooLarge trims a UUID slice so the surrounding event
// envelope stays under maxBytes. maxBytes SHOULD be
// DefaultOutboxMaxPayloadBytes minus a safety margin for the non-ID
// fields of the event (tenant id, context id, timestamps, reason, etc).
//
// The function is pure: it returns (truncated, originalCount) without
// logging or emitting metrics. Callers that need an operator-visible
// trail detect truncation via `len(truncated) != originalCount` and
// invoke outboxtelemetry.RecordIDListTruncated. Caller responsibility is
// persisting the original count on the event (via TruncatedIDCount) so
// downstream consumers can detect data loss without re-measuring.
//
// UUID JSON encoding is fixed-width (36 chars plus quotes) so the
// serialized size is a closed-form function of the slice length. We use
// a binary search on that function rather than re-marshaling per trial.
func TruncateIDListIfTooLarge(
	ids []uuid.UUID,
	maxBytes int,
) (truncated []uuid.UUID, originalCount int) {
	originalCount = len(ids)
	if originalCount == 0 || maxBytes <= 0 {
		return ids, originalCount
	}

	if idListSerializedSize(originalCount) <= maxBytes {
		return ids, originalCount
	}

	// binarySearchMidFactor averages low+high with a +1 tiebreaker so the
	// search converges upward toward the largest fitting prefix rather
	// than oscillating around the cutoff.
	const binarySearchMidFactor = 2

	// Binary search for the largest prefix length whose serialized form
	// fits within maxBytes. O(log n) in the list size, no allocations.
	low, high := 0, originalCount
	for low < high {
		mid := (low + high + 1) / binarySearchMidFactor
		if idListSerializedSize(mid) <= maxBytes {
			low = mid
		} else {
			high = mid - 1
		}
	}

	return ids[:low], originalCount
}

// UUID JSON encoding constants for idListSerializedSize. Canonical UUIDs
// are 36 characters plus two quotes; JSON array encoding adds a comma
// between elements and surrounding brackets.
const (
	uuidJSONBytes     = 38 // 36 chars + 2 quotes
	separatorBytes    = 1  // single comma between elements
	bracketPairBytes  = 2  // leading '[' + trailing ']'
	emptyArrayJSONLen = 2  // "[]"
)

// idListSerializedSize returns the byte length of a JSON array containing
// idCount UUIDs in canonical RFC 4122 textual form.
//
//	[]                   -> 2 bytes
//	["<uuid>"]           -> 2 + 38 = 40 bytes
//	["<uuid>","<uuid>"]  -> 2 + 38*2 + 1 = 79 bytes
func idListSerializedSize(idCount int) int {
	if idCount <= 0 {
		return emptyArrayJSONLen
	}

	return bracketPairBytes + idCount*uuidJSONBytes + (idCount-1)*separatorBytes
}
