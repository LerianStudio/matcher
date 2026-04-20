// Copyright 2025 Lerian Studio. All rights reserved.
// Use of this source code is governed by an Elastic License 2.0
// that can be found in the LICENSE.md file.

package worker

import (
	"errors"

	vo "github.com/LerianStudio/matcher/internal/discovery/domain/value_objects"
	sharedPorts "github.com/LerianStudio/matcher/internal/shared/ports"
)

// unknownRetentionBucket is the label used when a candidate extraction does
// not match either of the known retention buckets (terminal / late-linked).
const unknownRetentionBucket = "unknown"

// unknownBridgeRetryPolicy is the label used by BridgeRetryPolicy.String when
// an unrecognised enum value is printed. Separate from
// unknownRetentionBucket because the semantics differ (retry policy vs
// retention bucket) even though the two layers happen to emit the same text
// today. Keeping the constants distinct prevents accidental cross-wiring if
// either layer later needs a more specific label.
const unknownBridgeRetryPolicy = "unknown"

// redisLockReleaseLua is the Lua script used by the bridge and custody
// retention workers to release a distributed lock only when the caller still
// owns the token. Declaring it once here (a) keeps the two workers in lock-
// step and (b) avoids goconst triple-occurrence flagging from duplicating
// the script per-worker. KEYS[1] is the lock key; ARGV[1] is the owner token.
const redisLockReleaseLua = `
if redis.call("GET", KEYS[1]) == ARGV[1] then
  return redis.call("DEL", KEYS[1])
else
  return 0
end
`

// BridgeRetryPolicy enumerates how the worker should respond to a single
// bridgeOne error.
type BridgeRetryPolicy int

const (
	// RetryTransient means the underlying failure is expected to clear
	// (network blip, custody 5xx). The worker increments bridge_attempts
	// and re-eligibilises the row for the next tick after backoff.
	RetryTransient BridgeRetryPolicy = iota

	// RetryTerminal means the failure is permanent. The worker calls
	// MarkBridgeFailed with the matched class so the row exits the
	// eligibility queue. Operators must intervene (rotate keys, refresh
	// Fetcher artifact, etc.) to re-enable bridging.
	RetryTerminal

	// RetryIdempotent means the failure is actually a successful outcome
	// from another worker (e.g. ErrExtractionAlreadyLinked). The worker
	// swallows it without touching attempts or persisting failure state.
	// bridgeOne already maps these to nil before reaching the classifier;
	// returning this value from the classifier is defensive in case a
	// future code path leaks an idempotent sentinel through.
	RetryIdempotent
)

// String returns the policy label for logging.
func (p BridgeRetryPolicy) String() string {
	switch p {
	case RetryTransient:
		return "transient"
	case RetryTerminal:
		return "terminal"
	case RetryIdempotent:
		return "idempotent"
	default:
		return unknownBridgeRetryPolicy
	}
}

// BridgeRetryClassification is the worker's full take on a bridgeOne error:
// what policy applies and (when terminal) which BridgeErrorClass to persist.
//
// Class is empty for Transient/Idempotent because nothing is persisted in
// those branches. For Terminal, Class is one of the four enumerated values
// from the value_objects package.
type BridgeRetryClassification struct {
	Policy BridgeRetryPolicy
	Class  vo.BridgeErrorClass
}

// ClassifyBridgeError maps a bridgeOne error to a retry classification.
//
// The classifier is deterministic and total: every non-nil input receives a
// classification. Unknown errors default to RetryTransient because the cost
// of treating a permanent failure as transient is bounded (max_attempts
// upgrades it to RetryTerminal anyway), while the cost of treating a
// transient failure as terminal is permanent extraction loss.
//
// Sentinels recognised:
//
//	Terminal:
//	  - ErrIntegrityVerificationFailed → BridgeErrorClassIntegrityFailed
//	  - ErrFetcherResourceNotFound     → BridgeErrorClassArtifactNotFound
//
//	Transient:
//	  - ErrArtifactRetrievalFailed (5xx, 408, 425, transport)
//	  - ErrCustodyStoreFailed       (S3 put transient)
//	  - ErrFetcherUnavailable       (full-service outage)
//
//	Idempotent (defensive — bridgeOne maps these to nil before classify):
//	  - ErrExtractionAlreadyLinked
//	  - ErrBridgeExtractionIneligible
//
//	Special: ErrBridgeSourceUnresolvable is treated as RetryTransient at
//	first; the worker eventually upgrades it to BridgeErrorClassSourceUnresolved
//	after max_attempts because operators may wire the source mid-cycle.
//
// Nil input returns RetryIdempotent — callers can short-circuit the
// no-error path without a separate nil check.
func ClassifyBridgeError(err error) BridgeRetryClassification {
	if err == nil {
		return BridgeRetryClassification{Policy: RetryIdempotent}
	}

	// Terminal classifications: permanent failure regardless of how many
	// attempts we throw at it.
	switch {
	case errors.Is(err, sharedPorts.ErrIntegrityVerificationFailed):
		return BridgeRetryClassification{
			Policy: RetryTerminal,
			Class:  vo.BridgeErrorClassIntegrityFailed,
		}
	case errors.Is(err, sharedPorts.ErrFetcherResourceNotFound):
		return BridgeRetryClassification{
			Policy: RetryTerminal,
			Class:  vo.BridgeErrorClassArtifactNotFound,
		}
	}

	// Idempotent signals (defensive — should already be filtered upstream).
	switch {
	case errors.Is(err, sharedPorts.ErrExtractionAlreadyLinked):
		return BridgeRetryClassification{Policy: RetryIdempotent}
	case errors.Is(err, sharedPorts.ErrBridgeExtractionIneligible):
		return BridgeRetryClassification{Policy: RetryIdempotent}
	}

	// All other recognised sentinels (transport, custody, full-service
	// unavailability, source-unresolvable) collapse to transient. The
	// max-attempts gate in the worker is what eventually escalates them.
	return BridgeRetryClassification{Policy: RetryTransient}
}

// EscalateAfterMaxAttempts returns the terminal class to persist when a
// transient failure has been retried up to the configured ceiling. It
// inspects the underlying error so the persisted class still carries the
// right semantic when possible (e.g. config gap → source_unresolved); when
// no informative sentinel is recognised, it falls back to the generic
// max_attempts_exceeded class.
func EscalateAfterMaxAttempts(err error) vo.BridgeErrorClass {
	if err == nil {
		return vo.BridgeErrorClassMaxAttemptsExceeded
	}

	if errors.Is(err, sharedPorts.ErrBridgeSourceUnresolvable) {
		return vo.BridgeErrorClassSourceUnresolved
	}

	return vo.BridgeErrorClassMaxAttemptsExceeded
}
