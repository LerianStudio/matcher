package value_objects

// IdempotencyStatus represents the state of an idempotency key.
type IdempotencyStatus string

const (
	// IdempotencyStatusUnknown indicates the key has never been seen.
	IdempotencyStatusUnknown IdempotencyStatus = "unknown"
	// IdempotencyStatusPending indicates the request is currently being processed.
	IdempotencyStatusPending IdempotencyStatus = "pending"
	// IdempotencyStatusComplete indicates the request finished successfully.
	IdempotencyStatusComplete IdempotencyStatus = "complete"
	// IdempotencyStatusFailed indicates the request failed and can be retried.
	IdempotencyStatusFailed IdempotencyStatus = "failed"
)

// IsValid reports whether the idempotency status is a known value.
func (s IdempotencyStatus) IsValid() bool {
	switch s {
	case IdempotencyStatusUnknown,
		IdempotencyStatusPending,
		IdempotencyStatusComplete,
		IdempotencyStatusFailed:
		return true
	default:
		return false
	}
}

// IdempotencyResult holds the cached response for a completed idempotent request.
type IdempotencyResult struct {
	// Status indicates the current state of the idempotency key.
	Status IdempotencyStatus
	// Response contains the serialized JSON response (empty if pending/failed).
	Response []byte
	// HTTPStatus is the original HTTP status code to replay.
	HTTPStatus int
}
