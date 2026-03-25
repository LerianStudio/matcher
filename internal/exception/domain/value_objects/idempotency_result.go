package value_objects

import shared "github.com/LerianStudio/matcher/internal/shared/domain"

// IdempotencyStatus represents the state of an idempotency key.
type IdempotencyStatus = shared.IdempotencyStatus

// Re-exported idempotency statuses from the shared kernel.
const (
	IdempotencyStatusUnknown  = shared.IdempotencyStatusUnknown
	IdempotencyStatusPending  = shared.IdempotencyStatusPending
	IdempotencyStatusComplete = shared.IdempotencyStatusComplete
	IdempotencyStatusFailed   = shared.IdempotencyStatusFailed
)

// IdempotencyResult holds the cached response for a completed idempotent request.
type IdempotencyResult = shared.IdempotencyResult
