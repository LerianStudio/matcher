package value_objects

import (
	"fmt"

	shared "github.com/LerianStudio/matcher/internal/shared/domain"
)

// Idempotency key validation errors re-exported from the shared kernel.
var (
	ErrEmptyIdempotencyKey   = shared.ErrEmptyIdempotencyKey
	ErrInvalidIdempotencyKey = shared.ErrInvalidIdempotencyKey
)

// IdempotencyKey represents a unique key for callback deduplication.
type IdempotencyKey = shared.IdempotencyKey

// CompareSignedKey verifies a signed key against the client key using HMAC.
func CompareSignedKey(signed string, clientKey IdempotencyKey, secret string) bool {
	return shared.CompareSignedKey(signed, clientKey, secret)
}

// ParseIdempotencyKey parses and validates an idempotency key string.
func ParseIdempotencyKey(value string) (IdempotencyKey, error) {
	key, err := shared.ParseIdempotencyKey(value)
	if err != nil {
		return "", fmt.Errorf("parse idempotency key: %w", err)
	}

	return key, nil
}
