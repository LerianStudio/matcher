package shared

import (
	"crypto/hmac"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"regexp"
	"strings"
)

// Idempotency key validation errors.
var (
	ErrEmptyIdempotencyKey   = errors.New("idempotency key is required")
	ErrInvalidIdempotencyKey = errors.New("invalid idempotency key format")
)

const idempotencyKeyMaxLength = 128

var idempotencyKeyPattern = regexp.MustCompile(`^[A-Za-z0-9:_-]+$`)

// IdempotencyKey represents a unique key for request deduplication.
//
// Server-side HMAC signing is available via SignKey() to prevent key prediction
// attacks. When an HMAC secret is configured, the signed (hashed) key is used
// for Redis storage while the client-facing key format remains unchanged.
type IdempotencyKey string

// IsValid returns true if the idempotency key is properly formatted.
func (key IdempotencyKey) IsValid() bool {
	s := string(key)
	if s == "" || len(s) > idempotencyKeyMaxLength {
		return false
	}

	return idempotencyKeyPattern.MatchString(s)
}

// String returns the string representation of the idempotency key.
func (key IdempotencyKey) String() string {
	return string(key)
}

// SignKey produces an HMAC-SHA256 hash of the idempotency key using the provided
// secret. The resulting hex-encoded string is what should be stored in Redis,
// preventing attackers who know the client key format from predicting storage keys.
//
// If secret is empty, the original key string is returned unchanged. This provides
// backward compatibility when HMAC signing is not configured.
func (key IdempotencyKey) SignKey(secret string) string {
	if secret == "" {
		return key.String()
	}

	mac := hmac.New(sha256.New, []byte(secret))
	mac.Write([]byte(key.String()))

	return hex.EncodeToString(mac.Sum(nil))
}

// CompareSignedKey verifies that a previously-stored signed key matches the
// HMAC-SHA256 signature of the given client key under the same secret.
func CompareSignedKey(signed string, clientKey IdempotencyKey, secret string) bool {
	return hmac.Equal([]byte(signed), []byte(clientKey.SignKey(secret)))
}

// ParseIdempotencyKey parses and validates an idempotency key string.
func ParseIdempotencyKey(value string) (IdempotencyKey, error) {
	trimmed := strings.TrimSpace(value)
	if trimmed == "" {
		return "", ErrEmptyIdempotencyKey
	}

	key := IdempotencyKey(trimmed)
	if !key.IsValid() {
		return "", ErrInvalidIdempotencyKey
	}

	return key, nil
}

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
	Status     IdempotencyStatus
	Response   []byte
	HTTPStatus int
}
