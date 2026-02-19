// Package ports defines outbound interfaces for the exception bounded context.
package ports

import (
	"context"
	"crypto/sha256"
	"database/sql"
	"encoding/hex"
	"time"

	"github.com/google/uuid"
)

//go:generate mockgen -destination=mocks/audit_publisher_mock.go -package=mocks . AuditPublisher

// ActorHashLength is the length of the truncated hash for actor pseudonymization.
// 16 hex characters provide ~64 bits of entropy; collisions reach ~50% probability
// after ~2^32 unique actors (birthday bound). This is acceptable for audit
// correlation but not for strong uniqueness guarantees; consumers that rely on
// ActorHashLength (including audit publishing code) should not treat it as a
// stable unique identifier and should pair it with additional disambiguators.
const ActorHashLength = 16

// HashActor generates a pseudonymized hash of an actor identifier.
// Uses SHA-256 truncated to 16 characters for readability while maintaining
// uniqueness for correlation purposes. The hash is one-way, so the original
// actor ID cannot be recovered from logs but can still be correlated across events.
func HashActor(actor string) string {
	if actor == "" {
		return ""
	}

	hash := sha256.Sum256([]byte(actor))
	fullHex := hex.EncodeToString(hash[:])

	return fullHex[:ActorHashLength]
}

// AuditEvent represents an audit event for exception operations.
// ActorHash provides a pseudonymized version of the actor identifier for PII protection.
// The hash allows audit correlation without exposing the original identifier.
type AuditEvent struct {
	ExceptionID uuid.UUID
	Action      string
	Actor       string // Original actor identifier (for internal use only)
	ActorHash   string // Pseudonymized actor hash (for audit storage)
	Notes       string
	ReasonCode  *string
	OccurredAt  time.Time
	Metadata    map[string]string
}

// GetActorHash returns the pseudonymized actor hash, deriving it from Actor when needed.
func (event AuditEvent) GetActorHash() string {
	if event.ActorHash != "" {
		return event.ActorHash
	}

	if event.Actor == "" {
		return ""
	}

	return HashActor(event.Actor)
}

// AuditPublisher publishes exception audit events.
type AuditPublisher interface {
	PublishExceptionEvent(ctx context.Context, event AuditEvent) error
	// PublishExceptionEventWithTx publishes an audit event within the provided transaction.
	// This enables atomic audit logging - either both the state change and audit succeed,
	// or both fail. Required for SOX compliance.
	PublishExceptionEventWithTx(ctx context.Context, tx *sql.Tx, event AuditEvent) error
}
