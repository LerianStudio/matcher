// Package ports defines outbound interfaces for the exception bounded context.
package ports

import (
	"context"
	"crypto/hmac"
	"crypto/sha256"
	"database/sql"
	"encoding/hex"
	"time"

	"github.com/google/uuid"
)

//go:generate mockgen -destination=mocks/audit_publisher_mock.go -package=mocks . AuditPublisher

// ActorHashLength is the length of the truncated hash for actor pseudonymization.
// 32 hex characters provide ~128 bits of entropy: collisions reach ~50% probability
// only after ~2^64 unique actors (birthday bound), removing the 64-bit collision
// risk of the earlier 16-character format. Hashes remain readable in UI and logs.
//
// Pre-existing audit_log rows hashed with the 16-character format are NOT
// re-hashed on upgrade — the audit chain is append-only. New rows produced
// after the upgrade use the new format.
const ActorHashLength = 32

// SaltProvider returns the salt used to key the actor hash. The salt is
// intentionally pulled lazily (not captured at publisher construction) so
// implementations can derive the salt per-tenant from the ctx, reload from
// systemplane without publisher reconstruction, or rotate without downtime.
//
// Returning an empty string disables salting: the hash degrades to the
// pre-salt SHA-256 truncation. This is the documented fallback for boot
// scenarios where the secret has not yet been provisioned — operators see
// the unsalted hashes in logs, recognise the format, and provision a salt
// via admin PUT.
type SaltProvider interface {
	SaltFor(ctx context.Context) string
}

// SaltProviderFunc adapts a plain function to SaltProvider.
type SaltProviderFunc func(ctx context.Context) string

// SaltFor implements SaltProvider.
func (fn SaltProviderFunc) SaltFor(ctx context.Context) string {
	if fn == nil {
		return ""
	}

	return fn(ctx)
}

// HashActor generates a pseudonymized hash of an actor identifier using an
// HMAC-SHA-256 keyed by salt. When salt is empty the function falls back to
// unsalted SHA-256 for boot-time compatibility; this mode is vulnerable to
// offline rainbow-table attacks on common actor IDs and should be replaced
// with a non-empty salt in production deployments.
//
// The hash is one-way, so the original actor ID cannot be recovered from
// logs but can still be correlated across events within a salt lifetime.
func HashActor(actor, salt string) string {
	if actor == "" {
		return ""
	}

	var digest []byte

	if salt == "" {
		sum := sha256.Sum256([]byte(actor))
		digest = sum[:]
	} else {
		mac := hmac.New(sha256.New, []byte(salt))
		mac.Write([]byte(actor))
		digest = mac.Sum(nil)
	}

	fullHex := hex.EncodeToString(digest)

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

// ResolveActorHash returns the pseudonymized actor hash, deriving it from
// Actor when the pre-computed ActorHash field is empty. When salt is empty
// the digest degrades to unsalted SHA-256 truncated to ActorHashLength hex
// characters, preserving the legacy correlation behaviour for boot paths
// that have not yet wired a SaltProvider.
func (event AuditEvent) ResolveActorHash(salt string) string {
	if event.ActorHash != "" {
		return event.ActorHash
	}

	if event.Actor == "" {
		return ""
	}

	return HashActor(event.Actor, salt)
}

// AuditPublisher publishes exception audit events.
type AuditPublisher interface {
	PublishExceptionEvent(ctx context.Context, event AuditEvent) error
	// PublishExceptionEventWithTx publishes an audit event within the provided transaction.
	// This enables atomic audit logging - either both the state change and audit succeed,
	// or both fail. Required for SOX compliance.
	PublishExceptionEventWithTx(ctx context.Context, tx *sql.Tx, event AuditEvent) error
}
