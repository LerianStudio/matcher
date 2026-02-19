// Package entities defines governance domain types and validation logic.
package entities

import (
	"context"
	"encoding/json"
	"errors"
	"strings"
	"time"

	"github.com/google/uuid"

	"github.com/LerianStudio/lib-uncommons/v2/uncommons/assert"

	"github.com/LerianStudio/matcher/internal/shared/constants"
)

// Field length limits matching database constraints.
const (
	MaxEntityTypeLength = 50
	MaxActionLength     = 50
	MaxActorIDLength    = 255
)

// Sentinel errors for audit log validation.
var (
	ErrTenantIDRequired   = errors.New("tenant id is required")
	ErrEntityTypeRequired = errors.New("entity type is required")
	ErrEntityTypeTooLong  = errors.New("entity type exceeds maximum length")
	ErrEntityIDRequired   = errors.New("entity id is required")
	ErrActionRequired     = errors.New("action is required")
	ErrActionTooLong      = errors.New("action exceeds maximum length")
	ErrActorIDTooLong     = errors.New("actor id exceeds maximum length")
	ErrChangesRequired    = errors.New("changes are required")
	ErrChangesInvalidJSON = errors.New("changes must be valid JSON")
)

// AuditLogFilter defines optional filters for listing audit logs.
type AuditLogFilter struct {
	Actor      *string
	DateFrom   *time.Time
	DateTo     *time.Time
	Action     *string
	EntityType *string
}

// AuditLog represents an immutable, append-only audit record.
//
// Each audit log entry includes a cryptographic hash chain for tamper detection:
//   - TenantSeq: Monotonic sequence number per tenant for deterministic ordering
//   - PrevHash: SHA-256 hash of the previous record (genesis uses 32 zero bytes)
//   - RecordHash: SHA-256(PrevHash || canonicalized record content)
//
// This blockchain-like structure ensures that any modification to a historical
// record breaks the chain and is detectable during verification.
type AuditLog struct {
	ID          uuid.UUID
	TenantID    uuid.UUID
	EntityType  string
	EntityID    uuid.UUID
	Action      string
	ActorID     *string
	Changes     []byte
	CreatedAt   time.Time
	TenantSeq   int64  // Sequence number within tenant's chain (1-based)
	PrevHash    []byte // SHA-256 hash of previous record (32 bytes)
	RecordHash  []byte // SHA-256 hash of this record (32 bytes)
	HashVersion int16  // Version of the hashing scheme
}

// NewAuditLog validates inputs and returns a new append-only audit log.
func NewAuditLog(
	ctx context.Context,
	tenantID uuid.UUID,
	entityType string,
	entityID uuid.UUID,
	action string,
	actorID *string,
	changes []byte,
) (*AuditLog, error) {
	asserter := assert.New(ctx, nil, constants.ApplicationName, "governance.audit_log.new")

	if err := asserter.That(ctx, tenantID != uuid.Nil, "tenant id is required"); err != nil {
		return nil, ErrTenantIDRequired
	}

	trimmedEntityType := strings.TrimSpace(entityType)
	if err := asserter.NotEmpty(ctx, trimmedEntityType, "entity type is required"); err != nil {
		return nil, ErrEntityTypeRequired
	}

	if err := asserter.That(ctx, len(trimmedEntityType) <= MaxEntityTypeLength, "entity type exceeds maximum length"); err != nil {
		return nil, ErrEntityTypeTooLong
	}

	if err := asserter.That(ctx, entityID != uuid.Nil, "entity id is required"); err != nil {
		return nil, ErrEntityIDRequired
	}

	trimmedAction := strings.TrimSpace(action)
	if err := asserter.NotEmpty(ctx, trimmedAction, "action is required"); err != nil {
		return nil, ErrActionRequired
	}

	if err := asserter.That(ctx, len(trimmedAction) <= MaxActionLength, "action exceeds maximum length"); err != nil {
		return nil, ErrActionTooLong
	}

	if err := asserter.That(ctx, len(changes) > 0, "changes are required"); err != nil {
		return nil, ErrChangesRequired
	}

	if err := asserter.That(ctx, json.Valid(changes), "changes must be valid JSON"); err != nil {
		return nil, ErrChangesInvalidJSON
	}

	var normalizedActorID *string

	if actorID != nil {
		trimmedActorID := strings.TrimSpace(*actorID)
		if trimmedActorID != "" {
			if err := asserter.That(ctx, len(trimmedActorID) <= MaxActorIDLength, "actor id exceeds maximum length"); err != nil {
				return nil, ErrActorIDTooLong
			}

			normalizedActorID = &trimmedActorID
		}
	}

	changesCopy := append([]byte(nil), changes...)

	return &AuditLog{
		ID:         uuid.New(),
		TenantID:   tenantID,
		EntityType: trimmedEntityType,
		EntityID:   entityID,
		Action:     trimmedAction,
		ActorID:    normalizedActorID,
		Changes:    changesCopy,
		CreatedAt:  time.Now().UTC(),
	}, nil
}
