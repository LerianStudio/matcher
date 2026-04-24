// Copyright 2025 Lerian Studio. All rights reserved.
// Use of this source code is governed by an Elastic License 2.0
// that can be found in the LICENSE.md file.

// Package shared provides shared domain types used across bounded contexts.
package shared

import (
	"context"
	"encoding/json"
	"errors"
	"strings"
	"time"

	"github.com/google/uuid"

	"github.com/LerianStudio/lib-commons/v5/commons/assert"

	"github.com/LerianStudio/matcher/internal/shared/constants"
)

// Field length limits for audit log validation, matching database constraints.
const (
	MaxEntityTypeLength = 50
	MaxActionLength     = 50
	MaxActorIDLength    = 255
)

// Sentinel errors for audit log validation.
// These are defined in the shared kernel so that all bounded contexts that
// publish audit events can reference them without importing governance/domain/entities.
var (
	ErrAuditTenantIDRequired   = errors.New("tenant id is required")
	ErrAuditEntityTypeRequired = errors.New("entity type is required")
	ErrAuditEntityTypeTooLong  = errors.New("entity type exceeds maximum length")
	ErrAuditEntityIDRequired   = errors.New("entity id is required")
	ErrAuditActionRequired     = errors.New("action is required")
	ErrAuditActionTooLong      = errors.New("action exceeds maximum length")
	ErrAuditActorIDTooLong     = errors.New("actor id exceeds maximum length")
	ErrAuditChangesRequired    = errors.New("changes are required")
	ErrAuditChangesInvalidJSON = errors.New("changes must be valid JSON")
)

// AuditLogFilter defines optional filters for listing audit logs.
// This is the shared kernel filter type used by all bounded contexts that
// query audit logs without directly importing governance/domain/entities.
type AuditLogFilter struct {
	Actor      *string
	DateFrom   *time.Time
	DateTo     *time.Time
	Action     *string
	EntityType *string
}

// AuditLog represents an immutable, append-only audit record.
// This is the shared kernel representation for cross-context use.
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
		return nil, ErrAuditTenantIDRequired
	}

	trimmedEntityType := strings.TrimSpace(entityType)
	if err := asserter.NotEmpty(ctx, trimmedEntityType, "entity type is required"); err != nil {
		return nil, ErrAuditEntityTypeRequired
	}

	if err := asserter.That(ctx, len(trimmedEntityType) <= MaxEntityTypeLength, "entity type exceeds maximum length"); err != nil {
		return nil, ErrAuditEntityTypeTooLong
	}

	if err := asserter.That(ctx, entityID != uuid.Nil, "entity id is required"); err != nil {
		return nil, ErrAuditEntityIDRequired
	}

	trimmedAction := strings.TrimSpace(action)
	if err := asserter.NotEmpty(ctx, trimmedAction, "action is required"); err != nil {
		return nil, ErrAuditActionRequired
	}

	if err := asserter.That(ctx, len(trimmedAction) <= MaxActionLength, "action exceeds maximum length"); err != nil {
		return nil, ErrAuditActionTooLong
	}

	if err := asserter.That(ctx, len(changes) > 0, "changes are required"); err != nil {
		return nil, ErrAuditChangesRequired
	}

	if err := asserter.That(ctx, json.Valid(changes), "changes must be valid JSON"); err != nil {
		return nil, ErrAuditChangesInvalidJSON
	}

	var normalizedActorID *string

	if actorID != nil {
		trimmedActorID := strings.TrimSpace(*actorID)
		if trimmedActorID != "" {
			if err := asserter.That(ctx, len(trimmedActorID) <= MaxActorIDLength, "actor id exceeds maximum length"); err != nil {
				return nil, ErrAuditActorIDTooLong
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
