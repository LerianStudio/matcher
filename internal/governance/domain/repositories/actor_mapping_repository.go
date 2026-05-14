// Copyright 2025 Lerian Studio. All rights reserved.
// Use of this source code is governed by an Elastic License 2.0
// that can be found in the LICENSE.md file.

// Package repositories provides governance persistence contracts.
package repositories

//go:generate mockgen -source=actor_mapping_repository.go -destination=mocks/actor_mapping_repository_mock.go -package=mocks

import (
	"context"
	"database/sql"

	"github.com/LerianStudio/matcher/internal/governance/domain/entities"
)

// ActorMappingRepository defines persistence operations for actor mappings.
// Identity fields (display_name, email) are append-only after first creation;
// the row may be redacted via PseudonymizeWithTx or removed via Delete to
// support GDPR right-to-erasure, but never mutated in place. AuditLog history
// is fully immutable; this repository sits one rung up that ladder.
type ActorMappingRepository interface {
	// Upsert creates a new mapping or returns the existing one. If a row for
	// actor_id already exists and the payload's display_name/email match the
	// stored identity fields exactly, the existing entity is returned
	// (idempotent success). If they differ — including when the stored row
	// has been pseudonymized to [REDACTED] — Upsert returns
	// ErrActorMappingImmutable and leaves the stored row untouched. This
	// guards against the pseudonymization-bypass attack vector where an
	// attacker could overwrite a redacted row by re-PUT-ing the actor_id.
	// Returns the canonical persisted entity, including generated fields, so
	// callers can continue without an additional read.
	Upsert(ctx context.Context, mapping *entities.ActorMapping) (*entities.ActorMapping, error)

	// GetByActorID retrieves an actor mapping by its actor ID.
	// Returns ErrActorMappingNotFound if no mapping exists.
	GetByActorID(ctx context.Context, actorID string) (*entities.ActorMapping, error)

	// PseudonymizeWithTx replaces PII fields (display_name, email) with
	// [REDACTED] using the caller-owned transaction. Production paths atomically
	// couple this mutation with a streaming emit; there is no non-transactional
	// variant by design.
	PseudonymizeWithTx(ctx context.Context, tx *sql.Tx, actorID string) error

	// Delete removes the actor mapping entirely (right-to-erasure).
	Delete(ctx context.Context, actorID string) error
}
