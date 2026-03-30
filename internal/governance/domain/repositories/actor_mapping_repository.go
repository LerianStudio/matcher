// Package repositories provides governance persistence contracts.
package repositories

//go:generate mockgen -source=actor_mapping_repository.go -destination=mocks/actor_mapping_repository_mock.go -package=mocks

import (
	"context"

	"github.com/LerianStudio/matcher/internal/governance/domain/entities"
)

// ActorMappingRepository defines persistence operations for actor mappings.
// This is a mutable repository (unlike AuditLogRepository) to support GDPR compliance.
type ActorMappingRepository interface {
	// Upsert creates or updates an actor mapping.
	// Uses INSERT ... ON CONFLICT (actor_id) DO UPDATE ... RETURNING for idempotency.
	// Returns the persisted entity (including DB-generated timestamps) so callers
	// do not need a separate read, avoiding read-replica lag issues.
	Upsert(ctx context.Context, mapping *entities.ActorMapping) (*entities.ActorMapping, error)

	// GetByActorID retrieves an actor mapping by its actor ID.
	// Returns ErrActorMappingNotFound if no mapping exists.
	GetByActorID(ctx context.Context, actorID string) (*entities.ActorMapping, error)

	// Pseudonymize replaces PII fields (display_name, email) with [REDACTED].
	Pseudonymize(ctx context.Context, actorID string) error

	// Delete removes the actor mapping entirely (right-to-erasure).
	Delete(ctx context.Context, actorID string) error
}
