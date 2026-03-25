package ports

import (
	"context"

	"github.com/google/uuid"
)

// ContextAccessInfo contains the minimal reconciliation-context state needed by
// HTTP ownership verifiers in other bounded contexts.
type ContextAccessInfo struct {
	ID     uuid.UUID
	Active bool
}

// ContextAccessProvider provides reconciliation-context access checks for HTTP
// ownership verification without leaking configuration entities into other
// contexts. Tenant scoping is expected to come from the ambient request context
// and repository-layer isolation.
type ContextAccessProvider interface {
	FindByID(ctx context.Context, contextID uuid.UUID) (*ContextAccessInfo, error)
}
