// Copyright 2025 Lerian Studio.

package ports

import (
	"context"

	"github.com/LerianStudio/matcher/pkg/systemplane/domain"
)

// IdentityResolver extracts identity information from the request context.
// Implementations typically read JWT claims, API-key metadata, or service
// account credentials embedded in the context by authentication middleware.
type IdentityResolver interface {
	// Actor returns the actor making the current request.
	Actor(ctx context.Context) (domain.Actor, error)

	// TenantID returns the tenant identifier from the current request context.
	// For single-tenant deployments this may return a fixed default value.
	TenantID(ctx context.Context) (string, error)
}
