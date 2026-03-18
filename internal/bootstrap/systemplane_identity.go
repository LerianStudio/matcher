// Copyright 2025 Lerian Studio. All rights reserved.
// Use of this source code is governed by an Elastic License 2.0
// that can be found in the LICENSE.md file.

package bootstrap

import (
	"context"

	"github.com/LerianStudio/matcher/internal/auth"
	"github.com/LerianStudio/matcher/pkg/systemplane/domain"
	"github.com/LerianStudio/matcher/pkg/systemplane/ports"
)

// Compile-time interface check.
var _ ports.IdentityResolver = (*MatcherIdentityResolver)(nil)

// MatcherIdentityResolver bridges the systemplane IdentityResolver port
// to Matcher's JWT-based authentication context. It reads user and tenant
// identifiers that the auth middleware has already placed into the context.
type MatcherIdentityResolver struct{}

// Actor extracts the current actor from the request context by reading the
// user ID set by the tenant-extraction middleware. When no user identity is
// available (auth disabled, single-tenant mode, or no JWT), it returns an
// anonymous actor so that audit logging always has a non-empty actor ID.
func (r *MatcherIdentityResolver) Actor(ctx context.Context) (domain.Actor, error) {
	userID := auth.GetUserID(ctx)
	if userID == "" {
		return domain.Actor{ID: "anonymous"}, nil
	}

	return domain.Actor{ID: userID}, nil
}

// TenantID extracts the tenant identifier from the request context by
// delegating to auth.GetTenantID, which returns the default tenant ID for
// single-tenant deployments or when auth is disabled.
func (r *MatcherIdentityResolver) TenantID(ctx context.Context) (string, error) {
	return auth.GetTenantID(ctx), nil
}
