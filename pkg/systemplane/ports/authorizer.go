// Copyright 2025 Lerian Studio.

// Package ports defines systemplane application boundary contracts.
package ports

import "context"

// Authorizer checks whether the current actor has a specific permission.
// Permissions are opaque strings whose format is defined by the host
// application (e.g., "systemplane:config:write", "systemplane:settings:read").
//
// Implementations typically delegate to an external authorization service or
// evaluate local RBAC rules extracted from the request context.
type Authorizer interface {
	// Authorize returns nil if the actor identified in ctx has the given
	// permission, or domain.ErrPermissionDenied otherwise.
	Authorize(ctx context.Context, permission string) error
}
