// Copyright 2025 Lerian Studio. All rights reserved.
// Use of this source code is governed by an Elastic License 2.0
// that can be found in the LICENSE.md file.

package auth

import (
	"strings"

	"github.com/gofiber/fiber/v2"

	authMiddleware "github.com/LerianStudio/lib-auth/v3/auth/middleware"
)

// BuildProtectedAuthChain constructs the ordered list of Fiber middleware that
// guards a protected route's auth surface. The returned slice contains, in
// order:
//
//  1. validateTenantClaims — only when auth is enabled AND a non-nil auth
//     client is provided. Performs local JWT signature/tenancy validation so
//     invalid or missing tokens are rejected BEFORE any RBAC call hits
//     lib-auth's authorizer backend.
//  2. Authorize(resource, action) — one per action in the provided list. Each
//     Authorize handler short-circuits on denial with a 401/403 before later
//     middleware (tenant extraction, rate limiters, idempotency, handlers)
//     gets a chance to run. Running every action explicitly (instead of
//     collapsing to "any") preserves the strict ACL semantics the
//     bounded-context registrations rely on.
//  3. ExtractTenant — populates tenant/user context from the validated JWT
//     (or from configured defaults when auth is disabled). Runs AFTER the
//     Authorize handlers so a forged token cannot ride on a tenant context
//     populated before authorization — and so the downstream shared chain
//     (tenantDB, idempotency, rate limit) always sees a tenant id that the
//     authorizer explicitly approved.
//
// The function returns the sentinel errors ErrNilTenantExtractor, ErrNoActions,
// and ErrEmptyAction on invalid input so misconfiguration is caught at startup
// instead of surfacing as a confusing 500 at request time.
//
// This is the canonical chain builder used by bootstrap.protectedRouter to
// compose the per-route middleware stack. It replaces the previous
// router.Group("/", handlers...) pattern that caused every Protected(...)
// invocation to install its chain as app-global USE entries.
func BuildProtectedAuthChain(
	authClient *authMiddleware.AuthClient,
	extractor *TenantExtractor,
	resource string,
	actions []string,
) ([]fiber.Handler, error) {
	if extractor == nil {
		return nil, ErrNilTenantExtractor
	}

	if len(actions) == 0 {
		return nil, ErrNoActions
	}

	for _, action := range actions {
		if strings.TrimSpace(action) == "" {
			return nil, ErrEmptyAction
		}
	}

	// Capacity: optional validateTenantClaims (1) + one per action + ExtractTenant (1).
	// Using len(actions)+fixedChainOverhead is always safe: when validateTenantClaims
	// is skipped the slice simply has one less element than the reserved capacity.
	const fixedChainOverhead = 2 // validateTenantClaims + ExtractTenant

	handlers := make([]fiber.Handler, 0, len(actions)+fixedChainOverhead)

	if authClient != nil && extractor.authEnabled {
		handlers = append(handlers, extractor.validateTenantClaims())
	}

	for _, action := range actions {
		handlers = append(handlers, Authorize(authClient, resource, action))
	}

	handlers = append(handlers, extractor.ExtractTenant())

	return handlers, nil
}
