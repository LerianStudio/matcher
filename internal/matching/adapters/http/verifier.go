// Copyright 2025 Lerian Studio. All rights reserved.
// Use of this source code is governed by an Elastic License 2.0
// that can be found in the LICENSE.md file.

// Package http provides HTTP handlers for the matching domain.
package http

import (
	"context"
	"errors"
	"fmt"

	"github.com/google/uuid"

	sharedhttp "github.com/LerianStudio/lib-commons/v5/commons/net/http"

	"github.com/LerianStudio/matcher/internal/matching/ports"
)

// verifyContextOwnership encapsulates the shared verification logic used by both
// TenantOwnershipVerifier and ResourceOwnershipVerifier. It performs the FindByID
// call, nil check, ID mismatch detection, and active status validation.
func verifyContextOwnership(
	ctx context.Context,
	contextProvider ports.ContextProvider,
	tenantID, contextID uuid.UUID,
) error {
	ctxInfo, err := contextProvider.FindByID(ctx, tenantID, contextID)
	if err != nil {
		if errors.Is(err, sharedhttp.ErrContextNotFound) {
			return sharedhttp.ErrContextNotFound
		}

		// Return the infrastructure error unwrapped so the classifier
		// maps it to ErrContextLookupFailed (500) instead of access-denied (403).
		return fmt.Errorf("verify context ownership: %w", err)
	}

	if ctxInfo == nil {
		return fmt.Errorf("%w: context not found", sharedhttp.ErrContextNotFound)
	}

	// If we get a result, the context is reachable under the ambient tenant context.
	// Additional ID check for defense in depth.
	if ctxInfo.ID != contextID {
		return sharedhttp.ErrContextNotOwned
	}

	// Verify the context is active before allowing operations.
	if !ctxInfo.Active {
		return fmt.Errorf("%w: context is paused or disabled", sharedhttp.ErrContextNotActive)
	}

	return nil
}

// NewTenantOwnershipVerifier creates a new verifier using the context provider.
func NewTenantOwnershipVerifier(
	contextProvider ports.ContextProvider,
) sharedhttp.TenantOwnershipVerifier {
	return func(ctx context.Context, tenantID, contextID uuid.UUID) error {
		if contextProvider == nil {
			return fmt.Errorf("matching context verifier not initialized: %w", sharedhttp.ErrContextAccessDenied)
		}

		return verifyContextOwnership(ctx, contextProvider, tenantID, contextID)
	}
}

// NewResourceContextVerifier creates a ResourceOwnershipVerifier that verifies context
// ownership using tenant-scoped schema isolation. The tenantID is extracted from the
// request context (set by auth middleware) rather than passed as a parameter.
func NewResourceContextVerifier(
	contextProvider ports.ContextProvider,
	tenantExtractor func(ctx context.Context) string,
) sharedhttp.ResourceOwnershipVerifier {
	return func(ctx context.Context, contextID uuid.UUID) error {
		if contextProvider == nil {
			return fmt.Errorf("matching resource context verifier not initialized: %w", sharedhttp.ErrContextAccessDenied)
		}

		if tenantExtractor == nil {
			return fmt.Errorf("tenant extractor not initialized: %w", sharedhttp.ErrContextAccessDenied)
		}

		tenantIDStr := tenantExtractor(ctx)

		tenantID, err := uuid.Parse(tenantIDStr)
		if err != nil {
			return fmt.Errorf("%w: invalid tenant id", sharedhttp.ErrContextAccessDenied)
		}

		return verifyContextOwnership(ctx, contextProvider, tenantID, contextID)
	}
}
