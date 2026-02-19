// Package http provides HTTP handlers for ingestion operations.
package http

import (
	"context"
	"fmt"

	"github.com/google/uuid"

	sharedhttp "github.com/LerianStudio/lib-uncommons/v2/uncommons/net/http"
)

// NewTenantOwnershipVerifier creates a new verifier using the context provider.
func NewTenantOwnershipVerifier(ctxProvider contextProvider) sharedhttp.TenantOwnershipVerifier {
	return func(ctx context.Context, tenantID, contextID uuid.UUID) error {
		if ctxProvider == nil {
			return fmt.Errorf("ingestion context verifier not initialized: %w", sharedhttp.ErrContextAccessDenied)
		}

		ctxInfo, err := ctxProvider.FindByID(ctx, tenantID, contextID)
		if err != nil {
			// Return the infrastructure error unwrapped so the classifier
			// maps it to ErrContextLookupFailed (500) instead of access-denied (403).
			return fmt.Errorf("verify context ownership: %w", err)
		}

		if ctxInfo == nil {
			return sharedhttp.ErrContextNotFound
		}

		// The contextProvider.FindByID already filters by tenantID.
		// If we get a result, the context belongs to the tenant.
		// Additional ID check for defense in depth.
		if ctxInfo.ID != contextID {
			return sharedhttp.ErrContextNotOwned
		}

		// Verify the context is active before allowing operations.
		if !ctxInfo.Active {
			return fmt.Errorf("%w: context is paused", sharedhttp.ErrContextNotActive)
		}

		return nil
	}
}
