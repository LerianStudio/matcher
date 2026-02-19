package http

import (
	"context"
	"errors"
	"fmt"

	"github.com/google/uuid"

	sharedhttp "github.com/LerianStudio/lib-uncommons/v2/uncommons/net/http"
)

// NewTenantOwnershipVerifier creates a new verifier with the given context provider.
func NewTenantOwnershipVerifier(provider contextProvider) sharedhttp.TenantOwnershipVerifier {
	return func(ctx context.Context, tenantID, contextID uuid.UUID) error {
		if provider == nil {
			return fmt.Errorf("%w: verifier not initialized", sharedhttp.ErrContextAccessDenied)
		}

		info, err := provider.FindByID(ctx, tenantID, contextID)
		if err != nil {
			if errors.Is(err, sharedhttp.ErrContextNotOwned) {
				return sharedhttp.ErrContextNotOwned
			}

			if errors.Is(err, sharedhttp.ErrContextNotFound) {
				return sharedhttp.ErrContextNotFound
			}

			return fmt.Errorf("%w: %w", sharedhttp.ErrContextLookupFailed, err)
		}

		if info == nil {
			return sharedhttp.ErrContextNotFound
		}

		if info.ID != contextID {
			return sharedhttp.ErrContextNotOwned
		}

		if !info.Active {
			return sharedhttp.ErrContextNotActive
		}

		return nil
	}
}
