// Package http provides HTTP handlers for configuration management.
package http

import (
	"context"
	"database/sql"
	"errors"
	"fmt"

	"github.com/google/uuid"

	sharedhttp "github.com/LerianStudio/lib-uncommons/v2/uncommons/net/http"

	"github.com/LerianStudio/matcher/internal/configuration/services/query"
)

// NewTenantOwnershipVerifier creates a new verifier using the configuration query use case.
func NewTenantOwnershipVerifier(queryUseCase *query.UseCase) sharedhttp.TenantOwnershipVerifier {
	return func(ctx context.Context, tenantID, contextID uuid.UUID) error {
		if queryUseCase == nil {
			return fmt.Errorf("%w: verifier not initialized", sharedhttp.ErrContextAccessDenied)
		}

		reconciliationCtx, err := queryUseCase.GetContext(ctx, contextID)
		if err != nil {
			if errors.Is(err, sql.ErrNoRows) {
				return sharedhttp.ErrContextNotFound
			}

			return fmt.Errorf("failed to verify context ownership: %w", err)
		}

		if reconciliationCtx == nil {
			return sharedhttp.ErrContextNotFound
		}

		if reconciliationCtx.TenantID != tenantID {
			return sharedhttp.ErrContextNotOwned
		}

		return nil
	}
}
