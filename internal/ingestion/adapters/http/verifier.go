// Copyright 2025 Lerian Studio. All rights reserved.
// Use of this source code is governed by an Elastic License 2.0
// that can be found in the LICENSE.md file.

// Package http provides HTTP handlers for ingestion operations.
package http

import (
	"context"
	"fmt"

	"github.com/google/uuid"

	sharedhttp "github.com/LerianStudio/lib-commons/v5/commons/net/http"
)

// NewTenantOwnershipVerifier creates a new verifier using the context provider.
//
// Tenant isolation is enforced inside the provider: the backing
// configuration ContextRepository's FindByID method pulls tenant_id from
// the ambient request context and issues SELECT ... WHERE tenant_id = $1
// AND id = $2. A request for a contextID minted under a different tenant
// therefore returns sql.ErrNoRows → ContextAccessInfo{nil} → 404 here,
// before the handler runs.
//
// The verifier adds one check beyond what FindByID returns: the context
// must be Active. That prevents ingestion endpoints from running against
// paused contexts.
func NewTenantOwnershipVerifier(ctxProvider contextProvider) sharedhttp.TenantOwnershipVerifier {
	return func(ctx context.Context, _, contextID uuid.UUID) error {
		if ctxProvider == nil {
			return fmt.Errorf("ingestion context verifier not initialized: %w", sharedhttp.ErrContextAccessDenied)
		}

		ctxInfo, err := ctxProvider.FindByID(ctx, contextID)
		if err != nil {
			// Return the infrastructure error unwrapped so the classifier
			// maps it to ErrContextLookupFailed (500) instead of access-denied (403).
			return fmt.Errorf("verify context ownership: %w", err)
		}

		if ctxInfo == nil {
			return sharedhttp.ErrContextNotFound
		}

		if !ctxInfo.Active {
			return fmt.Errorf("%w: context is paused", sharedhttp.ErrContextNotActive)
		}

		return nil
	}
}
