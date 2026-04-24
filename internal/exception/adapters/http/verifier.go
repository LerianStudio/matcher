// Copyright 2025 Lerian Studio. All rights reserved.
// Use of this source code is governed by an Elastic License 2.0
// that can be found in the LICENSE.md file.

// Package http provides HTTP handlers for exception operations.
package http

import (
	"context"
	"fmt"

	"github.com/google/uuid"

	sharedhttp "github.com/LerianStudio/lib-commons/v5/commons/net/http"
)

// exceptionProvider defines the interface for checking exception existence within a tenant's schema.
type exceptionProvider interface {
	// ExistsForTenant checks if an exception with the given ID exists in the current tenant's schema.
	ExistsForTenant(ctx context.Context, id uuid.UUID) (bool, error)
}

// disputeProvider defines the interface for checking dispute existence within a tenant's schema.
type disputeProvider interface {
	// ExistsForTenant checks if a dispute with the given ID exists in the current tenant's schema.
	ExistsForTenant(ctx context.Context, id uuid.UUID) (bool, error)
}

// NewExceptionOwnershipVerifier creates a new verifier using the exception provider.
func NewExceptionOwnershipVerifier(
	provider exceptionProvider,
) sharedhttp.ResourceOwnershipVerifier {
	return func(ctx context.Context, exceptionID uuid.UUID) error {
		if provider == nil {
			return fmt.Errorf("exception ownership verifier not initialized: %w", ErrExceptionAccessDenied)
		}

		// The provider uses tenant-scoped transactions (schema isolation).
		// If the exception exists in the tenant's schema, ownership is verified.
		exists, err := provider.ExistsForTenant(ctx, exceptionID)
		if err != nil {
			// Return the infrastructure error unwrapped so the classifier
			// maps it to ErrLookupFailed (500) instead of access-denied (403).
			return fmt.Errorf("verify exception existence: %w", err)
		}

		if !exists {
			return ErrExceptionNotFound
		}

		return nil
	}
}

// NewDisputeOwnershipVerifier creates a new verifier using the dispute provider.
func NewDisputeOwnershipVerifier(provider disputeProvider) sharedhttp.ResourceOwnershipVerifier {
	return func(ctx context.Context, disputeID uuid.UUID) error {
		if provider == nil {
			return fmt.Errorf("dispute ownership verifier not initialized: %w", ErrDisputeAccessDenied)
		}

		// The provider uses tenant-scoped transactions (schema isolation).
		// If the dispute exists in the tenant's schema, ownership is verified.
		exists, err := provider.ExistsForTenant(ctx, disputeID)
		if err != nil {
			// Return the infrastructure error unwrapped so the classifier
			// maps it to ErrLookupFailed (500) instead of access-denied (403).
			return fmt.Errorf("verify dispute existence: %w", err)
		}

		if !exists {
			return ErrDisputeNotFound
		}

		return nil
	}
}
