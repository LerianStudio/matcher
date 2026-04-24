// Copyright 2025 Lerian Studio. All rights reserved.
// Use of this source code is governed by an Elastic License 2.0
// that can be found in the LICENSE.md file.

// Package http provides HTTP handlers for configuration management.
package http

import (
	"context"
	"database/sql"
	"errors"
	"fmt"

	"github.com/google/uuid"

	sharedhttp "github.com/LerianStudio/lib-commons/v5/commons/net/http"

	"github.com/LerianStudio/matcher/internal/configuration/domain/repositories"
)

// NewTenantOwnershipVerifier creates a new verifier using the context repository directly.
//
// SECURITY AUDIT NOTE (Taura Security, 2026-03):
// This verifier intentionally does NOT check context active status (unlike the matching,
// ingestion, and reporting verifiers which DO enforce ErrContextNotActive for paused contexts).
//
// Rationale: configuration endpoints must remain accessible regardless of context status so
// that administrators can re-activate PAUSED contexts, read/update configuration on any
// context, and delete contexts in any non-archived state. If the configuration verifier
// blocked paused contexts, a PAUSED context would become permanently irrecoverable because
// the PATCH endpoint (used to set status=ACTIVE) would itself be blocked.
//
// The matching, ingestion, and reporting verifiers correctly enforce the active-status
// gate because those are operational endpoints that should only process data when the
// context is actively running.
//
// Domain state machine (see reconciliation_context.go):
//
//	DRAFT   -> ACTIVE                   (Activate)
//	ACTIVE  -> PAUSED                   (Pause)
//	ACTIVE  -> ARCHIVED                 (Archive)
//	PAUSED  -> ACTIVE                   (Activate)  <-- this is the recovery path
//	PAUSED  -> ARCHIVED                 (Archive)
//	ARCHIVED -> (terminal, no transitions out)
func NewTenantOwnershipVerifier(contextRepo repositories.ContextRepository) sharedhttp.TenantOwnershipVerifier {
	return func(ctx context.Context, tenantID, contextID uuid.UUID) error {
		if contextRepo == nil {
			return fmt.Errorf("%w: verifier not initialized", sharedhttp.ErrContextAccessDenied)
		}

		reconciliationCtx, err := contextRepo.FindByID(ctx, contextID)
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

		// NOTE: No active-status check here. See the security audit note above.
		// Configuration endpoints must remain accessible for all non-archived states
		// to allow recovery from PAUSED status.

		return nil
	}
}
