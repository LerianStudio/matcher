// Copyright 2025 Lerian Studio. All rights reserved.
// Use of this source code is governed by an Elastic License 2.0
// that can be found in the LICENSE.md file.

// Package ports defines inbound and outbound interfaces for the matching context.
package ports

import (
	"context"

	"github.com/google/uuid"
	"github.com/shopspring/decimal"

	shared "github.com/LerianStudio/matcher/internal/shared/domain"
)

//go:generate mockgen -destination=mocks/context_provider_mock.go -package=mocks . ContextProvider

// ReconciliationContextInfo contains the context information needed by matching.
type ReconciliationContextInfo struct {
	ID               uuid.UUID
	Type             shared.ContextType
	Active           bool
	FeeToleranceAbs  decimal.Decimal
	FeeTolerancePct  decimal.Decimal
	FeeNormalization *string
}

// ContextProvider provides reconciliation context information for matching.
// This abstracts the Configuration context's ContextRepository.
type ContextProvider interface {
	FindByID(ctx context.Context, tenantID, contextID uuid.UUID) (*ReconciliationContextInfo, error)
}
