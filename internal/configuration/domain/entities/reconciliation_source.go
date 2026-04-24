// Copyright 2025 Lerian Studio. All rights reserved.
// Use of this source code is governed by an Elastic License 2.0
// that can be found in the LICENSE.md file.

package entities

import (
	shared "github.com/LerianStudio/matcher/internal/shared/domain"
)

// ReconciliationSource and its associated input types live canonically in
// internal/shared/domain. This file re-exports them as aliases so that the
// many call sites that import configuration/domain/entities continue to
// compile without modification.
//
// See docs/handoffs/simplify/T-007-followup.md for the consolidation rationale
// and CLAUDE.md's "type-alias pattern" section for the documented convention.

// ReconciliationSource is a type alias for the canonical shared.ReconciliationSource.
type ReconciliationSource = shared.ReconciliationSource

// CreateReconciliationSourceInput is a type alias for the canonical input type.
type CreateReconciliationSourceInput = shared.CreateReconciliationSourceInput

// UpdateReconciliationSourceInput is a type alias for the canonical input type.
type UpdateReconciliationSourceInput = shared.UpdateReconciliationSourceInput

// CreateContextSourceInput is a type alias for the canonical input type.
type CreateContextSourceInput = shared.CreateContextSourceInput

// Reconciliation source sentinel errors — re-exported from the shared kernel.
var (
	ErrNilReconciliationSource = shared.ErrNilReconciliationSource
	ErrSourceNameRequired      = shared.ErrSourceNameRequired
	ErrSourceNameTooLong       = shared.ErrSourceNameTooLong
	ErrSourceTypeInvalid       = shared.ErrSourceTypeInvalid
	ErrSourceContextRequired   = shared.ErrSourceContextRequired
	ErrSourceSideRequired      = shared.ErrSourceSideRequired
	ErrSourceSideInvalid       = shared.ErrSourceSideInvalid
)

// NewReconciliationSource is re-exported from the shared kernel via a function value.
var NewReconciliationSource = shared.NewReconciliationSource
