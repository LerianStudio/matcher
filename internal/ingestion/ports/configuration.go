// Copyright 2025 Lerian Studio. All rights reserved.
// Use of this source code is governed by an Elastic License 2.0
// that can be found in the LICENSE.md file.

// Package ports defines inbound and outbound interfaces for the ingestion context.
package ports

import (
	"context"

	"github.com/google/uuid"

	shared "github.com/LerianStudio/matcher/internal/shared/domain"
)

// FieldMapRepository defines the interface for accessing field map data.
// This is a port interface that allows ingestion to access configuration data
// without directly depending on the configuration context.
type FieldMapRepository interface {
	// FindBySourceID retrieves a field map by its associated source ID.
	FindBySourceID(ctx context.Context, sourceID uuid.UUID) (*shared.FieldMap, error)
}

// SourceRepository defines the interface for accessing reconciliation source data.
// This is a port interface that allows ingestion to access configuration data
// without directly depending on the configuration context.
type SourceRepository interface {
	// FindByID retrieves a reconciliation source by context ID and source ID.
	FindByID(ctx context.Context, contextID, id uuid.UUID) (*shared.ReconciliationSource, error)
}
