// Package shared contains domain types shared across multiple bounded contexts.
package shared

import (
	"time"

	"github.com/google/uuid"

	sharedfee "github.com/LerianStudio/matcher/internal/shared/domain/fee"
)

// FieldMap represents the mapping rules for a reconciliation source.
// This is a shared kernel type used by both Configuration and Ingestion contexts.
type FieldMap struct {
	ID        uuid.UUID
	ContextID uuid.UUID
	SourceID  uuid.UUID
	Mapping   map[string]any
	Version   int
	CreatedAt time.Time
	UpdatedAt time.Time
}

// ReconciliationSource represents a data source for reconciliation.
// This is a shared kernel type used by both Configuration and Ingestion contexts.
type ReconciliationSource struct {
	ID        uuid.UUID
	ContextID uuid.UUID
	Name      string
	Type      string
	Side      sharedfee.MatchingSide
	Config    map[string]any
	CreatedAt time.Time
	UpdatedAt time.Time
}
