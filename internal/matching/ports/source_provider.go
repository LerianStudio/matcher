package ports

import (
	"context"

	"github.com/google/uuid"

	sharedfee "github.com/LerianStudio/matcher/internal/shared/domain/fee"
)

//go:generate mockgen -destination=mocks/source_provider_mock.go -package=mocks . SourceProvider

// SourceType defines the type of data source as seen by the matching engine.
// These constants represent the matching engine's view of source types. The configuration
// context may use different source type values (e.g., BANK, GATEWAY, CUSTOM, FETCHER).
// The cross-context adapter maps configuration types to this matching-level representation.
// Matching treats SourceType as opaque metadata for observability and does not branch on it.
type SourceType string

// SourceType values.
const (
	SourceTypeLedger  SourceType = "LEDGER"
	SourceTypeFile    SourceType = "FILE"
	SourceTypeAPI     SourceType = "API"
	SourceTypeWebhook SourceType = "WEBHOOK"
)

// SourceInfo contains source information needed by matching.
type SourceInfo struct {
	ID   uuid.UUID
	Type SourceType
	Side sharedfee.MatchingSide
}

// SourceProvider provides reconciliation source information for matching.
// This abstracts the Configuration context's SourceRepository.
type SourceProvider interface {
	FindByContextID(ctx context.Context, contextID uuid.UUID) ([]*SourceInfo, error)
}
