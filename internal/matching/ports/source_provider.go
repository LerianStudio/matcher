package ports

import (
	"context"

	"github.com/google/uuid"

	sharedfee "github.com/LerianStudio/matcher/internal/shared/domain/fee"
)

//go:generate mockgen -destination=mocks/source_provider_mock.go -package=mocks . SourceProvider

// SourceType defines the type of data source.
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
