package ports

import (
	"context"

	"github.com/google/uuid"
	"github.com/shopspring/decimal"
)

//go:generate mockgen -destination=mocks/matching_gateway_mock.go -package=mocks . MatchingGateway

// ForceMatchInput contains parameters for creating a force match.
type ForceMatchInput struct {
	ExceptionID    uuid.UUID
	TransactionID  uuid.UUID
	Notes          string
	OverrideReason string
	Actor          string
}

// CreateAdjustmentInput contains parameters for creating an adjustment.
type CreateAdjustmentInput struct {
	ExceptionID   uuid.UUID
	TransactionID uuid.UUID
	Direction     string
	Amount        decimal.Decimal
	Currency      string
	Reason        string
	Notes         string
	Actor         string
}

// MatchingGateway abstracts interactions with the matching context for exception resolution.
// This is an output port for the exception context to coordinate cross-context operations.
type MatchingGateway interface {
	// CreateForceMatch creates a force match record and marks the transaction as matched.
	// It creates a manual match group linking the transaction with override metadata.
	CreateForceMatch(ctx context.Context, input ForceMatchInput) error

	// CreateAdjustment creates an adjustment record for a transaction.
	// The adjustment compensates for discrepancies found during reconciliation.
	CreateAdjustment(ctx context.Context, input CreateAdjustmentInput) error
}
