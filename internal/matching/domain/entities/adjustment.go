// Package entities provides matching domain entities.
package entities

import (
	"context"
	"fmt"
	"time"

	"github.com/google/uuid"
	"github.com/shopspring/decimal"

	"github.com/LerianStudio/lib-commons/v5/commons/assert"

	"github.com/LerianStudio/matcher/internal/shared/constants"
)

// AdjustmentType represents the category of an adjustment.
type AdjustmentType string

// Adjustment type constants.
const (
	AdjustmentTypeBankFee       AdjustmentType = "BANK_FEE"
	AdjustmentTypeFXDifference  AdjustmentType = "FX_DIFFERENCE"
	AdjustmentTypeRounding      AdjustmentType = "ROUNDING"
	AdjustmentTypeWriteOff      AdjustmentType = "WRITE_OFF"
	AdjustmentTypeMiscellaneous AdjustmentType = "MISCELLANEOUS"
)

// AdjustmentDirection represents the journal entry direction of an adjustment.
type AdjustmentDirection string

// Adjustment direction constants.
const (
	AdjustmentDirectionDebit  AdjustmentDirection = "DEBIT"
	AdjustmentDirectionCredit AdjustmentDirection = "CREDIT"
)

// IsValid returns true if the direction is valid.
func (d AdjustmentDirection) IsValid() bool {
	switch d {
	case AdjustmentDirectionDebit, AdjustmentDirectionCredit:
		return true
	}

	return false
}

func (d AdjustmentDirection) String() string {
	return string(d)
}

// IsValid returns true if the adjustment type is known.
func (t AdjustmentType) IsValid() bool {
	switch t {
	case AdjustmentTypeBankFee,
		AdjustmentTypeFXDifference,
		AdjustmentTypeRounding,
		AdjustmentTypeWriteOff,
		AdjustmentTypeMiscellaneous:
		return true
	}

	return false
}

func (t AdjustmentType) String() string {
	return string(t)
}

// Adjustment represents a balancing journal entry to resolve variance between matched transactions.
type Adjustment struct {
	ID            uuid.UUID
	ContextID     uuid.UUID
	MatchGroupID  *uuid.UUID
	TransactionID *uuid.UUID
	Type          AdjustmentType
	Direction     AdjustmentDirection
	Amount        decimal.Decimal
	Currency      string
	Description   string
	Reason        string
	CreatedBy     string
	CreatedAt     time.Time
	UpdatedAt     time.Time
}

// SignedAmount returns the amount with sign based on direction.
// DEBIT returns positive, CREDIT returns negative.
func (a *Adjustment) SignedAmount() decimal.Decimal {
	if a.Direction == AdjustmentDirectionCredit {
		return a.Amount.Neg()
	}

	return a.Amount
}

// NewAdjustment creates a validated Adjustment entity.
// Amount must be positive (non-zero, non-negative). Direction indicates debit or credit.
func NewAdjustment(
	ctx context.Context,
	contextID uuid.UUID,
	matchGroupID *uuid.UUID,
	transactionID *uuid.UUID,
	adjustmentType AdjustmentType,
	direction AdjustmentDirection,
	amount decimal.Decimal,
	currency string,
	description string,
	reason string,
	createdBy string,
) (*Adjustment, error) {
	asserter := assert.New(ctx, nil, constants.ApplicationName, "matching.adjustment.new")

	if err := asserter.That(ctx, contextID != uuid.Nil, "context id is required"); err != nil {
		return nil, fmt.Errorf("adjustment context id: %w", err)
	}

	if err := asserter.That(ctx, matchGroupID != nil || transactionID != nil, "match group id or transaction id is required"); err != nil {
		return nil, fmt.Errorf("adjustment target: %w", err)
	}

	if err := asserter.That(ctx, adjustmentType.IsValid(), "invalid adjustment type", "type", string(adjustmentType)); err != nil {
		return nil, fmt.Errorf("adjustment type: %w", err)
	}

	if err := asserter.That(ctx, direction.IsValid(), "invalid direction", "direction", string(direction)); err != nil {
		return nil, fmt.Errorf("adjustment direction: %w", err)
	}

	if err := asserter.That(ctx, amount.IsPositive(), "amount must be positive"); err != nil {
		return nil, fmt.Errorf("adjustment amount: %w", err)
	}

	if err := asserter.NotEmpty(ctx, currency, "currency is required"); err != nil {
		return nil, fmt.Errorf("adjustment currency: %w", err)
	}

	if err := asserter.NotEmpty(ctx, description, "description is required"); err != nil {
		return nil, fmt.Errorf("adjustment description: %w", err)
	}

	if err := asserter.NotEmpty(ctx, reason, "reason is required"); err != nil {
		return nil, fmt.Errorf("adjustment reason: %w", err)
	}

	if err := asserter.NotEmpty(ctx, createdBy, "created by is required"); err != nil {
		return nil, fmt.Errorf("adjustment created by: %w", err)
	}

	now := time.Now().UTC()

	return &Adjustment{
		ID:            uuid.New(),
		ContextID:     contextID,
		MatchGroupID:  matchGroupID,
		TransactionID: transactionID,
		Type:          adjustmentType,
		Direction:     direction,
		Amount:        amount,
		Currency:      currency,
		Description:   description,
		Reason:        reason,
		CreatedBy:     createdBy,
		CreatedAt:     now,
		UpdatedAt:     now,
	}, nil
}
