package entities

import (
	"context"
	"fmt"
	"time"

	"github.com/google/uuid"
	"github.com/shopspring/decimal"

	"github.com/LerianStudio/lib-commons/v4/commons/assert"

	"github.com/LerianStudio/matcher/internal/shared/constants"
)

// MatchItem represents an allocation of a transaction within a match group.
type MatchItem struct {
	ID                uuid.UUID
	MatchGroupID      uuid.UUID
	TransactionID     uuid.UUID
	AllocatedAmount   decimal.Decimal
	AllocatedCurrency string
	ExpectedAmount    decimal.Decimal
	AllowPartial      bool
	CreatedAt         time.Time
	UpdatedAt         time.Time
}

// NewMatchItem creates a new MatchItem that requires full allocation.
func NewMatchItem(
	ctx context.Context,
	transactionID uuid.UUID,
	allocatedAmount decimal.Decimal,
	currency string,
	expectedAmount decimal.Decimal,
) (*MatchItem, error) {
	return newMatchItem(ctx, transactionID, allocatedAmount, currency, expectedAmount, false)
}

// NewMatchItemWithPolicy creates a new MatchItem with a configurable partial allocation policy.
func NewMatchItemWithPolicy(
	ctx context.Context,
	transactionID uuid.UUID,
	allocatedAmount decimal.Decimal,
	currency string,
	expectedAmount decimal.Decimal,
	allowPartial bool,
) (*MatchItem, error) {
	return newMatchItem(ctx, transactionID, allocatedAmount, currency, expectedAmount, allowPartial)
}

func newMatchItem(
	ctx context.Context,
	transactionID uuid.UUID,
	allocatedAmount decimal.Decimal,
	currency string,
	expectedAmount decimal.Decimal,
	allowPartial bool,
) (*MatchItem, error) {
	asserter := assert.New(ctx, nil, constants.ApplicationName, "matching.match_item.new")

	if err := asserter.That(ctx, transactionID != uuid.Nil, "transaction id is required"); err != nil {
		return nil, fmt.Errorf("match item transaction id: %w", err)
	}

	if err := asserter.That(ctx, !allocatedAmount.IsNegative(), "allocated amount must be non-negative", "allocated_amount", allocatedAmount.String()); err != nil {
		return nil, fmt.Errorf("match item allocated amount: %w", err)
	}

	if err := asserter.That(ctx, !expectedAmount.IsNegative(), "expected amount must be non-negative", "expected_amount", expectedAmount.String()); err != nil {
		return nil, fmt.Errorf("match item expected amount: %w", err)
	}

	if err := asserter.That(ctx, !allocatedAmount.GreaterThan(expectedAmount), "allocated amount cannot exceed expected amount", "allocated_amount", allocatedAmount.String(), "expected_amount", expectedAmount.String()); err != nil {
		return nil, fmt.Errorf("match item allocation bounds: %w", err)
	}

	if !allowPartial {
		if err := asserter.That(ctx, allocatedAmount.Equal(expectedAmount), "allocated amount must equal expected amount", "allocated_amount", allocatedAmount.String(), "expected_amount", expectedAmount.String()); err != nil {
			return nil, fmt.Errorf("match item full allocation: %w", err)
		}
	}

	if err := asserter.NotEmpty(ctx, currency, "allocated currency is required"); err != nil {
		return nil, fmt.Errorf("match item currency: %w", err)
	}

	now := time.Now().UTC()

	return &MatchItem{
		ID:                uuid.New(),
		TransactionID:     transactionID,
		AllocatedAmount:   allocatedAmount,
		AllocatedCurrency: currency,
		ExpectedAmount:    expectedAmount,
		AllowPartial:      allowPartial,
		CreatedAt:         now,
		UpdatedAt:         now,
	}, nil
}

// ApplyAllocation updates the allocated amount on this match item.
func (item *MatchItem) ApplyAllocation(
	ctx context.Context,
	amount decimal.Decimal,
	requireFullCoverage bool,
) error {
	asserter := assert.New(
		ctx,
		nil,
		constants.ApplicationName,
		"matching.match_item.apply_allocation",
	)

	if err := asserter.NotNil(ctx, item, "match item is required"); err != nil {
		return fmt.Errorf("match item required: %w", err)
	}

	if err := asserter.That(ctx, !amount.IsNegative(), "allocated amount must be non-negative", "allocated_amount", amount.String()); err != nil {
		return fmt.Errorf("match item allocation amount: %w", err)
	}

	requireFull := requireFullCoverage || !item.AllowPartial
	if requireFull {
		if err := asserter.That(ctx, amount.Equal(item.ExpectedAmount), "allocated amount must equal expected amount", "allocated_amount", amount.String(), "expected_amount", item.ExpectedAmount.String()); err != nil {
			return fmt.Errorf("match item full allocation: %w", err)
		}
	} else {
		if err := asserter.That(ctx, !amount.GreaterThan(item.ExpectedAmount), "allocated amount cannot exceed expected amount", "allocated_amount", amount.String(), "expected_amount", item.ExpectedAmount.String()); err != nil {
			return fmt.Errorf("match item allocation bounds: %w", err)
		}
	}

	item.AllocatedAmount = amount
	item.UpdatedAt = time.Now().UTC()

	return nil
}
