// Package entities provides matching domain entities.
package entities

import (
	"context"
	"fmt"
	"time"

	"github.com/google/uuid"
	"github.com/shopspring/decimal"

	"github.com/LerianStudio/lib-uncommons/v2/uncommons/assert"

	"github.com/LerianStudio/matcher/internal/shared/constants"
)

// FeeVariance records the difference between expected and actual fees for a transaction.
type FeeVariance struct {
	ID            uuid.UUID
	ContextID     uuid.UUID
	RunID         uuid.UUID
	MatchGroupID  uuid.UUID
	TransactionID uuid.UUID
	RateID        uuid.UUID
	Currency      string
	ExpectedFee   decimal.Decimal
	ActualFee     decimal.Decimal
	Delta         decimal.Decimal
	ToleranceAbs  decimal.Decimal
	TolerancePct  decimal.Decimal
	VarianceType  string
	CreatedAt     time.Time
	UpdatedAt     time.Time
}

// NewFeeVariance creates a validated FeeVariance entity.
// Delta is computed internally as |expected - actual| to guarantee consistency.
func NewFeeVariance(
	ctx context.Context,
	contextID, runID, matchGroupID, transactionID, rateID uuid.UUID,
	currency string,
	expected, actual, tolAbs, tolPct decimal.Decimal,
	varianceType string,
) (*FeeVariance, error) {
	asserter := assert.New(ctx, nil, constants.ApplicationName, "matching.fee_variance.new")

	if err := asserter.That(ctx, contextID != uuid.Nil, "context id is required"); err != nil {
		return nil, fmt.Errorf("fee variance context id: %w", err)
	}

	if err := asserter.That(ctx, runID != uuid.Nil, "run id is required"); err != nil {
		return nil, fmt.Errorf("fee variance run id: %w", err)
	}

	if err := asserter.That(ctx, matchGroupID != uuid.Nil, "match group id is required"); err != nil {
		return nil, fmt.Errorf("fee variance match group id: %w", err)
	}

	if err := asserter.That(ctx, transactionID != uuid.Nil, "transaction id is required"); err != nil {
		return nil, fmt.Errorf("fee variance transaction id: %w", err)
	}

	if err := asserter.That(ctx, rateID != uuid.Nil, "rate id is required"); err != nil {
		return nil, fmt.Errorf("fee variance rate id: %w", err)
	}

	if err := asserter.NotEmpty(ctx, currency, "currency is required"); err != nil {
		return nil, fmt.Errorf("fee variance currency: %w", err)
	}

	if err := asserter.That(ctx, !expected.IsNegative() && !actual.IsNegative(), "fees must be non-negative"); err != nil {
		return nil, fmt.Errorf("fee variance amounts: %w", err)
	}

	if err := asserter.That(ctx, !tolAbs.IsNegative() && !tolPct.IsNegative(), "tolerances must be non-negative"); err != nil {
		return nil, fmt.Errorf("fee variance tolerances: %w", err)
	}

	if err := asserter.NotEmpty(ctx, varianceType, "variance type is required"); err != nil {
		return nil, fmt.Errorf("fee variance type: %w", err)
	}

	computedDelta := expected.Sub(actual).Abs()
	now := time.Now().UTC()

	return &FeeVariance{
		ID:            uuid.New(),
		ContextID:     contextID,
		RunID:         runID,
		MatchGroupID:  matchGroupID,
		TransactionID: transactionID,
		RateID:        rateID,
		Currency:      currency,
		ExpectedFee:   expected,
		ActualFee:     actual,
		Delta:         computedDelta,
		ToleranceAbs:  tolAbs,
		TolerancePct:  tolPct,
		VarianceType:  varianceType,
		CreatedAt:     now,
		UpdatedAt:     now,
	}, nil
}
