package fee

import (
	"context"
	"fmt"
)

// TransactionForFee holds transaction data needed for fee calculation and verification.
type TransactionForFee struct {
	Amount    Money
	ActualFee *Money
}

// CalculateExpectedFee computes the expected fee for a transaction using the given rate structure.
func CalculateExpectedFee(ctx context.Context, tx *TransactionForFee, rate *Rate) (Money, error) {
	if tx == nil {
		return Money{}, ErrNilTransaction
	}

	if rate == nil {
		return Money{}, ErrNilRate
	}

	if tx.Amount.Currency != rate.Currency {
		return Money{}, fmt.Errorf(
			"%w: tx=%s rate=%s",
			ErrCurrencyMismatch,
			tx.Amount.Currency,
			rate.Currency,
		)
	}

	base := tx.Amount.Amount.Abs()

	feeAmount, err := rate.Structure.Calculate(ctx, base)
	if err != nil {
		return Money{}, fmt.Errorf("calculate fee structure: %w", err)
	}

	return Money{Amount: feeAmount, Currency: rate.Currency}, nil
}
