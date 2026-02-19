//go:build unit

package fee

import (
	"context"
	"testing"

	"github.com/google/uuid"
	"github.com/shopspring/decimal"
)

func FuzzCalculateExpectedFee_NoPanic(f *testing.F) {
	f.Add(int64(100), int64(2), "0.015", "USD")
	f.Add(int64(0), int64(0), "0", "EUR")
	f.Add(int64(999999999), int64(8), "1", "BRL")
	f.Add(int64(1), int64(0), "0.5", "GBP")

	f.Fuzz(func(t *testing.T, amountInt, scale int64, rateStr, currency string) {
		ctx := context.Background()

		if scale < 0 || scale > 10 {
			scale = 2
		}

		if currency == "" {
			currency = "USD"
		}

		amount := decimal.New(amountInt, int32(-scale))
		if amount.IsNegative() {
			amount = amount.Abs()
		}

		rate, err := decimal.NewFromString(rateStr)
		if err != nil || rate.IsNegative() || rate.GreaterThan(decimal.NewFromInt(1)) {
			rate = decimal.RequireFromString("0.01")
		}

		tx := &TransactionForFee{
			Amount: Money{Amount: amount, Currency: currency},
		}

		rateEntity := &Rate{
			ID:        uuid.New(),
			Currency:  currency,
			Structure: PercentageFee{Rate: rate},
		}

		result, err := CalculateExpectedFee(ctx, tx, rateEntity)
		if err != nil {
			return
		}

		if result.Amount.IsNegative() {
			t.Errorf("fee should never be negative, got %s", result.Amount)
		}
	})
}

func FuzzCalculateExpectedFee_TieredNoPanic(f *testing.F) {
	f.Add(int64(100), int64(2))
	f.Add(int64(0), int64(0))
	f.Add(int64(1000000), int64(4))

	f.Fuzz(func(t *testing.T, amountInt, scale int64) {
		ctx := context.Background()

		if scale < 0 || scale > 10 {
			scale = 2
		}

		amount := decimal.New(amountInt, int32(-scale))
		if amount.IsNegative() {
			amount = amount.Abs()
		}

		upTo100 := decimal.NewFromInt(100)
		upTo1000 := decimal.NewFromInt(1000)

		tx := &TransactionForFee{
			Amount: Money{Amount: amount, Currency: "USD"},
		}

		rateEntity := &Rate{
			ID:       uuid.New(),
			Currency: "USD",
			Structure: TieredFee{Tiers: []Tier{
				{UpTo: &upTo100, Rate: decimal.RequireFromString("0.01")},
				{UpTo: &upTo1000, Rate: decimal.RequireFromString("0.02")},
				{UpTo: nil, Rate: decimal.RequireFromString("0.03")},
			}},
		}

		result, err := CalculateExpectedFee(ctx, tx, rateEntity)
		if err != nil {
			return
		}

		if result.Amount.IsNegative() {
			t.Errorf("fee should never be negative, got %s", result.Amount)
		}
	})
}
