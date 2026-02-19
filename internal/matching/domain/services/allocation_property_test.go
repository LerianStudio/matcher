//go:build unit

package services

import (
	"math/rand"
	"testing"
	"testing/quick"
	"time"

	"github.com/google/uuid"
	"github.com/shopspring/decimal"
	"github.com/stretchr/testify/require"
)

func TestProperty_Allocation_TotalNeverExceedsTarget(t *testing.T) {
	t.Parallel()

	cfg := quick.Config{
		MaxCount: 100,
		Rand:     rand.New(rand.NewSource(42)),
	}

	property := func(targetAmount uint16, candidateCount uint8) bool {
		if candidateCount == 0 || candidateCount > 20 || targetAmount == 0 {
			return true
		}

		target := CandidateTransaction{
			ID:             uuid.New(),
			Amount:         decimal.NewFromInt(int64(targetAmount)),
			OriginalAmount: decimal.NewFromInt(int64(targetAmount)),
			Currency:       "USD",
			Date:           time.Date(2026, 1, 1, 10, 0, 0, 0, time.UTC),
		}

		candidates := make([]CandidateTransaction, candidateCount)
		for idx := range candidates {
			candidates[idx] = CandidateTransaction{
				ID:             uuid.New(),
				Amount:         decimal.NewFromInt(int64((idx + 1) * 10)),
				OriginalAmount: decimal.NewFromInt(int64((idx + 1) * 10)),
				Currency:       "USD",
				Date:           time.Date(2026, 1, 1, 10, 0, 0, 0, time.UTC),
			}
		}

		allocCfg := &AllocationConfig{
			AllowPartial:   true,
			Direction:      AllocationDirectionLeftToRight,
			ToleranceMode:  AllocationToleranceAbsolute,
			ToleranceValue: decimal.Zero,
		}

		allocations, total, _, err := AllocateOneToMany(target, candidates, allocCfg)
		if err != nil {
			return true
		}

		if total.GreaterThan(target.Amount) {
			return false
		}

		var sum decimal.Decimal
		for _, alloc := range allocations {
			sum = sum.Add(alloc.AllocatedAmount)
		}

		return sum.Equal(total)
	}

	require.NoError(t, quick.Check(property, &cfg))
}

func TestProperty_Allocation_NoNegativeAmounts(t *testing.T) {
	t.Parallel()

	cfg := quick.Config{
		MaxCount: 100,
		Rand:     rand.New(rand.NewSource(123)),
	}

	property := func(targetAmount uint16, candidateAmounts []uint16) bool {
		if len(candidateAmounts) == 0 || len(candidateAmounts) > 20 || targetAmount == 0 {
			return true
		}

		target := CandidateTransaction{
			ID:             uuid.New(),
			Amount:         decimal.NewFromInt(int64(targetAmount)),
			OriginalAmount: decimal.NewFromInt(int64(targetAmount)),
			Currency:       "USD",
			Date:           time.Date(2026, 1, 1, 10, 0, 0, 0, time.UTC),
		}

		candidates := make([]CandidateTransaction, len(candidateAmounts))
		for idx, amt := range candidateAmounts {
			if amt == 0 {
				amt = 1
			}

			candidates[idx] = CandidateTransaction{
				ID:             uuid.New(),
				Amount:         decimal.NewFromInt(int64(amt)),
				OriginalAmount: decimal.NewFromInt(int64(amt)),
				Currency:       "USD",
				Date:           time.Date(2026, 1, 1, 10, 0, 0, 0, time.UTC),
			}
		}

		allocCfg := &AllocationConfig{
			AllowPartial:   true,
			Direction:      AllocationDirectionLeftToRight,
			ToleranceMode:  AllocationToleranceAbsolute,
			ToleranceValue: decimal.Zero,
		}

		allocations, _, _, err := AllocateOneToMany(target, candidates, allocCfg)
		if err != nil {
			return true
		}

		for _, alloc := range allocations {
			if alloc.AllocatedAmount.LessThan(decimal.Zero) {
				return false
			}
		}

		return true
	}

	require.NoError(t, quick.Check(property, &cfg))
}

func TestProperty_Allocation_CurrencyPreserved(t *testing.T) {
	t.Parallel()

	cfg := quick.Config{
		MaxCount: 50,
		Rand:     rand.New(rand.NewSource(456)),
	}

	currencies := []string{"USD", "EUR", "GBP", "BRL"}

	property := func(currencyIdx, candidateCount uint8) bool {
		if candidateCount == 0 || candidateCount > 10 {
			return true
		}

		currency := currencies[int(currencyIdx)%len(currencies)]

		target := CandidateTransaction{
			ID:             uuid.New(),
			Amount:         decimal.NewFromInt(1000),
			OriginalAmount: decimal.NewFromInt(1000),
			Currency:       currency,
			Date:           time.Date(2026, 1, 1, 10, 0, 0, 0, time.UTC),
		}

		candidates := make([]CandidateTransaction, candidateCount)
		for idx := range candidates {
			candidates[idx] = CandidateTransaction{
				ID:             uuid.New(),
				Amount:         decimal.NewFromInt(100),
				OriginalAmount: decimal.NewFromInt(100),
				Currency:       currency,
				Date:           time.Date(2026, 1, 1, 10, 0, 0, 0, time.UTC),
			}
		}

		allocCfg := &AllocationConfig{
			AllowPartial:   true,
			Direction:      AllocationDirectionLeftToRight,
			ToleranceMode:  AllocationToleranceAbsolute,
			ToleranceValue: decimal.Zero,
		}

		allocations, _, _, err := AllocateOneToMany(target, candidates, allocCfg)
		if err != nil {
			return true
		}

		for _, alloc := range allocations {
			if alloc.Currency != currency {
				return false
			}
		}

		return true
	}

	require.NoError(t, quick.Check(property, &cfg))
}

func TestProperty_Allocation_Determinism(t *testing.T) {
	t.Parallel()

	cfg := quick.Config{
		MaxCount: 50,
		Rand:     rand.New(rand.NewSource(789)),
	}

	property := func(seed int64, candidateCount uint8) bool {
		if candidateCount == 0 || candidateCount > 10 {
			return true
		}

		rng := rand.New(rand.NewSource(seed))

		targetAmt := int64(rng.Intn(1000) + 100)

		target := CandidateTransaction{
			ID:             uuid.New(),
			Amount:         decimal.NewFromInt(targetAmt),
			OriginalAmount: decimal.NewFromInt(targetAmt),
			Currency:       "USD",
			Date:           time.Date(2026, 1, 1, 10, 0, 0, 0, time.UTC),
		}

		candidates := make([]CandidateTransaction, candidateCount)
		for idx := range candidates {
			candidateAmt := int64(rng.Intn(100) + 10)

			candidates[idx] = CandidateTransaction{
				ID:             uuid.New(),
				Amount:         decimal.NewFromInt(candidateAmt),
				OriginalAmount: decimal.NewFromInt(candidateAmt),
				Currency:       "USD",
				Date:           time.Date(2026, 1, 1, 10, 0, 0, 0, time.UTC),
			}
		}

		allocCfg := &AllocationConfig{
			AllowPartial:   true,
			Direction:      AllocationDirectionLeftToRight,
			ToleranceMode:  AllocationToleranceAbsolute,
			ToleranceValue: decimal.Zero,
		}

		alloc1, total1, ok1, err1 := AllocateOneToMany(target, candidates, allocCfg)
		alloc2, total2, ok2, err2 := AllocateOneToMany(target, candidates, allocCfg)

		return verifyAllocationDeterminism(alloc1, total1, ok1, err1, alloc2, total2, ok2, err2)
	}

	require.NoError(t, quick.Check(property, &cfg))
}

func verifyAllocationDeterminism(
	alloc1 []Allocation, total1 decimal.Decimal, ok1 bool, err1 error,
	alloc2 []Allocation, total2 decimal.Decimal, ok2 bool, err2 error,
) bool {
	if (err1 == nil) != (err2 == nil) {
		return false
	}

	if ok1 != ok2 {
		return false
	}

	if !total1.Equal(total2) {
		return false
	}

	if len(alloc1) != len(alloc2) {
		return false
	}

	for idx := range alloc1 {
		if alloc1[idx].TransactionID != alloc2[idx].TransactionID {
			return false
		}

		if !alloc1[idx].AllocatedAmount.Equal(alloc2[idx].AllocatedAmount) {
			return false
		}
	}

	return true
}
