// Copyright 2025 Lerian Studio. All rights reserved.
// Use of this source code is governed by an Elastic License 2.0
// that can be found in the LICENSE.md file.

//go:build unit

package services

import (
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/shopspring/decimal"
	"github.com/stretchr/testify/require"
)

func TestAllocateOneToMany_FullCoverage(t *testing.T) {
	t.Parallel()

	cfg := &AllocationConfig{
		AllowPartial:   false,
		Direction:      AllocationDirectionLeftToRight,
		ToleranceMode:  AllocationToleranceAbsolute,
		ToleranceValue: decimal.Zero,
	}
	now := time.Date(2026, 1, 1, 10, 0, 0, 0, time.UTC)
	c40 := CandidateTransaction{
		ID:             uuid.New(),
		Amount:         decimal.RequireFromString("40"),
		OriginalAmount: decimal.RequireFromString("40"),
		Currency:       "USD",
		Date:           now,
	}
	c60 := CandidateTransaction{
		ID:             uuid.New(),
		Amount:         decimal.RequireFromString("60"),
		OriginalAmount: decimal.RequireFromString("60"),
		Currency:       "USD",
		Date:           now,
	}
	target := CandidateTransaction{
		ID:             uuid.New(),
		Amount:         decimal.RequireFromString("100"),
		OriginalAmount: decimal.RequireFromString("100"),
		Currency:       "USD",
		Date:           now,
	}
	candidates := []CandidateTransaction{c40, c60}

	allocations, total, ok, err := AllocateOneToMany(target, candidates, cfg)
	require.NoError(t, err)
	require.True(t, ok)
	require.Len(t, allocations, 2)
	require.Equal(t, c40.ID, allocations[0].TransactionID)
	require.True(t, allocations[0].AllocatedAmount.Equal(decimal.RequireFromString("40")))
	require.Equal(t, "USD", allocations[0].Currency)
	require.False(t, allocations[0].UseBaseAmount)
	require.Equal(t, c60.ID, allocations[1].TransactionID)
	require.True(t, allocations[1].AllocatedAmount.Equal(decimal.RequireFromString("60")))
	require.Equal(t, "USD", allocations[1].Currency)
	require.False(t, allocations[1].UseBaseAmount)
	require.True(t, total.Equal(decimal.RequireFromString("100")))
}

func TestAllocateOneToMany_DisallowPartial(t *testing.T) {
	t.Parallel()

	cfg := &AllocationConfig{
		AllowPartial:   false,
		Direction:      AllocationDirectionLeftToRight,
		ToleranceMode:  AllocationToleranceAbsolute,
		ToleranceValue: decimal.Zero,
	}
	now := time.Date(2026, 1, 1, 10, 0, 0, 0, time.UTC)
	c40 := CandidateTransaction{
		ID:             uuid.New(),
		Amount:         decimal.RequireFromString("40"),
		OriginalAmount: decimal.RequireFromString("40"),
		Currency:       "USD",
		Date:           now,
	}
	c80 := CandidateTransaction{
		ID:             uuid.New(),
		Amount:         decimal.RequireFromString("80"),
		OriginalAmount: decimal.RequireFromString("80"),
		Currency:       "USD",
		Date:           now,
	}
	target := CandidateTransaction{
		ID:             uuid.New(),
		Amount:         decimal.RequireFromString("100"),
		OriginalAmount: decimal.RequireFromString("100"),
		Currency:       "USD",
		Date:           now,
	}
	candidates := []CandidateTransaction{c40, c80}

	allocations, total, ok, err := AllocateOneToMany(target, candidates, cfg)
	require.NoError(t, err)
	require.False(t, ok)
	require.Len(t, allocations, 1)
	require.Equal(t, c40.ID, allocations[0].TransactionID)
	require.True(t, allocations[0].AllocatedAmount.Equal(decimal.RequireFromString("40")))
	require.Equal(t, "USD", allocations[0].Currency)
	require.False(t, allocations[0].UseBaseAmount)
	require.True(t, total.Equal(decimal.RequireFromString("40")))
}

func TestAllocateOneToMany_ToleranceAbsolute(t *testing.T) {
	t.Parallel()

	cfg := &AllocationConfig{
		AllowPartial:   true,
		Direction:      AllocationDirectionLeftToRight,
		ToleranceMode:  AllocationToleranceAbsolute,
		ToleranceValue: decimal.RequireFromString("1"),
	}
	now := time.Date(2026, 1, 1, 10, 0, 0, 0, time.UTC)
	c40 := CandidateTransaction{
		ID:             uuid.New(),
		Amount:         decimal.RequireFromString("40"),
		OriginalAmount: decimal.RequireFromString("40"),
		Currency:       "USD",
		Date:           now,
	}
	c59 := CandidateTransaction{
		ID:             uuid.New(),
		Amount:         decimal.RequireFromString("59"),
		OriginalAmount: decimal.RequireFromString("59"),
		Currency:       "USD",
		Date:           now,
	}
	target := CandidateTransaction{
		ID:             uuid.New(),
		Amount:         decimal.RequireFromString("100"),
		OriginalAmount: decimal.RequireFromString("100"),
		Currency:       "USD",
		Date:           now,
	}
	candidates := []CandidateTransaction{c40, c59}

	allocations, total, ok, err := AllocateOneToMany(target, candidates, cfg)
	require.NoError(t, err)
	require.True(t, ok)
	require.Len(t, allocations, 2)
	require.Equal(t, c40.ID, allocations[0].TransactionID)
	require.True(t, allocations[0].AllocatedAmount.Equal(decimal.RequireFromString("40")))
	require.Equal(t, "USD", allocations[0].Currency)
	require.False(t, allocations[0].UseBaseAmount)
	require.Equal(t, c59.ID, allocations[1].TransactionID)
	require.True(t, allocations[1].AllocatedAmount.Equal(decimal.RequireFromString("59")))
	require.Equal(t, "USD", allocations[1].Currency)
	require.False(t, allocations[1].UseBaseAmount)
	require.True(t, total.Equal(decimal.RequireFromString("99")))
}

func TestAllocateOneToMany_TolerancePercent(t *testing.T) {
	t.Parallel()

	cfg := &AllocationConfig{
		AllowPartial:   true,
		Direction:      AllocationDirectionLeftToRight,
		ToleranceMode:  AllocationTolerancePercent,
		ToleranceValue: decimal.RequireFromString("0.02"),
	}
	now := time.Date(2026, 1, 1, 10, 0, 0, 0, time.UTC)
	c40 := CandidateTransaction{
		ID:             uuid.New(),
		Amount:         decimal.RequireFromString("40"),
		OriginalAmount: decimal.RequireFromString("40"),
		Currency:       "USD",
		Date:           now,
	}
	c58 := CandidateTransaction{
		ID:             uuid.New(),
		Amount:         decimal.RequireFromString("58"),
		OriginalAmount: decimal.RequireFromString("58"),
		Currency:       "USD",
		Date:           now,
	}
	target := CandidateTransaction{
		ID:             uuid.New(),
		Amount:         decimal.RequireFromString("100"),
		OriginalAmount: decimal.RequireFromString("100"),
		Currency:       "USD",
		Date:           now,
	}
	candidates := []CandidateTransaction{c40, c58}

	allocations, total, ok, err := AllocateOneToMany(target, candidates, cfg)
	require.NoError(t, err)
	require.True(t, ok)
	require.Len(t, allocations, 2)
	require.Equal(t, c40.ID, allocations[0].TransactionID)
	require.True(t, allocations[0].AllocatedAmount.Equal(decimal.RequireFromString("40")))
	require.Equal(t, "USD", allocations[0].Currency)
	require.False(t, allocations[0].UseBaseAmount)
	require.Equal(t, c58.ID, allocations[1].TransactionID)
	require.True(t, allocations[1].AllocatedAmount.Equal(decimal.RequireFromString("58")))
	require.Equal(t, "USD", allocations[1].Currency)
	require.False(t, allocations[1].UseBaseAmount)
	require.True(t, total.Equal(decimal.RequireFromString("98")))
}

func TestAllocateOneToMany_PartialAllowed(t *testing.T) {
	t.Parallel()

	cfg := &AllocationConfig{
		AllowPartial:   true,
		Direction:      AllocationDirectionLeftToRight,
		ToleranceMode:  AllocationToleranceAbsolute,
		ToleranceValue: decimal.Zero,
	}
	now := time.Date(2026, 1, 1, 10, 0, 0, 0, time.UTC)
	c40 := CandidateTransaction{
		ID:             uuid.New(),
		Amount:         decimal.RequireFromString("40"),
		OriginalAmount: decimal.RequireFromString("40"),
		Currency:       "USD",
		Date:           now,
	}
	c80 := CandidateTransaction{
		ID:             uuid.New(),
		Amount:         decimal.RequireFromString("80"),
		OriginalAmount: decimal.RequireFromString("80"),
		Currency:       "USD",
		Date:           now,
	}
	target := CandidateTransaction{
		ID:             uuid.New(),
		Amount:         decimal.RequireFromString("100"),
		OriginalAmount: decimal.RequireFromString("100"),
		Currency:       "USD",
		Date:           now,
	}
	candidates := []CandidateTransaction{c40, c80}

	allocations, total, ok, err := AllocateOneToMany(target, candidates, cfg)
	require.NoError(t, err)
	require.True(t, ok)
	require.Len(t, allocations, 2)
	require.Equal(t, c40.ID, allocations[0].TransactionID)
	require.True(t, allocations[0].AllocatedAmount.Equal(decimal.RequireFromString("40")))
	require.Equal(t, "USD", allocations[0].Currency)
	require.False(t, allocations[0].UseBaseAmount)
	require.Equal(t, c80.ID, allocations[1].TransactionID)
	require.True(t, allocations[1].AllocatedAmount.Equal(decimal.RequireFromString("60")))
	require.Equal(t, "USD", allocations[1].Currency)
	require.False(t, allocations[1].UseBaseAmount)
	require.True(t, total.Equal(decimal.RequireFromString("100")))
}

func TestAllocateOneToMany_EmptyCandidates(t *testing.T) {
	t.Parallel()

	cfg := &AllocationConfig{
		AllowPartial:   true,
		Direction:      AllocationDirectionLeftToRight,
		ToleranceMode:  AllocationToleranceAbsolute,
		ToleranceValue: decimal.Zero,
	}

	_, total, ok, err := AllocateOneToMany(
		CandidateTransaction{Amount: decimal.NewFromInt(10), OriginalAmount: decimal.NewFromInt(10), Currency: "USD"},
		nil,
		cfg,
	)
	require.NoError(t, err)
	require.False(t, ok)
	require.True(t, total.IsZero())
}

func TestAllocateOneToMany_NilConfig(t *testing.T) {
	t.Parallel()

	err := allocationErrorOnly(
		AllocateOneToMany(CandidateTransaction{Amount: decimal.NewFromInt(10), OriginalAmount: decimal.NewFromInt(10)}, nil, nil),
	)
	require.ErrorIs(t, err, ErrAllocationConfigRequired)
}

func TestAllocateOneToMany_CandidateLimitExceeded(t *testing.T) {
	t.Parallel()

	cfg := &AllocationConfig{
		AllowPartial:   true,
		Direction:      AllocationDirectionLeftToRight,
		ToleranceMode:  AllocationToleranceAbsolute,
		ToleranceValue: decimal.Zero,
	}

	excess := maxAllocationCandidates + 1
	candidates := make([]CandidateTransaction, excess)

	allocations, total, matched, err := AllocateOneToMany(
		CandidateTransaction{Amount: decimal.NewFromInt(10), OriginalAmount: decimal.NewFromInt(10), Currency: "USD"},
		candidates,
		cfg,
	)
	require.ErrorIs(t, err, ErrCandidateSetTooLarge)
	require.Nil(t, allocations)
	require.True(t, total.IsZero())
	require.False(t, matched)
}

func TestAllocateOneToMany_TargetNegativeAmount(t *testing.T) {
	t.Parallel()

	cfg := &AllocationConfig{
		AllowPartial:   true,
		Direction:      AllocationDirectionLeftToRight,
		ToleranceMode:  AllocationToleranceAbsolute,
		ToleranceValue: decimal.Zero,
	}

	err := allocationErrorOnly(
		AllocateOneToMany(
			CandidateTransaction{Amount: decimal.RequireFromString("-1"), OriginalAmount: decimal.RequireFromString("-1"), Currency: "USD"},
			[]CandidateTransaction{{Amount: decimal.NewFromInt(1), OriginalAmount: decimal.NewFromInt(1), Currency: "USD"}},
			cfg,
		),
	)
	require.ErrorIs(t, err, ErrNegativeAllocationTarget)
}

func TestAllocateOneToMany_CandidateNegativeAmount(t *testing.T) {
	t.Parallel()

	cfg := &AllocationConfig{
		AllowPartial:   true,
		Direction:      AllocationDirectionLeftToRight,
		ToleranceMode:  AllocationToleranceAbsolute,
		ToleranceValue: decimal.Zero,
	}
	now := time.Date(2026, 1, 1, 10, 0, 0, 0, time.UTC)

	err := allocationErrorOnly(
		AllocateOneToMany(
			CandidateTransaction{
				Amount:         decimal.RequireFromString("10"),
				OriginalAmount: decimal.RequireFromString("10"),
				Currency:       "USD",
				Date:           now,
			},
			[]CandidateTransaction{
				{Amount: decimal.RequireFromString("-1"), OriginalAmount: decimal.RequireFromString("-1"), Currency: "USD", Date: now},
			},
			cfg,
		),
	)
	require.ErrorIs(t, err, ErrNegativeAllocationCandidate)
}

func TestAllocateOneToMany_UseBaseAmountMissingCurrencyIsSkipped(t *testing.T) {
	t.Parallel()

	cfg := &AllocationConfig{
		AllowPartial:   true,
		Direction:      AllocationDirectionLeftToRight,
		ToleranceMode:  AllocationToleranceAbsolute,
		ToleranceValue: decimal.Zero,
		UseBaseAmount:  true,
	}

	base := decimal.RequireFromString("10")
	now := time.Date(2026, 1, 1, 10, 0, 0, 0, time.UTC)

	allocations, total, ok, err := AllocateOneToMany(
		CandidateTransaction{
			Amount:         decimal.RequireFromString("10"),
			OriginalAmount: decimal.RequireFromString("10"),
			AmountBase:     &base,
			Currency:       "USD",
			CurrencyBase:   "USD",
			Date:           now,
		},
		[]CandidateTransaction{
			{
				Amount:         decimal.RequireFromString("10"),
				OriginalAmount: decimal.RequireFromString("10"),
				AmountBase:     &base,
				Currency:       "USD",
				Date:           now,
			},
		},
		cfg,
	)
	require.NoError(t, err)
	require.False(t, ok)
	require.Empty(t, allocations)
	require.True(t, total.IsZero())
}

func TestAllocateOneToMany_CurrencyMismatchIsSkipped(t *testing.T) {
	t.Parallel()

	cfg := &AllocationConfig{
		AllowPartial:   true,
		Direction:      AllocationDirectionLeftToRight,
		ToleranceMode:  AllocationToleranceAbsolute,
		ToleranceValue: decimal.Zero,
	}

	now := time.Date(2026, 1, 1, 10, 0, 0, 0, time.UTC)
	target := CandidateTransaction{
		ID:             uuid.New(),
		Amount:         decimal.RequireFromString("10"),
		OriginalAmount: decimal.RequireFromString("10"),
		Currency:       "USD",
		Date:           now,
	}
	candidates := []CandidateTransaction{
		{ID: uuid.New(), Amount: decimal.RequireFromString("10"), OriginalAmount: decimal.RequireFromString("10"), Currency: "EUR", Date: now},
	}

	allocations, total, ok, err := AllocateOneToMany(target, candidates, cfg)
	require.NoError(t, err)
	require.False(t, ok)
	require.Empty(t, allocations)
	require.True(t, total.IsZero())
}

func TestAllocateOneToMany_BaseCurrencyMismatchIsSkipped(t *testing.T) {
	t.Parallel()

	cfg := &AllocationConfig{
		AllowPartial:   true,
		Direction:      AllocationDirectionLeftToRight,
		ToleranceMode:  AllocationToleranceAbsolute,
		ToleranceValue: decimal.Zero,
		UseBaseAmount:  true,
	}

	baseTarget := decimal.RequireFromString("10")
	baseCandidate := decimal.RequireFromString("10")
	now := time.Date(2026, 1, 1, 10, 0, 0, 0, time.UTC)
	target := CandidateTransaction{
		ID:             uuid.New(),
		Amount:         decimal.RequireFromString("10"),
		OriginalAmount: decimal.RequireFromString("10"),
		AmountBase:     &baseTarget,
		Currency:       "USD",
		CurrencyBase:   "USD",
		Date:           now,
	}
	candidates := []CandidateTransaction{
		{
			ID:             uuid.New(),
			Amount:         decimal.RequireFromString("10"),
			OriginalAmount: decimal.RequireFromString("10"),
			AmountBase:     &baseCandidate,
			Currency:       "USD",
			CurrencyBase:   "BRL",
			Date:           now,
		},
	}

	allocations, total, ok, err := AllocateOneToMany(target, candidates, cfg)
	require.NoError(t, err)
	require.False(t, ok)
	require.Empty(t, allocations)
	require.True(t, total.IsZero())
}

func TestAllocateOneToMany_CandidateBaseAmountMissing(t *testing.T) {
	t.Parallel()

	cfg := &AllocationConfig{
		AllowPartial:   true,
		Direction:      AllocationDirectionLeftToRight,
		ToleranceMode:  AllocationToleranceAbsolute,
		ToleranceValue: decimal.Zero,
		UseBaseAmount:  true,
	}

	baseTarget := decimal.RequireFromString("10")
	now := time.Date(2026, 1, 1, 10, 0, 0, 0, time.UTC)
	target := CandidateTransaction{
		ID:             uuid.New(),
		Amount:         decimal.RequireFromString("10"),
		OriginalAmount: decimal.RequireFromString("10"),
		AmountBase:     &baseTarget,
		Currency:       "USD",
		CurrencyBase:   "USD",
		Date:           now,
	}
	candidates := []CandidateTransaction{
		{
			ID:             uuid.New(),
			Amount:         decimal.RequireFromString("10"),
			OriginalAmount: decimal.RequireFromString("10"),
			Currency:       "USD",
			CurrencyBase:   "USD",
			Date:           now,
		},
	}

	allocations, total, ok, err := AllocateOneToMany(target, candidates, cfg)
	require.NoError(t, err)
	require.False(t, ok)
	require.Empty(t, allocations)
	require.True(t, total.IsZero())
}

func TestWithinAllocationTolerance_PartialDisabled_AcceptsResidualWithinThreshold(t *testing.T) {
	t.Parallel()

	cfg := &AllocationConfig{
		AllowPartial:   false,
		Direction:      AllocationDirectionLeftToRight,
		ToleranceMode:  AllocationToleranceAbsolute,
		ToleranceValue: decimal.RequireFromString("5"),
	}

	ok := withinAllocationTolerance(
		decimal.RequireFromString("4"),
		decimal.RequireFromString("100"),
		cfg,
	)
	require.True(t, ok)
}

func TestWithinAllocationTolerance_NilConfig(t *testing.T) {
	t.Parallel()

	ok := withinAllocationTolerance(
		decimal.RequireFromString("1"),
		decimal.RequireFromString("100"),
		nil,
	)
	require.False(t, ok)
}

func TestWithinAllocationTolerance_UnknownMode(t *testing.T) {
	t.Parallel()

	cfg := &AllocationConfig{
		AllowPartial:   true,
		Direction:      AllocationDirectionLeftToRight,
		ToleranceMode:  AllocationToleranceMode("UNKNOWN"),
		ToleranceValue: decimal.Zero,
	}

	ok := withinAllocationTolerance(
		decimal.RequireFromString("1"),
		decimal.RequireFromString("100"),
		cfg,
	)
	require.False(t, ok)
}

func TestWithinAllocationTolerance_NegativeThresholdPercent(t *testing.T) {
	t.Parallel()

	cfg := &AllocationConfig{
		AllowPartial:   true,
		Direction:      AllocationDirectionLeftToRight,
		ToleranceMode:  AllocationTolerancePercent,
		ToleranceValue: decimal.RequireFromString("-0.01"),
	}

	ok := withinAllocationTolerance(
		decimal.RequireFromString("1"),
		decimal.RequireFromString("100"),
		cfg,
	)
	require.False(t, ok)
}

func TestAllocateOneToMany_DeterministicOrderAndNoMutation(t *testing.T) {
	t.Parallel()

	cfg := &AllocationConfig{
		AllowPartial:   true,
		Direction:      AllocationDirectionLeftToRight,
		ToleranceMode:  AllocationToleranceAbsolute,
		ToleranceValue: decimal.Zero,
	}

	now := time.Date(2026, 1, 1, 10, 0, 0, 0, time.UTC)
	c60 := CandidateTransaction{
		ID:             uuid.MustParse("00000000-0000-0000-0000-00000000d060"),
		Amount:         decimal.RequireFromString("60"),
		OriginalAmount: decimal.RequireFromString("60"),
		Currency:       "USD",
		Date:           now,
	}
	c40 := CandidateTransaction{
		ID:             uuid.MustParse("00000000-0000-0000-0000-00000000d040"),
		Amount:         decimal.RequireFromString("40"),
		OriginalAmount: decimal.RequireFromString("40"),
		Currency:       "USD",
		Date:           now,
	}
	candidates := []CandidateTransaction{c60, c40}
	original := append([]CandidateTransaction(nil), candidates...)
	target := CandidateTransaction{
		ID:             uuid.MustParse("00000000-0000-0000-0000-00000000d001"),
		Amount:         decimal.RequireFromString("100"),
		OriginalAmount: decimal.RequireFromString("100"),
		Currency:       "USD",
		Date:           now,
	}

	allocationsFirst, _, okFirst, err := AllocateOneToMany(target, candidates, cfg)
	require.NoError(t, err)
	require.True(t, okFirst)

	allocationsSecond, _, okSecond, err := AllocateOneToMany(target, candidates, cfg)
	require.NoError(t, err)
	require.True(t, okSecond)

	require.Equal(t, original, candidates)
	require.Equal(t, allocationsFirst, allocationsSecond)
	require.Len(t, allocationsFirst, 2)
	require.Equal(t, c40.ID, allocationsFirst[0].TransactionID)
	require.True(t, allocationsFirst[0].AllocatedAmount.Equal(decimal.RequireFromString("40")))
	require.Equal(t, "USD", allocationsFirst[0].Currency)
	require.False(t, allocationsFirst[0].UseBaseAmount)
	require.Equal(t, c60.ID, allocationsFirst[1].TransactionID)
	require.True(t, allocationsFirst[1].AllocatedAmount.Equal(decimal.RequireFromString("60")))
	require.Equal(t, "USD", allocationsFirst[1].Currency)
	require.False(t, allocationsFirst[1].UseBaseAmount)
}

func TestAllocateOneToMany_TargetZeroAmount(t *testing.T) {
	t.Parallel()

	cfg := &AllocationConfig{
		AllowPartial:   true,
		Direction:      AllocationDirectionLeftToRight,
		ToleranceMode:  AllocationToleranceAbsolute,
		ToleranceValue: decimal.Zero,
	}

	_, total, ok, err := AllocateOneToMany(
		CandidateTransaction{Amount: decimal.Zero, OriginalAmount: decimal.Zero},
		[]CandidateTransaction{{Amount: decimal.NewFromInt(5), OriginalAmount: decimal.NewFromInt(5)}},
		cfg,
	)
	require.NoError(t, err)
	require.False(t, ok)
	require.True(t, total.IsZero())
}

func allocationErrorOnly(_ []Allocation, _ decimal.Decimal, _ bool, err error) error {
	return err
}

func TestAllocateOneToMany_SkipsZeroCandidate(t *testing.T) {
	t.Parallel()

	cfg := &AllocationConfig{
		AllowPartial:   true,
		Direction:      AllocationDirectionLeftToRight,
		ToleranceMode:  AllocationToleranceAbsolute,
		ToleranceValue: decimal.Zero,
	}

	now := time.Date(2026, 1, 1, 10, 0, 0, 0, time.UTC)
	c0 := CandidateTransaction{ID: uuid.New(), Amount: decimal.Zero, OriginalAmount: decimal.Zero, Currency: "USD", Date: now}
	c10 := CandidateTransaction{
		ID:             uuid.New(),
		Amount:         decimal.RequireFromString("10"),
		OriginalAmount: decimal.RequireFromString("10"),
		Currency:       "USD",
		Date:           now,
	}
	c20 := CandidateTransaction{
		ID:             uuid.New(),
		Amount:         decimal.RequireFromString("20"),
		OriginalAmount: decimal.RequireFromString("20"),
		Currency:       "USD",
		Date:           now,
	}
	candidates := []CandidateTransaction{c0, c10, c20}
	target := CandidateTransaction{
		ID:             uuid.New(),
		Amount:         decimal.RequireFromString("30"),
		OriginalAmount: decimal.RequireFromString("30"),
		Currency:       "USD",
		Date:           now,
	}

	allocations, total, ok, err := AllocateOneToMany(target, candidates, cfg)
	require.NoError(t, err)
	require.True(t, ok)
	require.Len(t, allocations, 2)
	require.Equal(t, c10.ID, allocations[0].TransactionID)
	require.Equal(t, "USD", allocations[0].Currency)
	require.False(t, allocations[0].UseBaseAmount)
	require.Equal(t, c20.ID, allocations[1].TransactionID)
	require.Equal(t, "USD", allocations[1].Currency)
	require.False(t, allocations[1].UseBaseAmount)
	require.True(t, total.Equal(decimal.RequireFromString("30")))
}

func TestBuildAllocationProposal_NoMatchReturnsNil(t *testing.T) {
	t.Parallel()

	rule := RuleDefinition{
		ID: uuid.New(),
		Allocation: &AllocationConfig{
			AllowPartial:   false,
			Direction:      AllocationDirectionLeftToRight,
			ToleranceMode:  AllocationToleranceAbsolute,
			ToleranceValue: decimal.Zero,
		},
	}

	now := time.Date(2026, 1, 1, 10, 0, 0, 0, time.UTC)
	target := CandidateTransaction{
		ID:             uuid.New(),
		Amount:         decimal.RequireFromString("100"),
		OriginalAmount: decimal.RequireFromString("100"),
		Currency:       "USD",
		Date:           now,
	}
	candidates := []CandidateTransaction{
		{ID: uuid.New(), Amount: decimal.RequireFromString("60"), OriginalAmount: decimal.RequireFromString("60"), Currency: "USD", Date: now},
	}

	proposal, err := BuildAllocationProposal(rule, target, candidates, 70)
	require.NoError(t, err)
	require.Nil(t, proposal)
}

func TestBuildAllocationProposal_MissingBaseAmountReturnsNil(t *testing.T) {
	t.Parallel()

	rule := RuleDefinition{
		ID: uuid.New(),
		Allocation: &AllocationConfig{
			AllowPartial:   true,
			Direction:      AllocationDirectionLeftToRight,
			ToleranceMode:  AllocationToleranceAbsolute,
			ToleranceValue: decimal.Zero,
			UseBaseAmount:  true,
		},
	}

	now := time.Date(2026, 1, 1, 10, 0, 0, 0, time.UTC)
	target := CandidateTransaction{
		ID:             uuid.New(),
		Amount:         decimal.RequireFromString("100"),
		OriginalAmount: decimal.RequireFromString("100"),
		Currency:       "USD",
		Date:           now,
	}
	candidates := []CandidateTransaction{
		{ID: uuid.New(), Amount: decimal.RequireFromString("60"), OriginalAmount: decimal.RequireFromString("60"), Currency: "USD", Date: now},
	}

	proposal, err := BuildAllocationProposal(rule, target, candidates, 70)
	require.NoError(t, err)
	require.Nil(t, proposal)

	rule.Allocation.UseBaseAmount = true
	baseAmount := decimal.RequireFromString("100")
	target.AmountBase = &baseAmount
	target.CurrencyBase = "USD"
	proposal, err = BuildAllocationProposal(rule, target, candidates, 70)
	require.NoError(t, err)
	require.Nil(t, proposal)
}

func TestAllocateOneToMany_UseBaseAmount(t *testing.T) {
	t.Parallel()

	cfg := &AllocationConfig{
		AllowPartial:   true,
		Direction:      AllocationDirectionLeftToRight,
		ToleranceMode:  AllocationToleranceAbsolute,
		ToleranceValue: decimal.Zero,
		UseBaseAmount:  true,
	}

	now := time.Date(2026, 1, 1, 10, 0, 0, 0, time.UTC)
	base := decimal.RequireFromString("50")
	target := CandidateTransaction{
		ID:             uuid.New(),
		Amount:         decimal.RequireFromString("100"),
		OriginalAmount: decimal.RequireFromString("100"),
		AmountBase:     &base,
		Currency:       "EUR",
		CurrencyBase:   "USD",
		Date:           now,
	}
	candidateBase := decimal.RequireFromString("50")
	candidates := []CandidateTransaction{
		{
			ID:             uuid.New(),
			Amount:         decimal.RequireFromString("90"),
			OriginalAmount: decimal.RequireFromString("90"),
			AmountBase:     &candidateBase,
			Currency:       "EUR",
			CurrencyBase:   "USD",
			Date:           now,
		},
	}

	allocations, total, ok, err := AllocateOneToMany(target, candidates, cfg)
	require.NoError(t, err)
	require.True(t, ok)
	require.Len(t, allocations, 1)
	require.True(t, allocations[0].AllocatedAmount.Equal(decimal.RequireFromString("50")))
	require.Equal(t, "USD", allocations[0].Currency)
	require.True(t, allocations[0].UseBaseAmount)
	require.True(t, total.Equal(decimal.RequireFromString("50")))
}

func TestWithinAllocationTolerance_NegativeAbsolute(t *testing.T) {
	t.Parallel()

	cfg := &AllocationConfig{
		AllowPartial:   true,
		Direction:      AllocationDirectionLeftToRight,
		ToleranceMode:  AllocationToleranceAbsolute,
		ToleranceValue: decimal.RequireFromString("-1"),
	}

	ok := withinAllocationTolerance(
		decimal.RequireFromString("1"),
		decimal.RequireFromString("100"),
		cfg,
	)
	require.False(t, ok)
}
