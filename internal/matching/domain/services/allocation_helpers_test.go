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

func TestBuildAllocationProposalNilConfig(t *testing.T) {
	t.Parallel()

	rule := RuleDefinition{ID: uuid.New(), Allocation: nil}
	proposal, err := BuildAllocationProposal(rule, CandidateTransaction{ID: uuid.New()}, nil, 90)
	require.NoError(t, err)
	require.Nil(t, proposal)
}

func TestBuildAllocationProposalLeftToRight(t *testing.T) {
	t.Parallel()

	rule := RuleDefinition{
		ID: uuid.New(),
		Allocation: &AllocationConfig{
			AllowPartial:   true,
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
	candidates := []CandidateTransaction{c40, c80}

	proposal, err := BuildAllocationProposal(rule, target, candidates, 88)
	require.NoError(t, err)
	require.NotNil(t, proposal)
	require.Equal(t, "1:N", proposal.Mode)
	require.Equal(t, []uuid.UUID{target.ID}, proposal.LeftIDs)
	require.Equal(t, []uuid.UUID{c40.ID, c80.ID}, proposal.RightIDs)
	require.Len(t, proposal.LeftAllocations, 1)
	require.True(
		t,
		proposal.LeftAllocations[0].AllocatedAmount.Equal(decimal.RequireFromString("100")),
	)
	require.Equal(t, "USD", proposal.LeftAllocations[0].Currency)
	require.False(t, proposal.LeftAllocations[0].UseBaseAmount)
	require.Len(t, proposal.RightAllocations, 2)
	require.Equal(t, c40.ID, proposal.RightAllocations[0].TransactionID)
	require.True(
		t,
		proposal.RightAllocations[0].AllocatedAmount.Equal(decimal.RequireFromString("40")),
	)
	require.Equal(t, "USD", proposal.RightAllocations[0].Currency)
	require.False(t, proposal.RightAllocations[0].UseBaseAmount)
	require.Equal(t, c80.ID, proposal.RightAllocations[1].TransactionID)
	require.True(
		t,
		proposal.RightAllocations[1].AllocatedAmount.Equal(decimal.RequireFromString("60")),
	)
	require.Equal(t, "USD", proposal.RightAllocations[1].Currency)
	require.False(t, proposal.RightAllocations[1].UseBaseAmount)
}

func TestBuildAllocationProposalInvalidDirection(t *testing.T) {
	t.Parallel()

	rule := RuleDefinition{
		ID: uuid.New(),
		Allocation: &AllocationConfig{
			AllowPartial:   true,
			Direction:      AllocationDirection("BAD"),
			ToleranceMode:  AllocationToleranceAbsolute,
			ToleranceValue: decimal.Zero,
		},
	}

	_, err := BuildAllocationProposal(
		rule,
		CandidateTransaction{ID: uuid.New(), Amount: decimal.NewFromInt(1), OriginalAmount: decimal.NewFromInt(1)},
		[]CandidateTransaction{{ID: uuid.New(), Amount: decimal.NewFromInt(1), OriginalAmount: decimal.NewFromInt(1)}},
		88,
	)
	require.ErrorIs(t, err, ErrInvalidAllocationDirection)
}

func TestBuildAllocationProposalRightToLeft(t *testing.T) {
	t.Parallel()

	rule := RuleDefinition{
		ID: uuid.New(),
		Allocation: &AllocationConfig{
			AllowPartial:   true,
			Direction:      AllocationDirectionRightToLeft,
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
		{ID: uuid.New(), Amount: decimal.RequireFromString("40"), OriginalAmount: decimal.RequireFromString("40"), Currency: "USD", Date: now},
		{ID: uuid.New(), Amount: decimal.RequireFromString("60"), OriginalAmount: decimal.RequireFromString("60"), Currency: "USD", Date: now},
	}

	proposal, err := BuildAllocationProposal(rule, target, candidates, 88)
	require.NoError(t, err)
	require.NotNil(t, proposal)
	require.Equal(t, "N:1", proposal.Mode)
	require.Len(t, proposal.LeftIDs, 2)
	require.Len(t, proposal.RightIDs, 1)
	require.Equal(t, target.ID, proposal.RightIDs[0])
	require.Len(t, proposal.LeftAllocations, 2)
	require.Equal(t, candidates[0].ID, proposal.LeftAllocations[0].TransactionID)
	require.Equal(t, "USD", proposal.LeftAllocations[0].Currency)
	require.False(t, proposal.LeftAllocations[0].UseBaseAmount)
	require.Equal(t, candidates[1].ID, proposal.LeftAllocations[1].TransactionID)
	require.Equal(t, "USD", proposal.LeftAllocations[1].Currency)
	require.False(t, proposal.LeftAllocations[1].UseBaseAmount)
	require.Len(t, proposal.RightAllocations, 1)
	require.True(
		t,
		proposal.RightAllocations[0].AllocatedAmount.Equal(decimal.RequireFromString("100")),
	)
	require.Equal(t, "USD", proposal.RightAllocations[0].Currency)
	require.False(t, proposal.RightAllocations[0].UseBaseAmount)
}

func TestValidateAllocationConfig(t *testing.T) {
	t.Parallel()

	err := validateAllocationConfig(nil)
	require.ErrorIs(t, err, ErrAllocationConfigRequired)

	err = validateAllocationConfig(&AllocationConfig{ToleranceMode: "BAD"})
	require.ErrorIs(t, err, ErrInvalidAllocationToleranceMode)

	err = validateAllocationConfig(
		&AllocationConfig{ToleranceMode: AllocationToleranceAbsolute, Direction: "BAD"},
	)
	require.ErrorIs(t, err, ErrInvalidAllocationDirection)

	err = validateAllocationConfig(
		&AllocationConfig{
			ToleranceMode:  AllocationToleranceAbsolute,
			Direction:      AllocationDirectionLeftToRight,
			ToleranceValue: decimal.NewFromInt(-1),
		},
	)
	require.ErrorIs(t, err, ErrNegativeAllocationTolerance)
}

func TestAllocationAmountMust(t *testing.T) {
	t.Parallel()

	cfg := &AllocationConfig{UseBaseAmount: true}
	_, err := allocationAmount(CandidateTransaction{Amount: decimal.NewFromInt(1)}, cfg)
	require.ErrorIs(t, err, ErrBaseAmountRequired)
}

func TestAllocationAmountRequiresBaseCurrency(t *testing.T) {
	t.Parallel()

	cfg := &AllocationConfig{UseBaseAmount: true}
	base := decimal.NewFromInt(10)
	_, err := allocationAmount(
		CandidateTransaction{Amount: decimal.NewFromInt(1), AmountBase: &base},
		cfg,
	)
	require.ErrorIs(t, err, ErrBaseCurrencyRequired)
}
