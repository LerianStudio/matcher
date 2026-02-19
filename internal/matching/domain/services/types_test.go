//go:build unit

package services

import (
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/shopspring/decimal"
	"github.com/stretchr/testify/require"
)

func TestCandidateTransaction_Instantiation(t *testing.T) {
	t.Parallel()

	id := uuid.New()
	sourceID := uuid.New()
	amount := decimal.RequireFromString("100.50")
	baseAmount := decimal.RequireFromString("90.25")
	date := time.Date(2026, 1, 15, 10, 30, 0, 0, time.UTC)

	tx := CandidateTransaction{
		ID:             id,
		SourceID:       sourceID,
		Amount:         amount,
		OriginalAmount: amount,
		Currency:       "USD",
		AmountBase:     &baseAmount,
		CurrencyBase:   "EUR",
		Date:           date,
		Reference:      "REF-123",
	}

	require.Equal(t, id, tx.ID)
	require.Equal(t, sourceID, tx.SourceID)
	require.True(t, amount.Equal(tx.Amount))
	require.Equal(t, "USD", tx.Currency)
	require.NotNil(t, tx.AmountBase)
	require.True(t, baseAmount.Equal(*tx.AmountBase))
	require.Equal(t, "EUR", tx.CurrencyBase)
	require.Equal(t, date, tx.Date)
	require.Equal(t, "REF-123", tx.Reference)
}

func TestCandidateTransaction_NilAmountBase(t *testing.T) {
	t.Parallel()

	tx := CandidateTransaction{
		ID:             uuid.New(),
		Amount:         decimal.RequireFromString("50.00"),
		OriginalAmount: decimal.RequireFromString("50.00"),
		Currency:       "BRL",
		AmountBase:     nil,
	}

	require.Nil(t, tx.AmountBase)
	require.NotEqual(t, uuid.Nil, tx.ID)
	require.True(t, decimal.RequireFromString("50.00").Equal(tx.Amount))
	require.Equal(t, "BRL", tx.Currency)
}

func TestAllocation_Instantiation(t *testing.T) {
	t.Parallel()

	txID := uuid.New()
	allocatedAmount := decimal.RequireFromString("250.75")

	alloc := Allocation{
		TransactionID:   txID,
		AllocatedAmount: allocatedAmount,
		Currency:        "GBP",
		UseBaseAmount:   true,
	}

	require.Equal(t, txID, alloc.TransactionID)
	require.True(t, allocatedAmount.Equal(alloc.AllocatedAmount))
	require.Equal(t, "GBP", alloc.Currency)
	require.True(t, alloc.UseBaseAmount)
}

func TestAllocation_UseBaseAmountFalse(t *testing.T) {
	t.Parallel()

	alloc := Allocation{
		TransactionID:   uuid.New(),
		AllocatedAmount: decimal.RequireFromString("100.00"),
		Currency:        "USD",
		UseBaseAmount:   false,
	}

	require.False(t, alloc.UseBaseAmount)
	require.NotEqual(t, uuid.Nil, alloc.TransactionID)
	require.True(t, decimal.RequireFromString("100.00").Equal(alloc.AllocatedAmount))
	require.Equal(t, "USD", alloc.Currency)
}

func TestMatchProposal_Instantiation(t *testing.T) {
	t.Parallel()

	ruleID := uuid.New()
	leftID1 := uuid.New()
	leftID2 := uuid.New()
	rightID1 := uuid.New()

	leftAlloc := Allocation{
		TransactionID:   leftID1,
		AllocatedAmount: decimal.RequireFromString("50.00"),
		Currency:        "USD",
		UseBaseAmount:   false,
	}
	rightAlloc := Allocation{
		TransactionID:   rightID1,
		AllocatedAmount: decimal.RequireFromString("50.00"),
		Currency:        "USD",
		UseBaseAmount:   false,
	}

	proposal := MatchProposal{
		RuleID:           ruleID,
		LeftIDs:          []uuid.UUID{leftID1, leftID2},
		RightIDs:         []uuid.UUID{rightID1},
		LeftAllocations:  []Allocation{leftAlloc},
		RightAllocations: []Allocation{rightAlloc},
		Score:            95,
		Mode:             "N:1",
	}

	require.Equal(t, ruleID, proposal.RuleID)
	require.Len(t, proposal.LeftIDs, 2)
	require.Equal(t, leftID1, proposal.LeftIDs[0])
	require.Equal(t, leftID2, proposal.LeftIDs[1])
	require.Len(t, proposal.RightIDs, 1)
	require.Equal(t, rightID1, proposal.RightIDs[0])
	require.Len(t, proposal.LeftAllocations, 1)
	require.Len(t, proposal.RightAllocations, 1)
	require.Equal(t, 95, proposal.Score)
	require.Equal(t, "N:1", proposal.Mode)
}

func TestMatchProposal_EmptySlices(t *testing.T) {
	t.Parallel()

	proposal := MatchProposal{
		RuleID:           uuid.New(),
		LeftIDs:          nil,
		RightIDs:         nil,
		LeftAllocations:  nil,
		RightAllocations: nil,
		Score:            0,
		Mode:             "1:1",
	}

	require.Nil(t, proposal.LeftIDs)
	require.Nil(t, proposal.RightIDs)
	require.Nil(t, proposal.LeftAllocations)
	require.Nil(t, proposal.RightAllocations)
	require.Equal(t, 0, proposal.Score)
	require.NotEqual(t, uuid.Nil, proposal.RuleID)
	require.Equal(t, "1:1", proposal.Mode)
}
