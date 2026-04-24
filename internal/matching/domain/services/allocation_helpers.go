// Copyright 2025 Lerian Studio. All rights reserved.
// Use of this source code is governed by an Elastic License 2.0
// that can be found in the LICENSE.md file.

package services

import (
	"errors"

	"github.com/google/uuid"
	"github.com/shopspring/decimal"
)

// Sentinel errors for allocation validation.
var (
	ErrInvalidAllocationToleranceMode = errors.New("invalid allocation tolerance mode")
	ErrInvalidAllocationDirection     = errors.New("invalid allocation direction")
	ErrNegativeAllocationTolerance    = errors.New("allocation tolerance must be non-negative")
)

// AllocationProposalResult contains the result of building an allocation proposal.
type AllocationProposalResult struct {
	Proposal *MatchProposal
	Failure  *AllocationFailure
}

// BuildAllocationProposal creates a match proposal from allocation rules.
func BuildAllocationProposal(
	rule RuleDefinition,
	target CandidateTransaction,
	candidates []CandidateTransaction,
	score int,
) (*MatchProposal, error) {
	result, err := buildAllocationProposalDetailed(rule, target, candidates, score)
	if err != nil {
		return nil, err
	}

	return result.Proposal, nil
}

func buildAllocationProposalDetailed(
	rule RuleDefinition,
	target CandidateTransaction,
	candidates []CandidateTransaction,
	score int,
) (*AllocationProposalResult, error) {
	if rule.Allocation == nil || len(candidates) == 0 {
		return &AllocationProposalResult{}, nil
	}

	if err := validateAllocationConfig(rule.Allocation); err != nil {
		return nil, err
	}

	allocResult, err := allocateByRuleDetailed(rule.Allocation, target, candidates)
	if err != nil {
		return nil, err
	}

	if allocResult.Failure != nil {
		return &AllocationProposalResult{Failure: allocResult.Failure}, nil
	}

	if !allocResult.Complete {
		return &AllocationProposalResult{}, nil
	}

	proposal := buildAllocationMatchProposal(
		rule.Allocation,
		rule,
		target,
		allocResult.Allocations,
		allocResult.Total,
		score,
	)
	if proposal == nil {
		return &AllocationProposalResult{}, nil
	}

	return &AllocationProposalResult{Proposal: proposal}, nil
}

func allocateByRuleDetailed(
	cfg *AllocationConfig,
	target CandidateTransaction,
	candidates []CandidateTransaction,
) (*AllocationResult, error) {
	switch cfg.Direction {
	// Allocation is direction-agnostic at this stage; the same AllocateOneToManyDetailed
	// logic applies regardless of direction. Direction only affects how proposals are
	// constructed downstream in buildAllocationMatchProposal, where left/right IDs
	// and allocations are swapped based on the direction.
	case AllocationDirectionLeftToRight, AllocationDirectionRightToLeft:
		return AllocateOneToManyDetailed(target, candidates, cfg)
	default:
		return nil, ErrInvalidAllocationDirection
	}
}

func buildAllocationMatchProposal(
	cfg *AllocationConfig,
	rule RuleDefinition,
	target CandidateTransaction,
	allocations []Allocation,
	allocatedTotal decimal.Decimal,
	score int,
) *MatchProposal {
	if cfg == nil {
		return nil
	}

	mode := "1:N"
	leftIDs := []uuid.UUID{target.ID}

	var rightIDs []uuid.UUID

	targetCurrency := target.Currency
	if cfg.UseBaseAmount {
		targetCurrency = target.CurrencyBase
	}

	leftAllocations := []Allocation{
		{
			TransactionID:   target.ID,
			AllocatedAmount: allocatedTotal,
			Currency:        targetCurrency,
			UseBaseAmount:   cfg.UseBaseAmount,
		},
	}
	rightAllocations := allocations

	if cfg.Direction == AllocationDirectionRightToLeft {
		mode = "N:1"
		leftIDs = make([]uuid.UUID, 0, len(allocations))

		for _, allocation := range allocations {
			leftIDs = append(leftIDs, allocation.TransactionID)
		}

		rightIDs = []uuid.UUID{target.ID}
		leftAllocations = allocations
		rightAllocations = []Allocation{
			{
				TransactionID:   target.ID,
				AllocatedAmount: allocatedTotal,
				Currency:        targetCurrency,
				UseBaseAmount:   cfg.UseBaseAmount,
			},
		}
	} else {
		rightIDs = make([]uuid.UUID, 0, len(allocations))

		for _, allocation := range allocations {
			rightIDs = append(rightIDs, allocation.TransactionID)
		}
	}

	return &MatchProposal{
		RuleID:           rule.ID,
		LeftIDs:          leftIDs,
		RightIDs:         rightIDs,
		LeftAllocations:  leftAllocations,
		RightAllocations: rightAllocations,
		Score:            score,
		Mode:             mode,
	}
}

func validateAllocationConfig(cfg *AllocationConfig) error {
	if cfg == nil {
		return ErrAllocationConfigRequired
	}

	if cfg.ToleranceMode != AllocationToleranceAbsolute &&
		cfg.ToleranceMode != AllocationTolerancePercent {
		return ErrInvalidAllocationToleranceMode
	}

	if cfg.Direction != AllocationDirectionLeftToRight &&
		cfg.Direction != AllocationDirectionRightToLeft {
		return ErrInvalidAllocationDirection
	}

	if cfg.ToleranceValue.IsNegative() {
		return ErrNegativeAllocationTolerance
	}

	return nil
}
