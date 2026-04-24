// Copyright 2025 Lerian Studio. All rights reserved.
// Use of this source code is governed by an Elastic License 2.0
// that can be found in the LICENSE.md file.

package services

import (
	"errors"

	"github.com/google/uuid"
	"github.com/shopspring/decimal"
)

// Sentinel errors for allocation operations.
var (
	ErrAllocationConfigRequired    = errors.New("allocation config is required")
	ErrBaseAmountRequired          = errors.New("base amount is required for allocation")
	ErrBaseCurrencyRequired        = errors.New("base currency is required for allocation")
	ErrNegativeAllocationAmount    = errors.New("allocation amounts must be non-negative")
	ErrNegativeAllocationTarget    = errors.New("allocation target amount must be non-negative")
	ErrNegativeAllocationCandidate = errors.New("allocation candidate amount must be non-negative")
)

const maxAllocationCandidates = 100000

// AllocationResult contains the result of an allocation operation including any failure information.
type AllocationResult struct {
	Allocations []Allocation
	Total       decimal.Decimal
	Complete    bool
	Failure     *AllocationFailure
}

// AllocateOneToMany allocates a target transaction across multiple candidates.
//
// Determinism contract:
// - candidates are cloned and sorted deterministically (SortTransactions) prior to allocation.
// - returned allocations are in that deterministic order.
// - input slices are never mutated.
func AllocateOneToMany(
	target CandidateTransaction,
	candidates []CandidateTransaction,
	cfg *AllocationConfig,
) ([]Allocation, decimal.Decimal, bool, error) {
	result, err := AllocateOneToManyDetailed(target, candidates, cfg)
	if err != nil {
		return nil, decimal.Zero, false, err
	}

	return result.Allocations, result.Total, result.Complete, nil
}

// AllocateOneToManyDetailed allocates a target transaction across multiple candidates
// and returns structured failure information when allocation cannot be completed.
func AllocateOneToManyDetailed(
	target CandidateTransaction,
	candidates []CandidateTransaction,
	cfg *AllocationConfig,
) (*AllocationResult, error) {
	if err := validateAllocationInputs(cfg); err != nil {
		return nil, err
	}

	if len(candidates) == 0 {
		return &AllocationResult{}, nil
	}

	if len(candidates) > maxAllocationCandidates {
		return nil, ErrCandidateSetTooLarge
	}

	sourceAmount, failure, err := allocationAmountDetailed(target, cfg)
	if err != nil {
		return nil, err
	}

	if failure != nil {
		return &AllocationResult{Failure: failure}, nil
	}

	if sourceAmount.IsNegative() {
		return nil, ErrNegativeAllocationTarget
	}

	if sourceAmount.IsZero() {
		return &AllocationResult{}, nil
	}

	allocationCurrency, currencyFailure := resolveAllocationCurrency(target, cfg)
	if currencyFailure != nil {
		return &AllocationResult{Failure: currencyFailure}, nil
	}

	sorted := cloneTransactions(candidates)
	SortTransactions(sorted)

	allocations, total, complete, err := allocateCandidates(
		sorted,
		sourceAmount,
		cfg,
		allocationCurrency,
	)
	if err != nil {
		return nil, err
	}

	return buildAllocationResult(
		target.ID,
		sourceAmount,
		allocations,
		total,
		complete,
		allocationCurrency,
		cfg,
	), nil
}

func resolveAllocationCurrency(
	target CandidateTransaction,
	cfg *AllocationConfig,
) (string, *AllocationFailure) {
	if !cfg.UseBaseAmount {
		return target.Currency, nil
	}

	if target.CurrencyBase == "" {
		return "", NewFXRateUnavailableFailure(target.ID, AllocationFailureMissingCurrencyBase)
	}

	return target.CurrencyBase, nil
}

func buildAllocationResult(
	targetID uuid.UUID,
	sourceAmount decimal.Decimal,
	allocations []Allocation,
	total decimal.Decimal,
	complete bool,
	currency string,
	cfg *AllocationConfig,
) *AllocationResult {
	result := &AllocationResult{
		Allocations: allocations,
		Total:       total,
		Complete:    complete,
	}

	if !complete && !cfg.AllowPartial && len(allocations) > 0 {
		gap := sourceAmount.Sub(total)
		result.Failure = NewSplitIncompleteFailure(
			targetID,
			sourceAmount.String(),
			total.String(),
			gap.String(),
			currency,
			cfg.UseBaseAmount,
			cfg.AllowPartial,
		)
	}

	return result
}

func validateAllocationInputs(cfg *AllocationConfig) error {
	if cfg == nil {
		return ErrAllocationConfigRequired
	}

	return validateAllocationConfig(cfg)
}

func cloneTransactions(in []CandidateTransaction) []CandidateTransaction {
	out := make([]CandidateTransaction, len(in))
	copy(out, in)

	return out
}

func allocateCandidates(
	candidates []CandidateTransaction,
	sourceAmount decimal.Decimal,
	cfg *AllocationConfig,
	allocationCurrency string,
) ([]Allocation, decimal.Decimal, bool, error) {
	allocations := make([]Allocation, 0, len(candidates))
	remaining := sourceAmount
	sum := decimal.Zero

	for _, candidate := range candidates {
		allocation, newRemaining, ok, err := tryAllocateCandidate(
			candidate,
			remaining,
			cfg,
			allocationCurrency,
		)
		if err != nil {
			return nil, decimal.Zero, false, err
		}

		if !ok {
			continue
		}

		allocations = append(allocations, allocation)
		sum = sum.Add(allocation.AllocatedAmount)
		remaining = newRemaining

		if withinAllocationTolerance(remaining, sourceAmount, cfg) {
			return allocations, sum, true, nil
		}
	}

	complete := withinAllocationTolerance(remaining, sourceAmount, cfg) && len(allocations) > 0

	return allocations, sum, complete, nil
}

func tryAllocateCandidate(
	candidate CandidateTransaction,
	remaining decimal.Decimal,
	cfg *AllocationConfig,
	allocationCurrency string,
) (Allocation, decimal.Decimal, bool, error) {
	amount, err := allocationAmount(candidate, cfg)
	if err != nil {
		if errors.Is(err, ErrBaseAmountRequired) || errors.Is(err, ErrBaseCurrencyRequired) {
			return Allocation{}, remaining, false, nil
		}

		return Allocation{}, decimal.Zero, false, err
	}

	if amount.IsNegative() {
		return Allocation{}, decimal.Zero, false, ErrNegativeAllocationCandidate
	}

	if cfg.UseBaseAmount {
		if candidate.CurrencyBase == "" {
			return Allocation{}, remaining, false, nil
		}

		if candidate.CurrencyBase != allocationCurrency {
			return Allocation{}, remaining, false, nil
		}
	} else if candidate.Currency != allocationCurrency {
		return Allocation{}, remaining, false, nil
	}

	if amount.IsZero() {
		return Allocation{}, remaining, false, nil
	}

	if amount.GreaterThan(remaining) {
		if !cfg.AllowPartial {
			// No partial allocation allowed; deterministically skip this candidate.
			return Allocation{}, remaining, false, nil
		}

		amount = remaining
	}

	allocation := Allocation{
		TransactionID:   candidate.ID,
		AllocatedAmount: amount,
		Currency:        allocationCurrency,
		UseBaseAmount:   cfg.UseBaseAmount,
	}
	newRemaining := remaining.Sub(amount)

	if newRemaining.IsNegative() {
		// Defensive guard: should not happen due to capping logic above.
		return Allocation{}, decimal.Zero, false, ErrNegativeAllocationAmount
	}

	return allocation, newRemaining, true, nil
}

func allocationAmount(tx CandidateTransaction, cfg *AllocationConfig) (decimal.Decimal, error) {
	amount, failure, err := allocationAmountDetailed(tx, cfg)
	if err != nil {
		return decimal.Zero, err
	}

	if failure != nil {
		if failure.Code == AllocationFailureFXRateUnavailable &&
			failure.Meta[AllocationFailureMetaMissingKey] == AllocationFailureMissingAmountBase {
			return decimal.Zero, ErrBaseAmountRequired
		}

		return decimal.Zero, ErrBaseCurrencyRequired
	}

	return amount, nil
}

func allocationAmountDetailed(
	tx CandidateTransaction,
	cfg *AllocationConfig,
) (decimal.Decimal, *AllocationFailure, error) {
	if cfg == nil {
		return decimal.Zero, nil, ErrAllocationConfigRequired
	}

	if cfg.UseBaseAmount {
		if tx.AmountBase == nil {
			return decimal.Zero, NewFXRateUnavailableFailure(
				tx.ID,
				AllocationFailureMissingAmountBase,
			), nil
		}

		if tx.CurrencyBase == "" {
			return decimal.Zero, NewFXRateUnavailableFailure(
				tx.ID,
				AllocationFailureMissingCurrencyBase,
			), nil
		}

		return *tx.AmountBase, nil, nil
	}

	return tx.Amount, nil, nil
}

func withinAllocationTolerance(remaining, target decimal.Decimal, cfg *AllocationConfig) bool {
	if cfg == nil {
		return false
	}

	absRemaining := remaining.Abs()

	switch cfg.ToleranceMode {
	case AllocationTolerancePercent:
		threshold := target.Abs().Mul(cfg.ToleranceValue)
		if threshold.IsNegative() {
			return false
		}

		return absRemaining.LessThanOrEqual(threshold)

	case AllocationToleranceAbsolute:
		if cfg.ToleranceValue.IsNegative() {
			return false
		}

		return absRemaining.LessThanOrEqual(cfg.ToleranceValue)

	default:
		return false
	}
}
