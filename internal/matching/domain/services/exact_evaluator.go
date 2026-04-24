// Copyright 2025 Lerian Studio. All rights reserved.
// Use of this source code is governed by an Elastic License 2.0
// that can be found in the LICENSE.md file.

// Package services provides matching rule evaluators and related errors, such as ErrExactConfigRequired.
// It contains reusable domain logic for exact and tolerance-based matching operations.
package services

import (
	"errors"
	"strings"
)

// Sentinel errors for exact match evaluation.
var (
	ErrExactConfigRequired  = errors.New("exact config is required")
	ErrInvalidDatePrecision = errors.New("invalid date precision")
)

// ExactMatch evaluates if two transactions match based on exact matching rules.
func ExactMatch(left, right CandidateTransaction, cfg *ExactConfig) (bool, error) {
	if cfg == nil {
		return false, ErrExactConfigRequired
	}

	if !matchAmountAndCurrency(left, right, cfg) {
		return false, nil
	}

	if !matchBaseAmountAndCurrency(left, right, cfg) {
		return false, nil
	}

	matched, err := matchDate(left, right, cfg)
	if err != nil || !matched {
		return matched, err
	}

	if !matchReference(left, right, cfg) {
		return false, nil
	}

	return true, nil
}

func matchAmountAndCurrency(left, right CandidateTransaction, cfg *ExactConfig) bool {
	if cfg.MatchAmount && !left.Amount.Equal(right.Amount) {
		return false
	}

	if cfg.MatchCurrency && left.Currency != right.Currency {
		return false
	}

	return true
}

func matchBaseAmountAndCurrency(left, right CandidateTransaction, cfg *ExactConfig) bool {
	if cfg.MatchBaseAmount {
		if left.AmountBase == nil || right.AmountBase == nil {
			return false
		}

		if !left.AmountBase.Equal(*right.AmountBase) {
			return false
		}
	}

	if cfg.MatchBaseCurrency {
		if left.CurrencyBase == "" || right.CurrencyBase == "" {
			return false
		}

		if left.CurrencyBase != right.CurrencyBase {
			return false
		}
	}

	return true
}

func matchDate(left, right CandidateTransaction, cfg *ExactConfig) (bool, error) {
	if !cfg.MatchDate {
		return true, nil
	}

	switch cfg.DatePrecision {
	case DatePrecisionDay:
		return DayUTC(left.Date).Equal(DayUTC(right.Date)), nil
	case DatePrecisionTimestamp:
		return left.Date.UTC().Equal(right.Date.UTC()), nil
	default:
		return false, ErrInvalidDatePrecision
	}
}

func matchReference(left, right CandidateTransaction, cfg *ExactConfig) bool {
	if !cfg.MatchReference {
		return true
	}

	leftRef := strings.TrimSpace(left.Reference)
	rightRef := strings.TrimSpace(right.Reference)

	if cfg.ReferenceMustSet && (leftRef == "" || rightRef == "") {
		return false
	}

	if cfg.CaseInsensitive {
		return strings.EqualFold(leftRef, rightRef)
	}

	return leftRef == rightRef
}
