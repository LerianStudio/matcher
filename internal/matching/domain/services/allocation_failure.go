// Copyright 2025 Lerian Studio. All rights reserved.
// Use of this source code is governed by an Elastic License 2.0
// that can be found in the LICENSE.md file.

package services

import "github.com/google/uuid"

// AllocationFailureCode represents the reason an allocation could not be completed.
type AllocationFailureCode string

const (
	// AllocationFailureFXRateUnavailable indicates base amount/currency fields are missing.
	AllocationFailureFXRateUnavailable AllocationFailureCode = "FX_RATE_UNAVAILABLE"
	// AllocationFailureSplitIncomplete indicates allocation did not fully cover the target amount.
	AllocationFailureSplitIncomplete AllocationFailureCode = "SPLIT_INCOMPLETE"
)

const (
	// AllocationFailureMetaMissingKey identifies the missing base field in metadata.
	AllocationFailureMetaMissingKey = "missing"
	// AllocationFailureMissingAmountBase indicates the base amount is missing.
	AllocationFailureMissingAmountBase = "amount_base"
	// AllocationFailureMissingCurrencyBase indicates the base currency is missing.
	AllocationFailureMissingCurrencyBase = "currency_base"

	boolTrue  = "true"
	boolFalse = "false"
)

// AllocationFailure captures structured information about why an allocation could not be completed.
type AllocationFailure struct {
	Code     AllocationFailureCode
	TargetID uuid.UUID
	Meta     map[string]string
}

// NewFXRateUnavailableFailure creates a failure for missing base amount/currency.
func NewFXRateUnavailableFailure(targetID uuid.UUID, missing string) *AllocationFailure {
	return &AllocationFailure{
		Code:     AllocationFailureFXRateUnavailable,
		TargetID: targetID,
		Meta:     map[string]string{AllocationFailureMetaMissingKey: missing},
	}
}

// NewSplitIncompleteFailure creates a failure for incomplete allocation coverage.
func NewSplitIncompleteFailure(
	targetID uuid.UUID,
	expectedTotal, allocatedTotal, gap, currency string,
	useBaseAmount, allowPartial bool,
) *AllocationFailure {
	return &AllocationFailure{
		Code:     AllocationFailureSplitIncomplete,
		TargetID: targetID,
		Meta: map[string]string{
			"expected_total":  expectedTotal,
			"allocated_total": allocatedTotal,
			"gap":             gap,
			"currency":        currency,
			"use_base_amount": boolToString(useBaseAmount),
			"allow_partial":   boolToString(allowPartial),
		},
	}
}

func boolToString(b bool) string {
	if b {
		return boolTrue
	}

	return boolFalse
}
