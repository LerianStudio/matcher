// Copyright 2025 Lerian Studio. All rights reserved.
// Use of this source code is governed by an Elastic License 2.0
// that can be found in the LICENSE.md file.

package value_objects

import (
	"errors"
	"strings"
)

// ErrInvalidAdjustmentReason is returned when parsing an invalid adjustment reason.
var ErrInvalidAdjustmentReason = errors.New("invalid adjustment reason")

// AdjustmentReasonCode represents reasons for manual entry adjustments.
type AdjustmentReasonCode string

// AdjustmentReasonCode values.
const (
	AdjustmentReasonAmountCorrection   AdjustmentReasonCode = "AMOUNT_CORRECTION"
	AdjustmentReasonCurrencyCorrection AdjustmentReasonCode = "CURRENCY_CORRECTION"
	AdjustmentReasonDateCorrection     AdjustmentReasonCode = "DATE_CORRECTION"
	AdjustmentReasonOther              AdjustmentReasonCode = "OTHER"
)

// String returns the string representation of the AdjustmentReasonCode.
func (reason AdjustmentReasonCode) String() string {
	return string(reason)
}

// IsValid checks if the adjustment reason is valid.
func (reason AdjustmentReasonCode) IsValid() bool {
	switch reason {
	case AdjustmentReasonAmountCorrection,
		AdjustmentReasonCurrencyCorrection,
		AdjustmentReasonDateCorrection,
		AdjustmentReasonOther:
		return true
	default:
		return false
	}
}

// ParseAdjustmentReason parses a string into an AdjustmentReasonCode.
func ParseAdjustmentReason(value string) (AdjustmentReasonCode, error) {
	trimmed := strings.TrimSpace(value)
	if trimmed == "" {
		return "", ErrInvalidAdjustmentReason
	}

	reason := AdjustmentReasonCode(strings.ToUpper(trimmed))
	if !reason.IsValid() {
		return "", ErrInvalidAdjustmentReason
	}

	return reason, nil
}
