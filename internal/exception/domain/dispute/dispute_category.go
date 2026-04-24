// Copyright 2025 Lerian Studio. All rights reserved.
// Use of this source code is governed by an Elastic License 2.0
// that can be found in the LICENSE.md file.

package dispute

import (
	"errors"
	"strings"
)

// ErrInvalidDisputeCategory is returned when parsing an invalid dispute category.
var ErrInvalidDisputeCategory = errors.New("invalid dispute category")

// DisputeCategory represents the type/reason of a dispute.
type DisputeCategory string

// DisputeCategory values.
const (
	DisputeCategoryBankFeeError         DisputeCategory = "BANK_FEE_ERROR"
	DisputeCategoryUnrecognizedCharge   DisputeCategory = "UNRECOGNIZED_CHARGE"
	DisputeCategoryDuplicateTransaction DisputeCategory = "DUPLICATE_TRANSACTION"
	DisputeCategoryAmountMismatch       DisputeCategory = "AMOUNT_MISMATCH"
	DisputeCategoryOther                DisputeCategory = "OTHER"
)

// IsValid checks if the category is valid.
func (category DisputeCategory) IsValid() bool {
	switch category {
	case DisputeCategoryBankFeeError,
		DisputeCategoryUnrecognizedCharge,
		DisputeCategoryDuplicateTransaction,
		DisputeCategoryAmountMismatch,
		DisputeCategoryOther:
		return true
	default:
		return false
	}
}

// String returns the string representation of the category.
func (category DisputeCategory) String() string {
	return string(category)
}

// ParseDisputeCategory parses a string into a DisputeCategory.
func ParseDisputeCategory(value string) (DisputeCategory, error) {
	category := DisputeCategory(strings.ToUpper(value))
	if !category.IsValid() {
		return "", ErrInvalidDisputeCategory
	}

	return category, nil
}
