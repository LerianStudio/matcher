// Copyright 2025 Lerian Studio. All rights reserved.
// Use of this source code is governed by an Elastic License 2.0
// that can be found in the LICENSE.md file.

package services

import (
	"github.com/shopspring/decimal"

	matchingVO "github.com/LerianStudio/matcher/internal/matching/domain/value_objects"
)

// ConvertToBaseAmount converts a transaction amount into base currency.
// Semantics: baseAmount = amount * fxRate.
// Rounding is deterministic and reuses tolerance rounding.
func ConvertToBaseAmount(
	amount decimal.Decimal,
	fxRate matchingVO.FXRate,
	roundingScale int,
	roundingMode RoundingMode,
) (decimal.Decimal, error) {
	base := amount.Mul(fxRate.Rate())

	return roundAmount(base, roundingScale, roundingMode)
}
