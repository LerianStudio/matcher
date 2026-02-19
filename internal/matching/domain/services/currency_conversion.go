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
