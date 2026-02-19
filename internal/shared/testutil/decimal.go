// Package testutil provides testing utilities for the matcher project.
package testutil

import "github.com/shopspring/decimal"

// DecimalPtr returns a pointer to the provided decimal value.
func DecimalPtr(value decimal.Decimal) *decimal.Decimal {
	return &value
}
