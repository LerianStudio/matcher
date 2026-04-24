// Copyright 2025 Lerian Studio. All rights reserved.
// Use of this source code is governed by an Elastic License 2.0
// that can be found in the LICENSE.md file.

// Package testutil provides testing utilities for the matcher project.
package testutil

import "github.com/shopspring/decimal"

// DecimalPtr returns a pointer to the provided decimal value.
func DecimalPtr(value decimal.Decimal) *decimal.Decimal {
	return &value
}
