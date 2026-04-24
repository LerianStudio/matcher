// Copyright 2025 Lerian Studio. All rights reserved.
// Use of this source code is governed by an Elastic License 2.0
// that can be found in the LICENSE.md file.

package value_objects

import (
	"errors"
	"strings"
	"time"

	"github.com/shopspring/decimal"
)

var (
	// ErrFXRateNotPositive indicates the FX rate must be positive.
	ErrFXRateNotPositive = errors.New("fx rate must be positive")
	// ErrFXRateSourceRequired indicates a source is required for FX rates.
	ErrFXRateSourceRequired = errors.New("fx rate source is required")
	// ErrFXRateEffectiveDateRequired indicates an effective date is required for FX rates.
	ErrFXRateEffectiveDateRequired = errors.New("fx rate effective date is required")
)

// FXRate is a small immutable value object used by matching when FX is needed.
// This task does not fetch rates at runtime; this VO exists to validate/transport a rate.
//
// Semantics assumed by conversion helper: baseAmount = amount * rate.
type FXRate struct {
	rate        decimal.Decimal
	source      string
	effectiveAt time.Time
}

// validateFXRateFields checks the FX rate values and returns an error if invalid.
func validateFXRateFields(rate decimal.Decimal, source string, effectiveAt time.Time) error {
	if rate.LessThanOrEqual(decimal.Zero) {
		return ErrFXRateNotPositive
	}

	if strings.TrimSpace(source) == "" {
		return ErrFXRateSourceRequired
	}

	if effectiveAt.IsZero() {
		return ErrFXRateEffectiveDateRequired
	}

	return nil
}

// NewFXRate validates and returns a new FXRate value object.
func NewFXRate(rate decimal.Decimal, source string, effectiveAt time.Time) (FXRate, error) {
	if err := validateFXRateFields(rate, source, effectiveAt); err != nil {
		return FXRate{}, err
	}

	return FXRate{
		rate:        rate,
		source:      strings.TrimSpace(source),
		effectiveAt: effectiveAt.UTC(),
	}, nil
}

// Rate returns the FX rate value.
func (r FXRate) Rate() decimal.Decimal {
	return r.rate
}

// Source returns the FX rate source identifier.
func (r FXRate) Source() string {
	return r.source
}

// EffectiveAt returns the FX rate effective timestamp.
func (r FXRate) EffectiveAt() time.Time {
	return r.effectiveAt
}

// IsValid returns true if the FX rate has valid values.
// A valid FX rate must have a positive rate, non-empty source, and non-zero effective date.
func (r FXRate) IsValid() bool {
	return validateFXRateFields(r.rate, r.source, r.effectiveAt) == nil
}
