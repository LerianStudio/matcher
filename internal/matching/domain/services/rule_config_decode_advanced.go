// Copyright 2025 Lerian Studio. All rights reserved.
// Use of this source code is governed by an Elastic License 2.0
// that can be found in the LICENSE.md file.

package services

import (
	"github.com/shopspring/decimal"
)

func decodeAllocationConfig(configMap map[string]any) (*AllocationConfig, error) {
	cfg, err := initAllocationConfig(configMap)
	if err != nil {
		return nil, err
	}

	if err := decodeAllocationDirection(configMap, &cfg); err != nil {
		return nil, err
	}

	if err := decodeAllocationToleranceMode(configMap, &cfg); err != nil {
		return nil, err
	}

	return &cfg, nil
}

func initAllocationConfig(configMap map[string]any) (AllocationConfig, error) {
	allowPartial, err := getBool(configMap, "allowPartial", false)
	if err != nil {
		return AllocationConfig{}, err
	}

	useBaseAmount, err := getBool(configMap, "allocationUseBaseAmount", false)
	if err != nil {
		return AllocationConfig{}, err
	}

	toleranceValue, err := getDecimal(configMap, "allocationToleranceValue", decimal.Zero)
	if err != nil {
		return AllocationConfig{}, err
	}

	if toleranceValue.IsNegative() {
		return AllocationConfig{}, wrapDecodeErr("allocationToleranceValue must be non-negative")
	}

	return AllocationConfig{
		AllowPartial:   allowPartial,
		Direction:      AllocationDirectionLeftToRight,
		ToleranceMode:  AllocationToleranceAbsolute,
		ToleranceValue: toleranceValue,
		UseBaseAmount:  useBaseAmount,
	}, nil
}

func decodeAllocationDirection(configMap map[string]any, cfg *AllocationConfig) error {
	value, ok := configMap["allocationDirection"]
	if !ok {
		return nil
	}

	strVal, ok := value.(string)
	if !ok {
		return wrapDecodeErr("allocationDirection must be string")
	}

	switch AllocationDirection(strVal) {
	case AllocationDirectionLeftToRight, AllocationDirectionRightToLeft:
		cfg.Direction = AllocationDirection(strVal)
	default:
		return wrapDecodeErr("invalid allocationDirection")
	}

	return nil
}

func decodeAllocationToleranceMode(configMap map[string]any, cfg *AllocationConfig) error {
	value, ok := configMap["allocationToleranceMode"]
	if !ok {
		return nil
	}

	strVal, ok := value.(string)
	if !ok {
		return wrapDecodeErr("allocationToleranceMode must be string")
	}

	switch AllocationToleranceMode(strVal) {
	case AllocationToleranceAbsolute, AllocationTolerancePercent:
		cfg.ToleranceMode = AllocationToleranceMode(strVal)
	default:
		return wrapDecodeErr("invalid allocationToleranceMode")
	}

	return nil
}

func decodeDateLagConfig(configMap map[string]any) (DateLagConfig, error) {
	cfg, err := initDateLagConfig(configMap)
	if err != nil {
		return DateLagConfig{}, err
	}

	if err := validateDateLagBounds(cfg); err != nil {
		return DateLagConfig{}, err
	}

	if err := decodeDateLagDirection(configMap, &cfg); err != nil {
		return DateLagConfig{}, err
	}

	if err := decodeDateLagMatchFields(configMap, &cfg); err != nil {
		return DateLagConfig{}, err
	}

	return cfg, nil
}

func initDateLagConfig(configMap map[string]any) (DateLagConfig, error) {
	minDays, err := getInt(configMap, "minDays", 0)
	if err != nil {
		return DateLagConfig{}, err
	}

	maxDays, err := getInt(configMap, "maxDays", 0)
	if err != nil {
		return DateLagConfig{}, err
	}

	inclusive, err := getBool(configMap, "inclusive", true)
	if err != nil {
		return DateLagConfig{}, err
	}

	feeTolerance, err := getDecimal(configMap, "feeTolerance", decimal.Zero)
	if err != nil {
		return DateLagConfig{}, err
	}

	if feeTolerance.IsNegative() {
		return DateLagConfig{}, wrapDecodeErr("feeTolerance must be non-negative")
	}

	return DateLagConfig{
		MinDays:       minDays,
		MaxDays:       maxDays,
		Inclusive:     inclusive,
		Direction:     DateLagDirectionAbs,
		FeeTolerance:  feeTolerance,
		MatchScore:    defaultDateLagScore,
		MatchCurrency: true,
	}, nil
}

func validateDateLagBounds(cfg DateLagConfig) error {
	if cfg.MinDays < 0 || cfg.MaxDays < 0 {
		return wrapDecodeErr("minDays/maxDays must be >= 0")
	}

	if cfg.MaxDays < cfg.MinDays {
		return wrapDecodeErr("maxDays must be >= minDays")
	}

	if cfg.MinDays > maxDateLagDays || cfg.MaxDays > maxDateLagDays {
		return wrapDecodeErr("date lag days exceed maximum")
	}

	// Reject Inclusive=false with MinDays=0: this combination excludes same-day
	// transactions (diff=0 fails diff > 0), which is almost certainly a misconfiguration.
	// Use Inclusive=true with MinDays=0 to include same-day transactions.
	if !cfg.Inclusive && cfg.MinDays == 0 {
		return wrapDecodeErr(
			"exclusive bounds (inclusive=false) with minDays=0 excludes same-day transactions; " +
				"use inclusive=true with minDays=0 to include same-day matches, " +
				"or set minDays=1 for exclusive bounds starting from 1 day",
		)
	}

	return nil
}

func decodeDateLagDirection(configMap map[string]any, cfg *DateLagConfig) error {
	value, ok := configMap["direction"]
	if !ok {
		return nil
	}

	strVal, ok := value.(string)
	if !ok {
		return wrapDecodeErr("direction must be string")
	}

	switch DateLagDirection(strVal) {
	case DateLagDirectionAbs, DateLagDirectionLeftBeforeRight, DateLagDirectionRightBeforeLeft:
		cfg.Direction = DateLagDirection(strVal)
	default:
		return wrapDecodeErr("invalid direction")
	}

	return nil
}

func decodeDateLagMatchFields(configMap map[string]any, cfg *DateLagConfig) error {
	var err error

	cfg.MatchCurrency, err = getBool(configMap, "matchCurrency", cfg.MatchCurrency)
	if err != nil {
		return err
	}

	cfg.MatchScore, err = getInt(configMap, "matchScore", cfg.MatchScore)
	if err != nil {
		return err
	}

	return validateScore(cfg.MatchScore)
}
