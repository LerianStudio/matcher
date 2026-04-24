// Copyright 2025 Lerian Studio. All rights reserved.
// Use of this source code is governed by an Elastic License 2.0
// that can be found in the LICENSE.md file.

package services

import (
	"strings"

	"github.com/shopspring/decimal"
)

func decodeToleranceConfig(configMap map[string]any) (ToleranceConfig, error) {
	cfg, err := initToleranceConfig(configMap)
	if err != nil {
		return ToleranceConfig{}, err
	}

	if err := validateToleranceBounds(cfg); err != nil {
		return ToleranceConfig{}, err
	}

	if err := decodeToleranceRoundingMode(configMap, &cfg); err != nil {
		return ToleranceConfig{}, err
	}

	if err := decodeTolerancePercentageBase(configMap, &cfg); err != nil {
		return ToleranceConfig{}, err
	}

	if err := decodeToleranceAmounts(configMap, &cfg); err != nil {
		return ToleranceConfig{}, err
	}

	if err := decodeToleranceMatchFields(configMap, &cfg); err != nil {
		return ToleranceConfig{}, err
	}

	return cfg, nil
}

func initToleranceConfig(configMap map[string]any) (ToleranceConfig, error) {
	absDefault, err := decimal.NewFromString("0.50")
	if err != nil {
		return ToleranceConfig{}, wrapDecodeErr("invalid default absTolerance")
	}

	pctDefault, err := decimal.NewFromString("0.005")
	if err != nil {
		return ToleranceConfig{}, wrapDecodeErr("invalid default percentTolerance")
	}

	dateWindowDays, err := getInt(configMap, "dateWindowDays", 0)
	if err != nil {
		return ToleranceConfig{}, err
	}

	roundingScale, err := getInt(configMap, "roundingScale", defaultRoundingScale)
	if err != nil {
		return ToleranceConfig{}, err
	}

	return ToleranceConfig{
		MatchCurrency:      true,
		DateWindowDays:     dateWindowDays,
		RoundingScale:      roundingScale,
		RoundingMode:       RoundingHalfUp,
		AbsAmountTolerance: absDefault,
		PercentTolerance:   pctDefault,
		MatchBaseAmount:    false,
		MatchBaseCurrency:  false,
		MatchScore:         defaultToleranceScore,
		MatchBaseScore:     defaultToleranceBase,
		PercentageBase:     TolerancePercentageBaseMax,
	}, nil
}

func validateToleranceBounds(cfg ToleranceConfig) error {
	if cfg.DateWindowDays < 0 {
		return wrapDecodeErr("dateWindowDays must be >= 0")
	}

	if cfg.DateWindowDays > maxDateWindowDays {
		return wrapDecodeErr("dateWindowDays exceeds maximum")
	}

	if cfg.RoundingScale < 0 {
		return wrapDecodeErr("roundingScale must be >= 0")
	}

	if cfg.RoundingScale > maxRoundingScale {
		return wrapDecodeErr("roundingScale exceeds maximum")
	}

	return nil
}

func decodeToleranceRoundingMode(configMap map[string]any, cfg *ToleranceConfig) error {
	value, ok := configMap["roundingMode"]
	if !ok {
		return nil
	}

	strVal, ok := value.(string)
	if !ok {
		return wrapDecodeErr("roundingMode must be string")
	}

	switch RoundingMode(strVal) {
	case RoundingHalfUp, RoundingBankers, RoundingFloor, RoundingCeil, RoundingTruncate:
		cfg.RoundingMode = RoundingMode(strVal)
	default:
		return wrapDecodeErr("invalid roundingMode")
	}

	return nil
}

func decodeTolerancePercentageBase(configMap map[string]any, cfg *ToleranceConfig) error {
	value, ok := configMap["percentageBase"]
	if !ok {
		return nil // Use default (MAX)
	}

	strVal, ok := value.(string)
	if !ok {
		return wrapDecodeErr("percentageBase must be string")
	}

	normalized := TolerancePercentageBase(strings.ToUpper(strVal))

	switch normalized {
	case TolerancePercentageBaseMax, TolerancePercentageBaseMin,
		TolerancePercentageBaseAvg, TolerancePercentageBaseLeft,
		TolerancePercentageBaseRight:
		cfg.PercentageBase = normalized
	default:
		return wrapDecodeErr("invalid percentageBase: must be MAX, MIN, AVERAGE, LEFT, or RIGHT")
	}

	return nil
}

func decodeToleranceAmounts(configMap map[string]any, cfg *ToleranceConfig) error {
	absTol, err := getDecimal(configMap, "absTolerance", cfg.AbsAmountTolerance)
	if err != nil {
		return err
	}

	pctTol, err := getDecimal(configMap, "percentTolerance", cfg.PercentTolerance)
	if err != nil {
		return err
	}

	if absTol.IsNegative() {
		return wrapDecodeErr("absTolerance must be non-negative")
	}

	if pctTol.IsNegative() {
		return wrapDecodeErr("percentTolerance must be non-negative")
	}

	cfg.AbsAmountTolerance = absTol
	cfg.PercentTolerance = pctTol

	return nil
}

func decodeToleranceMatchFields(configMap map[string]any, cfg *ToleranceConfig) error {
	var err error

	cfg.MatchCurrency, err = getBool(configMap, "matchCurrency", cfg.MatchCurrency)
	if err != nil {
		return err
	}

	cfg.MatchBaseAmount, err = getBool(configMap, "matchBaseAmount", cfg.MatchBaseAmount)
	if err != nil {
		return err
	}

	cfg.MatchBaseCurrency, err = getBool(configMap, "matchBaseCurrency", cfg.MatchBaseCurrency)
	if err != nil {
		return err
	}

	cfg.MatchScore, err = getInt(configMap, "matchScore", cfg.MatchScore)
	if err != nil {
		return err
	}

	cfg.MatchBaseScore, err = getInt(configMap, "matchBaseScore", cfg.MatchBaseScore)
	if err != nil {
		return err
	}

	if err := validateScore(cfg.MatchScore); err != nil {
		return err
	}

	if cfg.MatchBaseAmount || cfg.MatchBaseCurrency {
		if err := validateScore(cfg.MatchBaseScore); err != nil {
			return err
		}
	}

	if err := decodeToleranceReferenceFields(configMap, cfg); err != nil {
		return err
	}

	return nil
}

// decodeToleranceReferenceFields parses reference matching fields for tolerance rules.
// Defaults match ExactConfig for consistency: matchReference=true, caseInsensitive=true.
func decodeToleranceReferenceFields(configMap map[string]any, cfg *ToleranceConfig) error {
	var err error

	cfg.MatchReference, err = getBool(configMap, "matchReference", true)
	if err != nil {
		return err
	}

	cfg.CaseInsensitive, err = getBool(configMap, "caseInsensitive", true)
	if err != nil {
		return err
	}

	cfg.ReferenceMustSet, err = getBool(configMap, "referenceMustSet", false)
	if err != nil {
		return err
	}

	return nil
}
