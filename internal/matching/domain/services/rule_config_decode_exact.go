// Copyright 2025 Lerian Studio. All rights reserved.
// Use of this source code is governed by an Elastic License 2.0
// that can be found in the LICENSE.md file.

package services

func decodeExactConfig(configMap map[string]any) (ExactConfig, error) {
	cfg := defaultExactConfig()

	if err := decodeExactBoolFields(configMap, &cfg); err != nil {
		return ExactConfig{}, err
	}

	if err := decodeExactScoreFields(configMap, &cfg); err != nil {
		return ExactConfig{}, err
	}

	if err := decodeExactDatePrecision(configMap, &cfg); err != nil {
		return ExactConfig{}, err
	}

	return cfg, nil
}

func defaultExactConfig() ExactConfig {
	return ExactConfig{
		MatchAmount:       true,
		MatchCurrency:     true,
		MatchDate:         true,
		DatePrecision:     DatePrecisionDay,
		MatchReference:    true,
		CaseInsensitive:   true,
		MatchBaseAmount:   false,
		MatchBaseCurrency: false,
		MatchScore:        defaultExactScore,
		MatchBaseScore:    defaultExactBaseScore,
	}
}

func decodeExactBoolFields(configMap map[string]any, cfg *ExactConfig) error {
	var err error

	cfg.MatchAmount, err = getBool(configMap, "matchAmount", cfg.MatchAmount)
	if err != nil {
		return err
	}

	cfg.MatchCurrency, err = getBool(configMap, "matchCurrency", cfg.MatchCurrency)
	if err != nil {
		return err
	}

	cfg.MatchDate, err = getBool(configMap, "matchDate", cfg.MatchDate)
	if err != nil {
		return err
	}

	cfg.MatchReference, err = getBool(configMap, "matchReference", cfg.MatchReference)
	if err != nil {
		return err
	}

	cfg.CaseInsensitive, err = getBool(configMap, "caseInsensitive", cfg.CaseInsensitive)
	if err != nil {
		return err
	}

	cfg.ReferenceMustSet, err = getBool(configMap, "referenceMustSet", false)
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

	allocationUseBaseAmount, err := getBool(configMap, "allocationUseBaseAmount", false)
	if err != nil {
		return err
	}

	if allocationUseBaseAmount {
		cfg.MatchBaseAmount = true
		cfg.MatchBaseCurrency = true
	}

	return nil
}

func decodeExactScoreFields(configMap map[string]any, cfg *ExactConfig) error {
	var err error

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

	return nil
}

func decodeExactDatePrecision(configMap map[string]any, cfg *ExactConfig) error {
	value, ok := configMap["datePrecision"]
	if !ok {
		return nil
	}

	strVal, ok := value.(string)
	if !ok {
		return wrapDecodeErr("datePrecision must be string")
	}

	switch DatePrecision(strVal) {
	case DatePrecisionDay, DatePrecisionTimestamp:
		cfg.DatePrecision = DatePrecision(strVal)
	default:
		return wrapDecodeErr("invalid datePrecision")
	}

	return nil
}
