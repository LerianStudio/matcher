// Copyright 2025 Lerian Studio. All rights reserved.
// Use of this source code is governed by an Elastic License 2.0
// that can be found in the LICENSE.md file.

package services

import (
	"errors"

	shared "github.com/LerianStudio/matcher/internal/shared/domain"
)

// ErrRuleConfigDecode is returned when rule configuration decoding fails.
var ErrRuleConfigDecode = errors.New("rule config decode error")

// Static errors for parseIntValue to satisfy err113 linter.
var (
	errOutOfRange = errors.New("out of range")
	errMustBeInt  = errors.New("must be integer")
)

// ValidateRuleConfig validates that a rule's config matches the expected schema for its type.
// This function can be used during rule creation/update to ensure invalid rules are rejected early.
func ValidateRuleConfig(ruleType shared.RuleType, config map[string]any) error {
	if config == nil {
		return wrapDecodeErr("config is nil")
	}

	if len(config) == 0 {
		return wrapDecodeErr("config is empty")
	}

	rule := &shared.MatchRule{
		Type:   ruleType,
		Config: config,
	}

	_, err := DecodeRuleDefinition(rule)
	if err != nil {
		return err
	}

	return nil
}

// Default values for rule configuration.
const (
	maxRoundingScale      = 10
	maxDateWindowDays     = 3650
	maxDateLagDays        = 3650
	defaultExactScore     = 100
	defaultExactBaseScore = 90
	defaultToleranceScore = 85
	defaultToleranceBase  = 80
	defaultDateLagScore   = 80
	defaultRoundingScale  = 2
)

// DecodeRuleDefinition decodes a shared MatchRule into a RuleDefinition.
func DecodeRuleDefinition(rule *shared.MatchRule) (RuleDefinition, error) {
	def, err := RuleDefinitionFromMatchRule(rule)
	if err != nil {
		return RuleDefinition{}, err
	}

	switch def.Type {
	case shared.RuleTypeExact:
		return decodeExactRule(rule.Config, def)
	case shared.RuleTypeTolerance:
		return decodeToleranceRule(rule.Config, def)
	case shared.RuleTypeDateLag:
		return decodeDateLagRule(rule.Config, def)
	default:
		return RuleDefinition{}, ErrUnsupportedRuleType
	}
}

func decodeExactRule(config map[string]any, def RuleDefinition) (RuleDefinition, error) {
	cfg, err := decodeExactConfig(config)
	if err != nil {
		return RuleDefinition{}, err
	}

	def.Exact = &cfg

	def.Allocation, err = decodeAllocationConfig(config)
	if err != nil {
		return RuleDefinition{}, err
	}

	alignBaseAmountSettings(config, def.Allocation, &cfg.MatchBaseAmount, &cfg.MatchBaseCurrency)

	return def, nil
}

func decodeToleranceRule(config map[string]any, def RuleDefinition) (RuleDefinition, error) {
	cfg, err := decodeToleranceConfig(config)
	if err != nil {
		return RuleDefinition{}, err
	}

	def.Tolerance = &cfg

	def.Allocation, err = decodeAllocationConfig(config)
	if err != nil {
		return RuleDefinition{}, err
	}

	alignBaseAmountSettings(config, def.Allocation, &cfg.MatchBaseAmount, &cfg.MatchBaseCurrency)

	return def, nil
}

func decodeDateLagRule(config map[string]any, def RuleDefinition) (RuleDefinition, error) {
	cfg, err := decodeDateLagConfig(config)
	if err != nil {
		return RuleDefinition{}, err
	}

	def.DateLag = &cfg

	def.Allocation, err = decodeAllocationConfig(config)
	if err != nil {
		return RuleDefinition{}, err
	}

	return def, nil
}

func alignBaseAmountSettings(
	config map[string]any,
	allocation *AllocationConfig,
	matchBaseAmount *bool,
	matchBaseCurrency *bool,
) {
	// Bidirectional alignment: allocation.UseBaseAmount ↔ matching.MatchBaseAmount
	// If allocation explicitly uses base amounts, enable base matching
	if allocation != nil && allocation.UseBaseAmount {
		if matchBaseAmount != nil {
			*matchBaseAmount = true
		}

		if matchBaseCurrency != nil {
			*matchBaseCurrency = true
		}
	}

	// If base matching is enabled and allocationUseBaseAmount wasn't explicitly set to false,
	// align allocation to also use base amounts for consistency
	if allocation != nil && matchBaseAmount != nil && *matchBaseAmount &&
		!hasExplicitFalse(config, "allocationUseBaseAmount") {
		allocation.UseBaseAmount = true
	}
}
