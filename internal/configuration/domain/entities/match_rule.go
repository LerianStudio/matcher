// Copyright 2025 Lerian Studio. All rights reserved.
// Use of this source code is governed by an Elastic License 2.0
// that can be found in the LICENSE.md file.

// Package entities holds configuration domain entities.
package entities

import (
	"context"
	"errors"
	"fmt"

	"github.com/google/uuid"

	shared "github.com/LerianStudio/matcher/internal/shared/domain"
)

// Cross-aggregate constraint errors.
var (
	// ErrRulePriorityConflict is returned when the priority already exists in context.
	ErrRulePriorityConflict = errors.New("priority already exists in context")
)

// ErrRuleConfigMissingRequiredKeys is returned when a rule config lacks recognized keys for its type.
var ErrRuleConfigMissingRequiredKeys = errors.New("rule config missing recognized keys for rule type")

// MatchRule is an alias to the shared kernel MatchRule.
type MatchRule = shared.MatchRule

// MatchRules is a slice of MatchRule pointers.
type MatchRules = shared.MatchRules

// RuleType is an alias to the shared kernel RuleType.
type RuleType = shared.RuleType

// CreateMatchRuleInput is an alias to the shared kernel CreateMatchRuleInput.
type CreateMatchRuleInput = shared.CreateMatchRuleInput

// UpdateMatchRuleInput is an alias to the shared kernel UpdateMatchRuleInput.
type UpdateMatchRuleInput = shared.UpdateMatchRuleInput

// Re-export shared sentinel errors for compatibility.
var (
	ErrMatchRuleNil        = shared.ErrMatchRuleNil
	ErrRuleContextRequired = shared.ErrRuleContextRequired
	ErrRulePriorityInvalid = shared.ErrRulePriorityInvalid
	ErrRuleTypeInvalid     = shared.ErrRuleTypeInvalid
	ErrRuleConfigRequired  = shared.ErrRuleConfigRequired
)

// NewMatchRule validates input and returns a new match rule entity.
func NewMatchRule(
	ctx context.Context,
	contextID uuid.UUID,
	input CreateMatchRuleInput,
) (*MatchRule, error) {
	rule, err := shared.NewMatchRule(ctx, contextID, input)
	if err != nil {
		return nil, fmt.Errorf("creating match rule: %w", err)
	}

	return rule, nil
}

// knownExactKeys are the recognized config keys for EXACT rule type.
var knownExactKeys = map[string]bool{
	"matchAmount": true, "matchCurrency": true, "matchDate": true,
	"matchReference": true, "caseInsensitive": true, "referenceMustSet": true,
	"matchBaseAmount": true, "matchBaseCurrency": true,
	"datePrecision": true, "matchScore": true, "matchBaseScore": true,
	"allowPartial": true, "allocationDirection": true,
	"allocationToleranceMode": true, "allocationToleranceValue": true,
	"allocationUseBaseAmount": true,
}

// knownToleranceKeys are the recognized config keys for TOLERANCE rule type.
var knownToleranceKeys = map[string]bool{
	"absTolerance": true, "percentTolerance": true,
	"dateWindowDays": true, "roundingScale": true, "roundingMode": true,
	"percentageBase": true, "matchCurrency": true,
	"matchBaseAmount": true, "matchBaseCurrency": true,
	"matchScore": true, "matchBaseScore": true,
	"allowPartial": true, "allocationDirection": true,
	"allocationToleranceMode": true, "allocationToleranceValue": true,
	"allocationUseBaseAmount": true,
}

// knownDateLagKeys are the recognized config keys for DATE_LAG rule type.
var knownDateLagKeys = map[string]bool{
	"minDays": true, "maxDays": true, "inclusive": true,
	"direction": true, "feeTolerance": true,
	"matchCurrency": true, "matchScore": true,
	"allowPartial": true, "allocationDirection": true,
	"allocationToleranceMode": true, "allocationToleranceValue": true,
	"allocationUseBaseAmount": true,
}

// ValidateMatchRuleConfig validates match rule config against rule type schema.
//
// This function performs structural validation:
//   - Config is non-empty
//   - Rule type is valid
//   - Config contains at least one recognized key for the given rule type
//
// Full schema validation (field types, value ranges, detailed constraints per rule type)
// is performed by the matching context's DecodeRuleDefinition at match execution time.
// This separation is intentional to avoid coupling the configuration domain to
// matching-specific rule decoder logic.
//
// Architecture Note: Moving full validation here would require either:
// 1. Moving the entire rule decoder to shared kernel (significant refactor), or
// 2. Defining a port interface that matching implements (adds complexity).
//
// The current approach catches obviously misconfigured rules (completely unrecognized keys)
// while deferring detailed value validation to the matching context.
func ValidateMatchRuleConfig(ruleType shared.RuleType, config map[string]any) error {
	if len(config) == 0 {
		return ErrRuleConfigRequired
	}

	if !ruleType.Valid() {
		return ErrRuleTypeInvalid
	}

	if err := validateConfigKeysForType(ruleType, config); err != nil {
		return err
	}

	return nil
}

// validateConfigKeysForType checks that the config contains at least one recognized
// key for the given rule type, catching obvious misconfiguration early.
func validateConfigKeysForType(ruleType shared.RuleType, config map[string]any) error {
	knownKeys := knownKeysForType(ruleType)
	if knownKeys == nil {
		// Unknown rule type; skip key validation (already caught by type check above).
		return nil
	}

	for key := range config {
		if knownKeys[key] {
			return nil
		}
	}

	return fmt.Errorf("%w: %s expects keys like %s",
		ErrRuleConfigMissingRequiredKeys, ruleType, exampleKeysForType(ruleType))
}

// knownKeysForType returns the set of recognized config keys for a rule type.
func knownKeysForType(ruleType shared.RuleType) map[string]bool {
	switch ruleType {
	case shared.RuleTypeExact:
		return knownExactKeys
	case shared.RuleTypeTolerance:
		return knownToleranceKeys
	case shared.RuleTypeDateLag:
		return knownDateLagKeys
	default:
		return nil
	}
}

// exampleKeysForType returns example config keys for error messages.
func exampleKeysForType(ruleType shared.RuleType) string {
	switch ruleType {
	case shared.RuleTypeExact:
		return "matchAmount, matchCurrency, matchDate"
	case shared.RuleTypeTolerance:
		return "absTolerance, percentTolerance, dateWindowDays"
	case shared.RuleTypeDateLag:
		return "minDays, maxDays, direction"
	default:
		return ""
	}
}
