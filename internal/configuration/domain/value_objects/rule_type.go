package value_objects

import (
	"fmt"

	shared "github.com/LerianStudio/matcher/internal/shared/domain"
)

// RuleType is an alias to the shared kernel RuleType.
type RuleType = shared.RuleType

// Re-export constants.
const (
	RuleTypeExact     RuleType = shared.RuleTypeExact
	RuleTypeTolerance RuleType = shared.RuleTypeTolerance
	RuleTypeDateLag   RuleType = shared.RuleTypeDateLag
)

// ErrInvalidRuleType re-exports the shared kernel invalid rule type error.
var ErrInvalidRuleType = shared.ErrInvalidRuleType

// ParseRuleType parses a string into a RuleType.
func ParseRuleType(s string) (RuleType, error) {
	rt, err := shared.ParseRuleType(s)
	if err != nil {
		return "", fmt.Errorf("parsing rule type: %w", err)
	}

	return rt, nil
}
