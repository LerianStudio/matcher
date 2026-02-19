package services

import (
	"errors"

	"github.com/google/uuid"
	"github.com/shopspring/decimal"

	shared "github.com/LerianStudio/matcher/internal/shared/domain"
)

// Sentinel errors for rule definition operations.
var (
	ErrNilMatchRule        = errors.New("match rule is nil")
	ErrUnsupportedRuleType = errors.New("unsupported rule type")
)

// DatePrecision defines the precision level for date comparisons.
type DatePrecision string

// Date precision options.
const (
	DatePrecisionDay       DatePrecision = "DAY"
	DatePrecisionTimestamp DatePrecision = "TIMESTAMP"
)

// RoundingMode defines how decimal values should be rounded.
type RoundingMode string

// Rounding mode options.
const (
	RoundingHalfUp   RoundingMode = "HALF_UP"
	RoundingBankers  RoundingMode = "BANKERS"
	RoundingFloor    RoundingMode = "FLOOR"
	RoundingCeil     RoundingMode = "CEIL"
	RoundingTruncate RoundingMode = "TRUNCATE"
)

// ExactConfig defines parameters for exact matching rules.
type ExactConfig struct {
	MatchAmount       bool
	MatchCurrency     bool
	MatchDate         bool
	DatePrecision     DatePrecision
	MatchReference    bool
	CaseInsensitive   bool
	ReferenceMustSet  bool
	MatchBaseAmount   bool
	MatchBaseCurrency bool
	MatchScore        int
	MatchBaseScore    int
}

// ToleranceConfig defines parameters for tolerance-based matching rules.
type ToleranceConfig struct {
	MatchCurrency      bool
	DateWindowDays     int
	AbsAmountTolerance decimal.Decimal
	PercentTolerance   decimal.Decimal
	RoundingScale      int
	RoundingMode       RoundingMode
	MatchBaseAmount    bool
	MatchBaseCurrency  bool
	MatchScore         int
	MatchBaseScore     int

	// PercentageBase determines which amount is used as the base for percentage tolerance.
	// Default: MAX (backward compatible). Options: MAX, MIN, AVERAGE, LEFT, RIGHT.
	PercentageBase TolerancePercentageBase

	// Reference matching (optional, disabled by default for backward compatibility).
	// When MatchReference is false, reference matching is skipped and ReferenceScore = 1.0.
	MatchReference   bool
	CaseInsensitive  bool
	ReferenceMustSet bool
}

// DateLagDirection specifies how date differences should be computed.
type DateLagDirection string

// Date lag direction options.
const (
	DateLagDirectionAbs             DateLagDirection = "ABS"
	DateLagDirectionLeftBeforeRight DateLagDirection = "LEFT_BEFORE_RIGHT"
	DateLagDirectionRightBeforeLeft DateLagDirection = "RIGHT_BEFORE_LEFT"
)

// DateLagConfig defines parameters for date-lag matching rules.
type DateLagConfig struct {
	MinDays       int
	MaxDays       int
	Inclusive     bool
	Direction     DateLagDirection
	FeeTolerance  decimal.Decimal
	MatchScore    int
	MatchCurrency bool
}

// AllocationDirection specifies whether left side maps to right or vice versa.
type AllocationDirection string

// AllocationToleranceMode specifies how allocation tolerance is computed.
type AllocationToleranceMode string

// Allocation direction options.
const (
	AllocationDirectionLeftToRight AllocationDirection = "LEFT_TO_RIGHT"
	AllocationDirectionRightToLeft AllocationDirection = "RIGHT_TO_LEFT"
)

// Allocation tolerance mode options.
const (
	AllocationToleranceAbsolute AllocationToleranceMode = "ABS"
	AllocationTolerancePercent  AllocationToleranceMode = "PERCENT"
)

// TolerancePercentageBase defines the base amount used for percentage tolerance calculation.
type TolerancePercentageBase string

const (
	// TolerancePercentageBaseMax uses the maximum of both amounts (current default behavior).
	TolerancePercentageBaseMax TolerancePercentageBase = "MAX"
	// TolerancePercentageBaseMin uses the minimum of both amounts (stricter matching).
	TolerancePercentageBaseMin TolerancePercentageBase = "MIN"
	// TolerancePercentageBaseAvg uses the average of both amounts.
	TolerancePercentageBaseAvg TolerancePercentageBase = "AVERAGE"
	// TolerancePercentageBaseLeft uses the left (source) amount only.
	TolerancePercentageBaseLeft TolerancePercentageBase = "LEFT"
	// TolerancePercentageBaseRight uses the right (target) amount only.
	TolerancePercentageBaseRight TolerancePercentageBase = "RIGHT"
)

// AllocationConfig defines parameters for 1:N or N:1 allocation.
type AllocationConfig struct {
	AllowPartial   bool
	Direction      AllocationDirection
	ToleranceMode  AllocationToleranceMode
	ToleranceValue decimal.Decimal
	UseBaseAmount  bool
}

// RuleDefinition represents a decoded match rule for the engine.
type RuleDefinition struct {
	ID       uuid.UUID
	Priority int
	Type     shared.RuleType

	Exact      *ExactConfig
	Tolerance  *ToleranceConfig
	DateLag    *DateLagConfig
	Allocation *AllocationConfig
}

// RuleDefinitionFromMatchRule converts a shared MatchRule to a RuleDefinition.
func RuleDefinitionFromMatchRule(rule *shared.MatchRule) (RuleDefinition, error) {
	if rule == nil {
		return RuleDefinition{}, ErrNilMatchRule
	}

	if !rule.Type.Valid() {
		return RuleDefinition{}, ErrUnsupportedRuleType
	}

	return RuleDefinition{
		ID:       rule.ID,
		Priority: rule.Priority,
		Type:     rule.Type,
	}, nil
}
