package fee

import "errors"

// Fee domain errors.
var (
	ErrNilRate                 = errors.New("rate is nil")
	ErrNilFeeStructure         = errors.New("fee structure is nil")
	ErrInvalidCurrency         = errors.New("invalid currency")
	ErrCurrencyMismatch        = errors.New("currency mismatch")
	ErrNegativeAmount          = errors.New("amount must be non-negative")
	ErrInvalidPercentageRate   = errors.New("percentage rate must be between 0 and 1 inclusive")
	ErrInvalidTieredDefinition = errors.New("invalid tiered fee definition")
	ErrToleranceNegative       = errors.New("tolerance must be non-negative")
	ErrNilTransaction          = errors.New("transaction is nil")
	ErrActualFeeMissing        = errors.New("actual fee is missing")

	// Fee schedule errors.
	ErrScheduleTenantIDRequired = errors.New("fee schedule tenant id is required")
	ErrScheduleNameRequired     = errors.New("fee schedule name is required")
	ErrScheduleNameTooLong      = errors.New("fee schedule name exceeds 100 characters")
	ErrScheduleItemsRequired    = errors.New("fee schedule must have at least one item")
	ErrDuplicateItemPriority    = errors.New("duplicate item priority in fee schedule")
	ErrInvalidApplicationOrder  = errors.New("invalid application order")
	ErrInvalidRoundingScale     = errors.New("rounding scale must be between 0 and 10")
	ErrInvalidRoundingMode      = errors.New("invalid rounding mode")
	ErrItemNameRequired         = errors.New("fee schedule item name is required")
	ErrNilSchedule              = errors.New("fee schedule is nil")
	ErrInvalidNormalizationMode = errors.New("invalid normalization mode")
	ErrGrossConvergenceFailed   = errors.New("gross calculation failed to converge")
	ErrFeeScheduleNotFound      = errors.New("fee schedule not found")

	// Fee rule errors.
	ErrFeeRuleNameRequired       = errors.New("fee rule name is required")
	ErrFeeRuleNameTooLong        = errors.New("fee rule name must not exceed 100 characters")
	ErrFeeRuleScheduleIDRequired = errors.New("fee rule fee schedule id is required")
	ErrFeeRuleContextIDRequired  = errors.New("fee rule context id is required")
	ErrFeeRuleNotFound           = errors.New("fee rule not found")
	ErrFeeRuleCountLimitExceeded = errors.New("fee rule count exceeds the maximum allowed per context")
	ErrFeeRulePriorityNegative   = errors.New("fee rule priority must be non-negative")
	ErrInvalidMatchingSide       = errors.New("invalid matching side: must be LEFT, RIGHT, or ANY")
	ErrInvalidPredicateOperator  = errors.New("invalid predicate operator: must be EQUALS, IN, or EXISTS")
	ErrPredicateFieldRequired    = errors.New("predicate field is required")
	ErrPredicateFieldTooLong     = errors.New("predicate field name exceeds maximum length")
	ErrPredicateValueRequired    = errors.New("predicate value is required for EQUALS operator")
	ErrPredicateValueForbidden   = errors.New("predicate value is not allowed for this operator")
	ErrPredicateValueTooLong     = errors.New("predicate value exceeds maximum length")
	ErrPredicateValuesRequired   = errors.New("predicate values are required for IN operator")
	ErrPredicateValuesForbidden  = errors.New("predicate values are not allowed for this operator")
	ErrPredicateValuesTooMany    = errors.New("predicate values list exceeds maximum count")
	ErrFeeRuleTooManyPredicates  = errors.New("fee rule must not have more than 50 predicates")
)
