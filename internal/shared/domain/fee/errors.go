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
)
