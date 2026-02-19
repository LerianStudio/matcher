// Package enums defines matching domain enumerations and constants.
package enums

// Exception reason constants define the allowed reasons for exception records.
// These form an allowlist for security validation.
const (
	ReasonUnmatched            = "UNMATCHED"
	ReasonFXRateUnavailable    = "FX_RATE_UNAVAILABLE"
	ReasonMissingBaseAmount    = "MISSING_BASE_AMOUNT"
	ReasonMissingBaseCurrency  = "MISSING_BASE_CURRENCY"
	ReasonSplitIncomplete      = "SPLIT_INCOMPLETE"
	ReasonValidationFailed     = "VALIDATION_FAILED"
	ReasonSourceMismatch       = "SOURCE_MISMATCH"
	ReasonDuplicateTransaction = "DUPLICATE_TRANSACTION"
	ReasonFeeVariance          = "FEE_VARIANCE"
	ReasonFeeDataMissing       = "FEE_DATA_MISSING"
	ReasonFeeCurrencyMismatch  = "FEE_CURRENCY_MISMATCH"

	// MaxReasonLength is the maximum allowed length for exception reason.
	MaxReasonLength = 64
)

// validReasons contains the allowlist of valid exception reasons.
var validReasons = map[string]struct{}{
	ReasonUnmatched:            {},
	ReasonFXRateUnavailable:    {},
	ReasonMissingBaseAmount:    {},
	ReasonMissingBaseCurrency:  {},
	ReasonSplitIncomplete:      {},
	ReasonValidationFailed:     {},
	ReasonSourceMismatch:       {},
	ReasonDuplicateTransaction: {},
	ReasonFeeVariance:          {},
	ReasonFeeDataMissing:       {},
	ReasonFeeCurrencyMismatch:  {},
}

// SanitizeReason validates and sanitizes an exception reason.
// Returns the sanitized reason or the default "UNMATCHED" if invalid.
func SanitizeReason(reason string) string {
	if reason == "" {
		return ReasonUnmatched
	}

	// Enforce length bounds
	if len(reason) > MaxReasonLength {
		reason = reason[:MaxReasonLength]
	}

	// Check against allowlist
	if _, ok := validReasons[reason]; ok {
		return reason
	}

	// Return default for unknown reasons
	return ReasonUnmatched
}
