package value_objects

import (
	"errors"
	"strings"
)

// ErrInvalidOverrideReason is returned when parsing an invalid override reason.
var ErrInvalidOverrideReason = errors.New("invalid override reason")

// OverrideReason represents reasons for overriding standard exception handling.
type OverrideReason string

// OverrideReason values.
const (
	OverrideReasonPolicyException OverrideReason = "POLICY_EXCEPTION"
	OverrideReasonOpsApproval     OverrideReason = "OPS_APPROVAL"
	OverrideReasonCustomerDispute OverrideReason = "CUSTOMER_DISPUTE"
	OverrideReasonDataCorrection  OverrideReason = "DATA_CORRECTION"
)

// IsValid checks if the override reason is valid.
func (reason OverrideReason) IsValid() bool {
	switch reason {
	case OverrideReasonPolicyException,
		OverrideReasonOpsApproval,
		OverrideReasonCustomerDispute,
		OverrideReasonDataCorrection:
		return true
	default:
		return false
	}
}

// ParseOverrideReason parses a string into an OverrideReason.
func ParseOverrideReason(value string) (OverrideReason, error) {
	trimmed := strings.TrimSpace(value)
	if trimmed == "" {
		return "", ErrInvalidOverrideReason
	}

	reason := OverrideReason(strings.ToUpper(trimmed))
	if !reason.IsValid() {
		return "", ErrInvalidOverrideReason
	}

	return reason, nil
}
