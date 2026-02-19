// Package value_objects provides exception domain value objects.
// Severity types are re-exported from the shared kernel for backward compatibility.
package value_objects

import (
	sharedException "github.com/LerianStudio/matcher/internal/shared/domain/exception"
)

// Type aliases for backward compatibility.
type (
	// ExceptionSeverity represents exception priority.
	ExceptionSeverity = sharedException.ExceptionSeverity
	// SeverityClassificationInput contains normalized values for classification.
	SeverityClassificationInput = sharedException.SeverityClassificationInput
	// SeverityRule defines ordered thresholds for severity classification.
	SeverityRule = sharedException.SeverityRule
	// SeverityClassificationResult is returned from classification.
	SeverityClassificationResult = sharedException.SeverityClassificationResult
)

// Re-exported severity constants.
var (
	ExceptionSeverityLow      = sharedException.ExceptionSeverityLow
	ExceptionSeverityMedium   = sharedException.ExceptionSeverityMedium
	ExceptionSeverityHigh     = sharedException.ExceptionSeverityHigh
	ExceptionSeverityCritical = sharedException.ExceptionSeverityCritical
)

// Re-exported errors.
var (
	ErrInvalidExceptionSeverity = sharedException.ErrInvalidExceptionSeverity
	ErrEmptySeverityRules       = sharedException.ErrEmptySeverityRules
)

// ReasonFXRateUnavailable flags missing FX rates used during severity classification.
const ReasonFXRateUnavailable = sharedException.ReasonFXRateUnavailable

// ClassifyExceptionSeverity evaluates ordered rules and returns the first match.
var ClassifyExceptionSeverity = sharedException.ClassifyExceptionSeverity

// DefaultSeverityRules returns the PRD default rules in descending severity.
var DefaultSeverityRules = sharedException.DefaultSeverityRules

// ParseExceptionSeverity parses a string into an ExceptionSeverity.
var ParseExceptionSeverity = sharedException.ParseExceptionSeverity
