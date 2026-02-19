// Package utils provides shared text processing utilities used across bounded contexts.
package utils

import "strings"

// NormalizeOptionalText trims whitespace from an optional string pointer.
// Returns nil if the input is nil or the trimmed result is empty.
func NormalizeOptionalText(value *string) *string {
	if value == nil {
		return nil
	}

	trimmed := strings.TrimSpace(*value)
	if trimmed == "" {
		return nil
	}

	return &trimmed
}
