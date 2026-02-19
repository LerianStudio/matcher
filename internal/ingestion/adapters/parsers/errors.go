// Package parsers provides file format parsers for ingestion.
package parsers

import (
	"fmt"
	"regexp"
)

// pathPattern matches file system paths in error messages.
var pathPattern = regexp.MustCompile(`(?:^|[^a-zA-Z0-9])/[a-zA-Z0-9_\-./]+`)

// SanitizeErrorMessage removes potentially sensitive information from error messages.
// In production, this prevents leaking internal paths or implementation details.
func SanitizeErrorMessage(msg string, forProduction bool) string {
	if !forProduction {
		return msg
	}

	// Remove file paths
	sanitized := pathPattern.ReplaceAllString(msg, "[path]")

	// Truncate very long messages
	const maxLen = 200
	if len(sanitized) > maxLen {
		sanitized = sanitized[:maxLen] + "..."
	}

	return sanitized
}

// GenericParseError returns a generic error message for production use.
func GenericParseError(row int) string {
	return fmt.Sprintf("parse error at row %d", row)
}
