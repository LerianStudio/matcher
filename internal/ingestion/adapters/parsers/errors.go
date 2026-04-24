// Copyright 2025 Lerian Studio. All rights reserved.
// Use of this source code is governed by an Elastic License 2.0
// that can be found in the LICENSE.md file.

// Package parsers provides file format parsers for ingestion.
package parsers

import (
	"errors"
	"fmt"
	"regexp"
)

// Package-level sentinel errors for all parsers.
var (
	errMissingFieldMap        = errors.New("field map is required")
	errMissingFieldMapping    = errors.New("field map mapping is required")
	errMissingIngestionJob    = errors.New("ingestion job is required")
	errInvalidMappingFormat   = errors.New("field map mapping must contain string values")
	errReaderRequired         = errors.New("reader is required")
	errCallbackRequired       = errors.New("chunk callback is required")
	errMissingMappingKey      = errors.New("field map missing required mapping key")
	errEmptyMappingValue      = errors.New("field map mapping value must be non-empty")
	errDateEmpty              = errors.New("date value is empty")
	errUnsupportedDateFormat  = errors.New("unsupported date format")
	errRegistryNotInitialized = errors.New("parser registry not initialized")
	errUnsupportedFormat      = errors.New("unsupported format")
	errJSONPayloadInvalid     = errors.New("json payload must be an object or array of objects")
	errJSONArrayNotObjects    = errors.New("json array must contain objects")
	errJSONUnexpectedKeyType  = errors.New("expected string key in json object")
	errInvalidCurrencyCode    = errors.New("invalid ISO 4217 currency code")
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
