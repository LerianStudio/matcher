// Copyright 2025 Lerian Studio. All rights reserved.
// Use of this source code is governed by an Elastic License 2.0
// that can be found in the LICENSE.md file.

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
