// Copyright 2025 Lerian Studio. All rights reserved.
// Use of this source code is governed by an Elastic License 2.0
// that can be found in the LICENSE.md file.

// Package constants provides application-wide constant values.
package constants

const (
	// DefaultPaginationLimit is the default number of items returned when clients
	// do not provide a valid positive limit.
	DefaultPaginationLimit = 20
	// MaximumPaginationLimit is the hard cap for list endpoints and repositories.
	MaximumPaginationLimit = 200
)
