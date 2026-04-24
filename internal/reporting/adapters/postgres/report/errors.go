// Copyright 2025 Lerian Studio. All rights reserved.
// Use of this source code is governed by an Elastic License 2.0
// that can be found in the LICENSE.md file.

// Package report provides PostgreSQL adapters for the reporting report aggregate.
package report

import "errors"

// Repository-specific errors.
var (
	ErrRepositoryNotInitialized = errors.New("report repository not initialized")
	ErrContextIDRequired        = errors.New("context_id is required")
	ErrLimitMustBePositive      = errors.New("limit must be positive")
	ErrOffsetMustBeNonNegative  = errors.New("offset must be non-negative")
	ErrLimitExceedsMaximum      = errors.New("limit exceeds maximum allowed (1000)")
	ErrExportLimitExceeded      = errors.New("export record limit exceeded")
	ErrMaxRecordsMustBePositive = errors.New("maxRecords must be positive")
	ErrInvalidVarianceCursor    = errors.New("invalid variance cursor")
)
