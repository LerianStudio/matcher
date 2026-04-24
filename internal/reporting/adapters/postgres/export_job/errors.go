// Copyright 2025 Lerian Studio. All rights reserved.
// Use of this source code is governed by an Elastic License 2.0
// that can be found in the LICENSE.md file.

// Package export_job provides PostgreSQL adapters for the reporting export job aggregate.
package export_job

import "errors"

// Repository-specific errors.
var (
	ErrRepositoryNotInitialized = errors.New("export job repository not initialized")
	ErrCursorEncoderRequired    = errors.New("cursor encoder is required")
)
