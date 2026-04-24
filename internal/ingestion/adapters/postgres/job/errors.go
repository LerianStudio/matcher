// Copyright 2025 Lerian Studio. All rights reserved.
// Use of this source code is governed by an Elastic License 2.0
// that can be found in the LICENSE.md file.

// Package job provides PostgreSQL repository implementation for ingestion jobs.
package job

import "errors"

var (
	errJobEntityRequired = errors.New("ingestion job entity is required")
	errJobModelRequired  = errors.New("ingestion job model is required")
	errInvalidJobStatus  = errors.New("invalid job status")
	errRepoNotInit       = errors.New("job repository not initialized")
)
