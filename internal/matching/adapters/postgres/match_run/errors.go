// Copyright 2025 Lerian Studio. All rights reserved.
// Use of this source code is governed by an Elastic License 2.0
// that can be found in the LICENSE.md file.

// Package match_run provides PostgreSQL adapters for match runs.
package match_run

import "errors"

var (
	// ErrRepoNotInitialized is returned when the repository is not initialized.
	ErrRepoNotInitialized = errors.New("match run repository not initialized")
	// ErrMatchRunEntityNeeded is returned when the match run entity is missing.
	ErrMatchRunEntityNeeded = errors.New("match run entity is required")
	// ErrMatchRunModelNeeded is returned when the match run model is missing.
	ErrMatchRunModelNeeded = errors.New("match run model is required")
	// ErrInvalidTx is returned when an invalid transaction is provided.
	ErrInvalidTx = errors.New("match run repository invalid transaction")
)
