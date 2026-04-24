// Copyright 2025 Lerian Studio. All rights reserved.
// Use of this source code is governed by an Elastic License 2.0
// that can be found in the LICENSE.md file.

// Package match_item provides PostgreSQL adapter errors for match items.
package match_item

import "errors"

var (
	// ErrRepoNotInitialized is returned when the repository is not initialized.
	ErrRepoNotInitialized = errors.New("match item repository not initialized")
	// ErrMatchItemEntityNeeded is returned when the match item entity is missing.
	ErrMatchItemEntityNeeded = errors.New("match item entity is required")
	// ErrMatchItemModelNeeded is returned when the match item model is missing.
	ErrMatchItemModelNeeded = errors.New("match item model is required")
	// ErrInvalidTx is returned when an invalid transaction is provided.
	ErrInvalidTx = errors.New("match item repository invalid transaction")
)
