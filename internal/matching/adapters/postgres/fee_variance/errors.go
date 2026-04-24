// Copyright 2025 Lerian Studio. All rights reserved.
// Use of this source code is governed by an Elastic License 2.0
// that can be found in the LICENSE.md file.

// Package fee_variance provides PostgreSQL adapter errors for fee variances.
package fee_variance

import "errors"

var (
	// ErrRepoNotInitialized is returned when the repository is not initialized.
	ErrRepoNotInitialized = errors.New("fee variance repository not initialized")
	// ErrFeeVarianceEntityNeeded is returned when the fee variance entity is missing.
	ErrFeeVarianceEntityNeeded = errors.New("fee variance entity is required")
	// ErrFeeVarianceModelNeeded is returned when the fee variance model is missing.
	ErrFeeVarianceModelNeeded = errors.New("fee variance model is required")
	// ErrInvalidTx is returned when an invalid transaction is provided.
	ErrInvalidTx = errors.New("fee variance repository invalid transaction")
)
