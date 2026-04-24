// Copyright 2025 Lerian Studio. All rights reserved.
// Use of this source code is governed by an Elastic License 2.0
// that can be found in the LICENSE.md file.

// Package fee_schedule provides PostgreSQL adapter errors for fee schedules.
package fee_schedule

import "errors"

var (
	// ErrRepoNotInitialized is returned when the repository is not initialized.
	ErrRepoNotInitialized = errors.New("fee schedule repository not initialized")
	// ErrFeeScheduleModelNeeded is returned when the fee schedule model is missing.
	ErrFeeScheduleModelNeeded = errors.New("fee schedule model is required")
	// ErrUnknownStructureType is returned when the fee structure type is unknown.
	ErrUnknownStructureType = errors.New("unknown fee structure type")
	// ErrInvalidTx is returned when the transaction is missing.
	ErrInvalidTx = errors.New("fee schedule repository invalid transaction")
)
