// Package rate provides PostgreSQL adapter errors for fee rates.
package rate

import "errors"

var (
	// ErrRepoNotInitialized is returned when the repository is not initialized.
	ErrRepoNotInitialized = errors.New("rate repository not initialized")
	// ErrRateModelNeeded is returned when the rate model is missing.
	ErrRateModelNeeded = errors.New("rate model is required")
	// ErrRateNotFound is returned when the rate is not found.
	ErrRateNotFound = errors.New("rate not found")
	// ErrUnknownStructureType is returned when the fee structure type is unknown.
	ErrUnknownStructureType = errors.New("unknown fee structure type")
)
