// Package dashboard provides PostgreSQL adapters for the reporting dashboard aggregate.
package dashboard

import "errors"

// Repository-specific errors.
var (
	ErrRepositoryNotInitialized = errors.New("dashboard repository not initialized")
	ErrContextIDRequired        = errors.New("context_id is required")
)

// Unexported sentinel errors for internal validation.
var (
	errInvalidAgeBucketOrder   = errors.New("invalid age bucket order")
	errInvalidAgeExposureOrder = errors.New("invalid age exposure order")
)
