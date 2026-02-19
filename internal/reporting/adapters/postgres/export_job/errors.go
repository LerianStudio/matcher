// Package export_job provides PostgreSQL adapters for the reporting export job aggregate.
package export_job

import "errors"

// Repository-specific errors.
var (
	ErrRepositoryNotInitialized = errors.New("export job repository not initialized")
)
