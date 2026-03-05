// Package job provides PostgreSQL repository implementation for ingestion jobs.
package job

import "errors"

var (
	errJobEntityRequired = errors.New("ingestion job entity is required")
	errJobModelRequired  = errors.New("ingestion job model is required")
	errInvalidJobStatus  = errors.New("invalid job status")
	errRepoNotInit       = errors.New("job repository not initialized")
)
