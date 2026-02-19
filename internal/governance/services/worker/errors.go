// Package worker provides background job processing for governance.
package worker

import "errors"

// Sentinel errors for archival worker operations.
var (
	// ErrWorkerAlreadyRunning indicates the worker is already started.
	ErrWorkerAlreadyRunning = errors.New("archival worker is already running")

	// ErrWorkerNotRunning indicates the worker is not started.
	ErrWorkerNotRunning = errors.New("archival worker is not running")

	// ErrNilArchiveRepo indicates the archive metadata repository is nil.
	ErrNilArchiveRepo = errors.New("archive metadata repository is required")

	// ErrNilPartitionManager indicates the partition manager is nil.
	ErrNilPartitionManager = errors.New("partition manager is required")

	// ErrNilStorageClient indicates the object storage client is nil.
	ErrNilStorageClient = errors.New("object storage client is required")

	// ErrNilRedisClient indicates the infrastructure provider (for Redis) is nil.
	ErrNilRedisClient = errors.New("infrastructure provider is required for distributed lock")

	// ErrArchivalInProgress indicates the archival lock is held by another instance.
	ErrArchivalInProgress = errors.New("archival lock held by another instance")

	// ErrChecksumMismatch indicates the archive checksum does not match the source.
	ErrChecksumMismatch = errors.New("archive checksum does not match source")

	// ErrRowCountMismatch indicates the archive row count does not match the source.
	ErrRowCountMismatch = errors.New("archive row count does not match source")
)
