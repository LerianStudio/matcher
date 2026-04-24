// Copyright 2025 Lerian Studio. All rights reserved.
// Use of this source code is governed by an Elastic License 2.0
// that can be found in the LICENSE.md file.

// Package archivemetadata provides the PostgreSQL adapter for archive metadata persistence.
package archivemetadata

import (
	"errors"

	governanceErrors "github.com/LerianStudio/matcher/internal/governance/domain/errors"
	pgcommon "github.com/LerianStudio/matcher/internal/shared/adapters/postgres/common"
)

// Sentinel errors for the archive metadata repository.
var (
	ErrRepositoryNotInitialized = errors.New("archive metadata repository not initialized")
	ErrMetadataRequired         = errors.New("archive metadata is required")
	ErrIDRequired               = errors.New("id is required")
	ErrTenantIDRequired         = errors.New("tenant id is required")
	ErrPartitionNameRequired    = errors.New("partition name is required")
	ErrLimitMustBePositive      = errors.New("limit must be positive")
	ErrNilScanner               = errors.New("nil scanner")
	ErrTransactionRequired      = pgcommon.ErrTransactionRequired

	ErrInvalidArchiveStatus = errors.New("invalid archive status from database")

	// ErrMetadataNotFound is returned when archive metadata is not found.
	// Re-exported from domain/errors for adapter-layer consumers.
	ErrMetadataNotFound = governanceErrors.ErrMetadataNotFound
)
