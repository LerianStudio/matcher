// Copyright 2025 Lerian Studio. All rights reserved.
// Use of this source code is governed by an Elastic License 2.0
// that can be found in the LICENSE.md file.

// Package schema provides PostgreSQL repository implementation for DiscoveredSchema entities.
package schema

import (
	"errors"

	pgcommon "github.com/LerianStudio/matcher/internal/shared/adapters/postgres/common"
)

// Sentinel errors for schema repository operations.
var (
	ErrRepoNotInitialized  = errors.New("schema repository not initialized")
	ErrEntityRequired      = errors.New("discovered schema entity is required")
	ErrModelRequired       = errors.New("discovered schema model is required")
	ErrTransactionRequired = pgcommon.ErrTransactionRequired
)
