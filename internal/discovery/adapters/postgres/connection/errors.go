// Copyright 2025 Lerian Studio. All rights reserved.
// Use of this source code is governed by an Elastic License 2.0
// that can be found in the LICENSE.md file.

// Package connection provides PostgreSQL repository implementation for FetcherConnection entities.
package connection

import (
	"github.com/LerianStudio/matcher/internal/discovery/domain/repositories"
	pgcommon "github.com/LerianStudio/matcher/internal/shared/adapters/postgres/common"
)

// Sentinel errors for connection repository operations.
var (
	// ErrConnectionNotFound re-exports the domain-level sentinel for adapter compatibility.
	ErrConnectionNotFound  = repositories.ErrConnectionNotFound
	ErrRepoNotInitialized  = repositories.ErrRepoNotInitialized
	ErrEntityRequired      = repositories.ErrEntityRequired
	ErrTransactionRequired = pgcommon.ErrTransactionRequired
)
