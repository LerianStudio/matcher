// Copyright 2025 Lerian Studio. All rights reserved.
// Use of this source code is governed by an Elastic License 2.0
// that can be found in the LICENSE.md file.

// Package context provides PostgreSQL repository implementation for reconciliation contexts.
package context

import (
	"errors"

	pgcommon "github.com/LerianStudio/matcher/internal/shared/adapters/postgres/common"
)

// Sentinel errors for context model operations.
var (
	ErrContextEntityRequired = errors.New("context entity is required")
	ErrContextModelRequired  = errors.New("context model is required")
	ErrTenantIDRequired      = errors.New("tenant ID is required")
	ErrRepoNotInitialized    = errors.New("context repository not initialized")
	ErrTransactionRequired   = pgcommon.ErrTransactionRequired
)
