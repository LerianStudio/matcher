// Package source provides PostgreSQL repository implementation for reconciliation sources.
package source

import (
	"errors"

	pgcommon "github.com/LerianStudio/matcher/internal/shared/adapters/postgres/common"
)

// Sentinel errors for source model operations.
var (
	ErrSourceEntityRequired   = errors.New("source entity is required")
	ErrSourceEntityIDRequired = errors.New("source entity ID is required")
	ErrSourceModelRequired    = errors.New("source model is required")
	ErrRepoNotInitialized     = errors.New("source repository not initialized")
	ErrConnectionRequired     = pgcommon.ErrConnectionRequired
	ErrTransactionRequired    = pgcommon.ErrTransactionRequired
)
