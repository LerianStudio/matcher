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
