// Package match_group provides PostgreSQL adapter errors for match groups.
package match_group

import (
	"errors"

	pgcommon "github.com/LerianStudio/matcher/internal/shared/adapters/postgres/common"
)

var (
	// ErrRepoNotInitialized is returned when the repository is not initialized.
	ErrRepoNotInitialized = errors.New("match group repository not initialized")
	// ErrMatchGroupEntityNeeded is returned when the match group entity is missing.
	ErrMatchGroupEntityNeeded = errors.New("match group entity is required")
	// ErrMatchGroupModelNeeded is returned when the match group model is missing.
	ErrMatchGroupModelNeeded = errors.New("match group model is required")
	// ErrInvalidTx is returned when an invalid transaction is provided.
	ErrInvalidTx = errors.New("match group repository invalid transaction")
	// ErrTransactionRequired is returned when a transaction is required but not provided.
	// Re-exported from pgcommon for backward compatibility.
	ErrTransactionRequired = pgcommon.ErrTransactionRequired
)
