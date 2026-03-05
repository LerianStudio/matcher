// Package exception provides PostgreSQL persistence for exception entities.
package exception

import (
	"errors"

	pgcommon "github.com/LerianStudio/matcher/internal/shared/adapters/postgres/common"
)

// Repository errors.
var (
	ErrRepoNotInitialized     = errors.New("exception repository not initialized")
	ErrConcurrentModification = errors.New("exception was modified by another process")
	ErrTransactionRequired    = pgcommon.ErrTransactionRequired
)
