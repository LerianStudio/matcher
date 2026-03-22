// Package extraction provides PostgreSQL repository implementation for ExtractionRequest entities.
package extraction

import (
	"errors"

	pgcommon "github.com/LerianStudio/matcher/internal/shared/adapters/postgres/common"
)

// Sentinel errors for extraction repository operations.
var (
	ErrRepoNotInitialized  = errors.New("extraction repository not initialized")
	ErrEntityRequired      = errors.New("extraction request entity is required")
	ErrModelRequired       = errors.New("extraction request model is required")
	ErrTransactionRequired = pgcommon.ErrTransactionRequired
)
