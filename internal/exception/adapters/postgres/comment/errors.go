// Package comment provides PostgreSQL persistence for exception comment entities.
package comment

import (
	"errors"

	"github.com/LerianStudio/matcher/internal/exception/domain/entities"
)

// Repository errors.
var (
	ErrRepoNotInitialized  = errors.New("comment repository not initialized")
	ErrCommentNotFound     = entities.ErrCommentNotFound
	ErrCommentNil          = errors.New("comment is nil")
	ErrTransactionRequired = errors.New("transaction is required")
)
