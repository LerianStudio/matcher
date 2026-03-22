// Package match_rule provides PostgreSQL repository implementation for match rules.
package match_rule

import (
	"errors"

	pgcommon "github.com/LerianStudio/matcher/internal/shared/adapters/postgres/common"
)

// Sentinel errors for match rule model operations.
var (
	ErrMatchRuleEntityRequired    = errors.New("match rule entity is required")
	ErrMatchRuleModelRequired     = errors.New("match rule model is required")
	ErrMatchRuleContextIDRequired = errors.New("match rule context ID is required")
	ErrRepoNotInitialized         = errors.New("match rule repository not initialized")
	ErrRuleIDsRequired            = errors.New("rule ids are required")
	ErrCursorNotFound             = errors.New("cursor not found")
	ErrTransactionRequired        = pgcommon.ErrTransactionRequired
)
