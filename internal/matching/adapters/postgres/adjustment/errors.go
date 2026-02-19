// Package adjustment provides PostgreSQL persistence for adjustment entities.
package adjustment

import "errors"

// Sentinel errors for adjustment repository operations.
var (
	ErrRepoNotInitialized         = errors.New("adjustment repository not initialized")
	ErrAdjustmentEntityNeeded     = errors.New("adjustment entity is required")
	ErrAdjustmentModelNeeded      = errors.New("adjustment model is required")
	ErrInvalidTx                  = errors.New("invalid transaction type")
	ErrTransactionRequired        = errors.New("transaction is required")
	ErrInvalidTransactionType     = errors.New("invalid transaction type: expected *sql.Tx")
	ErrInvalidAdjustmentType      = errors.New("invalid adjustment type")
	ErrInvalidAdjustmentDirection = errors.New("invalid adjustment direction")
)
