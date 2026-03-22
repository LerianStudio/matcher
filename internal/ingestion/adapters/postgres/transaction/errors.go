// Package transaction provides PostgreSQL repository for transactions.
package transaction

import (
	"errors"

	pgcommon "github.com/LerianStudio/matcher/internal/shared/adapters/postgres/common"
)

var (
	errTxEntityRequired        = errors.New("transaction entity is required")
	errTxModelRequired         = errors.New("transaction model is required")
	errTxRequired              = pgcommon.ErrTransactionRequired
	errInvalidExtractionStatus = errors.New("invalid extraction status")
	errInvalidTxStatus         = errors.New("invalid transaction status")
	errTxRepoNotInit           = errors.New("transaction repository not initialized")
	errContextIDRequired       = errors.New("context id is required")
	errJobIDRequired           = errors.New("job id is required")
	errLimitMustBePositive     = errors.New("limit must be greater than zero")
	errOffsetMustBeNonNegative = errors.New("offset must be greater or equal to zero")
)
