package schedule

import "errors"

// Sentinel errors for schedule repository.
var (
	ErrScheduleEntityRequired    = errors.New("schedule entity is required")
	ErrScheduleContextIDRequired = errors.New("schedule context id is required")
	ErrScheduleModelRequired     = errors.New("schedule model is required")
	ErrRepoNotInitialized        = errors.New("schedule repository not initialized")
	ErrTransactionRequired       = errors.New("transaction is required")
)
