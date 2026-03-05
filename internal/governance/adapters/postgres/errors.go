// Package postgres provides PostgreSQL adapters for the governance bounded context.
package postgres

import (
	"errors"

	governanceErrors "github.com/LerianStudio/matcher/internal/governance/domain/errors"
	pgcommon "github.com/LerianStudio/matcher/internal/shared/adapters/postgres/common"
)

// Repository-specific errors.
var (
	// ErrRepositoryNotInitialized is returned when the repository is not initialized.
	ErrRepositoryNotInitialized = errors.New("repository not initialized")

	// ErrAuditLogRequired is returned when the audit log is missing.
	ErrAuditLogRequired = errors.New("audit log is required")

	// ErrAuditLogNotFound is returned when the audit log is not found.
	// Re-exported from domain/errors for adapter-layer consumers.
	ErrAuditLogNotFound = governanceErrors.ErrAuditLogNotFound

	// ErrIDRequired is returned when the ID is missing.
	ErrIDRequired = errors.New("id is required")

	// ErrLimitMustBePositive is returned when the limit is not positive.
	ErrLimitMustBePositive = errors.New("limit must be positive")

	// ErrTransactionRequired is returned when a transaction is required but not provided.
	// Re-exported from pgcommon for backward compatibility.
	ErrTransactionRequired = pgcommon.ErrTransactionRequired

	// ErrNilScanner is returned when a nil scanner is passed to scanAuditLog.
	ErrNilScanner = errors.New("nil scanner")

	// ErrPreviousRecordNotFound is returned when the previous record in the hash chain is not found.
	ErrPreviousRecordNotFound = errors.New("previous record not found")

	// ErrCursorEncoderRequired is returned when a cursor encoder callback is required but nil.
	ErrCursorEncoderRequired = errors.New("cursor encoder is required")
)
