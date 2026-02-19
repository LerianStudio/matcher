package audit

import "errors"

// ErrNilAuditLogRepository indicates the audit log repository is nil.
var ErrNilAuditLogRepository = errors.New("audit log repository is required")

// ErrTransactionRequired indicates a transaction is required for atomic operations.
var ErrTransactionRequired = errors.New("transaction is required for atomic audit logging")
