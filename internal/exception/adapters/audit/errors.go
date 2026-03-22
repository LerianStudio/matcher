package audit

import (
	"errors"
)

// ErrNilAuditLogRepository indicates the audit log repository is nil.
var ErrNilAuditLogRepository = errors.New("audit log repository is required")
