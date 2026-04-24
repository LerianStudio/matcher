// Copyright 2025 Lerian Studio. All rights reserved.
// Use of this source code is governed by an Elastic License 2.0
// that can be found in the LICENSE.md file.

package audit

import (
	"errors"
)

// ErrNilAuditLogRepository indicates the audit log repository is nil.
var ErrNilAuditLogRepository = errors.New("audit log repository is required")
