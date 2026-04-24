// Copyright 2025 Lerian Studio. All rights reserved.
// Use of this source code is governed by an Elastic License 2.0
// that can be found in the LICENSE.md file.

// Package adjustment provides PostgreSQL persistence for adjustment entities.
package adjustment

import (
	"errors"

	pgcommon "github.com/LerianStudio/matcher/internal/shared/adapters/postgres/common"
)

// Sentinel errors for adjustment repository operations.
var (
	ErrRepoNotInitialized         = errors.New("adjustment repository not initialized")
	ErrAdjustmentEntityNeeded     = errors.New("adjustment entity is required")
	ErrAdjustmentModelNeeded      = errors.New("adjustment model is required")
	ErrTransactionRequired        = pgcommon.ErrTransactionRequired
	ErrInvalidAdjustmentType      = errors.New("invalid adjustment type")
	ErrInvalidAdjustmentDirection = errors.New("invalid adjustment direction")
	ErrAuditLogRepoRequired       = errors.New("audit log repository is required for CreateWithAuditLog")
	ErrAuditLogRequired           = errors.New("audit log is required for SOX-compliant adjustment creation")
)
