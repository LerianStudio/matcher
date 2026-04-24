// Copyright 2025 Lerian Studio. All rights reserved.
// Use of this source code is governed by an Elastic License 2.0
// that can be found in the LICENSE.md file.

// Package field_map provides PostgreSQL repository implementation for field maps.
package field_map

import (
	"errors"

	pgcommon "github.com/LerianStudio/matcher/internal/shared/adapters/postgres/common"
)

// Sentinel errors for field map model operations.
var (
	ErrFieldMapEntityRequired   = errors.New("field map entity is required")
	ErrFieldMapEntityIDRequired = errors.New("field map entity ID is nil")
	ErrFieldMapModelRequired    = errors.New("field map model is required")
	ErrRepoNotInitialized       = errors.New("field map repository not initialized")
	ErrTransactionRequired      = pgcommon.ErrTransactionRequired
)
