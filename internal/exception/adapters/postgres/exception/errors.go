// Copyright 2025 Lerian Studio. All rights reserved.
// Use of this source code is governed by an Elastic License 2.0
// that can be found in the LICENSE.md file.

// Package exception provides PostgreSQL persistence for exception entities.
package exception

import (
	"errors"

	pgcommon "github.com/LerianStudio/matcher/internal/shared/adapters/postgres/common"
)

// Repository errors.
var (
	ErrRepoNotInitialized     = errors.New("exception repository not initialized")
	ErrConcurrentModification = errors.New("exception was modified by another process")
	ErrTransactionRequired    = pgcommon.ErrTransactionRequired
)
