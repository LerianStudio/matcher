// Copyright 2025 Lerian Studio. All rights reserved.
// Use of this source code is governed by an Elastic License 2.0
// that can be found in the LICENSE.md file.

// Package dispute provides PostgreSQL persistence for dispute entities.
package dispute

import (
	"errors"

	"github.com/LerianStudio/matcher/internal/exception/domain/dispute"
	pgcommon "github.com/LerianStudio/matcher/internal/shared/adapters/postgres/common"
)

// Repository errors.
var (
	ErrRepoNotInitialized  = errors.New("dispute repository not initialized")
	ErrDisputeNotFound     = dispute.ErrNotFound
	ErrDisputeNil          = errors.New("dispute is nil")
	ErrTransactionRequired = pgcommon.ErrTransactionRequired
)
