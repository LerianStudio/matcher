// Copyright 2025 Lerian Studio. All rights reserved.
// Use of this source code is governed by an Elastic License 2.0
// that can be found in the LICENSE.md file.

// Package fee_rule provides PostgreSQL persistence for fee rule entities.
package fee_rule

import (
	"errors"

	pgcommon "github.com/LerianStudio/matcher/internal/shared/adapters/postgres/common"
)

// Sentinel errors for fee rule repository.
var (
	ErrRepoNotInitialized  = errors.New("fee rule repository not initialized")
	ErrFeeRuleModelNeeded  = errors.New("fee rule model is required for entity conversion")
	ErrFeeRuleEntityNil    = errors.New("fee rule entity is required")
	ErrFeeRuleEntityIDNil  = errors.New("fee rule entity ID is required")
	ErrTransactionRequired = pgcommon.ErrTransactionRequired
)
