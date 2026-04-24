// Copyright 2025 Lerian Studio. All rights reserved.
// Use of this source code is governed by an Elastic License 2.0
// that can be found in the LICENSE.md file.

package match_rule

import (
	"github.com/LerianStudio/matcher/internal/shared/ports"
)

const (
	matchRuleColumns      = "id, context_id, priority, type, config, created_at, updated_at"
	reorderPriorityOffset = 1000
	argsPerRuleID         = 3
)

// Repository provides PostgreSQL operations for match rules.
type Repository struct {
	provider ports.InfrastructureProvider
}

// NewRepository creates a new match rule repository.
func NewRepository(provider ports.InfrastructureProvider) *Repository {
	return &Repository{provider: provider}
}
