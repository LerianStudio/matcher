// Copyright 2025 Lerian Studio. All rights reserved.
// Use of this source code is governed by an Elastic License 2.0
// that can be found in the LICENSE.md file.

// Package query provides read operations for governance entities.
//
// Audit log queries live here rather than in the HTTP handler so that
// read operations honor matcher's CQRS separation (docs/PROJECT_RULES.md §1):
// writes in services/command/, reads in services/query/.
package query

import (
	"errors"

	"github.com/LerianStudio/matcher/internal/governance/domain/repositories"
)

// Sentinel errors for query use case validation.
var (
	// ErrQueryRepoRequired indicates the audit log repository is nil.
	ErrQueryRepoRequired = errors.New("audit log repository is required")
)

// UseCase provides query operations for governance audit logs.
type UseCase struct {
	repo repositories.AuditLogRepository
}

// NewUseCase creates a new query use case with the required audit log repository.
func NewUseCase(repo repositories.AuditLogRepository) (*UseCase, error) {
	if repo == nil {
		return nil, ErrQueryRepoRequired
	}

	return &UseCase{repo: repo}, nil
}
