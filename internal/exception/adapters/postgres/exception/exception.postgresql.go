// Copyright 2025 Lerian Studio. All rights reserved.
// Use of this source code is governed by an Elastic License 2.0
// that can be found in the LICENSE.md file.

package exception

import (
	"github.com/LerianStudio/matcher/internal/exception/domain/repositories"
	"github.com/LerianStudio/matcher/internal/shared/ports"
)

// Repository persists exceptions in PostgreSQL.
type Repository struct {
	provider ports.InfrastructureProvider
}

// NewRepository creates a new exception repository.
func NewRepository(provider ports.InfrastructureProvider) *Repository {
	return &Repository{provider: provider}
}

var _ repositories.ExceptionRepository = (*Repository)(nil)
