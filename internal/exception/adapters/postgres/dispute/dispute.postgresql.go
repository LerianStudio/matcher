// Copyright 2025 Lerian Studio. All rights reserved.
// Use of this source code is governed by an Elastic License 2.0
// that can be found in the LICENSE.md file.

package dispute

import (
	"context"

	"github.com/google/uuid"

	"github.com/LerianStudio/matcher/internal/exception/domain/dispute"
	"github.com/LerianStudio/matcher/internal/exception/domain/repositories"
	pgcommon "github.com/LerianStudio/matcher/internal/shared/adapters/postgres/common"
	"github.com/LerianStudio/matcher/internal/shared/ports"
)

// Repository persists disputes in PostgreSQL.
type Repository struct {
	provider ports.InfrastructureProvider
}

// NewRepository creates a new dispute repository.
func NewRepository(provider ports.InfrastructureProvider) *Repository {
	return &Repository{provider: provider}
}

func (repo *Repository) findByIDExec(
	ctx context.Context,
	qe pgcommon.QueryExecutor,
	id uuid.UUID,
) (*dispute.Dispute, error) {
	row := qe.QueryRowContext(ctx, `
		SELECT id, exception_id, category, state, description,
		       opened_by, resolution, reopen_reason, evidence, created_at, updated_at
		FROM disputes
		WHERE id = $1
	`, id.String())

	return scanDispute(row)
}

var _ repositories.DisputeRepository = (*Repository)(nil)
