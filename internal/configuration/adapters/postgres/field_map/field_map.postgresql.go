// Copyright 2025 Lerian Studio. All rights reserved.
// Use of this source code is governed by an Elastic License 2.0
// that can be found in the LICENSE.md file.

package field_map

import (
	shared "github.com/LerianStudio/matcher/internal/shared/domain"
	"github.com/LerianStudio/matcher/internal/shared/ports"
)

const fieldMapColumns = "id, context_id, source_id, mapping, version, created_at, updated_at"

// existsBySourceIDsBatchSize limits the number of source IDs per IN clause query.
// This protects against Postgres parameter limits (max ~32767) and prevents
// query planner degradation with very large IN clauses. The value is chosen
// conservatively to ensure good performance across typical workloads.
const existsBySourceIDsBatchSize = 1000

// Repository provides PostgreSQL operations for field maps.
type Repository struct {
	provider ports.InfrastructureProvider
}

// NewRepository creates a new field map repository.
func NewRepository(provider ports.InfrastructureProvider) *Repository {
	return &Repository{provider: provider}
}

func scanFieldMap(scanner interface{ Scan(dest ...any) error }) (*shared.FieldMap, error) {
	var model FieldMapPostgreSQLModel
	if err := scanner.Scan(
		&model.ID,
		&model.ContextID,
		&model.SourceID,
		&model.Mapping,
		&model.Version,
		&model.CreatedAt,
		&model.UpdatedAt,
	); err != nil {
		return nil, err
	}

	return model.ToEntity()
}
