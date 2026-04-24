// Copyright 2025 Lerian Studio. All rights reserved.
// Use of this source code is governed by an Elastic License 2.0
// that can be found in the LICENSE.md file.

package extraction

import (
	"github.com/LerianStudio/matcher/internal/discovery/domain/entities"
	"github.com/LerianStudio/matcher/internal/discovery/domain/repositories"
	"github.com/LerianStudio/matcher/internal/shared/ports"
)

// Compile-time check that Repository implements ExtractionRepository.
var _ repositories.ExtractionRepository = (*Repository)(nil)

const (
	tableName = "extraction_requests"
	// allColumns is the canonical SELECT list. Order MUST match scanExtraction.
	// Bridge* columns added in migration 000026 (T-005) live at the tail so
	// adding them did not perturb existing column ordinals; custody_deleted_at
	// from migration 000027 (T-006 polish) is appended for the same reason.
	allColumns = "id, connection_id, ingestion_job_id, fetcher_job_id, tables, start_date, end_date, filters, status, result_path, error_message, created_at, updated_at, bridge_attempts, bridge_last_error, bridge_last_error_message, bridge_failed_at, custody_deleted_at"
)

// Repository provides PostgreSQL operations for ExtractionRequest entities.
type Repository struct {
	provider ports.InfrastructureProvider
}

// NewRepository creates a new extraction repository.
func NewRepository(provider ports.InfrastructureProvider) *Repository {
	return &Repository{provider: provider}
}

func nullableJSON(data []byte) any {
	if data == nil {
		return nil
	}

	return data
}

// scanExtraction scans a SQL row into an ExtractionRequest domain entity.
// Column order MUST match allColumns; the bridge_* columns are at the tail
// because they were added in migration 000026, and custody_deleted_at is at
// the very tail because it was added in migration 000027.
func scanExtraction(scanner interface{ Scan(dest ...any) error }) (*entities.ExtractionRequest, error) {
	var model ExtractionModel
	if err := scanner.Scan(
		&model.ID,
		&model.ConnectionID,
		&model.IngestionJobID,
		&model.FetcherJobID,
		&model.Tables,
		&model.StartDate,
		&model.EndDate,
		&model.Filters,
		&model.Status,
		&model.ResultPath,
		&model.ErrorMessage,
		&model.CreatedAt,
		&model.UpdatedAt,
		&model.BridgeAttempts,
		&model.BridgeLastError,
		&model.BridgeLastErrorMessage,
		&model.BridgeFailedAt,
		&model.CustodyDeletedAt,
	); err != nil {
		return nil, err
	}

	return model.ToDomain()
}
