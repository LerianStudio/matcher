// Package extraction provides PostgreSQL repository implementation for ExtractionRequest entities.
package extraction

import (
	"database/sql"
	"encoding/json"
	"fmt"
	"time"

	"github.com/google/uuid"

	"github.com/LerianStudio/matcher/internal/discovery/domain/entities"
	vo "github.com/LerianStudio/matcher/internal/discovery/domain/value_objects"
	pgcommon "github.com/LerianStudio/matcher/internal/shared/adapters/postgres/common"
)

// ExtractionModel is the PostgreSQL representation of an ExtractionRequest.
// IngestionJobID is nullable to allow extraction requests to exist independently
// before being linked to an ingestion job.
type ExtractionModel struct {
	ID             uuid.UUID      `db:"id"`
	ConnectionID   uuid.UUID      `db:"connection_id"`
	IngestionJobID uuid.NullUUID  `db:"ingestion_job_id"` // Nullable
	FetcherJobID   sql.NullString `db:"fetcher_job_id"`
	Tables         []byte         `db:"tables"`  // JSONB
	Filters        []byte         `db:"filters"` // JSONB, nullable (nil slice persists SQL NULL)
	Status         string         `db:"status"`
	ResultPath     sql.NullString `db:"result_path"`
	ErrorMessage   sql.NullString `db:"error_message"`
	CreatedAt      time.Time      `db:"created_at"`
	UpdatedAt      time.Time      `db:"updated_at"`
}

// ToDomain converts the PostgreSQL model to a domain entity.
func (model *ExtractionModel) ToDomain() (*entities.ExtractionRequest, error) {
	if model == nil {
		return nil, ErrModelRequired
	}

	status, err := vo.ParseExtractionStatus(model.Status)
	if err != nil {
		return nil, fmt.Errorf("parse extraction status '%s': %w", model.Status, err)
	}

	var tables map[string]any
	if len(model.Tables) > 0 {
		if err := json.Unmarshal(model.Tables, &tables); err != nil {
			return nil, fmt.Errorf("unmarshal tables: %w", err)
		}
	}

	var filters map[string]any
	if len(model.Filters) > 0 {
		if err := json.Unmarshal(model.Filters, &filters); err != nil {
			return nil, fmt.Errorf("unmarshal filters: %w", err)
		}
	}

	// Convert nullable UUID to domain UUID (zero UUID if NULL).
	var ingestionJobID uuid.UUID
	if model.IngestionJobID.Valid {
		ingestionJobID = model.IngestionJobID.UUID
	}

	return &entities.ExtractionRequest{
		ID:             model.ID,
		ConnectionID:   model.ConnectionID,
		IngestionJobID: ingestionJobID,
		FetcherJobID:   nullStringToString(model.FetcherJobID),
		Tables:         tables,
		Filters:        filters,
		Status:         status,
		ResultPath:     nullStringToString(model.ResultPath),
		ErrorMessage:   nullStringToString(model.ErrorMessage),
		CreatedAt:      model.CreatedAt,
		UpdatedAt:      model.UpdatedAt,
	}, nil
}

// FromDomain converts a domain entity to a PostgreSQL model.
func FromDomain(entity *entities.ExtractionRequest) (*ExtractionModel, error) {
	if entity == nil {
		return nil, ErrEntityRequired
	}

	tablesJSON, err := entity.TablesJSON()
	if err != nil {
		return nil, fmt.Errorf("marshal tables: %w", err)
	}

	filtersJSON, err := entity.FiltersJSON()
	if err != nil {
		return nil, fmt.Errorf("marshal filters: %w", err)
	}

	// Convert domain UUID to nullable UUID (NULL if zero UUID).
	var ingestionJobID uuid.NullUUID
	if entity.IngestionJobID != uuid.Nil {
		ingestionJobID = uuid.NullUUID{UUID: entity.IngestionJobID, Valid: true}
	}

	return &ExtractionModel{
		ID:             entity.ID,
		ConnectionID:   entity.ConnectionID,
		IngestionJobID: ingestionJobID,
		FetcherJobID:   pgcommon.StringToNullString(entity.FetcherJobID),
		Tables:         tablesJSON,
		Filters:        filtersJSON,
		Status:         entity.Status.String(),
		ResultPath:     pgcommon.StringToNullString(entity.ResultPath),
		ErrorMessage:   pgcommon.StringToNullString(entity.ErrorMessage),
		CreatedAt:      entity.CreatedAt,
		UpdatedAt:      entity.UpdatedAt,
	}, nil
}

// nullStringToString converts a sql.NullString to a plain string.
// Returns empty string for invalid (NULL) values.
func nullStringToString(ns sql.NullString) string {
	if !ns.Valid {
		return ""
	}

	return ns.String
}
