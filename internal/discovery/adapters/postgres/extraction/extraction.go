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
//
// Bridge* fields (T-005) capture the Matcher-side bridge pipeline's retry-and-
// failure state. BridgeLastError / BridgeLastErrorMessage / BridgeFailedAt are
// nullable: NULL means "no terminal failure yet, still retryable". Non-NULL
// means the bridge worker has given up on this row and excluded it from
// future eligibility scans.
type ExtractionModel struct {
	ID                     uuid.UUID      `db:"id"`
	ConnectionID           uuid.UUID      `db:"connection_id"`
	IngestionJobID         uuid.NullUUID  `db:"ingestion_job_id"` // Nullable
	FetcherJobID           sql.NullString `db:"fetcher_job_id"`
	Tables                 []byte         `db:"tables"` // JSONB
	StartDate              sql.NullString `db:"start_date"`
	EndDate                sql.NullString `db:"end_date"`
	Filters                []byte         `db:"filters"` // JSONB, nullable (nil slice persists SQL NULL)
	Status                 string         `db:"status"`
	ResultPath             sql.NullString `db:"result_path"`
	ErrorMessage           sql.NullString `db:"error_message"`
	CreatedAt              time.Time      `db:"created_at"`
	UpdatedAt              time.Time      `db:"updated_at"`
	BridgeAttempts         int            `db:"bridge_attempts"`
	BridgeLastError        sql.NullString `db:"bridge_last_error"`         // Nullable
	BridgeLastErrorMessage sql.NullString `db:"bridge_last_error_message"` // Nullable
	BridgeFailedAt         sql.NullTime   `db:"bridge_failed_at"`          // Nullable
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

	var bridgeLastError vo.BridgeErrorClass
	if model.BridgeLastError.Valid && model.BridgeLastError.String != "" {
		parsed, parseErr := vo.ParseBridgeErrorClass(model.BridgeLastError.String)
		if parseErr != nil {
			return nil, fmt.Errorf("parse bridge_last_error %q: %w", model.BridgeLastError.String, parseErr)
		}

		bridgeLastError = parsed
	}

	var bridgeFailedAt time.Time
	if model.BridgeFailedAt.Valid {
		bridgeFailedAt = model.BridgeFailedAt.Time
	}

	return &entities.ExtractionRequest{
		ID:                     model.ID,
		ConnectionID:           model.ConnectionID,
		IngestionJobID:         ingestionJobID,
		FetcherJobID:           nullStringToString(model.FetcherJobID),
		Tables:                 tables,
		StartDate:              nullStringToString(model.StartDate),
		EndDate:                nullStringToString(model.EndDate),
		Filters:                filters,
		Status:                 status,
		ResultPath:             nullStringToString(model.ResultPath),
		ErrorMessage:           nullStringToString(model.ErrorMessage),
		CreatedAt:              model.CreatedAt,
		UpdatedAt:              model.UpdatedAt,
		BridgeAttempts:         model.BridgeAttempts,
		BridgeLastError:        bridgeLastError,
		BridgeLastErrorMessage: nullStringToString(model.BridgeLastErrorMessage),
		BridgeFailedAt:         bridgeFailedAt,
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

	var bridgeLastError sql.NullString
	if entity.BridgeLastError != "" {
		bridgeLastError = sql.NullString{String: entity.BridgeLastError.String(), Valid: true}
	}

	var bridgeFailedAt sql.NullTime
	if !entity.BridgeFailedAt.IsZero() {
		bridgeFailedAt = sql.NullTime{Time: entity.BridgeFailedAt, Valid: true}
	}

	return &ExtractionModel{
		ID:                     entity.ID,
		ConnectionID:           entity.ConnectionID,
		IngestionJobID:         ingestionJobID,
		FetcherJobID:           pgcommon.StringToNullString(entity.FetcherJobID),
		Tables:                 tablesJSON,
		StartDate:              pgcommon.StringToNullString(entity.StartDate),
		EndDate:                pgcommon.StringToNullString(entity.EndDate),
		Filters:                filtersJSON,
		Status:                 entity.Status.String(),
		ResultPath:             pgcommon.StringToNullString(entity.ResultPath),
		ErrorMessage:           pgcommon.StringToNullString(entity.ErrorMessage),
		CreatedAt:              entity.CreatedAt,
		UpdatedAt:              entity.UpdatedAt,
		BridgeAttempts:         entity.BridgeAttempts,
		BridgeLastError:        bridgeLastError,
		BridgeLastErrorMessage: pgcommon.StringToNullString(entity.BridgeLastErrorMessage),
		BridgeFailedAt:         bridgeFailedAt,
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
