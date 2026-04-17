package dto

import (
	"time"

	"github.com/google/uuid"

	"github.com/LerianStudio/matcher/internal/discovery/domain/entities"
	sharedPorts "github.com/LerianStudio/matcher/internal/shared/ports"
)

// ExtractionTableResponse is the per-table extraction config returned by the API.
type ExtractionTableResponse struct {
	Columns []string `json:"columns,omitempty"`
}

// DiscoveryStatusResponse is the response for GET /v1/discovery/status.
type DiscoveryStatusResponse struct {
	FetcherHealthy  bool       `json:"fetcherHealthy"`
	ConnectionCount int        `json:"connectionCount"`
	LastSyncAt      *time.Time `json:"lastSyncAt,omitempty"`
}

// ConnectionResponse is a single connection in list/detail responses.
type ConnectionResponse struct {
	ID               uuid.UUID `json:"id"`
	ConfigName       string    `json:"configName"`
	DatabaseType     string    `json:"databaseType"`
	Status           string    `json:"status"`
	SchemaDiscovered bool      `json:"schemaDiscovered"`
	LastSeenAt       time.Time `json:"lastSeenAt"`
}

// ConnectionListResponse wraps a list of connections.
type ConnectionListResponse struct {
	Connections []ConnectionResponse `json:"connections"`
}

// SchemaTableResponse represents a table schema.
type SchemaTableResponse struct {
	TableName string                 `json:"tableName"`
	Columns   []SchemaColumnResponse `json:"columns"`
}

// SchemaColumnResponse represents a column in the API schema response.
// Note: Type and Nullable are historical fields -- the current Fetcher API
// only returns flat field names. These fields remain for backward
// compatibility with persisted data and are omitted from responses when empty.
type SchemaColumnResponse struct {
	Name     string `json:"name"`
	Type     string `json:"type,omitempty"`
	Nullable bool   `json:"nullable,omitempty"`
}

// ConnectionSchemaResponse wraps the schema for a connection.
type ConnectionSchemaResponse struct {
	ConnectionID uuid.UUID             `json:"connectionId"`
	Tables       []SchemaTableResponse `json:"tables"`
}

// RefreshDiscoveryResponse is the response for POST /v1/discovery/refresh.
type RefreshDiscoveryResponse struct {
	ConnectionsSynced int `json:"connectionsSynced"`
}

// TestConnectionResponse is the response for POST /v1/discovery/connections/:connectionId/test.
type TestConnectionResponse struct {
	ConnectionID uuid.UUID `json:"connectionId"`
	Healthy      bool      `json:"healthy"`
	LatencyMs    int64     `json:"latencyMs"`
	ErrorMessage string    `json:"errorMessage,omitempty"`
}

// ExtractionRequestResponse is the response for extraction request endpoints.
//
// The bridge_* and custody_* fields surface the Matcher-side bridging
// pipeline's retry-and-failure state so operators drilling into a failed
// extraction see the failure class, message, attempt count, and failure
// timestamp without a second call. They are independent of Status — Status
// describes the upstream Fetcher pipeline while BridgeLastError describes
// what happened when the Matcher worker tried to retrieve, verify, custody,
// ingest, and link the extraction's output. All bridge/custody fields are
// omitted from responses when zero/nil.
//
// BridgeLastErrorMessage is an appended-history string: the first (primary)
// failure reason stays at the head; subsequent same-class failures are
// appended with a '→ ' separator. BridgeFailedAt is frozen at the first
// failure so it marks the definitive terminal timestamp; later history
// entries are progress notes. Total length bounded — overflow truncates the
// tail with '...' so the primary reason stays intact.
type ExtractionRequestResponse struct {
	ID                     uuid.UUID                          `json:"id"`
	ConnectionID           uuid.UUID                          `json:"connectionId"`
	IngestionJobID         *uuid.UUID                         `json:"ingestionJobId,omitempty"`
	Tables                 map[string]ExtractionTableResponse `json:"tables"`
	StartDate              string                             `json:"startDate,omitempty"`
	EndDate                string                             `json:"endDate,omitempty"`
	Filters                *sharedPorts.ExtractionFilters     `json:"filters,omitempty"`
	Status                 string                             `json:"status"`
	ErrorMessage           string                             `json:"errorMessage,omitempty"`
	CreatedAt              time.Time                          `json:"createdAt"`
	UpdatedAt              time.Time                          `json:"updatedAt"`
	BridgeAttempts         int                                `json:"bridgeAttempts,omitempty"`
	BridgeLastError        string                             `json:"bridgeLastError,omitempty"`
	BridgeLastErrorMessage string                             `json:"bridgeLastErrorMessage,omitempty"` // appended-history; see type docstring
	BridgeFailedAt         *time.Time                         `json:"bridgeFailedAt,omitempty"`
	CustodyDeletedAt       *time.Time                         `json:"custodyDeletedAt,omitempty"`
}

// ConnectionFromEntity converts a domain entity to a response DTO.
func ConnectionFromEntity(entity *entities.FetcherConnection) ConnectionResponse {
	if entity == nil {
		return ConnectionResponse{}
	}

	return ConnectionResponse{
		ID:               entity.ID,
		ConfigName:       entity.ConfigName,
		DatabaseType:     entity.DatabaseType,
		Status:           entity.Status.String(),
		SchemaDiscovered: entity.SchemaDiscovered,
		LastSeenAt:       entity.LastSeenAt,
	}
}

// ExtractionRequestFromEntity converts an extraction request entity to a response DTO.
func ExtractionRequestFromEntity(entity *entities.ExtractionRequest) ExtractionRequestResponse {
	if entity == nil {
		return ExtractionRequestResponse{}
	}

	var ingestionJobID *uuid.UUID

	if entity.IngestionJobID != uuid.Nil {
		jobID := entity.IngestionJobID
		ingestionJobID = &jobID
	}

	filters, _ := sharedPorts.ExtractionFiltersFromMap(entity.Filters)

	// BridgeFailedAt is a value type on the entity (zero when no terminal
	// bridge failure) but a pointer in the DTO so omitempty keeps the field
	// out of the response for extractions with no bridge state.
	var bridgeFailedAt *time.Time
	if !entity.BridgeFailedAt.IsZero() {
		failedAt := entity.BridgeFailedAt
		bridgeFailedAt = &failedAt
	}

	return ExtractionRequestResponse{
		ID:                     entity.ID,
		ConnectionID:           entity.ConnectionID,
		IngestionJobID:         ingestionJobID,
		Tables:                 extractionTablesFromEntity(entity.Tables),
		StartDate:              entity.StartDate,
		EndDate:                entity.EndDate,
		Filters:                filters,
		Status:                 entity.Status.String(),
		ErrorMessage:           entity.ErrorMessage,
		CreatedAt:              entity.CreatedAt,
		UpdatedAt:              entity.UpdatedAt,
		BridgeAttempts:         entity.BridgeAttempts,
		BridgeLastError:        entity.BridgeLastError.String(),
		BridgeLastErrorMessage: entity.BridgeLastErrorMessage,
		BridgeFailedAt:         bridgeFailedAt,
		CustodyDeletedAt:       entity.CustodyDeletedAt,
	}
}

func extractionTablesFromEntity(tables map[string]any) map[string]ExtractionTableResponse {
	if len(tables) == 0 {
		return map[string]ExtractionTableResponse{}
	}

	result := make(map[string]ExtractionTableResponse, len(tables))
	for tableName, rawCfg := range tables {
		cfgMap, ok := rawCfg.(map[string]any)
		if !ok {
			result[tableName] = ExtractionTableResponse{}
			continue
		}

		rawColumns, ok := cfgMap["columns"]
		if !ok {
			result[tableName] = ExtractionTableResponse{}
			continue
		}

		result[tableName] = ExtractionTableResponse{Columns: extractionColumnsFromAny(rawColumns)}
	}

	return result
}

func extractionColumnsFromAny(rawColumns any) []string {
	columns := make([]string, 0)

	switch typed := rawColumns.(type) {
	case []string:
		columns = append(columns, typed...)
	case []any:
		for _, rawColumn := range typed {
			column, isString := rawColumn.(string)
			if !isString {
				continue
			}

			columns = append(columns, column)
		}
	}

	return columns
}
