package dto

import (
	"time"

	"github.com/google/uuid"

	"github.com/LerianStudio/matcher/internal/discovery/domain/entities"
)

// DiscoveryStatusResponse is the response for GET /v1/discovery/status.
type DiscoveryStatusResponse struct {
	FetcherHealthy  bool      `json:"fetcherHealthy"`
	ConnectionCount int       `json:"connectionCount"`
	LastSyncAt      time.Time `json:"lastSyncAt,omitempty"`
}

// ConnectionResponse is a single connection in list/detail responses.
type ConnectionResponse struct {
	ID               uuid.UUID `json:"id"`
	FetcherConnID    string    `json:"fetcherConnId"`
	ConfigName       string    `json:"configName"`
	DatabaseType     string    `json:"databaseType"`
	Host             string    `json:"host"`
	Port             int       `json:"port"`
	DatabaseName     string    `json:"databaseName"`
	ProductName      string    `json:"productName"`
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

// SchemaColumnResponse represents a column.
type SchemaColumnResponse struct {
	Name     string `json:"name"`
	Type     string `json:"type"`
	Nullable bool   `json:"nullable"`
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
	ConnectionID  uuid.UUID `json:"connectionId"`
	FetcherConnID string    `json:"fetcherConnId"`
	Healthy       bool      `json:"healthy"`
	LatencyMs     int64     `json:"latencyMs"`
	ErrorMessage  string    `json:"errorMessage,omitempty"`
}

// ConnectionFromEntity converts a domain entity to a response DTO.
func ConnectionFromEntity(entity *entities.FetcherConnection) ConnectionResponse {
	if entity == nil {
		return ConnectionResponse{}
	}

	return ConnectionResponse{
		ID:               entity.ID,
		FetcherConnID:    entity.FetcherConnID,
		ConfigName:       entity.ConfigName,
		DatabaseType:     entity.DatabaseType,
		Host:             entity.Host,
		Port:             entity.Port,
		DatabaseName:     entity.DatabaseName,
		ProductName:      entity.ProductName,
		Status:           entity.Status.String(),
		SchemaDiscovered: entity.SchemaDiscovered,
		LastSeenAt:       entity.LastSeenAt,
	}
}
