package fetcher

import sharedPorts "github.com/LerianStudio/matcher/internal/shared/ports"

// fetcherHealthResponse maps to GET /health.
type fetcherHealthResponse struct {
	Status string `json:"status"`
}

// fetcherConnectionResponse maps to a single connection in GET /api/v1/connections.
type fetcherConnectionResponse struct {
	ID           string `json:"id"`
	ConfigName   string `json:"configName"`
	DatabaseType string `json:"databaseType"`
	Host         string `json:"host"`
	Port         int    `json:"port"`
	DatabaseName string `json:"databaseName"`
	ProductName  string `json:"productName"`
	Status       string `json:"status"`
}

// fetcherConnectionListResponse maps to the GET /api/v1/connections response.
type fetcherConnectionListResponse struct {
	Connections []fetcherConnectionResponse `json:"connections"`
}

// fetcherColumnResponse maps to a column in schema response.
type fetcherColumnResponse struct {
	Name     string `json:"name"`
	Type     string `json:"type"`
	Nullable bool   `json:"nullable"`
}

// fetcherTableResponse maps to a table in schema response.
type fetcherTableResponse struct {
	TableName string                  `json:"tableName"`
	Columns   []fetcherColumnResponse `json:"columns"`
}

// fetcherSchemaResponse maps to GET /api/v1/connections/:id/schema.
type fetcherSchemaResponse struct {
	ConnectionID string                 `json:"connectionId"`
	Tables       []fetcherTableResponse `json:"tables"`
}

// fetcherTestResponse maps to POST /api/v1/connections/:id/test.
type fetcherTestResponse struct {
	ConnectionID string `json:"connectionId"`
	Healthy      bool   `json:"healthy"`
	LatencyMs    int64  `json:"latencyMs"`
	ErrorMessage string `json:"errorMessage,omitempty"`
}

// fetcherExtractionSubmitRequest is the request body for POST /api/v1/extractions.
type fetcherExtractionSubmitRequest struct {
	ConnectionID string                            `json:"connectionId"`
	Tables       map[string]fetcherExtractionTable `json:"tables"`
	Filters      *sharedPorts.ExtractionFilters    `json:"filters,omitempty"`
}

// fetcherExtractionTable configures extraction for a single table.
type fetcherExtractionTable struct {
	Columns   []string `json:"columns,omitempty"`
	StartDate string   `json:"startDate,omitempty"`
	EndDate   string   `json:"endDate,omitempty"`
}

// fetcherExtractionSubmitResponse maps to POST /api/v1/extractions response.
type fetcherExtractionSubmitResponse struct {
	JobID string `json:"jobId"`
}

// fetcherExtractionStatusResponse maps to GET /api/v1/extractions/:jobId response.
type fetcherExtractionStatusResponse struct {
	JobID        string `json:"jobId"`
	Status       string `json:"status"`
	Progress     int    `json:"progress"`
	ResultPath   string `json:"resultPath,omitempty"`
	ErrorMessage string `json:"errorMessage,omitempty"`
}
