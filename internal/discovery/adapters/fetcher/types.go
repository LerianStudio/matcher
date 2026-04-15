package fetcher

// fetcherHealthResponse maps to GET /health.
type fetcherHealthResponse struct {
	Status string `json:"status"`
}

// fetcherConnectionResponse maps to a single connection in GET /v1/management/connections.
type fetcherConnectionResponse struct {
	ID           string         `json:"id"`
	ConfigName   string         `json:"configName"`
	Type         string         `json:"type"`
	Host         string         `json:"host"`
	Port         int            `json:"port"`
	Schema       string         `json:"schema"`
	DatabaseName string         `json:"databaseName"`
	UserName     string         `json:"userName"`
	ProductName  string         `json:"productName"`
	SSL          bool           `json:"ssl,omitempty"`
	Metadata     map[string]any `json:"metadata,omitempty"`
	CreatedAt    string         `json:"createdAt,omitempty"`
	UpdatedAt    string         `json:"updatedAt,omitempty"`
}

// fetcherConnectionListResponse maps to the GET /v1/management/connections response.
type fetcherConnectionListResponse struct {
	Items []fetcherConnectionResponse `json:"items"`
	Page  int                         `json:"page"`
	Limit int                         `json:"limit"`
	Total int                         `json:"total"`
}

// fetcherTableResponse maps to a table in the schema response.
type fetcherTableResponse struct {
	Name   string   `json:"name"`
	Fields []string `json:"fields"`
}

// fetcherSchemaResponse maps to GET /v1/management/connections/:id/schema.
type fetcherSchemaResponse struct {
	ID           string                 `json:"id"`
	ConfigName   string                 `json:"configName"`
	DatabaseName string                 `json:"databaseName"`
	Type         string                 `json:"type"`
	Tables       []fetcherTableResponse `json:"tables"`
}

// fetcherTestResponse maps to POST /v1/management/connections/:id/test.
type fetcherTestResponse struct {
	Status    string `json:"status"`
	Message   string `json:"message,omitempty"`
	LatencyMs int64  `json:"latencyMs"`
}

// fetcherDataRequest is the nested data request within an extraction submission.
type fetcherDataRequest struct {
	MappedFields map[string]map[string][]string       `json:"mappedFields"`
	Filters      map[string]map[string]map[string]any `json:"filters,omitempty"`
}

// fetcherExtractionSubmitRequest is the request body for POST /v1/fetcher.
type fetcherExtractionSubmitRequest struct {
	DataRequest fetcherDataRequest `json:"dataRequest"`
	Metadata    map[string]any     `json:"metadata"`
}

// fetcherExtractionSubmitResponse maps to POST /v1/fetcher response.
type fetcherExtractionSubmitResponse struct {
	JobID     string `json:"jobId"`
	Status    string `json:"status,omitempty"`
	CreatedAt string `json:"createdAt,omitempty"`
	Message   string `json:"message,omitempty"`
}

// fetcherExtractionStatusResponse maps to GET /v1/fetcher/:jobId response.
type fetcherExtractionStatusResponse struct {
	ID          string         `json:"id"`
	Status      string         `json:"status"`
	ResultPath  string         `json:"resultPath,omitempty"`
	ResultHmac  string         `json:"resultHmac,omitempty"`
	RequestHash string         `json:"requestHash,omitempty"`
	Metadata    map[string]any `json:"metadata,omitempty"`
	CreatedAt   string         `json:"createdAt,omitempty"`
	CompletedAt string         `json:"completedAt,omitempty"`
}
