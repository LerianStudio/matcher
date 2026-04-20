package fetcher

import "reflect"

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

// fetcherFilterCondition mirrors Fetcher's FilterCondition struct
// (fetcher/pkg/model/job/job_queue.go). Each field is an operator with a slice
// of values. Callers typically use Eq for equality filters, but the full
// operator set is available for forward compatibility.
type fetcherFilterCondition struct {
	Eq      []any `json:"eq,omitempty"`
	Gt      []any `json:"gt,omitempty"`
	Gte     []any `json:"gte,omitempty"`
	Lt      []any `json:"lt,omitempty"`
	Lte     []any `json:"lte,omitempty"`
	Between []any `json:"between,omitempty"`
	In      []any `json:"in,omitempty"`
	Nin     []any `json:"nin,omitempty"`
	Ne      []any `json:"ne,omitempty"`
	Like    []any `json:"like,omitempty"`
}

// fetcherDataRequest is the nested data request within an extraction submission.
// Filters uses the typed fetcherFilterCondition to produce a symmetric wire format
// matching the response-side shape returned by Fetcher's status endpoint.
type fetcherDataRequest struct {
	MappedFields map[string]map[string][]string                          `json:"mappedFields"`
	Filters      map[string]map[string]map[string]fetcherFilterCondition `json:"filters,omitempty"`
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
// MappedFields and Filters echo what was submitted (potentially with server-side
// transformation, e.g. unqualified table names auto-qualified to schema.table).
type fetcherExtractionStatusResponse struct {
	ID           string                         `json:"id"`
	Status       string                         `json:"status"`
	ResultPath   string                         `json:"resultPath,omitempty"`
	ResultHmac   string                         `json:"resultHmac,omitempty"`
	RequestHash  string                         `json:"requestHash,omitempty"`
	MappedFields map[string]map[string][]string `json:"mappedFields,omitempty"`
	Metadata     map[string]any                 `json:"metadata,omitempty"`
	CreatedAt    string                         `json:"createdAt,omitempty"`
	CompletedAt  string                         `json:"completedAt,omitempty"`
}

// filterConditionFromMap converts an untyped map[string]any (Fetcher wire
// format) back into a fetcherFilterCondition. Unknown keys are silently
// ignored for forward compatibility.
func filterConditionFromMap(m map[string]any) fetcherFilterCondition {
	fc := fetcherFilterCondition{}

	for key, val := range m {
		slice, ok := normalizeFilterValues(val)
		if !ok {
			continue
		}

		switch key {
		case "eq":
			fc.Eq = slice
		case "gt":
			fc.Gt = slice
		case "gte":
			fc.Gte = slice
		case "lt":
			fc.Lt = slice
		case "lte":
			fc.Lte = slice
		case "between":
			fc.Between = slice
		case "in":
			fc.In = slice
		case "nin":
			fc.Nin = slice
		case "ne":
			fc.Ne = slice
		case "like":
			fc.Like = slice
		}
	}

	return fc
}

func normalizeFilterValues(val any) ([]any, bool) {
	if direct, ok := val.([]any); ok {
		return append([]any(nil), direct...), true
	}

	rv := reflect.ValueOf(val)
	if !rv.IsValid() {
		return nil, false
	}

	kind := rv.Kind()
	if kind != reflect.Slice && kind != reflect.Array {
		return []any{val}, true
	}

	if kind == reflect.Slice && rv.IsNil() {
		return []any{}, true
	}

	values := make([]any, 0, rv.Len())
	for i := 0; i < rv.Len(); i++ {
		values = append(values, rv.Index(i).Interface())
	}

	return values, true
}
