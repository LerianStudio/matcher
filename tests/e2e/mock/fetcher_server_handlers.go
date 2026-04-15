//go:build e2e

package mock

import (
	"encoding/json"
	"fmt"
	"net/http"
	"strings"
	"time"
)

// --- Handlers ---

// handleHealth: GET /health
// The real client (IsHealthy) checks for status 200 and parses {"status":"ok"|"healthy"}.
func (s *MockFetcherServer) handleHealth(w http.ResponseWriter, _ *http.Request) {
	s.mu.RLock()
	healthy := s.healthy
	s.mu.RUnlock()

	if !healthy {
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusServiceUnavailable)
		writeJSON(w, map[string]string{"status": "unhealthy"})

		return
	}

	w.Header().Set("Content-Type", "application/json")
	writeJSON(w, map[string]string{"status": "ok"})
}

// handleListConnections: GET /v1/management/connections
// The real Fetcher expects X-Product-Name to be the literal "matcher" (the calling
// service's product name). Tenant isolation comes from JWT, not from this header.
// The mock mirrors this: it returns ALL connections when X-Product-Name == "matcher",
// and returns an empty list otherwise (simulating an unrecognized caller).
// Response shape: {"items":[...], "page":1, "limit":N, "total":N}
func (s *MockFetcherServer) handleListConnections(w http.ResponseWriter, r *http.Request) {
	s.mu.RLock()
	conns := make([]MockConnection, len(s.connections))
	copy(conns, s.connections)
	s.mu.RUnlock()

	// The real Fetcher uses X-Product-Name to identify the calling product.
	// It is always the literal "matcher" — not a per-connection product name.
	productName := r.Header.Get("X-Product-Name")

	type connectionJSON struct {
		ID           string         `json:"id"`
		ConfigName   string         `json:"configName"`
		Type         string         `json:"type"`
		Host         string         `json:"host"`
		Port         int            `json:"port"`
		Schema       string         `json:"schema,omitempty"`
		DatabaseName string         `json:"databaseName"`
		UserName     string         `json:"userName,omitempty"`
		ProductName  string         `json:"productName"`
		Metadata     map[string]any `json:"metadata,omitempty"`
		CreatedAt    string         `json:"createdAt,omitempty"`
		UpdatedAt    string         `json:"updatedAt,omitempty"`
	}

	items := make([]connectionJSON, 0, len(conns))

	// When productName is "matcher" (or empty, for backward compat in tests),
	// return all connections. Any other value yields an empty list.
	if productName != "" && productName != "matcher" {
		w.Header().Set("Content-Type", "application/json")
		writeJSON(w, map[string]any{
			"items": items,
			"page":  1,
			"limit": 0,
			"total": 0,
		})

		return
	}

	for _, c := range conns {
		items = append(items, connectionJSON{
			ID:           c.ID,
			ConfigName:   c.ConfigName,
			Type:         c.Type,
			Host:         c.Host,
			Port:         c.Port,
			Schema:       c.Schema,
			DatabaseName: c.DatabaseName,
			UserName:     c.UserName,
			ProductName:  c.ProductName,
			Metadata:     c.Metadata,
			CreatedAt:    c.CreatedAt.Format(time.RFC3339),
			UpdatedAt:    c.UpdatedAt.Format(time.RFC3339),
		})
	}

	w.Header().Set("Content-Type", "application/json")
	writeJSON(w, map[string]any{
		"items": items,
		"page":  1,
		"limit": len(items),
		"total": len(items),
	})
}

// handleGetSchema: GET /v1/management/connections/{id}/schema
// Response shape: {"id":"...", "configName":"...", "databaseName":"...", "type":"...", "tables":[{"name":"...", "fields":["..."]}]}
func (s *MockFetcherServer) handleGetSchema(w http.ResponseWriter, r *http.Request) {
	connID := r.PathValue("id")

	s.mu.RLock()
	schema, ok := s.schemas[connID]
	s.mu.RUnlock()

	if !ok || schema == nil {
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusNotFound)
		writeJSON(w, map[string]string{"error": "connection not found"})

		return
	}

	type tableJSON struct {
		Name   string   `json:"name"`
		Fields []string `json:"fields"`
	}

	type schemaJSON struct {
		ID           string      `json:"id"`
		ConfigName   string      `json:"configName"`
		DatabaseName string      `json:"databaseName"`
		Type         string      `json:"type"`
		Tables       []tableJSON `json:"tables"`
	}

	tables := make([]tableJSON, 0, len(schema.Tables))
	for _, t := range schema.Tables {
		fields := make([]string, len(t.Fields))
		copy(fields, t.Fields)

		tables = append(tables, tableJSON{
			Name:   t.Name,
			Fields: fields,
		})
	}

	resp := schemaJSON{
		ID:           schema.ID,
		ConfigName:   schema.ConfigName,
		DatabaseName: schema.DatabaseName,
		Type:         schema.Type,
		Tables:       tables,
	}

	w.Header().Set("Content-Type", "application/json")
	writeJSON(w, resp)
}

// handleTestConnection: POST /v1/management/connections/{id}/test
// Response shape: {"status":"success", "message":"...", "latencyMs":N}
func (s *MockFetcherServer) handleTestConnection(w http.ResponseWriter, r *http.Request) {
	connID := r.PathValue("id")

	s.mu.RLock()
	result, ok := s.testResults[connID]
	s.mu.RUnlock()

	if !ok || result == nil {
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusNotFound)
		writeJSON(w, map[string]string{"error": "connection not found"})

		return
	}

	type testJSON struct {
		Status    string `json:"status"`
		Message   string `json:"message,omitempty"`
		LatencyMs int64  `json:"latencyMs"`
	}

	resp := testJSON{
		Status:    result.Status,
		Message:   result.Message,
		LatencyMs: result.LatencyMs,
	}

	w.Header().Set("Content-Type", "application/json")
	writeJSON(w, resp)
}

// handleSubmitExtraction: POST /v1/fetcher
// Accepts JSON body: {"dataRequest":{"mappedFields":{...}, "filters":{...}}, "metadata":{"source":"..."}}
// Response shape: HTTP 202 with {"jobId":"...", "status":"pending", "createdAt":"...", "message":"extraction job accepted"}
//
// The mock generates a job ID from an atomic counter prefixed with "job-",
// or uses metadata.source to produce a semi-deterministic ID for tests.
func (s *MockFetcherServer) handleSubmitExtraction(w http.ResponseWriter, r *http.Request) {
	var reqBody struct {
		DataRequest struct {
			MappedFields map[string]map[string][]string       `json:"mappedFields"`
			Filters      map[string]map[string]map[string]any `json:"filters"`
		} `json:"dataRequest"`
		Metadata map[string]any `json:"metadata"`
	}

	if err := json.NewDecoder(r.Body).Decode(&reqBody); err != nil {
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusBadRequest)
		writeJSON(w, map[string]string{"error": "invalid request body"})

		return
	}

	source, _ := reqBody.Metadata["source"].(string)
	source = strings.TrimSpace(source)

	if source == "" {
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusBadRequest)
		writeJSON(w, map[string]string{"error": "metadata.source is required"})

		return
	}

	// Generate a semi-deterministic job ID from the source name.
	jobID := fmt.Sprintf("job-%s-%d", source, s.jobCounter.Add(1))

	now := time.Now().UTC()

	s.mu.Lock()
	_, exists := s.jobs[jobID]
	if !exists {
		s.jobs[jobID] = &MockExtractionJob{
			ID:           jobID,
			Status:       "pending",
			Metadata:     reqBody.Metadata,
			MappedFields: reqBody.DataRequest.MappedFields,
			CreatedAt:    now,
		}
	}
	s.mu.Unlock()

	type submitJSON struct {
		JobID     string `json:"jobId"`
		Status    string `json:"status"`
		CreatedAt string `json:"createdAt"`
		Message   string `json:"message"`
	}

	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusAccepted)
	writeJSON(w, submitJSON{
		JobID:     jobID,
		Status:    "pending",
		CreatedAt: now.Format(time.RFC3339),
		Message:   "extraction job accepted",
	})
}

// handleGetExtractionStatus: GET /v1/fetcher/{jobId}
// Response shape: {"id":"...", "status":"completed", "resultPath":"...", "resultHmac":"...", "requestHash":"...", "metadata":{...}, "createdAt":"...", "completedAt":"..."}
func (s *MockFetcherServer) handleGetExtractionStatus(w http.ResponseWriter, r *http.Request) {
	jobID := r.PathValue("jobId")

	s.mu.RLock()
	job, ok := s.jobs[jobID]

	var resp map[string]any
	if ok {
		resp = map[string]any{
			"id":        job.ID,
			"status":    job.Status,
			"createdAt": job.CreatedAt.Format(time.RFC3339),
		}

		if job.ResultPath != "" {
			resp["resultPath"] = job.ResultPath
		}

		if job.ResultHmac != "" {
			resp["resultHmac"] = job.ResultHmac
		}

		if job.RequestHash != "" {
			resp["requestHash"] = job.RequestHash
		}

		if len(job.Metadata) > 0 {
			resp["metadata"] = job.Metadata
		}

		if job.CompletedAt != nil {
			resp["completedAt"] = job.CompletedAt.Format(time.RFC3339)
		}
	}

	s.mu.RUnlock()

	if !ok {
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusNotFound)
		writeJSON(w, map[string]string{"error": "job not found"})

		return
	}

	w.Header().Set("Content-Type", "application/json")
	writeJSON(w, resp)
}

// --- Helpers ---

// writeJSON marshals v to JSON and writes it to w. On marshal failure it writes
// a plaintext 500 error. This is a test helper, so the simplified error handling
// is acceptable.
func writeJSON(w http.ResponseWriter, v any) {
	data, err := json.Marshal(v)
	if err != nil {
		http.Error(w, "mock server: json marshal error: "+err.Error(), http.StatusInternalServerError)

		return
	}

	_, _ = w.Write(data)
}
