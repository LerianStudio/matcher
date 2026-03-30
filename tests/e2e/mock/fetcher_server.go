//go:build e2e

// Package mock provides test doubles for external services used in E2E tests.
package mock

import (
	"context"
	"encoding/json"
	"fmt"
	"net"
	"net/http"
	"strings"
	"sync"
	"time"

	"github.com/LerianStudio/lib-commons/v4/commons/runtime"
)

// MockFetcherServer is an in-process HTTP server that mimics the Fetcher REST API.
// Tests can manipulate its state (connections, schemas, jobs, health) through
// thread-safe setter methods, and the mock will return corresponding JSON
// responses that the real HTTPFetcherClient can parse without error.
type MockFetcherServer struct {
	mu       sync.RWMutex
	server   *http.Server
	listener net.Listener
	baseURL  string

	// Controllable state
	healthy     bool
	connections []MockConnection
	schemas     map[string]*MockSchema        // connectionID -> schema
	testResults map[string]*MockTestResult    // connectionID -> test result
	jobs        map[string]*MockExtractionJob // jobID -> job state
}

// MockConnection represents a database connection as returned by Fetcher.
type MockConnection struct {
	OrgID        string
	ID           string
	ConfigName   string
	DatabaseType string
	Host         string
	Port         int
	DatabaseName string
	ProductName  string
	Status       string
}

// MockSchema represents a connection's discovered schema.
type MockSchema struct {
	ConnectionID string
	Tables       []MockTable
}

// MockTable represents a database table in a schema response.
type MockTable struct {
	TableName string
	Columns   []MockColumn
}

// MockColumn represents a column in a table schema.
type MockColumn struct {
	Name     string
	Type     string
	Nullable bool
}

// MockTestResult represents the outcome of testing a connection.
type MockTestResult struct {
	Healthy      bool
	LatencyMs    int64
	ErrorMessage string
}

// MockExtractionJob represents the state of an extraction job.
type MockExtractionJob struct {
	JobID        string
	Status       string
	Progress     int
	ResultPath   string
	ErrorMessage string
}

// NewMockFetcherServer creates a new mock Fetcher server with healthy defaults.
func NewMockFetcherServer() *MockFetcherServer {
	return &MockFetcherServer{
		healthy:     true,
		connections: make([]MockConnection, 0),
		schemas:     make(map[string]*MockSchema),
		testResults: make(map[string]*MockTestResult),
		jobs:        make(map[string]*MockExtractionJob),
	}
}

// StartOnPort begins listening on the specified port (0 means random).
// Returns the base URL or an error.
func (s *MockFetcherServer) StartOnPort(port int) (string, error) {
	mux := http.NewServeMux()
	s.registerRoutes(mux)

	addr := fmt.Sprintf("0.0.0.0:%d", port)

	var err error

	s.listener, err = net.Listen("tcp", addr)
	if err != nil {
		return "", fmt.Errorf("mock fetcher listen: %w", err)
	}

	tcpAddr, _ := s.listener.Addr().(*net.TCPAddr)
	s.baseURL = fmt.Sprintf("http://127.0.0.1:%d", tcpAddr.Port)

	s.server = &http.Server{
		Handler:      mux,
		ReadTimeout:  30 * time.Second,
		WriteTimeout: 30 * time.Second,
	}

	runtime.SafeGoWithContextAndComponent(
		context.Background(),
		nil,
		"e2e-mock",
		"fetcher_server.serve",
		runtime.KeepRunning,
		func(_ context.Context) {
			// Serve returns ErrServerClosed on graceful shutdown; ignore it.
			_ = s.server.Serve(s.listener)
		},
	)

	return s.baseURL, nil
}

// Stop gracefully shuts down the mock server.
func (s *MockFetcherServer) Stop() error {
	if s.server == nil {
		return nil
	}

	return s.server.Close()
}

// Reset clears all state back to defaults (healthy, no connections/schemas/jobs).
func (s *MockFetcherServer) Reset() {
	s.mu.Lock()
	defer s.mu.Unlock()

	s.healthy = true
	s.connections = make([]MockConnection, 0)
	s.schemas = make(map[string]*MockSchema)
	s.testResults = make(map[string]*MockTestResult)
	s.jobs = make(map[string]*MockExtractionJob)
}

// --- State manipulation ---

// SetHealthy controls whether GET /health returns "ok" (200) or "unhealthy" (503).
func (s *MockFetcherServer) SetHealthy(healthy bool) {
	s.mu.Lock()
	defer s.mu.Unlock()

	s.healthy = healthy
}

// AddConnection adds a connection that will appear in ListConnections responses.
func (s *MockFetcherServer) AddConnection(conn MockConnection) {
	s.mu.Lock()
	defer s.mu.Unlock()

	s.connections = append(s.connections, conn)
}

// SetSchema sets the schema response for a given connection ID.
// A defensive copy is made so callers cannot mutate server state after the call.
func (s *MockFetcherServer) SetSchema(connectionID string, schema *MockSchema) {
	s.mu.Lock()
	defer s.mu.Unlock()

	if schema == nil {
		delete(s.schemas, connectionID)
		return
	}

	cp := &MockSchema{ConnectionID: schema.ConnectionID}
	for _, tbl := range schema.Tables {
		cols := make([]MockColumn, len(tbl.Columns))
		copy(cols, tbl.Columns)
		cp.Tables = append(cp.Tables, MockTable{TableName: tbl.TableName, Columns: cols})
	}

	s.schemas[connectionID] = cp
}

// SetTestResult sets the test-connection response for a given connection ID.
// A defensive copy is made so callers cannot mutate server state after the call.
func (s *MockFetcherServer) SetTestResult(connectionID string, result *MockTestResult) {
	s.mu.Lock()
	defer s.mu.Unlock()

	if result == nil {
		delete(s.testResults, connectionID)
		return
	}

	cp := &MockTestResult{
		Healthy:      result.Healthy,
		LatencyMs:    result.LatencyMs,
		ErrorMessage: result.ErrorMessage,
	}
	s.testResults[connectionID] = cp
}

// AddJob adds an extraction job that can be polled by its JobID.
func (s *MockFetcherServer) AddJob(job MockExtractionJob) {
	s.mu.Lock()
	defer s.mu.Unlock()

	s.jobs[job.JobID] = &job
}

// SetJobStatus updates the status and progress of an existing extraction job.
func (s *MockFetcherServer) SetJobStatus(jobID string, status string, progress int) {
	s.mu.Lock()
	defer s.mu.Unlock()

	if job, ok := s.jobs[jobID]; ok {
		job.Status = status
		job.Progress = progress
	}
}

// --- Route registration ---

func (s *MockFetcherServer) registerRoutes(mux *http.ServeMux) {
	// Go 1.22+ ServeMux supports method+path patterns.
	mux.HandleFunc("GET /health", s.handleHealth)
	mux.HandleFunc("GET /api/v1/connections", s.handleListConnections)
	mux.HandleFunc("POST /api/v1/extractions", s.handleSubmitExtraction)

	// Paths with path segments that need manual extraction:
	// Go 1.22 ServeMux supports {name} wildcards.
	mux.HandleFunc("GET /api/v1/connections/{id}/schema", s.handleGetSchema)
	mux.HandleFunc("POST /api/v1/connections/{id}/test", s.handleTestConnection)
	mux.HandleFunc("GET /api/v1/extractions/{jobId}", s.handleGetExtractionStatus)
}

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

// handleListConnections: GET /api/v1/connections?orgId=X
// Response shape: {"connections":[...]}
// Each connection: {"id","configName","databaseType","host","port","databaseName","productName","status"}
func (s *MockFetcherServer) handleListConnections(w http.ResponseWriter, r *http.Request) {
	s.mu.RLock()
	conns := make([]MockConnection, len(s.connections))
	copy(conns, s.connections)
	s.mu.RUnlock()

	orgID := r.URL.Query().Get("orgId")
	if orgID == "" {
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusBadRequest)
		writeJSON(w, map[string]string{"error": "orgId query parameter is required"})

		return
	}

	type connectionJSON struct {
		ID           string `json:"id"`
		ConfigName   string `json:"configName"`
		DatabaseType string `json:"databaseType"`
		Host         string `json:"host"`
		Port         int    `json:"port"`
		DatabaseName string `json:"databaseName"`
		ProductName  string `json:"productName"`
		Status       string `json:"status"`
	}

	items := make([]connectionJSON, 0, len(conns))
	for _, c := range conns {
		if c.OrgID != orgID {
			continue
		}

		items = append(items, connectionJSON{
			ID:           c.ID,
			ConfigName:   c.ConfigName,
			DatabaseType: c.DatabaseType,
			Host:         c.Host,
			Port:         c.Port,
			DatabaseName: c.DatabaseName,
			ProductName:  c.ProductName,
			Status:       c.Status,
		})
	}

	w.Header().Set("Content-Type", "application/json")
	writeJSON(w, map[string]any{"connections": items})
}

// handleGetSchema: GET /api/v1/connections/{id}/schema
// Response shape: {"connectionId":"...","tables":[{"tableName":"...","columns":[{"name","type","nullable"}]}]}
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

	type columnJSON struct {
		Name     string `json:"name"`
		Type     string `json:"type"`
		Nullable bool   `json:"nullable"`
	}

	type tableJSON struct {
		TableName string       `json:"tableName"`
		Columns   []columnJSON `json:"columns"`
	}

	type schemaJSON struct {
		ConnectionID string      `json:"connectionId"`
		Tables       []tableJSON `json:"tables"`
	}

	tables := make([]tableJSON, 0, len(schema.Tables))
	for _, t := range schema.Tables {
		cols := make([]columnJSON, 0, len(t.Columns))
		for _, c := range t.Columns {
			cols = append(cols, columnJSON{
				Name:     c.Name,
				Type:     c.Type,
				Nullable: c.Nullable,
			})
		}

		tables = append(tables, tableJSON{
			TableName: t.TableName,
			Columns:   cols,
		})
	}

	resp := schemaJSON{
		ConnectionID: schema.ConnectionID,
		Tables:       tables,
	}

	w.Header().Set("Content-Type", "application/json")
	writeJSON(w, resp)
}

// handleTestConnection: POST /api/v1/connections/{id}/test
// Response shape: {"connectionId":"...","healthy":true,"latencyMs":42,"errorMessage":"..."}
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
		ConnectionID string `json:"connectionId"`
		Healthy      bool   `json:"healthy"`
		LatencyMs    int64  `json:"latencyMs"`
		ErrorMessage string `json:"errorMessage,omitempty"`
	}

	resp := testJSON{
		ConnectionID: connID,
		Healthy:      result.Healthy,
		LatencyMs:    result.LatencyMs,
		ErrorMessage: result.ErrorMessage,
	}

	w.Header().Set("Content-Type", "application/json")
	writeJSON(w, resp)
}

// handleSubmitExtraction: POST /api/v1/extractions
// Accepts JSON body with connectionId, tables, filters.
// Response shape: {"jobId":"..."}
//
// The mock generates a job ID from the connection ID in the request body,
// or returns a default if the body cannot be parsed. This lets tests
// pre-populate job state via AddJob with a known ID.
func (s *MockFetcherServer) handleSubmitExtraction(w http.ResponseWriter, r *http.Request) {
	// Parse request body to extract connectionId (used for deterministic job ID generation).
	var reqBody struct {
		ConnectionID string `json:"connectionId"`
	}

	if err := json.NewDecoder(r.Body).Decode(&reqBody); err != nil {
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusBadRequest)
		writeJSON(w, map[string]string{"error": "invalid request body"})

		return
	}

	reqBody.ConnectionID = strings.TrimSpace(reqBody.ConnectionID)
	if reqBody.ConnectionID == "" {
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusBadRequest)
		writeJSON(w, map[string]string{"error": "invalid connectionId"})

		return
	}

	// Generate a deterministic job ID so tests can AddJob before the call.
	jobID := "job-" + reqBody.ConnectionID

	s.mu.Lock()
	_, exists := s.jobs[jobID]
	if !exists {
		s.jobs[jobID] = &MockExtractionJob{
			JobID:    jobID,
			Status:   "PENDING",
			Progress: 0,
		}
	}
	s.mu.Unlock()

	type submitJSON struct {
		JobID string `json:"jobId"`
	}

	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusCreated)
	writeJSON(w, submitJSON{JobID: jobID})
}

// handleGetExtractionStatus: GET /api/v1/extractions/{jobId}
// Response shape: {"jobId":"...","status":"COMPLETE","progress":100,"resultPath":"/data/...","errorMessage":"..."}
func (s *MockFetcherServer) handleGetExtractionStatus(w http.ResponseWriter, r *http.Request) {
	jobID := r.PathValue("jobId")

	s.mu.RLock()
	job, ok := s.jobs[jobID]

	var resp extractionStatusJSON
	if ok {
		resp = extractionStatusJSON{
			JobID:        job.JobID,
			Status:       job.Status,
			Progress:     job.Progress,
			ResultPath:   job.ResultPath,
			ErrorMessage: job.ErrorMessage,
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

// extractionStatusJSON matches the exact shape of fetcherExtractionStatusResponse in types.go.
type extractionStatusJSON struct {
	JobID        string `json:"jobId"`
	Status       string `json:"status"`
	Progress     int    `json:"progress"`
	ResultPath   string `json:"resultPath,omitempty"`
	ErrorMessage string `json:"errorMessage,omitempty"`
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
