//go:build e2e

// Package mock provides test doubles for external services used in E2E tests.
package mock

import (
	"context"
	"fmt"
	"net"
	"net/http"
	"sync"
	"sync/atomic"
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

	// jobCounter generates deterministic job IDs for extraction submissions.
	jobCounter atomic.Int64

	// Controllable state
	healthy     bool
	connections []MockConnection
	schemas     map[string]*MockSchema        // connectionID -> schema
	testResults map[string]*MockTestResult    // connectionID -> test result
	jobs        map[string]*MockExtractionJob // jobID -> job state
}

// MockConnection represents a database connection as returned by Fetcher.
type MockConnection struct {
	ID           string
	ConfigName   string
	Type         string // e.g. "postgresql", "mysql"
	Host         string
	Port         int
	Schema       string
	DatabaseName string
	UserName     string
	ProductName  string
	Metadata     map[string]any
	CreatedAt    time.Time
	UpdatedAt    time.Time
}

// MockSchema represents a connection's discovered schema.
type MockSchema struct {
	ID           string
	ConfigName   string
	DatabaseName string
	Type         string
	Tables       []MockTable
}

// MockTable represents a database table in a schema response.
type MockTable struct {
	Name   string
	Fields []string
}

// MockTestResult represents the outcome of testing a connection.
type MockTestResult struct {
	Status    string // "success" or "error"
	Message   string
	LatencyMs int64
}

// MockExtractionJob represents the state of an extraction job.
type MockExtractionJob struct {
	ID           string
	Status       string // lowercase: pending, processing, completed, failed
	ResultPath   string
	ResultHmac   string
	RequestHash  string
	Metadata     map[string]any
	MappedFields map[string]map[string][]string
	CreatedAt    time.Time
	CompletedAt  *time.Time
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

	if tcpAddr, ok := s.listener.Addr().(*net.TCPAddr); ok {
		s.baseURL = fmt.Sprintf("http://127.0.0.1:%d", tcpAddr.Port)
	} else {
		s.baseURL = fmt.Sprintf("http://%s", s.listener.Addr().String())
	}

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
	s.jobCounter.Store(0)
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

	if conn.CreatedAt.IsZero() {
		conn.CreatedAt = time.Now().UTC()
	}

	if conn.UpdatedAt.IsZero() {
		conn.UpdatedAt = time.Now().UTC()
	}

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

	cp := &MockSchema{
		ID:           schema.ID,
		ConfigName:   schema.ConfigName,
		DatabaseName: schema.DatabaseName,
		Type:         schema.Type,
	}

	for _, tbl := range schema.Tables {
		fields := make([]string, len(tbl.Fields))
		copy(fields, tbl.Fields)
		cp.Tables = append(cp.Tables, MockTable{Name: tbl.Name, Fields: fields})
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
		Status:    result.Status,
		Message:   result.Message,
		LatencyMs: result.LatencyMs,
	}
	s.testResults[connectionID] = cp
}

// AddJob adds an extraction job that can be polled by its ID.
func (s *MockFetcherServer) AddJob(job MockExtractionJob) {
	s.mu.Lock()
	defer s.mu.Unlock()

	if job.CreatedAt.IsZero() {
		job.CreatedAt = time.Now().UTC()
	}

	s.jobs[job.ID] = &job
}

// SetJobStatus updates the status of an existing extraction job.
func (s *MockFetcherServer) SetJobStatus(jobID string, status string) {
	s.mu.Lock()
	defer s.mu.Unlock()

	if job, ok := s.jobs[jobID]; ok {
		job.Status = status

		if status == "completed" || status == "failed" {
			now := time.Now().UTC()
			job.CompletedAt = &now
		}
	}
}

// GetLastJobID returns the ID of the most recently submitted extraction job.
// This allows tests to capture the actual generated ID instead of hardcoding it.
func (s *MockFetcherServer) GetLastJobID() string {
	s.mu.RLock()
	defer s.mu.RUnlock()

	// Find the job with the highest counter suffix (most recent).
	var latest string
	var latestTime time.Time

	for _, job := range s.jobs {
		if latest == "" || job.CreatedAt.After(latestTime) {
			latest = job.ID
			latestTime = job.CreatedAt
		}
	}

	return latest
}

// --- Route registration ---

func (s *MockFetcherServer) registerRoutes(mux *http.ServeMux) {
	// Go 1.22+ ServeMux supports method+path patterns.
	mux.HandleFunc("GET /health", s.handleHealth)
	mux.HandleFunc("GET /v1/management/connections", s.handleListConnections)
	mux.HandleFunc("POST /v1/fetcher", s.handleSubmitExtraction)

	// Paths with path segments that need manual extraction:
	// Go 1.22 ServeMux supports {name} wildcards.
	mux.HandleFunc("GET /v1/management/connections/{id}/schema", s.handleGetSchema)
	mux.HandleFunc("POST /v1/management/connections/{id}/test", s.handleTestConnection)
	mux.HandleFunc("GET /v1/fetcher/{jobId}", s.handleGetExtractionStatus)
}
