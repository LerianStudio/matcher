package ports

import (
	"context"
	"errors"
	"time"
)

var (
	// ErrFetcherUnavailable indicates the Fetcher service could not be reached.
	ErrFetcherUnavailable = errors.New("fetcher service is unavailable")
	// ErrFetcherResourceNotFound indicates Fetcher returned 404 for a requested resource.
	ErrFetcherResourceNotFound = errors.New("fetcher resource not found")
)

//go:generate mockgen -source=fetcher.go -destination=mocks/fetcher_mock.go -package=mocks

// FetcherConnection represents a database connection managed by Fetcher.
type FetcherConnection struct {
	ID           string
	ConfigName   string
	DatabaseType string
	Host         string
	Port         int
	DatabaseName string
	ProductName  string
	Status       string
}

// FetcherTableSchema represents a table's schema as discovered by Fetcher.
type FetcherTableSchema struct {
	TableName string
	Columns   []FetcherColumnInfo
}

// FetcherColumnInfo represents a column's metadata.
type FetcherColumnInfo struct {
	Name     string
	Type     string
	Nullable bool
}

// FetcherSchema represents the full schema of a Fetcher connection.
type FetcherSchema struct {
	ConnectionID string
	Tables       []FetcherTableSchema
	DiscoveredAt time.Time
}

// FetcherTestResult represents the result of testing a Fetcher connection.
type FetcherTestResult struct {
	ConnectionID string
	Healthy      bool
	LatencyMs    int64
	ErrorMessage string
}

// ExtractionJobInput represents the input for submitting a data extraction job.
type ExtractionJobInput struct {
	ConnectionID string
	Tables       map[string]ExtractionTableConfig
	Filters      *ExtractionFilters
}

// ExtractionTableConfig defines what to extract from a single table.
type ExtractionTableConfig struct {
	Columns   []string
	StartDate string
	EndDate   string
}

// ExtractionJobStatus represents the status of a running extraction job.
type ExtractionJobStatus struct {
	JobID        string
	Status       string // PENDING, RUNNING, COMPLETE, FAILED
	Progress     int    // 0-100
	ResultPath   string
	ErrorMessage string
}

// FetcherClient defines the interface for communicating with the Fetcher service.
// This is a shared port interface that allows the discovery context to communicate
// with the external Fetcher microservice without hard-coding HTTP details.
type FetcherClient interface {
	// IsHealthy checks if the Fetcher service is reachable and healthy.
	IsHealthy(ctx context.Context) bool

	// ListConnections retrieves all database connections managed by Fetcher.
	ListConnections(ctx context.Context, orgID string) ([]*FetcherConnection, error)

	// GetSchema retrieves the schema (tables and columns) for a specific connection.
	GetSchema(ctx context.Context, connectionID string) (*FetcherSchema, error)

	// TestConnection tests connectivity for a specific Fetcher connection.
	TestConnection(ctx context.Context, connectionID string) (*FetcherTestResult, error)

	// SubmitExtractionJob submits an async data extraction job to Fetcher.
	// Returns the Fetcher-assigned job ID.
	SubmitExtractionJob(ctx context.Context, input ExtractionJobInput) (string, error)

	// GetExtractionJobStatus polls the status of a running extraction job.
	GetExtractionJobStatus(ctx context.Context, jobID string) (*ExtractionJobStatus, error)
}
