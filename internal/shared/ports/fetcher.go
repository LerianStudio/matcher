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
	Schema       string
	DatabaseName string
	UserName     string
	ProductName  string
	Metadata     map[string]any
	CreatedAt    time.Time
	UpdatedAt    time.Time
}

// FetcherTableSchema represents a table's schema as discovered by Fetcher.
// Fetcher returns flat field name arrays per table, not rich column metadata.
type FetcherTableSchema struct {
	Name   string   `json:"Name"`
	Fields []string `json:"Fields"`
}

// FetcherSchema represents the full schema of a Fetcher connection.
type FetcherSchema struct {
	ID           string               `json:"ID"`
	ConfigName   string               `json:"ConfigName"`
	DatabaseName string               `json:"DatabaseName"`
	Type         string               `json:"Type"`
	Tables       []FetcherTableSchema `json:"Tables"`
	DiscoveredAt time.Time            `json:"DiscoveredAt"`
}

// FetcherTestResult represents the result of testing a Fetcher connection.
// Consumers derive Healthy from Status == "success".
type FetcherTestResult struct {
	Status    string
	Message   string
	LatencyMs int64
}

// ExtractionJobInput represents the input for submitting a data extraction job.
// MappedFields is configName -> table -> columns. Filters is configName -> table -> filter.
// Metadata must include a "source" key required by Fetcher.
type ExtractionJobInput struct {
	MappedFields map[string]map[string][]string
	Filters      map[string]map[string]map[string]any
	Metadata     map[string]any
}

// ExtractionJobStatus represents the status of a running extraction job.
type ExtractionJobStatus struct {
	ID     string
	Status string // pending, running, completed, failed
	// ResultPath is the S3 object key where Fetcher stored the (encrypted) extraction output.
	ResultPath string
	// ResultHmac is the HMAC-SHA256 hex digest that Fetcher's worker computed over the
	// plaintext JSON result *before* AES-GCM encryption and storage. Fetcher signs the
	// document using a key derived via HKDF from its master key (APP_ENC_KEY) with context
	// "fetcher-external-hmac-v1".
	//
	// Matcher currently stores this value but does NOT verify it because:
	//  1. The Matcher does not download or decrypt the extraction result data — it only
	//     tracks the ResultPath as metadata. The ingestion context receives data through
	//     separate upload flows, not by reading from Fetcher's S3 bucket.
	//  2. Verification requires the external HMAC key, which is derived from Fetcher's
	//     APP_ENC_KEY via `make derive-key`. Sharing this key requires operational
	//     coordination (e.g., a shared secret in AWS Secrets Manager or a new env var
	//     FETCHER_EXTERNAL_HMAC_KEY provisioned during deployment).
	//  3. Even with the key, the stored data is AES-GCM encrypted — verification would
	//     also require the storage encryption derived key to first decrypt, then HMAC-check.
	//
	// To enable end-to-end verification, the following would be needed:
	//  - Fetcher's external HMAC key shared with Matcher (env var or secret manager).
	//  - Matcher downloading and decrypting the result (requires the storage encryption key too).
	//  - Or: Fetcher providing a verification endpoint that accepts a hash and confirms integrity.
	ResultHmac  string
	RequestHash string
	Metadata    map[string]any
	// CreatedAt is the timestamp when the extraction job was created by Fetcher.
	CreatedAt time.Time
	// CompletedAt is the timestamp when the extraction job completed. Nil while
	// the job is still running or queued. Consumers MUST nil-check before
	// dereferencing -- use IsZero() on the dereferenced value only after the
	// pointer is confirmed non-nil.
	CompletedAt *time.Time
}

// FetcherClient defines the interface for communicating with the Fetcher service.
// This is a shared port interface that allows the discovery context to communicate
// with the external Fetcher microservice without hard-coding HTTP details.
type FetcherClient interface {
	// IsHealthy checks if the Fetcher service is reachable and healthy.
	IsHealthy(ctx context.Context) bool

	// ListConnections retrieves all database connections managed by Fetcher.
	// productName is sent as X-Product-Name header to scope the listing.
	ListConnections(ctx context.Context, productName string) ([]*FetcherConnection, error)

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
