// Package query provides read operations for discovery management.
package query

import (
	"context"
	"errors"
	"time"

	libLog "github.com/LerianStudio/lib-commons/v5/commons/log"

	"github.com/LerianStudio/matcher/internal/discovery/domain/repositories"
	"github.com/LerianStudio/matcher/internal/discovery/ports"
	sharedPorts "github.com/LerianStudio/matcher/internal/shared/ports"
)

// SchemaCache is the narrow subset of the schemacache.Cache surface the
// query use case depends on. Defining it here lets tests substitute a
// stub without importing the concrete package.
//
// The concrete SchemaCache satisfies this interface by method
// match; no explicit assertion is required.
type SchemaCache interface {
	GetSchema(ctx context.Context, connectionID string) (*sharedPorts.FetcherSchema, error)
	SetSchema(ctx context.Context, connectionID string, schema *sharedPorts.FetcherSchema, ttl time.Duration) error
}

// Sentinel errors for query use case validation.
var (
	ErrNilFetcherClient        = errors.New("fetcher client is required")
	ErrNilConnectionRepository = errors.New("connection repository is required")
	ErrNilSchemaRepository     = errors.New("schema repository is required")
	ErrNilExtractionRepository = errors.New("extraction repository is required")
)

// Sentinel errors for query results.
var (
	// ErrConnectionNotFound is returned when a requested connection does not exist.
	ErrConnectionNotFound = errors.New("connection not found")
	// ErrExtractionNotFound is returned when a requested extraction does not exist.
	ErrExtractionNotFound = errors.New("extraction not found")
)

// UseCase orchestrates discovery read operations.
type UseCase struct {
	fetcherClient    sharedPorts.FetcherClient
	connRepo         repositories.ConnectionRepository
	schemaRepo       repositories.SchemaRepository
	extractionRepo   repositories.ExtractionRepository
	logger           libLog.Logger
	schemaCache      SchemaCache                 // optional cache layer
	cacheTTL         time.Duration               // TTL for cached schemas
	heartbeatReader  ports.BridgeHeartbeatReader // optional bridge worker liveness source (C15)
	heartbeatStaleAt time.Duration               // worker marked unhealthy when staleness > this
}

// NewUseCase creates a new discovery query use case.
func NewUseCase(
	fetcherClient sharedPorts.FetcherClient,
	connRepo repositories.ConnectionRepository,
	schemaRepo repositories.SchemaRepository,
	extractionRepo repositories.ExtractionRepository,
	logger libLog.Logger,
) (*UseCase, error) {
	if fetcherClient == nil {
		return nil, ErrNilFetcherClient
	}

	if connRepo == nil {
		return nil, ErrNilConnectionRepository
	}

	if schemaRepo == nil {
		return nil, ErrNilSchemaRepository
	}

	if extractionRepo == nil {
		return nil, ErrNilExtractionRepository
	}

	if logger == nil {
		logger = &libLog.NopLogger{}
	}

	return &UseCase{
		fetcherClient:  fetcherClient,
		connRepo:       connRepo,
		schemaRepo:     schemaRepo,
		extractionRepo: extractionRepo,
		logger:         logger,
	}, nil
}

// WithSchemaCache adds an optional schema cache to the query use case.
func (uc *UseCase) WithSchemaCache(cache SchemaCache, ttl time.Duration) {
	uc.schemaCache = cache
	uc.cacheTTL = ttl
}

// WithBridgeHeartbeatReader wires the optional liveness source consumed by
// CountBridgeReadinessByTenant. staleAfter is the threshold beyond which a
// missing / old heartbeat flips the dashboard's worker-healthy indicator
// to false; callers typically pass 3 × bridgeInterval to stay consistent
// with the worker's write TTL. A non-positive staleAfter disables the
// derived-healthy computation — the timestamp and staleness seconds are
// still reported. C15.
func (uc *UseCase) WithBridgeHeartbeatReader(reader ports.BridgeHeartbeatReader, staleAfter time.Duration) {
	if uc == nil {
		return
	}

	uc.heartbeatReader = reader
	uc.heartbeatStaleAt = staleAfter
}
