// Package query provides read operations for discovery management.
package query

import (
	"errors"
	"time"

	libLog "github.com/LerianStudio/lib-commons/v4/commons/log"

	"github.com/LerianStudio/matcher/internal/discovery/domain/repositories"
	"github.com/LerianStudio/matcher/internal/discovery/ports"
	sharedPorts "github.com/LerianStudio/matcher/internal/shared/ports"
)

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
	fetcherClient  sharedPorts.FetcherClient
	connRepo       repositories.ConnectionRepository
	schemaRepo     repositories.SchemaRepository
	extractionRepo repositories.ExtractionRepository
	logger         libLog.Logger
	schemaCache    ports.SchemaCache // optional cache layer
	cacheTTL       time.Duration     // TTL for cached schemas
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
func (uc *UseCase) WithSchemaCache(cache ports.SchemaCache, ttl time.Duration) {
	uc.schemaCache = cache
	uc.cacheTTL = ttl
}
