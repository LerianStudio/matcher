// Package command provides write operations for discovery management.
package command

import (
	"errors"
	"fmt"

	libLog "github.com/LerianStudio/lib-commons/v4/commons/log"

	"github.com/LerianStudio/matcher/internal/discovery/domain/repositories"
	"github.com/LerianStudio/matcher/internal/discovery/ports"
	"github.com/LerianStudio/matcher/internal/discovery/services/syncer"
	sharedPorts "github.com/LerianStudio/matcher/internal/shared/ports"
)

// Sentinel errors for discovery commands.
var (
	ErrFetcherNotEnabled          = errors.New("fetcher integration is not enabled")
	ErrFetcherUnavailable         = errors.New("fetcher service is unavailable")
	ErrConnectionNotFound         = errors.New("fetcher connection not found")
	ErrSourceNotFetcherType       = errors.New("source is not a FETCHER type")
	ErrMissingFetcherConnectionID = errors.New("source config missing fetcherConnectionId")
	ErrExtractionTimeout          = errors.New("extraction job timed out")
	ErrExtractionFailed           = errors.New("extraction job failed")
	ErrExtractionNotFound         = errors.New("extraction request not found")
	ErrNilExtractionStatus        = errors.New("fetcher returned nil extraction status")
	ErrNilFetcherClient           = errors.New("fetcher client is required")
	ErrNilConnectionRepository    = errors.New("connection repository is required")
	ErrNilSchemaRepository        = errors.New("schema repository is required")
	ErrNilExtractionRepository    = errors.New("extraction repository is required")
)

// UseCase orchestrates discovery write operations.
type UseCase struct {
	fetcherClient    sharedPorts.FetcherClient
	connRepo         repositories.ConnectionRepository
	schemaRepo       repositories.SchemaRepository
	extractionRepo   repositories.ExtractionRepository
	logger           libLog.Logger
	extractionPoller ports.ExtractionJobPoller // optional async poller
	syncer           *syncer.ConnectionSyncer
}

// NewUseCase creates a new discovery command use case.
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

	cs, err := syncer.NewConnectionSyncer(connRepo, schemaRepo)
	if err != nil {
		return nil, fmt.Errorf("create connection syncer: %w", err)
	}

	return &UseCase{
		fetcherClient:  fetcherClient,
		connRepo:       connRepo,
		schemaRepo:     schemaRepo,
		extractionRepo: extractionRepo,
		logger:         logger,
		syncer:         cs,
	}, nil
}

// WithExtractionPoller adds an optional extraction poller for async job monitoring.
func (uc *UseCase) WithExtractionPoller(poller ports.ExtractionJobPoller) {
	uc.extractionPoller = poller
}
