// Package command provides write operations for discovery management.
package command

import (
	"errors"
	"fmt"
	"time"

	libLog "github.com/LerianStudio/lib-commons/v5/commons/log"

	"github.com/LerianStudio/matcher/internal/discovery/domain/repositories"
	"github.com/LerianStudio/matcher/internal/discovery/extractionpoller"
	"github.com/LerianStudio/matcher/internal/discovery/schemacache"
	"github.com/LerianStudio/matcher/internal/discovery/services/syncer"
	sharedPorts "github.com/LerianStudio/matcher/internal/shared/ports"
)

// Sentinel errors for discovery commands.
var (
	ErrFetcherUnavailable           = errors.New("fetcher service is unavailable")
	ErrConnectionNotFound           = errors.New("fetcher connection not found")
	ErrInvalidExtractionRequest     = errors.New("invalid extraction request")
	ErrExtractionTrackingIncomplete = errors.New("extraction tracking is incomplete")
	ErrDiscoveryRefreshInProgress   = errors.New("discovery refresh already in progress")
	ErrExtractionNotFound           = errors.New("extraction request not found")
	ErrNilExtractionStatus          = errors.New("fetcher returned nil extraction status")
	ErrNilTestConnectionResult      = errors.New("fetcher returned nil connection test result")
	ErrNilFetcherClient             = errors.New("fetcher client is required")
	ErrNilConnectionRepository      = errors.New("connection repository is required")
	ErrNilSchemaRepository          = errors.New("schema repository is required")
	ErrNilExtractionRepository      = errors.New("extraction repository is required")
)

const discoveryRefreshLockTTLMultiplier = 2

// UseCase orchestrates discovery write operations.
type UseCase struct {
	fetcherClient        sharedPorts.FetcherClient
	connRepo             repositories.ConnectionRepository
	schemaRepo           repositories.SchemaRepository
	extractionRepo       repositories.ExtractionRepository
	logger               libLog.Logger
	extractionPoller     *extractionpoller.Poller // optional async poller
	syncer               *syncer.ConnectionSyncer
	refreshLockProvider  sharedPorts.InfrastructureProvider
	refreshLockTTL       time.Duration
	refreshLockTTLGetter func() time.Duration
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
func (uc *UseCase) WithExtractionPoller(poller *extractionpoller.Poller) {
	if uc == nil {
		return
	}

	uc.extractionPoller = poller
}

// WithSchemaCache wires an optional cache into the connection syncer so manual
// discovery refreshes immediately replace stale cached schemas.
func (uc *UseCase) WithSchemaCache(cache *schemacache.Cache, ttl time.Duration) {
	if uc == nil || uc.syncer == nil {
		return
	}

	uc.syncer.WithSchemaCache(cache, ttl)
}

// WithDiscoveryRefreshLock enables distributed locking for manual refresh operations.
func (uc *UseCase) WithDiscoveryRefreshLock(provider sharedPorts.InfrastructureProvider, interval time.Duration) {
	if uc == nil {
		return
	}

	uc.refreshLockProvider = provider

	if interval <= 0 {
		interval = time.Minute
	}

	uc.refreshLockTTL = discoveryRefreshLockTTLMultiplier * interval
}

// WithDiscoveryRefreshLockGetter configures a dynamic TTL getter for the
// discovery refresh lock, allowing runtime config updates to adjust the lock.
func (uc *UseCase) WithDiscoveryRefreshLockGetter(getter func() time.Duration) {
	if uc == nil {
		return
	}

	uc.refreshLockTTLGetter = getter
}
